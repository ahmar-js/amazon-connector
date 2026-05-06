import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from django.db import transaction
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from sqlalchemy import bindparam, text

from .data_processor import process_amazon_data
from .job_locks import (
    AMAZON_ORDERS_LOCK_NAME,
    LOCK_OWNER_MAIN_INGESTION,
    LOCK_OWNER_STATUS_SYNC,
    acquire_job_lock,
    get_job_lock,
    is_lock_active,
    is_protected_ingestion_window,
    release_job_lock,
)
from .marketplaces_creds import get_credentials_for_marketplace, normalize_company_name
from .models import SCMOrderReconciliationQueue

logger = logging.getLogger(__name__)


PKT_TIMEZONE = ZoneInfo("Asia/Karachi")

SHIPPED_STATUS = "Shipped"
FINAL_ORDER_STATUSES = {"Shipped", "Canceled", "Unfulfillable"}
CHANGEABLE_ORDER_STATUSES = {
    "Pending",
    "Unshipped",
    "PartiallyShipped",
    "InvoiceUnconfirmed",
    "PendingAvailability",
}

STATUS_SYNC_LOCK_EXPIRES_MINUTES = 30
MAX_RECONCILIATION_RUNTIME_MINUTES = 60
MAX_RECONCILIATION_ROWS_PER_RUN = 500
MAX_RECONCILIATION_ROWS_PER_MARKETPLACE = 100
MAX_RECONCILIATION_BATCH_SIZE = 50
MAX_RECONCILIATION_BACKFILL_ROWS_PER_TABLE = 5000

ORDER_LOOKUP_DELAY_SECONDS = 2
ORDER_ITEMS_LOOKUP_DELAY_SECONDS = 2
API_MAX_RETRIES = 3
API_REQUEST_TIMEOUT_SECONDS = 60

SCM_SKU_MAPPER_TABLE_MAPPING = {
    "A1PA6795UKMFR9": "scm_sku_mapper_de",
    "A1RKKUPIHCS9HS": "scm_sku_mapper_es",
    "APJ6JRA9NG5V4": "scm_sku_mapper_it",
    "A1F83G8C2ARO7P": "scm_sku_mapper_uk",
    "ATVPDKIKX0DER": "scm_sku_mapper_usa",
    "A2EUQ1WTGCTBG2": "scm_sku_mapper_ca",
    "A13V1IB3VIYZZH": "scm_sku_mapper_fr",
}

MARKETPLACE_ID_TO_CODE = {
    "ATVPDKIKX0DER": "US",
    "A2EUQ1WTGCTBG2": "CA",
    "A1F83G8C2ARO7P": "UK",
    "A1PA6795UKMFR9": "DE",
    "A13V1IB3VIYZZH": "FR",
    "APJ6JRA9NG5V4": "IT",
    "A1RKKUPIHCS9HS": "ES",
}

MARKETPLACE_CODE_TO_ID = {code: marketplace_id for marketplace_id, code in MARKETPLACE_ID_TO_CODE.items()}

SP_API_BASE_URLS = {
    "ATVPDKIKX0DER": "https://sellingpartnerapi-na.amazon.com",
    "A2EUQ1WTGCTBG2": "https://sellingpartnerapi-na.amazon.com",
    "A1F83G8C2ARO7P": "https://sellingpartnerapi-eu.amazon.com",
    "A1PA6795UKMFR9": "https://sellingpartnerapi-eu.amazon.com",
    "A13V1IB3VIYZZH": "https://sellingpartnerapi-eu.amazon.com",
    "APJ6JRA9NG5V4": "https://sellingpartnerapi-eu.amazon.com",
    "A1RKKUPIHCS9HS": "https://sellingpartnerapi-eu.amazon.com",
}

SCM_AMAZON_ORDERS_COLUMNS = [
    "CLEAN_DateTime",
    "Date",
    "OrderId",
    "SKU",
    "Type",
    "Company",
    "Region",
    "Quantity",
    "FulfillmentChannel",
    "data_fetch_Date",
]


def _normalize_text(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value).strip()


def _normalize_status(value) -> str:
    return _normalize_text(value)


def is_final_order_status(status: str) -> bool:
    return _normalize_status(status) in FINAL_ORDER_STATUSES


def is_changeable_order_status(status: str) -> bool:
    normalized = _normalize_status(status)
    if not normalized:
        return False
    if normalized in FINAL_ORDER_STATUSES:
        return False
    return normalized in CHANGEABLE_ORDER_STATUSES or normalized not in FINAL_ORDER_STATUSES


def _ensure_aware_utc(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if timezone.is_naive(value):
        return timezone.make_aware(value, timezone=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_datetime_value(value) -> Optional[datetime]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return _ensure_aware_utc(value.to_pydatetime())
    if isinstance(value, datetime):
        return _ensure_aware_utc(value)

    value_str = str(value).strip()
    if not value_str:
        return None
    if value_str.endswith("Z"):
        value_str = value_str[:-1] + "+00:00"

    parsed = parse_datetime(value_str)
    if parsed is None:
        try:
            parsed = datetime.fromisoformat(value_str)
        except ValueError:
            return None
    return _ensure_aware_utc(parsed)


def _business_today_start_utc(now: Optional[datetime] = None) -> datetime:
    current_time = _ensure_aware_utc(now or timezone.now())
    pkt_now = current_time.astimezone(PKT_TIMEZONE)
    pkt_midnight = datetime.combine(pkt_now.date(), datetime.min.time(), tzinfo=PKT_TIMEZONE)
    return pkt_midnight.astimezone(timezone.utc)


def _is_purchase_date_allowed(purchase_date: Optional[datetime], now: Optional[datetime] = None) -> bool:
    if purchase_date is None:
        return False
    return _ensure_aware_utc(purchase_date) < _business_today_start_utc(now)


def calculate_next_check_at(purchase_date: Optional[datetime], reference_time: Optional[datetime] = None) -> datetime:
    now = _ensure_aware_utc(reference_time or timezone.now())
    purchase_dt = _ensure_aware_utc(purchase_date) if purchase_date else None
    age = now - purchase_dt if purchase_dt else timedelta(days=30)

    if age < timedelta(hours=24):
        delay = timedelta(hours=6)
    elif age < timedelta(days=3):
        delay = timedelta(hours=12)
    elif age < timedelta(days=7):
        delay = timedelta(hours=36)
    elif age < timedelta(days=30):
        delay = timedelta(hours=72)
    else:
        delay = timedelta(days=7)

    return now + delay


def _marketplace_code_for_id(marketplace_id: str) -> str:
    return MARKETPLACE_ID_TO_CODE.get(marketplace_id, marketplace_id)


def _validated_source_table(marketplace_id: str, source_table: str) -> Optional[str]:
    expected = SCM_SKU_MAPPER_TABLE_MAPPING.get(marketplace_id)
    if expected and expected == source_table:
        return expected
    logger.error(
        "[scm_reconciliation] Unsafe source table rejected marketplace_id=%s source_table=%s expected=%s",
        marketplace_id,
        source_table,
        expected,
    )
    return None


def _get_mssql_table_columns(conn, source_table: str) -> set:
    rows = conn.execute(
        text(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = :table_name
            """
        ),
        {"table_name": source_table},
    ).fetchall()
    return {row[0] for row in rows}


def _build_backfill_query(source_table: str, selected_columns: List[str], limit_per_table: int):
    safe_table = f"[{source_table}]"
    columns_sql = ", ".join(f"[{column}]" for column in selected_columns)
    purchase_expr = "TRY_CONVERT(datetime2, REPLACE(REPLACE([PurchaseDate], 'T', ' '), 'Z', ''))"
    query = text(
        f"""
        SELECT TOP {int(limit_per_table)} {columns_sql}
        FROM {safe_table}
        WHERE [OrderStatus] IN :statuses
          AND [AmazonOrderId] IS NOT NULL
          AND [PurchaseDate] IS NOT NULL
          AND {purchase_expr} >= :start_utc
          AND {purchase_expr} < :cutoff_utc
        ORDER BY {purchase_expr} ASC
        """
    )
    return query.bindparams(bindparam("statuses", expanding=True))


def backfill_reconciliation_queue_from_scm_sources(
    days: int = 45,
    marketplace_code: Optional[str] = None,
    company_name: Optional[str] = None,
    limit_per_table: int = MAX_RECONCILIATION_BACKFILL_ROWS_PER_TABLE,
) -> Dict:
    """
    Backfill reconciliation queue rows from existing scm_sku_mapper_* MSSQL tables.

    This is idempotent and only queues non-final rows older than the current PKT
    business day. It does not call Amazon and does not write scm_amazon_orders.
    """
    if days <= 0:
        raise ValueError("days must be greater than 0")
    if limit_per_table <= 0:
        raise ValueError("limit_per_table must be greater than 0")

    now = timezone.now()
    cutoff_utc = _business_today_start_utc(now)
    start_utc = cutoff_utc - timedelta(days=days)
    requested_marketplace_code = marketplace_code.upper().strip() if marketplace_code else None
    selected_statuses = sorted(CHANGEABLE_ORDER_STATUSES)

    summary = {
        "status": "completed",
        "days": days,
        "marketplace_code": requested_marketplace_code,
        "company_name": company_name,
        "limit_per_table": limit_per_table,
        "start_utc": start_utc.isoformat(),
        "cutoff_utc": cutoff_utc.isoformat(),
        "tables_scanned": 0,
        "source_rows": 0,
        "inserted": 0,
        "updated": 0,
        "skipped_final": 0,
        "skipped_current_day": 0,
        "skipped_existing_final": 0,
        "skipped_invalid": 0,
        "errors": [],
    }

    if requested_marketplace_code and requested_marketplace_code not in MARKETPLACE_CODE_TO_ID:
        raise ValueError(f"Unsupported marketplace_code: {requested_marketplace_code}")

    from .simple_db_save import create_mssql_connection

    engine = create_mssql_connection()
    with engine.connect() as conn:
        for current_marketplace_id, source_table in SCM_SKU_MAPPER_TABLE_MAPPING.items():
            current_marketplace_code = _marketplace_code_for_id(current_marketplace_id)
            if requested_marketplace_code and current_marketplace_code != requested_marketplace_code:
                continue

            expected_table = _validated_source_table(current_marketplace_id, source_table)
            if expected_table is None:
                summary["errors"].append(f"invalid_source_table:{source_table}")
                continue

            try:
                available_columns = _get_mssql_table_columns(conn, expected_table)
                required_columns = ["PurchaseDate", "AmazonOrderId", "OrderStatus"]
                if any(column not in available_columns for column in required_columns):
                    missing = [column for column in required_columns if column not in available_columns]
                    message = f"{expected_table}: missing required columns {missing}"
                    logger.error("[scm_reconciliation] Backfill skipped table=%s missing=%s", expected_table, missing)
                    summary["errors"].append(message)
                    continue

                candidate_columns = [
                    "PurchaseDate",
                    "PurchaseDate_conversion",
                    "AmazonOrderId",
                    "ASIN",
                    "Title",
                    "SalesChannel",
                    "Region",
                    "OrderStatus",
                    "FulfillmentChannel",
                    "SellerSKU",
                    "QuantityOrdered",
                    "Company",
                    "LastUpdateDate",
                ]
                selected_columns = [column for column in candidate_columns if column in available_columns]
                query = _build_backfill_query(expected_table, selected_columns, limit_per_table)
                params = {
                    "statuses": selected_statuses,
                    "start_utc": start_utc.replace(tzinfo=None),
                    "cutoff_utc": cutoff_utc.replace(tzinfo=None),
                }
                logger.info(
                    "[scm_reconciliation] Backfill scanning table=%s marketplace=%s days=%s limit=%s",
                    expected_table,
                    current_marketplace_code,
                    days,
                    limit_per_table,
                )
                result = conn.execute(query, params)
                rows = result.fetchall()
                df = pd.DataFrame(rows, columns=result.keys())
                summary["tables_scanned"] += 1
                summary["source_rows"] += len(df)

                if df.empty:
                    logger.info("[scm_reconciliation] Backfill found no rows table=%s", expected_table)
                    continue

                if company_name:
                    if "Company" not in df.columns:
                        logger.warning(
                            "[scm_reconciliation] Backfill company filter requested but table has no Company column table=%s",
                            expected_table,
                        )
                        df = df.iloc[0:0]
                    else:
                        df = df[df["Company"].fillna("").astype(str).str.strip() == normalize_company_name(company_name)]

                if df.empty:
                    logger.info("[scm_reconciliation] Backfill found no rows after company filter table=%s", expected_table)
                    continue

                if "Company" in df.columns:
                    grouped = df.groupby(df["Company"].fillna("").astype(str).str.strip(), dropna=False)
                    for row_company, company_df in grouped:
                        resolved_company = normalize_company_name(row_company or company_name)
                        queue_result = upsert_non_final_orders_to_reconciliation_queue(
                            company_df,
                            marketplace_id=current_marketplace_id,
                            source_table=expected_table,
                            company_name=resolved_company,
                            due_immediately=True,
                        )
                        for key in ("inserted", "updated", "skipped_final", "skipped_current_day", "skipped_existing_final", "skipped_invalid"):
                            summary[key] += queue_result.get(key, 0)
                else:
                    queue_result = upsert_non_final_orders_to_reconciliation_queue(
                        df,
                        marketplace_id=current_marketplace_id,
                        source_table=expected_table,
                        company_name=company_name,
                        due_immediately=True,
                    )
                    for key in ("inserted", "updated", "skipped_final", "skipped_current_day", "skipped_existing_final", "skipped_invalid"):
                        summary[key] += queue_result.get(key, 0)

            except Exception as exc:
                logger.error(
                    "[scm_reconciliation] Backfill failed table=%s error=%s",
                    expected_table,
                    exc,
                    exc_info=True,
                )
                summary["errors"].append(f"{expected_table}: {exc}")

    if summary["errors"]:
        summary["status"] = "partial_success"
    logger.info("[scm_reconciliation] Backfill finished summary=%s", summary)
    return summary


def upsert_non_final_orders_to_reconciliation_queue(
    mssql_df: pd.DataFrame,
    marketplace_id: str,
    source_table: str,
    company_name: Optional[str] = None,
    due_immediately: bool = False,
) -> Dict:
    """Upsert non-final SCM rows into the reconciliation queue.

    Main ingestion uses backoff before the first status check. Historical
    backfill marks rows due immediately so the first reconciliation run can
    start catching up without waiting days.
    """
    result = {
        "success": True,
        "inserted": 0,
        "updated": 0,
        "skipped_final": 0,
        "skipped_current_day": 0,
        "skipped_existing_final": 0,
        "skipped_invalid": 0,
    }

    if mssql_df is None or mssql_df.empty:
        return result

    expected_table = _validated_source_table(marketplace_id, source_table)
    if expected_table is None:
        result["success"] = False
        result["error"] = "invalid_source_table"
        return result

    marketplace_code = _marketplace_code_for_id(marketplace_id)
    resolved_company = normalize_company_name(company_name)
    now = timezone.now()

    for _, row in mssql_df.iterrows():
        status = _normalize_status(row.get("OrderStatus"))
        if not is_changeable_order_status(status):
            result["skipped_final"] += 1
            continue

        amazon_order_id = _normalize_text(row.get("AmazonOrderId"))
        if not amazon_order_id:
            result["skipped_invalid"] += 1
            continue

        purchase_date = _parse_datetime_value(row.get("PurchaseDate"))
        if not _is_purchase_date_allowed(purchase_date, now):
            result["skipped_current_day"] += 1
            continue

        seller_sku = _normalize_text(row.get("SellerSKU"))
        asin = _normalize_text(row.get("ASIN"))
        last_update_date = _parse_datetime_value(row.get("LastUpdateDate"))
        next_check_at = now if due_immediately else calculate_next_check_at(purchase_date, now)

        with transaction.atomic():
            queue_row = (
                SCMOrderReconciliationQueue.objects
                .select_for_update()
                .filter(
                    company_name=resolved_company,
                    marketplace_code=marketplace_code,
                    amazon_order_id=amazon_order_id,
                    seller_sku=seller_sku,
                    asin=asin,
                )
                .first()
            )

            if queue_row and queue_row.is_final:
                result["skipped_existing_final"] += 1
                continue

            if queue_row:
                queue_row.marketplace_id = marketplace_id
                queue_row.source_table = expected_table
                queue_row.current_status = status
                queue_row.purchase_date = purchase_date
                queue_row.last_update_date = last_update_date or queue_row.last_update_date
                if queue_row.next_check_at is None or queue_row.next_check_at > next_check_at:
                    queue_row.next_check_at = next_check_at
                queue_row.last_error = ""
                queue_row.save(
                    update_fields=[
                        "marketplace_id",
                        "source_table",
                        "current_status",
                        "purchase_date",
                        "last_update_date",
                        "next_check_at",
                        "last_error",
                        "updated_at",
                    ]
                )
                result["updated"] += 1
            else:
                SCMOrderReconciliationQueue.objects.create(
                    company_name=resolved_company,
                    marketplace_code=marketplace_code,
                    marketplace_id=marketplace_id,
                    source_table=expected_table,
                    amazon_order_id=amazon_order_id,
                    seller_sku=seller_sku,
                    asin=asin,
                    current_status=status,
                    purchase_date=purchase_date,
                    last_update_date=last_update_date,
                    next_check_at=next_check_at,
                )
                result["inserted"] += 1

    logger.info("[scm_reconciliation] Queue upsert result: %s", result)
    return result


def _status_sync_should_exit() -> Tuple[bool, str]:
    if is_protected_ingestion_window(log_result=True):
        logger.info("[scm_reconciliation] Protected ingestion window active; status sync will exit")
        return True, "protected_window"

    lock = get_job_lock(AMAZON_ORDERS_LOCK_NAME)
    if lock and lock.stop_requested:
        logger.warning("[scm_reconciliation] Stop requested on global order lock; status sync will exit")
        return True, "stop_requested"
    if is_lock_active(lock) and lock.locked_by == LOCK_OWNER_MAIN_INGESTION:
        logger.info("[scm_reconciliation] Main ingestion is running; status sync will exit")
        return True, "main_ingestion_running"

    return False, ""


def _get_due_queue_rows(remaining_rows: int, marketplace_counts: Dict[str, int]) -> List[SCMOrderReconciliationQueue]:
    now = timezone.now()
    cutoff = _business_today_start_utc(now)
    selected = []
    scan_limit = min(max(MAX_RECONCILIATION_BATCH_SIZE * 3, MAX_RECONCILIATION_BATCH_SIZE), remaining_rows * 3)

    candidates = (
        SCMOrderReconciliationQueue.objects
        .filter(
            is_final=False,
            next_check_at__lte=now,
            purchase_date__lt=cutoff,
        )
        .order_by("marketplace_code", "next_check_at", "id")[:scan_limit]
    )

    for row in candidates:
        if marketplace_counts[row.marketplace_code] >= MAX_RECONCILIATION_ROWS_PER_MARKETPLACE:
            continue
        selected.append(row)
        marketplace_counts[row.marketplace_code] += 1
        if len(selected) >= min(MAX_RECONCILIATION_BATCH_SIZE, remaining_rows):
            break

    logger.info(
        "[scm_reconciliation] Selected %s due rows cutoff_utc=%s remaining_rows=%s",
        len(selected),
        cutoff,
        remaining_rows,
    )
    return selected


def _get_access_token(marketplace_id: str, company_name: str) -> str:
    creds = get_credentials_for_marketplace(marketplace_id, normalize_company_name(company_name))
    response = requests.post(
        "https://api.amazon.com/auth/o2/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": creds["refresh_token"],
            "client_id": creds["app_id"],
            "client_secret": creds["client_secret"],
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=API_REQUEST_TIMEOUT_SECONDS,
    )
    if response.status_code != 200:
        raise RuntimeError(f"Token refresh failed HTTP {response.status_code}: {response.text[:300]}")
    return response.json()["access_token"]


def _api_get_json(url: str, headers: Dict[str, str], operation: str, params: Optional[Dict] = None) -> Dict:
    for attempt in range(1, API_MAX_RETRIES + 1):
        response = requests.get(url, headers=headers, params=params, timeout=API_REQUEST_TIMEOUT_SECONDS)
        if response.status_code == 200:
            return response.json()

        if response.status_code in (429, 500, 503):
            retry_after = response.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                delay = int(retry_after)
            else:
                delay = min(300, 30 * (2 ** (attempt - 1)))
            logger.warning(
                "[scm_reconciliation] API retry operation=%s status=%s attempt=%s/%s delay=%ss",
                operation,
                response.status_code,
                attempt,
                API_MAX_RETRIES,
                delay,
            )
            if attempt < API_MAX_RETRIES:
                time.sleep(delay)
                continue

        raise RuntimeError(f"{operation} failed HTTP {response.status_code}: {response.text[:300]}")

    raise RuntimeError(f"{operation} failed after {API_MAX_RETRIES} attempts")


def _fetch_order(base_url: str, headers: Dict[str, str], order_id: str) -> Dict:
    logger.info("[scm_reconciliation] Fetching order status order_id=%s", order_id)
    data = _api_get_json(f"{base_url}/orders/v0/orders/{order_id}", headers, f"getOrder {order_id}")
    order = data.get("payload")
    if not order:
        raise RuntimeError(f"getOrder returned no payload for {order_id}")
    return order


def _fetch_order_items(base_url: str, headers: Dict[str, str], order_id: str) -> List[Dict]:
    logger.info("[scm_reconciliation] Fetching order items order_id=%s", order_id)
    all_items = []
    next_token = None
    while True:
        url = f"{base_url}/orders/v0/orders/{order_id}/orderItems"
        params = {"NextToken": next_token} if next_token else None
        data = _api_get_json(url, headers, f"getOrderItems {order_id}", params=params)
        payload = data.get("payload", {})
        all_items.extend(payload.get("OrderItems", []))
        next_token = payload.get("NextToken")
        if not next_token:
            break
    return all_items


def _clean_sql_value(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        value = value.to_pydatetime()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def _prepare_scm_orders_df(azure_df: pd.DataFrame) -> pd.DataFrame:
    if azure_df is None or azure_df.empty:
        return pd.DataFrame(columns=SCM_AMAZON_ORDERS_COLUMNS)

    available_cols = [col for col in SCM_AMAZON_ORDERS_COLUMNS if col in azure_df.columns]
    df = azure_df[available_cols].copy()

    if "CLEAN_DateTime" in df.columns:
        df["CLEAN_DateTime"] = pd.to_datetime(df["CLEAN_DateTime"], errors="coerce", utc=False)
        try:
            if hasattr(df["CLEAN_DateTime"].dtype, "tz") and df["CLEAN_DateTime"].dt.tz is not None:
                df["CLEAN_DateTime"] = df["CLEAN_DateTime"].dt.tz_convert("UTC").dt.tz_localize(None)
        except Exception:
            pass
        df["Date"] = pd.to_datetime(df["CLEAN_DateTime"].dt.date, errors="coerce")
    elif "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    for col in SCM_AMAZON_ORDERS_COLUMNS:
        if col not in df.columns:
            df[col] = None

    return df[SCM_AMAZON_ORDERS_COLUMNS].copy()


def _insert_scm_amazon_orders_if_not_exists(conn, azure_df: pd.DataFrame) -> Tuple[int, int]:
    scm_orders_df = _prepare_scm_orders_df(azure_df)
    if scm_orders_df.empty:
        return 0, 0

    insert_sql = text(
        """
        INSERT INTO scm_amazon_orders
            ([CLEAN_DateTime], [Date], [OrderId], [SKU], [Type], [Company], [Region],
             [Quantity], [FulfillmentChannel], [data_fetch_Date])
        VALUES
            (:CLEAN_DateTime, :Date, :OrderId, :SKU, :Type, :Company, :Region,
             :Quantity, :FulfillmentChannel, :data_fetch_Date)
        """
    )
    exists_sql = text(
        """
        SELECT TOP 1 1
        FROM scm_amazon_orders WITH (UPDLOCK, HOLDLOCK)
        WHERE OrderId = :OrderId
          AND SKU = :SKU
          AND Region = :Region
          AND Company = :Company
        """
    )

    inserted = 0
    duplicates = 0

    for _, row in scm_orders_df.iterrows():
        params = {col: _clean_sql_value(row.get(col)) for col in SCM_AMAZON_ORDERS_COLUMNS}
        exists = conn.execute(
            exists_sql,
            {
                "OrderId": params.get("OrderId"),
                "SKU": params.get("SKU"),
                "Region": params.get("Region"),
                "Company": params.get("Company"),
            },
        ).first()
        if exists:
            duplicates += 1
            logger.info(
                "[scm_reconciliation] Duplicate shipped order avoided OrderId=%s SKU=%s Region=%s Company=%s",
                params.get("OrderId"),
                params.get("SKU"),
                params.get("Region"),
                params.get("Company"),
            )
            continue

        conn.execute(insert_sql, params)
        inserted += 1
        logger.info(
            "[scm_reconciliation] Shipped order inserted OrderId=%s SKU=%s Region=%s Company=%s",
            params.get("OrderId"),
            params.get("SKU"),
            params.get("Region"),
            params.get("Company"),
        )

    return inserted, duplicates


def _update_source_order_status(conn, source_table: str, order_id: str, status: str) -> int:
    safe_table = f"[{source_table}]"
    result = conn.execute(
        text(f"UPDATE {safe_table} SET OrderStatus = :status WHERE AmazonOrderId = :order_id"),
        {"status": status, "order_id": order_id},
    )
    rowcount = result.rowcount or 0
    logger.info(
        "[scm_reconciliation] Source table status updated table=%s order_id=%s status=%s rows=%s",
        source_table,
        order_id,
        status,
        rowcount,
    )
    return rowcount


def _mark_rows_api_failed(rows: Iterable[SCMOrderReconciliationQueue], error: str, current_status: Optional[str] = None) -> None:
    now = timezone.now()
    next_check_at = now + timedelta(hours=1)
    row_ids = [row.id for row in rows]
    update_fields = {
        "next_check_at": next_check_at,
        "last_error": error[:2000],
        "updated_at": now,
    }
    if current_status:
        update_fields["current_status"] = current_status
    SCMOrderReconciliationQueue.objects.filter(id__in=row_ids, is_final=False).update(**update_fields)
    logger.warning("[scm_reconciliation] Rows rescheduled after API/db failure ids=%s error=%s", row_ids, error[:300])


def _mark_rows_final(rows: Iterable[SCMOrderReconciliationQueue], final_status: str, last_update_date: Optional[datetime]) -> None:
    now = timezone.now()
    row_ids = [row.id for row in rows]
    SCMOrderReconciliationQueue.objects.filter(id__in=row_ids, is_final=False).update(
        current_status=final_status,
        last_update_date=last_update_date,
        is_final=True,
        final_status=final_status,
        last_checked_at=now,
        last_error="",
        updated_at=now,
    )
    logger.info("[scm_reconciliation] Rows marked final ids=%s final_status=%s", row_ids, final_status)


def _reschedule_non_final_rows(
    rows: Iterable[SCMOrderReconciliationQueue],
    current_status: str,
    last_update_date: Optional[datetime],
) -> None:
    now = timezone.now()
    for row in rows:
        next_count = row.check_count + 1
        purchase_date = row.purchase_date
        next_check_at = calculate_next_check_at(purchase_date, now)
        SCMOrderReconciliationQueue.objects.filter(id=row.id, is_final=False).update(
            current_status=current_status,
            last_update_date=last_update_date,
            check_count=next_count,
            last_checked_at=now,
            next_check_at=next_check_at,
            last_error="",
            updated_at=now,
        )
        logger.info(
            "[scm_reconciliation] Row rescheduled id=%s order_id=%s status=%s check_count=%s next_check_at=%s",
            row.id,
            row.amazon_order_id,
            current_status,
            next_count,
            next_check_at,
        )


def _group_rows_by_order(rows: Iterable[SCMOrderReconciliationQueue]) -> Dict[Tuple[str, str, str, str], List[SCMOrderReconciliationQueue]]:
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row.company_name, row.marketplace_id, row.source_table, row.amazon_order_id)].append(row)
    return grouped


def _process_order_group(
    rows: List[SCMOrderReconciliationQueue],
    token_cache: Dict[Tuple[str, str], str],
) -> Dict:
    first = rows[0]
    marketplace_id = first.marketplace_id or MARKETPLACE_CODE_TO_ID.get(first.marketplace_code, "")
    source_table = _validated_source_table(marketplace_id, first.source_table)
    if source_table is None:
        _mark_rows_api_failed(rows, "invalid_source_table")
        return {"processed": len(rows), "errors": len(rows)}

    base_url = SP_API_BASE_URLS.get(marketplace_id)
    if not base_url:
        _mark_rows_api_failed(rows, f"unsupported_marketplace:{marketplace_id}")
        return {"processed": len(rows), "errors": len(rows)}

    token_key = (first.company_name, marketplace_id)
    if token_key not in token_cache:
        token_cache[token_key] = _get_access_token(marketplace_id, first.company_name)

    headers = {
        "x-amz-access-token": token_cache[token_key],
        "Content-Type": "application/json",
        "x-amz-date": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "User-Agent": "AmazonConnector/1.0",
    }

    try:
        order = _fetch_order(base_url, headers, first.amazon_order_id)
    except Exception as exc:
        _mark_rows_api_failed(rows, str(exc))
        return {"processed": len(rows), "errors": len(rows)}

    status = _normalize_status(order.get("OrderStatus"))
    last_update_date = _parse_datetime_value(order.get("LastUpdateDate"))
    logger.info(
        "[scm_reconciliation] Order status checked order_id=%s old_statuses=%s new_status=%s",
        first.amazon_order_id,
        sorted({row.current_status for row in rows}),
        status,
    )

    from .simple_db_save import create_mssql_connection

    if status == SHIPPED_STATUS:
        try:
            time.sleep(ORDER_ITEMS_LOOKUP_DELAY_SECONDS)
            items = _fetch_order_items(base_url, headers, first.amazon_order_id)
            for item in items:
                item["order_id"] = first.amazon_order_id

            marketplace_code = _marketplace_code_for_id(marketplace_id)
            _, azure_df = process_amazon_data([order], items, marketplace_code, first.company_name)

            engine = create_mssql_connection()
            with engine.begin() as conn:
                _update_source_order_status(conn, source_table, first.amazon_order_id, status)
                inserted, duplicates = _insert_scm_amazon_orders_if_not_exists(conn, azure_df)

            _mark_rows_final(rows, status, last_update_date)
            return {
                "processed": len(rows),
                "marked_final": len(rows),
                "shipped_inserted": inserted,
                "duplicates_avoided": duplicates,
            }
        except Exception as exc:
            logger.error(
                "[scm_reconciliation] Failed to reconcile shipped order order_id=%s error=%s",
                first.amazon_order_id,
                exc,
                exc_info=True,
            )
            _mark_rows_api_failed(rows, str(exc), current_status=status)
            return {"processed": len(rows), "errors": len(rows)}

    if status in FINAL_ORDER_STATUSES:
        try:
            engine = create_mssql_connection()
            with engine.begin() as conn:
                _update_source_order_status(conn, source_table, first.amazon_order_id, status)
            _mark_rows_final(rows, status, last_update_date)
            return {"processed": len(rows), "marked_final": len(rows)}
        except Exception as exc:
            logger.error(
                "[scm_reconciliation] Failed to mark final non-shipped order order_id=%s error=%s",
                first.amazon_order_id,
                exc,
                exc_info=True,
            )
            _mark_rows_api_failed(rows, str(exc), current_status=status)
            return {"processed": len(rows), "errors": len(rows)}

    try:
        engine = create_mssql_connection()
        with engine.begin() as conn:
            _update_source_order_status(conn, source_table, first.amazon_order_id, status)
    except Exception as exc:
        logger.warning(
            "[scm_reconciliation] Source status update failed for non-final order order_id=%s error=%s",
            first.amazon_order_id,
            exc,
        )
        _mark_rows_api_failed(rows, str(exc), current_status=status)
        return {"processed": len(rows), "errors": len(rows)}

    _reschedule_non_final_rows(rows, status, last_update_date)
    return {"processed": len(rows), "rescheduled": len(rows)}


def _process_reconciliation_batch(rows: List[SCMOrderReconciliationQueue]) -> Dict:
    logger.info("[scm_reconciliation] Processing reconciliation batch rows=%s", len(rows))
    summary = {
        "processed": 0,
        "marked_final": 0,
        "rescheduled": 0,
        "shipped_inserted": 0,
        "duplicates_avoided": 0,
        "errors": 0,
        "stopped_early": False,
    }
    token_cache = {}

    for _, grouped_rows in _group_rows_by_order(rows).items():
        should_exit, reason = _status_sync_should_exit()
        if should_exit:
            logger.warning("[scm_reconciliation] Stopping before next order group reason=%s", reason)
            summary["stopped_early"] = True
            break

        group_result = _process_order_group(grouped_rows, token_cache)
        for key in summary:
            if key in group_result and isinstance(summary[key], int):
                summary[key] += group_result[key]
        time.sleep(ORDER_LOOKUP_DELAY_SECONDS)

    logger.info("[scm_reconciliation] Batch finished summary=%s", summary)
    return summary


def run_scm_order_reconciliation() -> Dict:
    logger.info("[scm_reconciliation] Reconciliation started")
    started_at = timezone.now()
    deadline = time.monotonic() + (MAX_RECONCILIATION_RUNTIME_MINUTES * 60)
    marketplace_counts = defaultdict(int)
    summary = {
        "status": "completed",
        "processed": 0,
        "marked_final": 0,
        "rescheduled": 0,
        "shipped_inserted": 0,
        "duplicates_avoided": 0,
        "errors": 0,
        "batches": 0,
        "stop_reason": "",
        "started_at": started_at.isoformat(),
    }

    should_exit, reason = _status_sync_should_exit()
    if should_exit:
        summary["status"] = "skipped"
        summary["stop_reason"] = reason
        logger.info("[scm_reconciliation] Reconciliation skipped reason=%s", reason)
        return summary

    while summary["processed"] < MAX_RECONCILIATION_ROWS_PER_RUN:
        if time.monotonic() >= deadline:
            summary["status"] = "stopped"
            summary["stop_reason"] = "runtime_limit_reached"
            logger.warning("[scm_reconciliation] Runtime limit reached")
            break

        should_exit, reason = _status_sync_should_exit()
        if should_exit:
            summary["status"] = "stopped" if summary["processed"] else "skipped"
            summary["stop_reason"] = reason
            logger.warning("[scm_reconciliation] Reconciliation stopped before batch reason=%s", reason)
            break

        lock_result = acquire_job_lock(
            AMAZON_ORDERS_LOCK_NAME,
            locked_by=LOCK_OWNER_STATUS_SYNC,
            expires_minutes=STATUS_SYNC_LOCK_EXPIRES_MINUTES,
        )
        if not lock_result.acquired:
            summary["status"] = "skipped" if summary["processed"] == 0 else "stopped"
            summary["stop_reason"] = f"lock_busy:{lock_result.locked_by}"
            logger.info("[scm_reconciliation] Lock busy; reconciliation exiting owner=%s", lock_result.locked_by)
            break

        logger.info("[scm_reconciliation] Lock acquired for status_sync batch")
        try:
            should_exit, reason = _status_sync_should_exit()
            if should_exit:
                summary["status"] = "stopped" if summary["processed"] else "skipped"
                summary["stop_reason"] = reason
                logger.warning("[scm_reconciliation] Reconciliation stopped after lock acquisition reason=%s", reason)
                break

            remaining = MAX_RECONCILIATION_ROWS_PER_RUN - summary["processed"]
            rows = _get_due_queue_rows(remaining, marketplace_counts)
            if not rows:
                summary["stop_reason"] = "no_due_rows"
                logger.info("[scm_reconciliation] No due rows found")
                break

            logger.info(
                "[scm_reconciliation] Marketplace batch starting marketplaces=%s rows=%s",
                sorted({row.marketplace_code for row in rows}),
                len(rows),
            )
            batch_result = _process_reconciliation_batch(rows)
            summary["batches"] += 1
            for key in ("processed", "marked_final", "rescheduled", "shipped_inserted", "duplicates_avoided", "errors"):
                summary[key] += batch_result.get(key, 0)

            if batch_result.get("stopped_early"):
                summary["status"] = "stopped"
                summary["stop_reason"] = "stopped_early"
                break
        finally:
            release_job_lock(AMAZON_ORDERS_LOCK_NAME, locked_by=LOCK_OWNER_STATUS_SYNC)
            logger.info("[scm_reconciliation] Lock released for status_sync batch")

    summary["finished_at"] = timezone.now().isoformat()
    logger.info("[scm_reconciliation] Reconciliation finished summary=%s", summary)
    return summary
