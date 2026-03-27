# api/tasks.py
from datetime import timedelta, datetime, time
from celery import shared_task
from celery import group, chain
from celery.exceptions import Retry
from django.conf import settings
import logging
from .models import MarketplaceLastRun, SCMLastRun
from django.utils import timezone
import os, json
import requests
from django.db import transaction
from django.utils.dateparse import parse_datetime
from .marketplaces_creds import (
    CREDENTIALS,
    COMPANY_MARKETPLACE_CREDENTIAL_MAP,
    DEFAULT_COMPANY_NAME,
    MARKETPLACE_CREDENTIAL_MAP,
    get_credentials_for_marketplace,
    get_credential_group_for_marketplace,
    normalize_company_name,
)

logger = logging.getLogger(__name__)

def get_credentials(marketplace_id: str, company_name: str | None = None) -> dict:
    return get_credentials_for_marketplace(marketplace_id, normalize_company_name(company_name))


def get_access_token(marketplace_id: str, company_name: str | None = None) -> str:
    """
    Get access token dynamically for any marketplace.
    Credentials are fetched from MARKETPLACE_CREDENTIAL_MAP and CREDENTIALS.
    """
    resolved_company = normalize_company_name(company_name)
    try:
        credss = get_credentials(marketplace_id, resolved_company)
    except KeyError:
        raise ValueError(
            f"Unsupported marketplace/company credentials not found - Marketplace: {marketplace_id}, Company: {resolved_company}"
        )
    
    # Build payload dynamically from credentials 
    payload = {
        "appId": credss["app_id"],
        "clientSecret": credss["client_secret"],
        "refreshToken": credss["refresh_token"],
        "auto_save": True,
    }
    
    logger.info(f"[get_access_token] Requesting token for marketplace: {marketplace_id}, company: {resolved_company}")
    
    response = requests.post(
        "http://127.0.0.1:8000/api/connect/",
        json=payload,
        timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT),
    )
    
    if response.status_code < 200 or response.status_code >= 300:
        raise Exception(
            f"Failed to get access token for {marketplace_id}/{resolved_company}: HTTP {response.status_code} - {response.text}"
        )
    
    
    # Go one directory up to reach amazon_connector/
    base_dir = os.path.dirname(os.path.dirname(__file__))
    creds_path = os.path.join(base_dir, 'creds.json')

    with open(creds_path, 'r') as f:
        creds = json.load(f)
    return creds['access_token']

# def get_access_token():
#     """
#     Get access token 
#     """
#     # Go one directory up to reach amazon_connector/
#     base_dir = os.path.dirname(os.path.dirname(__file__))
#     creds_path = os.path.join(base_dir, 'creds.json')
    
#     with open(creds_path, 'r') as f:
#         creds = json.load(f)
#     return creds['access_token']

# Constants for controller loop
# Seed date: if last_run is None, we'll start fetching from the day AFTER this timestamp.
# Example flow expects first fetch to be 2023-11-02 when seed is 2023-11-01 23:59:59Z.
SEED_START_LAST_RUN = timezone.datetime(2023, 11, 1, 23, 59, 59, tzinfo=timezone.utc)
# Fixed inclusive end date per requirements
END_DATE = timezone.datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

# HTTP timeouts for calling the local Django endpoint
# Defaults: connect 5s, read 4h (adjust via env if fetch can be longer)
HTTP_CONNECT_TIMEOUT = int(os.getenv("FETCH_CONNECT_TIMEOUT", "5"))
HTTP_READ_TIMEOUT = int(os.getenv("FETCH_READ_TIMEOUT", "14400"))  # 4 hours
# Rate limiting configuration to avoid Amazon API throttling
# Delay between consecutive marketplace fetches (in seconds)
MARKETPLACE_FETCH_DELAY = int(os.getenv("MARKETPLACE_FETCH_DELAY", "120"))  # 90 seconds
# Delay for same credential group (marketplaces sharing credentials)
SAME_CREDENTIAL_GROUP_DELAY = int(os.getenv("SAME_CREDENTIAL_GROUP_DELAY", "60"))  # 1 minute 

def _ensure_aware_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if timezone.is_naive(dt):
        return timezone.make_aware(dt, timezone=timezone.utc)
    # Normalize to UTC if needed
    return dt.astimezone(timezone.utc)

def _parse_last_run(value) -> datetime | None:
    """
    Accepts a datetime or an ISO8601 string and returns an aware UTC datetime, or None.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return _ensure_aware_utc(value)
    if isinstance(value, str):
        # Normalize trailing 'Z' to '+00:00' for robust parsing
        s = value.strip()
        if s.endswith('Z'):
            s = s[:-1] + '+00:00'
        dt = parse_datetime(s)
        if dt is None:
            return None
        return _ensure_aware_utc(dt)
    return None

def _parse_iso_utc(value: str) -> datetime:
    """
    Parse an ISO8601 string that may end with 'Z' into an aware UTC datetime.
    """
    s = value.strip()
    if s.endswith('Z'):
        s = s[:-1] + '+00:00'
    dt = parse_datetime(s)
    return _ensure_aware_utc(dt)

def _day_window_after(last_run: datetime | None) -> tuple[datetime, datetime]:
    """
    Given last_run, compute the next day's [start, end] window in UTC.
    If last_run is None, use SEED_START_LAST_RUN and return the following day's window.
    Returns (start_dt, end_dt) both aware UTC datetimes.
    """
    lr = _parse_last_run(last_run) or SEED_START_LAST_RUN
    # Next day
    next_day_date = (lr.astimezone(timezone.utc).date() + timedelta(days=1))
    start_dt = timezone.datetime.combine(next_day_date, time(0, 0, 0, tzinfo=timezone.utc))
    end_dt = timezone.datetime.combine(next_day_date, time(23, 59, 59, tzinfo=timezone.utc))
    return start_dt, end_dt

def _within_end_date(start_dt: datetime) -> bool:
    """
    Return True if the day for start_dt is on/before END_DATE's date.
    """
    start_day = start_dt.astimezone(timezone.utc).date()
    end_day = END_DATE.astimezone(timezone.utc).date()
    return start_day <= end_day



@shared_task(bind=True, queue='fetching', soft_time_limit=21600, time_limit=21900)
def fetch_orders_for_marketplace(self, marketplace_id: str, start_iso: str, end_iso: str, company_name: str = DEFAULT_COMPANY_NAME):
    """
    Fetch orders for a single marketplace for a single day window [start, end].
    On success, update that marketplace's last_run to the day's end time.
    """
    try:
        resolved_company = normalize_company_name(company_name)
        access_token = get_access_token(marketplace_id, resolved_company)
        print("Cheking access token:", access_token)
        logger.info(
            f"[fetch_orders_for_marketplace] Obtained access token for {marketplace_id}/{resolved_company}, access_token={access_token}"
        )

        # Idempotency and sequence guard: only process if this window equals the next required day
        start_dt = _parse_iso_utc(start_iso)
        end_dt_req = _parse_iso_utc(end_iso)
        with transaction.atomic():
            obj = MarketplaceLastRun.objects.select_for_update().get(
                marketplace_id=marketplace_id,
                company_name=resolved_company,
            )
            expected_start_dt, expected_end_dt = _day_window_after(obj.last_run)
            if start_dt != expected_start_dt:
                logger.info(
                    f"[fetch_orders_for_marketplace] Skip {marketplace_id}/{resolved_company}: requested {start_dt.date()} != expected {expected_start_dt.date()} (idempotent guard)"
                )
                return {
                    "marketplace_id": marketplace_id,
                    "company_name": resolved_company,
                    "status": "skipped",
                    "requested": start_iso,
                    "expected": expected_start_dt.isoformat().replace("+00:00", "Z"),
                }

        payload = {
            "access_token": access_token,
            "marketplace_id": marketplace_id,
            "company_name": resolved_company,
            "start_date": start_iso,
            "end_date": end_iso,
            "auto_save": True,
        }

        logger.info(f"[fetch_orders_for_marketplace] {marketplace_id}/{resolved_company} -> {start_iso} to {end_iso}")
        response = requests.post(
            "http://127.0.0.1:8000/api/fetch-data/",
            json=payload,
            timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT),
        )

        if 200 <= response.status_code < 300:
            # Persist last_run as end of the day, only if still expected (avoid racing duplicates)
            end_dt = _parse_iso_utc(end_iso)
            with transaction.atomic():
                obj = MarketplaceLastRun.objects.select_for_update().get(
                    marketplace_id=marketplace_id,
                    company_name=resolved_company,
                )
                curr_expected_start, _ = _day_window_after(obj.last_run)
                if curr_expected_start != start_dt:
                    logger.info(
                        f"[fetch_orders_for_marketplace] Not updating last_run for {marketplace_id}/{resolved_company}: window already advanced elsewhere"
                    )
                else:
                    obj.last_run = end_dt
                    obj.save(update_fields=["last_run"])
            logger.info(f"[fetch_orders_for_marketplace] Updated last_run for {marketplace_id}/{resolved_company} -> {end_iso}")
            return {
                "marketplace_id": marketplace_id,
                "company_name": resolved_company,
                "status": "ok",
                "fetched": [start_iso, end_iso],
            }
        else:
            logger.warning(
                f"[fetch_orders_for_marketplace] Failed for {marketplace_id}/{resolved_company} ({response.status_code}): {response.text[:500]}"
            )
            raise Exception(f"HTTP {response.status_code}")

    except requests.exceptions.ConnectionError as exc:
        # Connection errors often indicate rate limiting - use exponential backoff
        retry_count = self.request.retries
        backoff_delay = min(300, 60 * (2 ** retry_count))  # Exponential backoff, max 5 minutes
        logger.warning(
            f"[fetch_orders_for_marketplace] Connection error for {marketplace_id}/{resolved_company} (attempt {retry_count + 1}/5). "
            f"Likely rate limited. Retrying in {backoff_delay}s"
        )
        raise self.retry(exc=exc, countdown=backoff_delay, max_retries=5)
    except requests.exceptions.Timeout as exc:
        logger.error(f"[fetch_orders_for_marketplace] Timeout for {marketplace_id}/{resolved_company}: {exc}")
        raise self.retry(exc=exc, countdown=60, max_retries=3)
    except Exception as exc:
        error_str = str(exc).lower()
        # Check if error indicates rate limiting
        if any(keyword in error_str for keyword in ['rate limit', 'throttl', 'quota', 'too many requests']):
            retry_count = self.request.retries
            backoff_delay = min(600, 120 * (2 ** retry_count))  # Longer backoff for explicit rate limits
            logger.warning(
                f"[fetch_orders_for_marketplace] Rate limit detected for {marketplace_id}/{resolved_company} (attempt {retry_count + 1}/5). "
                f"Retrying in {backoff_delay}s"
            )
            raise self.retry(exc=exc, countdown=backoff_delay, max_retries=5)
        
        logger.error(f"[fetch_orders_for_marketplace] Error for {marketplace_id}/{resolved_company}: {exc}")
        raise self.retry(exc=exc, countdown=30, max_retries=5)



@shared_task(bind=True, queue='fetching')
def process_marketplaces(self):
    """
    Controller task with rate limiting:
    - Reads all marketplaces and their last_run
    - Schedules per-marketplace day fetch tasks (one day per marketplace)
    - Implements rate limiting to avoid Amazon API throttling
    - Chains itself with appropriate delays based on credential groups
    - Stops when all marketplaces have reached END_DATE
    """
    try:
        # Pull all marketplaces; we'll choose the one whose next-day window is earliest
        # (round-robin by day across marketplaces). Ties are broken deterministically by marketplace_id.
        records = list(MarketplaceLastRun.objects.all())
        if not records:
            logger.info("[process_marketplaces] No marketplaces found. Re-queuing in 60s.")
            self.apply_async(countdown=60)
            return {"status": "empty"}

        # Pick exactly ONE marketplace for this iteration (strict one-at-a-time processing)
        candidates = []  # (start_dt, company_name, marketplace_id, rec, end_dt, credential_group)
        for rec in records:
            start_dt, end_dt = _day_window_after(rec.last_run)
            eligible = _within_end_date(start_dt)
            
            # Get credential group for rate limiting awareness
            try:
                cred_group = get_credential_group_for_marketplace(rec.marketplace_id, rec.company_name)
            except:
                cred_group = "unknown"
            
            logger.info(
                f"[process_marketplaces] Candidate {rec.company_name}/{rec.marketplace_id}: last_run={rec.last_run} -> next_day={start_dt.date()} eligible={eligible} cred_group={cred_group}"
            )
            if eligible:
                candidates.append((start_dt, rec.company_name, rec.marketplace_id, rec, end_dt, cred_group))

        if not candidates:
            logger.info("[process_marketplaces] All marketplaces have reached END_DATE. Controller will stop.")
            return {"status": "completed"}

        # Choose the company/marketplace whose next day is earliest.
        # Ties are broken deterministically by company_name, then marketplace_id.
        candidates.sort(key=lambda x: (x[0], x[1], x[2]))
        start_dt, company_name, _mid, next_rec, end_dt, cred_group = candidates[0]
        
        logger.info(
            f"[process_marketplaces] Chosen next: {company_name}/{next_rec.marketplace_id} for day {start_dt.date()} (earliest among {len(candidates)} candidates) - credential_group={cred_group}"
        )
        start_iso = start_dt.isoformat().replace("+00:00", "Z")
        end_iso = end_dt.isoformat().replace("+00:00", "Z")

        logger.info(
            f"[process_marketplaces] Scheduling single task for {company_name}/{next_rec.marketplace_id}: {start_iso} -> {end_iso}"
        )

        # Determine delay based on rate limiting strategy
        # If next marketplace in queue uses same credential group, use longer delay
        next_delay = MARKETPLACE_FETCH_DELAY  # Default delay
        
        if len(candidates) > 1:
            # Check if next candidate uses same credential group
            next_candidate_cred_group = candidates[1][5]
            if next_candidate_cred_group == cred_group:
                next_delay = SAME_CREDENTIAL_GROUP_DELAY
                logger.info(
                    f"[process_marketplaces] Next marketplace shares credential group '{cred_group}', using extended delay: {next_delay}s"
                )
            else:
                logger.info(
                    f"[process_marketplaces] Next marketplace uses different credential group, using standard delay: {next_delay}s"
                )
        
        # Chain a single marketplace-day fetch, then re-queue the controller with calculated delay
        ch = chain(
            fetch_orders_for_marketplace.si(next_rec.marketplace_id, start_iso, end_iso, company_name),
            process_marketplaces.si().set(countdown=next_delay),
        )
        ch.apply_async()

        return {
            "status": "dispatched-one", 
            "company_name": company_name,
            "marketplace_id": next_rec.marketplace_id, 
            "date": start_iso[:10],
            "next_delay": next_delay,
            "credential_group": cred_group
        }

    except Exception as exc:
        logger.error(f"[process_marketplaces] Controller error: {exc}")
        # Back off a bit, then try again
        raise self.retry(exc=exc, countdown=120, max_retries=10)

def _fixed_ranges_config() -> dict[str, list[tuple[str, str]]]:
    pass
    #USA: ATVPDKIKX

# ============================================================================
# MISSING ORDERS FETCH FOR USA MARKETPLACE
# ============================================================================
# USA Marketplace ID
# USA_MARKETPLACE_ID = "ATVPDKIKX0DER"

# Missing date ranges for USA marketplace (start_date, end_date inclusive)
# USA_MISSING_DATE_RANGES = [
#     ("2024-11-27", "2024-11-29"),
#     ("2025-02-21", "2025-02-23"),
#     ("2025-08-15", "2025-08-17"),
# ]


# ============================================================================
# MISSING ORDERS FETCH FOR IT MARKETPLACE
# ============================================================================

# IT Marketplace ID
CA_MARKETPLACE_ID = "A2EUQ1WTGCTBG2"
MARKETPLACE_NAME = "CA"

# Missing date ranges for IT marketplace (start_date, end_date inclusive)
CA_MISSING_DATE_RANGES = [
    ("2024-11-28", "2024-11-30"),
    ("2025-02-21", "2025-02-23"),
    ("2025-02-24", "2025-02-24"),
]


# Path to track progress of missing orders fetch
def _get_usa_missing_orders_tracking_path() -> str:
    """Get path to the tracking file for {MARKETPLACE_NAME} missing orders."""
    base_dir = os.path.dirname(os.path.dirname(__file__))
    return os.path.join(base_dir, f'{MARKETPLACE_NAME}_missing_orders_progress.json')

def _expand_date_ranges_to_days(date_ranges: list[tuple[str, str]]) -> list[str]:
    """
    Expand date ranges to individual days.
    Returns a list of date strings in 'YYYY-MM-DD' format.
    """
    all_days = []
    for start_str, end_str in date_ranges:
        start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
        current = start_date
        while current <= end_date:
            all_days.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
    return all_days

def _load_usa_missing_orders_progress() -> dict:
    """
    Load progress tracking for {MARKETPLACE_NAME} missing orders.
    Returns dict with 'completed_days' list and 'last_processed' info.
    """
    tracking_path = _get_usa_missing_orders_tracking_path()
    if os.path.exists(tracking_path):
        try:
            with open(tracking_path, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {"completed_days": [], "last_processed": None, "started_at": None}

def _save_usa_missing_orders_progress(progress: dict) -> None:
    """Save progress tracking for {MARKETPLACE_NAME} missing orders."""
    tracking_path = _get_usa_missing_orders_tracking_path()
    with open(tracking_path, 'w') as f:
        json.dump(progress, f, indent=2)

def _get_next_usa_missing_day() -> str | None:
    """
    Get the next day to fetch for {MARKETPLACE_NAME} missing orders.
    Returns date string 'YYYY-MM-DD' or None if all days are completed.
    """
    all_days = _expand_date_ranges_to_days(CA_MISSING_DATE_RANGES)
    progress = _load_usa_missing_orders_progress()
    completed = set(progress.get("completed_days", []))
    
    for day in all_days:
        if day not in completed:
            return day
    return None

def _mark_usa_day_completed(day: str) -> None:
    """Mark a day as completed in the tracking file."""
    progress = _load_usa_missing_orders_progress()
    if day not in progress["completed_days"]:
        progress["completed_days"].append(day)
    progress["last_processed"] = day
    progress["last_processed_at"] = timezone.now().isoformat()
    _save_usa_missing_orders_progress(progress)


@shared_task(bind=True, queue='fetching', soft_time_limit=21600, time_limit=21900)
def fetch_missing_orders_usa_day(self, day_str: str):
    """
    Fetch orders for USA marketplace for a single day.
    Similar to fetch_orders_for_marketplace but for missing orders recovery.
    
    Args:
        day_str: Date string in 'YYYY-MM-DD' format
    """
    marketplace_id = CA_MARKETPLACE_ID
    
    try:
        access_token = get_access_token(marketplace_id)
        logger.info(f"[fetch_missing_orders_{MARKETPLACE_NAME}_day] Obtained access token for {MARKETPLACE_NAME}, day={day_str}")

        # Build ISO timestamps for the day
        start_iso = f"{day_str}T00:00:00Z"
        end_iso = f"{day_str}T23:59:59Z"

        # Check if this day was already completed (idempotency guard)
        progress = _load_usa_missing_orders_progress()
        if day_str in progress.get("completed_days", []):
            logger.info(f"[fetch_missing_orders_{MARKETPLACE_NAME}_day] Day {day_str} already completed, skipping")
            return {
                "marketplace_id": marketplace_id,
                "status": "skipped",
                "day": day_str,
                "reason": "already_completed"
            }

        payload = {
            "access_token": access_token,
            "marketplace_id": marketplace_id,
            "start_date": start_iso,
            "end_date": end_iso,
            "auto_save": True,
        }

        logger.info(f"[fetch_missing_orders_{MARKETPLACE_NAME}_day] {MARKETPLACE_NAME} -> {start_iso} to {end_iso}")
        response = requests.post(
            "http://127.0.0.1:8000/api/fetch-data/",
            json=payload,
            timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT),
        )

        if 200 <= response.status_code < 300:
            # Mark this day as completed
            _mark_usa_day_completed(day_str)
            logger.info(f"[fetch_missing_orders_{MARKETPLACE_NAME}_day] Completed day {day_str} for {MARKETPLACE_NAME}")
            return {"marketplace_id": marketplace_id, "status": "ok", "day": day_str}
        else:
            logger.warning(
                f"[fetch_missing_orders_{MARKETPLACE_NAME}_day] Failed for {MARKETPLACE_NAME} day {day_str} ({response.status_code}): {response.text[:500]}"
            )
            raise Exception(f"HTTP {response.status_code}")

    except requests.exceptions.ConnectionError as exc:
        retry_count = self.request.retries
        backoff_delay = min(300, 60 * (2 ** retry_count))
        logger.warning(
            f"[fetch_missing_orders_{MARKETPLACE_NAME}_day] Connection error for {MARKETPLACE_NAME} day {day_str} (attempt {retry_count + 1}/5). "
            f"Retrying in {backoff_delay}s"
        )
        raise self.retry(exc=exc, countdown=backoff_delay, max_retries=5)
    except requests.exceptions.Timeout as exc:
        logger.error(f"[fetch_missing_orders_{MARKETPLACE_NAME}_day] Timeout for {MARKETPLACE_NAME} day {day_str}: {exc}")
        raise self.retry(exc=exc, countdown=60, max_retries=3)
    except Exception as exc:
        error_str = str(exc).lower()
        if any(keyword in error_str for keyword in ['rate limit', 'throttl', 'quota', 'too many requests']):
            retry_count = self.request.retries
            backoff_delay = min(600, 120 * (2 ** retry_count))
            logger.warning(
                f"[fetch_missing_orders_{MARKETPLACE_NAME}_day] Rate limit detected for {MARKETPLACE_NAME} day {day_str} (attempt {retry_count + 1}/5). "
                f"Retrying in {backoff_delay}s"
            )
            raise self.retry(exc=exc, countdown=backoff_delay, max_retries=5)
        
        logger.error(f"[fetch_missing_orders_{MARKETPLACE_NAME}_day] Error for {MARKETPLACE_NAME} day {day_str}: {exc}")
        raise self.retry(exc=exc, countdown=30, max_retries=5)


@shared_task(bind=True, queue='fetching')
def process_missing_orders_usa(self):
    """
    Controller task for fetching missing {MARKETPLACE_NAME} orders.
    - Reads all pending days from the configured date ranges
    - Fetches one day at a time with rate limiting
    - Tracks progress in a JSON file
    - Chains itself until all days are completed
    """
    try:
        # Initialize progress file if this is the first run
        progress = _load_usa_missing_orders_progress()
        if progress.get("started_at") is None:
            progress["started_at"] = timezone.now().isoformat()
            _save_usa_missing_orders_progress(progress)

        # Get next day to fetch
        next_day = _get_next_usa_missing_day()
        
        if next_day is None:
            logger.info("[process_missing_orders_{MARKETPLACE_NAME}] All missing days have been fetched for {MARKETPLACE_NAME}!")
            progress = _load_usa_missing_orders_progress()
            progress["completed_at"] = timezone.now().isoformat()
            _save_usa_missing_orders_progress(progress)
            return {"status": "completed", "message": "All {MARKETPLACE_NAME} missing orders fetched"}

        # Calculate progress stats
        all_days = _expand_date_ranges_to_days(CA_MISSING_DATE_RANGES)
        completed_count = len(progress.get("completed_days", []))
        total_count = len(all_days)
        
        logger.info(
            f"[process_missing_orders_{MARKETPLACE_NAME}] Scheduling fetch for day {next_day} "
            f"(progress: {completed_count}/{total_count} days completed)"
        )

        # Chain: fetch one day, then re-queue controller with delay
        ch = chain(
            fetch_missing_orders_usa_day.si(next_day),
            process_missing_orders_usa.si().set(countdown=MARKETPLACE_FETCH_DELAY),
        )
        ch.apply_async()

        return {
            "status": "dispatched",
            "day": next_day,
            "progress": f"{completed_count}/{total_count}",
            "next_delay": MARKETPLACE_FETCH_DELAY
        }

    except Exception as exc:
        logger.error(f"[process_missing_orders_{MARKETPLACE_NAME}] Controller error: {exc}")
        raise self.retry(exc=exc, countdown=120, max_retries=10)


def get_usa_missing_orders_status() -> dict:
    """
    Utility function to get current status of {MARKETPLACE_NAME} missing orders fetch.
    Can be called from views or management commands.
    """
    progress = _load_usa_missing_orders_progress()
    all_days = _expand_date_ranges_to_days(CA_MISSING_DATE_RANGES)
    completed = progress.get("completed_days", [])
    pending = [d for d in all_days if d not in completed]
    
    return {
        "total_days": len(all_days),
        "completed_days": len(completed),
        "pending_days": len(pending),
        "next_pending_day": pending[0] if pending else None,
        "completed_list": completed,
        "pending_list": pending,
        "started_at": progress.get("started_at"),
        "last_processed": progress.get("last_processed"),
        "last_processed_at": progress.get("last_processed_at"),
        "completed_at": progress.get("completed_at"),
    }


def reset_usa_missing_orders_progress() -> dict:
    """
    Reset the {MARKETPLACE_NAME} missing orders progress tracking.
    Use this to restart the fetch from the beginning.
    """
    tracking_path = _get_usa_missing_orders_tracking_path()
    if os.path.exists(tracking_path):
        os.remove(tracking_path)
    return {"status": "reset", "message": "{MARKETPLACE_NAME} missing orders progress has been reset"}


@shared_task(bind=True, queue='reports')
def generate_reports(self):
    """
    Asynchronous task to generate reports
    """
    try:
        print("Generating reports...")
        marketplaces = ['IT']
        payload = {
            "marketplaces": marketplaces,
        }
        response = requests.post("http://127.0.0.1:8000/api/inventory/reports/", json=payload)
        logger.info(f"API Response ({response.status_code})")

        
    except Exception as exc:
        logger.error(f"Report generation task failed: {exc}")
        raise self.retry(exc=exc, countdown=60, max_retries=3)

@shared_task(bind=True, queue='fetching_scm')
def fetch_scm_data(self):
    """
    Fetching task for SCM data - DEPRECATED, use process_scm_marketplaces instead
    """
    logger.warning("fetch_scm_data is deprecated. Use process_scm_marketplaces instead.")
    return {"status": "deprecated", "message": "Use process_scm_marketplaces task"}


# ============================================================================
# SCM DATA FETCH - PER DAY PER MARKETPLACE
# ============================================================================

# SCM Seed date: Start fetching from Jan 1, 2026
SCM_SEED_START_LAST_RUN = timezone.datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc)


def _scm_day_window_after(last_run: datetime | None) -> tuple[datetime, datetime]:
    """
    Given last_run, compute the next day's [start, end] window in UTC for SCM.
    If last_run is None, use SCM_SEED_START_LAST_RUN and return Jan 1, 2026.
    Returns (start_dt, end_dt) both aware UTC datetimes.
    """
    lr = _parse_last_run(last_run) or SCM_SEED_START_LAST_RUN
    # Next day
    next_day_date = (lr.astimezone(timezone.utc).date() + timedelta(days=1))
    start_dt = timezone.datetime.combine(next_day_date, time(0, 0, 0, tzinfo=timezone.utc))
    end_dt = timezone.datetime.combine(next_day_date, time(23, 59, 59, tzinfo=timezone.utc))
    return start_dt, end_dt


def _scm_within_end_date(start_dt: datetime) -> bool:
    """
    Return True if the day for start_dt is on/before yesterday (current date - 1).
    """
    start_day = start_dt.astimezone(timezone.utc).date()
    # End date is yesterday (current date - 1)
    yesterday = (timezone.now().astimezone(timezone.utc).date() - timedelta(days=1))
    return start_day <= yesterday


@shared_task(bind=True, queue='fetching_scm', soft_time_limit=21600, time_limit=21900)
def fetch_scm_for_marketplace(self, marketplace_id: str, start_iso: str, end_iso: str, company_name: str = DEFAULT_COMPANY_NAME):
    """
    Fetch SCM data for a single marketplace for a single day window [start, end].
    On success, update that marketplace's last_run in SCMLastRun table.
    """
    try:
        resolved_company = normalize_company_name(company_name)
        access_token = get_access_token(marketplace_id, resolved_company)
        logger.info(f"[fetch_scm_for_marketplace] Obtained access token for {resolved_company}/{marketplace_id}")

        # Idempotency and sequence guard: only process if this window equals the next required day
        start_dt = _parse_iso_utc(start_iso)
        end_dt_req = _parse_iso_utc(end_iso)
        
        with transaction.atomic():
            obj = SCMLastRun.objects.select_for_update().get(
                marketplace_id=marketplace_id,
                company_name=resolved_company,
            )
            expected_start_dt, expected_end_dt = _scm_day_window_after(obj.last_run)
            if start_dt != expected_start_dt:
                logger.info(
                    f"[fetch_scm_for_marketplace] Skip {resolved_company}/{marketplace_id}: requested {start_dt.date()} != expected {expected_start_dt.date()} (idempotent guard)"
                )
                return {
                    "marketplace_id": marketplace_id,
                    "company_name": resolved_company,
                    "status": "skipped",
                    "requested": start_iso,
                    "expected": expected_start_dt.isoformat().replace("+00:00", "Z"),
                }

        payload = {
            "access_token": access_token,
            "marketplace_id": marketplace_id,
            "company_name": resolved_company,
            "start_date": start_iso,
            "end_date": end_iso,
            "auto_save": True,
            "data_type": "scm_data",  # This triggers SCM processing
        }

        logger.info(f"[fetch_scm_for_marketplace] {resolved_company}/{marketplace_id} -> {start_iso} to {end_iso}")
        response = requests.post(
            "http://127.0.0.1:8000/api/fetch-data/",
            json=payload,
            timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT),
        )

        if 200 <= response.status_code < 300:
            # Parse response to check results
            response_data = response.json()
            
            # Check how many orders were fetched from Amazon
            metadata = response_data.get('data', {}).get('metadata', {})
            total_orders_fetched = metadata.get('total_orders_fetched', 0)
            
            # Check database save info
            db_save_info = response_data.get('data', {}).get('processed_data', {}).get('database_save', {})
            db_save_success = db_save_info.get('success', False)
            records_saved = db_save_info.get('records_saved', 0)
            
            # Case 1: No orders fetched from Amazon (valid - no new orders for this day)
            # Update last_run and move on
            if total_orders_fetched == 0:
                logger.info(
                    f"[fetch_scm_for_marketplace] No orders for {marketplace_id} on this day. Marking as complete."
                )
                end_dt = _parse_iso_utc(end_iso)
                with transaction.atomic():
                    obj = SCMLastRun.objects.select_for_update().get(
                        marketplace_id=marketplace_id,
                        company_name=resolved_company,
                    )
                    curr_expected_start, _ = _scm_day_window_after(obj.last_run)
                    if curr_expected_start == start_dt:
                        obj.last_run = end_dt
                        obj.save(update_fields=["last_run", "updated_at"])
                        logger.info(f"[fetch_scm_for_marketplace] Updated SCM last_run for {resolved_company}/{marketplace_id} -> {end_iso} (no orders)")
                return {
                    "marketplace_id": marketplace_id, 
                    "company_name": resolved_company,
                    "status": "ok_no_orders", 
                    "fetched": [start_iso, end_iso], 
                    "data_type": "scm_data",
                    "records_saved": 0,
                    "total_orders_fetched": 0
                }
            
            # Case 2: Orders were fetched but DB save failed - don't update last_run
            if not db_save_success and records_saved == 0:
                logger.warning(
                    f"[fetch_scm_for_marketplace] Data fetch succeeded but DB save failed for {resolved_company}/{marketplace_id}. "
                    f"db_save_success={db_save_success}, records_saved={records_saved}. Moving to next marketplace."
                )
                return {
                    "marketplace_id": marketplace_id, 
                    "company_name": resolved_company,
                    "status": "db_save_failed", 
                    "fetched": [start_iso, end_iso], 
                    "data_type": "scm_data",
                    "records_saved": records_saved
                }
            
            # Case 3: Orders fetched and saved successfully - update last_run
            end_dt = _parse_iso_utc(end_iso)
            with transaction.atomic():
                obj = SCMLastRun.objects.select_for_update().get(
                    marketplace_id=marketplace_id,
                    company_name=resolved_company,
                )
                curr_expected_start, _ = _scm_day_window_after(obj.last_run)
                if curr_expected_start != start_dt:
                    logger.info(
                        f"[fetch_scm_for_marketplace] Not updating last_run for {resolved_company}/{marketplace_id}: window already advanced elsewhere"
                    )
                else:
                    obj.last_run = end_dt
                    obj.save(update_fields=["last_run", "updated_at"])
            logger.info(f"[fetch_scm_for_marketplace] Updated SCM last_run for {resolved_company}/{marketplace_id} -> {end_iso} ({records_saved} records saved)")
            return {
                "marketplace_id": marketplace_id,
                "company_name": resolved_company,
                "status": "ok",
                "fetched": [start_iso, end_iso],
                "data_type": "scm_data",
                "records_saved": records_saved,
            }
        else:
            response_text = response.text[:500]
            logger.warning(
                f"[fetch_scm_for_marketplace] Failed for {resolved_company}/{marketplace_id} ({response.status_code}): {response_text}"
            )
            
            # Check for Amazon's "2 minutes delay" error - retry after 1 hour
            if response.status_code == 400 and '2 minutes' in response_text.lower():
                logger.warning(
                    f"[fetch_scm_for_marketplace] Amazon data delay error for {marketplace_id}. "
                    f"Company: {resolved_company}. "
                    f"Data not yet available. Retrying in 1 hour."
                )
                raise self.retry(exc=Exception(f"Amazon data delay - HTTP 400"), countdown=3600, max_retries=3)
            
            raise Exception(f"HTTP {response.status_code}")

    except requests.exceptions.ConnectionError as exc:
        retry_count = self.request.retries
        backoff_delay = min(300, 60 * (2 ** retry_count))
        logger.warning(
            f"[fetch_scm_for_marketplace] Connection error for {resolved_company}/{marketplace_id} (attempt {retry_count + 1}/5). "
            f"Retrying in {backoff_delay}s"
        )
        raise self.retry(exc=exc, countdown=backoff_delay, max_retries=5)
    except requests.exceptions.Timeout as exc:
        logger.error(f"[fetch_scm_for_marketplace] Timeout for {resolved_company}/{marketplace_id}: {exc}")
        raise self.retry(exc=exc, countdown=60, max_retries=3)
    except Retry:
        # Re-raise Retry exceptions without catching them in the generic handler
        raise
    except Exception as exc:
        error_str = str(exc).lower()
        if any(keyword in error_str for keyword in ['rate limit', 'throttl', 'quota', 'too many requests']):
            retry_count = self.request.retries
            backoff_delay = min(600, 120 * (2 ** retry_count))
            logger.warning(
                f"[fetch_scm_for_marketplace] Rate limit detected for {resolved_company}/{marketplace_id} (attempt {retry_count + 1}/5). "
                f"Retrying in {backoff_delay}s"
            )
            raise self.retry(exc=exc, countdown=backoff_delay, max_retries=5)
        
        logger.error(f"[fetch_scm_for_marketplace] Error for {resolved_company}/{marketplace_id}: {exc}")
        raise self.retry(exc=exc, countdown=30, max_retries=5)


@shared_task(bind=True, queue='fetching_scm')
def process_scm_marketplaces(self):
    """
    Controller task for SCM data fetching with rate limiting:
    - Reads all marketplaces and their SCM last_run
    - Schedules per-marketplace day fetch tasks (one day per marketplace)
    - Implements rate limiting to avoid Amazon API throttling
    - Chains itself with appropriate delays based on credential groups
    - Stops when all marketplaces have reached yesterday's date
    
    SCM Date Range:
    - Start: January 1, 2026
    - End: Current date - 1 (yesterday)
    """
    try:
        records = list(SCMLastRun.objects.all())
        if not records:
            logger.info("[process_scm_marketplaces] No SCM marketplaces found. Re-queuing in 60s.")
            self.apply_async(countdown=60)
            return {"status": "empty"}

        # Pick exactly ONE marketplace for this iteration (strict one-at-a-time processing)
        candidates = []  # (start_dt, company_name, marketplace_id, rec, end_dt, credential_group)
        for rec in records:
            start_dt, end_dt = _scm_day_window_after(rec.last_run)
            eligible = _scm_within_end_date(start_dt)
            
            # Get credential group for rate limiting awareness
            try:
                cred_group = get_credential_group_for_marketplace(rec.marketplace_id, rec.company_name)
            except:
                cred_group = "unknown"
            
            logger.info(
                f"[process_scm_marketplaces] Candidate {rec.company_name}/{rec.marketplace_id}: last_run={rec.last_run} -> next_day={start_dt.date()} eligible={eligible} cred_group={cred_group}"
            )
            if eligible:
                candidates.append((start_dt, rec.company_name, rec.marketplace_id, rec, end_dt, cred_group))

        if not candidates:
            # All marketplaces have caught up to yesterday.
            # Re-queue to check again later (when a new day becomes available).
            # Check every 4 hours to see if we've crossed into a new day.
            recheck_delay = 4 * 60 * 60  # 4 hours in seconds
            logger.info(
                f"[process_scm_marketplaces] All marketplaces have reached yesterday. "
                f"Re-queueing to check again in {recheck_delay // 3600} hours."
            )
            self.apply_async(countdown=recheck_delay)
            return {"status": "waiting", "message": "All SCM marketplaces caught up to yesterday. Will recheck later."}

        # Choose the company/marketplace whose next day is earliest.
        candidates.sort(key=lambda x: (x[0], x[1], x[2]))
        start_dt, company_name, _mid, next_rec, end_dt, cred_group = candidates[0]
        
        logger.info(
            f"[process_scm_marketplaces] Chosen next: {company_name}/{next_rec.marketplace_id} for day {start_dt.date()} (earliest among {len(candidates)} candidates) - credential_group={cred_group}"
        )
        start_iso = start_dt.isoformat().replace("+00:00", "Z")
        end_iso = end_dt.isoformat().replace("+00:00", "Z")

        logger.info(
            f"[process_scm_marketplaces] Scheduling SCM task for {company_name}/{next_rec.marketplace_id}: {start_iso} -> {end_iso}"
        )

        # Determine delay based on rate limiting strategy
        next_delay = MARKETPLACE_FETCH_DELAY  # Default delay
        
        if len(candidates) > 1:
            next_candidate_cred_group = candidates[1][5]
            if next_candidate_cred_group == cred_group:
                next_delay = SAME_CREDENTIAL_GROUP_DELAY
                logger.info(
                    f"[process_scm_marketplaces] Next marketplace shares credential group '{cred_group}', using extended delay: {next_delay}s"
                )
            else:
                logger.info(
                    f"[process_scm_marketplaces] Next marketplace uses different credential group, using standard delay: {next_delay}s"
                )
        
        # Chain a single marketplace-day fetch, then re-queue the controller with calculated delay
        ch = chain(
            fetch_scm_for_marketplace.si(next_rec.marketplace_id, start_iso, end_iso, company_name),
            process_scm_marketplaces.si().set(countdown=next_delay),
        )
        ch.apply_async()

        return {
            "status": "dispatched-one", 
            "company_name": company_name,
            "marketplace_id": next_rec.marketplace_id, 
            "date": start_iso[:10],
            "next_delay": next_delay,
            "credential_group": cred_group,
            "data_type": "scm_data"
        }

    except Exception as exc:
        logger.error(f"[process_scm_marketplaces] Controller error: {exc}")
        raise self.retry(exc=exc, countdown=120, max_retries=10)


def get_scm_status() -> dict:
    """
    Utility function to get current status of SCM data fetching.
    Can be called from views or management commands.
    """
    records = list(SCMLastRun.objects.all())
    yesterday = (timezone.now().astimezone(timezone.utc).date() - timedelta(days=1))
    
    status_list = []
    all_caught_up = True
    
    for rec in records:
        next_start, _ = _scm_day_window_after(rec.last_run)
        is_caught_up = not _scm_within_end_date(next_start)
        if not is_caught_up:
            all_caught_up = False
        
        status_list.append({
            "company_name": rec.company_name,
            "marketplace_id": rec.marketplace_id,
            "last_run": rec.last_run.isoformat() if rec.last_run else None,
            "next_day": next_start.date().isoformat(),
            "is_caught_up": is_caught_up,
        })
    
    return {
        "marketplaces": status_list,
        "yesterday": yesterday.isoformat(),
        "all_caught_up": all_caught_up,
        "total_marketplaces": len(records),
    }


def reset_scm_progress(marketplace_id: str = None, company_name: str = None) -> dict:
    """
    Reset SCM progress for a specific marketplace or all marketplaces.
    This sets last_run to None, which will cause fetching to start from Jan 1, 2026.
    
    Args:
        marketplace_id: Specific marketplace to reset, or None for all
    """
    filters = {}
    if marketplace_id:
        filters["marketplace_id"] = marketplace_id
    if company_name:
        filters["company_name"] = normalize_company_name(company_name)

    queryset = SCMLastRun.objects.filter(**filters) if filters else SCMLastRun.objects.all()
    updated = queryset.update(last_run=None)

    return {
        "status": "reset",
        "marketplace_id": marketplace_id,
        "company_name": filters.get("company_name"),
        "updated": updated,
    }

