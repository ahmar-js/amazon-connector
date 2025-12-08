# api/tasks.py
from datetime import timedelta, datetime, time
from celery import shared_task
from celery import group, chain
from django.conf import settings
import logging
from .models import MarketplaceLastRun
from django.utils import timezone
import os, json
import requests
from django.db import transaction
from django.utils.dateparse import parse_datetime

logger = logging.getLogger(__name__)

def get_access_token():
    """
    Get access token 
    """
    # Go one directory up to reach amazon_connector/
    base_dir = os.path.dirname(os.path.dirname(__file__))
    creds_path = os.path.join(base_dir, 'creds.json')
    
    with open(creds_path, 'r') as f:
        creds = json.load(f)
    return creds['access_token']

# Constants for controller loop
# Seed date: if last_run is None, we'll start fetching from the day AFTER this timestamp.
# Example flow expects first fetch to be 2023-11-02 when seed is 2023-11-01 23:59:59Z.
SEED_START_LAST_RUN = timezone.datetime(2023, 11, 1, 23, 59, 59, tzinfo=timezone.utc)
# Fixed inclusive end date per requirements
END_DATE = timezone.datetime(2025, 3, 31, 23, 59, 59, tzinfo=timezone.utc)

# HTTP timeouts for calling the local Django endpoint
# Defaults: connect 5s, read 4h (adjust via env if fetch can be longer)
HTTP_CONNECT_TIMEOUT = int(os.getenv("FETCH_CONNECT_TIMEOUT", "5"))
HTTP_READ_TIMEOUT = int(os.getenv("FETCH_READ_TIMEOUT", "14400"))  # 4 hours

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

# @shared_task(bind=True, queue='fetching')
# def fetch_amazon_data(self):
#     try:
#         print("Fetching Amazon data...")
#         logger.info("Fetching Amazon data...")

#         marketplaces = ['APJ6JRA9NG5V4']
#         access_token = get_access_token()
#         logger.info(f"access token: {access_token}")

#         for marketplace in marketplaces:
#             marketplace_last_run = MarketplaceLastRun.objects.get(marketplace_id=marketplace)
#             # marketplace_last_run, created = MarketplaceLastRun.objects.get_or_create(marketplace_id=marketplace)

#             if marketplace_last_run.last_run is None:
#                 logger.info("Inside IF statement")
#                 start_date = "2024-03-04T00:00:00Z"
#                 end_date = "2024-03-04T23:59:59Z"
#                 # start_date = (timezone.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0).isoformat().replace('+00:00', 'Z')
#                 # end_date = (timezone.now() - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999).isoformat().replace('+00:00', 'Z')
#                 logger.info(f"start_date: {start_date}, end_date: {end_date} inside IF Statement")
#             else:
#                 logger.info("Inside ELSE statement")
#                 start_date = "2025-03-04T00:00:00Z"
#                 end_date = "2025-03-04T23:59:59Z"
#                 # start_date = marketplace_last_run.last_run.isoformat().replace('+00:00', 'Z')
#                 # end_date = timezone.now().replace(hour=23, minute=59, second=59, microsecond=999999).isoformat().replace('+00:00', 'Z')
#                 logger.info(f"start_date: {start_date}, end_date: {end_date} inside ELSE Statement")

#             logger.info(f"Marketplace: {marketplace}, Start: {start_date}, End: {end_date}")

#             payload = {
#                 "access_token": access_token,
#                 "marketplace_id": marketplace,
#                 "start_date": str(start_date),
#                 "end_date": str(end_date),
#                 "auto_save": True,
#             }
#             # logger.info("payload: ", payload)
#             logger.info(f"payload: {payload}")

#             # Call your Django API to fetch the data from amazon
#             response = requests.post("http://127.0.0.1:8000/api/fetch-data/", json=payload)

#             logger.info(f"API Response ({response.status_code})")
#             # timestamp = timezone.now().strftime('%Y%m%d_%H%M%S')
#             # filename = f'amazon_response_{marketplace}_{timestamp}.txt'
#             # file_path = os.path.join(os.path.dirname(__file__), 'responses', filename)
#             # os.makedirs(os.path.dirname(file_path), exist_ok=True)

#             # with open(file_path, 'w', encoding='utf-8') as f:
#             #     f.write(response.text)

#             # logger.info(f"Response saved to {file_path}")

#             if response.status_code == 200:
#                 # Save last run time only if successful
#                 marketplace_last_run.last_run = timezone.now()
#                 marketplace_last_run.save()
#             else:
#                 logger.warning(f"Failed to fetch data for {marketplace}: {response.status_code}")
#                 raise Exception(f"Fetch failed with {response.status_code}: {response.text}")

#     except Exception as exc:
#         logger.error(f"Task failed: {exc}")
#         raise self.retry(exc=exc, countdown=60, max_retries=3)


@shared_task(bind=True, queue='fetching', soft_time_limit=21600, time_limit=21900)
def fetch_orders_for_marketplace(self, marketplace_id: str, start_iso: str, end_iso: str):
    """
    Fetch orders for a single marketplace for a single day window [start, end].
    On success, update that marketplace's last_run to the day's end time.
    """
    try:
        access_token = get_access_token()

        # Idempotency and sequence guard: only process if this window equals the next required day
        start_dt = _parse_iso_utc(start_iso)
        end_dt_req = _parse_iso_utc(end_iso)
        with transaction.atomic():
            obj = MarketplaceLastRun.objects.select_for_update().get(marketplace_id=marketplace_id)
            expected_start_dt, expected_end_dt = _day_window_after(obj.last_run)
            if start_dt != expected_start_dt:
                logger.info(
                    f"[fetch_orders_for_marketplace] Skip {marketplace_id}: requested {start_dt.date()} != expected {expected_start_dt.date()} (idempotent guard)"
                )
                return {
                    "marketplace_id": marketplace_id,
                    "status": "skipped",
                    "requested": start_iso,
                    "expected": expected_start_dt.isoformat().replace("+00:00", "Z"),
                }

        payload = {
            "access_token": access_token,
            "marketplace_id": marketplace_id,
            "start_date": start_iso,
            "end_date": end_iso,
            "auto_save": True,
        }

        logger.info(f"[fetch_orders_for_marketplace] {marketplace_id} -> {start_iso} to {end_iso}")
        response = requests.post(
            "http://127.0.0.1:8000/api/fetch-data/",
            json=payload,
            timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT),
        )

        if 200 <= response.status_code < 300:
            # Persist last_run as end of the day, only if still expected (avoid racing duplicates)
            end_dt = _parse_iso_utc(end_iso)
            with transaction.atomic():
                obj = MarketplaceLastRun.objects.select_for_update().get(marketplace_id=marketplace_id)
                curr_expected_start, _ = _day_window_after(obj.last_run)
                if curr_expected_start != start_dt:
                    logger.info(
                        f"[fetch_orders_for_marketplace] Not updating last_run for {marketplace_id}: window already advanced elsewhere"
                    )
                else:
                    obj.last_run = end_dt
                    obj.save(update_fields=["last_run"])
            logger.info(f"[fetch_orders_for_marketplace] Updated last_run for {marketplace_id} -> {end_iso}")
            return {"marketplace_id": marketplace_id, "status": "ok", "fetched": [start_iso, end_iso]}
        else:
            logger.warning(
                f"[fetch_orders_for_marketplace] Failed for {marketplace_id} ({response.status_code}): {response.text[:500]}"
            )
            raise Exception(f"HTTP {response.status_code}")

    except Exception as exc:
        logger.error(f"[fetch_orders_for_marketplace] Error for {marketplace_id}: {exc}")
        raise self.retry(exc=exc, countdown=30, max_retries=5)


@shared_task(bind=True, queue='fetching')
def process_marketplaces(self):
    """
    Controller task:
    - Reads all marketplaces and their last_run
    - Schedules per-marketplace day fetch tasks (one day per marketplace)
    - Chains itself (after all current tasks finish) with a delay to continue looping
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
        candidates = []  # (start_dt, marketplace_id, rec, end_dt)
        for rec in records:
            start_dt, end_dt = _day_window_after(rec.last_run)
            eligible = _within_end_date(start_dt)
            logger.info(
                f"[process_marketplaces] Candidate {rec.marketplace_id}: last_run={rec.last_run} -> next_day={start_dt.date()} eligible={eligible}"
            )
            if eligible:
                candidates.append((start_dt, rec.marketplace_id, rec, end_dt))

        if not candidates:
            logger.info("[process_marketplaces] All marketplaces have reached END_DATE. Controller will stop.")
            return {"status": "completed"}

        # Choose the marketplace whose next day is earliest; ties broken by marketplace_id
        candidates.sort(key=lambda x: (x[0], x[1]))
        start_dt, _mid, next_rec, end_dt = candidates[0]
        logger.info(
            f"[process_marketplaces] Chosen next: {next_rec.marketplace_id} for day {start_dt.date()} (earliest among {len(candidates)} candidates)"
        )
        start_iso = start_dt.isoformat().replace("+00:00", "Z")
        end_iso = end_dt.isoformat().replace("+00:00", "Z")

        logger.info(
            f"[process_marketplaces] Scheduling single task for {next_rec.marketplace_id}: {start_iso} -> {end_iso}"
        )

        # Chain a single marketplace-day fetch, then re-queue the controller after 10s
        ch = chain(
            fetch_orders_for_marketplace.si(next_rec.marketplace_id, start_iso, end_iso),
            process_marketplaces.si().set(countdown=10),
        )
        ch.apply_async()

        return {"status": "dispatched-one", "marketplace_id": next_rec.marketplace_id, "date": start_iso[:10]}

    except Exception as exc:
        logger.error(f"[process_marketplaces] Controller error: {exc}")
        # Back off a bit, then try again
        raise self.retry(exc=exc, countdown=120, max_retries=10)

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

@shared_task(bind=True, queue='syncing')
def sync_amazon_data(self, data, **kwargs):
    """
    Asynchronous task to sync processed data to databases
    """
    try:
        print("Syncing Amazon data...")
        # Move your database sync logic here
        pass
    except Exception as exc:
        logger.error(f"Sync task failed: {exc}")
        raise self.retry(exc=exc, countdown=60, max_retries=3)