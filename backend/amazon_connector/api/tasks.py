# api/tasks.py
from datetime import timedelta, datetime
from celery import shared_task, chain
from django.conf import settings
import logging
from .models import MarketplaceLastRun, Activities
from django.utils import timezone
import os, json
import requests
from .marketplaces import get_marketplace_id, get_available_marketplaces

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

def _iso_z(dt: datetime) -> str:
    """Helper: ensure UTC Zulu ISO format."""
    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt, timezone=timezone.utc)
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _day_ranges(start_dt: datetime, end_dt: datetime):
    """Yield (start, end) for each day between start_dt (inclusive) and end_dt (exclusive)."""
    cur = start_dt
    while cur < end_dt:
        next_day = min(cur + timedelta(days=1), end_dt)
        yield cur, next_day
        cur = next_day


def _is_marketplace_busy(marketplace_id: str) -> bool:
    """Prevent overlapping tasks for the same marketplace by checking any in-progress orders activity."""
    return Activities.objects.filter(
        marketplace_id=marketplace_id,
        activity_type='orders',
        status='in_progress',
    ).exists()


@shared_task(bind=True, queue='fetching')
def fetch_marketplace_day(self, marketplace_id: str, start_iso: str, end_iso: str):
    """
    Fetch one day's data for a marketplace and update its last_run on success.

    Retries are scoped to this single day. A soft lock is taken via Activities(unique_in_progress) to
    prevent duplicate processing for the same marketplace and day.
    """
    access_token = None
    activity = None
    try:
        logger.info(f"[DayTask] {marketplace_id} {start_iso} -> {end_iso}")
        # If another task is already processing any range for this marketplace, skip for now
        if _is_marketplace_busy(marketplace_id):
            logger.warning(f"[DayTask] {marketplace_id} is busy with another in-progress task. Skipping this run.")
            return {'skipped': True, 'reason': 'marketplace_busy'}

        access_token = get_access_token()

        # Create in-progress activity as a per-day lock
        start_dt = datetime.fromisoformat(start_iso.replace('Z', ''))
        end_dt = datetime.fromisoformat(end_iso.replace('Z', ''))

        activity, created = Activities.objects.get_or_create(
            marketplace_id=marketplace_id,
            activity_type='orders',
            date_from=start_dt.date(),
            date_to=end_dt.date(),
            status='in_progress',
            defaults={
                'action': 'automatic',
                'detail': f'Starting fetch for {marketplace_id} {start_dt.date()} -> {end_dt.date()}',
            }
        )
        if not created:
            # Another worker has the lock for this specific day
            logger.info(f"[DayTask] Lock exists for {marketplace_id} {start_dt.date()} - skipping")
            return {'skipped': True, 'reason': 'day_locked'}

        payload = {
            'access_token': access_token,
            'marketplace_id': marketplace_id,
            'start_date': start_iso,
            'end_date': end_iso,
            'auto_save': True,
        }
        response = requests.post("http://127.0.0.1:8000/api/fetch-data/", json=payload, timeout=60)
        logger.info(f"[DayTask] API status {response.status_code} for {marketplace_id} {start_iso} -> {end_iso}")

        if response.status_code != 200:
            raise Exception(f"API error {response.status_code}: {response.text[:500]}")

        # Mark activity completed (best-effort parse of counts)
        try:
            data = response.json()
            orders = data.get('orders_count') or data.get('orders_fetched') or 0
            items = data.get('items_count') or data.get('items_fetched') or 0
            activity.orders_fetched = orders
            activity.items_fetched = items
            activity.status = 'completed'
            activity.detail = activity.detail + ' | Completed successfully'
            activity.save(update_fields=['orders_fetched', 'items_fetched', 'status', 'detail', 'updated_at'])
        except Exception as parse_err:
            logger.warning(f"[DayTask] Could not parse response json for activity stats: {parse_err}")
            activity.status = 'completed'
            activity.save(update_fields=['status', 'updated_at'])

        # Advance last_run to the end of this window
        MarketplaceLastRun.objects.filter(marketplace_id=marketplace_id).update(last_run=end_dt)
        return {'success': True}

    except Exception as exc:
        logger.error(f"[DayTask] Failed for {marketplace_id} {start_iso}->{end_iso}: {exc}")
        # Mark activity failed if it was created
        if activity and activity.status == 'in_progress':
            activity.status = 'failed'
            activity.error_message = str(exc)
            activity.detail = (activity.detail or '') + ' | Failed'
            try:
                activity.save(update_fields=['status', 'error_message', 'detail', 'updated_at'])
            except Exception:
                pass
        # Retry this day only
        raise self.retry(exc=exc, countdown=60, max_retries=3)


@shared_task(bind=True, queue='fetching')
def fetch_marketplace_data(self, marketplace_id: str):
    """
    Orchestrate fetching for a single marketplace from its last_run to now, one day at a time.

    Builds a Celery chain of per-day tasks so they execute sequentially for this marketplace,
    but different marketplaces can run in parallel. Prevents overlapping work per-marketplace.
    """
    try:
        # Early overlap check
        if _is_marketplace_busy(marketplace_id):
            logger.warning(f"[MarketTask] {marketplace_id} already has an in-progress task. Skipping dispatch.")
            return {'skipped': True, 'reason': 'marketplace_busy'}

        try:
            mlr = MarketplaceLastRun.objects.get(marketplace_id=marketplace_id)
            start_dt = mlr.last_run or (timezone.now() - timedelta(days=1))
        except MarketplaceLastRun.DoesNotExist:
            logger.warning(f"[MarketTask] No MarketplaceLastRun row for {marketplace_id}; skipping")
            return {'skipped': True, 'reason': 'no_last_run_row'}

        now_utc = timezone.now()
        if start_dt >= now_utc:
            logger.info(f"[MarketTask] Up-to-date for {marketplace_id} (last_run >= now)")
            return {'up_to_date': True}

        # Build chain of daily tasks
        subtasks = []
        for day_start, day_end in _day_ranges(start_dt, now_utc):
            subtasks.append(
                fetch_marketplace_day.s(
                    marketplace_id,
                    _iso_z(day_start),
                    _iso_z(day_end),
                ).set(queue='fetching')
            )

        if not subtasks:
            return {'up_to_date': True}

        ch = chain(*subtasks)
        ch.apply_async()
        logger.info(f"[MarketTask] Dispatched {len(subtasks)} day tasks for {marketplace_id}")
        return {'dispatched_days': len(subtasks)}

    except Exception as exc:
        logger.error(f"[MarketTask] Orchestration failed for {marketplace_id}: {exc}")
        raise self.retry(exc=exc, countdown=60, max_retries=3)


@shared_task(bind=True, queue='fetching')
def fetch_amazon_data(self):
    """
    Beat-triggered task: enumerate marketplaces and dispatch a per-marketplace chain of daily tasks.

    This satisfies:
    - Get all marketplace IDs from marketplace_last_run table
    - Read last_run per marketplace
    - Dispatch separate Celery chains per marketplace (parallel across markets, sequential per day)
    """
    try:
        logger.info("[Beat] Scheduling per-marketplace fetch chains…")

        # Trust the authoritative table for marketplaces to fetch
        rows = list(MarketplaceLastRun.objects.all().values('marketplace_id', 'last_run'))
        if not rows:
            logger.warning("[Beat] No MarketplaceLastRun rows found. Nothing to schedule.")
            return {'scheduled': 0}

        scheduled = 0
        for row in rows:
            marketplace_id = row['marketplace_id']
            # Dispatch per-marketplace orchestrator
            fetch_marketplace_data.apply_async(args=[marketplace_id], queue='fetching')
            scheduled += 1

        logger.info(f"[Beat] Scheduled {scheduled} marketplace chains")
        return {'scheduled': scheduled}

    except Exception as exc:
        logger.error(f"[Beat] Task failed: {exc}")
        raise self.retry(exc=exc, countdown=60, max_retries=3)

@shared_task(bind=True, queue='reports')
def generate_reports(self):
    """
    Asynchronous task to generate reports
    """
    try:
        print("Generating reports...")
        # Default to all marketplace codes
        marketplaces = list(get_available_marketplaces().keys())
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