import logging
from dataclasses import dataclass
from datetime import datetime, time as datetime_time, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from django.db import IntegrityError, connection, transaction
from django.db.models import Q
from django.utils import timezone

from .models import SCMJobLock

logger = logging.getLogger(__name__)


AMAZON_ORDERS_LOCK_NAME = "amazon_sp_api_orders_lock"
LOCK_OWNER_MAIN_INGESTION = "main_ingestion"
LOCK_OWNER_STATUS_SYNC = "status_sync"

MAIN_INGESTION_LOCK_EXPIRES_MINUTES = 480
MAIN_INGESTION_LOCK_WAIT_SECONDS = 5 * 60
MAIN_INGESTION_LOCK_POLL_SECONDS = 15

PROTECTED_WINDOW_TIMEZONE = "Asia/Karachi"
PROTECTED_WINDOW_START = datetime_time(23, 45)
PROTECTED_WINDOW_END = datetime_time(8, 15)


@dataclass(frozen=True)
class JobLockAcquireResult:
    acquired: bool
    reason: str
    lock: Optional[SCMJobLock] = None

    @property
    def locked_by(self) -> Optional[str]:
        return self.lock.locked_by if self.lock else None

    @property
    def expires_at(self) -> Optional[datetime]:
        return self.lock.expires_at if self.lock else None


def _is_mssql_connection() -> bool:
    return connection.vendor.lower() in {"microsoft", "mssql"}


def _ensure_aware_utc(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if timezone.is_naive(value):
        return timezone.make_aware(value, timezone=timezone.utc)
    return value.astimezone(timezone.utc)


def _is_lock_active(lock: Optional[SCMJobLock], now: Optional[datetime] = None) -> bool:
    if lock is None or not lock.locked_by or not lock.expires_at:
        return False
    current_time = _ensure_aware_utc(now or timezone.now())
    expires_at = _ensure_aware_utc(lock.expires_at)
    return expires_at is not None and expires_at > current_time


def is_lock_active(lock: Optional[SCMJobLock], now: Optional[datetime] = None) -> bool:
    return _is_lock_active(lock, now)


def _row_to_lock(row) -> SCMJobLock:
    return SCMJobLock(
        job_name=row[0],
        locked_at=_ensure_aware_utc(row[1]),
        expires_at=_ensure_aware_utc(row[2]),
        locked_by=row[3],
        stop_requested=bool(row[4]),
        created_at=_ensure_aware_utc(row[5]),
        updated_at=_ensure_aware_utc(row[6]),
    )


def _select_lock_for_update(job_name: str) -> Optional[SCMJobLock]:
    if _is_mssql_connection():
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT job_name, locked_at, expires_at, locked_by, stop_requested, created_at, updated_at
                FROM scm_job_locks WITH (UPDLOCK, HOLDLOCK)
                WHERE job_name = %s
                """,
                [job_name],
            )
            row = cursor.fetchone()
        return _row_to_lock(row) if row else None

    return SCMJobLock.objects.select_for_update().filter(job_name=job_name).first()


def _insert_lock(job_name: str, locked_by: Optional[str], expires_at: Optional[datetime], stop_requested: bool) -> SCMJobLock:
    now = timezone.now()
    locked_at = now if locked_by else None

    if _is_mssql_connection():
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO scm_job_locks
                    (job_name, locked_at, expires_at, locked_by, stop_requested, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                [job_name, locked_at, expires_at, locked_by, stop_requested, now, now],
            )
        return SCMJobLock(
            job_name=job_name,
            locked_at=locked_at,
            expires_at=expires_at,
            locked_by=locked_by,
            stop_requested=stop_requested,
            created_at=now,
            updated_at=now,
        )

    return SCMJobLock.objects.create(
        job_name=job_name,
        locked_at=locked_at,
        expires_at=expires_at,
        locked_by=locked_by,
        stop_requested=stop_requested,
    )


def _update_lock(
    job_name: str,
    *,
    locked_at: Optional[datetime],
    expires_at: Optional[datetime],
    locked_by: Optional[str],
    stop_requested: bool,
) -> SCMJobLock:
    now = timezone.now()

    if _is_mssql_connection():
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE scm_job_locks
                SET locked_at = %s,
                    expires_at = %s,
                    locked_by = %s,
                    stop_requested = %s,
                    updated_at = %s
                WHERE job_name = %s
                """,
                [locked_at, expires_at, locked_by, stop_requested, now, job_name],
            )
        return SCMJobLock(
            job_name=job_name,
            locked_at=locked_at,
            expires_at=expires_at,
            locked_by=locked_by,
            stop_requested=stop_requested,
            updated_at=now,
        )

    SCMJobLock.objects.filter(job_name=job_name).update(
        locked_at=locked_at,
        expires_at=expires_at,
        locked_by=locked_by,
        stop_requested=stop_requested,
        updated_at=now,
    )
    return SCMJobLock.objects.get(job_name=job_name)


def _acquire_job_lock_with_conditional_update(job_name: str, locked_by: str, expires_at: datetime) -> JobLockAcquireResult:
    """
    Atomic acquisition path for databases without row-level select_for_update.

    SQLite ignores select_for_update(), so this path uses a single conditional
    UPDATE for existing rows. The database decides whether the row is currently
    available at update time, which prevents two workers from both replacing the
    same expired lock.
    """
    now = timezone.now()

    with transaction.atomic():
        previous_lock = SCMJobLock.objects.filter(job_name=job_name).first()
        updated = (
            SCMJobLock.objects
            .filter(job_name=job_name)
            .filter(
                Q(locked_by__isnull=True)
                | Q(expires_at__isnull=True)
                | Q(expires_at__lte=now)
            )
            .update(
                locked_at=now,
                expires_at=expires_at,
                locked_by=locked_by,
                stop_requested=False,
                updated_at=now,
            )
        )

        if updated:
            lock = SCMJobLock.objects.get(job_name=job_name)
            if previous_lock and previous_lock.expires_at and _ensure_aware_utc(previous_lock.expires_at) <= _ensure_aware_utc(now):
                logger.warning(
                    "[job_lock] Expired lock replaced job_name=%s previous_owner=%s previous_expires_at=%s",
                    job_name,
                    previous_lock.locked_by,
                    previous_lock.expires_at,
                )
                reason = "expired_replaced"
            else:
                reason = "acquired_existing_available"
            logger.info("[job_lock] Lock acquired job_name=%s locked_by=%s reason=%s", job_name, locked_by, reason)
            return JobLockAcquireResult(True, reason, lock)

        if previous_lock is not None:
            logger.info(
                "[job_lock] Lock busy job_name=%s held_by=%s expires_at=%s stop_requested=%s",
                job_name,
                previous_lock.locked_by,
                previous_lock.expires_at,
                previous_lock.stop_requested,
            )
            return JobLockAcquireResult(False, "busy", previous_lock)

    try:
        lock = _insert_lock(job_name, locked_by, expires_at, stop_requested=False)
        logger.info("[job_lock] Lock acquired job_name=%s locked_by=%s", job_name, locked_by)
        return JobLockAcquireResult(True, "acquired", lock)
    except IntegrityError:
        logger.info("[job_lock] Lock insert raced for job_name=%s; checking current holder", job_name)
        lock = SCMJobLock.objects.filter(job_name=job_name).first()
        return JobLockAcquireResult(False, "busy_after_insert_race", lock)


def acquire_job_lock(job_name: str, locked_by: str, expires_minutes: int) -> JobLockAcquireResult:
    """
    Atomically acquire a database-backed job lock.

    On MSSQL, the existing row/key range is read using UPDLOCK + HOLDLOCK so
    competing workers serialize acquisition attempts around the same job_name.
    """
    logger.info(
        "[job_lock] Attempting lock acquisition job_name=%s locked_by=%s expires_minutes=%s",
        job_name,
        locked_by,
        expires_minutes,
    )

    now = timezone.now()
    expires_at = now + timedelta(minutes=expires_minutes)

    if not _is_mssql_connection():
        return _acquire_job_lock_with_conditional_update(job_name, locked_by, expires_at)

    try:
        with transaction.atomic():
            lock = _select_lock_for_update(job_name)

            if lock is None:
                lock = _insert_lock(job_name, locked_by, expires_at, stop_requested=False)
                logger.info("[job_lock] Lock acquired job_name=%s locked_by=%s", job_name, locked_by)
                return JobLockAcquireResult(True, "acquired", lock)

            if _is_lock_active(lock, now):
                logger.info(
                    "[job_lock] Lock busy job_name=%s held_by=%s expires_at=%s stop_requested=%s",
                    job_name,
                    lock.locked_by,
                    lock.expires_at,
                    lock.stop_requested,
                )
                return JobLockAcquireResult(False, "busy", lock)

            if lock.expires_at and _ensure_aware_utc(lock.expires_at) <= _ensure_aware_utc(now):
                logger.warning(
                    "[job_lock] Expired lock replaced job_name=%s previous_owner=%s previous_expires_at=%s",
                    job_name,
                    lock.locked_by,
                    lock.expires_at,
                )
                reason = "expired_replaced"
            else:
                reason = "acquired_unlocked_row"

            lock = _update_lock(
                job_name,
                locked_at=now,
                expires_at=expires_at,
                locked_by=locked_by,
                stop_requested=False,
            )
            logger.info("[job_lock] Lock acquired job_name=%s locked_by=%s reason=%s", job_name, locked_by, reason)
            return JobLockAcquireResult(True, reason, lock)

    except IntegrityError:
        logger.info("[job_lock] Lock insert raced for job_name=%s; retrying acquisition once", job_name)
        with transaction.atomic():
            lock = _select_lock_for_update(job_name)
            if lock and not _is_lock_active(lock):
                lock = _update_lock(
                    job_name,
                    locked_at=now,
                    expires_at=expires_at,
                    locked_by=locked_by,
                    stop_requested=False,
                )
                logger.info("[job_lock] Lock acquired after insert race job_name=%s locked_by=%s", job_name, locked_by)
                return JobLockAcquireResult(True, "acquired_after_insert_race", lock)
            return JobLockAcquireResult(False, "busy_after_insert_race", lock)


def release_job_lock(job_name: str, locked_by: Optional[str] = None, force: bool = False) -> bool:
    """Release a lock without releasing another owner's active lock by accident."""
    if not _is_mssql_connection():
        now = timezone.now()
        if force:
            updated = SCMJobLock.objects.filter(job_name=job_name).update(
                locked_at=None,
                expires_at=None,
                locked_by=None,
                updated_at=now,
            )
            if updated:
                logger.info("[job_lock] Lock released job_name=%s released_by=%s force=%s", job_name, locked_by, force)
                return True
            logger.info("[job_lock] Release skipped; lock row does not exist job_name=%s", job_name)
            return False

        if not locked_by:
            current = get_job_lock(job_name)
            logger.warning(
                "[job_lock] Release skipped; locked_by is required unless force=True job_name=%s current_owner=%s",
                job_name,
                current.locked_by if current else None,
            )
            return False

        updated = SCMJobLock.objects.filter(job_name=job_name, locked_by=locked_by).update(
            locked_at=None,
            expires_at=None,
            locked_by=None,
            updated_at=now,
        )
        if updated:
            logger.info("[job_lock] Lock released job_name=%s released_by=%s force=%s", job_name, locked_by, force)
            return True

        current = get_job_lock(job_name)
        if current is None:
            logger.info("[job_lock] Release skipped; lock row does not exist job_name=%s", job_name)
        else:
            logger.warning(
                "[job_lock] Release skipped due owner mismatch job_name=%s requested_owner=%s current_owner=%s",
                job_name,
                locked_by,
                current.locked_by,
            )
        return False

    with transaction.atomic():
        lock = _select_lock_for_update(job_name)
        if lock is None:
            logger.info("[job_lock] Release skipped; lock row does not exist job_name=%s", job_name)
            return False

        if not force:
            if not locked_by:
                logger.warning(
                    "[job_lock] Release skipped; locked_by is required unless force=True job_name=%s current_owner=%s",
                    job_name,
                    lock.locked_by,
                )
                return False
            if lock.locked_by != locked_by:
                logger.warning(
                    "[job_lock] Release skipped due owner mismatch job_name=%s requested_owner=%s current_owner=%s",
                    job_name,
                    locked_by,
                    lock.locked_by,
                )
                return False

        _update_lock(
            job_name,
            locked_at=None,
            expires_at=None,
            locked_by=None,
            stop_requested=lock.stop_requested,
        )
        logger.info("[job_lock] Lock released job_name=%s released_by=%s force=%s", job_name, locked_by, force)
        return True


def request_job_stop(job_name: str) -> bool:
    """Set the cooperative stop flag for the current/future owner of a job lock."""
    if not _is_mssql_connection():
        now = timezone.now()
        updated = SCMJobLock.objects.filter(job_name=job_name).update(
            stop_requested=True,
            updated_at=now,
        )
        if not updated:
            try:
                _insert_lock(job_name, locked_by=None, expires_at=None, stop_requested=True)
            except IntegrityError:
                SCMJobLock.objects.filter(job_name=job_name).update(
                    stop_requested=True,
                    updated_at=timezone.now(),
                )
        logger.warning("[job_lock] Stop requested job_name=%s", job_name)
        return True

    try:
        with transaction.atomic():
            lock = _select_lock_for_update(job_name)
            if lock is None:
                _insert_lock(job_name, locked_by=None, expires_at=None, stop_requested=True)
            elif not lock.stop_requested:
                _update_lock(
                    job_name,
                    locked_at=lock.locked_at,
                    expires_at=lock.expires_at,
                    locked_by=lock.locked_by,
                    stop_requested=True,
                )
            logger.warning("[job_lock] Stop requested job_name=%s", job_name)
            return True
    except IntegrityError:
        logger.info("[job_lock] Stop request insert raced for job_name=%s; retrying once", job_name)
        return request_job_stop(job_name)


def clear_job_stop_request(job_name: str) -> bool:
    if not _is_mssql_connection():
        updated = SCMJobLock.objects.filter(job_name=job_name, stop_requested=True).update(
            stop_requested=False,
            updated_at=timezone.now(),
        )
        if updated:
            logger.info("[job_lock] Stop request cleared job_name=%s", job_name)
            return True
        if SCMJobLock.objects.filter(job_name=job_name).exists():
            logger.info("[job_lock] Clear stop skipped; stop already false job_name=%s", job_name)
        else:
            logger.info("[job_lock] Clear stop skipped; lock row does not exist job_name=%s", job_name)
        return False

    with transaction.atomic():
        lock = _select_lock_for_update(job_name)
        if lock is None:
            logger.info("[job_lock] Clear stop skipped; lock row does not exist job_name=%s", job_name)
            return False
        if not lock.stop_requested:
            logger.info("[job_lock] Clear stop skipped; stop already false job_name=%s", job_name)
            return False
        _update_lock(
            job_name,
            locked_at=lock.locked_at,
            expires_at=lock.expires_at,
            locked_by=lock.locked_by,
            stop_requested=False,
        )
        logger.info("[job_lock] Stop request cleared job_name=%s", job_name)
        return True


def get_job_lock(job_name: str) -> Optional[SCMJobLock]:
    return SCMJobLock.objects.filter(job_name=job_name).first()


def is_job_lock_active(job_name: str) -> bool:
    return _is_lock_active(get_job_lock(job_name))


def is_protected_ingestion_window(now: Optional[datetime] = None, log_result: bool = False) -> bool:
    """Return True during 11:45 PM through 8:15 AM PKT."""
    current_time = now or timezone.now()
    current_time = _ensure_aware_utc(current_time)
    pkt_now = current_time.astimezone(ZoneInfo(PROTECTED_WINDOW_TIMEZONE))
    pkt_clock = pkt_now.time()
    in_window = pkt_clock >= PROTECTED_WINDOW_START or pkt_clock <= PROTECTED_WINDOW_END

    if log_result:
        logger.info(
            "[job_lock] Protected ingestion window check pkt_now=%s in_window=%s",
            pkt_now.isoformat(),
            in_window,
        )

    return in_window
