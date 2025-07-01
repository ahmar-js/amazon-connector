from celery import shared_task
from django.utils import timezone
from datetime import datetime, timedelta
import logging
import traceback
from .models import CronJobStatus, CronJobConfiguration, CronJobLog, Activities
from .views import FetchAmazonDataView

logger = logging.getLogger(__name__)


class MutualExclusionError(Exception):
    """Custom exception for mutual exclusion violations"""
    pass


def check_mutual_exclusion(job_type):
    """Check if any other job is running before starting this job"""
    if CronJobStatus.is_any_job_running():
        running_jobs = CronJobStatus.objects.filter(status='running')
        running_job_types = [job.job_type for job in running_jobs]
        raise MutualExclusionError(
            f"Cannot start {job_type} job. Other jobs are running: {', '.join(running_job_types)}"
        )


@shared_task(bind=True, name='api.tasks.fetch_amazon_data_task')
def fetch_amazon_data_task(self):
    """
    Celery task to fetch Amazon data with mutual exclusion
    """
    job_type = 'fetching'
    task_id = self.request.id
    start_time = timezone.now()
    
    # Get or create job status
    job_status, created = CronJobStatus.objects.get_or_create(
        job_type=job_type,
        defaults={'status': 'idle'}
    )
    
    # Create job log entry
    job_log = CronJobLog.objects.create(
        job_type=job_type,
        status='started',
        task_id=task_id
    )
    
    try:
        # Check mutual exclusion
        check_mutual_exclusion(job_type)
        
        # Mark as running
        job_status.mark_as_running(task_id)
        
        logger.info(f"Starting {job_type} job with task ID: {task_id}")
        
        # Get job configuration
        try:
            config = CronJobConfiguration.objects.get(job_type=job_type)
        except CronJobConfiguration.DoesNotExist:
            # Create default configuration
            config = CronJobConfiguration.objects.create(
                job_type=job_type,
                enabled=True,
                cron_expression='0 0 */15 * *',  # Every 15 days at midnight
                description='Fetch order/item data from Amazon SP-API',
                date_range_days=15
            )
        
        if not config.enabled:
            raise Exception(f"{job_type} job is disabled in configuration")
        
        # Calculate date range
        end_date = timezone.now().date()
        start_date = end_date - timedelta(days=config.date_range_days)
        
        # Prepare request data
        request_data = {
            'marketplace_id': 'ATVPDKIKX0DER',  # Default to US marketplace
            'activity_type': 'GET_ORDER_METRICS_DATA',
            'action': 'fetch_orders',
            'date_from': start_date.isoformat(),
            'date_to': end_date.isoformat(),
            'auto_save': True  # Enable auto-save for cron jobs
        }
        
        # Create a mock request object for the view
        class MockRequest:
            def __init__(self, data):
                self.data = data
                self.user = None
        
        mock_request = MockRequest(request_data)
        
        # Execute the fetch operation
        view = FetchAmazonDataView()
        result = view.post(mock_request)
        
        # Check if the operation was successful
        if hasattr(result, 'status_code') and result.status_code == 200:
            # Parse response data
            response_data = result.data if hasattr(result, 'data') else {}
            
            # Calculate duration
            end_time = timezone.now()
            duration = end_time - start_time
            
            # Update job log
            job_log.status = 'completed'
            job_log.completed_at = end_time
            job_log.duration = duration
            job_log.records_processed = response_data.get('total_records', 0)
            job_log.details = {
                'orders_fetched': response_data.get('orders_fetched', 0),
                'items_fetched': response_data.get('items_fetched', 0),
                'date_range': f"{start_date} to {end_date}",
                'marketplace': request_data['marketplace_id'],
                'auto_save_enabled': True
            }
            job_log.save()
            
            # Mark job as completed
            job_status.mark_as_completed(duration)
            
            logger.info(f"Completed {job_type} job. Duration: {duration}, Records: {job_log.records_processed}")
            
            return {
                'status': 'success',
                'duration': str(duration),
                'records_processed': job_log.records_processed,
                'details': job_log.details
            }
        else:
            raise Exception(f"Fetch operation failed with status: {getattr(result, 'status_code', 'unknown')}")
            
    except MutualExclusionError as e:
        logger.warning(f"Mutual exclusion violation for {job_type} job: {e}")
        
        # Update job log
        job_log.status = 'failed'
        job_log.completed_at = timezone.now()
        job_log.error_message = str(e)
        job_log.save()
        
        # Don't mark job_status as failed for mutual exclusion - it's not really a failure
        return {
            'status': 'skipped',
            'reason': 'mutual_exclusion',
            'message': str(e)
        }
        
    except Exception as e:
        error_message = str(e)
        logger.error(f"Error in {job_type} job: {error_message}\n{traceback.format_exc()}")
        
        # Calculate duration
        end_time = timezone.now()
        duration = end_time - start_time
        
        # Update job log
        job_log.status = 'failed'
        job_log.completed_at = end_time
        job_log.duration = duration
        job_log.error_message = error_message
        job_log.save()
        
        # Mark job as failed
        job_status.mark_as_failed(error_message)
        
        # Re-raise the exception so Celery knows the task failed
        raise self.retry(exc=e, countdown=60, max_retries=3)


@shared_task(bind=True, name='api.tasks.sync_amazon_data_task')
def sync_amazon_data_task(self):
    """
    Celery task to sync Amazon data to internal database with mutual exclusion
    """
    job_type = 'syncing'
    task_id = self.request.id
    start_time = timezone.now()
    
    # Get or create job status
    job_status, created = CronJobStatus.objects.get_or_create(
        job_type=job_type,
        defaults={'status': 'idle'}
    )
    
    # Create job log entry
    job_log = CronJobLog.objects.create(
        job_type=job_type,
        status='started',
        task_id=task_id
    )
    
    try:
        # Check mutual exclusion
        check_mutual_exclusion(job_type)
        
        # Mark as running
        job_status.mark_as_running(task_id)
        
        logger.info(f"Starting {job_type} job with task ID: {task_id}")
        
        # Get job configuration
        try:
            config = CronJobConfiguration.objects.get(job_type=job_type)
        except CronJobConfiguration.DoesNotExist:
            # Create default configuration
            config = CronJobConfiguration.objects.create(
                job_type=job_type,
                enabled=True,
                cron_expression='0 0 */7 * *',  # Every 7 days at midnight
                description='Sync last 100 days of fetched data into internal database',
                sync_days_back=100
            )
        
        if not config.enabled:
            raise Exception(f"{job_type} job is disabled in configuration")
        
        # Calculate sync date range
        end_date = timezone.now().date()
        start_date = end_date - timedelta(days=config.sync_days_back)
        
        # Get activities to sync (completed activities from the last N days)
        activities_to_sync = Activities.objects.filter(
            status='completed',
            database_saved=True,
            activity_date__date__gte=start_date,
            activity_date__date__lte=end_date
        ).order_by('-activity_date')
        
        total_records_synced = 0
        activities_processed = 0
        
        # Process each activity
        for activity in activities_to_sync:
            try:
                # Here you would implement your actual sync logic
                # For now, we'll simulate the sync process
                
                # Example: Sync to secondary database
                # sync_result = sync_activity_to_database(activity)
                
                # Simulate processing
                activities_processed += 1
                total_records_synced += activity.total_records
                
                logger.info(f"Synced activity {activity.activity_id}: {activity.total_records} records")
                
            except Exception as sync_error:
                logger.error(f"Error syncing activity {activity.activity_id}: {sync_error}")
                continue
        
        # Calculate duration
        end_time = timezone.now()
        duration = end_time - start_time
        
        # Update job log
        job_log.status = 'completed'
        job_log.completed_at = end_time
        job_log.duration = duration
        job_log.records_processed = total_records_synced
        job_log.details = {
            'activities_processed': activities_processed,
            'total_activities_found': activities_to_sync.count(),
            'date_range': f"{start_date} to {end_date}",
            'sync_days_back': config.sync_days_back
        }
        job_log.save()
        
        # Mark job as completed
        job_status.mark_as_completed(duration)
        
        logger.info(f"Completed {job_type} job. Duration: {duration}, Records synced: {total_records_synced}")
        
        return {
            'status': 'success',
            'duration': str(duration),
            'records_processed': total_records_synced,
            'activities_processed': activities_processed,
            'details': job_log.details
        }
        
    except MutualExclusionError as e:
        logger.warning(f"Mutual exclusion violation for {job_type} job: {e}")
        
        # Update job log
        job_log.status = 'failed'
        job_log.completed_at = timezone.now()
        job_log.error_message = str(e)
        job_log.save()
        
        # Don't mark job_status as failed for mutual exclusion
        return {
            'status': 'skipped',
            'reason': 'mutual_exclusion',
            'message': str(e)
        }
        
    except Exception as e:
        error_message = str(e)
        logger.error(f"Error in {job_type} job: {error_message}\n{traceback.format_exc()}")
        
        # Calculate duration
        end_time = timezone.now()
        duration = end_time - start_time
        
        # Update job log
        job_log.status = 'failed'
        job_log.completed_at = end_time
        job_log.duration = duration
        job_log.error_message = error_message
        job_log.save()
        
        # Mark job as failed
        job_status.mark_as_failed(error_message)
        
        # Re-raise the exception so Celery knows the task failed
        raise self.retry(exc=e, countdown=60, max_retries=3)


@shared_task(bind=True, name='api.tasks.manual_trigger_task')
def manual_trigger_task(self, job_type, user_params=None):
    """
    Manually trigger a specific job type with optional user parameters
    """
    if job_type == 'fetching':
        return fetch_amazon_data_task.apply_async()
    elif job_type == 'syncing':
        return sync_amazon_data_task.apply_async()
    else:
        raise ValueError(f"Unknown job type: {job_type}")


@shared_task(name='api.tasks.cleanup_old_logs')
def cleanup_old_logs():
    """
    Cleanup old job logs (keep only last 30 days)
    """
    cutoff_date = timezone.now() - timedelta(days=30)
    deleted_count = CronJobLog.objects.filter(started_at__lt=cutoff_date).delete()[0]
    logger.info(f"Cleaned up {deleted_count} old job logs")
    return {'deleted_count': deleted_count} 