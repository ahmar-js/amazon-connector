from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.utils import timezone
from django.db import transaction
from datetime import datetime, timedelta
import json
import logging
from celery import current_app
from django_celery_beat.models import PeriodicTask, CrontabSchedule

from .models import CronJobStatus, CronJobConfiguration, CronJobLog
from .tasks import fetch_amazon_data_task, sync_amazon_data_task, manual_trigger_task

logger = logging.getLogger(__name__)


class CronJobStatusView(APIView):
    """
    API view to get the status of all cron jobs
    """
    
    def get(self, request):
        """Get status of all cron jobs"""
        try:
            # Get or create job statuses
            fetching_status, _ = CronJobStatus.objects.get_or_create(
                job_type='fetching',
                defaults={'status': 'idle'}
            )
            syncing_status, _ = CronJobStatus.objects.get_or_create(
                job_type='syncing',
                defaults={'status': 'idle'}
            )
            
            # Get configurations
            fetching_config, _ = CronJobConfiguration.objects.get_or_create(
                job_type='fetching',
                defaults={
                    'enabled': True,
                    'cron_expression': '0 0 */15 * *',
                    'description': 'Fetch order/item data from Amazon SP-API every 15 days',
                    'date_range_days': 15
                }
            )
            syncing_config, _ = CronJobConfiguration.objects.get_or_create(
                job_type='syncing',
                defaults={
                    'enabled': True,
                    'cron_expression': '0 0 */7 * *',
                    'description': 'Sync last 100 days of data every 7 days',
                    'sync_days_back': 100
                }
            )
            
            # Calculate next run times from periodic tasks
            def get_next_run_time(job_type):
                try:
                    periodic_task = PeriodicTask.objects.get(name=f"{job_type}_job")
                    if periodic_task.enabled and periodic_task.crontab:
                        # This is a simplified calculation - in production you might want to use a library
                        # like croniter for more accurate next run calculations
                        return timezone.now() + timedelta(hours=1)  # Placeholder
                    return None
                except PeriodicTask.DoesNotExist:
                    return None
            
            fetching_status.next_run = get_next_run_time('fetching')
            syncing_status.next_run = get_next_run_time('syncing')
            
            # Prepare response data
            response_data = {
                'jobs': {
                    'fetching': {
                        'status': {
                            'job_type': fetching_status.job_type,
                            'status': fetching_status.status,
                            'last_run': fetching_status.last_run.isoformat() if fetching_status.last_run else None,
                            'next_run': fetching_status.next_run.isoformat() if fetching_status.next_run else None,
                            'last_duration': str(fetching_status.last_duration) if fetching_status.last_duration else None,
                            'error_message': fetching_status.error_message,
                        },
                        'configuration': {
                            'job_type': fetching_config.job_type,
                            'enabled': fetching_config.enabled,
                            'cron_expression': fetching_config.cron_expression,
                            'description': fetching_config.description,
                            'date_range_days': fetching_config.date_range_days,
                        },
                    },
                    'syncing': {
                        'status': {
                            'job_type': syncing_status.job_type,
                            'status': syncing_status.status,
                            'last_run': syncing_status.last_run.isoformat() if syncing_status.last_run else None,
                            'next_run': syncing_status.next_run.isoformat() if syncing_status.next_run else None,
                            'last_duration': str(syncing_status.last_duration) if syncing_status.last_duration else None,
                            'error_message': syncing_status.error_message,
                        },
                        'configuration': {
                            'job_type': syncing_config.job_type,
                            'enabled': syncing_config.enabled,
                            'cron_expression': syncing_config.cron_expression,
                            'description': syncing_config.description,
                            'sync_days_back': syncing_config.sync_days_back,
                        },
                    }
                },
                'system_status': {
                    'any_job_running': CronJobStatus.is_any_job_running(),
                    'celery_status': self._check_celery_status(),
                    'redis_status': self._check_redis_status(),
                }
            }
            
            return Response(response_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Error getting cron job status: {e}")
            return Response(
                {'error': 'Failed to get job status', 'details': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def _check_celery_status(self):
        """Check if Celery workers are running"""
        try:
            inspect = current_app.control.inspect()
            stats = inspect.stats()
            return {'active': bool(stats), 'workers': list(stats.keys()) if stats else []}
        except Exception:
            return {'active': False, 'workers': []}
    
    def _check_redis_status(self):
        """Check Redis connection status"""
        try:
            from django.core.cache import cache
            cache.set('health_check', 'ok', 30)
            result = cache.get('health_check')
            return {'active': result == 'ok'}
        except Exception:
            return {'active': False}


class CronJobConfigurationView(APIView):
    """
    API view to manage cron job configurations
    """
    
    def get(self, request, job_type=None):
        """Get configuration for a specific job type or all jobs"""
        try:
            if job_type:
                try:
                    config = CronJobConfiguration.objects.get(job_type=job_type)
                    return Response({
                        'job_type': config.job_type,
                        'enabled': config.enabled,
                        'cron_expression': config.cron_expression,
                        'description': config.description,
                        'date_range_days': config.date_range_days,
                        'sync_days_back': config.sync_days_back,
                    })
                except CronJobConfiguration.DoesNotExist:
                    return Response(
                        {'error': f'Configuration for {job_type} not found'},
                        status=status.HTTP_404_NOT_FOUND
                    )
            else:
                configs = CronJobConfiguration.objects.all()
                return Response([{
                    'job_type': config.job_type,
                    'enabled': config.enabled,
                    'cron_expression': config.cron_expression,
                    'description': config.description,
                    'date_range_days': config.date_range_days,
                    'sync_days_back': config.sync_days_back,
                } for config in configs])
                
        except Exception as e:
            logger.error(f"Error getting job configuration: {e}")
            return Response(
                {'error': 'Failed to get job configuration', 'details': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def put(self, request, job_type):
        """Update configuration for a specific job type"""
        try:
            with transaction.atomic():
                config, created = CronJobConfiguration.objects.get_or_create(
                    job_type=job_type,
                    defaults={'cron_expression': '0 0 */15 * *'}
                )
                
                # Update fields from request data
                if 'enabled' in request.data:
                    config.enabled = request.data['enabled']
                if 'cron_expression' in request.data:
                    config.cron_expression = request.data['cron_expression']
                if 'description' in request.data:
                    config.description = request.data['description']
                if 'date_range_days' in request.data:
                    config.date_range_days = request.data['date_range_days']
                if 'sync_days_back' in request.data:
                    config.sync_days_back = request.data['sync_days_back']
                
                config.save()
                
                return Response({
                    'success': True,
                    'message': f'Configuration updated for {job_type}',
                    'data': {
                        'job_type': config.job_type,
                        'enabled': config.enabled,
                        'cron_expression': config.cron_expression,
                        'description': config.description,
                        'date_range_days': config.date_range_days,
                        'sync_days_back': config.sync_days_back,
                    }
                })
                    
        except Exception as e:
            logger.error(f"Error updating job configuration: {e}")
            return Response(
                {'error': 'Failed to update job configuration', 'details': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class CronJobTriggerView(APIView):
    """
    API view to manually trigger cron jobs
    """
    
    def post(self, request, job_type):
        """Manually trigger a specific job type"""
        try:
            # Validate job type
            if job_type not in ['fetching', 'syncing']:
                return Response(
                    {'error': f'Invalid job type: {job_type}'},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Check if any job is currently running
            if CronJobStatus.is_any_job_running():
                running_jobs = CronJobStatus.objects.filter(status='running')
                running_job_types = [job.job_type for job in running_jobs]
                return Response({
                    'error': 'Cannot trigger job while other jobs are running',
                    'running_jobs': running_job_types
                }, status=status.HTTP_409_CONFLICT)
            
            # Check if the specific job is enabled
            try:
                config = CronJobConfiguration.objects.get(job_type=job_type)
                if not config.enabled:
                    return Response({
                        'error': f'{job_type} job is disabled',
                        'message': 'Enable the job in configuration before triggering'
                    }, status=status.HTTP_400_BAD_REQUEST)
            except CronJobConfiguration.DoesNotExist:
                return Response(
                    {'error': f'Configuration for {job_type} not found'},
                    status=status.HTTP_404_NOT_FOUND
                )
            
            # Trigger the appropriate task
            if job_type == 'fetching':
                task_result = fetch_amazon_data_task.delay()
            elif job_type == 'syncing':
                task_result = sync_amazon_data_task.delay()
            
            return Response({
                'success': True,
                'message': f'{job_type} job triggered successfully',
                'task_id': task_result.id
            })
            
        except Exception as e:
            logger.error(f"Error triggering {job_type} job: {e}")
            return Response(
                {'error': f'Failed to trigger {job_type} job', 'details': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class CronJobLogsView(APIView):
    """
    API view to get cron job execution logs
    """
    
    def get(self, request, job_type=None):
        """Get execution logs for a specific job type or all jobs"""
        try:
            # Get query parameters
            limit = int(request.GET.get('limit', 50))
            offset = int(request.GET.get('offset', 0))
            
            # Filter logs
            if job_type:
                logs = CronJobLog.objects.filter(job_type=job_type)
            else:
                logs = CronJobLog.objects.all()
            
            # Apply pagination
            total_count = logs.count()
            logs = logs[offset:offset + limit]
            
            return Response({
                'logs': [{
                    'job_type': log.job_type,
                    'status': log.status,
                    'task_id': log.task_id,
                    'started_at': log.started_at.isoformat(),
                    'completed_at': log.completed_at.isoformat() if log.completed_at else None,
                    'duration': str(log.duration) if log.duration else None,
                    'records_processed': log.records_processed,
                    'error_message': log.error_message,
                    'details': log.details,
                } for log in logs],
                'pagination': {
                    'total': total_count,
                    'limit': limit,
                    'offset': offset,
                    'has_more': offset + limit < total_count
                }
            })
            
        except Exception as e:
            logger.error(f"Error getting job logs: {e}")
            return Response(
                {'error': 'Failed to get job logs', 'details': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class CronJobTaskStatusView(APIView):
    """
    API view to get the status of a specific Celery task
    """
    
    def get(self, request, task_id):
        """Get the status of a specific Celery task"""
        try:
            from celery.result import AsyncResult
            
            task_result = AsyncResult(task_id)
            
            response_data = {
                'task_id': task_id,
                'status': task_result.status,
                'result': task_result.result,
                'traceback': task_result.traceback,
                'date_done': task_result.date_done.isoformat() if task_result.date_done else None,
            }
            
            return Response(response_data)
            
        except Exception as e:
            logger.error(f"Error getting task status: {e}")
            return Response(
                {'error': 'Failed to get task status', 'details': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class CronJobStatsView(APIView):
    """
    API view to get cron job statistics and analytics
    """
    
    def get(self, request):
        """Get comprehensive statistics about cron job executions"""
        try:
            # Get date range (default to last 30 days)
            days = int(request.GET.get('days', 30))
            start_date = timezone.now() - timedelta(days=days)
            
            # Get logs within date range
            logs = CronJobLog.objects.filter(started_at__gte=start_date)
            
            # Calculate statistics
            stats = {
                'summary': {
                    'total_executions': logs.count(),
                    'successful_executions': logs.filter(status='completed').count(),
                    'failed_executions': logs.filter(status='failed').count(),
                    'avg_duration': self._calculate_avg_duration(logs.filter(status='completed')),
                },
                'by_job_type': {},
                'recent_executions': [{
                    'job_type': log.job_type,
                    'status': log.status,
                    'started_at': log.started_at.isoformat(),
                    'duration': str(log.duration) if log.duration else None,
                    'records_processed': log.records_processed,
                } for log in logs.order_by('-started_at')[:10]],
                'execution_timeline': self._get_execution_timeline(logs, days)
            }
            
            # Calculate stats by job type
            for job_type in ['fetching', 'syncing']:
                job_logs = logs.filter(job_type=job_type)
                stats['by_job_type'][job_type] = {
                    'total_executions': job_logs.count(),
                    'successful_executions': job_logs.filter(status='completed').count(),
                    'failed_executions': job_logs.filter(status='failed').count(),
                    'avg_duration': self._calculate_avg_duration(job_logs.filter(status='completed')),
                    'total_records_processed': sum(
                        log.records_processed or 0 for log in job_logs.filter(status='completed')
                    )
                }
            
            return Response(stats)
            
        except Exception as e:
            logger.error(f"Error getting job stats: {e}")
            return Response(
                {'error': 'Failed to get job stats', 'details': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def _calculate_avg_duration(self, logs):
        """Calculate average duration for completed logs"""
        durations = [log.duration for log in logs if log.duration]
        if durations:
            avg_seconds = sum(d.total_seconds() for d in durations) / len(durations)
            return str(timedelta(seconds=int(avg_seconds)))
        return '0:00:00'
    
    def _get_execution_timeline(self, logs, days):
        """Get execution timeline data for charts"""
        timeline = []
        for i in range(days):
            date = timezone.now().date() - timedelta(days=i)
            day_logs = logs.filter(started_at__date=date)
            timeline.append({
                'date': date.isoformat(),
                'total_executions': day_logs.count(),
                'successful_executions': day_logs.filter(status='completed').count(),
                'failed_executions': day_logs.filter(status='failed').count(),
            })
        return list(reversed(timeline)) 