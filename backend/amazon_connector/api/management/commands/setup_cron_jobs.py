from django.core.management.base import BaseCommand
from django.utils import timezone
from api.models import CronJobStatus, CronJobConfiguration
from django_celery_beat.models import PeriodicTask, CrontabSchedule


class Command(BaseCommand):
    help = 'Set up initial cron job configurations and schedules'

    def add_arguments(self, parser):
        parser.add_argument(
            '--reset',
            action='store_true',
            help='Reset existing configurations',
        )

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Setting up cron jobs...'))
        
        # Reset if requested
        if options['reset']:
            self.stdout.write('Resetting existing configurations...')
            CronJobConfiguration.objects.all().delete()
            CronJobStatus.objects.all().delete()
            PeriodicTask.objects.filter(name__in=['fetching_job', 'syncing_job']).delete()
        
        # Create or update fetching job configuration
        fetching_config, created = CronJobConfiguration.objects.get_or_create(
            job_type='fetching',
            defaults={
                'enabled': True,
                'cron_expression': '0 0 */15 * *',  # Every 15 days at midnight
                'description': 'Fetch order/item data from Amazon SP-API every 15 days',
                'date_range_days': 15,
                'sync_days_back': 100,  # Not used for fetching but required by model
            }
        )
        
        if created:
            self.stdout.write(
                self.style.SUCCESS('Created fetching job configuration')
            )
        else:
            self.stdout.write('Fetching job configuration already exists')
        
        # Create or update syncing job configuration
        syncing_config, created = CronJobConfiguration.objects.get_or_create(
            job_type='syncing',
            defaults={
                'enabled': True,
                'cron_expression': '0 0 */7 * *',  # Every 7 days at midnight
                'description': 'Sync last 100 days of fetched data into internal database every 7 days',
                'date_range_days': 15,  # Not used for syncing but required by model
                'sync_days_back': 100,
            }
        )
        
        if created:
            self.stdout.write(
                self.style.SUCCESS('Created syncing job configuration')
            )
        else:
            self.stdout.write('Syncing job configuration already exists')
        
        # Create or update job statuses
        fetching_status, created = CronJobStatus.objects.get_or_create(
            job_type='fetching',
            defaults={'status': 'idle'}
        )
        
        if created:
            self.stdout.write(
                self.style.SUCCESS('Created fetching job status')
            )
        
        syncing_status, created = CronJobStatus.objects.get_or_create(
            job_type='syncing',
            defaults={'status': 'idle'}
        )
        
        if created:
            self.stdout.write(
                self.style.SUCCESS('Created syncing job status')
            )
        
        # Create periodic tasks
        self._create_periodic_task('fetching', fetching_config)
        self._create_periodic_task('syncing', syncing_config)
        
        # Create cleanup task for old logs
        self._create_cleanup_task()
        
        self.stdout.write(
            self.style.SUCCESS('Successfully set up cron jobs!')
        )
        
        # Display summary
        self.stdout.write('\n' + '='*50)
        self.stdout.write(self.style.SUCCESS('CRON JOBS SUMMARY'))
        self.stdout.write('='*50)
        
        self.stdout.write(f'Fetching Job:')
        self.stdout.write(f'  - Enabled: {fetching_config.enabled}')
        self.stdout.write(f'  - Schedule: {fetching_config.cron_expression}')
        self.stdout.write(f'  - Description: {fetching_config.description}')
        self.stdout.write(f'  - Date Range: {fetching_config.date_range_days} days')
        
        self.stdout.write(f'\nSyncing Job:')
        self.stdout.write(f'  - Enabled: {syncing_config.enabled}')
        self.stdout.write(f'  - Schedule: {syncing_config.cron_expression}')
        self.stdout.write(f'  - Description: {syncing_config.description}')
        self.stdout.write(f'  - Sync Range: {syncing_config.sync_days_back} days back')
        
        self.stdout.write('\n' + '='*50)
        self.stdout.write(self.style.WARNING('NEXT STEPS:'))
        self.stdout.write('1. Start Redis server: redis-server')
        self.stdout.write('2. Start Celery worker: celery -A amazon_connector worker --loglevel=info')
        self.stdout.write('3. Start Celery beat: celery -A amazon_connector beat --loglevel=info --scheduler django_celery_beat.schedulers:DatabaseScheduler')
        self.stdout.write('4. Access the cron job management UI in your frontend')
        self.stdout.write('='*50)

    def _create_periodic_task(self, job_type, config):
        """Create or update a periodic task"""
        try:
            # Parse cron expression
            cron_parts = config.cron_expression.strip().split()
            if len(cron_parts) != 5:
                self.stdout.write(
                    self.style.ERROR(f'Invalid cron expression for {job_type}: {config.cron_expression}')
                )
                return
            
            minute, hour, day_of_month, month, day_of_week = cron_parts
            
            # Convert to django-celery-beat format
            schedule, created = CrontabSchedule.objects.get_or_create(
                minute=minute,
                hour=hour,
                day_of_week=day_of_week if day_of_week != '*' else '',
                day_of_month=day_of_month if day_of_month != '*' else '',
                month_of_year=month if month != '*' else '',
            )
            
            # Task mapping
            task_mapping = {
                'fetching': 'api.tasks.fetch_amazon_data_task',
                'syncing': 'api.tasks.sync_amazon_data_task',
            }
            
            # Create or update periodic task
            periodic_task, created = PeriodicTask.objects.update_or_create(
                name=f"{job_type}_job",
                defaults={
                    'crontab': schedule,
                    'task': task_mapping[job_type],
                    'enabled': config.enabled,
                    'args': '[]',
                    'kwargs': '{}',
                }
            )
            
            if created:
                self.stdout.write(
                    self.style.SUCCESS(f'Created periodic task for {job_type} job')
                )
            else:
                self.stdout.write(f'Updated periodic task for {job_type} job')
                
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Error creating periodic task for {job_type}: {e}')
            )

    def _create_cleanup_task(self):
        """Create a cleanup task for old logs"""
        try:
            # Create schedule for daily cleanup at 2 AM
            schedule, created = CrontabSchedule.objects.get_or_create(
                minute='0',
                hour='2',
                day_of_week='',
                day_of_month='',
                month_of_year='',
            )
            
            # Create cleanup task
            cleanup_task, created = PeriodicTask.objects.update_or_create(
                name='cleanup_old_logs',
                defaults={
                    'crontab': schedule,
                    'task': 'api.tasks.cleanup_old_logs',
                    'enabled': True,
                    'args': '[]',
                    'kwargs': '{}',
                }
            )
            
            if created:
                self.stdout.write(
                    self.style.SUCCESS('Created cleanup task for old logs')
                )
            else:
                self.stdout.write('Updated cleanup task for old logs')
                
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Error creating cleanup task: {e}')
            ) 