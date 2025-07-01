from django.db import models
from django.core.validators import MinValueValidator
from django.utils import timezone
from django_celery_beat.models import PeriodicTask
import uuid

# Create your models here.
class Activities(models.Model):
    ACTIVITY_TYPE_CHOICES = [
        ('fetch', 'Data Fetch'),
        # ('export', 'Data Export'),
        ('sync', 'Data Sync'),
    ]
    
    ACTION_CHOICES = [
        ('manual', 'Manual'),
        # ('scheduled', 'Scheduled'),
        ('automatic', 'Automatic'),
    ]
    
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('in_progress', 'In Progress'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
        ('cancelled', 'Cancelled'),
    ]
    
    # Primary key with UUID for better security and uniqueness
    activity_id = models.UUIDField(
        primary_key=True, 
        default=uuid.uuid4, 
        editable=False,
        help_text="Unique identifier for this activity"
    )
    
    # Marketplace information
    marketplace_id = models.CharField(
        max_length=255,
        help_text="Amazon marketplace ID (e.g., ATVPDKIKX0DER for US)"
    )
    
    # Activity classification
    activity_type = models.CharField(
        max_length=50,
        choices=ACTIVITY_TYPE_CHOICES,
        default='fetch',
        help_text="Type of activity performed"
    )
    
    # Timing information
    activity_date = models.DateTimeField(
        auto_now_add=True,
        help_text="When this activity was initiated"
    )
    
    # Date range for the data operation
    date_from = models.DateField(
        help_text="Start date for data range"
    )
    
    date_to = models.DateField(
        help_text="End date for data range"
    )
    
    # Operation details
    action = models.CharField(
        max_length=50,
        choices=ACTION_CHOICES,
        default='manual',
        help_text="How this activity was triggered"
    )
    
    # Status tracking
    status = models.CharField(
        max_length=50,
        choices=STATUS_CHOICES,
        default='pending',
        help_text="Current status of the activity"
    )
    
    # Results tracking
    orders_fetched = models.PositiveIntegerField(
        default=0,
        validators=[MinValueValidator(0)],
        help_text="Number of orders successfully fetched"
    )
    
    items_fetched = models.PositiveIntegerField(
        default=0,
        validators=[MinValueValidator(0)],
        help_text="Number of order items successfully fetched"
    )
    
    # Duration tracking
    duration_seconds = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0)],
        help_text="Total time taken for the activity in seconds"
    )
    
    # User-friendly status message
    detail = models.TextField(
        blank=True,
        default='',
        help_text="User-friendly description of the activity status or any errors"
    )
    
    # Error tracking
    error_message = models.TextField(
        blank=True,
        null=True,
        help_text="Technical error message if the activity failed"
    )
    
    # Database save tracking
    database_saved = models.BooleanField(
        default=False,
        help_text="Whether the processed data was saved to databases (MSSQL and Azure)"
    )
    
    # Audit fields
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        verbose_name = "Activity"
        verbose_name_plural = "Activities"
        ordering = ['-activity_date']
        indexes = [
            models.Index(fields=['marketplace_id', '-activity_date']),
            models.Index(fields=['activity_type', '-activity_date']),
            models.Index(fields=['status', '-activity_date']),
        ]
        # Add unique constraint to prevent duplicate fetch operations
        constraints = [
            models.UniqueConstraint(
                fields=['marketplace_id', 'activity_type', 'date_from', 'date_to', 'status'],
                condition=models.Q(status='in_progress'),
                name='unique_in_progress_activity'
            )
        ]
    
    def __str__(self):
        return f'{self.get_activity_type_display()} - {self.marketplace_id} ({self.status})'
    
    @property
    def marketplace_name(self):
        """Get human-readable marketplace name"""
        marketplace_map = {
            'ATVPDKIKX0DER': 'United States',
            'A2EUQ1WTGCTBG2': 'Canada',
            'A1F83G8C2ARO7P': 'United Kingdom',
            'A1PA6795UKMFR9': 'Germany',
            'A13V1IB3VIYZZH': 'France',
            'APJ6JRA9NG5V4': 'Italy',
            'A1RKKUPIHCS9HS': 'Spain',
        }
        return marketplace_map.get(self.marketplace_id, self.marketplace_id)
    
    @property
    def duration_formatted(self):
        """Get formatted duration string"""
        if not self.duration_seconds:
            return "N/A"
        
        if self.duration_seconds < 60:
            return f"{self.duration_seconds:.1f}s"
        elif self.duration_seconds < 3600:
            minutes = self.duration_seconds / 60
            return f"{minutes:.1f}m"
        else:
            hours = self.duration_seconds / 3600
            return f"{hours:.1f}h"
    
    @property
    def total_records(self):
        """Get total number of records processed"""
        return self.orders_fetched + self.items_fetched


class CronJobStatus(models.Model):
    """Model to track the status of cron jobs and enforce mutual exclusion"""
    
    JOB_TYPES = [
        ('fetching', 'Fetching Job'),
        ('syncing', 'Syncing Job'),
    ]
    
    STATUS_CHOICES = [
        ('idle', 'Idle'),
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]
    
    job_type = models.CharField(max_length=20, choices=JOB_TYPES, unique=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='idle')
    last_run = models.DateTimeField(null=True, blank=True)
    next_run = models.DateTimeField(null=True, blank=True)
    last_duration = models.DurationField(null=True, blank=True)
    task_id = models.CharField(max_length=255, null=True, blank=True)
    error_message = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'cron_job_status'
        verbose_name = 'Cron Job Status'
        verbose_name_plural = 'Cron Job Statuses'
    
    def __str__(self):
        return f"{self.get_job_type_display()} - {self.get_status_display()}"
    
    @classmethod
    def is_any_job_running(cls):
        """Check if any job is currently running"""
        return cls.objects.filter(status='running').exists()
    
    @classmethod
    def is_job_running(cls, job_type):
        """Check if a specific job type is running"""
        try:
            job_status = cls.objects.get(job_type=job_type)
            return job_status.status == 'running'
        except cls.DoesNotExist:
            return False
    
    def mark_as_running(self, task_id):
        """Mark job as running"""
        self.status = 'running'
        self.task_id = task_id
        self.error_message = None
        self.save()
    
    def mark_as_completed(self, duration=None):
        """Mark job as completed"""
        self.status = 'completed'
        self.last_run = timezone.now()
        if duration:
            self.last_duration = duration
        self.task_id = None
        self.error_message = None
        self.save()
    
    def mark_as_failed(self, error_message):
        """Mark job as failed"""
        self.status = 'failed'
        self.last_run = timezone.now()
        self.error_message = error_message
        self.task_id = None
        self.save()


class CronJobConfiguration(models.Model):
    """Model to store user-configurable cron job settings"""
    
    JOB_TYPES = [
        ('fetching', 'Fetching Job'),
        ('syncing', 'Syncing Job'),
    ]
    
    job_type = models.CharField(max_length=20, choices=JOB_TYPES, unique=True)
    enabled = models.BooleanField(default=True)
    cron_expression = models.CharField(max_length=100, help_text="Cron expression (e.g., '0 0 */15 * *')")
    description = models.TextField(blank=True)
    
    # Fetching job specific fields
    date_range_days = models.IntegerField(default=15, help_text="Number of days to fetch (for fetching job)")
    
    # Syncing job specific fields
    sync_days_back = models.IntegerField(default=100, help_text="Number of days back to sync (for syncing job)")
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'cron_job_configuration'
        verbose_name = 'Cron Job Configuration'
        verbose_name_plural = 'Cron Job Configurations'
    
    def __str__(self):
        return f"{self.get_job_type_display()} - {'Enabled' if self.enabled else 'Disabled'}"
    
    @property
    def periodic_task(self):
        """Get the associated PeriodicTask"""
        try:
            return PeriodicTask.objects.get(name=f"{self.job_type}_job")
        except PeriodicTask.DoesNotExist:
            return None
    
    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        self.update_periodic_task()
    
    def update_periodic_task(self):
        """Update or create the associated PeriodicTask"""
        from django_celery_beat.models import CrontabSchedule
        
        # Parse cron expression
        try:
            cron_parts = self.cron_expression.strip().split()
            if len(cron_parts) != 5:
                raise ValueError("Invalid cron expression")
            
            minute, hour, day_of_month, month, day_of_week = cron_parts
            
            # Convert cron format to django-celery-beat format
            if day_of_week == '*':
                day_of_week = None
            if day_of_month == '*':
                day_of_month = None
            if month == '*':
                month = None
            if hour == '*':
                hour = None
            if minute == '*':
                minute = None
            
            # Create or get crontab schedule
            schedule, created = CrontabSchedule.objects.get_or_create(
                minute=minute or '*',
                hour=hour or '*',
                day_of_week=day_of_week or '*',
                day_of_month=day_of_month or '*',
                month_of_year=month or '*',
            )
            
            # Task name and function mapping
            task_mapping = {
                'fetching': 'api.tasks.fetch_amazon_data_task',
                'syncing': 'api.tasks.sync_amazon_data_task',
            }
            
            # Update or create periodic task
            periodic_task, created = PeriodicTask.objects.update_or_create(
                name=f"{self.job_type}_job",
                defaults={
                    'crontab': schedule,
                    'task': task_mapping[self.job_type],
                    'enabled': self.enabled,
                    'args': '[]',
                    'kwargs': '{}',
                }
            )
            
        except Exception as e:
            # Log error but don't fail the save
            print(f"Error updating periodic task for {self.job_type}: {e}")


class CronJobLog(models.Model):
    """Model to log cron job executions"""
    
    STATUS_CHOICES = [
        ('started', 'Started'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]
    
    job_type = models.CharField(max_length=20)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES)
    task_id = models.CharField(max_length=255)
    started_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    duration = models.DurationField(null=True, blank=True)
    records_processed = models.IntegerField(null=True, blank=True)
    error_message = models.TextField(null=True, blank=True)
    details = models.JSONField(default=dict, blank=True)
    
    class Meta:
        db_table = 'cron_job_log'
        verbose_name = 'Cron Job Log'
        verbose_name_plural = 'Cron Job Logs'
        ordering = ['-started_at']
    
    def __str__(self):
        return f"{self.job_type} - {self.status} - {self.started_at}"