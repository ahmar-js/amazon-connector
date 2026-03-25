from django.db import models
from django.core.validators import MinValueValidator
from django.utils import timezone
import uuid

MARKETPLACE_NAME_MAP = {
    'ATVPDKIKX0DER': 'United States',
    'A2EUQ1WTGCTBG2': 'Canada',
    'A1F83G8C2ARO7P': 'United Kingdom',
    'A1PA6795UKMFR9': 'Germany',
    'A13V1IB3VIYZZH': 'France',
    'APJ6JRA9NG5V4': 'Italy',
    'A1RKKUPIHCS9HS': 'Spain',
}


def resolve_marketplace_name(marketplace_id: str) -> str:
    return MARKETPLACE_NAME_MAP.get(marketplace_id, marketplace_id)

# Create your models here.
class Activities(models.Model):
    ACTIVITY_TYPE_CHOICES = [
        ('orders', 'Data Fetch'),
        ('reports', 'Reports Fetch'),
        # ('export', 'Data Export'),
        ('sync', 'Data Sync'),
    ]
    
    ACTION_CHOICES = [
        ('manual', 'Manual'),
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

    marketplace_name = models.CharField(
        max_length=100,
        default='',
        help_text="Human-readable marketplace name derived from marketplace_id"
    )

    company_name = models.CharField(
        max_length=100,
        default='B2Fitinss',
        help_text="Company/account name used for this fetch (e.g., B2Fitinss, RDX INC LTD)"
    )
    
    # Activity classification
    activity_type = models.CharField(
        max_length=50,
        choices=ACTIVITY_TYPE_CHOICES,
        default='fetch orders',
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
    
    # Separate database save tracking
    mssql_saved = models.BooleanField(
        default=False,
        help_text="Whether the processed data was saved to MSSQL database"
    )
    
    azure_saved = models.BooleanField(
        default=False,
        help_text="Whether the processed data was saved to Azure database"
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
            models.Index(fields=['company_name', '-activity_date']),
            models.Index(fields=['activity_type', '-activity_date']),
            models.Index(fields=['status', '-activity_date']),
        ]
        # Add unique constraint to prevent duplicate fetch operations
        constraints = [
            models.UniqueConstraint(
                fields=['company_name', 'marketplace_id', 'activity_type', 'date_from', 'date_to', 'status'],
                condition=models.Q(status='in_progress'),
                name='unique_in_progress_activity'
            )
        ]
    
    def __str__(self):
        return f'{self.get_activity_type_display()} - {self.marketplace_id} ({self.status})'
    
    def save(self, *args, **kwargs):
        self.marketplace_name = resolve_marketplace_name(self.marketplace_id)
        super().save(*args, **kwargs)
    
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


class MarketplaceLastRun(models.Model):
    """Model to track the last run time for each marketplace"""
    company_name = models.CharField(
        max_length=100,
        default='B2Fitinss',
        help_text="Company/account name used for this marketplace tracking row"
    )
    marketplace_id = models.CharField(max_length=255)
    marketplace_name = models.CharField(
        max_length=100,
        default='',
        help_text="Human-readable marketplace name derived from marketplace_id"
    )
    last_run = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'marketplace_last_run'
        verbose_name = 'Marketplace Last Run'
        verbose_name_plural = 'Marketplace Last Runs'
        constraints = [
            models.UniqueConstraint(
                fields=['company_name', 'marketplace_id'],
                name='unique_marketplace_company_last_run'
            )
        ]

    def __str__(self):
        return f"{self.company_name} - {self.marketplace_id}"

    def save(self, *args, **kwargs):
        self.marketplace_name = resolve_marketplace_name(self.marketplace_id)
        super().save(*args, **kwargs)


class SCMLastRun(models.Model):
    """Model to track the last run time for SCM data fetching per marketplace"""
    company_name = models.CharField(
        max_length=100,
        default='B2Fitinss',
        help_text="Company/account name used for this SCM tracking row"
    )
    marketplace_id = models.CharField(
        max_length=255,
        help_text="Amazon marketplace ID (e.g., ATVPDKIKX0DER for US)"
    )
    marketplace_name = models.CharField(
        max_length=100,
        default='',
        help_text="Human-readable marketplace name derived from marketplace_id"
    )
    last_run = models.DateTimeField(
        null=True, 
        blank=True,
        help_text="Last successful fetch date/time for this marketplace"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'scm_last_run'
        verbose_name = 'SCM Last Run'
        verbose_name_plural = 'SCM Last Runs'
        constraints = [
            models.UniqueConstraint(
                fields=['company_name', 'marketplace_id'],
                name='unique_scm_marketplace_company_last_run'
            )
        ]
    
    def __str__(self):
        return f"SCM {self.company_name} {self.marketplace_id} - Last run: {self.last_run}"

    def save(self, *args, **kwargs):
        self.marketplace_name = resolve_marketplace_name(self.marketplace_id)
        super().save(*args, **kwargs)