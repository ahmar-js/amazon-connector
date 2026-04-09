from django.contrib import admin
from .models import Activities, MarketplaceLastRun, SCMLastRun, InventoryReportLog

# Register your models here.

@admin.register(Activities)
class ActivitiesAdmin(admin.ModelAdmin):
    list_display = ['activity_id', 'company_name', 'marketplace_id', 'marketplace_name', 'activity_type', 'status', 'date_from', 'date_to', 'activity_date']
    list_filter = ['status', 'activity_type', 'company_name', 'marketplace_id']
    search_fields = ['company_name', 'marketplace_id', 'marketplace_name', 'activity_id']


@admin.register(MarketplaceLastRun)
class MarketplaceLastRunAdmin(admin.ModelAdmin):
    list_display = ['company_name', 'marketplace_id', 'marketplace_name', 'last_run']
    list_filter = ['company_name', 'marketplace_id']
    search_fields = ['company_name', 'marketplace_id', 'marketplace_name']


@admin.register(SCMLastRun)
class SCMLastRunAdmin(admin.ModelAdmin):
    list_display = ['company_name', 'marketplace_id', 'marketplace_name', 'last_run', 'created_at', 'updated_at']
    list_filter = ['company_name', 'marketplace_id']
    search_fields = ['company_name', 'marketplace_id', 'marketplace_name']
    readonly_fields = ['created_at', 'updated_at']


@admin.register(InventoryReportLog)
class InventoryReportLogAdmin(admin.ModelAdmin):
    list_display = [
        'run_id', 'marketplace_code', 'report_date', 'status',
        'items_count', 'mssql_saved', 'azure_saved', 'triggered_by',
        'duration_seconds', 'created_at',
    ]
    list_filter = ['status', 'marketplace_code', 'triggered_by', 'mssql_saved', 'azure_saved', 'report_date']
    search_fields = ['run_id', 'marketplace_code', 'report_id', 'error_message']
    readonly_fields = ['created_at', 'updated_at']
    ordering = ['-created_at']