from django.contrib import admin
from .models import Activities, MarketplaceLastRun, SCMLastRun

# Register your models here.

@admin.register(Activities)
class ActivitiesAdmin(admin.ModelAdmin):
    list_display = ['activity_id', 'company_name', 'marketplace_id', 'activity_type', 'status', 'date_from', 'date_to', 'activity_date']
    list_filter = ['status', 'activity_type', 'company_name', 'marketplace_id']
    search_fields = ['company_name', 'marketplace_id', 'activity_id']


@admin.register(MarketplaceLastRun)
class MarketplaceLastRunAdmin(admin.ModelAdmin):
    list_display = ['company_name', 'marketplace_id', 'last_run']
    list_filter = ['company_name', 'marketplace_id']
    search_fields = ['company_name', 'marketplace_id']


@admin.register(SCMLastRun)
class SCMLastRunAdmin(admin.ModelAdmin):
    list_display = ['company_name', 'marketplace_id', 'last_run', 'created_at', 'updated_at']
    list_filter = ['company_name', 'marketplace_id']
    search_fields = ['company_name', 'marketplace_id']
    readonly_fields = ['created_at', 'updated_at']