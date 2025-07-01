from django.urls import path
from .views import (
    ConnectAmazonStoreView, 
    RefreshAccessTokenView, 
    TestConnectionView, 
    ConnectionStatusView, 
    ManualRefreshTokenView, 
    FetchAmazonDataView,
    DownloadProcessedDataView,
    ProcessedDataStatusView,
    ActivitiesListView,
    ActivityDetailView,
    ActivitiesStatsView
)

from .cron_views import (
    CronJobStatusView,
    CronJobConfigurationView,
    CronJobTriggerView,
    CronJobLogsView,
    CronJobTaskStatusView,
    CronJobStatsView
)

urlpatterns = [
    path('connect/', ConnectAmazonStoreView.as_view(), name='connect_amazon_store'),
    path('test-connection/', TestConnectionView.as_view(), name='test_amazon_connection'),
    path('refresh-token/', RefreshAccessTokenView.as_view(), name='refresh_access_token'),
    path('connection-status/', ConnectionStatusView.as_view(), name='connection_status'),
    path('manual-refresh/', ManualRefreshTokenView.as_view(), name='manual_refresh_token'),
    path('fetch-data/', FetchAmazonDataView.as_view(), name='fetch_amazon_data'),
    path('download-processed/', DownloadProcessedDataView.as_view(), name='download_processed_data'),
    path('processed-status/', ProcessedDataStatusView.as_view(), name='processed_data_status'),
    # Activity management endpoints
    path('activities/', ActivitiesListView.as_view(), name='activities_list'),
    path('activities/<uuid:activity_id>/', ActivityDetailView.as_view(), name='activity_detail'),
    path('activities/stats/', ActivitiesStatsView.as_view(), name='activities_stats'),
    
    # Cron job management endpoints
    path('cron/status/', CronJobStatusView.as_view(), name='cron_job_status'),
    path('cron/config/', CronJobConfigurationView.as_view(), name='cron_job_config_list'),
    path('cron/config/<str:job_type>/', CronJobConfigurationView.as_view(), name='cron_job_config_detail'),
    path('cron/trigger/<str:job_type>/', CronJobTriggerView.as_view(), name='cron_job_trigger'),
    path('cron/logs/', CronJobLogsView.as_view(), name='cron_job_logs'),
    path('cron/logs/<str:job_type>/', CronJobLogsView.as_view(), name='cron_job_logs_by_type'),
    path('cron/task/<str:task_id>/', CronJobTaskStatusView.as_view(), name='cron_task_status'),
    path('cron/stats/', CronJobStatsView.as_view(), name='cron_job_stats'),
] 