from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import (
    ConnectAmazonStoreView, 
    RefreshAccessTokenView, 
    TestConnectionView, 
    ConnectionStatusView, 
    ManualRefreshTokenView, 
    FetchAmazonDataView,
    FetchMissingOrderItemsView,
    DownloadProcessedDataView,
    ProcessedDataStatusView,
    ActivitiesListView,
    ActivityDetailView,
    ActivitiesStatsView,
    # TestFetchOrderItemsView
)
from .inventory_views import (
    FetchInventoryReportView,
    CreateReportScheduleView,
    GetReportSchedulesView,
    CancelReportScheduleView,
)

# Create router for ViewSets
router = DefaultRouter()

urlpatterns = [
    # Amazon Store Connection endpoints
    path('connect/', ConnectAmazonStoreView.as_view(), name='connect_amazon_store'),
    path('test-connection/', TestConnectionView.as_view(), name='test_amazon_connection'),
    path('refresh-token/', RefreshAccessTokenView.as_view(), name='refresh_access_token'),
    path('connection-status/', ConnectionStatusView.as_view(), name='connection_status'),
    path('manual-refresh/', ManualRefreshTokenView.as_view(), name='manual_refresh_token'),
    path('fetch-data/', FetchAmazonDataView.as_view(), name='fetch_amazon_data'),
    path('fetch-missing-items/', FetchMissingOrderItemsView.as_view(), name='fetch_missing_order_items'),
    # path('test-fetch-order-items/', TestFetchOrderItemsView.as_view(), name='test_fetch_order_items'),
    path('download-processed/', DownloadProcessedDataView.as_view(), name='download_processed_data'),
    path('processed-status/', ProcessedDataStatusView.as_view(), name='processed_data_status'),

    # Inventory report endpoints
    path('inventory/reports/', FetchInventoryReportView.as_view(), name='fetch_inventory_reports'),
    path('inventory/report-schedules/', CreateReportScheduleView.as_view(), name='create_report_schedule'),
    path('inventory/report-schedules/list/', GetReportSchedulesView.as_view(), name='get_report_schedules'),
    path('inventory/report-schedules/<str:report_schedule_id>/', CancelReportScheduleView.as_view(), name='cancel_report_schedule'),
    
    # Activity management endpoints
    path('activities/', ActivitiesListView.as_view(), name='activities_list'),
    path('activities/<uuid:activity_id>/', ActivityDetailView.as_view(), name='activity_detail'),
    path('activities/stats/', ActivitiesStatsView.as_view(), name='activities_stats'),
    
    # Include router URLs
    path('', include(router.urls)),
] 