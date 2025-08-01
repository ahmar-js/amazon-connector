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
    
    # Activity management endpoints
    path('activities/', ActivitiesListView.as_view(), name='activities_list'),
    path('activities/<uuid:activity_id>/', ActivityDetailView.as_view(), name='activity_detail'),
    path('activities/stats/', ActivitiesStatsView.as_view(), name='activities_stats'),
    
    # Include router URLs
    path('', include(router.urls)),
] 