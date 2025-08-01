import axios, { type AxiosInstance, type AxiosResponse, type AxiosError, type InternalAxiosRequestConfig } from 'axios'

// API Configuration
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000/api'
const API_TIMEOUT = 86400000 // 24 hours (86,400,000 milliseconds) for large data fetching operations

// Response interfaces
export interface ApiResponse<T = any> {
  success: boolean
  message?: string
  data?: T
  error?: string
  details?: string
  status_code?: number
}

export interface AmazonConnectionRequest {
  appId: string
  clientSecret: string
  refreshToken: string
}

export interface TokenData {
  access_token: string
  token_type: string
  expires_in: number
  expires_at: string
  refresh_token: string
  app_id: string
  connected_at?: string
  refreshed_at?: string
}

export interface RefreshTokenRequest {
  appId: string
  clientSecret: string
  refreshToken: string
}

export interface TestConnectionRequest {
  appId: string
  clientSecret: string
  refreshToken: string
}

export interface TestConnectionResponse {
  app_id: string
  token_type: string
  expires_in: number
  tested_at: string
}

export interface ConnectionStatusResponse {
  isConnected: boolean
  app_id?: string
  token_type?: string
  expires_in?: number
  expires_at?: string
  connected_at?: string
  last_refreshed?: string
  is_expired?: boolean
  has_refresh_token?: boolean
  message?: string
}

// Amazon Data Fetch interfaces
export interface FetchAmazonDataRequest {
  marketplace_id: string
  start_date: string
  end_date: string
  max_orders?: number // Optional - if not provided, fetches all orders
  auto_save?: boolean // Optional - if true, automatically saves to database
}

// Raw Amazon data interfaces - these now represent the actual Amazon API response structure
export interface AmazonOrder {
  // Use any to allow all raw Amazon fields without restriction
  [key: string]: any
  // Common Amazon order fields (but there may be many more)
  AmazonOrderId?: string
  OrderStatus?: string
  PurchaseDate?: string
  LastUpdateDate?: string
  OrderTotal?: {
    CurrencyCode?: string
    Amount?: string
  }
  NumberOfItemsShipped?: number
  NumberOfItemsUnshipped?: number
  PaymentMethod?: string
  MarketplaceId?: string
  ShipmentServiceLevelCategory?: string
  OrderType?: string
  EarliestShipDate?: string
  LatestShipDate?: string
  EarliestDeliveryDate?: string
  LatestDeliveryDate?: string
  IsBusinessOrder?: boolean
  IsPrime?: boolean
  IsPremiumOrder?: boolean
  IsGlobalExpressEnabled?: boolean
  ReplacedOrderId?: string
  IsReplacementOrder?: boolean
  PromiseResponseDueDate?: string
  IsEstimatedShipDateSet?: boolean
  IsSoldByAB?: boolean
  IsIBA?: boolean
  DefaultShipFromLocationAddress?: any
  BuyerInvoicePreference?: string
  BuyerTaxInformation?: any
  FulfillmentInstruction?: any
  IsISPU?: boolean
  IsAccessPointOrder?: boolean
  MarketplaceTaxInfo?: any
  SellerDisplayName?: string
  ShippingAddress?: any
  BuyerInfo?: any
  AutomatedShippingSettings?: any
  HasRegulatedItems?: boolean
  ElectronicInvoiceStatus?: string
  SalesChannel?: string
  FulfillmentChannel?: string
  items?: AmazonOrderItem[] // Nested items for convenience
}

export interface AmazonOrderItem {
  // Use any to allow all raw Amazon fields without restriction
  [key: string]: any
  // Common Amazon order item fields (but there may be many more)
  OrderItemId?: string
  order_id?: string // Added by backend for reference
  ASIN?: string
  SellerSKU?: string
  BuyerProductIdentifier?: string
  Title?: string
  QuantityOrdered?: number
  QuantityShipped?: number
  ProductInfo?: any
  PointsGranted?: any
  ItemPrice?: {
    CurrencyCode?: string
    Amount?: string
  }
  ShippingPrice?: {
    CurrencyCode?: string
    Amount?: string
  }
  ItemTax?: {
    CurrencyCode?: string
    Amount?: string
  }
  ShippingTax?: {
    CurrencyCode?: string
    Amount?: string
  }
  ShippingDiscount?: {
    CurrencyCode?: string
    Amount?: string
  }
  ShippingDiscountTax?: {
    CurrencyCode?: string
    Amount?: string
  }
  PromotionDiscount?: {
    CurrencyCode?: string
    Amount?: string
  }
  PromotionDiscountTax?: {
    CurrencyCode?: string
    Amount?: string
  }
  PromotionIds?: string[]
  CODFee?: {
    CurrencyCode?: string
    Amount?: string
  }
  CODFeeDiscount?: {
    CurrencyCode?: string
    Amount?: string
  }
  IsGift?: boolean
  ConditionNote?: string
  ConditionId?: string
  ConditionSubtypeId?: string
  ScheduledDeliveryStartDate?: string
  ScheduledDeliveryEndDate?: string
  PriceDesignation?: string
  TaxCollection?: any
  SerialNumberRequired?: boolean
  IsTransparency?: boolean
  IossNumber?: string
  StoreChainStoreId?: string
  DeemedResellerCategory?: string
  BuyerInfo?: any
  BuyerRequestedCancel?: any
  ItemApprovalContext?: any
  SerialNumbers?: string[]
}

export interface FetchDataMetadata {
  total_orders_fetched: number
  total_items_fetched: number
  marketplace_id: string
  marketplace_name: string
  date_range: {
    start_date: string
    end_date: string
  }
  fetch_completed_at: string
  performance: {
    total_time_seconds: number
    orders_fetch_time_seconds: number
    items_fetch_time_seconds: number
    average_time_per_order: number
  }
}

export interface FetchAmazonDataResponse {
  orders: AmazonOrder[]
  order_items: AmazonOrderItem[]
  processed_data?: {
    mssql_records: number
    azure_records: number
    cache_key: string
    available_for_download: boolean
    database_save?: {
      attempted: boolean
      success: boolean | null
      records_saved: number
      details?: any
    }
  }
  processing_error?: string
  metadata: FetchDataMetadata
}

// Processed data interfaces
export interface ProcessedDataFile {
  filename: string
  size: number
  created: string
  type: 'MSSQL' | 'AZURE'
}

export interface ProcessedDataStatusResponse {
  status: 'available' | 'no_data'
  stats: {
    total_files: number
    mssql_files: number
    azure_files: number
    total_size: number
    total_size_mb: number
  }
  latest_files: ProcessedDataFile[]
}

export interface DownloadProcessedDataRequest {
  cache_key: string
  data_type: 'mssql' | 'azure'
}

// Activity interfaces
export interface Activity {
  activity_id: string
  marketplace_id: string
  marketplace_name: string
  activity_type: 'fetch' | 'export' | 'sync'
  activity_type_display: string
  status: 'pending' | 'in_progress' | 'completed' | 'failed' | 'cancelled'
  status_display: string
  action: 'manual' | 'scheduled' | 'automatic'
  action_display: string
  activity_date: string
  date_from: string
  date_to: string
  orders_fetched: number
  items_fetched: number
  total_records: number
  duration_seconds?: number
  duration_formatted: string
  detail: string
  error_message?: string
  database_saved: boolean
  mssql_saved: boolean
  azure_saved: boolean
  created_at: string
  updated_at: string
}

export interface ActivitiesListRequest {
  page?: number
  page_size?: number
  marketplace_id?: string
  status?: string
  activity_type?: string
  search?: string
  date_from?: string
  date_to?: string
}

export interface ActivitiesListResponse {
  activities: Activity[]
  pagination: {
    current_page: number
    total_pages: number
    total_items: number
    page_size: number
    has_next: boolean
    has_previous: boolean
  }
}

export interface ActivityStatsRequest {
  days?: number
  marketplace_id?: string
}

export interface ActivityStats {
  period: {
    days: number
    start_date: string
    end_date: string
    marketplace_id?: string
  }
  summary: {
    total_activities: number
    completed_activities: number
    failed_activities: number
    in_progress_activities: number
    success_rate: number
    total_orders_processed: number
    total_items_processed: number
    average_duration_seconds?: number
    average_duration_formatted?: string
  }
  breakdowns: {
    by_status: {
      [key: string]: {
        display: string
        count: number
        percentage: number
      }
    }
    by_marketplace: {
      [key: string]: {
        name: string
        count: number
        percentage: number
      }
    }
  }
  recent_activities: Array<{
    activity_id: string
    marketplace_name: string
    status: string
    status_display: string
    activity_date: string
    orders_fetched: number
    items_fetched: number
    duration_formatted: string
    detail: string
  }>
}

// Custom error class for API errors
export class ApiError extends Error {
  public status: number
  public details?: string

  constructor(message: string, status: number = 500, details?: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.details = details
  }
}

// Token Manager Class
class TokenManager {
  private static instance: TokenManager
  private tokenData: TokenData | null = null
  private refreshTimer: NodeJS.Timeout | null = null
  private isRefreshing = false
  private refreshPromise: Promise<TokenData> | null = null
  private periodicCheckTimer: NodeJS.Timeout | null = null
  private eventListeners: { [event: string]: Function[] } = {}

  private constructor() {
    this.loadTokenFromStorage()
    
    // Start periodic token validity checks
    this.startPeriodicTokenCheck()
    
    console.log('🔧 TokenManager initialized')
  }

  public static getInstance(): TokenManager {
    if (!TokenManager.instance) {
      TokenManager.instance = new TokenManager()
    }
    return TokenManager.instance
  }

  // Save token data and schedule refresh
  public setTokenData(tokenData: TokenData): void {
    const wasConnected = this.hasActiveConnection()
    this.tokenData = tokenData
    this.saveTokenToStorage(tokenData)
    this.scheduleTokenRefresh()
    
    const isNowConnected = this.hasActiveConnection()
    console.log('🔐 Token data updated and saved')
    
    // Emit events for status changes
    if (!wasConnected && isNowConnected) {
      console.log('📡 Connection established')
      this.emitEvent('connectionEstablished', tokenData)
    } else if (wasConnected && isNowConnected) {
      console.log('🔄 Token refreshed successfully')
      this.emitEvent('tokenRefreshed', tokenData)
    }
    
    this.emitEvent('tokenUpdated', tokenData)
  }

  // Check if token is still valid
  public isTokenValid(): boolean {
    if (!this.tokenData || !this.tokenData.access_token) {
      return false
    }

    if (!this.tokenData.expires_at) {
      // If no expiry time, assume token is invalid for safety
      console.warn('⚠️ Token has no expiry time, treating as invalid')
      return false
    }

    try {
      const expiryTime = new Date(this.tokenData.expires_at).getTime()
      const currentTime = Date.now()
      const timeUntilExpiry = expiryTime - currentTime
      
      // Consider token invalid if it expires within the next 5 minutes
      // This provides a buffer for API calls that might take some time
      const bufferTime = 5 * 60 * 1000 // 5 minutes in milliseconds
      const isValid = timeUntilExpiry > bufferTime
      
      if (!isValid) {
        console.log(`⏰ Token expires in ${Math.round(timeUntilExpiry / 1000 / 60)} minutes, treating as invalid`)
      }
      
      return isValid
    } catch (error) {
      console.error('❌ Error checking token validity:', error)
      return false
    }
  }

  // Get current access token
  public getAccessToken(): string | null {
    return this.tokenData?.access_token || null
  }

  // Get all token data
  public getTokenData(): TokenData | null {
    return this.tokenData
  }

  // Check if we can perform automatic refresh
  public canAutoRefresh(): boolean {
    // We can auto-refresh if we have token data with refresh_token
    return !!(this.tokenData && this.tokenData.refresh_token)
  }

  // Get fresh access token (refresh if needed)
  public async getFreshAccessToken(): Promise<string | null> {
    // First check if current token is valid
    if (this.isTokenValid()) {
      console.log('✅ Current access token is valid')
      return this.getAccessToken()
    }

    // Check if we can auto-refresh
    if (!this.canAutoRefresh()) {
      console.warn('⚠️ Cannot auto-refresh token: missing refresh token or token data')
      return null
    }

    // If already refreshing, wait for it
    if (this.isRefreshing && this.refreshPromise) {
      try {
        console.log('⏳ Token refresh already in progress, waiting...')
        const newTokenData = await this.refreshPromise
        return newTokenData.access_token
      } catch (error) {
        console.error('❌ Failed to wait for token refresh:', error)
        return null
      }
    }

    // Start new refresh with retry logic
    let retryCount = 0
    const maxRetries = 3
    
    while (retryCount < maxRetries) {
      try {
        console.log(`🔄 Token invalid, attempting refresh (attempt ${retryCount + 1}/${maxRetries})...`)
        const newTokenData = await this.refreshAccessToken()
        console.log('✅ Token refreshed successfully')
        return newTokenData.access_token
      } catch (error) {
        retryCount++
        console.error(`❌ Token refresh attempt ${retryCount} failed:`, error)
        
        if (retryCount >= maxRetries) {
          console.error('❌ All token refresh attempts failed')
          return null
        }
        
        // Wait before retrying (exponential backoff)
        const waitTime = Math.pow(2, retryCount) * 1000 // 2s, 4s, 8s
        console.log(`⏳ Waiting ${waitTime}ms before retry...`)
        await new Promise(resolve => setTimeout(resolve, waitTime))
      }
    }
    
    return null
  }

  // Refresh access token using refresh token
  public async refreshAccessToken(): Promise<TokenData> {
    if (this.isRefreshing && this.refreshPromise) {
      return this.refreshPromise
    }

    if (!this.tokenData) {
      throw new ApiError('No token data available for refresh')
    }

    this.isRefreshing = true
    this.refreshPromise = this.performTokenRefresh()

    try {
      const newTokenData = await this.refreshPromise
      this.setTokenData(newTokenData)
      return newTokenData
    } finally {
      this.isRefreshing = false
      this.refreshPromise = null
    }
  }

  private async performTokenRefresh(): Promise<TokenData> {
    if (!this.tokenData) {
      throw new ApiError('No token data available for refresh')
    }

    if (!this.tokenData.refresh_token) {
      throw new ApiError('No refresh token available')
    }

    console.log('🔄 Refreshing access token automatically using backend...')

    try {
      // Use the backend's manual refresh endpoint which uses stored credentials
      const response = await apiClient.post<ApiResponse<TokenData>>('/manual-refresh/')

      if (!response.data.success || !response.data.data) {
        console.error('❌ Backend token refresh failed:', response.data.error)
        throw new ApiError(
          response.data.error || 'Token refresh failed',
          response.status,
          response.data.details
        )
      }

      console.log('✅ Token refreshed successfully via backend')
      
      // Validate the returned token data
      const newTokenData = response.data.data
      if (!newTokenData.access_token || !newTokenData.expires_at) {
        console.error('❌ Invalid token data received from backend')
        throw new ApiError('Invalid token data received from backend')
      }

      return newTokenData
    } catch (error) {
      console.error('❌ Token refresh error:', error)
      
      // If it's a network error or 500 error, we might want to retry
      if (error instanceof ApiError && (error.status >= 500 || error.status === 0)) {
        console.log('🔄 Network/server error during refresh, will retry later')
        throw error
      }
      
      // For authentication errors (401, 403), the refresh token might be invalid
      if (error instanceof ApiError && (error.status === 401 || error.status === 403)) {
        console.error('🚨 Authentication error during refresh - refresh token may be invalid')
        // Clear the token data to force re-authentication
        this.clearTokenData()
        throw new ApiError(
          'Authentication failed - please reconnect your Amazon account',
          401,
          'Your refresh token has expired or is invalid. Please reconnect to continue.'
        )
      }
      
      throw error
    }
  }

  private scheduleTokenRefresh(): void {
    if (this.refreshTimer) {
      clearTimeout(this.refreshTimer)
    }

    if (!this.tokenData || !this.tokenData.expires_at || !this.canAutoRefresh()) {
      console.log('⏰ Skipping token refresh scheduling - missing requirements')
      return
    }

    const expiryTime = new Date(this.tokenData.expires_at).getTime()
    const currentTime = Date.now()
    const refreshTime = expiryTime - currentTime - (10 * 60 * 1000) // Refresh 10 minutes before expiry

    if (refreshTime > 0) {
      this.refreshTimer = setTimeout(async () => {
        try {
          console.log('🔄 Auto-refreshing access token...')
          await this.refreshAccessToken()
          console.log('✅ Access token auto-refreshed successfully')
          // Re-schedule the next refresh after successful refresh
          this.scheduleTokenRefresh()
        } catch (error) {
          console.error('❌ Auto token refresh failed:', error)
          // Try again in 5 minutes if refresh failed
          this.refreshTimer = setTimeout(() => this.scheduleTokenRefresh(), 5 * 60 * 1000)
        }
      }, refreshTime)

      console.log(`⏰ Token refresh scheduled in ${Math.round(refreshTime / 1000 / 60)} minutes`)
    } else {
      // Token is already expired or will expire soon, refresh immediately
      console.log('⚠️ Token expired or expiring soon, refreshing immediately...')
      this.performImmediateRefresh()
    }
  }

  private async performImmediateRefresh(): Promise<void> {
    try {
      console.log('🔄 Performing immediate token refresh...')
      await this.refreshAccessToken()
      console.log('✅ Immediate token refresh successful')
      // Schedule the next refresh
      this.scheduleTokenRefresh()
    } catch (error) {
      console.error('❌ Immediate token refresh failed:', error)
      // Try again in 1 minute
      this.refreshTimer = setTimeout(() => this.performImmediateRefresh(), 60 * 1000)
    }
  }

  private startPeriodicTokenCheck(): void {
    if (this.periodicCheckTimer) {
      clearInterval(this.periodicCheckTimer)
    }

    this.periodicCheckTimer = setInterval(() => {
      if (this.tokenData && !this.isRefreshing) {
        // Check if token needs refresh
        const expiryTime = new Date(this.tokenData.expires_at).getTime()
        const currentTime = Date.now()
        const timeUntilExpiry = expiryTime - currentTime

        // If token expires in less than 15 minutes, attempt refresh
        if (timeUntilExpiry < 15 * 60 * 1000 && timeUntilExpiry > 0) {
          console.log('⏰ Periodic check: Token expiring soon, triggering refresh...')
          this.performImmediateRefresh()
        } else if (timeUntilExpiry <= 0) {
          console.log('⏰ Periodic check: Token expired, triggering immediate refresh...')
          this.performImmediateRefresh()
        }
      }
    }, 5 * 60 * 1000) // Check every 5 minutes

    console.log('⏰ Started periodic token validity check (every 5 minutes)')
  }

  private saveTokenToStorage(tokenData: TokenData): void {
    try {
      // Store both metadata and tokens securely
      const storageData = {
        access_token: tokenData.access_token,
        expires_at: tokenData.expires_at,
        expires_in: tokenData.expires_in,
        token_type: tokenData.token_type,
        app_id: tokenData.app_id,
        refresh_token: tokenData.refresh_token,
        connected_at: tokenData.connected_at,
        refreshed_at: tokenData.refreshed_at,
      }
      localStorage.setItem('amazon_token_data', JSON.stringify(storageData))
      console.log('🔐 Token data saved to storage')
    } catch (error) {
      console.warn('Failed to save token data to storage:', error)
    }
  }

  private loadTokenFromStorage(): void {
    try {
      const stored = localStorage.getItem('amazon_token_data')
      if (stored) {
        const parsedData = JSON.parse(stored) as TokenData
        
        // Validate the loaded token data
        if (!parsedData.access_token || !parsedData.refresh_token || !parsedData.expires_at) {
          console.warn('⚠️ Invalid token data in storage, clearing...')
          localStorage.removeItem('amazon_token_data')
          return
        }
        
        // Restore the token data
        this.tokenData = parsedData
        
        console.log('🔄 Restored token data from storage')
        console.log(`📊 Token status: valid=${this.isTokenValid()}, expires_at=${this.tokenData.expires_at}`)
        
        // Schedule refresh if token is still valid or can be refreshed
        if (this.tokenData) {
          // Check if we have refresh capability
          if (this.tokenData.refresh_token) {
            console.log('✅ Refresh token available for auto-refresh')
            this.scheduleTokenRefresh()
          } else {
            console.warn('⚠️ No refresh token available - manual reconnection will be required')
          }
        }
      } else {
        console.log('📭 No token data found in storage')
      }
    } catch (error) {
      console.warn('❌ Failed to load token data from storage:', error)
      // Clear potentially corrupted data
      try {
        localStorage.removeItem('amazon_token_data')
      } catch (clearError) {
        console.warn('Failed to clear corrupted token data:', clearError)
      }
    }
  }

  public clearTokenData(): void {
    this.tokenData = null
    if (this.refreshTimer) {
      clearTimeout(this.refreshTimer)
      this.refreshTimer = null
    }
    if (this.periodicCheckTimer) {
      clearInterval(this.periodicCheckTimer)
      this.periodicCheckTimer = null
    }
    try {
      localStorage.removeItem('amazon_token_data')
      localStorage.removeItem('amazon_token_info')
    } catch (error) {
      console.warn('Failed to clear token data from storage:', error)
    }
    console.log('🗑️ All token data cleared')
  }

  public getRefreshStatus(): { canRefresh: boolean; reason?: string } {
    if (!this.tokenData) {
      return { canRefresh: false, reason: 'No token data' }
    }
    if (!this.tokenData.refresh_token) {
      return { canRefresh: false, reason: 'No refresh token' }
    }
    return { canRefresh: true }
  }

  public hasActiveConnection(): boolean {
    // Check if we have a valid token
    if (this.isTokenValid()) {
      return true
    }
    
    // Check if we can auto-refresh
    if (this.canAutoRefresh()) {
      return true
    }
    
    // No valid connection
    console.log('📊 Connection status: No active connection available')
    return false
  }

  // Event system for token status changes
  public addEventListener(event: string, callback: Function): void {
    if (!this.eventListeners[event]) {
      this.eventListeners[event] = []
    }
    this.eventListeners[event].push(callback)
  }

  public removeEventListener(event: string, callback: Function): void {
    if (this.eventListeners[event]) {
      this.eventListeners[event] = this.eventListeners[event].filter(cb => cb !== callback)
    }
  }

  private emitEvent(event: string, data?: any): void {
    if (this.eventListeners[event]) {
      this.eventListeners[event].forEach(callback => {
        try {
          callback(data)
        } catch (error) {
          console.error('Error in event listener:', error)
        }
      })
    }
  }
}

// Create axios instance with default configuration
const createApiClient = (): AxiosInstance => {
  const client = axios.create({
    baseURL: API_BASE_URL,
    timeout: API_TIMEOUT,
    headers: {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
    },
    withCredentials: false, // Set to true if you need to send cookies
  })

  // Request interceptor
  client.interceptors.request.use(
    async (config: InternalAxiosRequestConfig) => {
      // Add timestamp to prevent caching
      config.headers.set('X-Timestamp', Date.now().toString())

      // Note: We don't add Authorization headers here because:
      // 1. Amazon SP-API uses x-amz-access-token header (handled by backend)
      // 2. Our backend endpoints don't require Bearer tokens
      // 3. The access_token is sent in the request body for data fetching

      // Log request in development
      if (import.meta.env.DEV) {
        console.log(`🚀 API Request: ${config.method?.toUpperCase()} ${config.url}`, {
          data: config.data ? { ...config.data, access_token: config.data.access_token ? '[REDACTED]' : undefined } : undefined,
          headers: config.headers,
        })
      }

      return config
    },
    (error: AxiosError) => {
      console.error('❌ Request Error:', error)
      return Promise.reject(error)
    }
  )

  // Response interceptor
  client.interceptors.response.use(
    (response: AxiosResponse) => {
      // Log successful response
      console.log(`✅ ${response.status} ${response.config.method?.toUpperCase()} ${response.config.url}`, {
        success: response.data?.success,
        message: response.data?.message,
        duration: response.headers['x-response-time'] || 'unknown'
      })

      return response
    },
    async (error: AxiosError) => {
      // Handle token-related errors
      if (error.response?.status === 401) {
        const errorData = error.response.data as any
        
        // Check if it's a token-related error
        if (errorData?.error?.includes('token') || errorData?.error?.includes('access') || errorData?.error?.includes('authentication')) {
          console.log('🔄 Token-related error detected, attempting refresh...')
          
          const tokenManager = TokenManager.getInstance()
          try {
            // Try to refresh the token
            await tokenManager.refreshAccessToken()
            console.log('✅ Token refreshed successfully, please retry your request')
            
            // Don't automatically retry here - let the user retry manually
            // This prevents infinite loops and gives better user control
          } catch (refreshError) {
            console.error('❌ Token refresh failed:', refreshError)
            tokenManager.clearTokenData()
            
            // Enhance the error message for better user experience
            const enhancedError = new ApiError(
              'Your Amazon connection has expired',
              401,
              'Please reconnect your Amazon account to continue fetching data'
            )
            return Promise.reject(enhancedError)
          }
        }
      }

      // Log error details
      console.error(`❌ ${error.response?.status || 'NETWORK'} ${error.config?.method?.toUpperCase()} ${error.config?.url}`, {
        status: error.response?.status,
        data: error.response?.data,
        message: error.message
      })

      return Promise.reject(error)
    }
  )

  return client
}

// Transform axios errors to ApiError
function transformError(error: unknown): ApiError {
  // Check if it's an axios error
  if (axios.isAxiosError(error)) {
    // Server responded with error status
    const { status, data } = error.response || {}
    
    // Use backend error messages if available (they're already user-friendly)
    const message = data?.error || data?.message || getGenericErrorMessage(status)
    const details = data?.details || error.message
    return new ApiError(message, status || 500, details)
  } else if (error instanceof Error) {
    // Regular Error object
    if (error.message.includes('Network Error') || error.message.includes('fetch')) {
      return new ApiError('Could not connect to our servers', 0, 'Please check your internet connection and try again.')
    } else if (error.message.includes('timeout')) {
      return new ApiError('Connection timed out', 408, 'The request took too long. Please try again.')
    } else {
      return new ApiError('Something unexpected happened', 500, 'Please try again. If the problem continues, check your internet connection.')
    }
  } else {
    // Unknown error type
    return new ApiError('An unexpected error occurred', 500, 'Please try again.')
  }
}

// Helper function to provide user-friendly messages for HTTP status codes
function getGenericErrorMessage(status?: number): string {
  switch (status) {
    case 400:
      return 'Please check your input and try again'
    case 401:
      return 'Authentication failed'
    case 403:
      return 'Access denied'
    case 404:
      return 'Service not found'
    case 408:
      return 'Request timed out'
    case 429:
      return 'Too many requests'
    case 500:
      return 'Server error occurred'
    case 502:
      return 'Service temporarily unavailable'
    case 503:
      return 'Service unavailable'
    default:
      return 'Something went wrong'
  }
}

// Create the API client instance
export const apiClient = createApiClient()

// API Service class for Amazon connector operations
export class AmazonConnectorService {
  private static tokenManager = TokenManager.getInstance()

  /**
   * Connect to Amazon store using provided credentials
   */
  static async connectAmazonStore(
    credentials: AmazonConnectionRequest
  ): Promise<ApiResponse<TokenData>> {
    try {
      const response = await apiClient.post<ApiResponse<TokenData>>(
        '/connect/',
        credentials
      )

      // Store token data if successful
      if (response.data.success && response.data.data) {
        // Store token data along with client secret for automatic refresh
        this.tokenManager.setTokenData(response.data.data)
        console.log('🔐 Token data stored and refresh scheduled')
        
        // Log refresh capability
        const refreshStatus = this.tokenManager.getRefreshStatus()
        console.log('🔄 Auto-refresh capability:', refreshStatus)
      }

      return response.data
    } catch (error) {
      throw transformError(error)
    }
  }

  /**
   * Test Amazon connection without storing credentials
   */
  static async testConnection(credentials: TestConnectionRequest): Promise<ApiResponse<TestConnectionResponse>> {
    try {
      const response = await apiClient.post<ApiResponse<TestConnectionResponse>>(
        '/test-connection/',
        credentials
      )

      return response.data
    } catch (error) {
      throw transformError(error)
    }
  }

  // Refresh access token
  static async refreshAccessToken(credentials: RefreshTokenRequest): Promise<ApiResponse<TokenData>> {
    try {
      const response = await apiClient.post<ApiResponse<TokenData>>(
        '/refresh-token/',
        credentials
      )

      // Update token data if successful
      if (response.data.success && response.data.data) {
        this.tokenManager.setTokenData(response.data.data)
        console.log('🔄 Token refreshed and updated')
      }

      return response.data
    } catch (error) {
      throw transformError(error)
    }
  }

  // Get connection status from backend
  static async getConnectionStatus(): Promise<ApiResponse<ConnectionStatusResponse>> {
    try {
      const response = await apiClient.get<ApiResponse<ConnectionStatusResponse>>('/connection-status/')
      return response.data
    } catch (error) {
      throw transformError(error)
    }
  }

  // Manually refresh token using stored credentials
  static async manualRefreshToken(): Promise<ApiResponse<TokenData>> {
    try {
      const response = await apiClient.post<ApiResponse<TokenData>>('/manual-refresh/')
      
      if (response.data.success && response.data.data) {
        this.tokenManager.setTokenData(response.data.data)
        console.log('🔄 Manual token refresh successful and updated')
      }
      
      return response.data
    } catch (error) {
      throw transformError(error)
    }
  }

  // Get current token info
  static getTokenInfo(): TokenData | null {
    return this.tokenManager.getTokenData()
  }

  // Check if token is valid
  static isTokenValid(): boolean {
    return this.tokenManager.isTokenValid()
  }

  // Get fresh access token
  static async getFreshAccessToken(): Promise<string | null> {
    return this.tokenManager.getFreshAccessToken()
  }

  // Check refresh capability
  static getRefreshStatus(): { canRefresh: boolean; reason?: string } {
    return this.tokenManager.getRefreshStatus()
  }

  // Disconnect (clear token data)
  static disconnect(): void {
    this.tokenManager.clearTokenData()
    console.log('🔒 Disconnected and cleared token data')
  }

  /**
   * Check if we have an active Amazon connection
   */
  static hasActiveConnection(): boolean {
    return this.tokenManager.hasActiveConnection()
  }

  /**
   * Initialize the service and restore any existing connection
   */
  static initialize(): { hasConnection: boolean; needsReconnection: boolean } {
    const tokenData = this.tokenManager.getTokenData()
    const isValid = this.tokenManager.isTokenValid()
    const canRefresh = this.tokenManager.canAutoRefresh()
    const refreshStatus = this.tokenManager.getRefreshStatus()

    console.log('🔧 AmazonConnectorService initialized:', {
      hasTokenData: !!tokenData,
      isTokenValid: isValid,
      canAutoRefresh: canRefresh,
      refreshStatus: refreshStatus
    })

    return {
      hasConnection: !!tokenData,
      needsReconnection: !canRefresh && !!tokenData
    }
  }

  /**
   * Add event listener for token status changes
   */
  static addEventListener(event: string, callback: Function): void {
    this.tokenManager.addEventListener(event, callback)
  }

  /**
   * Remove event listener
   */
  static removeEventListener(event: string, callback: Function): void {
    this.tokenManager.removeEventListener(event, callback)
  }

  /**
   * Fetch Amazon data (orders and order items) for a specific marketplace and date range
   */
  static async fetchAmazonData(
    request: FetchAmazonDataRequest
  ): Promise<ApiResponse<FetchAmazonDataResponse>> {
    try {
      console.log('🔍 Starting Amazon data fetch process...')
      
      // Check if we have any connection at all
      if (!this.hasActiveConnection()) {
        throw new ApiError(
          'No Amazon connection available', 
          401, 
          'Please connect your Amazon account first before fetching data'
        )
      }

      // Get fresh access token with detailed logging
      console.log('🔑 Getting fresh access token...')
      const accessToken = await this.getFreshAccessToken()
      
      if (!accessToken) {
        // Check if we can provide more specific guidance
        const refreshStatus = this.getRefreshStatus()
        const tokenInfo = this.getTokenInfo()
        
        let errorDetails = 'Please reconnect your Amazon account'
        if (!refreshStatus.canRefresh) {
          errorDetails = `Cannot refresh token: ${refreshStatus.reason}. Please reconnect your Amazon account.`
        } else if (!tokenInfo) {
          errorDetails = 'No token data found. Please reconnect your Amazon account.'
        }
        
        throw new ApiError('No valid access token available', 401, errorDetails)
      }

      console.log('✅ Access token obtained successfully')

      // Prepare request data
      const requestData = {
        access_token: accessToken,
        marketplace_id: request.marketplace_id,
        start_date: request.start_date,
        end_date: request.end_date,
        max_orders: request.max_orders || null, // null means fetch all orders
        auto_save: request.auto_save || false // Include auto_save parameter
      }

      console.log('📊 Fetching Amazon data:', {
        marketplace: request.marketplace_id,
        dateRange: `${request.start_date} to ${request.end_date}`,
        maxOrders: requestData.max_orders || 'unlimited'
      })

      const response = await apiClient.post<ApiResponse<FetchAmazonDataResponse>>(
        '/fetch-data/',
        requestData
      )

      if (response.data.success) {
        console.log('✅ Amazon data fetched successfully:', {
          orders: response.data.data?.metadata.total_orders_fetched || 0,
          items: response.data.data?.metadata.total_items_fetched || 0,
          performance: response.data.data?.metadata.performance
        })
      }

      return response.data
    } catch (error) {
      console.error('❌ Amazon data fetch error:', error)
      
      // If it's already an ApiError, just re-throw it
      if (error instanceof ApiError) {
        throw error
      }
      
      // Transform other errors
      throw transformError(error)
    }
  }

  /**
   * Get status of processed data files
   */
  static async getProcessedDataStatus(): Promise<ApiResponse<ProcessedDataStatusResponse>> {
    try {
      console.log('📊 Getting processed data status...')
      
      const response: AxiosResponse<ApiResponse<ProcessedDataStatusResponse>> = await apiClient.get('/processed-status/')
      
      if (response.data.success && response.data.data) {
        console.log('✅ Processed data status retrieved:', response.data.data)
        return response.data
      } else {
        throw new ApiError(
          response.data.error || 'Failed to get processed data status',
          response.status,
          response.data.details
        )
      }
    } catch (error) {
      console.error('❌ Get processed data status error:', error)
      throw transformError(error)
    }
  }

  /**
   * Get list of available processed data files
   */
  static async getProcessedDataFiles(): Promise<ApiResponse<{ files: ProcessedDataFile[]; total_files: number }>> {
    try {
      console.log('📋 Getting processed data files list...')
      
      const response: AxiosResponse<ApiResponse<{ files: ProcessedDataFile[]; total_files: number }>> = await apiClient.get('/download-processed/')
      
      if (response.data.success && response.data.data) {
        console.log('✅ Processed data files list retrieved:', response.data.data)
        return response.data
      } else {
        throw new ApiError(
          response.data.error || 'Failed to get processed data files',
          response.status,
          response.data.details
        )
      }
    } catch (error) {
      console.error('❌ Get processed data files error:', error)
      throw transformError(error)
    }
  }

  /**
   * Download a processed data CSV file
   */
  static async downloadProcessedDataFile(request: DownloadProcessedDataRequest): Promise<void> {
    try {
      console.log('⬇️ Downloading processed data:', request.data_type, 'for cache key:', request.cache_key)
      
      const response = await apiClient.post('/download-processed/', request, {
        responseType: 'blob',
        headers: {
          'Accept': 'text/csv'
        }
      })
      
      // Extract filename from Content-Disposition header
      const contentDisposition = response.headers['content-disposition']
      let filename = `${request.data_type}_data_${Date.now()}.csv`
      
      if (contentDisposition) {
        const filenameMatch = contentDisposition.match(/filename="([^"]+)"/)
        if (filenameMatch) {
          filename = filenameMatch[1]
        }
      }
      
      // Create blob and download
      const blob = new Blob([response.data], { type: 'text/csv' })
      const url = window.URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = filename
      document.body.appendChild(link)
      link.click()
      document.body.removeChild(link)
      window.URL.revokeObjectURL(url)
      
      console.log('✅ File downloaded successfully:', filename)
    } catch (error) {
      console.error('❌ Download processed data file error:', error)
      throw transformError(error)
    }
  }

  /**
   * Get list of activities with pagination and filtering
   */
  static async getActivities(request: ActivitiesListRequest = {}): Promise<ApiResponse<ActivitiesListResponse>> {
    try {
      console.log('📋 Getting activities list...', request)
      
      // Build query parameters
      const params = new URLSearchParams()
      if (request.page) params.append('page', request.page.toString())
      if (request.page_size) params.append('page_size', request.page_size.toString())
      if (request.marketplace_id) params.append('marketplace_id', request.marketplace_id)
      if (request.status) params.append('status', request.status)
      if (request.activity_type) params.append('activity_type', request.activity_type)
      if (request.search) params.append('search', request.search)
      if (request.date_from) params.append('date_from', request.date_from)
      if (request.date_to) params.append('date_to', request.date_to)
      
      const queryString = params.toString()
      const url = `/activities/${queryString ? '?' + queryString : ''}`
      
      const response: AxiosResponse<ApiResponse<ActivitiesListResponse>> = await apiClient.get(url)
      
      if (response.data.success && response.data.data) {
        console.log('✅ Activities retrieved:', {
          total: response.data.data.pagination.total_items,
          current_page: response.data.data.pagination.current_page,
          activities: response.data.data.activities.length
        })
        return response.data
      } else {
        throw new ApiError(
          response.data.error || 'Failed to get activities',
          response.status,
          response.data.details
        )
      }
    } catch (error) {
      console.error('❌ Get activities error:', error)
      throw transformError(error)
    }
  }

  /**
   * Get detailed information about a specific activity
   */
  static async getActivityDetail(activityId: string): Promise<ApiResponse<Activity>> {
    try {
      console.log('🔍 Getting activity detail for:', activityId)
      
      const response: AxiosResponse<ApiResponse<Activity>> = await apiClient.get(`/activities/${activityId}/`)
      
      if (response.data.success && response.data.data) {
        console.log('✅ Activity detail retrieved:', response.data.data.activity_id)
        return response.data
      } else {
        throw new ApiError(
          response.data.error || 'Failed to get activity details',
          response.status,
          response.data.details
        )
      }
    } catch (error) {
      console.error('❌ Get activity detail error:', error)
      throw transformError(error)
    }
  }

  /**
   * Get activity statistics and summary
   */
  static async getActivityStats(request: ActivityStatsRequest = {}): Promise<ApiResponse<ActivityStats>> {
    try {
      console.log('📊 Getting activity statistics...', request)
      
      // Build query parameters
      const params = new URLSearchParams()
      if (request.days) params.append('days', request.days.toString())
      if (request.marketplace_id) params.append('marketplace_id', request.marketplace_id)
      
      const queryString = params.toString()
      const url = `/activities/stats/${queryString ? '?' + queryString : ''}`
      
      const response: AxiosResponse<ApiResponse<ActivityStats>> = await apiClient.get(url)
      
      if (response.data.success && response.data.data) {
        console.log('✅ Activity statistics retrieved:', {
          total_activities: response.data.data.summary.total_activities,
          success_rate: response.data.data.summary.success_rate,
          period: response.data.data.period.days + ' days'
        })
        return response.data
      } else {
        throw new ApiError(
          response.data.error || 'Failed to get activity statistics',
          response.status,
          response.data.details
        )
      }
    } catch (error) {
      console.error('❌ Get activity statistics error:', error)
      throw transformError(error)
    }
  }
}

// Utility function to download data as CSV with ALL raw Amazon fields
export function downloadAsCSV(data: any[], filename: string, type: 'orders' | 'items' = 'orders'): void {
  try {
    if (!data || data.length === 0) {
      throw new Error('No data available to download')
    }

    // Get all unique field names from all records
    const allFields = new Set<string>()
    data.forEach(record => {
      Object.keys(record).forEach(key => {
        // Skip nested items array for orders to avoid duplication
        if (key !== 'items') {
          allFields.add(key)
        }
      })
    })

    // Convert to sorted array for consistent column order
    const headers = Array.from(allFields).sort()
    
    // Helper function to safely extract nested values
    const extractValue = (obj: any, key: string): string => {
      const value = obj[key]
      if (value === null || value === undefined) {
        return ''
      }
      if (typeof value === 'object') {
        // For objects like OrderTotal, ItemPrice, etc., try to extract meaningful info
        if (value.Amount && value.CurrencyCode) {
          return `${value.Amount} ${value.CurrencyCode}`
        } else if (Array.isArray(value)) {
          return value.join('; ')
        } else {
          return JSON.stringify(value)
        }
      }
      if (typeof value === 'boolean') {
        return value ? 'Yes' : 'No'
      }
      return String(value)
    }

    // Create CSV content
    let csvContent = ''
    
    // Add headers
    csvContent = headers.map(header => `"${header}"`).join(',') + '\n'

    // Add data rows
    data.forEach(record => {
      const row = headers.map(header => {
        const value = extractValue(record, header)
        // Escape quotes in the value and wrap in quotes
        return `"${value.replace(/"/g, '""')}"`
      })
      csvContent += row.join(',') + '\n'
    })

    // Create and download the file
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' })
    const link = document.createElement('a')
    
    if (link.download !== undefined) {
      const url = URL.createObjectURL(blob)
      link.setAttribute('href', url)
      link.setAttribute('download', filename)
      link.style.visibility = 'hidden'
      document.body.appendChild(link)
      link.click()
      document.body.removeChild(link)
      URL.revokeObjectURL(url)
      
      console.log(`📥 CSV file downloaded: ${filename} with ${headers.length} columns and ${data.length} rows`)
    } else {
      throw new Error('CSV download not supported in this browser')
    }
  } catch (error) {
    console.error('❌ CSV download error:', error)
    throw new Error(`Failed to download CSV: ${error instanceof Error ? error.message : 'Unknown error'}`)
  }
}

// Export default client for direct use if needed
export default apiClient 