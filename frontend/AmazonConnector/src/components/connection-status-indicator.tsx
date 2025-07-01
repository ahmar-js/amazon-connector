import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Progress } from "@/components/ui/progress"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import { 
  CheckCircle, 
  AlertCircle, 
  XCircle, 
  RefreshCw, 
  Clock, 
  Wifi, 
  WifiOff, 
  Shield,
  Timer,
  Zap,
  Activity,
  Loader2
} from "lucide-react"
import { AmazonConnectorService, type TokenData, ApiError } from "@/lib/api"

interface ConnectionStatus {
  isConnected: boolean
  tokenData: TokenData | null
  isTokenValid: boolean
  canAutoRefresh: boolean
  refreshStatus: { canRefresh: boolean; reason?: string }
  timeUntilExpiry: number | null
  expiryPercentage: number
  lastRefreshTime: string | null
  nextRefreshTime: string | null
  isRefreshing: boolean
  refreshError: string | null
}

interface TimeUntilDisplay {
  hours: number
  minutes: number
  seconds: number
  isExpired: boolean
  isExpiringSoon: boolean // Less than 10 minutes
  isExpiredOrExpiring: boolean // Less than 5 minutes or expired
}

export function ConnectionStatusIndicator() {
  const [status, setStatus] = useState<ConnectionStatus>({
    isConnected: false,
    tokenData: null,
    isTokenValid: false,
    canAutoRefresh: false,
    refreshStatus: { canRefresh: false },
    timeUntilExpiry: null,
    expiryPercentage: 0,
    lastRefreshTime: null,
    nextRefreshTime: null,
    isRefreshing: false,
    refreshError: null,
  })



  // Calculate time until expiry
  const calculateTimeUntilExpiry = (expiresAt: string): TimeUntilDisplay => {
    const expiryTime = new Date(expiresAt).getTime()
    const currentTime = Date.now()
    const timeLeft = expiryTime - currentTime

    if (timeLeft <= 0) {
      return {
        hours: 0,
        minutes: 0,
        seconds: 0,
        isExpired: true,
        isExpiringSoon: false,
        isExpiredOrExpiring: true
      }
    }

    const hours = Math.floor(timeLeft / (1000 * 60 * 60))
    const minutes = Math.floor((timeLeft % (1000 * 60 * 60)) / (1000 * 60))
    const seconds = Math.floor((timeLeft % (1000 * 60)) / 1000)

    const isExpiringSoon = timeLeft < 10 * 60 * 1000 // Less than 10 minutes
    const isExpiredOrExpiring = timeLeft < 5 * 60 * 1000 // Less than 5 minutes

    return {
      hours,
      minutes,
      seconds,
      isExpired: false,
      isExpiringSoon,
      isExpiredOrExpiring
    }
  }

  // Calculate expiry percentage for progress bar
  const calculateExpiryPercentage = (expiresAt: string, expiresIn: number): number => {
    const expiryTime = new Date(expiresAt).getTime()
    const currentTime = Date.now()
    const totalDuration = expiresIn * 1000 // Convert to milliseconds
    const timeElapsed = totalDuration - (expiryTime - currentTime)
    const percentage = Math.max(0, Math.min(100, (timeElapsed / totalDuration) * 100))
    return percentage
  }

  // Update connection status
  const updateStatus = () => {
    const tokenData = AmazonConnectorService.getTokenInfo()
    const isTokenValid = AmazonConnectorService.isTokenValid()
    const refreshStatus = AmazonConnectorService.getRefreshStatus()
    
    const isConnected = !!tokenData
    const canAutoRefresh = refreshStatus.canRefresh

    let timeUntilExpiry: number | null = null
    let expiryPercentage = 0
    let nextRefreshTime: string | null = null

    if (tokenData?.expires_at) {
      const expiryTime = new Date(tokenData.expires_at).getTime()
      timeUntilExpiry = expiryTime - Date.now()
      expiryPercentage = calculateExpiryPercentage(tokenData.expires_at, tokenData.expires_in)
      
      // Calculate next refresh time (10 minutes before expiry)
      const refreshTime = expiryTime - (10 * 60 * 1000)
      if (refreshTime > Date.now()) {
        nextRefreshTime = new Date(refreshTime).toISOString()
      }
    }

    const lastRefreshTime = tokenData?.refreshed_at || tokenData?.connected_at || null

    setStatus(prev => ({
      ...prev,
      isConnected,
      tokenData,
      isTokenValid,
      canAutoRefresh,
      refreshStatus,
      timeUntilExpiry,
      expiryPercentage,
      lastRefreshTime,
      nextRefreshTime,
      refreshError: null, // Clear any previous errors on successful update
    }))
  }

  // Setup intervals for real-time updates
  useEffect(() => {
    // Initial update
    updateStatus()

    // Update every second for real-time countdown
    const interval = setInterval(updateStatus, 1000)

    return () => clearInterval(interval)
  }, [])

  // Get time display
  const timeDisplay = status.tokenData?.expires_at 
    ? calculateTimeUntilExpiry(status.tokenData.expires_at)
    : null

  // Determine overall status
  const getOverallStatus = () => {
    if (!status.isConnected) {
      return { 
        status: 'disconnected' as const, 
        icon: XCircle, 
        color: 'text-gray-500',
        bgColor: 'bg-gray-50 dark:bg-gray-950/50',
        borderColor: 'border-gray-200 dark:border-gray-800'
      }
    }
    
    if (timeDisplay?.isExpired) {
      return { 
        status: 'expired' as const, 
        icon: XCircle, 
        color: 'text-red-600',
        bgColor: 'bg-red-50 dark:bg-red-950/50',
        borderColor: 'border-red-200 dark:border-red-800'
      }
    }
    
    if (timeDisplay?.isExpiredOrExpiring) {
      return { 
        status: 'expiring' as const, 
        icon: AlertCircle, 
        color: 'text-orange-600',
        bgColor: 'bg-orange-50 dark:bg-orange-950/50',
        borderColor: 'border-orange-200 dark:border-orange-800'
      }
    }
    
    if (timeDisplay?.isExpiringSoon) {
      return { 
        status: 'warning' as const, 
        icon: AlertCircle, 
        color: 'text-yellow-600',
        bgColor: 'bg-yellow-50 dark:bg-yellow-950/50',
        borderColor: 'border-yellow-200 dark:border-yellow-800'
      }
    }
    
    return { 
      status: 'healthy' as const, 
      icon: CheckCircle, 
      color: 'text-green-600',
      bgColor: 'bg-green-50 dark:bg-green-950/50',
      borderColor: 'border-green-200 dark:border-green-800'
    }
  }

  const overallStatus = getOverallStatus()
  const StatusIcon = overallStatus.icon

  // Format time for display
  // const formatTime = (isoString: string) => {
  //   return new Date(isoString).toLocaleString()
  // }

  const formatTimeShort = (isoString: string) => {
    return new Date(isoString).toLocaleTimeString()
  }

  // Manual refresh function
  const handleManualRefresh = async () => {
    try {
      setStatus(prev => ({ ...prev, isRefreshing: true, refreshError: null }))
      
      const response = await AmazonConnectorService.manualRefreshToken()
      
      if (response.success && response.data) {
        console.log('✅ Manual refresh successful:', response.message)
        // Update status will be triggered by the interval
        updateStatus()
      } else {
        throw new Error(response.error || 'Manual refresh failed')
      }
    } catch (error) {
      console.error('❌ Manual refresh failed:', error)
      
      let errorMessage = 'Failed to refresh token'
      if (error instanceof ApiError) {
        errorMessage = error.message
      } else if (error instanceof Error) {
        errorMessage = error.message
      }
      
      setStatus(prev => ({ 
        ...prev, 
        isRefreshing: false, 
        refreshError: errorMessage 
      }))
    } finally {
      setStatus(prev => ({ ...prev, isRefreshing: false }))
    }
  }

  return (
    <Card className={`border-2 ${overallStatus.borderColor}`}>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <div className={`p-2 rounded-full ${overallStatus.bgColor}`}>
              <StatusIcon className={`h-5 w-5 ${overallStatus.color}`} />
            </div>
            <div>
              <CardTitle className="text-lg">Connection Status</CardTitle>
              <CardDescription className="text-sm">
                Real-time Amazon API connection monitoring
              </CardDescription>
            </div>
          </div>
          <div className="flex items-center space-x-2">
            {status.isConnected && (
              <Badge 
                variant={overallStatus.status === 'healthy' ? 'default' : 'destructive'}
                className={overallStatus.status === 'healthy' ? 'bg-green-600' : ''}
              >
                {status.isConnected ? (
                  <>
                    <Wifi className="h-3 w-3 mr-1" />
                    Connected
                  </>
                ) : (
                  <>
                    <WifiOff className="h-3 w-3 mr-1" />
                    Disconnected
                  </>
                )}
              </Badge>
            )}
          </div>
        </div>
      </CardHeader>

      <CardContent className="space-y-4">
        {status.isConnected && status.tokenData ? (
          <>
            {/* Token Expiry Section */}
            {timeDisplay && (
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-2">
                    <Timer className="h-4 w-4 text-muted-foreground" />
                    <span className="text-sm font-medium">Token Expiry</span>
                  </div>
                  {!timeDisplay.isExpired && (
                    <span className={`text-sm font-mono ${
                      timeDisplay.isExpiredOrExpiring ? 'text-red-600' :
                      timeDisplay.isExpiringSoon ? 'text-orange-600' :
                      'text-green-600'
                    }`}>
                      {timeDisplay.hours > 0 && `${timeDisplay.hours}h `}
                      {`${timeDisplay.minutes}m ${timeDisplay.seconds}s`}
                    </span>
                  )}
                  {timeDisplay.isExpired && (
                    <Badge variant="destructive" className="text-xs">
                      <XCircle className="h-3 w-3 mr-1" />
                      Expired
                    </Badge>
                  )}
                </div>

                {!timeDisplay.isExpired && (
                  <div className="space-y-2">
                    <Progress 
                      value={status.expiryPercentage} 
                      className="h-2"
                    />
                    <div className="flex justify-between text-xs text-muted-foreground">
                      <span>Token Age</span>
                      <span>{Math.round(status.expiryPercentage)}% elapsed</span>
                    </div>
                  </div>
                )}

                {timeDisplay.isExpired && (
                  <div className="rounded-md bg-red-50 dark:bg-red-950/50 p-3 border border-red-200 dark:border-red-800">
                    <p className="text-sm text-red-800 dark:text-red-200">
                      Your access token has expired. {status.canAutoRefresh ? 'Auto-refresh will attempt to renew it.' : 'Please reconnect your Amazon account.'}
                    </p>
                  </div>
                )}
              </div>
            )}

            {/* Auto-Refresh Status */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-2">
                    <RefreshCw className="h-4 w-4 text-muted-foreground" />
                    <span className="text-sm font-medium">Auto-Refresh</span>
                  </div>
                  {status.canAutoRefresh ? (
                    <Badge variant="default" className="bg-green-600 text-xs">
                      <Zap className="h-3 w-3 mr-1" />
                      Enabled
                    </Badge>
                  ) : (
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <Badge variant="secondary" className="text-xs cursor-help">
                          <AlertCircle className="h-3 w-3 mr-1" />
                          Disabled
                        </Badge>
                      </TooltipTrigger>
                      <TooltipContent>
                        <p>Reason: {status.refreshStatus.reason || 'Unknown'}</p>
                      </TooltipContent>
                    </Tooltip>
                  )}
                </div>
              </div>

              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-2">
                    <Activity className="h-4 w-4 text-muted-foreground" />
                    <span className="text-sm font-medium">Connection</span>
                  </div>
                  <Badge 
                    variant={status.isTokenValid ? "default" : "destructive"}
                    className={`text-xs ${status.isTokenValid ? 'bg-green-600' : ''}`}
                  >
                    <Shield className="h-3 w-3 mr-1" />
                    {status.isTokenValid ? 'Healthy' : 'Failed'}
                  </Badge>
                </div>
              </div>
            </div>

            {/* Manual Refresh Button */}
            <div className="flex flex-col space-y-2 mt-3">
              <div className="flex items-center justify-between">
                <span className="text-sm font-medium">Manual Refresh</span>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={handleManualRefresh}
                      disabled={status.isRefreshing}
                      className="h-7 px-3 cursor-pointer text-xs"
                    >
                      {status.isRefreshing ? (
                        <>
                          <Loader2 className="mr-1 animate-spin" />
                          Refreshing...
                        </>
                      ) : (
                        <>
                          <RefreshCw className="mr-1" />
                          Refresh Now
                        </>
                      )}
                    </Button>
                  </TooltipTrigger>
                  <TooltipContent>
                    <p>Manually refresh your access token</p>
                  </TooltipContent>
                </Tooltip>
              </div>
              
              {status.refreshError && (
                <div className="rounded-md bg-red-50 dark:bg-red-950/50 p-2 border border-red-200 dark:border-red-800">
                  <p className="text-xs text-red-800 dark:text-red-200">
                    {status.refreshError}
                  </p>
                </div>
              )}
            </div>

            {/* Timing Information */}
            <div className="space-y-3 pt-2 border-t border-muted-foreground/20">
              <h4 className="text-sm font-medium flex items-center space-x-2">
                <Clock className="h-4 w-4" />
                <span>Timing Information</span>
              </h4>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-xs">
                {status.lastRefreshTime && (
                  <div>
                    <p className="text-muted-foreground mb-1">Last Refresh</p>
                    <p className="font-mono">
                      {formatTimeShort(status.lastRefreshTime)}
                    </p>
                  </div>
                )}
                
                {status.nextRefreshTime && !timeDisplay?.isExpired && (
                  <div>
                    <p className="text-muted-foreground mb-1">Next Auto-Refresh</p>
                    <p className="font-mono">
                      {formatTimeShort(status.nextRefreshTime)}
                    </p>
                  </div>
                )}
                
                {status.tokenData.expires_at && (
                  <div>
                    <p className="text-muted-foreground mb-1">Token Expires</p>
                    <p className="font-mono">
                      {formatTimeShort(status.tokenData.expires_at)}
                    </p>
                  </div>
                )}
                
                {status.tokenData.connected_at && (
                  <div>
                    <p className="text-muted-foreground mb-1">Connected At</p>
                    <p className="font-mono">
                      {formatTimeShort(status.tokenData.connected_at)}
                    </p>
                  </div>
                )}
              </div>
            </div>

            {/* Technical Details */}
            <details className="text-xs">
              <summary className="cursor-pointer text-muted-foreground hover:text-foreground transition-colors">
                Technical Details
              </summary>
              <div className="mt-2 space-y-2 p-3 bg-muted/30 rounded-md">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                  <div>
                    <span className="text-muted-foreground">Token Type:</span>
                    <span className="ml-2 font-mono">{status.tokenData.token_type}</span>
                  </div>
                  <div>
                    <span className="text-muted-foreground">Expires In:</span>
                    <span className="ml-2 font-mono">{status.tokenData.expires_in}s</span>
                  </div>
                  <div className="md:col-span-2">
                    <span className="text-muted-foreground">App ID:</span>
                    <span className="ml-2 font-mono text-xs break-all">
                      {status.tokenData.app_id}
                    </span>
                  </div>
                </div>
              </div>
            </details>
          </>
        ) : (
          <div className="text-center py-8">
            <WifiOff className="h-12 w-12 text-muted-foreground mx-auto mb-3" />
            <p className="text-muted-foreground">No active Amazon connection</p>
            <p className="text-xs text-muted-foreground mt-1">
              Connect your Amazon account to see status information
            </p>
          </div>
        )}
      </CardContent>
    </Card>
  )
} 