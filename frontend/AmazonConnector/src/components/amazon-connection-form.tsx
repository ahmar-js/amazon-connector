import { zodResolver } from "@hookform/resolvers/zod"
import { useForm } from "react-hook-form"
import { z } from "zod"
import { useState, useEffect } from "react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Form, FormControl, FormDescription, FormField, FormItem, FormLabel, FormMessage } from "@/components/ui/form"
import { Badge } from "@/components/ui/badge"
import { Info, ExternalLink, Key, Shield, RefreshCw, CheckCircle, AlertCircle, ArrowLeft, LogOut, TestTube, Database, Wrench } from "lucide-react"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import { ManageDataDialog } from "./manage-data-dialog"
import { RepairDatesDialog } from "./repair-dates-dialog"
import { AmazonConnectorService, ApiError, type AmazonConnectionRequest } from "@/lib/api"

// Form validation schema
const amazonConnectionSchema = z.object({
  appId: z
    .string()
    .min(1, "Please enter your Application ID")
    .regex(/^amzn1\.application-oa2-client\.[a-f0-9]{32}$/, "Application ID must start with 'amzn1.application-oa2-client.' followed by 32 characters. Please copy it exactly from your Amazon Developer Console."),
  
  clientSecret: z
    .string()
    .min(1, "Please enter your Client Secret")
    .min(64, "Client Secret appears to be too short. Please make sure you've copied the complete secret from your Amazon app settings.")
    .regex(/^[A-Za-z0-9+/]/, "Client Secret should contain only letters, numbers, and +/- characters. Please copy it exactly from Amazon."),
  
  refreshToken: z
    .string()
    .min(1, "Please enter your Refresh Token")
    .regex(/^Atzr\|/, "Refresh Token must start with 'Atzr|'. Make sure you're copying the refresh token (not the access token) from Amazon."),
})

type AmazonConnectionFormValues = z.infer<typeof amazonConnectionSchema>

interface ConnectionState {
  isConnected: boolean
  connectionData?: any
  error?: string
  showForm?: boolean // New: to control form visibility when connected
  isDisconnecting?: boolean // Track disconnect operation
  disconnectSuccess?: boolean // Show disconnect success message
  // Test connection states
  isTesting?: boolean // Track test operation
  testResult?: {
    success: boolean
    message: string
    details?: string
    testedAt: string
  }
  // Manage Data dialog state
  isManageDataDialogOpen?: boolean
  // Session management
  needsReconnection?: boolean
  isInitializing?: boolean
  // Data fetching state
  isFetchingData?: boolean
  // Repair dates state
  isRepairingDates?: boolean
  isRepairDialogOpen?: boolean
  repairSummary?: any
  repairError?: string
}

interface AmazonConnectionFormProps {
  onDataFetchStart?: () => void
  onDataFetchEnd?: () => void
}

export function AmazonConnectionForm({ onDataFetchStart, onDataFetchEnd }: AmazonConnectionFormProps) {
  const [connectionState, setConnectionState] = useState<ConnectionState>({
    isConnected: false,
    showForm: false,
    isManageDataDialogOpen: false,
    isInitializing: true,
  })

  const form = useForm<AmazonConnectionFormValues>({
    resolver: zodResolver(amazonConnectionSchema),
    defaultValues: {
      appId: "",
      clientSecret: "",
      refreshToken: "",
    },
  })

  // Initialize service and check for existing connections
  useEffect(() => {
    const initializeService = async () => {
      try {
        const { hasConnection, needsReconnection } = AmazonConnectorService.initialize()
        
        if (hasConnection) {
          const tokenData = AmazonConnectorService.getTokenInfo()
          const isValid = AmazonConnectorService.isTokenValid()
          
          setConnectionState(prev => ({
            ...prev,
            isConnected: true,
            connectionData: tokenData,
            needsReconnection: needsReconnection,
            showForm: needsReconnection, // Show form if needs reconnection
            isInitializing: false,
            error: needsReconnection ? 'Your session has expired. Please reconnect your Amazon account.' : undefined
          }))
          
          console.log('🔄 Existing connection restored:', { isValid, needsReconnection })
        } else {
          setConnectionState(prev => ({
            ...prev,
            isInitializing: false,
          }))
        }
      } catch (error) {
        console.error('❌ Service initialization error:', error)
        setConnectionState(prev => ({
          ...prev,
          isInitializing: false,
          error: 'Failed to initialize connection service'
        }))
      }
    }

    initializeService()
  }, [])

  // Clear test results when form values change
  const clearTestResults = () => {
    setConnectionState(prev => ({
      ...prev,
      testResult: undefined,
      error: undefined
    }))
  }

  const onSubmit = async (values: AmazonConnectionFormValues) => {
    try {
      // Reset previous errors
      setConnectionState(prev => ({ ...prev, error: undefined }))

      // Call the API
      const response = await AmazonConnectorService.connectAmazonStore(values)

      if (response.success) {
        setConnectionState({
          isConnected: true,
          connectionData: response.data,
          error: undefined,
          showForm: false,
          needsReconnection: false, // Clear reconnection flag
          isManageDataDialogOpen: false,
        })

        // Optional: Show success message or redirect
        console.log("✅ Successfully connected to Amazon:", response.message)
      } else {
        setConnectionState(prev => ({
          ...prev,
          error: response.error || "Failed to connect to Amazon",
        }))
      }
    } catch (error) {
      console.error("❌ Connection error:", error)
      
      let errorMessage = "Something went wrong while connecting to Amazon"
      
      if (error instanceof ApiError) {
        // Use the already user-friendly message from the backend
        errorMessage = error.message
        if (error.details) {
          errorMessage += ` - ${error.details}`
        }
      } else if (error instanceof Error) {
        // Handle network or other errors
        if (error.message.includes('fetch')) {
          errorMessage = "Could not connect to our servers. Please check your internet connection and try again."
        } else if (error.message.includes('timeout')) {
          errorMessage = "The connection timed out. Please try again - Amazon's servers might be busy."
        } else {
          errorMessage = "An unexpected error occurred. Please try again."
        }
      }

      setConnectionState(prev => ({
        ...prev,
        error: errorMessage,
      }))
    }
  }

  const showConnectForm = () => {
    setConnectionState(prev => ({
      ...prev,
      showForm: true,
      error: undefined,
    }))
    form.reset()
  }

  const cancelNewConnection = () => {
    setConnectionState(prev => ({
      ...prev,
      showForm: false,
      error: undefined,
    }))
    form.reset()
  }

  const resetConnection = async () => {
    try {
      // Show disconnecting state
      setConnectionState(prev => ({ 
        ...prev, 
        isDisconnecting: true,
        error: undefined 
      }))

      // Call the service disconnect method to clear all token data
      AmazonConnectorService.disconnect()
      
      // Small delay to show the disconnecting state
      await new Promise(resolve => setTimeout(resolve, 500))
      
      // Reset UI state with success message
      setConnectionState({ 
        isConnected: false, 
        showForm: false,
        connectionData: undefined,
        error: undefined,
        isDisconnecting: false,
        disconnectSuccess: true,
        isManageDataDialogOpen: false, // Close dialog if open
      })
      
      // Reset form
      form.reset()
      
      console.log('🔒 Full disconnect completed - all data cleared')
      
      // Clear success message after 3 seconds
      setTimeout(() => {
        setConnectionState(prev => ({
          ...prev,
          disconnectSuccess: false,
        }))
      }, 3000)
      
    } catch (error) {
      console.error('❌ Disconnect error:', error)
      setConnectionState(prev => ({
        ...prev,
        isDisconnecting: false,
        error: 'Could not disconnect properly. Please refresh the page and try again.'
      }))
    }
  }

  const testConnection = async () => {
    try {
      // Validate form first
      const values = form.getValues()
      const validationResult = amazonConnectionSchema.safeParse(values)
      
      if (!validationResult.success) {
        // Trigger form validation to show errors
        form.trigger()
        return
      }

      // Show testing state
      setConnectionState(prev => ({ 
        ...prev, 
        isTesting: true,
        testResult: undefined,
        error: undefined 
      }))

      // Call the test API
      const response = await AmazonConnectorService.testConnection(values)

      if (response.success) {
        setConnectionState(prev => ({
          ...prev,
          isTesting: false,
          testResult: {
            success: true,
            message: response.message || 'Connection test successful!',
            details: 'Your credentials are valid and can be used to connect.',
            testedAt: new Date().toISOString()
          }
        }))
        
        console.log('✅ Connection test successful:', response.message)
      } else {
        setConnectionState(prev => ({
          ...prev,
          isTesting: false,
          testResult: {
            success: false,
            message: response.error || 'Connection test failed',
            details: response.details,
            testedAt: new Date().toISOString()
          }
        }))
      }
      
    } catch (error) {
      console.error('❌ Connection test error:', error)
      
      let errorMessage = "Connection test failed"
      let errorDetails = "Unable to test your credentials"
      
      if (error instanceof ApiError) {
        errorMessage = error.message
        errorDetails = error.details || "Please check your credentials and try again"
      } else if (error instanceof Error) {
        if (error.message.includes('fetch') || error.message.includes('network')) {
          errorMessage = "Could not reach our servers"
          errorDetails = "Please check your internet connection and try again"
        } else if (error.message.includes('timeout')) {
          errorMessage = "Connection test timed out"
          errorDetails = "Amazon took too long to respond. Please try again."
        }
      }

      setConnectionState(prev => ({
        ...prev,
        isTesting: false,
        testResult: {
          success: false,
          message: errorMessage,
          details: errorDetails,
          testedAt: new Date().toISOString()
        }
      }))
    }
  }

  const openManageDataDialog = () => {
    setConnectionState(prev => ({
      ...prev,
      isManageDataDialogOpen: true,
    }))
  }

  const closeManageDataDialog = (open: boolean) => {
    setConnectionState(prev => ({
      ...prev,
      isManageDataDialogOpen: open,
    }))
  }

  const handleRepairDates = async () => {
    try {
      setConnectionState(prev => ({
        ...prev,
        isRepairingDates: true,
        repairError: undefined,
        repairSummary: undefined,
      }))

      const response = await AmazonConnectorService.repairPurchaseDates()

      if (response.success) {
        setConnectionState(prev => ({
          ...prev,
          isRepairingDates: false,
          isRepairDialogOpen: true,
          repairSummary: response.data,
        }))
      }
    } catch (error) {
      console.error('❌ Repair dates error:', error)
      const errorMessage = error instanceof ApiError ? error.message : 'Failed to repair purchase dates'
      setConnectionState(prev => ({
        ...prev,
        isRepairingDates: false,
        isRepairDialogOpen: true,
        repairError: errorMessage,
      }))
    }
  }

  const closeRepairDialog = (open: boolean) => {
    setConnectionState(prev => ({
      ...prev,
      isRepairDialogOpen: open,
      repairSummary: open ? prev.repairSummary : undefined,
      repairError: open ? prev.repairError : undefined,
    }))
  }

  const handleDataFetchStart = () => {
    setConnectionState(prev => ({
      ...prev,
      isFetchingData: true,
    }))
    // Call parent callback if provided
    onDataFetchStart?.()
  }

  const handleDataFetchEnd = () => {
    setConnectionState(prev => ({
      ...prev,
      isFetchingData: false,
    }))
    // Call parent callback if provided
    onDataFetchEnd?.()
  }

  // const handleMarketplaceSelect = (marketplace: string) => {
  //   setConnectionState(prev => ({
  //     ...prev,
  //     selectedMarketplace: marketplace,
  //     isMarketplacePopoverOpen: false,
  //   }))
  // }

  // Show main success state when connected and not showing form
  const showSuccessState = connectionState.isConnected && !connectionState.showForm && !connectionState.needsReconnection
  // Show form when not connected OR when connected but wanting to change account OR when reconnection is needed
  const showFormState = !connectionState.isConnected || connectionState.showForm || connectionState.needsReconnection

  // Show loading state while initializing
  if (connectionState.isInitializing) {
    return (
      <div className="space-y-6">
        <Card className="border-2 border-dashed border-muted-foreground/25">
          <CardHeader>
            <div className="flex items-center space-x-2">
              <RefreshCw className="h-5 w-5 text-primary animate-spin" />
              <CardTitle className="text-lg">Initializing Amazon Connection</CardTitle>
            </div>
            <CardDescription>
              Checking for existing connection...
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex items-center justify-center py-8">
              <RefreshCw className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
          </CardContent>
        </Card>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <Card className="border-2 border-dashed border-muted-foreground/25">
        <CardHeader>
          <div className="flex items-center space-x-2">
            {connectionState.isConnected ? (
              <CheckCircle className="h-5 w-5 text-green-600" />
            ) : (
              <Shield className="h-5 w-5 text-primary" />
            )}
            <CardTitle className="text-lg">Amazon API Connection</CardTitle>
            {connectionState.isConnected && (
              <Badge variant="default" className="bg-green-600">
                Connected
              </Badge>
            )}
          </div>
          <CardDescription>
            {showSuccessState
              ? "Your Amazon Seller Central account is successfully connected."
              : connectionState.isConnected && connectionState.showForm
              ? "Connect a different Amazon Seller Central account."
              : connectionState.needsReconnection
              ? "Your session has expired. Please reconnect to continue."
              : "Connect your Amazon Seller Central account to start managing your products and orders."
            }
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* Session Expired / Reconnection Needed */}
          {connectionState.needsReconnection && (
            <div className="rounded-lg bg-amber-50 dark:bg-amber-950/50 p-4 border border-amber-200 dark:border-amber-800">
              <div className="flex items-start space-x-3">
                <AlertCircle className="h-5 w-5 text-amber-600 dark:text-amber-400 mt-0.5 flex-shrink-0" />
                <div className="space-y-2">
                  <h4 className="font-medium text-amber-900 dark:text-amber-100">Session Expired</h4>
                  <p className="text-sm text-amber-800 dark:text-amber-200">
                    Your Amazon connection has expired. This typically happens after a page refresh or extended inactivity. 
                    Please reconnect using your credentials to continue managing your data.
                  </p>
                </div>
              </div>
            </div>
          )}

          {/* Disconnect Success State */}
          {connectionState.disconnectSuccess && (
            <div className="rounded-lg bg-blue-50 dark:bg-blue-950/50 p-4 border border-blue-200 dark:border-blue-800">
              <div className="flex items-start space-x-3">
                <CheckCircle className="h-5 w-5 text-blue-600 dark:text-blue-400 mt-0.5 flex-shrink-0" />
                <div className="space-y-2">
                  <h4 className="font-medium text-blue-900 dark:text-blue-100">Successfully Disconnected</h4>
                  <p className="text-sm text-blue-800 dark:text-blue-200">
                    All connection data has been cleared. You can now connect a new Amazon account.
                  </p>
                </div>
              </div>
            </div>
          )}

          {/* Connection Success State */}
          {showSuccessState && connectionState.connectionData && (
            <div className="rounded-lg bg-green-50 dark:bg-green-950/50 p-4 border border-green-200 dark:border-green-800">
              <div className="flex items-start space-x-3">
                <CheckCircle className="h-5 w-5 text-green-600 dark:text-green-400 mt-0.5 flex-shrink-0" />
                <div className="space-y-2">
                  <h4 className="font-medium text-green-900 dark:text-green-100">Connection Successful!</h4>
                  <p className="text-sm text-green-800 dark:text-green-200">
                    Your Amazon store is now connected and ready to use. You can now fetch and store data to database.
                  </p>
                  <div className="flex flex-wrap items-center gap-2 mt-3">
                    <Button 
                      variant="outline" 
                      size="sm" 
                      onClick={showConnectForm} 
                      className="cursor-pointer"
                      disabled={connectionState.isFetchingData || connectionState.isRepairingDates}
                    >
                      <RefreshCw className="h-4 w-4 mr-2" />
                      Connect Different Account
                    </Button>
                    <Button 
                      variant="default" 
                      size="sm" 
                      onClick={openManageDataDialog} 
                      className="cursor-pointer"
                      disabled={connectionState.isFetchingData || connectionState.isRepairingDates}
                    >
                      <Database className="h-4 w-4 mr-2" />
                      {connectionState.isFetchingData ? 'Fetching Data...' : 'Manage Data'}
                    </Button>
                    <Button 
                      variant="secondary" 
                      size="sm" 
                      onClick={handleRepairDates} 
                      className="cursor-pointer"
                      disabled={connectionState.isFetchingData || connectionState.isRepairingDates}
                    >
                      <Wrench className="h-4 w-4 mr-2" />
                      {connectionState.isRepairingDates ? 'Repairing...' : 'Repair Dates'}
                    </Button>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Current Connection Info (shown when switching accounts) */}
          {connectionState.isConnected && connectionState.showForm && connectionState.connectionData && (
            <div className="rounded-lg bg-amber-50 dark:bg-amber-950/50 p-4 border border-amber-200 dark:border-amber-800">
              <div className="flex items-start space-x-3">
                <Info className="h-5 w-5 text-amber-600 dark:text-amber-400 mt-0.5 flex-shrink-0" />
                <div className="space-y-2">
                  <h4 className="font-medium text-amber-900 dark:text-amber-100">Currently Connected Account</h4>
                  <p className="text-sm text-amber-800 dark:text-amber-200">
                    You have an active connection to your Amazon store. You can keep using it or connect a different account below.
                  </p>
                  <div className="flex flex-wrap items-center gap-2 mt-3">
                    <Button variant="outline" size="sm" className="cursor-pointer" onClick={cancelNewConnection}>
                      <ArrowLeft className="h-4 w-4 mr-2" />
                      Keep Current Connection
                    </Button>
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <Button 
                          variant="destructive" 
                          size="sm" 
                          onClick={resetConnection}
                          disabled={connectionState.isDisconnecting || connectionState.isFetchingData}
                          className="cursor-pointer"
                        >
                          {connectionState.isDisconnecting ? (
                            <>
                              <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
                              Disconnecting...
                            </>
                          ) : connectionState.isFetchingData ? (
                            <>
                              <Database className="h-4 w-4 mr-2" />
                              Data Fetching...
                            </>
                          ) : (
                            <>
                              <LogOut className="h-4 w-4 mr-2" />
                              Disconnect
                            </>
                          )}
                        </Button>
                      </TooltipTrigger>
                      <TooltipContent>
                        <div className="text-sm">
                          <p className="font-medium">This will clear:</p>
                          <ul className="mt-1 space-y-1 text-xs">
                            <li>• Access & refresh tokens</li>
                            <li>• Stored credentials</li>
                            <li>• Auto-refresh timers</li>
                            <li>• Connection data</li>
                          </ul>
                        </div>
                      </TooltipContent>
                    </Tooltip>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Connection Error State */}
          {connectionState.error && (
            <div className="rounded-lg bg-red-50 dark:bg-red-950/50 p-4 border border-red-200 dark:border-red-800">
              <div className="flex items-start space-x-3">
                <AlertCircle className="h-5 w-5 text-red-600 dark:text-red-400 mt-0.5 flex-shrink-0" />
                <div className="space-y-2">
                  <h4 className="font-medium text-red-900 dark:text-red-100">Connection Failed</h4>
                  <p className="text-sm text-red-800 dark:text-red-200">
                    {connectionState.error}
                  </p>
                </div>
              </div>
            </div>
          )}

          {/* Test Connection Result */}
          {connectionState.testResult && (
            <div className={`rounded-lg p-4 border ${
              connectionState.testResult.success 
                ? 'bg-emerald-50 dark:bg-emerald-950/50 border-emerald-200 dark:border-emerald-800'
                : 'bg-orange-50 dark:bg-orange-950/50 border-orange-200 dark:border-orange-800'
            }`}>
              <div className="flex items-start space-x-3">
                {connectionState.testResult.success ? (
                  <CheckCircle className="h-5 w-5 text-emerald-600 dark:text-emerald-400 mt-0.5 flex-shrink-0" />
                ) : (
                  <AlertCircle className="h-5 w-5 text-orange-600 dark:text-orange-400 mt-0.5 flex-shrink-0" />
                )}
                <div className="space-y-2 flex-1">
                  <div className="flex items-center justify-between">
                    <h4 className={`font-medium ${
                      connectionState.testResult.success 
                        ? 'text-emerald-900 dark:text-emerald-100'
                        : 'text-orange-900 dark:text-orange-100'
                    }`}>
                      {connectionState.testResult.success ? 'Connection Test Passed ✓' : 'Connection Test Failed'}
                    </h4>
                    <Badge variant="outline" className="text-xs">
                      <TestTube className="h-3 w-3 mr-1" />
                      Test
                    </Badge>
                  </div>
                  <p className={`text-sm ${
                    connectionState.testResult.success 
                      ? 'text-emerald-800 dark:text-emerald-200'
                      : 'text-orange-800 dark:text-orange-200'
                  }`}>
                    {connectionState.testResult.message}
                  </p>
                  {connectionState.testResult.details && (
                    <p className={`text-xs ${
                      connectionState.testResult.success 
                        ? 'text-emerald-700 dark:text-emerald-300'
                        : 'text-orange-700 dark:text-orange-300'
                    }`}>
                      {connectionState.testResult.details}
                    </p>
                  )}
                  {connectionState.testResult.success && (
                    <div className="pt-2">
                      <p className="text-xs text-emerald-600 dark:text-emerald-400 font-medium">
                        ✓ Ready to connect! Your credentials are working properly.
                      </p>
                    </div>
                  )}
                </div>
              </div>
            </div>
          )}

          {/* Show form when not connected OR when wanting to connect different account */}
          {showFormState && (
            <>
              {/* Information Section */}
              <div className="rounded-lg bg-blue-50 dark:bg-blue-950/50 p-4 border border-blue-200 dark:border-blue-800">
                <div className="flex items-start space-x-3">
                  <Info className="h-5 w-5 text-blue-600 dark:text-blue-400 mt-0.5 flex-shrink-0" />
                  <div className="space-y-2">
                    <h4 className="font-medium text-blue-900 dark:text-blue-100">Before you start</h4>
                    <p className="text-sm text-blue-800 dark:text-blue-200">
                      You'll need to create an application in Amazon's Developer Console to get these credentials.
                    </p>
                    <Button variant="outline" size="sm" className="mt-2 cursor-pointer" asChild>
                      <a 
                        href="https://www.amazon.com/ap/signin?openid.pape.max_auth_age=3600&openid.return_to=https%3A%2F%2Fdeveloper.amazon.com%2Fsettings%2Fconsole%2Fhome&openid.identity=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select&openid.assoc_handle=mas_dev_portal&openid.mode=checkid_setup&language=en_US&openid.claimed_id=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select&pageId=amzn_developer_portal&openid.ns=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0&language=en_US"
                        target="_blank"
                        rel="noopener noreferrer"
                      >
                        <ExternalLink className="h-4 w-4 mr-2" />
                        Open Developer Console
                      </a>
                    </Button>
                  </div>
                </div>
              </div>

              {/* Form */}
              <Form {...form}>
                <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-6">
                  
                  {/* App ID Field */}
                  <FormField
                    control={form.control}
                    name="appId"
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel className="flex items-center space-x-2">
                          <Key className="h-4 w-4" />
                          <span>Application ID</span>
                          <Badge variant="secondary" className="text-xs">Required</Badge>
                        </FormLabel>
                        <FormControl>
                          <Input 
                            placeholder="amzn1.application-oa2-client.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" 
                            {...field} 
                            onChange={(e) => {
                              field.onChange(e)
                              clearTestResults()
                            }}
                          />
                        </FormControl>
                        <FormDescription>
                          Your Amazon application identifier. Starts with <code className="px-1 py-0.5 bg-muted rounded text-xs">amzn1.application-oa2-client.</code>
                        </FormDescription>
                        <FormMessage />
                      </FormItem>
                    )}
                  />

                  {/* Client Secret Field */}
                  <FormField
                    control={form.control}
                    name="clientSecret"
                    render={({ field }) => (
                      <FormItem>
                        <div className="flex items-center space-x-2">
                          <FormLabel className="flex items-center space-x-2">
                            <Shield className="h-4 w-4" />
                            <span>Client Secret</span>
                          </FormLabel>
                          <Badge variant="destructive" className="text-xs text-white">
                            Sensitive
                          </Badge>
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <Info className="h-4 w-4 text-muted-foreground cursor-help" />
                            </TooltipTrigger>
                            <TooltipContent>
                              <p>Keep this secret safe! Never share it publicly.</p>
                            </TooltipContent>
                          </Tooltip>
                        </div>
                        <FormControl>
                          <Input
                            type="password"
                            placeholder="Enter your client secret..."
                            {...field}
                            className="font-mono text-sm"
                            disabled={form.formState.isSubmitting}
                            onChange={(e) => {
                              field.onChange(e)
                              clearTestResults()
                            }}
                          />
                        </FormControl>
                        <FormDescription>
                          The secret key for your Amazon application. This is sensitive information.
                        </FormDescription>
                        <FormMessage />
                      </FormItem>
                    )}
                  />

                  {/* Refresh Token Field */}
                  <FormField
                    control={form.control}
                    name="refreshToken"
                    render={({ field }) => (
                      <FormItem>
                        <div className="flex items-center space-x-2">
                          <FormLabel className="flex items-center space-x-2">
                            <RefreshCw className="h-4 w-4" />
                            <span>Refresh Token</span>
                          </FormLabel>
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <Info className="h-4 w-4 text-muted-foreground cursor-help" />
                            </TooltipTrigger>
                            <TooltipContent>
                              <p>Obtained after OAuth authorization flow completion</p>
                            </TooltipContent>
                          </Tooltip>
                        </div>
                        <FormControl>
                          <Input
                            placeholder="Atzr|IwEBIA..."
                            {...field}
                            className="font-mono text-sm"
                            disabled={form.formState.isSubmitting}
                            onChange={(e) => {
                              field.onChange(e)
                              clearTestResults()
                            }}
                          />
                        </FormControl>
                        <FormDescription>
                          Long-lived token for API access. Starts with <code className="px-1 py-0.5 bg-muted rounded text-xs">Atzr|</code>
                        </FormDescription>
                        <FormMessage />
                      </FormItem>
                    )}
                  />

                  {/* Submit Button */}
                  <div className="flex flex-col space-y-4 pt-4">
                    {/* Test Connection Button */}
                    <div className="flex flex-col space-y-3">
                      <div className="flex items-center space-x-2">
                        <div className="flex-1 border-t border-muted-foreground/20"></div>
                        <span className="text-xs text-muted-foreground font-medium">VALIDATE CREDENTIALS</span>
                        <div className="flex-1 border-t border-muted-foreground/20"></div>
                      </div>
                      
                      <Button 
                        type="button"
                        variant="outline"
                        className="w-full cursor-pointer" 
                        size="lg"
                        onClick={testConnection}
                        disabled={connectionState.isTesting || form.formState.isSubmitting}
                      >
                        {connectionState.isTesting ? (
                          <>
                            <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
                            Testing Connection...
                          </>
                        ) : (
                          <>
                            <TestTube className="h-4 w-4 mr-2" />
                            Test Connection
                          </>
                        )}
                      </Button>
                      
                      <p className="text-xs text-muted-foreground text-center">
                        Verify your credentials without saving the connection
                      </p>
                    </div>

                    {/* Divider */}
                    <div className="flex items-center space-x-2">
                      <div className="flex-1 border-t border-muted-foreground/20"></div>
                      <span className="text-xs text-muted-foreground font-medium">OR CONNECT DIRECTLY</span>
                      <div className="flex-1 border-t border-muted-foreground/20"></div>
                    </div>

                    <Button 
                      type="submit" 
                      className="w-full cursor-pointer" 
                      size="lg"
                      disabled={form.formState.isSubmitting || connectionState.isTesting}
                    >
                      {form.formState.isSubmitting ? (
                        <>
                          <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
                          Connecting...
                        </>
                      ) : (
                        <>
                          <Shield className="h-4 w-4 mr-2" />
                          {connectionState.needsReconnection ? "Reconnect to Amazon" 
                           : connectionState.isConnected ? "Connect New Account" 
                           : "Connect to Amazon"}
                        </>
                      )}
                    </Button>
                    
                    {/* Connection Status */}
                    <div className="text-center">
                      <Badge variant="outline" className="text-xs">
                        🔒 Secure connection via Amazon API
                      </Badge>
                    </div>
                  </div>
                </form>
              </Form>
            </>
          )}
        </CardContent>
      </Card>

      {/* Help Section */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Need Help?</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="text-sm space-y-2">
            <p><strong>Step 1:</strong> Register as a developer in Amazon Seller Central</p>
            <p><strong>Step 2:</strong> Create an app in Amazon Developer Console</p>
            <p><strong>Step 3:</strong> Configure OAuth redirect URLs</p>
            <p><strong>Step 4:</strong> Complete authorization flow and copy credentials here</p>
          </div>
          <Button variant="outline" size="sm" className="w-full cursor-pointer" asChild>
            <a 
              href="https://developer-docs.amazon.com/amazon-shipping/docs/registering-as-a-developer"
              target="_blank"
              rel="noopener noreferrer"
            >
              <ExternalLink className="h-4 w-4 mr-2" />
              View Setup Guide
            </a>
          </Button>
        </CardContent>
      </Card>

      {/* Manage Data Dialog */}
      <ManageDataDialog
        isOpen={connectionState.isManageDataDialogOpen || false}
        onOpenChange={closeManageDataDialog}
        onDataFetchStart={handleDataFetchStart}
        onDataFetchEnd={handleDataFetchEnd}
      />

      {/* Repair Dates Dialog */}
      <RepairDatesDialog
        isOpen={connectionState.isRepairDialogOpen || false}
        onOpenChange={closeRepairDialog}
        summary={connectionState.repairSummary}
        error={connectionState.repairError}
      />
    </div>
  )
} 