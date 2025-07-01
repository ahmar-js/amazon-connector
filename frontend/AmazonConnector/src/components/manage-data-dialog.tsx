import { useState } from "react"
import { format } from "date-fns"
import { Button } from "@/components/ui/button"
import { Label } from "@/components/ui/label"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { Command, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList } from "@/components/ui/command"
import { Calendar } from "@/components/ui/calendar"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { Progress } from "@/components/ui/progress"
import { Badge } from "@/components/ui/badge"
import { Checkbox } from "@/components/ui/checkbox"
import { 
  Database, 
  Check, 
  ChevronsUpDown, 
  RefreshCw, 
  CalendarIcon, 
  AlertCircle, 
  CheckCircle, 
  Download,
  Clock,
  TrendingUp,
  Package,
  ShoppingCart,
  FileText,
  X
} from "lucide-react"
import { 
  AmazonConnectorService, 
  downloadAsCSV, 
  type FetchAmazonDataRequest, 
  type FetchAmazonDataResponse,
  type AmazonOrder,
  type AmazonOrderItem,
  type ProcessedDataFile,
  type DownloadProcessedDataRequest,
  ApiError 
} from "@/lib/api"

// Amazon marketplaces with proper mapping
const AMAZON_MARKETPLACES = [
  { value: "A1F83G8C2ARO7P", label: "United Kingdom", code: "UK", domain: "amazon.co.uk", disabled: false },
  { value: "APJ6JRA9NG5V4", label: "Italy", code: "IT", domain: "amazon.it", disabled: false },
  { value: "A1RKKUPIHCS9HS", label: "Spain", code: "ES", domain: "amazon.es", disabled: false },
  { value: "A1PA6795UKMFR9", label: "Germany", code: "DE", domain: "amazon.de", disabled: false },
  { value: "A13V1IB3VIYZZH", label: "France", code: "FR", domain: "amazon.fr", disabled: false },
  { value: "ATVPDKIKX0DER", label: "United States", code: "US", domain: "amazon.com", disabled: false },
  { value: "A2EUQ1WTGCTBG2", label: "Canada", code: "CA", domain: "amazon.ca", disabled: false },
] as const

interface ManageDataDialogProps {
  isOpen: boolean
  onOpenChange: (open: boolean) => void
  onDataFetchStart?: () => void
  onDataFetchEnd?: () => void
}

interface DialogState {
  selectedMarketplace: string
  isMarketplacePopoverOpen: boolean
  startDate?: Date
  endDate?: Date
  isStartDatePopoverOpen: boolean
  isEndDatePopoverOpen: boolean
  // Data fetching states
  isFetchingData: boolean
  fetchProgress: number
  fetchError?: string
  fetchSuccess?: boolean
  fetchedData?: FetchAmazonDataResponse
  // Processed data states
  processedFiles?: ProcessedDataFile[]
  isDownloadingProcessed: boolean
  // UI states
  showResults: boolean
  autoSaveToDatabase: boolean
  // Track if data was fetched without auto-save (to show save button)
  dataFetchedWithoutAutoSave: boolean
  isSavingToDatabase: boolean
}

export function ManageDataDialog({ isOpen, onOpenChange, onDataFetchStart, onDataFetchEnd }: ManageDataDialogProps) {
  const [dialogState, setDialogState] = useState<DialogState>({
    selectedMarketplace: "A1F83G8C2ARO7P", // Default to UK marketplace
    isMarketplacePopoverOpen: false,
    startDate: undefined,
    endDate: undefined,
    isStartDatePopoverOpen: false,
    isEndDatePopoverOpen: false,
    isFetchingData: false,
    fetchProgress: 0,
    fetchError: undefined,
    fetchSuccess: false,
    fetchedData: undefined,
    processedFiles: undefined,
    isDownloadingProcessed: false,
    showResults: false,
    autoSaveToDatabase: false,
    dataFetchedWithoutAutoSave: false,
    isSavingToDatabase: false,
  })

  const handleMarketplaceSelect = (marketplace: string) => {
    // Check if marketplace is disabled
    const marketplaceInfo = AMAZON_MARKETPLACES.find(m => m.value === marketplace)
    if (marketplaceInfo?.disabled) {
      return // Don't allow selection of disabled marketplaces
    }

    setDialogState(prev => ({
      ...prev,
      selectedMarketplace: marketplace,
      isMarketplacePopoverOpen: false,
      // Clear previous results when changing marketplace
      fetchError: undefined,
      fetchSuccess: false,
      fetchedData: undefined,
      showResults: false,
    }))
  }

  const handleStartDateSelect = (date: Date | undefined) => {
    setDialogState(prev => ({
      ...prev,
      startDate: date,
      isStartDatePopoverOpen: false,
      // Clear end date if it's before the new start date
      endDate: prev.endDate && date && prev.endDate < date ? undefined : prev.endDate,
      // Clear previous results when changing dates
      fetchError: undefined,
      fetchSuccess: false,
      fetchedData: undefined,
      showResults: false,
    }))
  }

  const handleEndDateSelect = (date: Date | undefined) => {
    setDialogState(prev => ({
      ...prev,
      endDate: date,
      isEndDatePopoverOpen: false,
      // Clear previous results when changing dates
      fetchError: undefined,
      fetchSuccess: false,
      fetchedData: undefined,
      showResults: false,
    }))
  }

  const handleRequestData = async () => {
    // Prevent multiple simultaneous requests
    if (dialogState.isFetchingData) {
      console.log('⚠️ Request already in progress, ignoring duplicate request')
      return
    }

    // Validate required fields
    if (!dialogState.selectedMarketplace) {
      setDialogState(prev => ({
        ...prev,
        fetchError: 'Please select a marketplace'
      }))
      return
    }

    if (!dialogState.startDate || !dialogState.endDate) {
      setDialogState(prev => ({
        ...prev,
        fetchError: 'Please select both start and end dates'
      }))
      return
    }

    try {
      // Notify parent that data fetch is starting
      onDataFetchStart?.()

      setDialogState(prev => ({
        ...prev,
        isFetchingData: true,
        fetchProgress: 0,
        fetchError: undefined,
        fetchSuccess: false,
        fetchedData: undefined,
        showResults: false,
      }))

      // Simulate progress updates
      const progressInterval = setInterval(() => {
        setDialogState(prev => ({
          ...prev,
          fetchProgress: Math.min(prev.fetchProgress + Math.random() * 15, 90)
        }))
      }, 500)

      // Prepare request data
      const request: FetchAmazonDataRequest = {
        marketplace_id: dialogState.selectedMarketplace,
        start_date: format(dialogState.startDate, 'yyyy-MM-dd'),
        end_date: format(dialogState.endDate, 'yyyy-MM-dd'),
        auto_save: dialogState.autoSaveToDatabase
        // No max_orders limit - fetch all orders in date range
      }

      console.log('📊 Requesting Amazon data:', request)

      // Make API call
      const response = await AmazonConnectorService.fetchAmazonData(request)

      // Clear progress interval
      clearInterval(progressInterval)

      if (response.success && response.data) {
        setDialogState(prev => ({
          ...prev,
          isFetchingData: false,
          fetchProgress: 100,
          fetchSuccess: true,
          fetchedData: response.data,
          showResults: true,
          // Track if data was fetched without auto-save (to show save button later)
          dataFetchedWithoutAutoSave: !prev.autoSaveToDatabase,
        }))

        console.log('✅ Data fetch successful:', response.data.metadata)
      } else {
        throw new Error(response.error || 'Failed to fetch data')
      }

    } catch (error) {
      console.error('❌ Data fetch error:', error)
      
      let errorMessage = 'Failed to fetch Amazon data'
      let errorDetails = 'Please try again'

      if (error instanceof ApiError) {
        errorMessage = error.message
        errorDetails = error.details || 'Please check your connection and try again'
      } else if (error instanceof Error) {
        if (error.message.includes('token')) {
          errorMessage = 'Authentication failed'
          errorDetails = 'Please reconnect your Amazon account and try again'
        } else if (error.message.includes('network') || error.message.includes('fetch')) {
          errorMessage = 'Connection failed'
          errorDetails = 'Please check your internet connection and try again'
        } else {
          errorDetails = error.message
        }
      }

      setDialogState(prev => ({
        ...prev,
        isFetchingData: false,
        fetchProgress: 0,
        fetchError: `${errorMessage}: ${errorDetails}`,
      }))
    } finally {
      // Notify parent that data fetch has ended
      onDataFetchEnd?.()
    }
  }

  const handleDownloadCSV = (type: 'orders' | 'items') => {
    if (!dialogState.fetchedData) return

    try {
      const selectedMarketplace = AMAZON_MARKETPLACES.find(m => m.value === dialogState.selectedMarketplace)
      const dateRange = `${format(dialogState.startDate!, 'yyyy-MM-dd')}_to_${format(dialogState.endDate!, 'yyyy-MM-dd')}`
      
      if (type === 'orders') {
        const filename = `amazon_orders_${selectedMarketplace?.code || 'unknown'}_${dateRange}.csv`
        downloadAsCSV(dialogState.fetchedData.orders, filename, 'orders')
      } else {
        const filename = `amazon_order_items_${selectedMarketplace?.code || 'unknown'}_${dateRange}.csv`
        downloadAsCSV(dialogState.fetchedData.order_items, filename, 'items')
      }
    } catch (error) {
      console.error('❌ CSV download error:', error)
      setDialogState(prev => ({
        ...prev,
        fetchError: `Download failed: ${error instanceof Error ? error.message : 'Unknown error'}`
      }))
    }
  }

  const handleDownloadProcessedData = async (type: 'mssql' | 'azure') => {
    if (!dialogState.fetchedData?.processed_data?.cache_key) return

    try {
      setDialogState(prev => ({ ...prev, isDownloadingProcessed: true }))

      const request: DownloadProcessedDataRequest = {
        cache_key: dialogState.fetchedData.processed_data.cache_key,
        data_type: type
      }

      await AmazonConnectorService.downloadProcessedDataFile(request)

      console.log(`✅ Downloaded processed ${type.toUpperCase()} data`)
    } catch (error) {
      console.error('❌ Processed data download error:', error)
      setDialogState(prev => ({
        ...prev,
        fetchError: `Processed data download failed: ${error instanceof Error ? error.message : 'Unknown error'}`
      }))
    } finally {
      setDialogState(prev => ({ ...prev, isDownloadingProcessed: false }))
    }
  }

  const handleDialogClose = () => {
    // Don't allow closing during data fetch
    if (dialogState.isFetchingData) {
      return
    }

    setDialogState(prev => ({
      ...prev,
      isMarketplacePopoverOpen: false,
      isStartDatePopoverOpen: false,
      isEndDatePopoverOpen: false,
      // Reset states when closing
      fetchError: undefined,
      fetchSuccess: false,
      showResults: false,
    }))
    onOpenChange(false)
  }

  const handleSaveToDatabase = async () => {
    if (!dialogState.fetchedData) return

    try {
      setDialogState(prev => ({ ...prev, isSavingToDatabase: true }))

      // TODO: Implement save to database API call
      // This will be implemented when backend is ready
      console.log('💾 Saving data to database...', {
        orders: dialogState.fetchedData.orders.length,
        items: dialogState.fetchedData.order_items.length
      })

      // Simulate API call for now
      await new Promise(resolve => setTimeout(resolve, 2000))

      setDialogState(prev => ({
        ...prev,
        isSavingToDatabase: false,
        dataFetchedWithoutAutoSave: false, // Hide save button after successful save
      }))

      console.log('✅ Data saved to database successfully')
    } catch (error) {
      console.error('❌ Save to database error:', error)
      setDialogState(prev => ({
        ...prev,
        isSavingToDatabase: false,
        fetchError: `Save to database failed: ${error instanceof Error ? error.message : 'Unknown error'}`
      }))
    }
  }

  const clearResults = () => {
    setDialogState(prev => ({
      ...prev,
      fetchError: undefined,
      fetchSuccess: false,
      fetchedData: undefined,
      showResults: false,
      fetchProgress: 0,
      dataFetchedWithoutAutoSave: false,
      isSavingToDatabase: false,
    }))
  }

  // Validate date range
  const isDateRangeValid = () => {
    if (!dialogState.startDate || !dialogState.endDate) return false
    if (dialogState.startDate > dialogState.endDate) return false
    
    // Check if date range is not more than 30 days
    const diffTime = Math.abs(dialogState.endDate.getTime() - dialogState.startDate.getTime())
    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24))
    return diffDays <= 30
  }

  const getDateValidationMessage = () => {
    if (!dialogState.startDate || !dialogState.endDate) {
      return "Both start and end dates are required"
    }
    if (dialogState.startDate > dialogState.endDate) {
      return "Start date must be before or equal to end date"
    }
    
    const diffTime = Math.abs(dialogState.endDate.getTime() - dialogState.startDate.getTime())
    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24))
    if (diffDays > 30) {
      return "Date range cannot exceed 30 days (Amazon API limitation)"
    }
    
    return null
  }

  const canRequestData = () => {
    return dialogState.selectedMarketplace && 
           isDateRangeValid() && 
           !dialogState.isFetchingData
  }

  return (
    <Dialog open={isOpen} onOpenChange={handleDialogClose}>
      <DialogContent className="sm:max-w-2xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Database className="h-5 w-5" />
            Manage Amazon Data
          </DialogTitle>
          <DialogDescription>
            Fetch orders and order items from your Amazon store for analysis and reporting.
          </DialogDescription>
        </DialogHeader>
        
        <div className="space-y-6 py-4">
          {/* Data Fetching Progress */}
          {dialogState.isFetchingData && (
            <div className="space-y-3">
              <div className="flex items-center gap-2">
                <RefreshCw className="h-4 w-4 animate-spin" />
                <span className="text-sm font-medium">Fetching all Amazon orders and items...</span>
              </div>
              <Progress value={dialogState.fetchProgress} className="w-full" />
              <p className="text-xs text-muted-foreground">
                Fetching all orders in the selected date range. This may take several minutes depending on the amount of data. Please don't close this dialog.
              </p>
            </div>
          )}

          {/* Error Message */}
          {dialogState.fetchError && (
            <Alert variant="destructive">
              <AlertCircle className="h-4 w-4" />
              <AlertDescription className="flex items-center justify-between">
                <span>{dialogState.fetchError}</span>
                <Button 
                  variant="ghost" 
                  size="sm" 
                  onClick={() => setDialogState(prev => ({ ...prev, fetchError: undefined }))}
                >
                  <X className="h-4 w-4" />
                </Button>
              </AlertDescription>
            </Alert>
          )}

          {/* Success Message with Results */}
          {dialogState.fetchSuccess && dialogState.fetchedData && (
            <Alert className="border-green-200 bg-green-50 dark:bg-green-950/50">
              <CheckCircle className="h-4 w-4 text-green-600" />
              <AlertDescription>
                <div className="space-y-3">
                  <div className="font-medium text-green-900 dark:text-green-100">
                    Data fetched and processed successfully!
                  </div>
                  
                  {/* Results Summary */}
                  <div className="grid grid-cols-2 gap-4 text-sm">
                    <div className="flex items-center gap-2">
                      <ShoppingCart className="h-4 w-4 text-blue-600" />
                      <span>{dialogState.fetchedData.metadata.total_orders_fetched || 0} Orders</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <Package className="h-4 w-4 text-purple-600" />
                      <span>{dialogState.fetchedData.metadata.total_items_fetched || 0} Items</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <Clock className="h-4 w-4 text-orange-600" />
                      <span>{dialogState.fetchedData.metadata.performance?.total_time_seconds?.toFixed(1) || '0.0'}s total</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <TrendingUp className="h-4 w-4 text-green-600" />
                      <span>{dialogState.fetchedData.metadata.performance?.average_time_per_order?.toFixed(2) || '0.00'}s/order</span>
                    </div>
                  </div>

                  {/* Database Save Status */}
                  {dialogState.fetchedData.processed_data?.database_save?.attempted && (
                    <div className="border-t pt-3">
                      <p className="text-xs font-medium text-muted-foreground mb-2">Database Save Status</p>
                      <div className="space-y-2">
                        {dialogState.fetchedData.processed_data.database_save.success ? (
                          <div className="flex items-center gap-2 text-sm">
                            <CheckCircle className="h-4 w-4 text-green-600" />
                            <span className="text-green-700 font-medium">
                              Auto-saved {dialogState.fetchedData.processed_data.database_save.records_saved} records to databases
                            </span>
                          </div>
                        ) : (
                          <div className="flex items-center gap-2 text-sm">
                            <AlertCircle className="h-4 w-4 text-red-600" />
                            <span className="text-red-700 font-medium">
                              Auto-save failed - data available for manual download
                            </span>
                          </div>
                        )}
                        
                        {/* Database Details */}
                        {dialogState.fetchedData.processed_data.database_save.success && dialogState.fetchedData.processed_data.database_save.details && (
                          <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 mt-2">
                            {/* MSSQL Save Result */}
                            {dialogState.fetchedData.processed_data.database_save.details.mssql_result && (
                              <div className="flex items-center gap-2 text-xs bg-blue-50 dark:bg-blue-950/20 px-2 py-1 rounded">
                                <Database className="h-3 w-3 text-blue-600" />
                                <span className="text-blue-700 dark:text-blue-300">
                                  MSSQL: {dialogState.fetchedData.processed_data.database_save.details.mssql_result.records_saved || 0} records
                                </span>
                              </div>
                            )}
                            
                            {/* Azure Save Result */}
                            {dialogState.fetchedData.processed_data.database_save.details.azure_result && (
                              <div className="flex items-center gap-2 text-xs bg-cyan-50 dark:bg-cyan-950/20 px-2 py-1 rounded">
                                <Database className="h-3 w-3 text-cyan-600" />
                                <span className="text-cyan-700 dark:text-cyan-300">
                                  Azure: {dialogState.fetchedData.processed_data.database_save.details.azure_result.records_saved || 0} records
                                </span>
                              </div>
                            )}
                          </div>
                        )}
                        
                        {/* Error Details */}
                        {!dialogState.fetchedData.processed_data.database_save.success && dialogState.fetchedData.processed_data.database_save.details?.error && (
                          <div className="text-xs text-red-600 bg-red-50 dark:bg-red-950/20 px-2 py-1 rounded">
                            Error: {dialogState.fetchedData.processed_data.database_save.details.error}
                          </div>
                        )}
                      </div>
                    </div>
                  )}

                  {/* Download Options */}
                  <div className="space-y-3 pt-2">
                    {/* Raw Data Downloads */}
                    <div>
                      <p className="text-xs font-medium text-muted-foreground mb-2">Raw Amazon Data</p>
                      <div className="flex flex-wrap gap-2">
                        <Button 
                          size="sm" 
                          variant="outline" 
                          onClick={() => handleDownloadCSV('orders')}
                          className="cursor-pointer"
                        >
                          <Download className="h-4 w-4 mr-2" />
                          Download Orders CSV
                        </Button>
                        <Button 
                          size="sm" 
                          variant="outline" 
                          onClick={() => handleDownloadCSV('items')}
                          className="cursor-pointer"
                        >
                          <Download className="h-4 w-4 mr-2" />
                          Download Items CSV
                        </Button>
                      </div>
                    </div>

                    {/* Processed Data Downloads */}
                    {dialogState.fetchedData.processed_data && (
                      <div>
                        <p className="text-xs font-medium text-muted-foreground mb-2">
                          Processed Data ({dialogState.fetchedData.processed_data.mssql_records + dialogState.fetchedData.processed_data.azure_records} total records)
                        </p>
                        <div className="flex flex-wrap gap-2">
                          <Button 
                            size="sm" 
                            variant="outline" 
                            onClick={() => handleDownloadProcessedData('mssql')}
                            disabled={dialogState.isDownloadingProcessed}
                            className="cursor-pointer"
                          >
                            <Download className="h-4 w-4 mr-2" />
                            {dialogState.isDownloadingProcessed ? 'Downloading...' : `MSSQL Data (${dialogState.fetchedData.processed_data.mssql_records})`}
                          </Button>
                          <Button 
                            size="sm" 
                            variant="outline" 
                            onClick={() => handleDownloadProcessedData('azure')}
                            disabled={dialogState.isDownloadingProcessed}
                            className="cursor-pointer"
                          >
                            <Download className="h-4 w-4 mr-2" />
                            {dialogState.isDownloadingProcessed ? 'Downloading...' : `Azure Data (${dialogState.fetchedData.processed_data.azure_records})`}
                          </Button>
                        </div>
                      </div>
                    )}

                    {/* Processing Error */}
                    {dialogState.fetchedData.processing_error && (
                      <div className="p-2 bg-yellow-50 border border-yellow-200 rounded text-xs">
                        <p className="font-medium text-yellow-800">Processing Note:</p>
                        <p className="text-yellow-700">Raw data was fetched successfully, but processing failed: {dialogState.fetchedData.processing_error}</p>
                      </div>
                    )}
                  </div>
                </div>
              </AlertDescription>
            </Alert>
          )}

          {/* Form Fields - Only show if not currently fetching */}
          {!dialogState.isFetchingData && (
            <>
              {/* Marketplace Selection */}
              <div className="space-y-2">
                <Label htmlFor="marketplace" className="text-sm font-medium">
                  Select Marketplace *
                </Label>
                <Popover 
                  open={dialogState.isMarketplacePopoverOpen} 
                  onOpenChange={(open) => 
                    setDialogState(prev => ({ ...prev, isMarketplacePopoverOpen: open }))
                  }
                >
                  <PopoverTrigger asChild>
                    <Button
                      variant="outline"
                      role="combobox"
                      aria-expanded={dialogState.isMarketplacePopoverOpen}
                      className="w-full justify-between"
                    >
                      {dialogState.selectedMarketplace
                        ? AMAZON_MARKETPLACES.find(marketplace => marketplace.value === dialogState.selectedMarketplace)?.label
                        : "Select marketplace..."}
                      <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                    </Button>
                  </PopoverTrigger>
                  <PopoverContent className="w-full p-0">
                    <Command>
                      <CommandInput placeholder="Search marketplace..." />
                      <CommandList>
                        <CommandEmpty>No marketplace found.</CommandEmpty>
                        <CommandGroup>
                          {AMAZON_MARKETPLACES.map((marketplace) => (
                            <CommandItem
                              key={marketplace.value}
                              value={marketplace.value}
                              onSelect={() => handleMarketplaceSelect(marketplace.value)}
                              disabled={marketplace.disabled}
                              className={marketplace.disabled ? "opacity-50 cursor-not-allowed" : ""}
                            >
                              <Check
                                className={`mr-2 h-4 w-4 ${
                                  dialogState.selectedMarketplace === marketplace.value ? "opacity-100" : "opacity-0"
                                }`}
                              />
                              <div className="flex flex-col">
                                <div className="flex items-center gap-2">
                                  <span className="font-medium">{marketplace.label}</span>
                                  {marketplace.disabled && (
                                    <Badge variant="secondary" className="text-[9px]">
                                      Coming Soon
                                    </Badge>
                                  )}
                                </div>
                                <span className="text-xs text-muted-foreground">{marketplace.domain}</span>
                              </div>
                            </CommandItem>
                          ))}
                        </CommandGroup>
                      </CommandList>
                    </Command>
                  </PopoverContent>
                </Popover>
                <p className="text-xs text-muted-foreground">
                  Choose the Amazon marketplace to fetch data from
                </p>
              </div>

              {/* Date Range Selection */}
              <div className="space-y-3">
                <Label className="text-sm font-medium">Date Range *</Label>
                
                <div className="grid grid-cols-2 gap-3">
                  {/* Start Date */}
                  <div className="space-y-2">
                    <Label htmlFor="startDate" className="text-xs text-muted-foreground">
                      Start Date
                    </Label>
                    <Popover
                      open={dialogState.isStartDatePopoverOpen}
                      onOpenChange={(open) => 
                        setDialogState(prev => ({ ...prev, isStartDatePopoverOpen: open }))
                      }
                    >
                      <PopoverTrigger asChild>
                        <Button
                          variant="outline"
                          className="w-full justify-start text-left font-normal"
                        >
                          <CalendarIcon className="mr-2 h-4 w-4" />
                          {dialogState.startDate ? (
                            format(dialogState.startDate, "MMM dd, yyyy")
                          ) : (
                            <span>Start date</span>
                          )}
                        </Button>
                      </PopoverTrigger>
                      <PopoverContent className="w-auto p-0" align="start">
                        <Calendar
                          mode="single"
                          selected={dialogState.startDate}
                          onSelect={handleStartDateSelect}
                          disabled={(date) => {
                            if (date > new Date()) return true
                            if (dialogState.endDate && date > dialogState.endDate) return true
                            return false
                          }}
                          initialFocus
                        />
                      </PopoverContent>
                    </Popover>
                  </div>

                  {/* End Date */}
                  <div className="space-y-2">
                    <Label htmlFor="endDate" className="text-xs text-muted-foreground">
                      End Date
                    </Label>
                    <Popover
                      open={dialogState.isEndDatePopoverOpen}
                      onOpenChange={(open) => 
                        setDialogState(prev => ({ ...prev, isEndDatePopoverOpen: open }))
                      }
                    >
                      <PopoverTrigger asChild>
                        <Button
                          variant="outline"
                          className="w-full justify-start text-left font-normal"
                        >
                          <CalendarIcon className="mr-2 h-4 w-4" />
                          {dialogState.endDate ? (
                            format(dialogState.endDate, "MMM dd, yyyy")
                          ) : (
                            <span>End date</span>
                          )}
                        </Button>
                      </PopoverTrigger>
                      <PopoverContent className="w-auto p-0" align="start">
                        <Calendar
                          mode="single"
                          selected={dialogState.endDate}
                          onSelect={handleEndDateSelect}
                          disabled={(date) => {
                            if (date > new Date()) return true
                            if (dialogState.startDate && date < dialogState.startDate) return true
                            return false
                          }}
                          initialFocus
                        />
                      </PopoverContent>
                    </Popover>
                  </div>
                </div>

                {/* Date validation message */}
                {getDateValidationMessage() && (
                  <div className="flex items-center gap-2 text-sm text-red-600">
                    <AlertCircle className="h-4 w-4" />
                    <span>{getDateValidationMessage()}</span>
                  </div>
                )}

                <p className="text-xs text-muted-foreground">
                  Select a date range to filter the data. Maximum 30 days allowed due to Amazon API limitations.
                </p>
              </div>

              {/* Auto-save to Database Option */}
              <div className="space-y-3 pt-2 border-t border-muted-foreground/10">
                <div className="flex items-start space-x-3">
                  <Checkbox
                    id="autoSaveToDatabase"
                    checked={dialogState.autoSaveToDatabase}
                    onCheckedChange={(checked) => 
                      setDialogState(prev => ({ 
                        ...prev, 
                        autoSaveToDatabase: checked === true 
                      }))
                    }
                    className="mt-1"
                  />
                  <div className="space-y-1">
                    <Label 
                      htmlFor="autoSaveToDatabase" 
                      className="text-sm font-medium cursor-pointer leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70"
                    >
                      Auto-save to Database
                    </Label>
                    <p className="text-xs text-muted-foreground leading-relaxed">
                      Automatically save fetched orders and items to your database for easy access and reporting. 
                      You can still download CSV files even if this option is disabled.
                    </p>
                  </div>
                </div>
              </div>
            </>
          )}
        </div>

        <DialogFooter className="flex-col sm:flex-row gap-2">
          {/* Show different buttons based on state */}
          {dialogState.showResults ? (
            <>
              <Button 
                variant="outline" 
                onClick={clearResults} 
                className="cursor-pointer"
                disabled={dialogState.isSavingToDatabase}
              >
                <RefreshCw className="h-4 w-4 mr-2" />
                Fetch New Data
              </Button>
              
              {/* Show Save to Database button if data was fetched without auto-save */}
              {dialogState.dataFetchedWithoutAutoSave && (
                <Button 
                  variant="secondary"
                  onClick={handleSaveToDatabase} 
                  className="cursor-pointer"
                  disabled={dialogState.isSavingToDatabase}
                >
                  {dialogState.isSavingToDatabase ? (
                    <>
                      <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
                      Saving to Database...
                    </>
                  ) : (
                    <>
                      <Database className="h-4 w-4 mr-2" />
                      Save to Database
                    </>
                  )}
                </Button>
              )}
              
              <Button 
                onClick={handleDialogClose} 
                className="cursor-pointer"
                disabled={dialogState.isSavingToDatabase}
              >
                <CheckCircle className="h-4 w-4 mr-2" />
                Done
              </Button>
            </>
          ) : (
            <>
              <Button 
                variant="outline" 
                onClick={handleDialogClose} 
                className="cursor-pointer"
                disabled={dialogState.isFetchingData}
              >
                Cancel
              </Button>
              <Button 
                onClick={handleRequestData} 
                disabled={!canRequestData()} 
                className="cursor-pointer"
              >
                {dialogState.isFetchingData ? (
                  <>
                    <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
                    Fetching Data...
                  </>
                ) : (
                  <>
                    <Database className="h-4 w-4 mr-2" />
                    Request Data
                  </>
                )}
              </Button>
            </>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

// Export marketplaces for external use if needed
export { AMAZON_MARKETPLACES }
export type { ManageDataDialogProps } 