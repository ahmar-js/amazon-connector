import { useState } from "react"
import { format } from "date-fns"
import { Button } from "@/components/ui/button"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion"
import {
  XCircle,
  CheckCircle,
  AlertCircle,
  Activity as ActivityIcon,
  Database,
  Copy,
  Info,
  Clock,
  TrendingUp,
  Bug,
  Shield,
  Settings,
  RefreshCw,
  Pause
} from "lucide-react"
import { type Activity } from "@/lib/api"

interface ActivityDetailsModalProps {
  isOpen: boolean
  onClose: () => void
  activity: Activity | null
  loading: boolean
}

const getStatusColor = (status: string) => {
  switch (status) {
    case 'completed':
      return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300'
    case 'failed':
      return 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300'
    case 'in_progress':
      return 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300'
    case 'pending':
      return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-300'
    case 'cancelled':
      return 'bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-300'
    default:
      return 'bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-300'
  }
}

const getStatusIcon = (status: string) => {
  switch (status) {
    case 'completed':
      return <CheckCircle className="h-4 w-4" />
    case 'failed':
      return <XCircle className="h-4 w-4" />
    case 'in_progress':
      return <RefreshCw className="h-4 w-4 animate-spin" />
    case 'pending':
      return <Clock className="h-4 w-4" />
    case 'cancelled':
      return <Pause className="h-4 w-4" />
    default:
      return <AlertCircle className="h-4 w-4" />
  }
}

export function ActivityDetailsModal({ isOpen, onClose, activity, loading }: ActivityDetailsModalProps) {
  const [copySuccess, setCopySuccess] = useState<string | null>(null)

  const copyToClipboard = async (text: string) => {
    try {
      await navigator.clipboard.writeText(text)
      setCopySuccess(text)
      setTimeout(() => setCopySuccess(null), 2000)
    } catch (error) {
      console.error('Failed to copy to clipboard:', error)
    }
  }

  if (!isOpen) return null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="fixed inset-0 bg-black/50" onClick={onClose} />
      <div className="relative bg-white dark:bg-gray-900 rounded-lg shadow-lg max-w-4xl w-full mx-4 max-h-[90vh] overflow-y-auto">
        <div className="p-6">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold">Activity Details</h2>
            <Button variant="ghost" size="sm" onClick={onClose}>
              <XCircle className="h-4 w-4" />
            </Button>
          </div>
          
          {loading ? (
            <div className="space-y-4 py-6">
              <div className="animate-pulse">
                <div className="h-4 bg-gray-200 rounded w-full mb-2"></div>
                <div className="h-4 bg-gray-200 rounded w-3/4 mb-2"></div>
                <div className="h-4 bg-gray-200 rounded w-1/2"></div>
              </div>
            </div>
          ) : activity ? (
            <div className="space-y-6">
              {/* Status Overview Card */}
              <div className="bg-gradient-to-r from-blue-50 to-indigo-50 dark:from-blue-900/20 dark:to-indigo-900/20 p-4 rounded-lg border border-blue-200 dark:border-blue-800">
                <div className="flex items-center justify-between mb-3">
                  <div className="flex items-center gap-2">
                    <ActivityIcon className="h-5 w-5 text-blue-600" />
                    <h3 className="font-semibold text-blue-900 dark:text-blue-100">Activity Overview</h3>
                  </div>
                  <Badge className={`${getStatusColor(activity.status)} px-3 py-1`}>
                    {getStatusIcon(activity.status)}
                    <span className="ml-2">{activity.status_display}</span>
                  </Badge>
                </div>
                
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <div className="flex items-center gap-2">
                      <Copy className="h-4 w-4 text-muted-foreground" />
                      <Label className="text-sm font-medium text-muted-foreground">Activity ID</Label>
                    </div>
                    <div className="flex items-center gap-2">
                      <code className="text-xs bg-white dark:bg-gray-800 px-2 py-1 rounded border font-mono">
                        {activity.activity_id}
                      </code>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-6 w-6 p-0"
                        onClick={() => copyToClipboard(activity.activity_id)}
                      >
                        <Copy className="h-3 w-3" />
                      </Button>
                      {copySuccess === activity.activity_id && (
                        <span className="text-xs text-green-600">Copied!</span>
                      )}
                    </div>
                  </div>
                  
                  <div className="space-y-2">
                    <div className="flex items-center gap-2">
                      <Database className="h-4 w-4 text-muted-foreground" />
                      <Label className="text-sm font-medium text-muted-foreground">Database Status</Label>
                    </div>
                    <div className="space-y-2">
                      {activity.status === 'completed' ? (
                        <div className="space-y-2">
                          {/* MSSQL Status */}
                          <div className="flex items-center gap-2">
                            <span className="text-sm text-muted-foreground min-w-[60px]">MSSQL:</span>
                            {activity.mssql_saved ? (
                              <Badge className="bg-green-800 hover:bg-green-900 text-white text-xs px-2 py-1">
                                <CheckCircle className="mr-1 h-3 w-3" />
                                Saved
                              </Badge>
                            ) : (
                              <Badge variant="destructive" className="text-xs px-2 py-1">
                                <XCircle className="mr-1 h-3 w-3" />
                                Failed
                              </Badge>
                            )}
                          </div>
                          
                          {/* Azure Status */}
                          <div className="flex items-center gap-2">
                            <span className="text-sm text-muted-foreground min-w-[60px]">Azure:</span>
                            {activity.azure_saved ? (
                              <Badge className="bg-green-800 hover:bg-green-900 text-white text-xs px-2 py-1">
                                <CheckCircle className="mr-1 h-3 w-3" />
                                Saved
                              </Badge>
                            ) : (
                              <Badge variant="destructive" className="text-xs px-2 py-1">
                                <XCircle className="mr-1 h-3 w-3" />
                                Failed
                              </Badge>
                            )}
                          </div>
                        </div>
                      ) : (
                        <Badge variant="secondary" className="text-xs px-2 py-1">
                          Not Applicable
                        </Badge>
                      )}
                    </div>
                  </div>
                </div>
              </div>

              {/* Details Grid */}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div className="space-y-4">
                  <h4 className="font-medium text-foreground flex items-center gap-2">
                    <Info className="h-4 w-4 text-blue-500" />
                    Basic Information
                  </h4>
                  
                  <div className="space-y-3">
                    <div className="flex justify-between items-center py-2 border-b border-border/50">
                      <span className="text-sm text-muted-foreground">Marketplace</span>
                      <Badge variant="outline" className="font-medium">
                        {activity.marketplace_name}
                      </Badge>
                    </div>
                    
                    <div className="flex justify-between items-center py-2 border-b border-border/50">
                      <span className="text-sm text-muted-foreground">Activity Type</span>
                      <span className="text-sm font-medium">{activity.activity_type_display}</span>
                    </div>
                    
                    <div className="flex justify-between items-center py-2 border-b border-border/50">
                      <span className="text-sm text-muted-foreground">Action</span>
                      <span className="text-sm font-medium">{activity.action_display}</span>
                    </div>
                  </div>
                </div>
                
                <div className="space-y-4">
                  <h4 className="font-medium text-foreground flex items-center gap-2">
                    <Clock className="h-4 w-4 text-green-500" />
                    Timing & Performance
                  </h4>
                  
                  <div className="space-y-3">
                    <div className="flex justify-between items-center py-2 border-b border-border/50">
                      <span className="text-sm text-muted-foreground">Started</span>
                      <span className="text-sm font-medium">
                        {format(new Date(activity.activity_date), "MMM dd, yyyy 'at' h:mm a")}
                      </span>
                    </div>
                    
                    <div className="flex justify-between items-center py-2 border-b border-border/50">
                      <span className="text-sm text-muted-foreground">Duration</span>
                      <Badge variant="secondary" className="font-mono text-xs">
                        {activity.duration_formatted}
                      </Badge>
                    </div>
                    
                    <div className="flex justify-between items-center py-2 border-b border-border/50">
                      <span className="text-sm text-muted-foreground">Date Range</span>
                      <span className="text-sm font-medium">
                        {format(new Date(activity.date_from), "MMM dd")} - {format(new Date(activity.date_to), "MMM dd")}
                      </span>
                    </div>
                  </div>
                </div>
              </div>

              {/* Performance Metrics */}
              {activity.status === 'completed' && activity.total_records > 0 && (
                <div className="bg-green-50 dark:bg-green-900/20 p-4 rounded-lg border border-green-200 dark:border-green-800">
                  <h4 className="font-medium text-green-900 dark:text-green-100 mb-3 flex items-center gap-2">
                    <TrendingUp className="h-4 w-4" />
                    Data Processing Results
                  </h4>
                  <div className="grid grid-cols-3 gap-4">
                    <div className="text-center">
                      <div className="text-2xl font-bold text-green-700 dark:text-green-300">
                        {activity.total_records.toLocaleString()}
                      </div>
                      <div className="text-xs text-green-600 dark:text-green-400">Total Records</div>
                    </div>
                    <div className="text-center">
                      <div className="text-2xl font-bold text-green-700 dark:text-green-300">
                        {activity.orders_fetched.toLocaleString()}
                      </div>
                      <div className="text-xs text-green-600 dark:text-green-400">Orders</div>
                    </div>
                    <div className="text-center">
                      <div className="text-2xl font-bold text-green-700 dark:text-green-300">
                        {activity.items_fetched.toLocaleString()}
                      </div>
                      <div className="text-xs text-green-600 dark:text-green-400">Items</div>
                    </div>
                  </div>
                </div>
              )}
              
              <div>
                <strong>Details:</strong>
                <div className="mt-2 p-3 bg-gray-100 dark:bg-gray-800 rounded">
                  <p className="text-sm whitespace-pre-wrap">{activity.detail || 'No details available'}</p>
                </div>
              </div>
              
              {(activity.mssql_saved || activity.azure_saved) && activity.detail?.includes('Auto-saved') && (
                <div>
                  <strong>Database Save Details:</strong>
                  <div className="mt-2 space-y-3">
                    {/* MSSQL Database Status */}
                    {activity.mssql_saved ? (
                      <div className="p-3 bg-green-100 dark:bg-green-900 rounded border border-green-200 dark:border-green-800">
                        <div className="flex items-center gap-2 mb-2">
                          <Database className="h-4 w-4 text-green-600" />
                          <span className="text-sm font-medium text-green-800 dark:text-green-200">MSSQL Database - Success</span>
                        </div>
                        <div className="text-xs space-y-1 text-green-700 dark:text-green-300">
                          <p>• Marketplace-specific table updated successfully</p>
                          <p>• All processed records have been permanently stored</p>
                          <p>• Data is available for reporting and analysis</p>
                        </div>
                      </div>
                    ) : (
                      <div className="p-3 bg-red-100 dark:bg-red-900 rounded border border-red-200 dark:border-red-800">
                        <div className="flex items-center gap-2 mb-2">
                          <Database className="h-4 w-4 text-red-600" />
                          <span className="text-sm font-medium text-red-800 dark:text-red-200">MSSQL Database - Failed</span>
                        </div>
                        <div className="text-xs space-y-1 text-red-700 dark:text-red-300">
                          <p>• Unable to save to marketplace-specific table</p>
                          <p>• Data processing completed but storage failed</p>
                          <p>• Please contact support for assistance</p>
                        </div>
                      </div>
                    )}
                    
                    {/* Azure Database Status */}
                    {activity.azure_saved ? (
                      <div className="p-3 bg-green-100 dark:bg-green-900 rounded border border-green-200 dark:border-green-800">
                        <div className="flex items-center gap-2 mb-2">
                          <Database className="h-4 w-4 text-green-600" />
                          <span className="text-sm font-medium text-green-800 dark:text-green-200">Azure Database - Success</span>
                        </div>
                        <div className="text-xs space-y-1 text-green-700 dark:text-green-300">
                          <p>• Central data warehouse updated successfully</p>
                          <p>• All processed records have been permanently stored</p>
                          <p>• Data is available for cross-marketplace analysis</p>
                        </div>
                      </div>
                    ) : (
                      <div className="p-3 bg-red-100 dark:bg-red-900 rounded border border-red-200 dark:border-red-800">
                        <div className="flex items-center gap-2 mb-2">
                          <Database className="h-4 w-4 text-red-600" />
                          <span className="text-sm font-medium text-red-800 dark:text-red-200">Azure Database - Failed</span>
                        </div>
                        <div className="text-xs space-y-1 text-red-700 dark:text-red-300">
                          <p>• Unable to save to central data warehouse</p>
                          <p>• Data processing completed but storage failed</p>
                          <p>• Please contact support for assistance</p>
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              )}
              
              {activity.error_message && (
                <div className="space-y-3">
                  <div className="flex items-center gap-2">
                    <AlertCircle className="h-5 w-5 text-red-500" />
                    <strong className="text-red-700 dark:text-red-400">Error Information</strong>
                  </div>
                  
                  {/* User-friendly error summary */}
                  <div className="p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg">
                    <div className="flex items-start gap-3">
                      <Shield className="h-5 w-5 text-red-500 mt-0.5 flex-shrink-0" />
                      <div>
                        <h4 className="font-medium text-red-800 dark:text-red-200 mb-1">
                          {activity.status === 'failed' ? 'Activity Failed' : 'Error Occurred'}
                        </h4>
                        <p className="text-sm text-red-700 dark:text-red-300">
                          {activity.error_message.includes('API') 
                            ? 'There was an issue connecting to Amazon\'s servers. This could be temporary - please try again later.'
                            : activity.error_message.includes('timeout')
                            ? 'The operation took too long to complete. This may be due to high server load or network issues.'
                            : activity.error_message.includes('authentication') || activity.error_message.includes('credentials')
                            ? 'There was an authentication problem. Please check your Amazon credentials and permissions.'
                            : activity.error_message.includes('rate limit') || activity.error_message.includes('throttle')
                            ? 'Too many requests were made too quickly. Amazon has temporarily limited access - please wait before retrying.'
                            : activity.error_message.includes('permission') || activity.error_message.includes('access')
                            ? 'Your account doesn\'t have the required permissions for this operation. Please contact your administrator.'
                            : 'An unexpected error occurred during the data fetching process. Our technical team has been notified.'
                          }
                        </p>
                        
                        {/* Suggested actions */}
                        <div className="mt-3 p-3 bg-white dark:bg-gray-800 rounded border border-red-200 dark:border-red-700">
                          <h5 className="text-sm font-medium text-gray-900 dark:text-gray-100 mb-2">Suggested Actions:</h5>
                          <ul className="text-xs text-gray-700 dark:text-gray-300 space-y-1">
                            {activity.error_message.includes('API') && (
                              <>
                                <li>• Wait a few minutes and try again</li>
                                <li>• Check Amazon Seller Central for any service disruptions</li>
                              </>
                            )}
                            {activity.error_message.includes('timeout') && (
                              <>
                                <li>• Reduce the date range for your request</li>
                                <li>• Try during off-peak hours</li>
                              </>
                            )}
                            {(activity.error_message.includes('authentication') || activity.error_message.includes('credentials')) && (
                              <>
                                <li>• Verify your Amazon credentials are correct</li>
                                <li>• Check if your access tokens have expired</li>
                                <li>• Ensure your app is registered with Amazon</li>
                              </>
                            )}
                            {(activity.error_message.includes('rate limit') || activity.error_message.includes('throttle')) && (
                              <>
                                <li>• Wait 15-30 minutes before retrying</li>
                                <li>• Reduce the frequency of your requests</li>
                              </>
                            )}
                            {(activity.error_message.includes('permission') || activity.error_message.includes('access')) && (
                              <>
                                <li>• Contact your system administrator</li>
                                <li>• Verify your Amazon account permissions</li>
                              </>
                            )}
                            <li>• Contact support if the problem persists</li>
                          </ul>
                        </div>
                      </div>
                    </div>
                  </div>

                  {/* Technical details accordion */}
                  <Accordion type="single" collapsible className="w-full">
                    <AccordionItem value="technical-details" className="border border-gray-200 dark:border-gray-700 rounded-lg">
                      <AccordionTrigger className="px-4 py-3 hover:bg-gray-50 dark:hover:bg-gray-800 rounded-t-lg">
                        <div className="flex items-center gap-2">
                          <Bug className="h-4 w-4 text-orange-500" />
                          <span className="font-medium">Technical Details</span>
                          <Badge variant="secondary" className="text-xs">For Developers</Badge>
                        </div>
                      </AccordionTrigger>
                      <AccordionContent className="px-4 pb-4">
                        <div className="space-y-3">
                          <div className="bg-gray-50 dark:bg-gray-800 p-3 rounded border">
                            <h6 className="text-sm font-medium text-gray-900 dark:text-gray-100 mb-2">Raw Error Message:</h6>
                            <code className="text-xs bg-gray-100 dark:bg-gray-900 p-2 rounded block whitespace-pre-wrap text-red-600 dark:text-red-400 font-mono">
                              {activity.error_message}
                            </code>
                          </div>
                          
                          <div className="bg-blue-50 dark:bg-blue-900/20 p-3 rounded border border-blue-200 dark:border-blue-800">
                            <h6 className="text-sm font-medium text-blue-900 dark:text-blue-100 mb-2">Debug Information:</h6>
                            <div className="text-xs text-blue-800 dark:text-blue-200 space-y-1">
                              <div><strong>Activity ID:</strong> {activity.activity_id}</div>
                              <div><strong>Marketplace:</strong> {activity.marketplace_name}</div>
                              <div><strong>Error Time:</strong> {format(new Date(activity.activity_date), "PPpp")}</div>
                              <div><strong>Duration:</strong> {activity.duration_formatted}</div>
                              <div><strong>Activity Type:</strong> {activity.activity_type_display}</div>
                            </div>
                          </div>

                          <div className="flex items-center gap-2 p-3 bg-yellow-50 dark:bg-yellow-900/20 rounded border border-yellow-200 dark:border-yellow-800">
                            <Settings className="h-4 w-4 text-yellow-600" />
                            <div className="text-xs text-yellow-800 dark:text-yellow-200">
                              <strong>Note:</strong> This information can help technical support diagnose and resolve the issue more quickly.
                            </div>
                          </div>
                        </div>
                      </AccordionContent>
                    </AccordionItem>
                  </Accordion>
                </div>
              )}
            </div>
          ) : (
            <p>No activity details available</p>
          )}
        </div>
      </div>
    </div>
  )
} 