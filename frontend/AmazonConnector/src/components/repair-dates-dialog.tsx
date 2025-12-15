import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { CheckCircle, AlertCircle, Info, Calendar, Trash2, X } from "lucide-react"
import { format } from "date-fns"

interface MarketplaceFixed {
  marketplace: string
  rows_deleted_mssql: number
  rows_deleted_azure: number
  new_last_run: string
}

interface MarketplaceError {
  marketplace: string
  error: string
}

interface RepairSummary {
  total_marketplaces_processed: number
  total_rows_deleted: number
  marketplaces_fixed: MarketplaceFixed[]
  marketplaces_with_errors: MarketplaceError[]
  marketplaces_no_anomalies: string[]
}

interface RepairDatesDialogProps {
  isOpen: boolean
  onOpenChange: (open: boolean) => void
  summary?: RepairSummary
  error?: string
}

export function RepairDatesDialog({ isOpen, onOpenChange, summary, error }: RepairDatesDialogProps) {
  const formatDateTime = (dateString: string) => {
    try {
      // Parse ISO datetime string (2023-10-31T23:59:59Z format)
      const date = new Date(dateString)
      return format(date, "MMM dd, yyyy 'at' HH:mm:ss")
    } catch (e) {
      return dateString
    }
  }

  const getMarketplaceName = (code: string) => {
    const names: Record<string, string> = {
      'usa': 'United States 🇺🇸',
      'ca': 'Canada 🇨🇦',
      'uk': 'United Kingdom 🇬🇧',
      'de': 'Germany 🇩🇪',
      'fr': 'France 🇫🇷',
      'it': 'Italy 🇮🇹',
      'es': 'Spain 🇪🇸'
    }
    return names[code.toLowerCase()] || code.toUpperCase()
  }

  return (
    <Dialog open={isOpen} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl max-h-[80vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Calendar className="h-5 w-5" />
            Purchase Dates Repair Results
          </DialogTitle>
          <DialogDescription>
            Summary of the date anomaly detection and repair operation
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          {/* Error Alert */}
          {error && (
            <Alert variant="destructive">
              <AlertCircle className="h-4 w-4" />
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}

          {/* Success Summary */}
          {summary && !error && (
            <>
              {/* Overview Stats */}
              <div className="grid grid-cols-3 gap-4">
                <div className="bg-blue-50 dark:bg-blue-950/20 p-4 rounded-lg border border-blue-200 dark:border-blue-800">
                  <div className="text-sm font-medium text-blue-900 dark:text-blue-100">Marketplaces Processed</div>
                  <div className="text-2xl font-bold text-blue-700 dark:text-blue-300 mt-1">
                    {summary.total_marketplaces_processed}
                  </div>
                </div>
                
                <div className="bg-red-50 dark:bg-red-950/20 p-4 rounded-lg border border-red-200 dark:border-red-800">
                  <div className="text-sm font-medium text-red-900 dark:text-red-100">Total Rows Deleted</div>
                  <div className="text-2xl font-bold text-red-700 dark:text-red-300 mt-1">
                    {summary.total_rows_deleted}
                  </div>
                </div>
                
                <div className="bg-green-50 dark:bg-green-950/20 p-4 rounded-lg border border-green-200 dark:border-green-800">
                  <div className="text-sm font-medium text-green-900 dark:text-green-100">Successfully Fixed</div>
                  <div className="text-2xl font-bold text-green-700 dark:text-green-300 mt-1">
                    {summary.marketplaces_fixed.length}
                  </div>
                </div>
              </div>

              {/* Marketplaces Fixed */}
              {summary.marketplaces_fixed.length > 0 && (
                <div className="space-y-2">
                  <h3 className="font-semibold flex items-center gap-2 text-green-700 dark:text-green-400">
                    <CheckCircle className="h-4 w-4" />
                    Successfully Repaired Marketplaces ({summary.marketplaces_fixed.length})
                  </h3>
                  <div className="space-y-3">
                    {summary.marketplaces_fixed.map((marketplace, index) => (
                      <div 
                        key={index} 
                        className="bg-green-50 dark:bg-green-950/20 border border-green-200 dark:border-green-800 rounded-lg p-4"
                      >
                        <div className="flex items-start justify-between mb-3">
                          <div className="font-medium text-green-900 dark:text-green-100">
                            {getMarketplaceName(marketplace.marketplace)}
                          </div>
                          <Badge variant="outline" className="bg-green-100 dark:bg-green-900 text-green-700 dark:text-green-300 border-green-300 dark:border-green-700">
                            Fixed
                          </Badge>
                        </div>
                        
                        <div className="grid grid-cols-2 gap-3 text-sm">
                          <div className="flex items-center gap-2">
                            <Trash2 className="h-4 w-4 text-red-500" />
                            <span className="text-gray-700 dark:text-gray-300">
                              MSSQL: <strong>{marketplace.rows_deleted_mssql}</strong> rows deleted
                            </span>
                          </div>
                          <div className="flex items-center gap-2">
                            <Trash2 className="h-4 w-4 text-red-500" />
                            <span className="text-gray-700 dark:text-gray-300">
                              Azure: <strong>{marketplace.rows_deleted_azure}</strong> rows deleted
                            </span>
                          </div>
                        </div>
                        
                        <div className="mt-3 pt-3 border-t border-green-200 dark:border-green-800">
                          <div className="flex items-center gap-2 text-sm">
                            <Calendar className="h-4 w-4 text-green-600 dark:text-green-400" />
                            <span className="text-gray-700 dark:text-gray-300">
                              Last Run Date: <strong className="text-green-700 dark:text-green-400">{formatDateTime(marketplace.new_last_run)}</strong>
                            </span>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Marketplaces with No Anomalies */}
              {summary.marketplaces_no_anomalies.length > 0 && (
                <div className="space-y-2">
                  <h3 className="font-semibold flex items-center gap-2 text-blue-700 dark:text-blue-400">
                    <Info className="h-4 w-4" />
                    No Anomalies Found ({summary.marketplaces_no_anomalies.length})
                  </h3>
                  <div className="flex flex-wrap gap-2">
                    {summary.marketplaces_no_anomalies.map((marketplace, index) => (
                      <Badge 
                        key={index} 
                        variant="outline"
                        className="bg-blue-50 dark:bg-blue-950/20 text-blue-700 dark:text-blue-300 border-blue-300 dark:border-blue-700"
                      >
                        {getMarketplaceName(marketplace)}
                      </Badge>
                    ))}
                  </div>
                </div>
              )}

              {/* Marketplaces with Errors */}
              {summary.marketplaces_with_errors.length > 0 && (
                <div className="space-y-2">
                  <h3 className="font-semibold flex items-center gap-2 text-amber-700 dark:text-amber-400">
                    <AlertCircle className="h-4 w-4" />
                    Marketplaces with Errors ({summary.marketplaces_with_errors.length})
                  </h3>
                  <div className="space-y-2">
                    {summary.marketplaces_with_errors.map((marketplace, index) => (
                      <Alert key={index} variant="default" className="bg-amber-50 dark:bg-amber-950/20 border-amber-300 dark:border-amber-800">
                        <AlertCircle className="h-4 w-4 text-amber-600 dark:text-amber-400" />
                        <AlertDescription className="text-amber-900 dark:text-amber-100">
                          <strong>{getMarketplaceName(marketplace.marketplace)}:</strong> {marketplace.error}
                        </AlertDescription>
                      </Alert>
                    ))}
                  </div>
                </div>
              )}

              {/* Success Message */}
              {summary.marketplaces_fixed.length > 0 && (
                <Alert className="bg-green-50 dark:bg-green-950/20 border-green-300 dark:border-green-700">
                  <CheckCircle className="h-4 w-4 text-green-600 dark:text-green-400" />
                  <AlertDescription className="text-green-900 dark:text-green-100">
                    Successfully repaired {summary.marketplaces_fixed.length} marketplace{summary.marketplaces_fixed.length !== 1 ? 's' : ''} and removed {summary.total_rows_deleted} anomalous record{summary.total_rows_deleted !== 1 ? 's' : ''}.
                  </AlertDescription>
                </Alert>
              )}

              {/* No Changes Message */}
              {summary.marketplaces_fixed.length === 0 && summary.marketplaces_with_errors.length === 0 && (
                <Alert className="bg-blue-50 dark:bg-blue-950/20 border-blue-300 dark:border-blue-700">
                  <Info className="h-4 w-4 text-blue-600 dark:text-blue-400" />
                  <AlertDescription className="text-blue-900 dark:text-blue-100">
                    No date anomalies were found in any marketplace. Your data is clean!
                  </AlertDescription>
                </Alert>
              )}
            </>
          )}
        </div>

        <DialogFooter>
          <Button onClick={() => onOpenChange(false)} variant="default" className="cursor-pointer">
            Close
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
