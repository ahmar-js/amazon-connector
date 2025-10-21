import { useState, useEffect, useMemo, type JSX } from "react"
import { format } from "date-fns"
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  getFilteredRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  useReactTable,
} from "@tanstack/react-table"
import type { 
  ColumnFiltersState,
  SortingState,
  VisibilityState,
} from "@tanstack/react-table"
import { Button } from "@/components/ui/button"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { Progress } from "@/components/ui/progress"
import { Skeleton } from "@/components/ui/skeleton"
import {
  ArrowUpDown,
  ChevronDown,
  Filter,
  RefreshCw,
  Eye,
  Calendar,
  Clock,
  CheckCircle,
  XCircle,
  AlertCircle,
  Pause,
  MoreHorizontal,
  TrendingUp,
  Activity as ActivityIcon,
  Database,
  FileText,
  BarChart3
} from "lucide-react"
import {
  AmazonConnectorService,
  type Activity,
  type ActivitiesListRequest,
  type ActivityStats,
  ApiError
} from "@/lib/api"
import { ActivityDetailsModal } from "./activity-details-modal"
import MARKETPLACES from '@/lib/marketplaces'

// Status color mapping
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

// Status icon mapping
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

// Marketplace options for filtering (derived from centralized mapping)
const MARKETPLACE_OPTIONS = Object.values(MARKETPLACES).map(m => ({ value: m.id, label: m.name || m.code, disabled: m.disabled }))

interface ActivitiesTableProps {
  className?: string
  refreshTrigger?: number // Add a trigger prop to force refresh
}

// Create column helper
const columnHelper = createColumnHelper<Activity>()

export function ActivitiesTable({ className, refreshTrigger }: ActivitiesTableProps) {
  // State management
  const [activities, setActivities] = useState<Activity[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [stats, setStats] = useState<ActivityStats | null>(null)
  const [statsLoading, setStatsLoading] = useState(true)
  
  // Table state
  const [sorting, setSorting] = useState<SortingState>([])
  const [columnFilters, setColumnFilters] = useState<ColumnFiltersState>([])
  const [columnVisibility, setColumnVisibility] = useState<VisibilityState>({})
  const [rowSelection, setRowSelection] = useState({})
  const [pagination, setPagination] = useState({
    pageIndex: 0,
    pageSize: 10,
  })
  
  // Filter state
  const [statusFilter, setStatusFilter] = useState<string>("")
  const [marketplaceFilter, setMarketplaceFilter] = useState<string>("")
  
  // Pagination state from API
  const [totalItems, setTotalItems] = useState(0)
  const [totalPages, setTotalPages] = useState(0)
  const [currentPage, setCurrentPage] = useState(1)
  
  // Modal state
  const [detailsModalOpen, setDetailsModalOpen] = useState(false)
  const [selectedActivity, setSelectedActivity] = useState<Activity | null>(null)
  const [loadingDetails, setLoadingDetails] = useState(false)

  // Event handlers
  const handleRefresh = () => {
    loadActivities(currentPage)
    loadStats()
  }

  const handleViewDetails = async (activityId: string) => {
    console.log('🔍 handleViewDetails called for:', activityId)
    try {
      console.log('📝 Setting modal states...')
      setLoadingDetails(true)
      setDetailsModalOpen(true)
      console.log('✅ Modal should be open now')
      
      // Fetch detailed activity data
      const response = await AmazonConnectorService.getActivityDetail(activityId)
      
      if (response.success && response.data) {
        console.log('✅ Setting selected activity:', response.data.activity_id)
        setSelectedActivity(response.data)
      } else {
        console.error('❌ Failed to get activity details:', response.error)
        setError(response.error || 'Failed to load activity details')
        setDetailsModalOpen(false)
      }
    } catch (error) {
      console.error('❌ Error loading activity details:', error)
      if (error instanceof ApiError) {
        setError(error.message)
      } else {
        setError('Failed to load activity details')
      }
      setDetailsModalOpen(false)
    } finally {
      setLoadingDetails(false)
      console.log('🏁 handleViewDetails completed')
    }
  }

  const handleViewError = (activity: Activity) => {
    // Show error details in the same modal
    setSelectedActivity(activity)
    setDetailsModalOpen(true)
  }
  
  const handleCloseDetailsModal = () => {
    setDetailsModalOpen(false)
    setSelectedActivity(null)
    setLoadingDetails(false)
  }
  
  

  const handleFilterChange = () => {
    setCurrentPage(1)
    setPagination(prev => ({ ...prev, pageIndex: 0 }))
    loadActivities(1)
  }

  // Column definitions using createColumnHelper
  const columns = useMemo(
    () => [
      columnHelper.accessor("activity_date", {
        header: ({ column }) => {
          return (
            <Button
              variant="ghost"
              onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}
              className="h-8 p-0 hover:bg-transparent w-full justify-center"
            >
              <Calendar className="h-4 w-4" />
              Date
              <ArrowUpDown className="h-4 w-4" />
            </Button>
          )
        },
        size: 140,
        cell: ({ row }) => {
          const dateValue = row.getValue("activity_date") as string
          if (!dateValue || dateValue === 'null' || dateValue === 'undefined') {
            return (
              <div className="flex flex-col">
                <span className="font-medium text-muted-foreground">
                  No date
                </span>
              </div>
            )
          }
          
          try {
            const date = new Date(dateValue)
            if (isNaN(date.getTime())) {
              return (
                <div className="flex flex-col">
                  <span className="font-medium text-muted-foreground">
                    Invalid date
                  </span>
                </div>
              )
            }
            
            return (
              <div className="flex flex-col items-center">
                <span className="font-medium">
                  {format(date, "MMM dd, yyyy")}
                </span>
                <span className="text-xs text-muted-foreground">
                  {format(date, "hh:mm:ss a")}
                </span>
              </div>
            )
          } catch (error) {
            return (
              <div className="flex flex-col">
                <span className="font-medium text-muted-foreground">
                  Invalid date
                </span>
              </div>
            )
          }
        },
      }),
      columnHelper.accessor("marketplace_name", {
        header: ({ column }) => {
          return (
            <Button
              variant="ghost"
              onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}
              className="h-8 p-0 hover:bg-transparent w-full justify-center"
            >
              <Database className="h-4 w-4 shrink-0" />
              <span className="">Marketplace</span>
              <ArrowUpDown className="h-4 w-4 shrink-0" />
            </Button>
          )
        },
        size: 70,
        minSize: 60,
        maxSize: 90,
        cell: ({ row }) => {
          return (
            <div className="flex flex-col items-center">
              <Badge variant="outline" className="font-mono text-xs max-w-[80px] overflow-hidden text-ellipsis whitespace-nowrap">
                {row.getValue("marketplace_name")}
              </Badge>
            </div>
          )
        },
      }),

      columnHelper.accessor("status", {
        header: ({ column }) => {
          return (
            <Button
              variant="ghost"
              onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}
              className="h-8 p-0 hover:bg-transparent w-full justify-center"
            >
              <ActivityIcon className="h-4 w-4" />
              Status
              <ArrowUpDown className="h-4 w-4" />
            </Button>
          )
        },
        size: 110,
        cell: ({ row }) => {
          const status = row.getValue("status") as string
          const statusDisplay = row.original.status_display
          
          return (
            <div className="flex flex-col items-center">
              <Badge className={getStatusColor(status)}>
                {getStatusIcon(status)}
                <span className="ml-1">{statusDisplay}</span>
              </Badge>
            </div>
          )
        },
      }),
            columnHelper.accessor("activity_type", {
        header: ({ column }) => {
          return (
            <Button
              variant="ghost"
              onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}
              className="h-8 p-0 hover:bg-transparent w-full justify-center"
            >
              <ActivityIcon className="h-4 w-4 shrink-0" />
              <span className="">Type</span>
              <ArrowUpDown className="h-4 w-4 shrink-0" />
            </Button>
          )
        },
        size: 90,
        minSize: 80,
        maxSize: 110,
        cell: ({ row }) => {
          return (
            <div className="flex flex-col items-center">
              <Badge variant="outline" className="font-mono text-xs max-w-[80px] overflow-hidden text-ellipsis whitespace-nowrap">
                {row.getValue("activity_type")}
              </Badge>
            </div>
          )
        },
      }),
      columnHelper.display({
        id: "date_range",
        header: () => (
          <div className="flex items-center justify-center w-full">
            <Calendar className="mr-2 h-4 w-4 shrink-0" />
            <span>Date Range</span>
          </div>
        ),
        size: 120,
        cell: ({ row }) => {
          const dateFromValue = row.original.date_from as string
          const dateToValue = row.original.date_to as string
          
          if (!dateFromValue || !dateToValue || 
              dateFromValue === 'null' || dateToValue === 'null' ||
              dateFromValue === 'undefined' || dateToValue === 'undefined') {
            return (
              <div className="flex flex-col text-sm">
                <span className="text-muted-foreground">No date range</span>
              </div>
            )
          }
          
          try {
            const dateFrom = new Date(dateFromValue)
            const dateTo = new Date(dateToValue)
            
            if (isNaN(dateFrom.getTime()) || isNaN(dateTo.getTime())) {
              return (
                <div className="flex flex-col text-sm">
                  <span className="text-muted-foreground">Invalid date range</span>
                </div>
              )
            }
            
            return (
              <div className="flex flex-col text-sm items-center">
                <span>{format(dateFrom, "MMM dd")} - {format(dateTo, "MMM dd")}</span>
                <span className="text-xs text-muted-foreground">
                  {Math.ceil((dateTo.getTime() - dateFrom.getTime()) / (1000 * 60 * 60 * 24))} days
                </span>
              </div>
            )
          } catch (error) {
            return (
              <div className="flex flex-col text-sm">
                <span className="text-muted-foreground">Invalid date range</span>
              </div>
            )
          }
        },
      }),
      columnHelper.accessor("total_records", {
        header: ({ column }) => {
          return (
            <Button
              variant="ghost"
              onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}
              className="h-8 p-0 hover:bg-transparent w-full justify-center"
            >
              <FileText className="h-4 w-4" />
              Records
              <ArrowUpDown className="h-4 w-4" />
            </Button>
          )
        },
        size: 130,
        cell: ({ row }) => {
          const orders = row.original.orders_fetched
          const items = row.original.items_fetched
          const total = row.getValue("total_records") as number
          
          return (
            <div className="flex flex-col text-sm items-center">
              <span className="font-medium">{total.toLocaleString()}</span>
              <span className="text-xs text-muted-foreground">
                {orders.toLocaleString()} orders, {items.toLocaleString()} items
              </span>
            </div>
          )
        },
      }),
      columnHelper.accessor("duration_formatted", {
        header: ({ column }) => {
          return (
            <Button
              variant="ghost"
              onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}
              className="h-8 p-0 hover:bg-transparent w-full justify-center"
            >
              <Clock className="h-4 w-4" />
              Duration
              <ArrowUpDown className="h-4 w-4" />
            </Button>
          )
        },
        size: 50,
        cell: ({ row }) => {
          const duration = row.getValue("duration_formatted") as string
          return (
            <div className="flex flex-col items-center">
              <Badge variant="secondary" className="font-mono">
                {duration}
              </Badge>
            </div>
          )
        },
      }),
      // columnHelper.accessor("detail", {
      //   header: "Details",
      //   size: 100,
      //   cell: ({ row }) => {
      //     const detail = row.getValue("detail") as string
      //     const truncated = detail.length > 20 ? detail.substring(0, 20) + "..." : detail
          
      //     return (
      //       <div className="max-w-[300px] min-w-[100px]">
      //         <span className="text-sm text-wrap break-words" title={detail}>
      //           {truncated}
      //         </span>
      //       </div>
      //     )
      //   },
      // }),
      columnHelper.accessor("database_saved", {
        header: ({ column }) => {
          return (
            <Button
              variant="ghost"
              onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}
              className="h-8 p-0 hover:bg-transparent"
            >
              <Database className=" h-4 w-4" />
              DB Saved
              <ArrowUpDown className=" h-4 w-4" />
            </Button>
          )
        },
        size: 80,
        minSize: 60,
        maxSize: 100,
        cell: ({ row }) => {
          const activity = row.original
          const status = activity.status
          
          // Only show database save status for completed activities
          if (status !== 'completed') {
            return (
              <div className="flex items-center justify-center w-full">
                <span className="text-xs text-muted-foreground">N/A</span>
              </div>
            )
          }
          
          // Determine save status based on individual database results
          const mssqlSaved = activity.mssql_saved
          const azureSaved = activity.azure_saved
          
          let badgeClass: string
          let badgeVariant: "default" | "destructive" | "secondary"
          let icon: JSX.Element
          let text: string
          
          if (mssqlSaved && azureSaved) {
            // Both databases saved successfully
            badgeVariant = "default"
            badgeClass = "bg-green-500 hover:bg-green-600 text-xs"
            icon = <CheckCircle className="mr-1 h-3 w-3" />
            text = "Yes"
          } else if (!mssqlSaved && !azureSaved) {
            // Neither database saved
            badgeVariant = "destructive"
            badgeClass = "text-xs"
            icon = <XCircle className="mr-1 h-3 w-3" />
            text = "No"
          } else {
            // Partial save (one succeeded, one failed)
            badgeVariant = "secondary"
            badgeClass = "bg-yellow-500 hover:bg-yellow-600 text-white text-xs"
            icon = <AlertCircle className="mr-1 h-3 w-3" />
            text = "Partial"
          }
          
          return (
            <div className="flex items-center justify-center w-full">
              <Badge variant={badgeVariant} className={badgeClass}>
                {icon}
                {text}
              </Badge>
            </div>
          )
        },
      }),
      columnHelper.display({
        id: "actions",
        size: 60,
        cell: ({ row }) => {
          const activity = row.original
          
          return (
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button 
                  variant="ghost" 
                  className="h-8 w-8 p-0 cursor-pointer hover:bg-muted/50 transition-colors duration-200 rounded-md"
                >
                  <span className="sr-only">Open menu</span>
                  <MoreHorizontal className="h-4 w-4 text-muted-foreground hover:text-foreground transition-colors" />
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end" className="w-56 shadow-lg border border-border/50">
                <div className="px-2 py-1.5 text-sm font-medium text-muted-foreground border-b border-border/50">
                  Activity Actions
                </div>
                <DropdownMenuCheckboxItem
                  onClick={() => handleViewDetails(activity.activity_id)}
                  className="cursor-pointer hover:bg-muted/50 transition-colors duration-200 py-2.5"
                >
                  <Eye className="mr-3 h-4 w-4 text-blue-500" />
                  <div className="flex flex-col">
                    <span className="font-medium">View Details</span>
                    <span className="text-xs text-muted-foreground">Complete activity information</span>
                  </div>
                </DropdownMenuCheckboxItem>
                {activity.error_message && (
                  <DropdownMenuCheckboxItem
                    onClick={() => handleViewError(activity)}
                    className="cursor-pointer hover:bg-red-50 dark:hover:bg-red-900/20 transition-colors duration-200 py-2.5"
                  >
                    <AlertCircle className="mr-3 h-4 w-4 text-red-500" />
                    <div className="flex flex-col">
                      <span className="font-medium text-red-700 dark:text-red-400">View Error</span>
                      <span className="text-xs text-red-600 dark:text-red-500">Diagnostic information</span>
                    </div>
                  </DropdownMenuCheckboxItem>
                )}
                {/* {activity.status === 'completed' && activity.total_records > 0 && (
                  <DropdownMenuCheckboxItem
                    onClick={() => window.open(`/api/activities/${activity.activity_id}/download`, '_blank')}
                    className="cursor-pointer hover:bg-green-50 dark:hover:bg-green-900/20 transition-colors duration-200 py-2.5"
                  >
                    <ExternalLink className="mr-3 h-4 w-4 text-green-500" />
                    <div className="flex flex-col">
                      <span className="font-medium text-green-700 dark:text-green-400">Download Data</span>
                      <span className="text-xs text-green-600 dark:text-green-500">{activity.total_records.toLocaleString()} records</span>
                    </div>
                  </DropdownMenuCheckboxItem>
                )} */}
              </DropdownMenuContent>
            </DropdownMenu>
          )
        },
      }),
    ],
    []
  )

  // Table instance
  const table = useReactTable({
    data: activities,
    columns,
    onSortingChange: setSorting,
    onColumnFiltersChange: setColumnFilters,
    getCoreRowModel: getCoreRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    onColumnVisibilityChange: setColumnVisibility,
    onRowSelectionChange: setRowSelection,
    state: {
      sorting,
      columnFilters,
      columnVisibility,
      rowSelection,
      pagination,
    },
    manualPagination: true,
    pageCount: totalPages,
    columnResizeMode: 'onChange',
    enableColumnResizing: true,
  })

  // Load activities data
  const loadActivities = async (page: number = 1) => {
    try {
      setLoading(true)
      setError(null)
      
      const request: ActivitiesListRequest = {
        page,
        page_size: pagination.pageSize,
        ...(statusFilter && { status: statusFilter }),
        ...(marketplaceFilter && { marketplace_id: marketplaceFilter }),
      }
      
      const response = await AmazonConnectorService.getActivities(request)
      
      if (response.success && response.data) {
        setActivities(response.data.activities)
        setTotalItems(response.data.pagination.total_items)
        setTotalPages(response.data.pagination.total_pages)
        setCurrentPage(response.data.pagination.current_page)
      } else {
        setError(response.error || 'Failed to load activities')
      }
    } catch (error) {
      console.error('Error loading activities:', error)
      if (error instanceof ApiError) {
        setError(error.message)
      } else {
        setError('Failed to load activities')
      }
    } finally {
      setLoading(false)
    }
  }

  // Load activity statistics
  const loadStats = async () => {
    try {
      setStatsLoading(true)
      const response = await AmazonConnectorService.getActivityStats({ days: 30 })
      
      if (response.success && response.data) {
        setStats(response.data)
      }
    } catch (error) {
      console.error('Error loading activity stats:', error)
    } finally {
      setStatsLoading(false)
    }
  }

  // Effects
  useEffect(() => {
    loadActivities()
    loadStats()
  }, [])

  useEffect(() => {
    handleFilterChange()
  }, [statusFilter, marketplaceFilter])

  // Refresh when trigger changes (for real-time updates)
  useEffect(() => {
    if (refreshTrigger && refreshTrigger > 0) {
      loadActivities(currentPage)
      loadStats()
    }
  }, [refreshTrigger])

  // Debug modal state changes
  useEffect(() => {
    console.log('🎭 Modal state changed:', { detailsModalOpen, selectedActivity: selectedActivity?.activity_id, loadingDetails });
  }, [detailsModalOpen, selectedActivity, loadingDetails])

  // Render loading skeleton
  if (loading && activities.length === 0) {
    return (
      <div className={`space-y-6 ${className}`}>
        {/* Stats skeleton */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          {[...Array(4)].map((_, i) => (
            <Card key={i}>
              <CardHeader className="pb-2">
                <Skeleton className="h-4 w-24" />
              </CardHeader>
              <CardContent>
                <Skeleton className="h-8 w-16 mb-2" />
                <Skeleton className="h-3 w-20" />
              </CardContent>
            </Card>
          ))}
        </div>
        
        {/* Table skeleton */}
        <Card>
          <CardHeader>
            <Skeleton className="h-6 w-32" />
            <Skeleton className="h-4 w-64" />
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {[...Array(5)].map((_, i) => (
                <Skeleton key={i} className="h-12 w-full" />
              ))}
            </div>
          </CardContent>
        </Card>

        <ActivityDetailsModal
          isOpen={detailsModalOpen}
          onClose={handleCloseDetailsModal}
          activity={selectedActivity}
          loading={loadingDetails}
        />
      </div>
    )
  }

  return (
    <div className={`space-y-6 ${className}`}>
      {/* Statistics Cards */}
      {stats && !statsLoading && (
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Total Activities</CardTitle>
              <ActivityIcon className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{stats.summary.total_activities}</div>
              <p className="text-xs text-muted-foreground">
                Last 30 days
              </p>
            </CardContent>
          </Card>
          
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Success Rate</CardTitle>
              <TrendingUp className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{stats.summary.success_rate}%</div>
              <Progress value={stats.summary.success_rate} className="mt-2" />
            </CardContent>
          </Card>
          
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Orders Processed</CardTitle>
              <FileText className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{stats.summary.total_orders_processed.toLocaleString()}</div>
              <p className="text-xs text-muted-foreground">
                {stats.summary.total_items_processed.toLocaleString()} items
              </p>
            </CardContent>
          </Card>
          
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Avg Duration</CardTitle>
              <Clock className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{stats.summary.average_duration_formatted || 'N/A'}</div>
              <p className="text-xs text-muted-foreground">
                Per activity
              </p>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Main Table Card */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="flex items-center gap-2">
                <BarChart3 className="h-5 w-5" />
                Activity History
              </CardTitle>
              <CardDescription>
                Track and monitor all data fetching activities
              </CardDescription>
            </div>
            <Button onClick={handleRefresh} disabled={loading} className="cursor-pointer">
              <RefreshCw className={`h-4 w-4 mr-2 ${loading ? 'animate-spin' : ''}`} />
              Refresh
            </Button>
          </div>
        </CardHeader>
        
        <CardContent>
          {/* Error Alert */}
          {error && (
            <Alert variant="destructive" className="mb-4">
              <AlertCircle className="h-4 w-4" />
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}

          {/* Filters */}
          <div className="flex items-center gap-4 mb-4">
            <Select value={statusFilter || "all"} onValueChange={(value) => setStatusFilter(value === "all" ? "" : value)}>
              <SelectTrigger className="w-[150px]">
                <SelectValue placeholder="All Status" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Status</SelectItem>
                <SelectItem value="completed">Completed</SelectItem>
                <SelectItem value="failed">Failed</SelectItem>
                <SelectItem value="in_progress">In Progress</SelectItem>
                <SelectItem value="pending">Pending</SelectItem>
                <SelectItem value="cancelled">Cancelled</SelectItem>
              </SelectContent>
            </Select>
            
            <Select value={marketplaceFilter || "all"} onValueChange={(value) => setMarketplaceFilter(value === "all" ? "" : value)}>
              <SelectTrigger className="w-[180px]">
                <SelectValue placeholder="All Marketplaces" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Marketplaces</SelectItem>
                {MARKETPLACE_OPTIONS.map((marketplace) => (
                  <SelectItem key={marketplace.value} value={marketplace.value}>
                    {marketplace.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>

            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="outline" className="cursor-pointer">
                  <Filter className="mr-2 h-4 w-4" />
                  Columns
                  <ChevronDown className="ml-2 h-4 w-4" />
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                {table
                  .getAllColumns()
                  .filter((column) => column.getCanHide())
                  .map((column) => {
                    return (
                      <DropdownMenuCheckboxItem
                        key={column.id}
                        className="capitalize"
                        checked={column.getIsVisible()}
                        onCheckedChange={(value) =>
                          column.toggleVisibility(!!value)
                        }
                      >
                        {column.id.replace('_', ' ')}
                      </DropdownMenuCheckboxItem>
                    )
                  })}
              </DropdownMenuContent>
            </DropdownMenu>
          </div>

          {/* Table */}
          <div className="w-full overflow-auto">
            <div className="rounded-md border min-w-[1200px]">
              <Table>
                <TableHeader>
                  {table.getHeaderGroups().map((headerGroup) => (
                    <TableRow key={headerGroup.id}>
                      {headerGroup.headers.map((header) => {
                        return (
                          <TableHead 
                            key={header.id}
                            style={{ 
                              width: header.getSize(),
                              minWidth: header.column.columnDef.minSize || 'auto',
                              maxWidth: header.column.columnDef.maxSize || 'auto'
                            }}
                          >
                            {header.isPlaceholder
                              ? null
                              : flexRender(
                                  header.column.columnDef.header,
                                  header.getContext()
                                )}
                          </TableHead>
                        )
                      })}
                    </TableRow>
                  ))}
                </TableHeader>
                <TableBody>
                  {table.getRowModel().rows?.length ? (
                    table.getRowModel().rows.map((row) => (
                      <TableRow
                        key={row.id}
                        data-state={row.getIsSelected() && "selected"}
                        className="cursor-pointer hover:bg-muted/50"
                      >
                        {row.getVisibleCells().map((cell) => (
                          <TableCell 
                            key={cell.id}
                            style={{ 
                              width: cell.column.getSize(),
                              minWidth: cell.column.columnDef.minSize || 'auto',
                              maxWidth: cell.column.columnDef.maxSize || 'auto'
                            }}
                          >
                            {flexRender(
                              cell.column.columnDef.cell,
                              cell.getContext()
                            )}
                          </TableCell>
                        ))}
                      </TableRow>
                    ))
                  ) : (
                    <TableRow>
                      <TableCell
                        colSpan={columns.length}
                        className="h-24 text-center"
                      >
                        {loading ? (
                          <div className="flex items-center justify-center">
                            <RefreshCw className="h-4 w-4 animate-spin mr-2" />
                            Loading activities...
                          </div>
                        ) : (
                          "No activities found."
                        )}
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </div>
          </div>

          {/* Pagination */}
          <div className="flex items-center justify-between space-x-2 py-4">
            <div className="flex-1 text-sm text-muted-foreground">
              Showing {((currentPage - 1) * pagination.pageSize) + 1} to{' '}
              {Math.min(currentPage * pagination.pageSize, totalItems)} of{' '}
              {totalItems} activities
            </div>
            <div className="flex items-center space-x-2">
              <Button
                variant="outline"
                size="sm"
                onClick={() => {
                  const newPage = currentPage - 1
                  setCurrentPage(newPage)
                  loadActivities(newPage)
                }}
                disabled={currentPage <= 1 || loading}
                className="cursor-pointer"
              >
                Previous
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={() => {
                  const newPage = currentPage + 1
                  setCurrentPage(newPage)
                  loadActivities(newPage)
                }}
                disabled={currentPage >= totalPages || loading}
                className="cursor-pointer"
              >
                Next
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Activity Details Modal */}
      <ActivityDetailsModal
        isOpen={detailsModalOpen}
        onClose={handleCloseDetailsModal}
        activity={selectedActivity}
        loading={loadingDetails}
      />
    </div>
  )
} 