import { useEffect, useMemo, useState } from 'react'
import { format } from 'date-fns'
import apiClient from '@/lib/api'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Badge } from '@/components/ui/badge'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Calendar, FileText, RefreshCw, Trash2, Loader2 } from 'lucide-react'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from '@/components/ui/alert-dialog'

// Add props
type Props = { reloadSignal?: number }

type Schedule = {
  reportType: string
  marketplace: string // code IT/DE/UK
  period: string
  nextReportCreationTime: string
  reportScheduleId: string
}

const MARKETPLACE_LABEL: Record<string,string> = {
  IT: 'Italy (IT)',
  DE: 'Germany (DE)',
  UK: 'United Kingdom (UK)'
}

export function SchedulesTable({ reloadSignal }: Props) {
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [rows, setRows] = useState<Schedule[]>([])
  const [marketplaceFilter, setMarketplaceFilter] = useState<string>('ALL')
  const [confirmOpen, setConfirmOpen] = useState(false)
  const [target, setTarget] = useState<Schedule | null>(null)
  const [deletingId, setDeletingId] = useState<string | null>(null)

  const filtered = useMemo(() => {
    if (marketplaceFilter === 'ALL') return rows
    return rows.filter(r => r.marketplace === marketplaceFilter)
  }, [rows, marketplaceFilter])

  // KPIs removed as requested

  const load = async () => {
    try {
      setLoading(true)
      setError(null)
      // Ask backend for all known marketplaces and our report type
      const query = `?marketplaces=IT,DE,UK&reportTypes=GET_FBA_MYI_ALL_INVENTORY_DATA`
      const res = await apiClient.get(`/inventory/report-schedules/list/${query}`)
      const data = res.data
      if (!data?.success || !data?.results) {
        setRows([])
        return
      }
      // Flatten
      const flattened: Schedule[] = []
      ;(['IT','DE','UK'] as const).forEach(code => {
        const node = data.results?.[code]
        const list = node?.schedules?.reportSchedules || []
        list.forEach((s: any) => {
          flattened.push({
            reportType: s.reportType,
            marketplace: code,
            period: s.period,
            nextReportCreationTime: s.nextReportCreationTime,
            reportScheduleId: s.reportScheduleId,
          })
        })
      })
      setRows(flattened)
    } catch (e: any) {
      setError(e?.message || 'Failed to load schedules')
      setRows([])
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [])
  useEffect(() => { if (reloadSignal !== undefined) { load() } }, [reloadSignal])

  const requestDelete = (record: Schedule) => {
    setTarget(record)
    setConfirmOpen(true)
  }

  const confirmDelete = async () => {
    if (!target) return
    try {
      setDeletingId(target.reportScheduleId)
      setError(null)
      const qs = `?marketplace=${encodeURIComponent(target.marketplace)}`
      await apiClient.delete(`/inventory/report-schedules/${encodeURIComponent(target.reportScheduleId)}/${qs}`)
      setConfirmOpen(false)
      setTarget(null)
      await load()
    } catch (e: any) {
      setError(e?.message || 'Failed to cancel schedule')
    } finally {
      setDeletingId(null)
    }
  }

  return (
    <div className="space-y-6">
      {/* KPIs removed */}

      {/* Table Card */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="flex items-center gap-2">
                <FileText className="h-5 w-5" />
                Scheduled Reports
              </CardTitle>
              <CardDescription>All inventory report schedules by region</CardDescription>
            </div>
            <div className="flex items-center gap-3">
              <Select value={marketplaceFilter} onValueChange={setMarketplaceFilter}>
                <SelectTrigger className="w-[180px] cursor-pointer">
                  <SelectValue placeholder="All Regions" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="ALL">All Regions</SelectItem>
                  <SelectItem value="IT">Italy (IT)</SelectItem>
                  <SelectItem value="DE">Germany (DE)</SelectItem>
                  <SelectItem value="UK">United Kingdom (UK)</SelectItem>
                </SelectContent>
              </Select>
              <Button onClick={load} disabled={loading} className="cursor-pointer">
                <RefreshCw className={`h-4 w-4 mr-2 ${loading ? 'animate-spin' : ''}`} />
                Refresh
              </Button>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {error && (
            <Alert variant="destructive" className="mb-4">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}

          {!loading && filtered.length === 0 && (
            <Alert className="mb-2">
              <AlertDescription>
                No schedules found for the selected region. Create a schedule above to see it here.
              </AlertDescription>
            </Alert>
          )}

          <div className="w-full overflow-auto">
            <div className="rounded-md border min-w-[900px]">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Region</TableHead>
                    <TableHead>Report Type</TableHead>
                    <TableHead>Period</TableHead>
                    <TableHead>Next Run</TableHead>
                    <TableHead>Schedule ID</TableHead>
                    <TableHead className="w-[120px]">Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {loading ? (
                    [...Array(4)].map((_, i) => (
                      <TableRow key={i}>
                        <TableCell><Skeleton className="h-4 w-28" /></TableCell>
                        <TableCell><Skeleton className="h-4 w-56" /></TableCell>
                        <TableCell><Skeleton className="h-4 w-24" /></TableCell>
                        <TableCell><Skeleton className="h-4 w-40" /></TableCell>
                        <TableCell><Skeleton className="h-4 w-40" /></TableCell>
                        <TableCell><Skeleton className="h-8 w-24" /></TableCell>
                      </TableRow>
                    ))
                  ) : (
                    filtered.map((r, idx) => {
                      const dt = new Date(r.nextReportCreationTime)
                      const hasDate = !isNaN(dt.getTime())
                      return (
                        <TableRow key={idx} className="hover:bg-muted/50">
                          <TableCell>
                            <Badge variant="outline" className="text-xs">{MARKETPLACE_LABEL[r.marketplace] || r.marketplace}</Badge>
                          </TableCell>
                          <TableCell>
                            <div className="font-mono text-xs">{r.reportType}</div>
                          </TableCell>
                          <TableCell>
                            <Badge variant="secondary" className="text-xs">{r.period}</Badge>
                          </TableCell>
                          <TableCell>
                            {hasDate ? (
                              <div className="flex items-center gap-2 text-sm">
                                <Calendar className="h-4 w-4 text-muted-foreground" />
                                <div className="flex flex-col">
                                  <span className="font-medium">{format(dt, 'MMM dd, yyyy')}</span>
                                  <span className="text-xs text-muted-foreground">{format(dt, 'hh:mm:ss a')}</span>
                                </div>
                              </div>
                            ) : (
                              <span className="text-muted-foreground">N/A</span>
                            )}
                          </TableCell>
                          <TableCell>
                            <div className="font-mono text-xs">{r.reportScheduleId}</div>
                          </TableCell>
                          <TableCell>
                            <AlertDialog open={confirmOpen && target?.reportScheduleId === r.reportScheduleId} onOpenChange={(o) => { if (!o) { setConfirmOpen(false); setTarget(null) } }}>
                              <AlertDialogTrigger asChild>
                                <Button
                                  variant="destructive"
                                  size="sm"
                                  className="cursor-pointer"
                                  onClick={(e) => { e.stopPropagation(); requestDelete(r) }}
                                  disabled={deletingId === r.reportScheduleId}
                                >
                                  {deletingId === r.reportScheduleId ? (
                                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                                  ) : (
                                    <Trash2 className="h-4 w-4 mr-2" />
                                  )}
                                  Delete
                                </Button>
                              </AlertDialogTrigger>
                              <AlertDialogContent>
                                <AlertDialogHeader>
                                  <AlertDialogTitle>Cancel this report schedule?</AlertDialogTitle>
                                  <AlertDialogDescription>
                                    This will cancel schedule <span className="font-mono">{r.reportScheduleId}</span> for {MARKETPLACE_LABEL[r.marketplace] || r.marketplace}.
                                    This action cannot be undone.
                                  </AlertDialogDescription>
                                </AlertDialogHeader>
                                <AlertDialogFooter>
                                  <AlertDialogCancel className="cursor-pointer">No, keep it</AlertDialogCancel>
                                  <AlertDialogAction
                                    className="cursor-pointer"
                                    onClick={(e) => { e.preventDefault(); confirmDelete() }}
                                    disabled={deletingId === r.reportScheduleId}
                                  >
                                    {deletingId === r.reportScheduleId ? 'Cancelling...' : 'Yes, cancel'}
                                  </AlertDialogAction>
                                </AlertDialogFooter>
                              </AlertDialogContent>
                            </AlertDialog>
                          </TableCell>
                        </TableRow>
                      )
                    })
                  )}
                </TableBody>
              </Table>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

export default SchedulesTable


