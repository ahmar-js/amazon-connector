import { useEffect, useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Calendar } from '@/components/ui/calendar'
import { Calendar as CalendarIcon, Clock } from 'lucide-react'
import apiClient from '@/lib/api'
import MARKETPLACES, { getEnabledMarketplaceCodes } from '@/lib/marketplaces'
import { Alert, AlertDescription } from '@/components/ui/alert'

// Add callback type
type Props = { onScheduled?: () => void }

export function ScheduleReport({ onScheduled }: Props) {
  const periodOptions = [
    { value: 'PT5M', label: 'Every 5 minutes' },
    { value: 'PT15M', label: 'Every 15 minutes' },
    { value: 'PT30M', label: 'Every 30 minutes' },
    { value: 'PT1H', label: 'Every 1 hour' },
    { value: 'PT2H', label: 'Every 2 hours' },
    { value: 'PT4H', label: 'Every 4 hours' },
    { value: 'PT8H', label: 'Every 8 hours' },
    { value: 'PT12H', label: 'Every 12 hours' },
    { value: 'P1D', label: 'Every 1 day' },
    { value: 'P2D', label: 'Every 2 days' },
    { value: 'P3D', label: 'Every 3 days' },
    { value: 'PT84H', label: 'Every 84 hours (3.5 days)' },
    { value: 'P7D', label: 'Every 7 days' },
    { value: 'P14D', label: 'Every 14 days' },
    { value: 'P15D', label: 'Every 15 days' },
    { value: 'P18D', label: 'Every 18 days' },
    { value: 'P30D', label: 'Every 30 days' },
    { value: 'P1M', label: 'Every 1 month' },
  ]
  // Region/marketplace select (single)
  const enabled = getEnabledMarketplaceCodes()
  const defaultMarketplace = enabled.length ? enabled[0] : 'IT'
  const [marketplace, setMarketplace] = useState<string>(defaultMarketplace)
  const [period, setPeriod] = useState('P1D')
  // Removed manual timestamp input; using calendar + time only
  const [nextDate, setNextDate] = useState<Date | undefined>(undefined)
  const [nextTimeHM, setNextTimeHM] = useState('')
  const timeZone = 'Asia/Karachi'
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState<any>(null)
  const today = (() => { const t = new Date(); t.setHours(0,0,0,0); return t })()

  // Keep a current time reference and initialize default selections
  const [now, setNow] = useState<Date>(new Date())
  useEffect(() => {
    // Update every 1s to keep min time and display fresh
    const id = setInterval(() => setNow(new Date()), 1000)
    return () => clearInterval(id)
  }, [])
  const nowHM = `${String(now.getHours()).padStart(2,'0')}:${String(now.getMinutes()).padStart(2,'0')}`

  // Initialize defaults on first render and keep time synced until user changes it
  const [userOverrodeTime, setUserOverrodeTime] = useState(false)
  useEffect(() => {
    if (!nextDate) {
      setNextDate(new Date())
    }
  }, [])
  useEffect(() => {
    if (!userOverrodeTime) {
      setNextTimeHM(nowHM)
    }
  }, [nowHM, userOverrodeTime])

  const isTodaySelected = !!nextDate && nextDate.getFullYear() === today.getFullYear() && nextDate.getMonth() === today.getMonth() && nextDate.getDate() === today.getDate()

  const computedLocalISO = (() => {
    if (!nextDate) return ''
    const y = nextDate.getFullYear()
    const m = String(nextDate.getMonth() + 1).padStart(2, '0')
    const d = String(nextDate.getDate()).padStart(2, '0')
    const hm = nextTimeHM || '00:00'
    return `${y}-${m}-${d}T${hm}:00`
  })()

  const computedUTCFromPKT = (() => {
    // Only provide a simple preview when Asia/Karachi (UTC+05:00)
    if (timeZone !== 'Asia/Karachi') return ''
    const iso = computedLocalISO
    if (!iso) return ''
    // Subtract 5 hours to approximate UTC
    const [datePart, timePart] = iso.split('T')
    if (!datePart || !timePart) return ''
    const [hh, mm, ss] = timePart.split(':').map(Number)
    const dt = new Date(Date.UTC(
      Number(datePart.slice(0, 4)),
      Number(datePart.slice(5, 7)) - 1,
      Number(datePart.slice(8, 10)),
      hh ?? 0, mm ?? 0, ss ?? 0
    ))
    // Remove 5 hours to get UTC equivalent of PKT naive time
    dt.setUTCHours(dt.getUTCHours() - 5)
    const pad = (n: number) => String(n).padStart(2, '0')
    const utc = `${dt.getUTCFullYear()}-${pad(dt.getUTCMonth() + 1)}-${pad(dt.getUTCDate())}T${pad(dt.getUTCHours())}:${pad(dt.getUTCMinutes())}:${pad(dt.getUTCSeconds())}Z`
    return utc
  })()

  const submit = async () => {
    try {
      setLoading(true)
      setResult(null)
      const payload = {
        reportType: 'GET_FBA_MYI_ALL_INVENTORY_DATA',
        period,
        nextReportCreationTime: computedLocalISO || undefined,
        timeZone,
        reportOptions: {},
        marketplaces: [marketplace],
      }
      const res = await apiClient.post('/inventory/report-schedules/', payload)
      setResult(res.data)
      if (res?.data?.success && onScheduled) {
        onScheduled()
      }
    } catch (e: any) {
      setResult({ success: false, error: e?.message })
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="py-6">
      <Card className="shadow-sm">
        <CardHeader>
          <CardTitle className="text-lg">Schedule report in Amazon</CardTitle>
        </CardHeader>
        <CardContent className="space-y-6">
          <Alert>
            <AlertDescription>
              Create an Amazon SP-API schedule for inventory reports. Pick your marketplace, set the frequency and the first run time. We’ll convert from Asia/Karachi to UTC automatically.
            </AlertDescription>
          </Alert>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="space-y-2">
              <Label>Report type</Label>
              <Input value="GET_FBA_MYI_ALL_INVENTORY_DATA" disabled className="bg-muted cursor-pointer" />
            </div>
            <div className="space-y-2">
              <Label>Region / Marketplace</Label>
              <Select value={marketplace} onValueChange={setMarketplace}>
                <SelectTrigger className="w-full cursor-pointer">
                  <SelectValue placeholder="Select marketplace" />
                </SelectTrigger>
                <SelectContent>
                  {Object.values(MARKETPLACES).map(m => (
                    <SelectItem key={m.code} value={m.code} disabled={m.disabled}>
                      {m.name ? `${m.name} (${m.code})` : m.code}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label>Period</Label>
              <Select value={period} onValueChange={setPeriod}>
                <SelectTrigger className="w-full cursor-pointer">
                  <SelectValue placeholder="Select frequency" />
                </SelectTrigger>
                <SelectContent>
                  {periodOptions.map(opt => (
                    <SelectItem key={opt.value} value={opt.value}>{opt.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label>Next report creation time (local)</Label>
              <Popover>
                <PopoverTrigger asChild>
                  <Button variant="outline" className="w-full justify-start cursor-pointer">
                    <CalendarIcon className="mr-2 h-4 w-4" />
                    {nextDate ? `${nextDate.toDateString()} ${nextTimeHM || ''}` : 'Pick date & time'}
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="p-3 space-y-3">
                  <Calendar
                    mode="single"
                    captionLayout="dropdown"
                    selected={nextDate}
                    onSelect={(d) => {
                      setNextDate(d)
                      // If switching to today and selected time is in past, bump to now
                      if (d) {
                        const t = new Date(d); t.setHours(0,0,0,0)
                        if (t.getTime() === today.getTime()) {
                          if (nextTimeHM && nextTimeHM < nowHM) {
                            setNextTimeHM(nowHM)
                          } else if (!nextTimeHM) {
                            setNextTimeHM(nowHM)
                          }
                        }
                      }
                    }}
                    disabled={{ before: today }}
                  />
                  <div className="flex items-center gap-2">
                    <Clock className="h-4 w-4 text-muted-foreground" />
                    <Input
                      type="time"
                      step={900}
                      min={isTodaySelected ? nowHM : undefined}
                      value={nextTimeHM}
                      className="cursor-pointer"
                      onChange={(e) => {
                        const v = e.target.value
                        if (isTodaySelected && v && v < nowHM) {
                          setNextTimeHM(nowHM)
                        } else {
                          setNextTimeHM(v)
                        }
                        setUserOverrodeTime(true)
                      }}
                    />
                    <Button
                      size="sm"
                      variant="secondary"
                      className="cursor-pointer"
                      onClick={() => {
                        setNextTimeHM(nowHM)
                        setUserOverrodeTime(true)
                      }}
                    >
                      Now
                    </Button>
                  </div>
                </PopoverContent>
              </Popover>
              <div className="text-xs text-muted-foreground">Timezone: Asia/Karachi (UTC+05:00). It will be converted to UTC automatically.</div>
              {computedUTCFromPKT && (
                <div className="text-xs text-muted-foreground mt-1">Preview UTC time: {computedUTCFromPKT}</div>
              )}
            </div>
            {/* Timezone fixed to Asia/Karachi by default */}
          </div>

          <div className="flex gap-2 items-center">
            <Button onClick={submit} disabled={loading} className="cursor-pointer">
              {loading ? 'Scheduling…' : 'Create schedule'}
            </Button>
            <Button variant="secondary" onClick={() => { setResult(null) }} disabled={loading} className="cursor-pointer">Clear output</Button>
          </div>

          {result && (
            <pre className="mt-4 text-sm bg-muted p-4 rounded-md overflow-auto max-h-80 border">
{JSON.stringify(result, null, 2)}
            </pre>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

export default ScheduleReport


