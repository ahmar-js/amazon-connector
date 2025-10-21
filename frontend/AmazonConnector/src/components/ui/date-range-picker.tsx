import * as React from "react"
import { format, addDays, isAfter, startOfWeek, startOfMonth, endOfMonth, subMonths } from "date-fns"
import type { DateRange } from "react-day-picker"

import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { Calendar as CalendarIcon } from "lucide-react"
import { Calendar } from "@/components/ui/calendar"
import { Label } from "@/components/ui/label"

type Props = {
  value?: DateRange
  onChange?: (range: DateRange | undefined) => void
  placeholder?: string
  className?: string
  numberOfMonths?: number
  maxDays?: number // maximum allowed range length (inclusive)
  minDate?: Date
  maxDate?: Date // default: today
  disabled?: boolean
  // Time controls
  showTimeInputs?: boolean
  startTime?: string // "HH:mm"
  endTime?: string // "HH:mm"
  onTimeChange?: (startTime: string, endTime: string) => void
  timeStepSeconds?: number // default 900 (15 min)
  // Presets
  showPresets?: boolean
}

export function DateRangePicker({
  value,
  onChange,
  placeholder = "Select date range",
  className,
  numberOfMonths = 2,
  maxDays = 30,
  minDate,
  maxDate,
  disabled,
  showTimeInputs = false,
  startTime = "00:00",
  endTime = "23:59",
  onTimeChange,
  timeStepSeconds = 900,
  showPresets = false,
}: Props) {
  const [open, setOpen] = React.useState(false)
  const today = React.useMemo(() => {
    const d = new Date()
    d.setHours(0, 0, 0, 0)
    return d
  }, [])

  const effectiveMaxDate = maxDate ? normalizeDate(maxDate) : today

  // Preset applier
  const applyPreset = usePresetApplier(onChange, setOpen)

  const label = React.useMemo(() => {
    if (value?.from && value?.to) {
      return `${format(value.from, "MMM dd, yyyy")} - ${format(value.to, "MMM dd, yyyy")}`
    }
    if (value?.from) {
      return `${format(value.from, "MMM dd, yyyy")} - ...`
    }
    return placeholder
  }, [value, placeholder])

  const handleSelect = (range: DateRange | undefined) => {
    if (!range?.from) {
      onChange?.(undefined)
      return
    }

    // When both from and to exist, enforce maxDays and maxDate
    if (range.to) {
      const cappedToByLength = addDays(range.from, Math.max(0, maxDays - 1))
      const cappedTo = minDateFirst(
        // do not allow after effectiveMaxDate
        isAfter(cappedToByLength, effectiveMaxDate) ? effectiveMaxDate : cappedToByLength,
        range.to
      )
      const normalized: DateRange = {
        from: normalizeDate(range.from),
        to: normalizeDate(cappedTo),
      }
      onChange?.(normalized)
      return
    }

    // Only from selected so far
    onChange?.({ from: normalizeDate(range.from), to: undefined })
  }

  // Disable rules: beyond maxDate, before minDate, and when picking end date, beyond from+maxDays-1
  const disabledMatchers = React.useMemo(() => {
    const matchers: any[] = []
    if (effectiveMaxDate) {
      matchers.push({ after: effectiveMaxDate })
    }
    if (minDate) {
      matchers.push({ before: normalizeDate(minDate) })
    }
    if (value?.from && !value?.to) {
      const endCap = addDays(value.from, Math.max(0, maxDays - 1))
      const endLimit = isAfter(endCap, effectiveMaxDate) ? effectiveMaxDate : endCap
      matchers.push({ after: endLimit })
    }
    return matchers
  }, [effectiveMaxDate, minDate, value?.from, value?.to, maxDays])

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          className={cn(
            "w-full justify-start text-left font-normal",
            !value?.from && "text-muted-foreground",
            className
          )}
          disabled={disabled}
        >
          <CalendarIcon className="mr-2 h-4 w-4" />
          {label}
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-auto p-0" align="start">
        <div className="p-3 space-y-3">
          {showPresets && (
            <div className="flex flex-wrap gap-2">
              {/* <Button variant="secondary" size="sm" onClick={() => applyPreset("today")}>Today</Button>
              <Button variant="secondary" size="sm" onClick={() => applyPreset("yesterday")}>Yesterday</Button>
              <Button variant="secondary" size="sm" onClick={() => applyPreset("last24h")}>Last 24h</Button>
              <Button variant="secondary" size="sm" onClick={() => applyPreset("last7d")}>Last 7 days</Button>
              <Button variant="secondary" size="sm" onClick={() => applyPreset("last30d")}>Last 30 days</Button>
              <Button variant="secondary" size="sm" onClick={() => applyPreset("thisweek")}>This Week</Button>
              <Button variant="secondary" size="sm" onClick={() => applyPreset("lastmonth")}>Last Month</Button>
              <Button variant="ghost" size="sm" onClick={() => applyPreset("clear")}>Clear</Button> */}
            </div>
          )}
          <Calendar
            captionLayout="dropdown"
            mode="range"
            selected={value}
            onSelect={handleSelect}
            numberOfMonths={numberOfMonths}
            disabled={disabledMatchers}
            classNames={{
              months: "w-full relative flex flex-col md:flex-row md:justify-between gap-4 md:gap-10",
              month: "flex flex-col w-full md:w-auto",
            }}
          />
          {showTimeInputs && (
            <TimeInputs
              startTime={startTime}
              endTime={endTime}
              timeStepSeconds={timeStepSeconds}
              onTimeChange={onTimeChange}
            />
          )}
        </div>
      </PopoverContent>
    </Popover>
  )
}

function normalizeDate(d: Date) {
  const n = new Date(d)
  n.setHours(0, 0, 0, 0)
  return n
}

function minDateFirst(a: Date, b: Date) {
  return isAfter(a, b) ? b : a
}

export type { DateRange }

// Helpers for presets
function startOfDay(d: Date) {
  const n = new Date(d); n.setHours(0,0,0,0); return n
}
function endOfDay(d: Date) {
  const n = new Date(d); n.setHours(23,59,59,999); return n
}

type PresetKey = "today" | "yesterday" | "last24h" | "last7d" | "last30d"
  | "thisweek" | "lastmonth" | "clear"

// Note: Apply presets by computing a DateRange and calling onChange, then close popover
function usePresetApplier(
  onChange: ((range: DateRange | undefined) => void) | undefined,
  setOpen: (open: boolean) => void
) {
  const applyPreset = React.useCallback((key: PresetKey) => {
    const now = new Date()
    let from: Date
    let to: Date
    switch (key) {
      case "today":
        from = startOfDay(now); to = endOfDay(now); break
      case "yesterday":
        from = startOfDay(addDays(now, -1)); to = endOfDay(addDays(now, -1)); break
      case "last24h":
        from = new Date(now.getTime() - 24*60*60*1000); to = now; break
      case "last7d":
        from = startOfDay(addDays(now, -6)); to = endOfDay(now); break
      case "last30d":
      default:
        from = startOfDay(addDays(now, -29)); to = endOfDay(now); break
      case "thisweek": {
        const sow = startOfWeek(now, { weekStartsOn: 1 })
        from = startOfDay(sow); to = endOfDay(now); break
      }
      case "lastmonth": {
        const prev = subMonths(now, 1)
        from = startOfDay(startOfMonth(prev)); to = endOfDay(endOfMonth(prev)); break
      }
      case "clear":
        onChange?.(undefined)
        // Reset times to defaults when clearing
        try { (onChange as any) && null } catch {}
        setOpen(false)
        return
    }
    onChange?.({ from, to })
    setOpen(false)
  }, [onChange, setOpen])

  return applyPreset
}

function TimeInputs({
  startTime,
  endTime,
  timeStepSeconds,
  onTimeChange,
}: {
  startTime: string
  endTime: string
  timeStepSeconds: number
  onTimeChange?: (start: string, end: string) => void
}) {
  const startRef = React.useRef<HTMLInputElement | null>(null)
  const endRef = React.useRef<HTMLInputElement | null>(null)

  const openPicker = (ref: React.RefObject<HTMLInputElement | null>) => {
    ref.current?.focus()
    ;(ref.current as any)?.showPicker?.()
  }

  return (
    <div className="grid grid-cols-2 gap-3">
      <div className="space-y-1.5">
        <Label htmlFor="drp-start-time" className="text-xs text-muted-foreground cursor-pointer" onClick={() => openPicker(startRef)}>
          Start Time
        </Label>
        <div className="rounded-md border border-input bg-background px-1 py-0.5 cursor-pointer" onClick={() => openPicker(startRef)}>
          <Input
            id="drp-start-time"
            ref={startRef}
            type="time"
            step={timeStepSeconds}
            value={startTime}
            onChange={(e) => onTimeChange?.(e.target.value, endTime)}
            className="border-0 px-0 focus-visible:ring-0 cursor-pointer h-8 text-sm"
          />
        </div>
      </div>
      <div className="space-y-1.5">
        <Label htmlFor="drp-end-time" className="text-xs text-muted-foreground cursor-pointer" onClick={() => openPicker(endRef)}>
          End Time
        </Label>
        <div className="rounded-md border border-input bg-background px-1 py-0.5 cursor-pointer" onClick={() => openPicker(endRef)}>
          <Input
            id="drp-end-time"
            ref={endRef}
            type="time"
            step={timeStepSeconds}
            value={endTime}
            onChange={(e) => onTimeChange?.(startTime, e.target.value)}
            className="border-0 px-0 focus-visible:ring-0 cursor-pointer h-8 text-sm"
          />
        </div>
      </div>
    </div>
  )
}
