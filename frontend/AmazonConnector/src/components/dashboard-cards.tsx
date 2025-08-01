import { useState } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Button } from "@/components/ui/button"
import { AmazonConnectionForm } from "./amazon-connection-form"
import { ConnectionStatusIndicator } from "./connection-status-indicator"
import { ActivitiesTable } from "./activities-table"
import { 
  Home,
} from "lucide-react"

export function DashboardCards() {
  const [activitiesRefreshTrigger, setActivitiesRefreshTrigger] = useState(0)

  const handleDataFetchStart = () => {
    // Trigger refresh when data fetch starts (to show new activity)
    setActivitiesRefreshTrigger(prev => prev + 1)
  }

  const handleDataFetchEnd = () => {
    // Trigger refresh when data fetch ends (to show updated status)
    setActivitiesRefreshTrigger(prev => prev + 1)
  }

  return (
    <div className="container mx-auto p-6">
      <div className="space-y-8 mt-6">
        {/* Top Row: Connection components */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          
          {/* Column 1: Amazon Connection Form */}
          <div className="space-y-6">
            <AmazonConnectionForm 
              onDataFetchStart={handleDataFetchStart}
              onDataFetchEnd={handleDataFetchEnd}
            />
          </div>

          {/* Column 2: Connection Status Indicator */}
          <div className="space-y-6">
            <ConnectionStatusIndicator />
          </div>
        </div>

        {/* Full Width Row: Activities Table */}
        <div className="w-full">
          <ActivitiesTable refreshTrigger={activitiesRefreshTrigger} />
        </div>
      </div>
    </div>
  )
} 