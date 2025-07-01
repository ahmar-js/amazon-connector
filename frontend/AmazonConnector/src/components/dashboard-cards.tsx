import { useState } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Button } from "@/components/ui/button"
import { AmazonConnectionForm } from "./amazon-connection-form"
import { ConnectionStatusIndicator } from "./connection-status-indicator"
import { ActivitiesTable } from "./activities-table"
import { CronJobManager } from "./cron-job-manager"
import { 
  Home, 
  Clock, 
  Settings, 
  Activity 
} from "lucide-react"

export function DashboardCards() {
  const [activitiesRefreshTrigger, setActivitiesRefreshTrigger] = useState(0)
  const [activeTab, setActiveTab] = useState('dashboard')

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
      <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
        <TabsList className="grid w-full grid-cols-2">
          <TabsTrigger value="dashboard" className="flex items-center gap-2">
            <Home className="h-4 w-4" />
            Dashboard
          </TabsTrigger>
          <TabsTrigger value="cron-jobs" className="flex items-center gap-2">
            <Clock className="h-4 w-4" />
            Cron Jobs
          </TabsTrigger>
        </TabsList>

        <TabsContent value="dashboard" className="space-y-8 mt-6">
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
        </TabsContent>

        <TabsContent value="cron-jobs" className="mt-6">
          <CronJobManager />
        </TabsContent>
      </Tabs>
    </div>
  )
} 