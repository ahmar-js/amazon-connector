import './App.css'
import Navbar from '@/components/navbar'
import { DashboardCards } from '@/components/dashboard-cards'
import { useState } from 'react'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs'
import ScheduleReport from '@/components/schedule-report'
import SchedulesTable from '@/components/schedules-table'

function App() {
  const [activeTab, setActiveTab] = useState('dashboard')
  const [schedulesReload, setSchedulesReload] = useState(0)
  return (
    <div className="min-h-screen bg-background text-foreground">
      <Navbar />
      <main className="container mx-auto p-4">
        <Tabs value={activeTab} onValueChange={setActiveTab}>
          <TabsList>
            <TabsTrigger value="dashboard">Dashboard</TabsTrigger>
            <TabsTrigger value="schedule">Schedule report in Amazon</TabsTrigger>
          </TabsList>

          <TabsContent value="dashboard">
            <DashboardCards />
          </TabsContent>

          <TabsContent value="schedule">
            <div className="space-y-8">
              <ScheduleReport onScheduled={() => setSchedulesReload(v => v + 1)} />
              <SchedulesTable reloadSignal={schedulesReload} />
            </div>
          </TabsContent>
        </Tabs>
      </main>
    </div>
  )
}

export default App
