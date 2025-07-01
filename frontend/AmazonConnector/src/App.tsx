import { Button } from "@/components/ui/button"
import './App.css'
import Navbar from '@/components/navbar'
import { DashboardCards } from '@/components/dashboard-cards'

function App() {
  return (
    <div className="min-h-screen bg-background text-foreground">
      <Navbar />
      <main>
        <DashboardCards />
      </main>
    </div>
  )
}

export default App
