import { ThemeToggle } from "@/components/theme-toggle"
import { AmazonLogo } from "@/components/amazon-logo"

export default function Navbar() {
  return (
    <nav className="sticky top-0 z-50 w-full border-b border-border/40 bg-background/95 backdrop-blur">
      <div className="container mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex h-16 items-center justify-between">
          {/* App Name/Logo - Left Side */}
          <div className="flex items-center space-x-3">
            <div className="flex-shrink-0 cursor-pointer">
              <AmazonLogo className="h-8 w-8 text-foreground transition-all duration-300 ease-in-out hover:text-[#FF9900] hover:scale-105" />
            </div>
          </div>

          {/* Right Side - Theme Toggle */}
          <div className="flex items-center space-x-2">
            <ThemeToggle />
          </div>
        </div>
      </div>

      {/* Mobile-first responsive enhancement */}
      <div className="absolute inset-x-0 top-full h-px bg-gradient-to-r from-transparent via-border to-transparent opacity-20" />
    </nav>
  )
} 