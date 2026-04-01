import { ReactNode } from 'react'
import { motion } from 'framer-motion'
import { Mic, Video, BarChart3, Settings } from 'lucide-react'
import type { AppMode } from '../types'

const modes = [
  { id: 'practice' as AppMode, label: 'Practice', icon: Mic, description: 'Free speaking' },
  { id: 'shadow' as AppMode, label: 'Shadow', icon: Video, description: 'YouTube shadowing' },
  { id: 'analyzer' as AppMode, label: 'Analyze', icon: BarChart3, description: 'Intonation coach' },
]

interface LayoutProps {
  children: ReactNode
  currentMode: AppMode
  onModeChange: (mode: AppMode) => void
  onSettingsClick: () => void
}

export function Layout({ children, currentMode, onModeChange, onSettingsClick }: LayoutProps) {
  return (
    <div className="min-h-screen bg-gray-950 flex flex-col">
      {/* Header */}
      <header className="border-b border-white/5 px-6 py-4">
        <div className="max-w-6xl mx-auto flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-xl bg-brand-600 flex items-center justify-center">
              <Mic className="w-5 h-5 text-white" />
            </div>
            <div>
              <h1 className="text-lg font-bold text-white tracking-tight">SpeakFlow</h1>
              <p className="text-xs text-gray-500">English Speaking Practice</p>
            </div>
          </div>

          {/* Mode Tabs */}
          <nav className="flex gap-1 p-1 glass rounded-xl">
            {modes.map((m) => (
              <button
                key={m.id}
                onClick={() => onModeChange(m.id)}
                className={`relative px-4 py-2 rounded-lg text-sm font-medium transition-colors duration-200 flex items-center gap-2 ${
                  currentMode === m.id ? 'text-white' : 'text-gray-400 hover:text-gray-200'
                }`}
              >
                {currentMode === m.id && (
                  <motion.div
                    layoutId="activeTab"
                    className="absolute inset-0 bg-brand-600/30 border border-brand-500/30 rounded-lg"
                    transition={{ type: 'spring', bounce: 0.2, duration: 0.5 }}
                  />
                )}
                <m.icon className="w-4 h-4 relative z-10" />
                <span className="relative z-10 hidden sm:inline">{m.label}</span>
              </button>
            ))}
          </nav>

          <button
            onClick={onSettingsClick}
            className="p-2 rounded-lg text-gray-400 hover:text-white hover:bg-white/5 transition-colors"
          >
            <Settings className="w-5 h-5" />
          </button>
        </div>
      </header>

      {/* Main Content */}
      <main className="flex-1 px-6 py-6 overflow-auto">
        <div className="max-w-6xl mx-auto h-full">
          {children}
        </div>
      </main>
    </div>
  )
}
