import { useState, useEffect } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { Layout } from './components/Layout'
import { PracticeMode } from './components/PracticeMode'
import { ShadowMode } from './components/ShadowMode'
import { SpeechAnalyzer } from './components/SpeechAnalyzer'
import { Settings } from './components/Settings'
import type { AppMode } from './types'

export default function App() {
  const [mode, setMode] = useState<AppMode>('practice')
  const [showSettings, setShowSettings] = useState(false)
  const [apiKey, setApiKey] = useState('')

  useEffect(() => {
    const saved = localStorage.getItem('speakflow_api_key')
    if (saved) setApiKey(saved)
  }, [])

  useEffect(() => {
    if (apiKey) localStorage.setItem('speakflow_api_key', apiKey)
  }, [apiKey])

  return (
    <Layout
      currentMode={mode}
      onModeChange={setMode}
      onSettingsClick={() => setShowSettings(true)}
    >
      <AnimatePresence mode="wait">
        <motion.div
          key={mode}
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -20 }}
          transition={{ duration: 0.3 }}
          className="h-full"
        >
          {mode === 'practice' && <PracticeMode apiKey={apiKey} />}
          {mode === 'shadow' && <ShadowMode apiKey={apiKey} />}
          {mode === 'analyzer' && <SpeechAnalyzer apiKey={apiKey} />}
        </motion.div>
      </AnimatePresence>

      <AnimatePresence>
        {showSettings && (
          <Settings
            apiKey={apiKey}
            onApiKeyChange={setApiKey}
            onClose={() => setShowSettings(false)}
          />
        )}
      </AnimatePresence>
    </Layout>
  )
}
