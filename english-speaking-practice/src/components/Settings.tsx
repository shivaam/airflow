import { motion } from 'framer-motion'
import { X, Key, Info } from 'lucide-react'

interface SettingsProps {
  apiKey: string
  onApiKeyChange: (key: string) => void
  onClose: () => void
}

export function Settings({ apiKey, onApiKeyChange, onClose }: SettingsProps) {
  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4"
      onClick={(e) => e.target === e.currentTarget && onClose()}
    >
      <motion.div
        initial={{ scale: 0.95, opacity: 0 }}
        animate={{ scale: 1, opacity: 1 }}
        exit={{ scale: 0.95, opacity: 0 }}
        className="glass w-full max-w-md p-6 space-y-6"
      >
        <div className="flex items-center justify-between">
          <h2 className="text-lg font-semibold text-white">Settings</h2>
          <button
            onClick={onClose}
            className="p-1.5 rounded-lg text-gray-400 hover:text-white hover:bg-white/10 transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="space-y-4">
          <div>
            <label className="flex items-center gap-2 text-sm font-medium text-gray-300 mb-2">
              <Key className="w-4 h-4" />
              Anthropic API Key
            </label>
            <input
              type="password"
              value={apiKey}
              onChange={(e) => onApiKeyChange(e.target.value)}
              placeholder="sk-ant-..."
              className="w-full px-4 py-3 bg-white/5 border border-white/10 rounded-xl text-white
                         placeholder-gray-500 focus:outline-none focus:border-brand-500/50 focus:ring-1
                         focus:ring-brand-500/30 transition-all text-sm font-mono"
            />
          </div>

          <div className="flex items-start gap-2 p-3 bg-brand-600/10 border border-brand-500/20 rounded-xl">
            <Info className="w-4 h-4 text-brand-400 mt-0.5 shrink-0" />
            <p className="text-xs text-brand-200/80 leading-relaxed">
              Your API key is stored locally in your browser and never sent to any server other
              than the Anthropic API. It's used to generate vocabulary and phrasing feedback.
            </p>
          </div>
        </div>

        <button onClick={onClose} className="btn-primary w-full">
          Done
        </button>
      </motion.div>
    </motion.div>
  )
}
