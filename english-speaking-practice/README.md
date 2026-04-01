# SpeakFlow — English Speaking Practice CLI

Practice and improve your spoken English with AI-powered feedback, YouTube shadowing, and Praat-powered intonation analysis.

## Quick Start

```bash
# 1. Clone
git clone https://github.com/shivaam/speakflow.git
cd speakflow

# 2. Install system deps
brew install portaudio ffmpeg          # macOS
# sudo apt install libportaudio2 ffmpeg  # Ubuntu/Debian

# 3. Install (pick one)
uv sync                                # if you have uv (recommended)
pip install -e .                       # or plain pip

# 4. Set your API key (for vocabulary feedback)
export ANTHROPIC_API_KEY="sk-ant-..."

# 5. Go!
speakflow practice
```

## Commands

### `speakflow practice` — Vocabulary Feedback

Speak on a prompt for 1 minute. Get AI-powered suggestions for richer vocabulary and more natural phrasing.

```bash
speakflow practice                          # random prompt, 60s
speakflow practice -d 30                    # 30 seconds
speakflow practice -p "Describe your job"   # custom topic
speakflow practice -m small.en              # more accurate transcription (slower)
```

**Example output:**
> "I ate the food so fast" → "I devoured the chicken" — *"devoured" conveys eating quickly and eagerly in one vivid word*

### `speakflow analyze` — Intonation Coach

Record yourself and see your pitch contour, speaking rate, pauses, and intonation patterns as ASCII graphs in your terminal.

```bash
speakflow analyze                           # 30s recording
speakflow analyze -d 60                     # 60s recording
speakflow analyze --save-png pitch.png      # export graph as image
```

### `speakflow shadow <url>` — YouTube Shadowing

Listen to a YouTube clip, repeat it, and compare your pitch and rhythm against the original.

```bash
speakflow shadow "https://youtube.com/watch?v=..."
speakflow shadow "https://youtube.com/watch?v=..." -s 60 -d 15   # start at 1:00, 15s
speakflow shadow "https://youtube.com/watch?v=..." --save-png comparison.png
```

## Tech Stack

| Tool | What it does |
|---|---|
| **Parselmouth (Praat)** | Gold-standard phonetics engine used by linguistics researchers |
| **faster-whisper** | Fast, accurate speech-to-text with word-level timestamps |
| **Claude API** | Vocabulary upgrades, grammar fixes, idiom suggestions |
| **yt-dlp** | YouTube audio download for shadowing |
| **plotext** | ASCII pitch/intensity graphs in your terminal |

## Requirements

- Python 3.10+
- PortAudio (for mic access)
- ffmpeg (for audio processing)
- Anthropic API key (for the `practice` command only)
