# SpeakFlow — English Speaking Practice CLI

Practice and improve your spoken English with AI-powered feedback, YouTube shadowing, and intonation analysis.

## Setup

```bash
cd english-speaking-practice
uv sync    # or: pip install -e .
```

You'll also need `ffmpeg` for audio processing:
```bash
# macOS
brew install ffmpeg

# Ubuntu/Debian
sudo apt install ffmpeg
```

## Usage

### 1. Practice — Get vocabulary feedback

Record yourself speaking on a prompt, get AI-powered suggestions for better vocabulary and phrasing.

```bash
# Random prompt, 60 seconds
speakflow practice

# Custom topic, 30 seconds
speakflow practice -d 30 -p "Tell me about your favorite hobby"

# Use a better Whisper model for more accurate transcription
speakflow practice -m small.en
```

Requires `ANTHROPIC_API_KEY` environment variable (or `--api-key` flag).

### 2. Analyze — Intonation and speech analysis

Get detailed analysis of your pitch, speaking rate, pauses, and intonation patterns.

```bash
# 30 second recording
speakflow analyze

# Save pitch graph as PNG
speakflow analyze --save-png pitch.png

# Longer recording
speakflow analyze -d 60
```

### 3. Shadow — YouTube video shadowing

Listen to a YouTube clip, repeat it, and compare your pitch/rhythm with the original.

```bash
# Shadow 30 seconds starting from the beginning
speakflow shadow "https://youtube.com/watch?v=..."

# Start at 1 minute, shadow for 15 seconds
speakflow shadow "https://youtube.com/watch?v=..." -s 60 -d 15

# Save comparison graph
speakflow shadow "https://youtube.com/watch?v=..." --save-png comparison.png
```

## Tech Stack

- **Parselmouth (Praat)** — Gold-standard phonetics analysis
- **faster-whisper** — Fast, accurate speech-to-text
- **Claude API** — Vocabulary and phrasing feedback
- **yt-dlp** — YouTube audio download
- **plotext** — Terminal ASCII graphs
