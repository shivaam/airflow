"""Speech-to-text transcription using faster-whisper."""

from pathlib import Path

import numpy as np
from rich.console import Console

console = Console()

# Lazy-loaded model to avoid slow import on every CLI invocation
_model = None


def _get_model(model_size: str = "base.en"):
    """Load the Whisper model (cached after first call)."""
    global _model
    if _model is None:
        from faster_whisper import WhisperModel

        console.print(f"[dim]Loading Whisper model '{model_size}'...[/dim]")
        _model = WhisperModel(model_size, device="cpu", compute_type="int8")
        console.print("[dim]Model loaded.[/dim]")
    return _model


def transcribe_audio(
    audio_path: str | Path,
    model_size: str = "base.en",
) -> dict:
    """Transcribe audio file to text with word-level timestamps.

    Returns:
        dict with keys:
        - text: full transcript string
        - segments: list of segments with start, end, text
        - words: list of words with start, end, word, probability
        - language: detected language
        - duration: audio duration in seconds
    """
    model = _get_model(model_size)

    console.print("[dim]Transcribing...[/dim]")
    segments_gen, info = model.transcribe(
        str(audio_path),
        word_timestamps=True,
        language="en",
    )

    segments = []
    words = []
    full_text_parts = []

    for segment in segments_gen:
        seg_data = {
            "start": segment.start,
            "end": segment.end,
            "text": segment.text.strip(),
        }
        segments.append(seg_data)
        full_text_parts.append(segment.text.strip())

        if segment.words:
            for w in segment.words:
                words.append({
                    "word": w.word.strip(),
                    "start": w.start,
                    "end": w.end,
                    "probability": w.probability,
                })

    full_text = " ".join(full_text_parts)
    console.print(f"[green]✓ Transcribed {info.duration:.1f}s of audio[/green]")

    return {
        "text": full_text,
        "segments": segments,
        "words": words,
        "language": info.language,
        "duration": info.duration,
    }


def transcribe_numpy(
    audio: np.ndarray,
    sample_rate: int = 16000,
    model_size: str = "base.en",
) -> dict:
    """Transcribe audio from a numpy array."""
    from .recorder import save_audio

    path = save_audio(audio, sample_rate)
    try:
        return transcribe_audio(path, model_size)
    finally:
        path.unlink(missing_ok=True)
