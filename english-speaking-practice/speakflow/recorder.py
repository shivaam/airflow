"""Audio recording from microphone."""

import tempfile
from pathlib import Path

import numpy as np
import sounddevice as sd
import soundfile as sf
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn

console = Console()

SAMPLE_RATE = 16000  # 16kHz — optimal for speech recognition


def record_audio(duration: int, sample_rate: int = SAMPLE_RATE) -> tuple[np.ndarray, int]:
    """Record audio from the default microphone.

    Returns the audio data as a numpy array and the sample rate.
    """
    console.print(f"\n[bold yellow]Recording for {duration} seconds...[/bold yellow]")
    console.print("[dim]Speak now! Press Ctrl+C to stop early.[/dim]\n")

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=40),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("[red]● Recording", total=duration)
            chunks = []
            chunk_duration = 0.5  # Record in 0.5s chunks for progress updates
            elapsed = 0.0

            while elapsed < duration:
                remaining = min(chunk_duration, duration - elapsed)
                chunk = sd.rec(
                    int(remaining * sample_rate),
                    samplerate=sample_rate,
                    channels=1,
                    dtype="float32",
                )
                sd.wait()
                chunks.append(chunk)
                elapsed += remaining
                progress.update(task, completed=min(elapsed, duration))

    except KeyboardInterrupt:
        console.print("\n[yellow]Recording stopped early.[/yellow]")

    if not chunks:
        return np.array([], dtype="float32"), sample_rate

    audio = np.concatenate(chunks, axis=0).flatten()
    console.print(f"[green]✓ Recorded {len(audio) / sample_rate:.1f}s of audio[/green]")
    return audio, sample_rate


def save_audio(audio: np.ndarray, sample_rate: int, path: str | Path | None = None) -> Path:
    """Save audio data to a WAV file."""
    if path is None:
        tmp = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
        path = Path(tmp.name)
        tmp.close()
    else:
        path = Path(path)

    sf.write(str(path), audio, sample_rate)
    return path


def play_audio(path: str | Path, sample_rate: int = SAMPLE_RATE) -> None:
    """Play an audio file through the default output device."""
    data, sr = sf.read(str(path))
    console.print("[dim]Playing audio...[/dim]")
    sd.play(data, sr)
    sd.wait()
    console.print("[dim]Playback finished.[/dim]")
