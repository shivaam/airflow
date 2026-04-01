"""YouTube shadowing — download audio and compare with user's speech."""

import subprocess
import tempfile
from pathlib import Path

import numpy as np
from rich.console import Console

from .pitch import extract_pitch
from .recorder import play_audio

console = Console()


def download_youtube_audio(
    url: str,
    start_time: float = 0,
    duration: float = 30,
    output_dir: str | Path | None = None,
) -> Path:
    """Download audio from a YouTube video using yt-dlp.

    Args:
        url: YouTube video URL
        start_time: Start time in seconds
        duration: Duration to download in seconds
        output_dir: Directory to save the audio file

    Returns:
        Path to the downloaded WAV file
    """
    if output_dir is None:
        output_dir = Path(tempfile.mkdtemp())
    else:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "reference.wav"

    console.print(f"[dim]Downloading audio from YouTube...[/dim]")

    # Download with yt-dlp, convert to WAV at 16kHz mono
    cmd = [
        "yt-dlp",
        "--extract-audio",
        "--audio-format", "wav",
        "--postprocessor-args", f"ffmpeg:-ar 16000 -ac 1 -ss {start_time} -t {duration}",
        "--output", str(output_dir / "reference.%(ext)s"),
        "--no-playlist",
        "--quiet",
        url,
    ]

    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except FileNotFoundError:
        console.print("[red]Error: yt-dlp not found. Install it: pip install yt-dlp[/red]")
        raise SystemExit(1)
    except subprocess.CalledProcessError as e:
        console.print(f"[red]Error downloading audio: {e.stderr}[/red]")
        raise SystemExit(1)

    if not output_path.exists():
        # yt-dlp might produce a different extension; find the WAV
        wav_files = list(output_dir.glob("reference*.wav"))
        if wav_files:
            output_path = wav_files[0]
        else:
            console.print("[red]Error: Could not find downloaded audio file[/red]")
            raise SystemExit(1)

    console.print(f"[green]✓ Downloaded reference audio ({duration}s)[/green]")
    return output_path


def compute_pitch_similarity(
    ref_pitch: dict,
    user_pitch: dict,
) -> dict:
    """Compare two pitch contours and compute similarity metrics.

    Args:
        ref_pitch: Output from extract_pitch() for reference
        user_pitch: Output from extract_pitch() for user

    Returns:
        dict with similarity metrics
    """
    ref_freqs = ref_pitch["frequencies"]
    user_freqs = user_pitch["frequencies"]
    ref_voiced = ref_pitch["voiced_mask"]
    user_voiced = user_pitch["voiced_mask"]

    # Normalize both to same length for comparison
    target_len = min(len(ref_freqs), len(user_freqs))
    if target_len == 0:
        return {"correlation": 0, "pitch_distance": 0, "rhythm_score": 0}

    ref_f = np.interp(
        np.linspace(0, 1, target_len),
        np.linspace(0, 1, len(ref_freqs)),
        ref_freqs,
    )
    user_f = np.interp(
        np.linspace(0, 1, target_len),
        np.linspace(0, 1, len(user_freqs)),
        user_freqs,
    )
    ref_v = np.interp(
        np.linspace(0, 1, target_len),
        np.linspace(0, 1, len(ref_voiced)),
        ref_voiced.astype(float),
    ) > 0.5
    user_v = np.interp(
        np.linspace(0, 1, target_len),
        np.linspace(0, 1, len(user_voiced)),
        user_voiced.astype(float),
    ) > 0.5

    # Pitch correlation (only where both are voiced)
    both_voiced = ref_v & user_v
    correlation = 0.0
    pitch_distance = 0.0

    if np.sum(both_voiced) > 10:
        r_voiced = ref_f[both_voiced]
        u_voiced = user_f[both_voiced]

        # Normalize to semitones relative to mean
        r_semi = 12 * np.log2(r_voiced / np.mean(r_voiced) + 1e-10)
        u_semi = 12 * np.log2(u_voiced / np.mean(u_voiced) + 1e-10)

        # Correlation of pitch contour shape
        if np.std(r_semi) > 0 and np.std(u_semi) > 0:
            correlation = float(np.corrcoef(r_semi, u_semi)[0, 1])

        # Mean absolute distance in semitones
        pitch_distance = float(np.mean(np.abs(r_semi - u_semi)))

    # Rhythm similarity (voiced/unvoiced pattern match)
    rhythm_score = float(np.mean(ref_v == user_v))

    # Pace comparison
    ref_rate = ref_pitch["stats"].get("mean", 0)
    user_rate = user_pitch["stats"].get("mean", 0)

    return {
        "correlation": round(correlation, 3),
        "pitch_distance_semitones": round(pitch_distance, 1),
        "rhythm_score": round(rhythm_score, 3),
        "ref_mean_pitch": round(ref_rate, 1),
        "user_mean_pitch": round(user_rate, 1),
    }
