"""Pitch and intonation analysis using Parselmouth (Praat)."""

from pathlib import Path

import numpy as np
import parselmouth
from parselmouth.praat import call
from rich.console import Console

console = Console()


def extract_pitch(
    audio_path: str | Path,
    time_step: float = 0.01,
    pitch_floor: float = 75.0,
    pitch_ceiling: float = 500.0,
) -> dict:
    """Extract pitch (F0) contour from audio file.

    Returns:
        dict with:
        - times: array of time points (seconds)
        - frequencies: array of F0 values (Hz), 0 where unvoiced
        - voiced_mask: boolean array, True where pitch is detected
        - stats: dict with mean, median, min, max, std, range
    """
    snd = parselmouth.Sound(str(audio_path))
    pitch = snd.to_pitch(time_step=time_step, pitch_floor=pitch_floor, pitch_ceiling=pitch_ceiling)

    times = pitch.xs()
    frequencies = np.array([pitch.get_value_at_time(t) for t in times])

    # Replace NaN with 0 (unvoiced regions)
    frequencies = np.nan_to_num(frequencies, nan=0.0)
    voiced_mask = frequencies > 0

    voiced_freqs = frequencies[voiced_mask]
    stats = {}
    if len(voiced_freqs) > 0:
        stats = {
            "mean": float(np.mean(voiced_freqs)),
            "median": float(np.median(voiced_freqs)),
            "min": float(np.min(voiced_freqs)),
            "max": float(np.max(voiced_freqs)),
            "std": float(np.std(voiced_freqs)),
            "range": float(np.max(voiced_freqs) - np.min(voiced_freqs)),
        }
    else:
        stats = {"mean": 0, "median": 0, "min": 0, "max": 0, "std": 0, "range": 0}

    return {
        "times": times,
        "frequencies": frequencies,
        "voiced_mask": voiced_mask,
        "stats": stats,
    }


def extract_intensity(audio_path: str | Path, time_step: float = 0.01) -> dict:
    """Extract intensity (loudness) contour from audio file."""
    snd = parselmouth.Sound(str(audio_path))
    intensity = snd.to_intensity(time_step=time_step)

    times = intensity.xs()
    values = np.array([intensity.get_value(t) for t in times])
    values = np.nan_to_num(values, nan=0.0)

    return {
        "times": times,
        "values": values,
        "mean": float(np.mean(values[values > 0])) if np.any(values > 0) else 0,
    }


def detect_pauses(audio_path: str | Path, min_pause_duration: float = 0.3) -> list[dict]:
    """Detect silent pauses in speech.

    Returns list of dicts with start, end, duration for each pause.
    """
    snd = parselmouth.Sound(str(audio_path))
    intensity = snd.to_intensity(time_step=0.01)

    times = intensity.xs()
    values = np.array([intensity.get_value(t) for t in times])
    values = np.nan_to_num(values, nan=0.0)

    # Threshold: below 50% of mean intensity = silence
    threshold = np.mean(values[values > 0]) * 0.5 if np.any(values > 0) else 0
    is_silent = values < threshold

    pauses = []
    in_pause = False
    pause_start = 0.0

    for i, (t, silent) in enumerate(zip(times, is_silent)):
        if silent and not in_pause:
            in_pause = True
            pause_start = t
        elif not silent and in_pause:
            in_pause = False
            duration = t - pause_start
            if duration >= min_pause_duration:
                pauses.append({"start": pause_start, "end": t, "duration": duration})

    return pauses


def estimate_speaking_rate(audio_path: str | Path) -> dict:
    """Estimate speaking rate using intensity peaks (syllable approximation).

    Returns dict with syllables_per_second and words_per_minute estimate.
    """
    snd = parselmouth.Sound(str(audio_path))
    intensity = snd.to_intensity(time_step=0.01)
    duration = snd.get_total_duration()

    times = intensity.xs()
    values = np.array([intensity.get_value(t) for t in times])
    values = np.nan_to_num(values, nan=0.0)

    if len(values) < 3:
        return {"syllables_per_second": 0, "words_per_minute": 0, "duration": duration}

    # Count intensity peaks as syllable approximation
    from scipy.signal import find_peaks

    # Normalize
    if np.max(values) > 0:
        norm_values = values / np.max(values)
    else:
        return {"syllables_per_second": 0, "words_per_minute": 0, "duration": duration}

    peaks, _ = find_peaks(norm_values, height=0.4, distance=5)
    n_syllables = len(peaks)

    syll_per_sec = n_syllables / duration if duration > 0 else 0
    # Rough approximation: ~1.5 syllables per word
    wpm = (syll_per_sec / 1.5) * 60

    return {
        "syllables_per_second": round(syll_per_sec, 1),
        "words_per_minute": round(wpm),
        "duration": round(duration, 1),
        "syllable_count": n_syllables,
    }


def analyze_intonation_patterns(pitch_data: dict) -> list[dict]:
    """Analyze intonation patterns (rising, falling, flat) in segments.

    Takes the output of extract_pitch() and identifies patterns.
    """
    times = pitch_data["times"]
    freqs = pitch_data["frequencies"]
    voiced = pitch_data["voiced_mask"]

    if not np.any(voiced):
        return []

    # Split into voiced segments
    segments = []
    in_segment = False
    seg_start = 0

    for i in range(len(voiced)):
        if voiced[i] and not in_segment:
            in_segment = True
            seg_start = i
        elif not voiced[i] and in_segment:
            in_segment = False
            if i - seg_start >= 5:  # At least 50ms of voiced speech
                segments.append((seg_start, i))

    if in_segment and len(voiced) - seg_start >= 5:
        segments.append((seg_start, len(voiced)))

    patterns = []
    for start, end in segments:
        seg_freqs = freqs[start:end]
        seg_times = times[start:end]

        # Linear regression to determine trend
        x = np.arange(len(seg_freqs))
        coeffs = np.polyfit(x, seg_freqs, 1)
        slope = coeffs[0]

        # Classify pattern
        if slope > 1.0:
            pattern = "rising"
        elif slope < -1.0:
            pattern = "falling"
        else:
            pattern = "flat"

        patterns.append({
            "start_time": float(seg_times[0]),
            "end_time": float(seg_times[-1]),
            "pattern": pattern,
            "slope": float(slope),
            "mean_pitch": float(np.mean(seg_freqs)),
        })

    return patterns
