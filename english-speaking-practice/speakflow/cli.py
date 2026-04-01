"""SpeakFlow CLI — English Speaking Practice."""

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import box

app = typer.Typer(
    name="speakflow",
    help="English speaking practice with AI feedback, shadowing, and intonation analysis.",
    no_args_is_help=True,
)
console = Console()


@app.command()
def practice(
    duration: int = typer.Option(60, "--duration", "-d", help="Recording duration in seconds"),
    prompt: Optional[str] = typer.Option(None, "--prompt", "-p", help="Custom speaking prompt (or random)"),
    difficulty: Optional[str] = typer.Option(None, "--difficulty", help="beginner, intermediate, or advanced"),
    model: str = typer.Option("base.en", "--model", "-m", help="Whisper model size"),
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", help="Anthropic API key"),
    save_audio: Optional[Path] = typer.Option(None, "--save-audio", help="Save recording to this path"),
) -> None:
    """Record yourself speaking, get vocabulary and phrasing feedback from Claude."""
    from .feedback import get_feedback
    from .prompts import get_random_prompt
    from .recorder import record_audio, save_audio as save_wav
    from .transcriber import transcribe_numpy

    # Show prompt
    if prompt is None:
        p = get_random_prompt(difficulty=difficulty)
        prompt = p["text"]
        console.print(Panel(
            f"[bold]{prompt}[/bold]\n\n[dim]{p['category']} · {p['difficulty']}[/dim]",
            title="[bold blue]Speaking Prompt[/bold blue]",
            border_style="blue",
            padding=(1, 2),
        ))
    else:
        console.print(Panel(
            f"[bold]{prompt}[/bold]",
            title="[bold blue]Your Topic[/bold blue]",
            border_style="blue",
            padding=(1, 2),
        ))

    console.print()
    input("[dim]Press Enter when ready to start recording...[/dim]")

    # Record
    audio, sr = record_audio(duration)
    if len(audio) == 0:
        console.print("[red]No audio recorded.[/red]")
        raise typer.Exit(1)

    # Save if requested
    if save_audio:
        save_wav(audio, sr, save_audio)
        console.print(f"[dim]Audio saved to {save_audio}[/dim]")

    # Transcribe
    result = transcribe_numpy(audio, sr, model_size=model)
    transcript = result["text"]

    if not transcript.strip():
        console.print("[yellow]No speech detected in recording.[/yellow]")
        raise typer.Exit(1)

    console.print(Panel(
        transcript,
        title="[bold]Your Speech[/bold]",
        border_style="white",
        padding=(1, 2),
    ))

    # Get feedback
    feedback = get_feedback(transcript, api_key=api_key)
    _display_feedback(feedback)


@app.command()
def analyze(
    duration: int = typer.Option(30, "--duration", "-d", help="Recording duration in seconds"),
    save_png: Optional[Path] = typer.Option(None, "--save-png", help="Save pitch graph as PNG"),
    save_audio: Optional[Path] = typer.Option(None, "--save-audio", help="Save recording to path"),
) -> None:
    """Record yourself and analyze pitch, intonation, pace, and pauses."""
    from .pitch import (
        analyze_intonation_patterns,
        detect_pauses,
        estimate_speaking_rate,
        extract_intensity,
        extract_pitch,
    )
    from .recorder import record_audio, save_audio as save_wav
    from .visualize import plot_intensity_terminal, plot_pitch_terminal, save_pitch_png

    console.print(Panel(
        "[bold]Speak naturally for the recording duration.\n"
        "Try reading a passage or talking about any topic.[/bold]",
        title="[bold green]Intonation Analysis[/bold green]",
        border_style="green",
        padding=(1, 2),
    ))

    input("[dim]Press Enter when ready to start recording...[/dim]")

    # Record
    audio, sr = record_audio(duration)
    if len(audio) == 0:
        console.print("[red]No audio recorded.[/red]")
        raise typer.Exit(1)

    # Save WAV for analysis
    wav_path = save_wav(audio, sr, save_audio)
    if save_audio:
        console.print(f"[dim]Audio saved to {save_audio}[/dim]")

    # Extract features
    console.print("\n[dim]Analyzing speech...[/dim]")
    pitch_data = extract_pitch(wav_path)
    intensity_data = extract_intensity(wav_path)
    pauses = detect_pauses(wav_path)
    rate = estimate_speaking_rate(wav_path)
    patterns = analyze_intonation_patterns(pitch_data)

    # Display pitch graph
    console.print()
    plot_pitch_terminal(
        pitch_data["times"],
        pitch_data["frequencies"],
        voiced_mask=pitch_data["voiced_mask"],
    )

    # Display intensity
    plot_intensity_terminal(intensity_data["times"], intensity_data["values"])

    # Stats table
    stats = pitch_data["stats"]
    table = Table(title="Speech Analysis", box=box.ROUNDED, border_style="green")
    table.add_column("Metric", style="bold")
    table.add_column("Value", justify="right")
    table.add_column("Assessment", style="dim")

    # Pitch stats
    table.add_row("Average Pitch", f"{stats['mean']:.0f} Hz", _pitch_assessment(stats["mean"]))
    table.add_row("Pitch Range", f"{stats['range']:.0f} Hz", _range_assessment(stats["range"]))
    table.add_row("Pitch Std Dev", f"{stats['std']:.1f} Hz", _variability_assessment(stats["std"]))

    # Speaking rate
    table.add_row("Speaking Rate", f"{rate['words_per_minute']} WPM", _rate_assessment(rate["words_per_minute"]))
    table.add_row("Syllables/sec", f"{rate['syllables_per_second']}", "")

    # Pauses
    table.add_row("Pauses Detected", str(len(pauses)), _pause_assessment(len(pauses), rate["duration"]))

    # Duration
    table.add_row("Duration", f"{rate['duration']}s", "")

    console.print()
    console.print(table)

    # Intonation patterns summary
    if patterns:
        rising = sum(1 for p in patterns if p["pattern"] == "rising")
        falling = sum(1 for p in patterns if p["pattern"] == "falling")
        flat = sum(1 for p in patterns if p["pattern"] == "flat")

        console.print(f"\n[bold]Intonation Patterns:[/bold]")
        console.print(f"  Rising: {rising}  |  Falling: {falling}  |  Flat: {flat}")

        if flat > rising + falling:
            console.print("[yellow]  → Your speech is relatively monotone. Try varying your pitch more![/yellow]")
        elif rising > falling * 2:
            console.print("[yellow]  → Lots of rising intonation — this can sound uncertain. Try ending statements with falling pitch.[/yellow]")
        else:
            console.print("[green]  → Good mix of intonation patterns![/green]")

    # Save PNG if requested
    if save_png:
        save_pitch_png(
            pitch_data["times"],
            pitch_data["frequencies"],
            save_png,
            voiced_mask=pitch_data["voiced_mask"],
        )

    # Clean up temp file
    if not save_audio:
        wav_path.unlink(missing_ok=True)


@app.command()
def shadow(
    url: str = typer.Argument(..., help="YouTube video URL"),
    start: float = typer.Option(0, "--start", "-s", help="Start time in seconds"),
    duration: float = typer.Option(30, "--duration", "-d", help="Duration in seconds"),
    save_png: Optional[Path] = typer.Option(None, "--save-png", help="Save comparison graph as PNG"),
) -> None:
    """Shadow a YouTube video — compare your pitch and rhythm with the original."""
    from .pitch import extract_pitch
    from .recorder import play_audio, record_audio, save_audio as save_wav
    from .shadow import compute_pitch_similarity, download_youtube_audio
    from .visualize import plot_pitch_comparison_terminal, save_pitch_png

    # Download reference audio
    ref_path = download_youtube_audio(url, start_time=start, duration=duration)

    # Play reference
    console.print(Panel(
        "[bold]Listen to the reference audio, then you'll repeat it.[/bold]",
        title="[bold magenta]Shadowing[/bold magenta]",
        border_style="magenta",
        padding=(1, 2),
    ))

    input("[dim]Press Enter to play reference audio...[/dim]")
    play_audio(ref_path)

    # Record user
    console.print()
    input("[dim]Press Enter when ready to record your version...[/dim]")
    audio, sr = record_audio(int(duration) + 5)  # Extra 5s buffer

    if len(audio) == 0:
        console.print("[red]No audio recorded.[/red]")
        raise typer.Exit(1)

    user_path = save_wav(audio, sr)

    # Extract pitch from both
    console.print("\n[dim]Analyzing both recordings...[/dim]")
    ref_pitch = extract_pitch(ref_path)
    user_pitch = extract_pitch(user_path)

    # Display comparison graph
    console.print()
    plot_pitch_comparison_terminal(
        ref_pitch["times"], ref_pitch["frequencies"],
        user_pitch["times"], user_pitch["frequencies"],
    )

    # Compute similarity
    similarity = compute_pitch_similarity(ref_pitch, user_pitch)

    # Display results
    table = Table(title="Shadowing Results", box=box.ROUNDED, border_style="magenta")
    table.add_column("Metric", style="bold")
    table.add_column("Value", justify="right")
    table.add_column("Assessment", style="dim")

    corr = similarity["correlation"]
    table.add_row(
        "Pitch Correlation",
        f"{corr:.1%}",
        "Excellent" if corr > 0.7 else "Good" if corr > 0.4 else "Keep practicing",
    )

    dist = similarity["pitch_distance_semitones"]
    table.add_row(
        "Pitch Distance",
        f"{dist:.1f} semitones",
        "Very close" if dist < 2 else "Close" if dist < 4 else "Room to improve",
    )

    rhythm = similarity["rhythm_score"]
    table.add_row(
        "Rhythm Match",
        f"{rhythm:.1%}",
        "Great rhythm" if rhythm > 0.7 else "Good" if rhythm > 0.5 else "Work on timing",
    )

    table.add_row("Reference Avg Pitch", f"{similarity['ref_mean_pitch']:.0f} Hz", "")
    table.add_row("Your Avg Pitch", f"{similarity['user_mean_pitch']:.0f} Hz", "")

    console.print()
    console.print(table)

    # Tips
    console.print("\n[bold]Tips:[/bold]")
    if corr < 0.5:
        console.print("  • Focus on matching the melody/intonation pattern of the speaker")
    if dist > 3:
        console.print("  • Try to match the speaker's pitch level — you don't need to copy exactly, but follow the contour")
    if rhythm < 0.6:
        console.print("  • Work on matching the rhythm — pause where the speaker pauses, speed up where they do")
    if corr > 0.7 and rhythm > 0.7:
        console.print("  • [green]Great job! Try a harder passage or increase the speed.[/green]")

    # Save PNG
    if save_png:
        save_pitch_png(
            user_pitch["times"],
            user_pitch["frequencies"],
            save_png,
            title="Shadowing Comparison",
            voiced_mask=user_pitch["voiced_mask"],
            ref_times=ref_pitch["times"],
            ref_freqs=ref_pitch["frequencies"],
        )

    # Cleanup
    user_path.unlink(missing_ok=True)
    ref_path.unlink(missing_ok=True)


def _pitch_assessment(mean_hz: float) -> str:
    if mean_hz == 0:
        return "No pitch detected"
    if mean_hz < 100:
        return "Low pitch (typical male)"
    if mean_hz < 180:
        return "Mid pitch"
    return "Higher pitch (typical female)"


def _range_assessment(range_hz: float) -> str:
    if range_hz < 50:
        return "Narrow — try more expression"
    if range_hz < 120:
        return "Normal range"
    return "Wide — very expressive!"


def _variability_assessment(std: float) -> str:
    if std < 15:
        return "Low variation (monotone)"
    if std < 40:
        return "Good variation"
    return "High variation"


def _rate_assessment(wpm: int) -> str:
    if wpm < 100:
        return "Slow — normal for practice"
    if wpm < 150:
        return "Good conversational pace"
    if wpm < 180:
        return "Brisk pace"
    return "Very fast"


def _pause_assessment(count: int, duration: float) -> str:
    pauses_per_min = count / (duration / 60) if duration > 0 else 0
    if pauses_per_min < 3:
        return "Very few pauses"
    if pauses_per_min < 8:
        return "Natural pause frequency"
    return "Frequent pauses"


if __name__ == "__main__":
    app()
