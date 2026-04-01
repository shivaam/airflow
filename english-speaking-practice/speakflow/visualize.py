"""Pitch and speech visualization — terminal ASCII + PNG export."""

from pathlib import Path

import numpy as np
import plotext as plt
from rich.console import Console

console = Console()


def plot_pitch_terminal(
    times: np.ndarray,
    frequencies: np.ndarray,
    title: str = "Pitch Contour (F0)",
    voiced_mask: np.ndarray | None = None,
) -> None:
    """Display pitch contour as ASCII graph in terminal."""
    # Filter to voiced regions only for cleaner display
    if voiced_mask is not None:
        plot_times = times[voiced_mask]
        plot_freqs = frequencies[voiced_mask]
    else:
        mask = frequencies > 0
        plot_times = times[mask]
        plot_freqs = frequencies[mask]

    if len(plot_times) == 0:
        console.print("[yellow]No voiced speech detected to plot.[/yellow]")
        return

    plt.clear_figure()
    plt.plot(plot_times.tolist(), plot_freqs.tolist(), marker="braille")
    plt.title(title)
    plt.xlabel("Time (s)")
    plt.ylabel("Frequency (Hz)")
    plt.plot_size(80, 25)
    plt.theme("dark")
    plt.show()


def plot_pitch_comparison_terminal(
    ref_times: np.ndarray,
    ref_freqs: np.ndarray,
    user_times: np.ndarray,
    user_freqs: np.ndarray,
    ref_label: str = "Reference",
    user_label: str = "You",
) -> None:
    """Display two pitch contours overlaid for comparison."""
    # Filter to voiced only
    ref_mask = ref_freqs > 0
    user_mask = user_freqs > 0

    ref_t = ref_times[ref_mask]
    ref_f = ref_freqs[ref_mask]
    user_t = user_times[user_mask]
    user_f = user_freqs[user_mask]

    if len(ref_t) == 0 and len(user_t) == 0:
        console.print("[yellow]No voiced speech detected in either recording.[/yellow]")
        return

    plt.clear_figure()

    if len(ref_t) > 0:
        plt.plot(ref_t.tolist(), ref_f.tolist(), label=ref_label, marker="braille")
    if len(user_t) > 0:
        plt.plot(user_t.tolist(), user_f.tolist(), label=user_label, marker="braille")

    plt.title("Pitch Comparison")
    plt.xlabel("Time (s)")
    plt.ylabel("Frequency (Hz)")
    plt.plot_size(80, 25)
    plt.theme("dark")
    plt.show()


def plot_intensity_terminal(
    times: np.ndarray,
    values: np.ndarray,
    title: str = "Intensity (Loudness)",
) -> None:
    """Display intensity contour as ASCII graph."""
    if len(times) == 0:
        return

    plt.clear_figure()
    plt.plot(times.tolist(), values.tolist(), marker="braille")
    plt.title(title)
    plt.xlabel("Time (s)")
    plt.ylabel("Intensity (dB)")
    plt.plot_size(80, 15)
    plt.theme("dark")
    plt.show()


def save_pitch_png(
    times: np.ndarray,
    frequencies: np.ndarray,
    output_path: str | Path,
    title: str = "Pitch Contour (F0)",
    voiced_mask: np.ndarray | None = None,
    ref_times: np.ndarray | None = None,
    ref_freqs: np.ndarray | None = None,
) -> Path:
    """Save pitch contour as a PNG image using matplotlib."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as mplt

    fig, ax = mplt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor("#1a1a2e")
    ax.set_facecolor("#1a1a2e")

    # Plot reference if provided
    if ref_times is not None and ref_freqs is not None:
        ref_mask = ref_freqs > 0
        if np.any(ref_mask):
            ax.plot(
                ref_times[ref_mask], ref_freqs[ref_mask],
                color="#4c6ef5", alpha=0.7, linewidth=2, label="Reference",
            )

    # Plot user pitch
    if voiced_mask is not None:
        plot_t = times[voiced_mask]
        plot_f = frequencies[voiced_mask]
    else:
        mask = frequencies > 0
        plot_t = times[mask]
        plot_f = frequencies[mask]

    if len(plot_t) > 0:
        label = "You" if ref_times is not None else "Pitch"
        ax.plot(plot_t, plot_f, color="#ff6b6b", linewidth=2, label=label)

    ax.set_title(title, color="white", fontsize=14, fontweight="bold")
    ax.set_xlabel("Time (s)", color="white")
    ax.set_ylabel("Frequency (Hz)", color="white")
    ax.tick_params(colors="white")
    ax.spines["bottom"].set_color("#333")
    ax.spines["left"].set_color("#333")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    if ref_times is not None:
        ax.legend(facecolor="#1a1a2e", edgecolor="#333", labelcolor="white")

    output_path = Path(output_path)
    fig.savefig(output_path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    mplt.close(fig)

    console.print(f"[green]✓ Saved pitch graph to {output_path}[/green]")
    return output_path
