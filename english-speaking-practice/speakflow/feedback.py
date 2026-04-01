"""LLM-powered vocabulary and phrasing feedback using Claude."""

import os

from anthropic import Anthropic
from rich.console import Console

console = Console()

FEEDBACK_SYSTEM_PROMPT = """\
You are an expert English language coach specializing in helping people improve their spoken English vocabulary and fluency.

You will receive a transcript of someone speaking. Your job is to:

1. Identify phrases that could be expressed more naturally, vividly, or precisely
2. Suggest vocabulary upgrades (e.g., "ate really fast" → "devoured", "very tired" → "exhausted")
3. Point out grammar issues if any
4. Suggest idiomatic expressions where appropriate
5. Note what the speaker did well

IMPORTANT GUIDELINES:
- Focus on the MOST impactful improvements (top 5-8), not every minor thing
- Prioritize vocabulary richness and natural phrasing over grammar pedantry
- Suggest alternatives that a native speaker would actually use in conversation
- Be encouraging — highlight strengths alongside improvements
- Consider the context and register (casual vs formal)

Respond in this exact JSON format:
{
  "overall_score": <1-10 fluency score>,
  "summary": "<1-2 sentence overall assessment>",
  "strengths": ["<strength 1>", "<strength 2>"],
  "improvements": [
    {
      "original": "<exact phrase from transcript>",
      "improved": "<better alternative>",
      "explanation": "<brief why this is better>",
      "category": "<vocabulary|grammar|fluency|idiom|formality>"
    }
  ],
  "tips": ["<actionable tip 1>", "<actionable tip 2>"]
}"""


def get_feedback(transcript: str, api_key: str | None = None) -> dict:
    """Get vocabulary and phrasing feedback from Claude.

    Args:
        transcript: The speech transcript to analyze.
        api_key: Anthropic API key. Falls back to ANTHROPIC_API_KEY env var.

    Returns:
        Parsed feedback dict with improvements and suggestions.
    """
    key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        console.print("[red]Error: No API key. Set ANTHROPIC_API_KEY or pass --api-key[/red]")
        raise SystemExit(1)

    client = Anthropic(api_key=key)

    console.print("[dim]Getting feedback from Claude...[/dim]")

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2000,
        system=FEEDBACK_SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": f"Here is the transcript of my spoken English. Please analyze it and provide feedback:\n\n\"{transcript}\"",
            }
        ],
    )

    import json

    text = response.content[0].text

    # Extract JSON from response (handle markdown code blocks)
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0]
    elif "```" in text:
        text = text.split("```")[1].split("```")[0]

    try:
        feedback = json.loads(text.strip())
    except json.JSONDecodeError:
        console.print("[yellow]Warning: Could not parse structured feedback, showing raw response[/yellow]")
        feedback = {
            "overall_score": 0,
            "summary": text,
            "strengths": [],
            "improvements": [],
            "tips": [],
        }

    console.print("[green]✓ Feedback received[/green]")
    return feedback
