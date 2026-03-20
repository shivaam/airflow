#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Send a notification message to Telegram.

Usage::

    # Simple message
    uv run --project dev python dev/telegram_notify.py "Hello from Airflow!"

    # Pipe content from stdin
    echo "Build passed" | uv run --project dev python dev/telegram_notify.py --stdin

    # Read message from a file
    uv run --project dev python dev/telegram_notify.py --file dev/reports/activity_summary_2026-03-20.md

Environment variables:
    TELEGRAM_BOT_TOKEN  - Bot token from @BotFather
    TELEGRAM_CHAT_ID    - Your numeric chat ID
"""

from __future__ import annotations

import sys

import requests
import rich_click as click
from rich.console import Console

console = Console(width=120, color_system="standard")

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
MAX_MESSAGE_LENGTH = 4096


def send_telegram_message(token: str, chat_id: str, text: str, parse_mode: str | None = None) -> None:
    """Send a message via the Telegram Bot API, splitting if needed."""
    chunks = _split_message(text, MAX_MESSAGE_LENGTH)
    for i, chunk in enumerate(chunks, 1):
        payload: dict[str, str] = {
            "chat_id": chat_id,
            "text": chunk,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode

        resp = requests.post(TELEGRAM_API.format(token=token), json=payload, timeout=30)
        if not resp.ok:
            error_detail = resp.json().get("description", resp.text)
            raise RuntimeError(f"Telegram API error: {resp.status_code} - {error_detail}")

        if len(chunks) > 1:
            console.print(f"[dim]Sent chunk {i}/{len(chunks)}[/]")


def _split_message(text: str, max_len: int) -> list[str]:
    """Split a long message into chunks, breaking at newlines when possible."""
    if len(text) <= max_len:
        return [text]

    chunks: list[str] = []
    while text:
        if len(text) <= max_len:
            chunks.append(text)
            break
        # Try to break at a newline
        split_at = text.rfind("\n", 0, max_len)
        if split_at == -1:
            split_at = max_len
        chunks.append(text[:split_at])
        text = text[split_at:].lstrip("\n")
    return chunks


@click.command(context_settings={"help_option_names": ["-h", "--help"], "max_content_width": 120})
@click.argument("message", required=False)
@click.option("--token", envvar="TELEGRAM_BOT_TOKEN", required=True, help="Telegram bot token.")
@click.option("--chat-id", envvar="TELEGRAM_CHAT_ID", required=True, help="Telegram chat ID.")
@click.option("--stdin", "from_stdin", is_flag=True, help="Read message from stdin.")
@click.option("--file", "file_path", type=click.Path(exists=True), help="Read message from a file.")
@click.option(
    "--parse-mode",
    type=click.Choice(["Markdown", "MarkdownV2", "HTML"], case_sensitive=False),
    default=None,
    help="Telegram parse mode for formatting.",
)
def main(
    message: str | None,
    token: str,
    chat_id: str,
    from_stdin: bool,
    file_path: str | None,
    parse_mode: str | None,
) -> None:
    """Send a notification message to your Telegram bot."""
    if file_path:
        with open(file_path) as f:
            text = f.read().strip()
    elif from_stdin:
        text = sys.stdin.read().strip()
    elif message:
        text = message
    else:
        console.print("[red]Provide a message argument, --stdin, or --file.[/]")
        sys.exit(1)

    if not text:
        console.print("[yellow]Empty message, nothing to send.[/]")
        sys.exit(0)

    send_telegram_message(token, chat_id, text, parse_mode)
    console.print("[green]Message sent to Telegram![/]")


if __name__ == "__main__":
    main()
