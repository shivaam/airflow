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
"""Fetch GitHub and Apache mailing list activity for a user and print structured output.

This script is designed to be called by the Claude Code ``/summarize-activity`` skill.
Claude reads the structured text output and generates an actionable summary.

Usage::

    uv run --project dev python dev/summarize_user_activity.py --config ~/.airflow-activity.yaml
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from time import sleep
from typing import Any

import requests
import rich_click as click
import yaml
from github import Auth, Github, GithubException
from rich.console import Console

logger = logging.getLogger(__name__)
console = Console(width=400, color_system="standard")

STATE_FILE = os.path.expanduser("~/.airflow-activity-state.json")
DEFAULT_CONFIG_PATH = os.path.expanduser("~/.airflow-activity.yaml")
PONY_MAIL_API = "https://lists.apache.org/api/stats.lua"

# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------


def load_config(path: str) -> dict[str, Any]:
    """Load and validate the YAML configuration file."""
    if not os.path.exists(path):
        console.print(f"[red]Config file not found: {path}[/]")
        console.print(
            "[yellow]Copy dev/activity_config_example.yaml to ~/.airflow-activity.yaml and edit it.[/]"
        )
        sys.exit(1)

    with open(path) as f:
        config = yaml.safe_load(f)

    if not config:
        console.print("[red]Config file is empty.[/]")
        sys.exit(1)

    required = ["username", "repos"]
    for key in required:
        if key not in config:
            console.print(f"[red]Missing required config key: {key}[/]")
            sys.exit(1)

    config.setdefault("labels", [])
    config.setdefault("mailing_lists", [])
    config.setdefault("mailing_list_prefixes", [])
    return config


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------


def load_state() -> dict[str, Any]:
    """Load the last-run state from disk."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def save_state(state: dict[str, Any]) -> None:
    """Persist state to disk."""
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def resolve_since(since_arg: str | None, state: dict[str, Any]) -> datetime:
    """Determine the *since* cutoff datetime.

    Priority: CLI ``--since`` flag > state file ``last_run`` > 3 days ago.
    """
    if since_arg:
        return datetime.fromisoformat(since_arg).replace(tzinfo=timezone.utc)
    last_run = state.get("last_run")
    if last_run:
        return datetime.fromisoformat(last_run).replace(tzinfo=timezone.utc)
    return datetime.now(tz=timezone.utc) - timedelta(days=3)


# ---------------------------------------------------------------------------
# GitHub fetchers
# ---------------------------------------------------------------------------


def _gh_search_with_retry(gh: Github, query: str, max_retries: int = 3) -> list:
    """Run a GitHub search, retrying on secondary rate-limit errors."""
    for attempt in range(max_retries):
        try:
            results = list(gh.search_issues(query))
            sleep(2)  # avoid secondary rate limit
            return results
        except GithubException as exc:
            if exc.status == 403 and attempt < max_retries - 1:
                wait = 2 ** (attempt + 2)
                console.print(f"[yellow]Rate-limited; waiting {wait}s before retry…[/]")
                sleep(wait)
            else:
                raise
    return []


def fetch_user_commented_issues(
    gh: Github,
    repos: list[str],
    username: str,
    since: datetime,
) -> dict[str, list[dict[str, Any]]]:
    """Find open issues/PRs the user commented on, with recent activity."""
    since_str = since.strftime("%Y-%m-%d")
    results: dict[str, list[dict[str, Any]]] = {}

    for repo_name in repos:
        query = f"commenter:{username} repo:{repo_name} is:open updated:>={since_str}"
        console.print(f"[dim]Searching: {query}[/]", highlight=False)
        issues = _gh_search_with_retry(gh, query)

        items: list[dict[str, Any]] = []
        for issue in issues:
            item: dict[str, Any] = {
                "number": issue.number,
                "title": issue.title,
                "url": issue.html_url,
                "is_pr": issue.pull_request is not None,
                "labels": [lbl.name for lbl in issue.labels],
                "state": issue.state,
                "updated_at": issue.updated_at.isoformat() if issue.updated_at else "",
                "comments": [],
            }

            # Fetch recent comments
            try:
                comments = issue.get_comments(since=since)
                for comment in comments:
                    item["comments"].append(
                        {
                            "user": comment.user.login if comment.user else "unknown",
                            "created_at": comment.created_at.isoformat() if comment.created_at else "",
                            "body": comment.body[:500] if comment.body else "",
                        }
                    )
            except GithubException:
                item["comments"].append(
                    {"user": "error", "created_at": "", "body": "Failed to fetch comments"}
                )

            items.append(item)
            sleep(1)  # gentle pacing for comment fetches

        if items:
            results[repo_name] = items

    return results


def fetch_tagged_activity(
    gh: Github,
    repos: list[str],
    labels: list[str],
    since: datetime,
) -> dict[str, list[dict[str, Any]]]:
    """Find new/updated issues and PRs matching monitored labels."""
    since_str = since.strftime("%Y-%m-%d")
    results: dict[str, list[dict[str, Any]]] = {}

    for repo_name in repos:
        for label in labels:
            query = f'label:"{label}" repo:{repo_name} updated:>={since_str}'
            console.print(f"[dim]Searching: {query}[/]", highlight=False)
            issues = _gh_search_with_retry(gh, query)

            for issue in issues:
                created_recently = issue.created_at and issue.created_at >= since
                key = f"{label}"
                if key not in results:
                    results[key] = []

                results[key].append(
                    {
                        "number": issue.number,
                        "title": issue.title,
                        "url": issue.html_url,
                        "repo": repo_name,
                        "is_pr": issue.pull_request is not None,
                        "is_new": created_recently,
                        "labels": [lbl.name for lbl in issue.labels],
                        "state": issue.state,
                        "created_at": issue.created_at.isoformat() if issue.created_at else "",
                        "updated_at": issue.updated_at.isoformat() if issue.updated_at else "",
                        "comments_count": issue.comments,
                    }
                )

    return results


# ---------------------------------------------------------------------------
# Mailing list fetcher
# ---------------------------------------------------------------------------


def fetch_mailing_list_activity(
    mailing_lists: list[dict[str, str]],
    since: datetime,
    prefixes: list[str],
) -> dict[str, list[dict[str, Any]]]:
    """Fetch recent emails from Apache Pony Mail archives."""
    since_epoch = since.timestamp()
    results: dict[str, list[dict[str, Any]]] = {}

    for ml in mailing_lists:
        list_name = ml["list"]
        domain = ml["domain"]
        full_name = f"{list_name}@{domain}"
        console.print(f"[dim]Fetching mailing list: {full_name}[/]")

        try:
            resp = requests.get(
                PONY_MAIL_API,
                params={"list": list_name, "domain": domain},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except (requests.RequestException, ValueError) as exc:
            console.print(f"[red]Failed to fetch {full_name}: {exc}[/]")
            continue

        emails = data.get("emails", [])
        # Filter by date
        recent = [e for e in emails if e.get("epoch", 0) >= since_epoch]

        if prefixes:
            filtered = []
            for email in recent:
                subject = email.get("subject", "")
                if any(subject.startswith(p) or f" {p}" in subject for p in prefixes):
                    filtered.append(email)
                elif not any(subject.startswith("[") for _ in [1]):
                    # Include emails without any prefix bracket too
                    filtered.append(email)
            recent = filtered

        # Group by thread
        threads: dict[str, list[dict[str, Any]]] = {}
        for email in recent:
            subject = email.get("subject", "(no subject)")
            # Normalize subject for grouping (strip Re: prefixes)
            clean_subject = subject
            while clean_subject.lower().startswith("re: "):
                clean_subject = clean_subject[4:]

            if clean_subject not in threads:
                threads[clean_subject] = []

            threads[clean_subject].append(
                {
                    "from": email.get("from", "unknown"),
                    "subject": subject,
                    "date": datetime.fromtimestamp(email.get("epoch", 0), tz=timezone.utc).strftime(
                        "%Y-%m-%d %H:%M"
                    ),
                    "body": (email.get("body", "") or "")[:800],
                    "mid": email.get("mid", ""),
                }
            )

        thread_list = []
        for subject, msgs in sorted(threads.items(), key=lambda x: len(x[1]), reverse=True):
            thread_list.append({"subject": subject, "messages": msgs, "count": len(msgs)})

        if thread_list:
            results[full_name] = thread_list

    return results


# ---------------------------------------------------------------------------
# Structured output
# ---------------------------------------------------------------------------


def print_structured_output(
    user_issues: dict[str, list[dict[str, Any]]],
    tagged_activity: dict[str, list[dict[str, Any]]],
    mailing_list_activity: dict[str, list[dict[str, Any]]],
    since: datetime,
) -> None:
    """Print structured text that Claude can read and summarize."""
    since_str = since.strftime("%Y-%m-%d")

    # Section 1: User's active issues/PRs
    print(f"\n=== SECTION: YOUR ACTIVE ISSUES & PRS (since {since_str}) ===\n")
    if not user_issues:
        print("No updated issues or PRs found where you have commented.\n")
    else:
        for repo, items in user_issues.items():
            print(f"## {repo}\n")
            for item in items:
                kind = "PR" if item["is_pr"] else "Issue"
                print(f"### {kind} #{item['number']}: {item['title']}")
                print(f"URL: {item['url']}")
                if item["labels"]:
                    print(f"Labels: {', '.join(item['labels'])}")
                print(f"Last updated: {item['updated_at']}")

                if item["comments"]:
                    print(f"Recent comments (since {since_str}):")
                    for comment in item["comments"]:
                        body_preview = comment["body"].replace("\n", " ").strip()
                        if len(body_preview) > 200:
                            body_preview = body_preview[:200] + "..."
                        print(f'  - @{comment["user"]} ({comment["created_at"]}): "{body_preview}"')
                else:
                    print("  No new comments.")
                print()

    # Section 2: Tagged activity
    print(f"\n=== SECTION: NEW ACTIVITY FOR MONITORED TAGS (since {since_str}) ===\n")
    if not tagged_activity:
        print("No new activity found for monitored labels.\n")
    else:
        for label, items in tagged_activity.items():
            print(f"## Label: {label}\n")
            new_items = [i for i in items if i.get("is_new")]
            updated_items = [i for i in items if not i.get("is_new")]

            if new_items:
                print(f"### NEW ({len(new_items)} items)")
                for item in new_items:
                    kind = "PR" if item["is_pr"] else "Issue"
                    print(f"  - {kind} #{item['number']}: {item['title']}")
                    print(
                        f"    URL: {item['url']} | Repo: {item['repo']} | Comments: {item['comments_count']}"
                    )
                print()

            if updated_items:
                print(f"### UPDATED ({len(updated_items)} items)")
                for item in updated_items:
                    kind = "PR" if item["is_pr"] else "Issue"
                    print(f"  - {kind} #{item['number']}: {item['title']}")
                    print(
                        f"    URL: {item['url']} | Repo: {item['repo']} | Comments: {item['comments_count']}"
                    )
                print()

    # Section 3: Mailing list activity
    print(f"\n=== SECTION: MAILING LIST ACTIVITY (since {since_str}) ===\n")
    if not mailing_list_activity:
        print("No recent mailing list activity found.\n")
    else:
        for list_name, threads in mailing_list_activity.items():
            print(f"## {list_name}\n")
            for thread in threads:
                print(f"### Thread: {thread['subject']} ({thread['count']} message(s))")
                for msg in thread["messages"][:5]:  # limit to 5 messages per thread
                    body_preview = msg["body"].replace("\n", " ").strip()
                    if len(body_preview) > 300:
                        body_preview = body_preview[:300] + "..."
                    print(f"  - From: {msg['from']} ({msg['date']})")
                    print(f"    {body_preview}")
                if thread["count"] > 5:
                    print(f"  ... and {thread['count'] - 5} more message(s)")
                print()

    print("\n=== END OF DATA ===")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.command(context_settings={"help_option_names": ["-h", "--help"], "max_content_width": 120})
@click.option(
    "--config",
    type=click.Path(),
    default=DEFAULT_CONFIG_PATH,
    show_default=True,
    help="Path to the YAML configuration file.",
)
@click.option(
    "--github-token",
    type=str,
    envvar="GITHUB_TOKEN",
    help="GitHub personal access token. Can also set GITHUB_TOKEN env var.",
)
@click.option(
    "--since",
    "since_override",
    type=str,
    default=None,
    help="Override the 'since' date (ISO format, e.g. 2026-03-17). Defaults to last run or 3 days ago.",
)
@click.option(
    "--section",
    type=click.Choice(["all", "github", "tags", "mailing-list"], case_sensitive=False),
    default="all",
    show_default=True,
    help="Which sections to fetch.",
)
def main(config: str, github_token: str | None, since_override: str | None, section: str) -> None:
    """Fetch recent activity from GitHub and Apache mailing lists.

    Outputs structured text for Claude Code to summarize via the /summarize-activity skill.
    """
    cfg = load_config(config)
    state = load_state()
    since = resolve_since(since_override, state)

    console.print(f"[bold]Fetching activity since {since.strftime('%Y-%m-%d %H:%M UTC')}[/]\n")

    user_issues: dict[str, list[dict[str, Any]]] = {}
    tagged_activity: dict[str, list[dict[str, Any]]] = {}
    mailing_list_data: dict[str, list[dict[str, Any]]] = {}

    # GitHub sections require a token
    if section in ("all", "github", "tags"):
        if not github_token:
            console.print(
                "[red]GitHub token required for GitHub sections. Set GITHUB_TOKEN or use --github-token.[/]"
            )
            sys.exit(1)
        gh = Github(auth=Auth.Token(github_token))

        if section in ("all", "github"):
            console.print("[bold blue]Fetching your commented issues & PRs...[/]")
            user_issues = fetch_user_commented_issues(gh, cfg["repos"], cfg["username"], since)

        if section in ("all", "tags") and cfg["labels"]:
            console.print("[bold blue]Fetching tagged activity...[/]")
            tagged_activity = fetch_tagged_activity(gh, cfg["repos"], cfg["labels"], since)

    if section in ("all", "mailing-list") and cfg["mailing_lists"]:
        console.print("[bold blue]Fetching mailing list activity...[/]")
        mailing_list_data = fetch_mailing_list_activity(
            cfg["mailing_lists"], since, cfg["mailing_list_prefixes"]
        )

    # Print structured output for Claude to consume
    print_structured_output(user_issues, tagged_activity, mailing_list_data, since)

    # Update state
    state["last_run"] = datetime.now(tz=timezone.utc).isoformat()
    save_state(state)
    console.print("\n[green]State saved. Next run will fetch activity since now.[/]")


if __name__ == "__main__":
    main()
