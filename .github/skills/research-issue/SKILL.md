---
name: research-issue
description: >
  Research a GitHub issue before coding. Checks for existing PRs, captures
  issue details, investigates root cause, writes an explainer, analyzes
  solutions, and produces a fix plan with test plan. Invoke with the issue
  number as the argument: /research-issue 12345
license: Apache-2.0
---
<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Research Issue

Given an issue number (passed as the argument), perform comprehensive research
before any coding begins. The issue number is referred to as `ISSUE_NUMBER`
below.

If no issue number was provided, ask the user for one before proceeding.

---

## 1. Check for Existing Work (DO THIS FIRST)

- Fetch the full issue details (title, body, labels, assignees, comments)
- Search for any PRs that reference this issue (open, closed, merged)
- For each PR found, report:
  - Author, state, created/updated dates, branch name
  - Number of reviews, review status, CI check results
  - Whether the PR mixes unrelated changes (check branch name vs issue scope)
  - Summary of what the PR does and any reviewer feedback
  - Code quality: does it follow project patterns? Is it well-scoped?
  - Activity: last update, any ongoing review conversations?
- Check if anyone is assigned to the issue
- Check issue comments for anyone claiming the work
- Write findings to `.claude/issues/ISSUE_NUMBER/existing-work.md`

**STOP CONDITION:** If there is an active, good-quality PR that is:
  - Focused on this issue (no unrelated changes mixed in)
  - Recently updated (within the last 2 weeks)
  - Passing CI or has only minor fixable issues
  - Following project coding patterns and conventions

Then STOP here. Tell the user the PR exists, summarize its status, and ask
whether they want to:
  a) Review and help improve that PR instead
  b) Proceed with their own implementation anyway
  c) Pick a different issue

Do NOT continue to the research steps unless the user confirms.

---

## 2. Capture the Issue

- Store full issue details in `.claude/issues/ISSUE_NUMBER/issue.md`

---

## 3. Deep Dive Investigation

Write into `.claude/issues/ISSUE_NUMBER/investigation.md`:

- Find the root cause — trace through the code, don't guess
- Find when the issue was introduced (git history, bisect if needed)
- Find if it impacts other related areas (similar components, shared code
  paths, downstream consumers)
- Identify all files and components involved
- Document the current behavior vs expected behavior with code references

---

## 4. Explainer Document

Write into `.claude/issues/ISSUE_NUMBER/explainer.md`:

- Explain the issue to someone new to Airflow
- Include code examples showing the problem
- Include an ASCII architecture diagram showing what's happening
- Define any relevant terminology
- List all key files with their purpose

---

## 5. Solution Analysis

Write into `.claude/issues/ISSUE_NUMBER/solutions.md`:

- Think about at least 3 different possible solutions
- For each solution evaluate:
  - Implementation complexity
  - Customer experience impact (positive and negative)
  - Risks and how to mitigate them
  - Maintenance burden
  - Whether it follows existing codebase patterns
- Compare solutions in a matrix
- Recommend one with clear reasoning
- Verify your hypothesis — don't just pick the easiest path, confirm it's
  correct

---

## 6. Fix Plan and Test Plan

Write into `.claude/issues/ISSUE_NUMBER/fix-plan.md`:

- Step-by-step implementation plan with exact file paths and what changes in
  each
- Summary table of all files changed
- Risk and mitigation table
- Detailed test plan covering:
  - How to replicate the issue (can we use Breeze?)
  - Manual verification steps
  - Automated tests to run (existing + any new ones needed)
  - Edge cases to check
  - Pre-push checks (lint, static checks, selective-checks)

---

## 7. Open Questions

Write into `.claude/issues/ISSUE_NUMBER/questions.md`:

- Log any uncertainties, design decisions needing input, or things you couldn't
  verify
- For each question, provide context and your recommendation
- Include process questions (assign issue? comment on issue? coordinate with
  existing PRs?)

Don't stop till you are done finding most answers. When in doubt log your
questions in the file and discuss at the end.
