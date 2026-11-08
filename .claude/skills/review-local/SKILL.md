---
name: review-local
description: Run local reviews under docs/development/reviews/README.md before creating, updating or merging a PR.
argument-hint: "[base-ref, default origin/main]"
allowed-tools: Read, Grep, Glob, Agent, Bash(git diff *), Bash(git status *), Bash(git rev-parse *), Bash(git merge-base *), Bash(gh run list *), Bash(gh run view *), Bash(gh run download *), Bash(gh pr view *), Bash(gh pr checks *)
---

Read and follow `docs/development/reviews/README.md` (Run a review). Base ref: $ARGUMENTS (default `origin/main`). Use the required named project agents through the Agent tool, with foreground calls when available. Wait for their final reports.
