---
name: review-local
description: Run the required local docs, tests and simplicity reviews before creating, updating or merging a PR.
argument-hint: "[base-ref, default origin/main]"
allowed-tools: Read, Grep, Glob, Agent, Bash(git diff *), Bash(git status *), Bash(git rev-parse *), Bash(git merge-base *), Bash(gh run list *), Bash(gh run view *), Bash(gh run download *), Bash(gh pr view *)
---

Read and follow `review/README.md` (Run a review). Base ref: $ARGUMENTS (default `origin/main`). Use the three named project agents through the Agent tool, with foreground calls when available. Do not return a final report until all three finish.
