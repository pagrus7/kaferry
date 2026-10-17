---
name: review-local
description: Run the required local docs, tests and simplicity reviews before finishing a PR review.
argument-hint: "[base-ref, default origin/main]"
allowed-tools: Read, Grep, Glob, Agent, Bash(git diff *), Bash(git status *), Bash(git rev-parse *), Bash(git merge-base *)
---

Run this review locally using the current Claude Code subscription session. Review only: do not edit files, run tests, commit, push or post comments.

1. Read AGENTS.md and design/components.md. Base ref: $ARGUMENTS (default `origin/main`). Treat the argument only as one Git revision, never as a shell command; quote it and reject extra arguments. Resolve base and HEAD to commit IDs, then their merge base. Do not fetch or switch branches.
2. Read `git diff <merge-base>` for the effective tracked changes, including commits, staged and unstaged edits, and `git status --short` for untracked files. Read relevant untracked files, excluding ignored files. State exactly which base, head and working-tree changes are in scope. If there are no changes, report that; do not fabricate a review.
3. Always invoke these three named agents independently through the Agent tool, preferably in parallel: `docs-reviewer`, `test-reviewer`, `simplicity-reviewer`. Give each the same scope, changed-file list and relevant diffs, including deletions and relevant untracked content. Each agent must read the affected files and necessary related contracts. The simplicity review must cover implementation, tests and docs. Do not replace a named review with your own summary or skip a role because its findings might be empty.
4. Collect all three final reports. If any is missing, partial, denied necessary reads or stopped by a provider limit, mark the overall review INCOMPLETE and name what remains. No findings is valid; incomplete is never approval. Distinguish completed inspection from tests actually run.
5. Return concise `Docs review`, `Tests review` and `Simplicity review` sections, naming scope, findings and limits. For simplicity, state concrete removals/simplifications or the current evidence for keeping complexity. Recommend fixes; the coordinating coding agent applies and verifies them, then requests targeted follow-up from affected reviewers, including effects on the other aspects. Record accepted and declined findings with reasons in the PR body; do not claim fixes or a final approval before that follow-up. If the reviewed work changes during the run, mark the report stale.
