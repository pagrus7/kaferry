---
name: review-local
description: Run the required local docs, tests and simplicity reviews before finishing a PR review.
argument-hint: "[base-ref, default origin/main]"
allowed-tools: Read, Grep, Glob, Agent, Bash(git diff *), Bash(git status *), Bash(git rev-parse *), Bash(git merge-base *)
---

Review only: do not edit files, run tests, commit, push or post comments. Run each Git command separately. Use `git diff --no-ext-diff` with revisions and paths only; never use `--output`, `--no-index` or external diff helpers. Shell assignments, chained commands and other Git operations are not pre-approved.

1. Read AGENTS.md and design/components.md. Base ref: $ARGUMENTS (default `origin/main`). Accept one Git revision, reject extra arguments or a leading `-`, and quote it. Resolve base and HEAD to commit IDs, then their merge base. Do not fetch or switch branches.
2. Read `git diff <merge-base>` for the effective tracked changes, including commits, staged and unstaged edits, and `git status --short` for untracked files. Read relevant untracked files, excluding ignored files. State exactly which base, head and working-tree changes are in scope. If there are no changes, report that; do not fabricate a review.
3. Always invoke `docs-reviewer`, `test-reviewer` and `simplicity-reviewer` independently through the Agent tool. Use foreground calls when available. Give each the same scope, changed-file list and relevant diffs, including deletions and untracked content. Pass available test/coverage evidence from the current conversation with its tested commit, or state none was supplied. Do not replace a named review with your own summary or skip a role because its findings might be empty.
4. Wait for all three final reports before returning the combined result; launching reviewers is not completion. If any report is missing, partial, denied necessary reads or stopped by a provider limit, mark the overall review INCOMPLETE and name what remains. No findings is valid. Distinguish inspection from tests actually run.
5. Return concise `Docs review`, `Tests review` and `Simplicity review` sections with scope, findings and limits. For simplicity, name concrete removals/simplifications or evidence for keeping complexity. Follow AGENTS.md for fixes, follow-up and recording results. If the reviewed work changes during the run, mark the report stale.
