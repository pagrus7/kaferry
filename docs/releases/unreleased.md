# Kaferry unreleased

## Changes

- `kaferry help` introduces the copy workflow with runnable examples and exit codes. Use `help sql` for the grammar and defaults and `help status` for saved-status verdicts.

## Upgrade

- Source installation requires Go 1.27 or newer.
- Top-level `--help` and `-h` exit 0. Explicit help prints to stdout, and command help groups short and long flag names.
