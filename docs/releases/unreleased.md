# Kaferry unreleased

## Changes

- `kaferry help` introduces the copy workflow with runnable examples and exit codes. Use `help sql` for the grammar and defaults and `help status` for saved-status verdicts.
- Use `kaferry help run` or `kaferry help explain` for command flags and an example.

## Upgrade

- Source installation requires Go 1.27 or newer.
- Top-level `--help` and `-h` exit 0. Explicit help prints to stdout, and command help groups short and long flag names.
- `kaferry status --json` uses version `2`. `meaning` is one sentence for the current verdict. Applicable caveats and suggested actions appear in the optional `notes` and `next` fields. Text output shows the current verdict's meaning. Use `kaferry help status` for the full legend.
