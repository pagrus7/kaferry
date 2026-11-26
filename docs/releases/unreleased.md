# Kaferry unreleased

## Changes

- `kaferry help` introduces the copy workflow with runnable examples and exit codes. Use `help sql` for the grammar and defaults and `help status` for saved-status verdicts.
- Use `kaferry help run` or `kaferry help explain` for command flags and an example.

## Upgrade

- Kafka copies now stop at the readable end saved for each partition. Use `FOLLOW = ON` in the source WITH to keep copying new records. Explicit START requires FOLLOW.
- Use a new group after upgrading. Previous Kafka commits and archive replay identities cannot resume under this version. A fresh group can copy records already present in the destination.
- Remove MAX_STALENESS and the worker.tip_wait setting. Schedule bounded exports after the window closes to include later arrivals. MAX_OUT_OF_ORDER defaults to OFF. It can shorten a Kafka scan when specified with an upper timestamp bound.
- Explain JSON version 3 includes `end_rule` and removes `max_staleness_ms`.
- Source installation requires Go 1.27 or newer.
- Top-level `--help` and `-h` exit 0. Explicit help prints to stdout, and command help groups short and long flag names.
- `kaferry status --json` uses version `3`. Update scripts that check `complete_within_staleness` to use `complete_within_disorder` for scans that stopped early under MAX_OUT_OF_ORDER. Its exit code is still 0. `meaning` is one sentence for the current verdict. Applicable caveats and suggested actions appear in the optional `notes` and `next` fields. Text output shows the current verdict's meaning. Use `kaferry help status` for the full legend.
