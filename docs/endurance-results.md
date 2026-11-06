# Endurance test results

The dedicated application endurance profile produces a readable report, full manifest and metrics JSON, and a checksummed archive containing logs and samples after every completed run, including failures.

[Run and publish an endurance test](../design/qualification.md#publish-endurance-results).

| Run | Outcome | Scope |
| --- | --- | --- |
| [2026-11-06 · Linux ARM64 rehearsal](endurance/2026-11-06-linux-arm64-rehearsal/endurance-results.md) | Passed; 12,400 records reconciled, matching hashes, no fidelity errors | One minute publishing plus five minutes draining; 24 source / 32 destination partitions; development binary identified in the report |

A run shorter than 24 hours is a rehearsal and does not establish 24 hours of continuous uptime. Reports retain the tested binary identity, environment, counts, delivery times, backlog, memory and shutdown observations. The linked JSON contains the complete manifest and measurements.
