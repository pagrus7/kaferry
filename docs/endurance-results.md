# Endurance test results

Read each report on GitHub for the tested version, workload, measurements and limits. Reports link the complete manifest and metrics JSON; downloadable logs and samples are retained in checksummed release assets where linked.

[Run and publish an endurance test](../design/qualification.md#publish-endurance-results).

| Run | Outcome | Scope |
| --- | --- | --- |
| [2026-11-06 · v0.12.0 · Linux ARM64 · 24 hours](endurance/2026-11-06-v0.12.0-linux-arm64-24h/endurance-results.md) | Passed; 17,856,000 records reconciled, matching hashes, no fidelity errors; all 1,440 destination pauses recovered | 24 hours publishing plus five minutes draining; 200 records/second plus bursts; 24 source / 32 destination partitions; exact release executable |
| [2026-11-06 · Linux ARM64 rehearsal](endurance/2026-11-06-linux-arm64-rehearsal/endurance-results.md) | Passed; 12,400 records reconciled, matching hashes, no fidelity errors | One minute publishing plus five minutes draining; 24 source / 32 destination partitions; development binary identified in the report |

A run shorter than 24 hours is a rehearsal and does not establish 24 hours of continuous uptime. Reports retain the tested binary identity, environment, counts, delivery times, backlog, memory and shutdown observations. The linked JSON contains the complete manifest and measurements.
