# Endurance rehearsal results

**PASSED** · started 2026-11-06T04:21:07.728816+00:00 · requested publishing 60 seconds plus 300 seconds draining.

Measured run time: 375.4 seconds, including fixture setup and cleanup.

Fixture source commit: `ec3b377085f7678d2f2111c0f03777eddffc0ed8` · tracked working tree dirty: `False`.

Tested executable: `kaferry dev (commit ec3b377085f7678d2f2111c0f03777eddffc0ed8; go1.27.2; franz-go v1.22.2)`

Binary SHA-256: `8b306ca6848aef776a085422c450b5be9139e722eae0f06c85334cea6a889139`

Host: `Linux-7.2.12-orbstack-00394-gbecdd6130068-aarch64-with-glibc2.36` · Go: `go version go1.27.2 linux/arm64`.

[Full manifest and measurements](endurance-results.json).

This single-process workload uses 24 source and 32 destination partitions, key routing, 200 records/second plus bursts, high entropy payloads and separate single-broker clusters with replication factor 1. It measures application endurance under this workload; it does not establish saturation capacity or replicated resilience.

This is a rehearsal; it does not establish 24 hours of continuous uptime.

| Additional measurement | Value |
| --- | ---: |
| CLI CPU seconds | 7.435167 |
| Destination stalls / recovered | 1 / 1 |
| Maximum stall recovery seconds | 0.018413355 |

Delivery times include producer queueing, copying and verifier delay. Memory peaks are sampled.

## Build results

| Command | Result | Seconds |
| --- | --- | ---: |

### Slowest tests

| Test | Package | Seconds |
| --- | --- | ---: |
| TestStreamingSoakFidelityCommitLagMemoryAndShutdown | github.com/pagrus7/kaferry/tests/compat | 361.11 |

Parent tests include subtest time; these durations must not be added together.


| Test | Result | Records | Records/s | Sampled peak RSS MiB | RSS samples | RSS growth MiB | Peak uncommitted records / bytes | Sampled max lag | Shutdown s |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TestStreamingSoakFidelityCommitLagMemoryAndShutdown | PASS | 12400 | 34 | 31.8 | 358 | -13.6 | 607 / 2170402 | 607 | 0.031 |

### Application endurance load

| Test | Profile | Base records/s | Scheduled records including bursts | Submitted / acked at publishing end | Acked after grace | Minimum window ratio | Load valid | Maximum scheduling debt |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: |
| TestStreamingSoakFidelityCommitLagMemoryAndShutdown | application-24x32-v1 | 200 | 12400 | 12399 / 12398 | 12400 | 1.000 | True | 402 |

Load validity requires achieved source input; eventual fidelity cannot qualify an underloaded run. The general records/s column includes drain and reconciliation time.


### Endurance delivery

| Test | Submitted | Published (acked) | Received | Unique | Missing | Duplicates | Corrupted / unexpected / reordered | Complete | Destination lag sampled max / final (records) | Consumer lag sampled max / final (offsets) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| TestStreamingSoakFidelityCommitLagMemoryAndShutdown | 12400 | 12400 | 12400 | 12400 | 0 | 0 | 0 / 0 / 0 | True | 405 / 0 | 4 / 0 |

Missing is unresolved until all source publications are acknowledged and final output is reconciled. Destination lag is acknowledged source records minus unique received IDs; consumer lag is the destination end-offset distance to the verifier. Archive capture verifies output after shutdown and has no live destination-lag measurement.


### Per-record delivery observations

| Test | Backlog now | Observed peak backlog (records) | Timed first receipts | p95 (ms) | p99 (ms) | p99.9 (ms) | Maximum delivery time (ms) | Unsettled IDs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TestStreamingSoakFidelityCommitLagMemoryAndShutdown | 0 | 406 | 12400 | 34.963 | 528.220 | 1035.469 | 1038.774 | 0 |

Backlog changes on each source acknowledgement and first unique destination receipt, matched by record ID. Its maximum is event-based at the test observer. Delivery time runs from test submission to first receipt using one host clock; it includes producer queueing, copying and verifier delay. Percentiles count every timed receipt, use nearest ranks and round up by less than 0.1%; the maximum is exact. Small populations can put p99.9 at the maximum. Broker-offset lag remains sampled.


### Key, payload and header integrity

| Test | SHA-256 match | Hashed submitted / unique received | Pending gaps | Submitted SHA-256 | Received SHA-256 |
| --- | --- | ---: | ---: | --- | --- |
| TestStreamingSoakFidelityCommitLagMemoryAndShutdown | True | 12400 / 12400 | 0 | `ae1789d47ae6edf83e8f6e7c944ecfc522098012b92f5b4f309e5a8c0a8313ef` | `ae1789d47ae6edf83e8f6e7c944ecfc522098012b92f5b4f309e5a8c0a8313ef` |

SHA-256 fingerprints cover actual submitted and first unique received key/value bytes and ordered headers, preserving duplicate headers and null versus empty bytes. Length-prefixed records are hashed in logical-ID order within each source partition. Match is unresolved until final reconciliation. Byte comparisons also check timestamps and archive provenance; duplicate, missing and ordering checks remain separate.


Throughput is informational on shared runners. Bounded load timing starts at SQL input release and ends at process exit. RSS includes one sample before SQL input; short copies may finish before another sample. JSON metrics and available environment, coverage and logs are retained in the report directory; CI uploads that directory as an artifact.
