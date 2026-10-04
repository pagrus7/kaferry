# Implementation results — 2026-10-03

Initial MVP measurements below describe `7ab6413`; subsequent review checks are recorded at the end.

## Delivered MVP

- All four copy directions; agreed SQL grammar, TOML/native properties, SQL precedence and `ENV`.
- Shared runner behind source/sink/progress interfaces; SRP components, constructor injection, Fx modules only at composition.
- Raw bytes/nulls/ordered headers; destination routing; read-committed input; saved positions/windows/identities.
- Independent worker completion; sampled-tip transaction handling; generation fencing; bounded work and ordered retry after queue saturation.
- Framed CBOR, durable output/checkpoints, local locks, snapshot input, guarded recovery and crash replay.
- Signals stop intake, drain writes for ten seconds, save confirmed progress; exit codes and concise JSON diagnostics.

## Verified targets

macOS ARM64, Go 1.27.1; Linux ARM64, Go 1.26 in Docker. Kafka 3.9.0, two independent single-broker KRaft clusters. Pinned Go client/librdkafka 2.15.0. Details: [client compatibility](client-compatibility.md).

## Checks

| Layer | Evidence |
| --- | --- |
| Unit/mock | Parser/planner boundaries; deterministic runner, clock, delivery and progress failures; cap admission, partial commits, late outcomes and ten-second drain |
| Component | Real files, CRC/size bounds, locks across checkpoint replacement, failed writes/checkpoints, changed identities and tail recovery |
| Integration/e2e | Four directions, separate clusters, opaque Schema Registry bytes, key routing, cooperative workers, transactions/control tails, timestamp boundaries, retention gaps, new partitions, physical CLI signal/resume |
| Crash/fault | Subprocess exit after output fsync and before checkpoint: duplicate only; broker pause during commit; native ownership loss after max-poll deadline |
| Fuzz | SQL and window normalization; final record decoder: 1,716,899 executions in approximately six seconds, no failure |
| Static/race | `go vet`; race suites on both verified OS targets; formatting and diff checks; CI YAML parsed |
| Dependencies | Go vulnerability scan: no vulnerabilities found; this does not scan bundled native libraries |

Complete unit/integration race suites passed on both targets. The combined coverage run measured 81.5% of internal statements; subsequent diagnostic changes passed targeted tests. GitHub CI is defined, including Linux AMD64 and scheduled fault/load jobs; remote CI has not run.

## Local load baseline

Four source/five destination partitions; 1–5 KiB values, nullable keys and duplicate/null/empty headers. Both runs verified every output record with no loss or duplicates. Elapsed includes worker startup and completion; fixture publication is excluded. RSS sampled every 50 ms.

| Setting | Records | Elapsed | Records/s | Child CPU | Peak RSS | Peak uncommitted records / logical bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Default caps | 16,000 | 2.320 s | 6,896 | 256 ms | 112.3 MiB | 9,278 / 29,125,765 |
| 32 worker / 8 per-partition records; 100 ms producer linger | 128 | 5.366 s | 24 | 89 ms | 33.3 MiB | 32 / 43,082 |

Small-cap fixture used 128 KiB worker/32 KiB partition byte caps and exercised repeated pause/resume. A 1,000-record version progressed but exceeded the 60-second test budget; reducing the fixture retained those cycles. Small caps have a substantial throughput cost with native fetch/queue defaults. These are local observations, not throughput guarantees.

## Size and complexity

- Production Go: 3,748 lines; tests: 3,210 lines.
- Highest cyclomatic complexity: composition `Run` 38, option validation 32, Kafka positioning 27; core runner `Run` 25. Measurements are signals, not thresholds.
- Stripped macOS ARM64 binary: 14,975,826 bytes. A rebuild with an empty build cache was identical (SHA256 `cb9c7573664eaa53e6e3f31d057fd5286e2694705ba0d7b4edf1ecf063920f0e`).
- User docs (`README.md` plus file format): 9,922 characters, below 10,000.
- Codec roundtrip: 335 MB/s at 1 KiB, 1,521 MB/s at 64 KiB, 1,991 MB/s at 1 MiB. Includes encoding, framing, CRC and decoding, not filesystem durability.

## Initial qualification gaps

At the initial MVP measurement, restricted ACL/SASL/TLS, additional broker/platform versions, multiple-broker failover and sustained soak were unqualified. Later checks below close part of that scope. Native timestamp/header/subscription limits and blocking shutdown calls remain documented in client compatibility. Archive version 1 stays unfrozen until the first final release.

## Reproduce

`make check build`; `make broker integration`; `make fault load benchmark metrics`; `make broker-down`.

Build flags: `-trimpath -ldflags='-s -w -buildid='`. CGO and a C compiler are required. Test brokers use project `kafka-fwd2-compat` on loopback ports 39092/40092. Other Docker projects are untouched.

## Implementation review verification

- Reviewed permanent output I/O failure handling, unbounded transient consumer recovery, revocation-only commit recovery, batched positioning and execution-stage exit codes against the agreed rules.
- Corrected two remaining edges: empty assignments make no offset requests, and poisoned output returns its original I/O error before validating another record. Added failing-then-passing regressions and Kafka durable-extent metadata checks.
- `make check build` passed. Full broker integration/race suites passed on macOS ARM64 and Linux ARM64; Linux tested the final corrections.
- Current production Go: 3,912 lines. Highest cyclomatic complexity: batched positioning 45; core runner 25. No complexity threshold is imposed.
- User docs: 9,995 characters. Full-file restart CRC validation and stable checkpoint lock files remain in place.

## Simplicity and component decomposition

Comparison with `5c82947`:

- Fx injects Kafka, file and runner factory interfaces. Factories create adapters; the engine orchestrates the query and resource lifetime. Separate engine, runner, checkpoint-writer, clock and logging modules.
- Kafka positioning separates window adoption, saved-offset validation and batched timestamp/end lookups from polling. Commit persistence has its own file.
- Runner iteration separates progress, periodic commits, event intake and interruption. Option validation separates group, cutoff and error policies. Archive initialization separates identity checks and recovery; checkpoint loading separates naming, locking and validation.

| Method / area | Before | After |
| --- | ---: | ---: |
| Maximum production cyclomatic complexity | 45 | 19 |
| Kafka positioning, largest function | 45 | 13 |
| Engine `Run` | 38 | 15 |
| Plan `options` | 32 | 10 |
| Runner `run` | 25 | 7 |
| Record `DecodeRecord` | 26 | 8 |
| `NewCheckpoints` | 26 | 5 |
| `NewOutput` | 25 | 3 |
| CLI `Execute` | 24 | 10 |

Production Go grew from 3,912 to 4,330 lines (418, 10.7%); test Go from 3,631 to 3,939. Additional interfaces and explicit setup/cleanup steps account for the production growth. The largest new positioning helper is 13, decoder helper 11, and output-recovery helper 9. Dependencies are unchanged. User docs remain 9,995 characters. Stripped macOS ARM64 binary: 15,025,506 bytes.

Checks passed on macOS ARM64: `make check build`, full Kafka integration/race suite (compatibility package 59.528 s), formatting and diff checks. New tests cover Fx factory replacement for all directions, startup-failure cleanup, one-off group consistency and cleanup, destination identity consistency, checkpoint-lock release after failed source open, and file-alias rejection before checkpoint creation. Decoder fuzzing completed 1,468,279 cases in approximately six seconds without failure. Linux was not rerun for this refactor.

Both bounded-load cases passed with full record fidelity and no loss or duplicates:

| Setting | Records | Elapsed | Records/s | Child CPU | Peak RSS | Peak uncommitted records / logical bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Default caps | 16,000 | 3.312 s | 4,830 | 322 ms | 111.3 MiB | 9,709 / 29,924,274 |
| Small caps / slow destination | 128 | 5.040 s | 25 | 95 ms | 32.8 MiB | 32 / 43,082 |

A sequential comparison against a separately built `5c82947` under the same broker conditions measured 4,805 records/s (3.330 s, 337 ms CPU). Repeating the refactored default case measured 6,406 records/s (2.498 s, 264 ms CPU). These short runs include startup and vary locally; they provide no evidence of a throughput regression and are not a sustained-throughput qualification.

## Interface boundaries and independent tests

- Configuration and planning expose Reader/Builder interfaces. Command orchestrates parse/load/plan/execute through injected interfaces and preserves exit classification.
- Runner depends on ProgressTracker, with a fresh tracker from ProgressFactory for every query.
- Kafka source/sink/metadata depend on native client ports. Positioning has an injected factory and explicit assignment/progress/retention results. Source prepares and saves commit metadata through AssignmentStore, without accessing the committer's private map.
- File source/output depend on decoder/encoder, checkpoint and I/O interfaces. Checkpoints own initialization and frame tracking.
- Handwritten mocks independently exercise planning failures, tracker rejection before publishing, Kafka initial-commit failures and missing results, ownership loss, routing/delivery attribution, invalid metadata, decode failure, and output write/flush/fsync failures. Real-file, Fx, crash and real-Kafka tests verify composition; the [testing map](implementation-plan.md#testing-pyramid) records the layers.

Final checks on macOS ARM64: `make check build`, `make integration` (compatibility package 56.936 s), `make load`, formatting and diff checks passed. Linux and hosted CI were not rerun for these changes.

Production Go: 4,556 lines (226 more than the preceding refactor); tests: 4,709 lines (770 more). Maximum production cyclomatic complexity remains 19. No new dependencies. User docs remain 9,995 characters; stripped binary is 15,058,626 bytes.

Both bounded-load cases verified full record fidelity, no loss or duplicates, and work within configured caps:

| Setting | Records | Elapsed | Records/s | Child CPU | Peak RSS | Peak uncommitted records / logical bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Default caps | 16,000 | 3.252 s | 4,919 | 301 ms | 118.2 MiB | 9,709 / 29,924,274 |
| Small caps / slow destination | 128 | 4.919 s | 26 | 99 ms | 32.9 MiB | 32 / 43,082 |


## Completeness and candidate verification

Candidate: `v0.1.0-rc.1`, based on `5c82947` plus the component refactor and acceptance work. The [completeness matrix](completeness.md) maps agreed rules to 100 named tests/fuzz targets; every cited name was checked against the source. No accepted copy semantics or runtime dependencies changed.

### Closed acceptance gaps

- Durable file publication has its own injected Publisher/Fx module. Its filesystem port and the checkpoint replacement helper permit real subprocess crashes at 13 initialization boundaries, using the production algorithms. Short writes fail explicitly.
- File tests cover simultaneous creators, changed last-frame CRC, shortened input, missing owned output, invalid extent boundaries, zero-filled tails and actual rename failure without losing prior checkpoints.
- Broker tests cover source recreation with still-valid saved offsets, excess workers exiting independently, concurrent/persisted/fresh-after-cleared-progress NOW windows, retention overtaking a suspended consumer, producer clock/counts for absent timestamps, and native worker death after broker delivery but before runner confirmation.
- The crash test copied every source record after restart; each appeared once or twice, with replay within the configured 16-record uncommitted cap. Partition-count changes independently warn while preserving the key and leaving routing to Kafka.
- Disposable three-broker TLS/SASL/ACL fixtures verify restricted successful copying, denied optional config preflight, deterministic authorization failure, bounded wrong-password/trust failure under SKIP, and a broker/controller leader stop with replication factor 3/minimum ISR 2. The copied 64-record sequence and final commit remained intact.

### Final gates

| Target / check | Result |
| --- | --- |
| macOS ARM64 / Go 1.27.1 | Full vet, unit/component race and Kafka integration race suites passed; compat 92.641 s |
| Linux ARM64 / Go 1.26, Docker | Full vet, unit/component race and Kafka integration race suites passed; compat 66.292 s |
| Linux AMD64 / Go 1.26, Docker emulation | Full vet, unit/component race and Kafka integration race suites passed; compat 97.160 s; release build succeeded |
| Fault probes | Broker-paused commit returned after 2.675 s; ownership-loss probe passed |
| Fuzz | Five-second targets: parser 176,590; window 568,825; framing 410,301; record decoder 371,270 executions; no failures |
| Security/failover | Fresh disposable cluster run passed; compat 39.039 s; cluster/temporary certificates removed |
| Packaging | Formatting, diff, shell syntax and YAML parsing passed; README + file format 9,948 characters |
| Reproducibility | Empty-cache macOS build byte-identical; SHA256 `7cce4e182cf0bae12439a8c2679c26f3a7ed7a992a6dee5dbc7e6888fee2118e` |
| Hosted CI | Unrun: repository has no remote; equivalent local gates are recorded above |

Cold fixture group coordinators and replication are now checked before running. The failover verifier uses short watermark queries and tolerates transport errors from the stopped broker. A stale verification admin request caused early fixture failures; the tool itself required no failover change. An overly short standby idle deadline was extended to allow observing all members under concurrent platform checks.

### Load and operating observations

Local macOS ARM64; four source/five destination partitions, 1–5 KiB payloads, nullable keys and duplicate/null/empty headers. All runs verified full record fidelity/order, no loss/duplicates, and configured uncommitted caps.

| Setting | Records | Elapsed | Records/s | Child CPU | Peak RSS | Peak uncommitted records / bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Default bounded copy | 16,000 | 3.171 s | 5,046 | 327 ms | 105.2 MiB | 9,709 / 29,924,274 |
| 32 worker / 8 partition record caps; 100 ms linger | 128 | 4.952 s | 26 | 108 ms | 32.7 MiB | 32 / 43,082 |
| Controlled two-minute stream | 74,816 | 120 s | 622 | 15.000 s | 47.2 MiB | 736 / 3,532,398 |

The stream rate is controlled, not maximum throughput. RSS warm/final medians: 46.2/47.2 MiB; maximum sampled committed-offset lag: 720 records; graceful shutdown: 257 ms, all progress saved. The test's RSS envelope is peak ≤512 MiB and final median ≤warm median +32 MiB. Two minutes do not establish long-term leak freedom.

Retain default caps (20,000 worker/5,000 partition records; 128/16 MiB logical bytes). Tiny caps impose significant native queue/fetch overhead. Logical caps exclude lookahead/native buffers; use actual RSS to provision workers. The 16-record crash case demonstrates bounded replay, not exactly-once delivery or a universal duplicate count. Longer soak: `KFWD_SOAK_DURATION=30m make soak`.

### Current size and simplicity

Production Go: 4,622 lines; tests: 6,251. Maximum production cyclomatic complexity stays 19 (45 before the refactor). The publication seam adds 66 production lines beyond the preceding interface work, allowing independently injected durability failures without duplicating the creation/replacement algorithms.

Stripped binaries: macOS ARM64 15,075,170 bytes; Linux ARM64 15,273,376; Linux AMD64 16,912,120. Codec benchmarks: 359/1,383/1,822 MB/s at 1 KiB/64 KiB/1 MiB, excluding filesystem durability. No performance guarantee or archive-format freeze is implied by the candidate tag.

### Remaining deployment work

Hosted CI needs a Git remote. Additional broker versions, SCRAM/mTLS/other authentication providers, encrypted broker/controller traffic, physical AMD64 hosts, hours-long/high-latency load and realistic retention need deployment-specific qualification. Recorded timestamp/subscription/identity/locking/shutdown limits remain accepted boundaries.

Reproduce: `make check build`; `make broker integration`; `make fault load soak qualification fuzz benchmark metrics`; `make broker-down`. Heavy suites run on scheduled/manual CI; unit and broker integration gates remain on every change. Qualification owns only `kafka-fwd2-qualification` on ports 41092/42092/43092; its public fixture credentials are never production defaults.
