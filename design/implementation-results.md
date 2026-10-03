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

## Qualification remaining

Restricted ACL/SASL/TLS configurations, additional broker/platform versions, multiple-broker failover, and sustained soak with realistic latency/retention remain deployment qualification work. Native timestamp/header/subscription limits and blocking shutdown calls are documented in client compatibility. The MVP has no release tag or frozen archive format.

## Reproduce

`make check build`; `make broker integration`; `make fault load benchmark metrics`; `make broker-down`.

Build flags: `-trimpath -ldflags='-s -w -buildid='`. CGO and a C compiler are required. Test brokers use project `kafka-fwd2-compat` on loopback ports 39092/40092. Other Docker projects are untouched.

## Implementation review verification

- Reviewed permanent output I/O failure handling, unbounded transient consumer recovery, revocation-only commit recovery, batched positioning and execution-stage exit codes against the agreed rules.
- Corrected two remaining edges: empty assignments make no offset requests, and poisoned output returns its original I/O error before validating another record. Added failing-then-passing regressions and Kafka durable-extent metadata checks.
- `make check build` passed. Full broker integration/race suites passed on macOS ARM64 and Linux ARM64; Linux tested the final corrections.
- Current production Go: 3,912 lines. Highest cyclomatic complexity: batched positioning 45; core runner 25. No complexity threshold is imposed.
- User docs: 9,995 characters. Full-file restart CRC validation and stable checkpoint lock files remain in place.
