# Qualification lab

Run the same scripts from a terminal, GitHub Actions or a local scheduler. No agent, chat session or hosted automation is required. [Component tests](components.md#test-targets) remain the fast development entrypoints; this lab owns disposable broker setup, teardown and qualification evidence.

## Prepare

Use Python 3.11+, the Go version required by `go.mod`, Docker with Compose, OpenSSL and Make. Linux and macOS AMD64/ARM64 are supported. Release comparisons also need an authenticated `gh` CLI with repository/attestation read access. Linux race tests need a C compiler. On Linux, install the Python venv package if the distribution packages it separately.

From the repository root:

```sh
make qualification-setup
python3 scripts/qualify.py plan --tier pr
```

If `python3` is an older system interpreter, use `make qualification-setup PYTHON=python3.14` and `python3.14` for the CLI commands. Setup builds Kaferry and installs the independent librdkafka checker into a checkout-local Python virtual environment. Run setup and qualification with the same Python major/minor version. Pass `--binary /absolute/path/to/kaferry` to test a downloaded executable; qualification does not rebuild that executable.

Reserve ports 39092/40092 for ordinary plain brokers, 39192/40192 for dedicated endurance brokers, 41092–43094 for the security listeners and 44088 for Kerberos. Run one lab per host, across all checkouts and do not run `make broker` or `make qualification` alongside it. The runner takes a host-wide lock for its fixed ports and Compose names; legacy Make entrypoints do not take that lock. Broker containers use only the `kaferry-compat`, `kaferry-endurance` and `kaferry-qualification` Compose projects. Other Docker workloads are outside the lab.

Start with one workload, 8 available CPUs, 16–20 GiB available RAM and 100 GiB free test storage; measure actual usage before increasing load. These are starting allocations, not capacity guarantees. Fixtures cap broker JVM heaps at 512 MiB and use four internal offset/transaction partitions.

## Compatibility matrix

[The version catalog](../tests/qualification/matrix.json) pins five Apache Kafka images by digest. [The workflow](../.github/workflows/compatibility.yml) runs the following plans:

| Tier | Broker copies | Authentication fixtures |
| --- | --- | --- |
| `pr` | Oldest/newest, both directions and same-version: four cases | Newest broker |
| `nightly` | Each version to itself: five cases | Newest broker |
| `full` | Every directed source/destination pair: 25 cases | All five versions |

```sh
python3 scripts/qualify.py matrix --tier pr
python3 scripts/qualify.py matrix --tier full --output build-results/full-matrix
```

Every tier also runs archive-capture endurance with two minutes of publishing and five minutes without new input. A matrix runs sequentially locally; CI runs up to six cases in parallel on separate hosted runners. Existing output directories are refused so failed evidence cannot be overwritten. Use a new output path for a follow-up run.

Scheduled main runs reuse a suite or matrix tier only when its latest completed attempt passed on the same commit and its test executions are less than 30 days old. Nightly results cannot substitute for the full matrix. Failed or cancelled attempts require retry; skipped runs and rerunning only the aggregate report do not renew evidence age. Missing or unavailable history runs the tests.

The planner's GitHub Actions summary names each suite/tier, says whether it will run or reuse evidence, and links the original execution with its date. Weekly extended tests always include fresh fuzzing; scheduled vulnerability scans and performance A/A calibration also continue on unchanged code. PR, manual and release invocations run their selected suites without scheduled-evidence reuse. These rules reuse scheduled evidence; they do not certify a newly packaged release executable.

Run an individual case:

```sh
python3 scripts/qualify.py run broker --source 3.9.2 --destination 4.4.0
python3 scripts/qualify.py run auth --source 4.4.0
```

The independent Python checker uses librdkafka; Kaferry uses franz-go. It seeds 96 records through all five compression codecs and checks both Kafka partitioning modes plus capture→clone→replay. Each route independently verifies seven records after its partial run, all 96 after resume, and unchanged output after its completed rerun. Python fastavro reads archive records independently of Kaferry. Reconciliation checks unique IDs, bytes, null versus empty, ordered duplicate headers, timestamps and preserved partition IDs. Counts come from consumed records, not Kafka offset differences or Kaferry's counters.

Security fixtures exercise PLAIN, SCRAM-SHA-256/512, signed OAuth client credentials, OAuth token files, Kerberos, server TLS, mTLS, denied access and replicated destination-leader failure. OAuth brokers validate RSA signatures, issuer, audience and expiry against an HTTP JWKS fixture; a live-copy test renews short-lived tokens while the issuer rotates keys. Token-file replacement is tested across invocations. The local KDC covers keytab login; prolonged ticket renewal and deployment-specific identity-provider behavior need separate evidence. Internal broker/controller listeners use plaintext.

These commands define test targets. A configuration is qualified only by a completed passing receipt for its exact source, destination and executable; the catalog alone is not evidence that it passed.

## Other suites

Every command attempts fixture cleanup and returns nonzero on failure or interruption:

```sh
python3 scripts/qualify.py run integration
python3 scripts/qualify.py run fault
python3 scripts/qualify.py run load
python3 scripts/qualify.py run resources
python3 scripts/qualify.py run model
python3 scripts/qualify.py run fuzz
python3 scripts/qualify.py run benchmark
python3 scripts/qualify.py run smoke
```

`resources` runs copy, capture, checkpoint and archive-adapter component tests, including injected failures at filesystem durability boundaries. `fault` includes abrupt process death and restart; its reconciliation permits replay duplicates under the at-least-once contract. `model` compares randomized ownership, delivery, gap and save sequences against a reference model. The same lifecycle target participates in `make fuzz`; `FUZZ_ITERATIONS` sets its execution budget.

For paired performance, use the existing standalone runner with two source revisions:

```sh
sh scripts/performance_run.sh origin/main HEAD --pairs 6 --archives
```

The wrapper tears down its disposable brokers after success, failure or interruption. Compare measurements on the same quiet machine; shared-runner throughput has no regression gate. [Performance interpretation](components.md#paired-performance).

If Docker itself is unavailable, cleanup can fail. The receipt preserves both suite and cleanup errors. After restoring Docker, remove only these lab projects:

```sh
docker compose -p kaferry-compat --profile cross-cluster down -v --remove-orphans
docker compose -p kaferry-endurance --profile cross-cluster down -v --remove-orphans
KAFERRY_QUAL_CERT_DIR=/tmp docker compose -p kaferry-qualification -f tests/qualification/compose.yaml down -v --remove-orphans
```

The temporary certificate path satisfies Compose interpolation during teardown; it does not create new brokers. Restarting Docker can interrupt other workloads and is outside the lab's automatic cleanup.

## Endurance on a persistent host

Keep a process alive continuously for the duration being claimed. Separate short runs do not add up to continuous uptime. The runner accepts publishing durations from one minute through seven days, adds a five-minute consume-only period and derives the Go test timeout from both periods plus cleanup time.

Prerequisites: Python 3.11+, Go, Docker and Compose, a host that stays awake, and exclusive use of the selected disposable brokers. `endurance` owns the separate `kaferry-endurance` project on localhost ports 39192 and 40192; other plain-broker suites use `kaferry-compat` on 39092 and 40092. The runner starts and removes its project and pauses its destination. Do not run another job against that project while it is active. Other host workloads can affect measurements.

```sh
make build
python3 scripts/qualify.py run endurance --output build-results/endurance-24h
# Optional short rehearsal; this does not qualify 24 hours of uptime.
python3 scripts/qualify.py run endurance --duration 1m --output build-results/endurance-rehearsal
```

The dedicated `endurance` suite uses the following fixed application workload. It stays out of PR, nightly and weekly matrices; their short `soak` and `capture` settings remain available. Use a fresh output directory for each run. The runner preserves evidence on failure and removes its disposable brokers in cleanup. Interrupt with Ctrl-C; an interrupted run cannot qualify. A fresh run starts fresh topics and progress rather than resuming an interrupted qualification.

| Setting | Purpose and acceptance |
| --- | --- |
| 24 hours publishing, then five minutes without scheduled input; one continuous CLI process | Demonstrates this process stays alive for the measured interval. `--duration` accepts one minute through seven days; durations below 24 hours are labeled rehearsals in the manifest. |
| 24 source / 32 destination partitions, `PARTITIONING=KEY` | Exercises assignment, per-partition saved progress and fan-out in one process. These are chosen benchmark counts, not a measured production average. No group scaling or rebalance claim. |
| 200 records/second plus 400 extra records at 10 seconds and every minute afterward | Moderate sustained load, with two seconds of extra input per burst. Scheduling follows elapsed time. Each minute and the final window must acknowledge at least 95% of scheduled input, including bursts; the final window is checked after the five-second grace. Submission debt may not exceed 1,400 records (five seconds plus a burst); every scheduled record must be submitted and acknowledged within five seconds after the publishing boundary. |
| 1,024–5,119 byte values, deterministic high entropy bytes, 257 cyclic keys with every seventh key null | Roughly 3 KiB average; exercises bytes and modest key reuse without compression making the workload trivial. This distribution does not model hot keys, large messages or every application schema. Source writes use no compression. |
| Separate single-broker source/destination clusters, replication factor 1 | Isolates destination stalls from source publication. Pause the entire destination for one second at 30 seconds and every minute afterward during publishing. After unpause, acknowledged input minus unique receipts must return to at most 20 records within 15 seconds; it must reach zero within 15 seconds after publishing stops. |
| Fidelity, ordering and saved progress | Require no missing, duplicate, corrupted, unexpected or reordered records and matching submission/receipt hashes. Non-null keys must reach their Kafka key-hash partition; misplaced records count as corrupted. Null keys use sticky placement. Ordering is checked within each source/destination partition pair, not globally across key routes. After shutdown each source partition must have its exact final next offset saved. This successful run requires zero duplicates; Kaferry's at-least-once contract still permits duplicates after a crash or failed progress save. |
| Shutdown and bounded work | SIGINT must exit successfully within 30 seconds after the drain. Final summary counts must match input, with at most 1,024 uncommitted records and 128 MiB. This run does not restart the process or inject source failure. |
| Memory and measurement | Sample CLI RSS once per second: peak at most 512 MiB, final median growth at most 32 MiB over the warm median. Endurance checks growth both just before publishing ends and after drain. Warm median uses samples 11–30; final medians use the last 20 samples. |

The rate checks make producer/verifier backpressure visible. The fixture uses nonblocking producer admission (rejection fails the run), a 2,048-record producer buffer and up to 512 receipts per verifier poll. Outstanding input and unresolved verification/digest history have an 8,192-ID bound; hitting a gate cannot reset the clock or reduce the target. Rate/debt failures invalidate load even when all records eventually reconcile. The last scheduled batch may be submitted during the five-second boundary grace; no additional input is scheduled then. A slow verifier can invalidate the harness; that failure alone does not establish a Kaferry throughput limit.

`manifest.json` records the workload, exact binary, images, duration and result. `capacity.json` records preflight space. The metrics file's `endurance` object records targets, submissions/acknowledgements at the publishing boundary and after grace, minimum window ratio, gate iterations, maximum scheduling debt, stall recoveries, loaded final RSS, and wall time spent submitting, verifying and sampling. Existing metrics retain CLI CPU seconds, sampled lag, RSS and final reconciliation. CPU and operation times are descriptive, with no hardware-independent CPU threshold. Sampling time includes admin queries and RSS collection; verifier time includes polling waits and byte/hash work. These times measure harness occupancy, not the overhead relative to an uninstrumented run. Rate gates reject a run that instrumentation underloads; profiling and an instrumentation A/B comparison remain separate work.

Endurance disables topic age/size retention until reconciliation. Preflight reserves three times the record count times 5,376 bytes, plus 4 GiB on **each** broker data filesystem: two full maximum-size copies with 50% overhead, with no compression credit and allowance for a shared Docker disk. The 24-hour default schedules 17,856,000 records and requires about 272 GiB free there. Evidence needs at least 1 GiB or 8 KiB per publishing second, whichever is greater. These are planning allowances, not guarantees against other disk users; monitor host/Docker capacity during the run. The runner does not prune active records. Record-ID overflow is rejected before intake.

This is application endurance under the stated local topology and load. It is neither a saturation benchmark nor replicated-cluster resilience evidence. Use `load` for bounded throughput comparisons and `auth` for replicated leader-failure scenarios; neither substitutes for long continuous uptime. A passing rehearsal, an underloaded run or several short runs cannot support a multi-day production claim. Claims exclude multiple Kaferry processes, replication/ISR behavior, network partitions, authentication overhead and arbitrary payload/key distributions. Match a real deployment with separately measured workloads before generalizing these results.

Archive capture and the smaller Kafka soak remain available:

```sh
python3 scripts/qualify.py run soak --duration 24h --rate 50
python3 scripts/qualify.py run capture --duration 72h --rate 50
```

`soak` and `endurance` verify topic output as it arrives and pauses the separate disposable destination once per minute during publishing. All three modes keep the same copy process alive for five more minutes after publishing stops. After shutdown, Kafka modes consume through the destination's final offsets; `capture` scans all completed archive files. Replay/file-to-file load tests are finite jobs; their repeated execution is not reported as continuous streaming uptime.

Metrics and JSONL samples record submitted input, broker-acknowledged publications, total and unique received records, verified records, pending IDs, duplicates, corruption, unexpected IDs and reordering. Final missing counts are available only after all publications are acknowledged and the final output scan completes; incomplete evidence reports `missing: null`. Failure reports preserve observed counters. Verification stops with partial totals if unresolved ID history exceeds 8,192 entries. Rates include the consume-only period and reconciliation.

Both Kafka and archive verifiers compare complete key/value bytes, ordered headers (including duplicate names and null versus empty values) and timestamps against the fixture. Archive verification also compares source cluster, topic ID, partition and offset. `delivery.integrity` adds SHA-256 evidence from actual submissions and first unique receipts, retaining the byte comparisons. Its `sha256-canonical-records-v1` encoding length-prefixes key/value bytes and ordered header names/values, distinguishes null from empty bytes and includes the header count. Each source partition hashes record fingerprints in logical-ID order; a final SHA-256 combines all source-partition digests in partition order. This permits different arrival orders between destination partitions. Only unresolved receipt digests are buffered, with the same 8,192-ID limit. Metrics and GitHub summaries retain both hashes, hashed counts and pending gaps; `matched` is null until final reconciliation. The digest covers key/value/headers; timestamps, provenance, missing records, duplicates and reordering have separate checks. Archive hashes are completed by the scan after shutdown; their live JSONL snapshots remain unresolved.

For Kafka output, destination lag is acknowledged source records minus unique IDs received by the verifier, bounded below by zero. Destination consumer lag is the sum of destination end offsets minus the verifier's next consumed offsets. JSONL samples retain instantaneous lags; metrics retain sampled peaks and, after reconciliation, final values. Consumer-offset sampling pauses while the destination fixture is paused; record-lag sampling continues. Offset distance is a backlog measure, not an independently verified record count. Archive output has no live destination-lag measurement.

Kafka endurance also records `delivery.observed`: source acknowledgements and first unique destination receipts update a synchronized tracker immediately, matching record IDs even when receipt precedes acknowledgement. `peak_backlog` is the maximum number of acknowledged IDs not yet received at those observation points; it is updated per event rather than sampled. `max_latency_seconds` measures every tracked first receipt from test submission, using the same host's monotonic clock. This includes producer queueing, copying and verifier delay; it does not isolate broker arrival or Kaferry processing time. `timed_receipts` states the measurement count; no receipts gives a null maximum. Completed timing state is released, leaving only unsettled IDs. These observations appear in metrics, JSONL snapshots and the GitHub summary. Absent observation fields mean unmeasured. Broker-offset peaks remain sampled, and archive verification after shutdown does not provide these live observations.

`p95_latency_seconds`, `p99_latency_seconds` and `p999_latency_seconds` report the nearest-rank 95th, 99th and 99.9th percentiles over every timed first receipt. A fixed 432 KiB histogram rounds percentile durations upward by less than 0.1%, capped at the exact maximum; it never samples or drops timings. No receipts gives null percentiles. Small populations can put p99.9 at the maximum. The histogram covers nonnegative 64-bit nanosecond durations, and its size does not grow with uptime.

The verifier releases contiguous record IDs and bounds outstanding streaming input; RSS monitoring retains fixed warm/final windows and a peak. JSONL samples go to disk. Checks cover record fidelity, saved progress, worker work caps, a 512 MiB sampled RSS ceiling and at most 32 MiB final-median RSS growth after warm-up. These measurements do not include Go heap or goroutine profiles, and sampling can miss short peaks.

Start at a moderate rate: 50 records/second with this fixture's roughly 3 KiB average payload is about 40 GB of raw input over 72 hours, before Kafka replicas, destination output and archives. Payloads compress heavily by default. Set `KAFERRY_ARCHIVE_PAYLOAD=random` for deterministic high-entropy values. Bound the test duration/rate to available disk; the script does not prune active records or archives during verification. Source retention must exceed the maximum test backlog or interruption. Record-ID overflow is rejected before intake.

Use a local scheduler such as launchd/cron to invoke these commands against approved commits or release executables, with one workload at a time and host sleep disabled for the run. No scheduler is installed automatically. Keep public PR execution on disposable hosted runners. GitHub-hosted jobs cannot remain alive for a multi-day run.

## Published-version contracts

[The release catalog](../tests/qualification/releases.json) states the expected contract for each selected release: successful resume or explicit rejection. It is intentionally explicit rather than assuming all older versions share state formats.

```sh
python3 scripts/qualify.py releases --output build-results/released-versions
```

This downloads native release archives and checksums, verifies archive attestations with `gh`, then runs each cataloged contract. Downloads remain in the ignored local cache. A rejected-state case also verifies that completed old Avro records remain readable with a fresh group. Update the catalog when the supported release set changes; the v0.9.0 rejection follows its documented unsupported state contract.

For a binary already downloaded and verified:

```sh
python3 scripts/qualify.py run migration --previous-binary /path/to/previous/kaferry
python3 scripts/qualify.py run migration --previous-binary /path/to/incompatible/kaferry --state-contract reject
```

The resume case checks both binary directions and unsupported metadata rejection. The incompatible case requires rejection before destination records change. [Release upgrade contracts](../releases/v0.10.0.md#upgrade).

## Managed-service qualification

Provision a sandbox cluster and test identity separately. The suite creates and deletes uniquely named test topics, so the identity needs those permissions in addition to Kaferry's usual read/write/group grants. It never provisions a cluster, VM, network or paid service.

Create a private JSON file outside the repository and evidence directory. For a plain or SCRAM service, `source` and `destination` contain Kaferry connection properties. For AWS IAM, also set `aws_region`; the independent client obtains OAuth IAM tokens through the AWS signer while Kaferry uses its own IAM mechanism. Standard AWS credential discovery applies. Optional `fixture_source`/`fixture_destination` objects override independent-client properties when fixture permissions or authentication differ. Optional `replication_factor` overrides the broker default.

```json
{
  "aws_region": "us-east-1",
  "source": {
    "bootstrap.servers": "MSK_BOOTSTRAP_BROKERS",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "AWS_MSK_IAM"
  },
  "destination": {
    "bootstrap.servers": "MSK_BOOTSTRAP_BROKERS",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "AWS_MSK_IAM"
  }
}
```

Replace both bootstrap placeholders and provide the account/network access before running:

```sh
python3 scripts/qualify.py setup --cloud
python3 scripts/qualify.py run cloud --config /private/path/qualification.json
```

One cluster can qualify source and destination authentication using separate topics. Cross-cluster or cross-region claims require those actual endpoints. The generated credential configuration is private and temporary, outside evidence uploads. CLI error logs are retained; use sandbox credentials and inspect logs before sharing them.

Use a same-VPC client for private MSK access. Keep certification batches short and low-volume; compare total provisioned/serverless pricing, and delete temporary infrastructure even after a failed test. Three brokers on one host demonstrate logical failover, not loss of independent machines or zones. Physical power-loss and particular shared-filesystem durability claims require the corresponding deployment.

## Evidence and release gates

CLI scenarios use the supplied executable; component/model/fuzz/benchmark tests compile checkout code and are labeled separately. Each suite writes `manifest.json` before execution and updates it atomically afterward. It records source revision/dirty state, executable digest/version, requested images, actual broker image evidence, platform/toolchain, required test names, elapsed time and result. Broker copies include independent reconciliation; endurance includes metrics and `soak.samples.jsonl`. Missing credentials, skipped required tests, empty selections and interrupted runs cannot qualify a case.

```sh
python3 scripts/qualify.py report build-results/full-matrix --tier full
```

The report requires every planned case exactly once and the same source revision and executable digest. It writes `qualification.json` and `qualification.md`, returning nonzero for incomplete evidence. Keep failed runs; compare follow-ups in new directories.

The release workflow tests the exact Linux AMD64 archive across the full matrix before publication. It verifies matrix identities against the packaged executable and retains checksummed `compatibility.json`/`compatibility.md` assets alongside the existing qualification summary. Detailed CI logs retain their workflow-specific lifetime. A long-run receipt for a different build does not certify the published binary.
