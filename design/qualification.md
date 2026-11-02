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

Reserve ports 39092/40092 for plain brokers, 41092–43094 for the security listeners and 44088 for Kerberos. Run one lab per host, across all checkouts and do not run `make broker` or `make qualification` alongside it. The runner takes a host-wide lock for its fixed ports and Compose names; legacy Make entrypoints do not take that lock. Broker containers use only the `kaferry-compat` and `kaferry-qualification` Compose projects. Other Docker workloads are outside the lab.

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

Every tier also runs a two-minute archive-capture endurance smoke. A matrix runs sequentially locally; CI runs up to six cases in parallel on separate hosted runners. Existing output directories are refused so failed evidence cannot be overwritten. Use a new output path for a follow-up run.

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
KAFERRY_QUAL_CERT_DIR=/tmp docker compose -p kaferry-qualification -f tests/qualification/compose.yaml down -v --remove-orphans
```

The temporary certificate path satisfies Compose interpolation during teardown; it does not create new brokers. Restarting Docker can interrupt other workloads and is outside the lab's automatic cleanup.

## Endurance on a persistent host

Keep a process alive continuously for the duration being claimed. Separate short runs do not add up to continuous uptime. The runner accepts integer seconds, minutes, hours or days, from one minute through seven days; it derives the Go test timeout from that duration.

```sh
python3 scripts/qualify.py run soak --duration 24h --rate 50
python3 scripts/qualify.py run capture --duration 72h --rate 50
```

`soak` verifies topic output as it arrives and pauses the separate disposable destination once per minute. `capture` runs continuous topic→archive copying and streams its completed files through verification after shutdown. Replay/file-to-file load tests are finite jobs; their repeated execution is not reported as continuous streaming uptime.

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
