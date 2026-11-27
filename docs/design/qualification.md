# Qualification lab

Run these scripts from a terminal, CI or a local scheduler. The lab starts disposable brokers, runs the selected executable and retains results. [Component tests](../../CONTRIBUTING.md#test-targets) are the fast development entrypoints.

## Prepare

Use Python 3.11+, the Go version in `go.mod`, Docker Engine 25+ with Compose 2.20.2+, OpenSSL and Make. Linux and macOS AMD64/ARM64 are supported. Race tests require a C compiler; Linux may need its distribution's Python venv package. Release comparisons require authenticated `gh` with repository and attestation read access.

From the repository root:

```sh
make qualification-setup
python3 -m tools.qualification.qualify plan --tier pr
```

Setup builds `bin/kaferry` and installs the independent librdkafka checker. Use the same Python major/minor for setup and runs. Set `--binary` to test an existing executable. Pass a fresh `--output` directory for evidence. Broker data stays on Docker storage. Archive outputs need [durable storage](archive.md#layout-and-ownership).

Run one coordinator per host across all checkouts. Keep the host awake and avoid concurrent `make broker` or `make qualification`, which share ports without acquiring the lab's lease. Start with eight free CPUs, 16–20 GiB available RAM and 100 GiB free test storage. Check the larger [endurance storage requirements](#endurance-on-a-persistent-host) before a long run.

## Choose a run

Run `python3 -m tools.qualification.qualify run --help` for supported versions, fault cases and flags. CI and releases also use the [matrix tiers](#broker-and-security-matrix).

| Command after `python3 -m tools.qualification.qualify` | Purpose |
| --- | --- |
| `run broker --source 3.9.2 --destination 4.4.0` | Independent Kafka/archive compatibility checks |
| `run auth --source 4.4.0` | Authentication, ACLs, TLS and replicated leader failure |
| `run replicated` | Healthy RF=3 copying |
| `run fault-replicated --case broker/source-leader` | One selected replicated fault; omit `--case` for all fourteen |
| `run integration` | CLI copying, fidelity, resume and source truncation |
| `run fault` | Outages, process death, restart and ownership changes |
| `run recovery --duration 2h` | Repeat the five recovery cases with fresh topics/processes |
| `run load` | Bounded Kafka/archive workload measurements |
| `run endurance --duration 24h` | One continuously running copier under the fixed application workload |
| `run endurance --topology replicated --duration 24h` | The same endurance workload on RF=3 clusters |
| `run soak --duration 24h --rate 50` | Smaller streaming Kafka soak |
| `run capture --duration 72h --rate 50` | Continuous archive capture |
| `run resources`, `run model`, `run fuzz`, `run benchmark` | Checkout component, model, fuzz or benchmark tests |
| `run smoke` | File-copy/tutorial checks |

All commands attempt cleanup and return nonzero on failure or interruption. CLI suites use the selected executable; component/model/fuzz/benchmark tests compile checkout code. Missing credentials or checker dependencies, skipped required tests and empty selections cannot qualify a run.

## Broker and security matrix

Select `pr` for the four oldest/newest broker pairs, `nightly` for each pinned version against itself, or `full` for every directed pair. PR and nightly tiers run security checks on the newest image. Full qualification checks every [pinned image](../../src/tests/qualification/matrix.json).

```sh
python3 -m tools.qualification.qualify matrix --tier full --output build-results/full-matrix
```

The independent Kafka client and Avro reader check routing, capture, replay and record fidelity after a partial copy, a resume and a completed rerun. Security fixtures cover supported authentication methods, TLS, denied access and replicated destination-leader failure. Token rotation is exercised with the fixture issuer. Prolonged Kerberos renewal and deployment-specific identity providers need their own runs.

## Replicated faults

First run a healthy copy through two disposable three-broker clusters with replication factor 3 and minimum in-sync replicas 2. Allow at least two Docker CPUs, 8 GiB of Docker memory, 4 GiB free on broker storage, 1 GiB on evidence storage, and the 25 published localhost ports in the [fixture](../../src/tests/replicated/compose.yaml). Worker-kill and coordinator-kill cases also require access to inspect the worker processes with `ps`.

```sh
python3 -m tools.qualification.qualify run replicated --binary bin/kaferry --output build-results/replicated-smoke
python3 -m tools.qualification.qualify run fault-replicated --case broker/source-leader --binary bin/kaferry --output build-results/source-leader
```

Choose the case for the failure you need to qualify:

| Family | What a pass establishes |
| --- | --- |
| Ownership | Worker loss, scaling and coordinator loss exercise group transitions and accepted commits by the new owners. Zombie fencing checks for accepted stale saves. A positive rejection claim requires an observed stale request and rejection for that partition. |
| Broker | Leader loss, rolling restarts or loss of in-sync replicas exercise stalled or transferred work, saved progress and recovery on the affected partitions. |
| Network | Route cuts, latency, bandwidth limits and delayed close exercise delivery stalls and recovery. Among network cases, only a whole-destination outage permits a copier restart. |

Check the recorded injection and recovery events, independently reconciled records, duplicate windows and saved positions. At each sampled cut, progress must stay within the contiguous output confirmed by the observer. Sampling two clusters cannot rule out a transient violation between observations. A missing lifetime summary leaves exact work peaks unknown. [Case timeout settings](../../src/tests/qualification/fault-timeouts-ms.json).

The runner may retry an inconclusive pre-fault readiness check once, after clean teardown in a clean checkout, with the same source and binary. It retains both attempts. A safety failure, a fault already attempted or failed cleanup precludes retry. Two inconclusive attempts return nonzero.

Relevant PRs run zombie fencing, worker scaling and destination-leaders outage. The weekly hosted run covers ten cases. Before promoting a release, run `network/latency`, `network/bandwidth`, `network/slow-close` and `ownership/coordinator-kill-rebalance` individually against the verified published native executable, each with fresh output, alongside 24-hour endurance. Disclose any missing local passes as explicit exclusions in the release notes. A single Docker host cannot qualify loss of a machine or zone. [Release procedure](../releases/README.md#procedure).

## Recovery and bounded load

Use `run fault` to exercise blocked-output failures, ownership loss and restart. The observer samples saved group offsets and independently reads acknowledged output. After restart it reconciles retained source records, including any duplicates, and checks final progress. `run recovery --duration 2h` repeats complete fault cycles with fresh processes and topics. For evidence from one continuously running process, use endurance.

### Load workload profiles

`run load` measures copying across four source and five destination partitions with key routing. Profiles exercise near-limit values, skewed keys, repeated headers, tombstones and each Kafka compression codec. Each checks record counts, hashes, routing and work caps. Compare rates only for matching versioned workloads and parameters. The tombstone case checks copying a null value without qualifying cleaner deletion.

## Endurance on a persistent host

Run a one-minute rehearsal before reserving a host for a 24-hour run. Use an explicit duration, candidate binary and fresh evidence directory:

```sh
python3 -m tools.qualification.qualify run endurance --duration 1m --output build-results/endurance-rehearsal
python3 -m tools.qualification.qualify run endurance --duration 24h --binary bin/kaferry --output build-results/endurance-24h
```

For 24 hours, allow about **272 GiB on each default broker filesystem** or **809 GiB on the replicated fixture's shared volume**, plus at least 1 GiB of evidence storage. The preflight also budgets 8 KiB of evidence per publishing second if that is larger. Monitor free space during the run. Preflight cannot reserve it. For topology comparisons, use `--source 4.4.0 --destination 4.4.0` for the single-broker baseline and `--topology replicated` for the replicated run.

One copier serves 24 source and 32 destination partitions at 200 scheduled records/s, with bursts and one-second destination stalls. A pass requires catch-up after stalls, matching final offsets and complete record reconciliation without duplicates or reordering within a source/destination partition pair. It also checks shutdown, tracked work and sampled memory. Sampling can miss short peaks. The pass covers the recorded duration, workload and topology. Repeated short runs cannot establish continuous uptime. Ownership changes and broker failures need separate fault tests.

### Publish endurance results

After cleanup, standalone endurance writes `summary.md` and `publication/`, including failed/interrupted runs. The package has Markdown/JSON results, an evidence tarball and checksums; unavailable observations stay unavailable and short runs are rehearsals. Existing package directories are refused.

```sh
python3 -m tools.qualification.endurance_results build-results/endurance-24h --output build-results/endurance-package
python3 -m tools.qualification.endurance_results build-results/endurance-24h --publish vX.Y.Z
```

Publishing is explicit. Run with `--binary` against the verified release executable first. The command downloads that release's platform archive, requires the measured executable digest to match, verifies package checksums and requires a clean fixture checkout. Preflight failures without metrics cannot upload; failed measured runs retain their status. With a custom package directory, repeat `--output` when publishing; `--repo OWNER/REPO` selects the repository. Assets get platform/date prefixes without rewriting existing assets, tags or release notes. The ordinary release-results generator remains separate.

## Read results and clean up

Start with `manifest.json`, `suite.log`, broker/CLI logs and any metrics or independent reconciliation results. The manifest records source/dirty state, executable digest/version, images, platforms, required tests, elapsed time and status. `binary_platform` comes from executable build settings; `execution_platform` comes from the checkout Go target. A command definition or catalog entry is not a passing qualification.

Fault results retain actual injection/heal events, sample coverage, independent final reconciliation and source/binary identity. Missing observations, unsafe prefixes, unexplained duplicates, failed reconciliation or cleanup remain failures. Keep failed directories and give follow-ups new paths. Never infer unavailable exact peaks from sampled values.

```sh
python3 -m tools.qualification.qualify report build-results/full-matrix --tier full
```

The report verifies every planned case exactly once against the same source revision and executable digest and writes `qualification.json` and `qualification.md`. Release CI tests the packaged Linux AMD64 executable. Use the verified published native executable for the [local release checks](#replicated-faults).

If Docker is unavailable, restore it, confirm the run has stopped and remove only the corresponding lab project:

```sh
docker compose -f src/tests/compose.yaml -p kaferry-compat --profile cross-cluster down -v --remove-orphans
docker compose -f src/tests/compose.yaml -p kaferry-endurance --profile cross-cluster down -v --remove-orphans
KAFERRY_QUAL_CERT_DIR=/tmp docker compose -p kaferry-qualification -f src/tests/qualification/compose.yaml down -v --remove-orphans
```

The auth certificate placeholder permits teardown without generating credentials. Restarting Docker can affect other workloads and is outside automatic cleanup.

For a stopped replicated run, restore its project and UUID from the manifest. Stop writers before authenticated deletion and preserve storage if any command fails:

```sh
(
  set -eu
  manifest=build-results/replicated-smoke/manifest.json
  project=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["fixture_project"])' "$manifest")
  KAFERRY_REPLICATED_RUN_ID=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["fixture_storage"]["run_id"])' "$manifest")
  export KAFERRY_REPLICATED_RUN_ID
  storage_script=$(cat src/tests/replicated/storage.sh)
  docker compose -f src/tests/replicated/compose.yaml -p "$project" --profile replicated stop --timeout 10
  docker compose -f src/tests/replicated/compose.yaml -p "$project" --profile replicated run --rm --no-deps --entrypoint /bin/sh storage -ec "$storage_script" storage clean
  docker compose -f src/tests/replicated/compose.yaml -p "$project" --profile replicated down -v --remove-orphans
)
```

## Paired performance

This comparison runs one revision after the other on the same quiet host:

```sh
KAFKA_TEST_BROKERS=127.0.0.1:19092 \
  sh tools/qualification/performance_run.sh origin/main HEAD --pairs 6 --archives
```

The wrapper cleans up after each comparison. [Performance CI](../../.github/workflows/performance.yml) warms up the binaries, alternates which runs first and reports the median ratio and pair range. Both revisions must support the chosen workloads. Archive comparisons require Avro support. The retained results describe that runner and workload, without a merge gate.

Use nightly same-binary calibration to measure noise. Review at least 20 runs before setting `PERFORMANCE_MODE=comparison`. Use `ci:performance` for a PR or supply a base in manual dispatch. Measurements and raw evidence are retained for 30 days.

## Published-version contracts

The [release catalog](../../src/tests/qualification/releases.json) selects the expected saved-state behavior for each release.

```sh
python3 -m tools.qualification.qualify releases --output build-results/released-versions
python3 -m tools.qualification.qualify run migration --previous-binary /path/to/previous/kaferry
python3 -m tools.qualification.qualify run migration --previous-binary /path/to/previous/kaferry --state-contract reject-progress
```

`releases` downloads native archives/checksums and verifies attestations with `gh`; downloads stay in the ignored cache. The default contract (`resume`) requires both binaries to continue the same job in either direction. Use `reject-progress` when the saved Kafka commits or archive replay identities are incompatible. Each binary must reject the other’s group before writing, and a fresh group must still read the completed Avro records. The `reject` contract separately checks old archive writer state. [Upgrade contracts](../releases/v0.10.0.md#upgrade).

## Managed-service qualification

Provision a sandbox and test identity separately; the lab creates/deletes unique topics but never provisions infrastructure. It needs topic-management permissions beyond ordinary copy grants. Keep private JSON outside the repository/evidence directory:

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

Use Kaferry connection properties for plain/SCRAM services. IAM also requires `aws_region`, AWS signer dependencies and normal credential discovery. Optional `fixture_source`/`fixture_destination` override independent-client properties, and `replication_factor` overrides broker defaults. Replace bootstrap placeholders and establish access before running:

```sh
python3 -m tools.qualification.qualify setup --cloud
python3 -m tools.qualification.qualify run cloud --config /private/path/qualification.json
```

One cluster can test both sides through separate topics; cross-cluster/region claims need those endpoints. Temporary credentials stay outside evidence, but retained CLI errors need inspection before sharing. Use sandbox credentials, short/low-volume batches and same-VPC clients for private MSK; delete temporary infrastructure separately. Deployment-specific power-loss/shared-filesystem claims require that deployment's evidence.
