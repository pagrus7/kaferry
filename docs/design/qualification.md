# Qualification lab

Run these scripts from a terminal, CI or a local scheduler. The lab starts disposable brokers, runs the selected executable and retains results. [Component tests](../../CONTRIBUTING.md#test-targets) are the fast development entrypoints.

## Prepare

Use Python 3.11+, the Go version in `go.mod`, Docker Engine 25+ with Compose 2.20.2+, OpenSSL and Make. Linux and macOS AMD64/ARM64 are supported. Race tests require a C compiler; Linux may need its distribution's Python venv package. Release comparisons require authenticated `gh` with repository and attestation read access.

From the repository root:

```sh
make qualification-setup
python3 -m tools.qualification.qualify plan --tier pr
```

Setup builds Kaferry and installs the independent librdkafka checker in a checkout-local venv. Use the same Python major/minor version for setup and runs; for example, `make qualification-setup PYTHON=python3.14`, followed by `python3.14 -m tools.qualification.qualify …`. Select an existing executable with `--binary /absolute/path/to/kaferry`; the default is `bin/kaferry`. Qualification does not rebuild it.

Run one coordinator per host across all checkouts. Commands run sequentially under a host-wide lease for fixed ports and Compose names. Do not run legacy `make broker` or `make qualification` alongside them; those targets do not acquire the lease. Keep the host awake during long runs. Other workloads can affect measurements and available disk.

| Fixture | Compose project | Localhost ports |
| --- | --- | --- |
| Default two-broker source/destination | `kaferry-compat` | 39092, 40092 |
| Default endurance source/destination | `kaferry-endurance` | 39192, 40192 |
| Authentication, TLS and Kerberos | `kaferry-qualification` | 41092–43094, 44088 |
| Two three-broker clusters and proxy | `kaferry-replicated` | 46092–47394 as listed below; API 48074 |

Start with one workload, eight available CPUs, 16–20 GiB available RAM and 100 GiB free test storage; long endurance needs substantially more disk as described below. These are starting allocations, not capacity guarantees. Broker heaps are capped at 512 MiB, with four internal offset/transaction partitions.

`--output` must name a fresh evidence directory. It also selects archive/checkpoint output where used; broker data stays on local Docker storage. A NAS evidence directory does not supply broker disk capacity. Existing output paths are refused so failures cannot be overwritten. Archive output requires the [archive filesystem contract](archive.md#layout-and-ownership).

## Choose a run

An **operating condition** produces zero duplicates, requires no process restart and leaves partition ownership unchanged. Endurance includes steady backpressure, bursts and its existing one-second destination stalls. A **fault** exceeds delivery/session bounds, changes ownership, kills a process or broker, or allows duplicates. Faults belong in fault suites, outside endurance. Change only topology when comparing endurance topologies; retain workload, gates and exposure.

| Command after `python3 -m tools.qualification.qualify` | Purpose |
| --- | --- |
| `run broker --source 3.9.2 --destination 4.4.0` | Independent Kafka/archive compatibility checks |
| `run auth --source 4.4.0` | Authentication, ACLs, TLS and replicated leader failure |
| `run replicated` | Healthy RF=3 copying and worker proxy-path isolation |
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

The [image catalog](../../src/tests/qualification/matrix.json) pins supported broker images by digest. Update its pins when changing supported images.

| Tier | Directed broker cases | Security fixture |
| --- | --- | --- |
| `pr` | Oldest/newest in both directions and same-version: four | Newest image |
| `nightly` | Every version to itself: five | Newest image |
| `full` | Every directed pair: 25 | All five images |

```sh
python3 -m tools.qualification.qualify matrix --tier pr
python3 -m tools.qualification.qualify matrix --tier full --output build-results/full-matrix
```

Local matrices run sequentially. CI distributes up to six cases across separate hosted runners. Every tier includes archive capture: one minute for PR, two minutes for nightly/full/release, followed by ten seconds without new input.

The independent checker uses librdkafka; Kaferry uses franz-go. It seeds 96 records through all five compression codecs and checks preserved/key Kafka routing plus capture→clone→replay. Each route checks seven records after partial completion, all 96 after resume and unchanged output after a completed rerun. Python fastavro reads archives independently. Reconciliation checks consumed record identities, complete bytes, null versus empty, ordered duplicate headers, timestamps and preserved partitions.

Security covers PLAIN, SCRAM-SHA-256/512, signed OAuth client credentials, token files, Kerberos keytabs, server TLS, mTLS, denied access and replicated destination-leader failure. OAuth checks signatures, issuer, audience and expiry; live copying renews short-lived tokens while keys rotate. Token-file replacement is checked across invocations. Prolonged Kerberos renewal and deployment-specific identity providers need separate runs. Internal broker/controller listeners use plaintext. Authentication keeps its disposable Compose fixture, private certificates/KDC state, startup/cleanup, image validation before tests and required-test checks.

Compatibility tests live in `src/tests/compatibility`, using regular helpers from `src/tests/system`. Relevant PR changes run the matrix; unrelated documentation/reviewer changes may skip it. Unknown paths run it. Scheduled main jobs may reuse a passing suite/tier on the same commit only while its test executions are less than 30 days old. Failed/cancelled attempts retry; skipped tests and aggregate report reruns do not renew evidence. Nightly cannot replace full. The planner links reused executions. Weekly fuzzing, vulnerability scans and performance calibration continue separately. Releases always test their packaged executable.

## Plain replicated smoke

```sh
python3 -m tools.qualification.qualify run replicated --binary bin/kaferry --output build-results/replicated-smoke
```

The fixture starts two plain three-broker clusters on the newest catalog image, with RF=3 and `min.insync.replicas=2`. Admission requires two Docker CPUs, 8 GiB runtime memory, 4 GiB free on broker storage, 1 GiB on evidence storage and all 25 published localhost ports available. These smoke allowances include the proxy but do not reserve capacity against other users.

Each run owns a fresh UUID directory beneath `kaferry-replicated/<project>/` on its disposable Docker volume. Cleanup stops writers, validates project/UUID ownership and removes only that directory. It refuses symlinks in ownership paths, preserves peer/unrelated data and stops on the first deletion error. The ownership marker survives partial deletion; failed final directory removal attempts to restore it. A failed stop or ownership check preserves the volume and fails qualification. There is no automatic cleanup retry.

Compose polls each broker's local `127.0.0.1:9092` TCP listener with a five-second probe timeout. API/ISR checks then require distinct cluster identities, three in-sync replicas per created partition and minimum ISR=2, with a 30-second topic setup window. The checker runs the same 96-record routes and partial/resume/completed checks as the broker matrix.

### Replicated network paths

The fixture pins [Toxiproxy 2.13.0](https://github.com/Shopify/toxiproxy/releases/tag/v2.13.0) by multi-platform digest. Its 36 routes separate admin, workers 1–3, replication and controller traffic for every broker. Brokers publish no direct host ports. Metadata advertises each client's matching proxy path.

| Path | Source ports | Destination ports |
| --- | --- | --- |
| Independent publisher, checker and admin | 46092–46094 | 47092–47094 |
| Worker 1 | 46192–46194 | 47192–47194 |
| Worker 2 | 46292–46294 | 47292–47294 |
| Worker 3 | 46392–46394 | 47392–47394 |

The API binds localhost 48074. Replication uses internal ports 26092–26094/27092–27094; controllers use 28092–28094/29092–29094. Health probes stay inside brokers and bypass proxies. The proxy namespace reserves route listeners/API from automatic outbound source-port allocation, including while listeners are disabled. The runner checks the reservation before configuring routes.

Before smoke or replicated faults, the route check publishes through each worker to a leader discovered through metadata, cuts that leader's worker proxy and requires failure while the independent admin path still works. After healing, a fresh worker producer succeeds. Independent consumption rejects an uncertain blocked record and requires exactly the three acknowledged records. RF3 endurance configures enabled routes without this cut/heal check.

The [fault helper](../../src/tests/system/faults) resolves leaders, coordinators, followers and worker paths at injection. It supports owned-container kill/start, restart, pause/unpause and network disconnect/reconnect, plus proxy disable, timeout, reset, latency, bandwidth and slow close. Time values are milliseconds; bandwidth is KB/s per connection. Empty listener selection covers all six paths for a broker; worker selection affects that worker only. Healing restores the frozen container IDs, proxies and original network aliases. Ownership labels and proxy addresses must match before mutation; partial injection unwinds prior mutations and failed healing fails the case.

A coordinator-path cut affects every worker connection to that broker, including colocated fetch/metadata traffic. Container disconnection affects replication/controllers too. Portable helper primitives alone do not qualify every combination against brokers. Use the runner to retain lease, startup and cleanup.

## Replicated faults

Use an existing candidate and a fresh evidence directory after [preparation](#prepare). These cases need the replicated smoke's capacity and exclusive ports. Live RSS uses native macOS task information or `ps` elsewhere; unavailable live RSS fails.

The worker-kill and coordinator-kill cases also require permission to execute `ps` and inspect their own stopped worker processes.

```sh
python3 -m tools.qualification.qualify run fault-replicated --case ownership/zombie-worker --binary bin/kaferry --output build-results/ownership-zombie
python3 -m tools.qualification.qualify run fault-replicated --case ownership/worker-kill-rebalance --binary bin/kaferry --output build-results/worker-kill
python3 -m tools.qualification.qualify run fault-replicated --case broker/source-leader --binary bin/kaferry --output build-results/source-leader
python3 -m tools.qualification.qualify run fault-replicated --case network/destination-outage --binary bin/kaferry --output build-results/destination-outage
```

All cases use 24 RF=3 partitions with minimum ISR=2, independent publication and a continuous saved-prefix observer. Publication schedules one record per partition every 100 ms; source acknowledgement/scheduler gaps over two seconds fail. Each offered write must complete successfully before reconciliation. The broker publisher permits safe idempotent retries within its delivery deadline, bounds outstanding writes per partition and drains offered writes when stopping.

| Case | Required effect and recovery |
| --- | --- |
| `ownership/zombie-worker` | Two workers share a group. Discard worker 1's upstream coordinator packets while keeping TCP connections open; observe actual session expiry, transfer to worker 2 and accepted peer progress on every transferred partition while both CLI processes remain alive before healing. |
| `ownership/worker-kill-rebalance` | Hold worker 1 with SIGSTOP, start worker 2, observe both in `PreparingRebalance` and recheck immediately before SIGKILL. The peer must own and save every partition in a later generation. |
| `ownership/coordinator-kill-rebalance` | Use the same held-worker rebalance, recheck its actual coordinator and kill that broker, then resume worker 1. Require replacement coordination while the broker stays stopped, restored ADMIN readiness/ISR3 and accepted progress from both routes. Required local case. |
| `ownership/worker-scale` | Add two workers while publishing, then stop those peers successfully. Independently require disjoint nonempty assignments and accepted commits in each 1→3→1 membership generation; the original worker stays alive. |
| `broker/source-leader` | Kill the resolved source leader; observe replacement leadership and publication before restoring it. |
| `broker/destination-leader` | Kill the resolved destination leader; observe replacement leadership, restore it and require saved progress. |
| `broker/rolling-source`, `broker/rolling-destination` | Restart brokers 1–3 in order; require readiness, all 24 ISR sets at three and saved progress before each next restart. |
| `broker/destination-isr-loss` | Cut the resolved leader's replication listener with controllers running. Freeze a partition on that same leader only after actual ISR=1, acknowledged source backlog and unresolved CLI work; use it for stall, prefix and same-process recovery checks. |
| `network/destination-outage` | Disable all worker destination routes for 66 seconds (delivery plus session). Observe fail-fast exit, heal and restart the same executable/configuration/SQL/group/route. |
| `network/destination-leaders-outage` | Cut partition 0's current leader route for six seconds. Its partitions must stall; other partitions must advance output and saves. |
| `network/latency` | Add 500 ms upstream latency to all worker destination routes for six seconds; require unresolved writes and at least 25 more output-backlog records than baseline. |
| `network/bandwidth` | Limit each worker destination connection to 32 KB/s upstream for six seconds; require increased backlog, unresolved writes and output below 75% of baseline. |
| `network/slow-close` | Add downstream 60-second close delay. An auxiliary one-ms upstream timeout supplies backend EOF on the same routes. Require stalled output/unresolved writes and an actual TCP EOF held at least six seconds, then released after removal. |

Slow close includes that connection interruption; the toxic alone cannot pass. The other network cases have no auxiliary stimulus. Only whole-destination network outage permits restart; the other four require the same CLI process to survive. Broker leader/rolling cases may restart after bounded fault-related exit 1; the ISR case forbids restart. Worker exits must fit the case’s actual fault/restart boundaries and recorded lifetime, status and driver intent. Coordinator restarts additionally require Kaferry `poll source` or `save progress` labels. Unknown failures do not qualify.

Zombie fencing requires correlated broker request/response observations for every selected CLI commit, no accepted stale-generation save after transfer, and positive fencing: stale-generation rejections covering worker 1's former partitions or legitimate rejoin with accepted progress on its new assignment. Missing/truncated logs or logging failures fail. Worker 2 remains alive and succeeds. Worker 1 may exit 1 after healing only when its late stale save was rejected before exit; early transport exit or unexplained failure fails. Publication and saved progress must advance on every partition after healing.

The zombie peer’s accepted saves must cover every transferred partition and have broker receipt times after the completed transfer observation. A request received before transfer cannot open the heal gate merely because its response completes afterward.

Worker-kill and scale cases permit no restart. The coordinator case permits one restart per affected route after broker API/ISR recovery, using the same executable, SQL, group, configuration and route. Its eligible exit 1 must occur between physical kill and observed recovery. Worker 1 may carry only the recorded hold/resume signals; its peer may carry no driver stop intent. Lifecycle evidence binds PID, executable, raw log and sole OS wait; prior cancellation or unmatched signals fail.

For these three cases, raw broker group descriptions, successful joins, decoded `SYNC_GROUP` assignments and accepted commits independently bind each stable generation to one selected route lifetime. Membership reads resolve the current coordinator; each two-second observation uses one complete audit snapshot inside its 60-second state deadline. Expired observations cannot establish readiness; malformed protocol records fail. This positive transition proof does not add the zombie case’s complete stale-generation fencing claim; coordinator death can lose in-flight request completions. Coordinator restoration checks target-pinned ADMIN/ISR3 before reapplying scoped audit logging and again for recovery.

| Scope | Configured bounds and complete post-heal observation |
| --- | --- |
| Zombie fencing | Session 6s, source RPC 8s, periodic save 10s, delivery 2s, shutdown 5s; observe their full 31s sum and a healthy sample. Three-minute case/four-minute Go deadline. |
| Worker kill / coordinator kill / scale | Session 6s, RPC 8s, rebalance 30s, save 10s, delivery 60s, shutdown 5s; coordinator also checks replica lag 30s. Post-heal sums 119/149/119s plus a healthy sample; Go deadlines 359/389/359s, with 60s added by the outer runner. |
| Broker | Session 6s, RPC 8s, save 10s, delivery 60s, shutdown 5s, replica lag 30s; observe the full 119s sum and a healthy sample. |
| Network | Session 6s, RPC 8s, save 10s, delivery 60s, shutdown 5s; observe full 89s and a healthy sample. Case/Go/outer deadlines are 293/353/413s. |

Broker recovery requires the owned container running with its recorded start time and a fresh all-topics Metadata response pinned to that broker's independent ADMIN route. Inspection and all selected requests share the two-second query context and 60-second recovery gate. Those same responses must satisfy the caller's ISR condition. ADMIN readiness alone does not establish worker-route recovery.

Network injection follows nonempty saved prefixes and a 30-second healthy measurement at at least 95% of scheduled 240 records/s, for both output and independent source acknowledgements. Before healthy/recovered measurements, each partition's acknowledgements must catch up to its original schedule (at most one current slot outstanding), with output backlog at most 24. Debt cannot be reset. After healing, every partition must save again and a six-second measurement must reach 95% of baseline. Leader stability and affected/unaffected partition checks remain required.

An unmet healthy-rate precondition ends before injection. Only successful normal CLI shutdown, independent reconciliation, prefix, duplicate and resource checks make the attempt **inconclusive**. The runner retains `attempt-1/` and allows one fresh-fixture retry after successful cleanup under the same lease and unchanged source/binary identities. Automatic retry requires a clean checkout, including no nonignored untracked files. Product/safety failures, cancellation and cleanup failures never retry. A second inconclusive attempt returns nonzero; a later pass retains the first attempt.

Every lifetime requires sampled RSS at most 512 MiB and observed current work within 1,024 records/128 MiB. Normal completion requires a final summary supplying exact lifetime peaks. A permitted broker/network fault exit may omit it; missing peaks remain null and that lifetime has no exact-cap claim. The three ownership-transition cases require final summaries for successful/fail-fast exits and permit missing peaks only after their explicit SIGKILL. Current samples cannot supply exact peaks. Independent full-record reconciliation, saved-prefix safety, duplicate attribution and complete recovery remain mandatory.

Hosted checks run the five broker faults, zombie fencing, worker kill, worker scaling and two destination outages: ten fault cases on separate runners. Four cases require local exact-candidate qualification alongside 24-hour endurance: `network/latency`, `network/bandwidth`, `network/slow-close` and `ownership/coordinator-kill-rebalance`. Run each with its `--case` and a fresh output directory. Missing local passes require explicit release-note exclusions. Selecting all fourteen cases gives 4,907s Go allowance plus 60s outer allowance; individual selection uses that case's budget. These local cases have no separate hosted scheduled tier. One-host clusters do not establish machine or zone-failure behavior.

## Recovery and bounded load

Run `make check` for race-enabled confirmed-prefix, stale-delivery, partial-save, shutdown and archive durability regressions. Start brokers with `make broker` before `make fault`; run `make qualification-setup` with the same Python major/minor first. Code PRs run one fault cycle. A local two-hour recovery run repeats complete cycles of the same five cases with fresh topics/processes and allows ten minutes to finish its final cycle. It does not establish continuous uptime.

Blocked-output cases first save a prefix on two partitions, pause the destination and observe new submissions, then kill/signal the CLI, exceed delivery timeout or transfer ownership. Saves must remain at the delivered prefix. A separate healthy-output case pauses the source after confirmed but unsaved work and requires shutdown to time out saving progress. Restart must recover every retained record, checking every received copy, including allowed duplicates. Integration separately checks explicit running out-of-range failure and restart after source truncation; see [record-error policies](../reference.md#record-error-policies).

The independent observer establishes a fixed-cut baseline with fresh saved group positions and empty output before copying. Only transient timeout, transport and coordinator-discovery errors may retry during setup; unsafe progress/changed records fail immediately. During measurement, undeclared failed queries fail on their first attempt, with one-second query deadlines.

At roughly one-second intervals and explicit fault boundaries, it reads saves, captures output ends and consumes only through those ends. Saves may not exceed the independently matched contiguous prefix for any source partition. These ordered cross-cluster samples are not atomic; intervals between samples and transient violations repaired before the output cut are outside the claim. Outages produce explicit gaps, not passing samples; the final observation must be available. This observer covers Kafka group progress, not archive checkpoints.

After writers stop, it consumes all retained input/output and checks full bytes, null/empty, ordered headers, timestamps and partition routing. Unique full source identities are required; identical ambiguous records fail. Per-partition output cuts bound duplicate windows before injection and after recovery, followed by the full configured timeout sum and a healthy observation. Both receipts of a consecutive duplicate pair outside every window fail. Interrupted/shortened observation cannot qualify. The adapter-only empty-topic probe establishes commit deadlines rather than no-loss behavior.

`fault` has a 1,073-second Go allowance: 600s execution plus 473s recovery horizons. Catalog windows are churn 135s, crash 96s, blocking commit 45s, and recovery 39/39/11/69/39s. Churn's overlapping windows add only its final horizon. Individual CLI/query deadlines stay fixed. Repeated recovery retains requested duration plus 600s.

### Load workload profiles

`load` uses four source/five destination partitions and key routing. Profiles require exact counts, matching submission/receipt hashes, no duplicates/corruption, ordered headers/timestamps, expected routing and configured work caps. Each has 3,200 records; near-limit has 256. Producer buffering is capped at 64.

| Profile | Input |
| --- | --- |
| `near-limit-v1` | High-entropy values of 999,000–999,127 bytes; topic maximum 1,000,000 with framing space. |
| `hot-keys-v1` | Zipf over 257 ranks, exponent 1.25/offset 1, deterministic PCG seed 42; no null keys. |
| `headers-v1` | 64 ordered headers, repeated names, null/empty and 256-byte values. |
| `tombstones-v1` | 32 cyclic non-null keys, every fifth value null, compaction/minimum lag/retention ten minutes; does not test cleaner deletion. |
| `source-none-v1`, `source-gzip-v1`, `source-snappy-v1`, `source-lz4-v1`, `source-zstd-v1` | Compressible cyclic-key records with the named source codec, retained and accounted from actual source batches. |

Measurements retain versioned workload parameters; release reporting rejects unknown profiles. Compare rates only when parameters match. These profiles leave fixed endurance unchanged.

## Endurance on a persistent host

Separate short runs do not add up to continuous uptime. The publishing duration accepts one minute through seven days; one copier stays alive throughout and for ten seconds after the later of scheduled end and final submission. The first five seconds after the boundary permit only overdue scheduled input.

```sh
python3 -m tools.qualification.qualify run endurance --duration 1m --output build-results/endurance-rehearsal
python3 -m tools.qualification.qualify run endurance --binary bin/kaferry --output build-results/endurance-24h
python3 -m tools.qualification.qualify run endurance --topology replicated --binary bin/kaferry --output build-results/replicated-endurance-24h
```

For a topology comparison, run the single-broker baseline with `--source 4.4.0 --destination 4.4.0` to match the replicated fixture’s Kafka version.

The default owns `kaferry-endurance`; RF3 uses the dedicated replicated fixture and requires ISR=3/minimum ISR=2 before copying. The workload and gates remain identical:

| Gate | Requirement |
| --- | --- |
| Input | One process, key routing, 24 source/32 destination partitions; deterministic high-entropy payloads ranging from 1,024 through 5,119 bytes, with null/binary keys and ordered duplicate/null/empty headers. |
| Schedule | 200 records/s; an extra 400-record burst at 10s and every minute afterward. Each minute and final rate window must acknowledge at least 95% of its scheduled target. Total scheduled input must be submitted and acknowledged within five seconds after publishing ends; maximum schedule debt 1,400 records (five seconds plus one burst). |
| Stall/recovery | Pause destination for one second at 30s and every minute afterward during publishing; pause all three RF3 destination brokers. After healing, acknowledged input minus unique receipts must return to at most 20 within 15s and zero by the ten-second consume-only end. |
| Fidelity and progress | No missing, duplicate, corrupted, unexpected or reordered records; matching submission/receipt hashes. Non-null keys use Kafka key-hash placement; null keys are sticky. Order is per source/destination partition pair. Save each source partition's exact final next offset. |
| Stop/work | SIGINT succeeds within 30s after drain; summary counts match input. At most 1,024 uncommitted records/128 MiB. No process restart or source fault. |
| Memory | Sample RSS each second: at most 512 MiB and final median growth at most 32 MiB over samples 11–30. Check loaded final median over the last 20 samples before publishing ends and idle median after drain; require five idle samples. Sampling can miss short peaks. |

Nonblocking producer admission rejects overload instead of reducing the target. Its buffer is 2,048 records; verifier polls at most 512 receipts, with outstanding input and unresolved receipt/digest history bounded at 8,192 IDs. Rate/debt failures invalidate the load even if records eventually reconcile. A slow verifier can invalidate the harness without establishing a Kaferry throughput limit.

Preflight requires three times scheduled record bytes at 5,376 bytes/record plus 4 GiB on **each** default broker filesystem; RF3 requires nine times those record bytes plus 4 GiB on its shared volume. This allows two full maximum-size copies with 50% overhead and gives no compression credit. A 24-hour run schedules 17,856,000 records: about **272 GiB default** or **809 GiB RF3**. Evidence needs at least 1 GiB or 8 KiB per publishing second, whichever is greater. Monitor host/Docker disk; admission does not reserve against other users. Active records are not pruned and record-ID overflow is rejected before intake.

Kafka modes verify output live and through final destination offsets after shutdown; capture scans completed archives. Both compare full keys/values, ordered headers and timestamps; archives also check provenance. `delivery.integrity` hashes submissions and unique receipts, with per-source-partition ordering and a final combined SHA-256. It covers bytes/headers; timestamp, provenance, duplicate and ordering checks remain separate. Incomplete reconciliation has `missing: null` and hash `matched: null`. Observed failure counters remain available.

Metrics and `soak.samples.jsonl` retain publications/acknowledgements, total/unique receipts, pending IDs, integrity counters, lag, RSS and work. Destination record lag is acknowledged input minus unique receipts; consumer lag is end offsets minus consumed offsets, sampled only while the destination runs. Offset distance is a backlog, not a record verification. Archive verification has no live destination lag.

`delivery.observed` tracks acknowledged IDs not yet received per event, including receipt-before-ack ordering. Its maximum latency runs from test submission to first receipt on the host monotonic clock, including queueing/copier/verifier delay. `timed_receipts` gives the population; no receipts means null maxima/percentiles. p95/p99/p99.9 use nearest rank over all first receipts, rounded upward by less than 0.1% and capped at the exact maximum. Their fixed histogram does not grow with uptime. These are end-to-end fixture measurements, not isolated broker or copier latency. CPU and harness operation wall times are descriptive without hardware-independent thresholds.

A successful endurance run has zero duplicates; the product's at-least-once contract still permits replay after crashes or failed saves. This run covers its recorded topology/workload and recoverable one-second stalls, excluding ownership changes, network partitions, authentication overhead and arbitrary input distributions. RF3 alone does not qualify broker-failure recovery. Rehearsals, underloaded runs and repeated finite load jobs cannot establish multi-day uptime. Use a local scheduler if needed; none is installed, and public hosted runners cannot sustain multi-day runs.

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

The report requires every planned case exactly once with matching source revision and executable digest. It writes `qualification.json`/`qualification.md` and fails incomplete evidence. Release CI tests the exact Linux AMD64 archive across the full matrix and retains checksummed compatibility assets. Local release gates additionally require 24-hour endurance and the moved network cases on the exact published native executable, or explicit case exclusions. Results from another build do not qualify the release.

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
sh tools/qualification/performance_run.sh origin/main HEAD --pairs 6 --archives
```

The wrapper cleans up on success, failure or interruption. [Performance CI](../../.github/workflows/performance.yml) is separate from required checks. It uses warm-ups and six interleaved sequential pairs, alternating first revision. Kafka input is capped at 160,000 records; file/archive input at 200,000. Both revisions need Avro support for archive comparisons. Summaries retain median throughput and ranges, with raw metrics/identities/images for 30 days; shared-runner rates have no merge gate.

Nightly A/A calibration uses the same binary on both sides. Review at least 20 calibration runs before setting `PERFORMANCE_MODE=comparison`. Changed-main comparisons use the last compatible retained scheduled result; missing/expired data requires calibration. Manual dispatch accepts a base; `ci:performance` opts PRs into the configured mode, using the merge base in comparison mode. Choose warnings only after measuring calibration noise.

## Published-version contracts

The [release catalog](../../src/tests/qualification/releases.json) explicitly selects successful resume or rejection for each release.

```sh
python3 -m tools.qualification.qualify releases --output build-results/released-versions
python3 -m tools.qualification.qualify run migration --previous-binary /path/to/previous/kaferry
python3 -m tools.qualification.qualify run migration --previous-binary /path/to/incompatible/kaferry --state-contract reject
```

`releases` downloads native archives/checksums and verifies attestations with `gh`; downloads stay in the ignored cache. Resume checks both binary directions and unsupported metadata rejection. Rejection must occur before destination records change and checks that completed old Avro remains readable with a fresh group. [Upgrade contracts](../releases/v0.10.0.md#upgrade).

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
