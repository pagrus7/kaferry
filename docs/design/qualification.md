# Qualification lab

Run the same scripts from a terminal, GitHub Actions or a local scheduler. No agent, chat session or hosted automation is required. [Component tests](../../CONTRIBUTING.md#test-targets) remain the fast development entrypoints; this lab owns disposable broker setup, teardown and qualification evidence.

## Prepare

Use Python 3.11+, the Go version required by `go.mod`, Docker Engine 25+ with Compose 2.20.2+, OpenSSL and Make. Linux and macOS AMD64/ARM64 are supported. Release comparisons also need an authenticated `gh` CLI with repository/attestation read access. Linux race tests need a C compiler. On Linux, install the Python venv package if the distribution packages it separately.

From the repository root:

```sh
make qualification-setup
python3 -m tools.qualification.qualify plan --tier pr
```

If `python3` is an older system interpreter, use `make qualification-setup PYTHON=python3.14` and `python3.14` for the CLI commands. Setup builds Kaferry and installs the independent librdkafka checker into a checkout-local Python virtual environment. Run setup and qualification with the same Python major/minor version. Use `--binary /absolute/path/to/kaferry` to select an existing executable. The default is `bin/kaferry`; qualification does not rebuild the selected executable.

Reserve ports 39092/40092 for ordinary plain brokers, 39192/40192 for dedicated endurance brokers, 41092–43094 for the security listeners and 44088 for Kerberos. The replicated fixture uses the [ports below](#replicated-network-paths). Run one lab coordinator per host, across all checkouts and do not run `make broker` or `make qualification` alongside it. The runner takes a host-wide lock for its fixed ports and Compose names; legacy Make entrypoints do not take that lock. Standalone broker containers use the `kaferry-compat`, `kaferry-endurance`, `kaferry-qualification` and `kaferry-replicated` Compose projects; parallel jobs use `kaferry-lab-<run-id>-<slot>`. Other Docker workloads are outside the lab.

Start with one workload, 8 available CPUs, 16–20 GiB available RAM and 100 GiB free test storage; measure actual usage before increasing load. These are starting allocations, not capacity guarantees. Fixtures cap broker JVM heaps at 512 MiB and use four internal offset/transaction partitions.

## Suite taxonomy

An **operating condition** produces zero duplicates, needs no process restart and leaves partition ownership unchanged. Destination stalls shorter than the delivery timeout, steady backpressure and bursts can be operating conditions. Endurance retains its existing one-second destination stalls.

A **fault** exceeds a delivery or session timeout, changes ownership, kills a process or broker, or allows duplicates. Faults belong in deterministic fault suites or chaos, never endurance. Endurance changes one variable per run: a topology variant must retain the workload, gates and fault exposure.

## Plain replicated smoke

The `replicated` suite owns two plain three-broker clusters on the newest catalog image, RF=3 and `min.insync.replicas=2`:

```sh
python3 -m tools.qualification.qualify run replicated --output build-results/replicated-smoke
```

Complete [preparation](#prepare), including the independent checker. The runner requires at least two Docker CPUs, 8 GiB of runtime memory, 4 GiB free on the broker filesystem and 1 GiB on the evidence filesystem before starting brokers. These allowances include the proxy process; they are smoke allowances, not long-run reservations or protection against other disk users. It verifies that all 25 localhost ports are available and holds the same host lease as the other suites.

The readiness CLI checks use 16–64 MiB Java heaps and set the JVM processor count to one. Broker JVM heaps remain capped at 512 MiB. Readiness requires a topic-list response from Kafka's API within five seconds.

The runner first verifies that each worker path crosses its proxy, as described below. The independent librdkafka checker then requires distinct three-broker clusters, three in-sync replicas per created partition and the topic minimum ISR of two. After creation is acknowledged, each topic has a 30-second setup window for that metadata and configuration to become visible. It checks 96 records through preserved/key routing, capture, clone and replay, including partial completion, resume and completed reruns. Kaferry uses worker 1's path while the checker uses its independent path. A passing receipt covers the fixture's path isolation and healthy replicated copying for the recorded executable; it does not establish Kaferry's broker/network fault recovery, multi-worker behavior or continuous uptime. Only the newest catalog version is accepted.

By default the fixture owns a disposable Docker volume. To use an existing volume, provision it with the label `kaferry.lab.storage=true`, then pass its name:

```sh
python3 -m tools.qualification.qualify run replicated --broker-volume LAB_VOLUME --output /path/to/fresh/evidence
```

`run replicated`, `run fault-replicated` and `run endurance --topology replicated` share this storage layout. Each run gets a fresh UUID directory beneath `kaferry-replicated/<project>/`. See [broker storage](#broker-storage) for external-volume requirements and cleanup guarantees.

`--output` selects evidence and the smoke's archive/checkpoint output; it does not select broker storage. Keep private driver options, server addresses and access configuration outside receipts intended for publication.

### Replicated network paths

The fixture pins [Toxiproxy 2.13.0](https://github.com/Shopify/toxiproxy/releases/tag/v2.13.0) by its multi-platform image digest. Each broker has separate admin, worker 1–3, replication and controller proxies: 36 routes in one disposable proxy container. The runner starts that container and populates its HTTP API before starting Kafka; configuration uses no checkout bind mount. Brokers publish no direct host ports. Client metadata advertises the corresponding worker or admin proxy for every broker. Replication metadata and controller quorum addresses also name their own proxies on the Compose network.

| Path | Source ports | Destination ports |
| --- | --- | --- |
| Independent publisher, oracle and admin | 46092–46094 | 47092–47094 |
| Worker 1 | 46192–46194 | 47192–47194 |
| Worker 2 | 46292–46294 | 47292–47294 |
| Worker 3 | 46392–46394 | 47392–47394 |

All published ports bind to localhost; the control API uses 48074. Replication uses 26092–26094/27092–27094 and controllers use 28092–28094/29092–29094 inside Compose only. Broker health checks connect locally; these checks do not carry copy or replication traffic.

Setup waits until every broker on the producer's route reports that the selected partition has three replicas, all in sync, and advertises the expected endpoints. It initializes the producer ID and opens the leader's produce connection before writing, within the existing case deadline. Setup observations are retained in `proxy-proof.log`.

For each cluster and worker route, the proxy proof bootstraps on broker 1, publishes to a partition led by another broker discovered through metadata, then disables that leader's worker proxy. Publication through the worker must fail while the independent admin path can still publish to the same partition. The diagnostic producer permits cancellation of an uncertain in-flight write and is closed before healing. A fresh producer on the same worker route must then publish successfully. Cancellation alone cannot prove the blocked record was never stored: an independent reader checks the exact three acknowledged records and the final end offset, rejecting any blocked record that reached Kafka. `proxy-events.json` records resolved targets and injection/heal times; `proxy-routes.json` records the configured paths. This catches clients bypassing proxies after metadata discovery.

The Go [fault helper](../../src/tests/system/faults) resolves leaders, group coordinators, an in-sync follower, whole clusters and worker destination/coordinator paths at injection time. Container faults use Docker kill/start, restart, pause/unpause and network disconnect/reconnect, preserving the original network aliases. Route faults disable proxies or apply directional timeout, reset, latency, bandwidth or slow-close toxics. Millisecond values apply to timeout, reset, latency and slow close; bandwidth is KB/s per connection. Empty listener selection addresses all six paths per resolved broker; worker selection addresses only that worker's path. Healing restores the originally resolved container IDs and proxy names, even after ownership moves. Ownership labels and proxy upstream/listen addresses must match the fixture before mutation. Partial injection unwinds earlier mutations; failed healing remains pending and fails the test.

A coordinator-path cut blocks every connection from that worker to the coordinator broker, including colocated fetch or metadata traffic. It does not filter Kafka request types. Container disconnection affects that broker's replication and controller connections as well as clients. The helper supplies portable primitives; this smoke does not exercise every primitive against live brokers or qualify recovery from their combinations. Use the runner so the host lease, startup and teardown remain in force.

## Replicated ownership fault

After [preparation](#prepare), use an existing candidate executable and a fresh evidence directory. This command needs the same exclusive ports, two Docker CPUs, 8 GiB runtime memory and storage allowances as the replicated smoke. Live RSS sampling uses native process task information on macOS; other platforms require `ps` with RSS reporting. Unavailable live RSS fails the case. It owns and cleans up the same fixture and runs all proxy-path proofs first. Optional `--broker-volume LAB_VOLUME` uses the labelled-volume procedure above.

```sh
python3 -m tools.qualification.qualify run fault-replicated --case ownership/zombie-worker --binary bin/kaferry --output build-results/ownership-zombie
```

`ownership/zombie-worker` starts two instances of that exact executable in one group, using separate worker routes. Source and destination topics each have 24 partitions, three replicas and a minimum of two in-sync replicas for writes. After both workers hold their initial assignments, an independent publisher submits one record per source partition every 100 milliseconds; acknowledgement gaps over two seconds fail. Fault timing follows observed group ownership and accepted commits.

A passing receipt requires the coordinator's actual session-expiry observation, independent transfer of worker 1's partitions to worker 2, and accepted peer progress on every transferred partition while both CLI processes remain alive before healing. The fault discards only worker 1's upstream packets to its resolved coordinator; it keeps established TCP connections open until healing removes the toxic and closes them. Dropping upstream heartbeats exposes a live old owner through broker expiry. Closing the route immediately can terminate the CLI before expiry and does not establish this exposure.

The registered session, RPC, periodic-save, delivery and shutdown bounds are six, eight, ten, two and five seconds. SQL and worker configuration consume this catalog. The eight-second RPC deadline is the midpoint between the six-second broker session and the product's existing ten-second periodic-save context. It allows an unresolved request across session expiry without extending the save bound. The independent observer covers the full 31-second sum after healing and a healthy observation afterward. Publishing and saved progress must advance again on every partition after healing. The case has a three-minute context and a four-minute outer Go deadline; these bounds and the existing caps remain fixed.

Scoped broker logging records network-received commits and correlated request/response completions around both CLI lifetimes, including member IDs and generations. Every received selected-CLI commit must have a complete response observation. No accepted stale-generation commit is permitted after transfer. Positive fencing evidence requires either actual stale-generation rejections covering worker 1's former partitions or a legitimate current-generation rejoin with accepted progress on its independently observed new assignment. Traffic absence alone cannot pass, and an absent request is never counted as a broker rejection.

Worker 2 must remain alive and finish successfully. Worker 1 may exit 1 after healing only when the audit proves its late stale-generation save was rejected before its observed exit; the existing product contract makes failed periodic saves fatal. Requiring worker 1 to survive that rejection would add a recovery guarantee the product does not make. Early transport exit, continued blockage or an unexplained error fails the case. Missing or truncated logs, logging failures and unsupported logging configurations also fail. Failed post-injection cleanup retains bounded diagnostic request/console captures with explicit partial-capture errors before restoring original logger levels; those files cannot substitute for passing proof.

Final librdkafka reconciliation checks full retained input and output, including permitted replay duplicates and saved progress. Each CLI must also stay within 512 MiB sampled RSS and its configured 1,024-record/128 MiB uncommitted caps. The output retains `manifest.json`, `replicated-ownership.json`, `ownership-proof.json`, raw broker/CLI logs and the shared observer's checksummed bootstrap and safety samples. Failure or Ctrl-C retains failed evidence and attempts fixture cleanup. A command or source test alone does not qualify an executable; qualification requires its completed passing receipt.

## Broker storage

`run --broker-volume NAME` also supports `auth` and the paired fixtures for `broker`, `fault`, `load`, `soak`, `capture`, `endurance`, `recovery`, `integration` and `migration --state-contract resume`. `parallel --broker-volume NAME` places both jobs' brokers on that volume. Cloud, migration rejection and suites without local brokers reject this option. Default broker storage is unchanged.

```sh
python3 -m tools.qualification.qualify run endurance --duration 1m --broker-volume LAB_VOLUME --output build-results/external-rehearsal
python3 -m tools.qualification.qualify parallel --duration 1m --companion-duration 1m --broker-volume LAB_VOLUME --output build-results/external-pair
python3 -m tools.qualification.qualify run auth --source 4.4.0 --broker-volume LAB_VOLUME --output build-results/external-auth
```

The volume must already exist with label `kaferry.lab.storage=true` and permit directory creation and broker ownership changes to UID/GID 1000. The runner checks the label before starting the initializer or brokers. Paired runs use fresh UUID directories beneath `kaferry-paired/<project>/`, with separate source/destination directories. Authentication uses `kaferry-auth/kaferry-qualification/<run-id>/`, with separate directories for all three brokers' Kafka data and metadata logs. Certificates, keys and client control files stay in the temporary host directory; KDC state stays in its disposable container. The volume is mounted at `/tmp/kaferry-logs`, so existing endurance and combined parallel disk checks measure the selected broker filesystem. `--output` still selects evidence and archive/checkpoint output separately; it does not move host temporary files.

For paired external storage, preflight additionally requires two Docker CPUs and 4 GiB of runtime memory, or four CPUs and 8 GiB for both parallel jobs together. `manifest.json` records required and observed capacity in `storage_capacity`; rejected checks retain `capacity.json`. These planning allowances include two 512 MiB broker heaps per job plus room for broker native memory and clients; they are not measured capacity guarantees. Default paired storage retains its existing disk checks without this CPU/memory preflight.

Authentication with external storage requires two Docker CPUs, 8 GiB of runtime memory, 4 GiB free on the selected broker filesystem and 1 GiB on the evidence filesystem. These are bounded-suite planning allowances, not storage reservations or measured capacity guarantees. Its existing authentication, ACL and leader-failure checks, startup deadline and broker heaps remain unchanged.

Cleanup stops writers, validates the run's project/UUID ownership marker and removes only that run directory. The external volume, peer runs and unrelated files remain. Failed ownership validation or writer shutdown preserves the run directory. Data deletion keeps the marker until the run's other files are removed; a failed deletion leaves the marker and remaining data, without undoing earlier deletions. If final directory removal fails, cleanup attempts to restore the marker and still fails qualification. There is no automatic cleanup retry. The volume driver selects its filesystem; this option does not configure host storage or establish shared-filesystem durability. Archive output still requires the [archive filesystem guarantees](archive.md#layout-and-ownership), including atomic rename without replacement; selecting a broker volume does not relax that contract. Inspect diagnostics before publication and keep private volume names, driver options, addresses and access configuration outside shared evidence.

On a deletion error, `storage-clean.log` retains a metadata-only census of that path, to four levels and 64 KiB. It does not follow symbolic links or read file contents. The census has a two-second deadline and one-second forced-termination allowance. `cleanup-container-state.json` records only service names, states and exit codes, with a separate two-second deadline and 64 KiB limit. Missing tools, failed queries, timeouts and truncated observations are explicit. These partial observations cannot establish the cause of a filesystem error or the absence of open kernel references. Diagnostics do not retry deletion or change the failed qualification result.

## Replicated broker faults

The five broker cases use the same [preparation](#prepare), exclusive fixture and independent saved-prefix observer as the ownership case. Each starts one selected CLI process and steady source publication across 24 RF=3 partitions with `min.insync.replicas=2`. Choose one case for a short run:

```sh
python3 -m tools.qualification.qualify run fault-replicated --case broker/source-leader --binary bin/kaferry --output build-results/source-leader
```

| Case | Required observation |
| --- | --- |
| `broker/source-leader` | Kill the source partition's leader resolved at injection; observe a replacement leader and continued publication before restoring the original broker. |
| `broker/destination-leader` | Kill the resolved destination leader; observe replacement leadership while input continues, then restore the broker and require saved progress. |
| `broker/rolling-source` | Restart source brokers 1–3 one at a time; observe broker health, all 24 ISR sets at three replicas and advancing saved progress before the next restart. |
| `broker/rolling-destination` | Apply the same sequence to the destination cluster. |
| `broker/destination-isr-loss` | Cut only the resolved destination leader's replication listener, leaving all controllers running. Select a partition on that same broker with actual ISR=1 below minISR=2 and active unacknowledged input; retain that partition for stall, saved-prefix and same-process recovery checks. |

One delayed source leader leaves at most one publication outstanding per partition; healthy partitions continue receiving offers every 100 milliseconds, and offer-schedule gaps over two seconds fail. Every offer must finish successfully before final reconciliation. A source acknowledgement timeout fails the fixture. Source records retain unique full identities. Broker faults can cause the existing fail-fast source, save or delivery errors; the leader and rolling cases record any resulting exit 1 and restart with the same executable, SQL, group and worker route after broker recovery. The ISR case permits no process restart.

The dedicated broker publisher uses the SDK's unlimited safe record-retry policy inside its configured delivery deadline. Idempotence remains enabled; permanent errors still fail, and every offered record must be acknowledged exactly once at the source. The shared fixture and zombie publisher retain their separate settings.

The case retains the original injection role and route, then freezes `isr_partition` only after observing ISR=1 on the same pre-fault leader, positive source-acknowledgement backlog beyond destination/saved positions and unresolved CLI work. Stall and recovery checks use that same partition; independent prefix, duplicate and final reconciliation checks still cover all 24 partitions.

The pinned SDK's API-negotiation EOF can accompany destination fail-fast delivery errors during broker loss. The harness permits that exact diagnostic only after healthy plaintext copying, during an affected destination kill/restart, with one terminal diagnostic per unconfirmed write in the final copy summary. A source kill or rolling restart can also permit one exact EOF labelled `poll source`, `save progress` or their nested combination, with every submitted write confirmed. This requires the same healthy worker lifetime, immutable log and sole-Wait evidence, no signal or driver stop, matching resolved targets and timeout catalog, and an exit strictly inside the physical source fault. The label identifies the failed operation, not the RPC cluster or cause; source polling can also validate destination metadata. Exact final lifetime caps, restart recovery and all independent safety checks remain required. Bare EOFs without failed destination writes, startup, TLS/configuration errors, unknown or compound terminal text and exits outside the eligible fault fail qualification.

The proxy-route proof retains bounded original SDK response/transport diagnostics and uses native protocol retries with a 10-millisecond metadata minimum age inside its two-second delivery bound. An unsuccessful healthy proof write still fails the run. The proof closes the cut producer before healing and requires exactly the three expected records.

Broker cases explicitly configure six-second sessions, eight-second source RPCs, the product's ten-second periodic-save context, 60-second delivery and five-second shutdown bounds. They verify the fixture's existing 30-second replica-lag timeout. Each complete post-heal observation covers their 119-second sum and a healthy observation afterward. The zombie case retains its separate 31-second window and existing deadlines. Omitting `--case` selects all eleven implemented replicated fault cases, including the network cases below; the runner sums their Go allowances instead of applying a single-case deadline. CI runs each selected case in its own 15-minute job and retains the separate healthy smoke artifact.

Each case retains resolved targets, start/heal times, actual timeout settings, observed transitions and every worker's exit/restart and resource measurements in `broker-CASE/replicated-broker.json`, plus raw CLI logs and shared independent observations. Passing requires exact final source counts and saved positions, full-record reconciliation, duplicate attribution to the observed fault windows, at most 512 MiB sampled CLI RSS and observed current work within the configured 1,024-record/128 MiB caps. Normal worker exits also require exact lifetime highwater from a complete final copy summary.

Post-fault broker recovery requires a running owned container and a fresh all-topics Metadata response pinned to that broker on the independent ADMIN route. Docker inspection and all selected API requests share the existing two-second query context and 60-second recovery gate. The same fresh responses must satisfy each caller's ISR condition; saved progress and all independent safety checks remain required. Receipts bind the resolved target, cluster identity, actual ADMIN connection, request/response times and bounded response content. Docker health remains diagnostic. This criterion does not measure the INTERNAL worker/replication path or JVM probe performance; initial Compose `up --wait` and its five-second topic-list health probe remain required.

`broker-polls.json` retains the latest 128 query/condition observations, including partial topology, query errors and selected Docker health details, with an omitted-poll count. Health details include container start time, rendered probe settings and the latest five probe results with timestamps, exit codes and bounded output. Malformed, unavailable and truncated observations are explicit. Failed runs also retain the latest bounded source-publisher SDK response/transport diagnostics in the suite log. These diagnostics do not substitute for a missing passing observation or extend any deadline.

The documented forced shutdown can call `os.Exit(1)` before the final summary while finishing writes or saving progress. Only one exact diagnostic for either phase, exit 1 without a driver stop or signal, and positive healthy pre-fault copying by that same worker permit missing lifetime peaks. The exit must be observed during a registered source or destination kill/restart or its existing recovery window. The phase identifies the blocked shutdown operation; it does not identify an RPC cluster or the cause of the failure. The ISR case still forbids restart.

Before checking a completed worker's summary, the harness writes immutable `worker-N.lifecycle.json` and `worker-N.exited.cli.log` evidence. These bind the sole `Wait` completion time, exit status, termination signal, prior driver signal intent, raw terminal text, log hash, current lifetime/binary identity and active fault timing. The pre-fault observation includes that worker's complete progress-log prefix and hash. Unsupported exits and unreadable logs retain their failure evidence. The collector verifies these files and bindings.

A recognized forced-copy exit records `lifetime_highwater_available=false`, `unavailable_highwater_phase`, null exact peaks and separately named sampled current-work maxima. `exact_lifetime_work_caps_qualified=false` excludes that lifetime from exact-cap qualification. No exact lifetime work-cap claim follows from those samples. RSS sampling, observed current-work checks and every independent no-loss, prefix and duplicate check remain required. Other missing summaries, intentional stops, stale worker baselines, unknown or compound terminal text and malformed evidence fail. These local clusters share one host. A receipt covers the recorded broker failures and replication paths; physical machine or zone loss requires a separate deployment.

## Replicated worker network faults

Use the same [preparation](#prepare), selected executable and exclusive replicated fixture as the broker cases. Each case publishes one record per partition every 100 milliseconds across 24 RF=3/minISR=2 partitions. Admin, publisher and continuous librdkafka observer connections use independent routes throughout; only worker 1's destination paths are changed.

```sh
python3 -m tools.qualification.qualify run fault-replicated --case network/destination-outage --binary bin/kaferry --output build-results/destination-outage
```

| Case | Required exposure |
| --- | --- |
| `network/destination-outage` | Disable all three worker destination proxies for the configured 60-second delivery timeout plus the six-second session interval. Observe the selected CLI's fail-fast exit, then heal and explicitly restart with the same executable, SQL, group and worker route. |
| `network/destination-leaders-outage` | Resolve destination partition 0's current leader and disable only its worker proxy for six seconds. Every partition led by that broker must stall; independently observed output and saved progress must advance on the remaining partitions. |
| `network/latency` | Add 500 ms upstream latency to all three worker destination routes for six seconds. Require unresolved CLI writes and at least 25 more records of independently observed output backlog than the healthy baseline. |
| `network/bandwidth` | Limit each worker destination connection to 32 KB/s upstream for six seconds. Require increased backlog, unresolved writes and actual output throughput below 75% of the healthy baseline. |
| `network/slow-close` | Add a downstream 60-second close delay on all worker destination routes. An explicit auxiliary one-millisecond upstream timeout supplies backend EOF to the same frozen routes, including existing CLI connections. Require stalled output and unresolved CLI writes; a separate real TCP connection on one of those routes must retain EOF for at least six seconds and release it after removal. |

Slow close measures this explicit EOF stimulus and the worker's bounded copying and recovery around it. Creating a toxic alone cannot pass. The auxiliary timeout is retained separately; the scenario does not isolate slow close from the connection interruption that activates it. The other four cases have no auxiliary stimulus. Only the whole-destination outage permits a process restart; the other cases require the same CLI process to survive.

Injection follows nonempty independently observed saved prefixes and a six-second healthy throughput measurement. Healthy output and independent source acknowledgements must sustain at least 95% of the scheduled 240 records/second. Source acknowledgement gaps over two seconds fail. After removal, every partition must save again, output backlog must return to at most 24 records, and another six-second output measurement must reach at least 95% of the healthy baseline. All six-second measurement and hold intervals consume the actual configured session setting; the outage and slow-close delays consume the actual configured delivery setting.

Network cases retain the broker cases' six-second session, eight-second source RPC, ten-second periodic-save, 60-second delivery and five-second shutdown settings. Their independent post-heal observation covers the complete 89-second sum and a healthy sample afterward. Each has a 269-second case context, a 329-second Go deadline and a 389-second outer deadline. Selecting all eleven replicated cases sums their individual Go allowances to 3,680 seconds, plus 60 seconds for the outer runner; selecting one case uses only that case's allowance. Legacy fault and zombie deadlines stay unchanged.

Each `network-CASE/replicated-network.json` retains the resolved worker proxies and owned containers, actual injection/heal timestamps, healthy/exposed/recovered observations, affected and unaffected partition IDs, connection stimulus where required, source publications, worker exits/restarts and resource measurements. The receipt requires final independent librdkafka full-record reconciliation, the continuous saved-prefix observer and its duplicate-window rule, at most 512 MiB sampled CLI RSS and the configured 1,024-record/128 MiB uncommitted caps. Missing, mismatched or ineffective exposure fails collection. These are deterministic test definitions; a selected executable is qualified only by a completed passing receipt. The shared-host fixture does not establish physical network or zone failure behavior.

## Two concurrent workloads

The `parallel` command holds the host lease for a pair of independent runs. It starts
one uninterrupted Kafka endurance run alongside repeated recovery (default two hours)
or archive capture. Each uses a unique Compose project, separate ports, temporary
files and evidence directory. Ports 45192/45193 and 45292/45293 must be available.

```sh
# Rehearse both jobs before committing the host to a day-long run.
python3 -m tools.qualification.qualify parallel --duration 1m --companion-duration 1m --output build-results/parallel-rehearsal
# Keep Kafka copying for 24 hours while recovery scenarios repeat for two hours.
python3 -m tools.qualification.qualify parallel --output build-results/parallel-24h
# Alternatively run Kafka endurance and archive capture for 24 hours each.
python3 -m tools.qualification.qualify parallel --companion capture --output build-results/parallel-capture-24h
```

Use `--binary` to give both jobs the same candidate executable. `--duration` controls
Kafka publishing; `--companion-duration` controls recovery or capture. Each streaming
job still includes its ten-second drain. The summary `parallel.json` retains both
manifests and the declared co-running workloads. Failure or Ctrl-C cancels the peer,
preserves both receipts and tears down only this pair's projects. Existing output
paths are refused. Other standalone suites, security fixtures and legacy Make commands
must not run alongside the pair. A short paired rehearsal also runs in PR CI when the
parallel runner, its workflow or relevant fault/soak tests change.

Admission checks the **combined** full disk allowance on both jobs' broker filesystems,
the host temporary filesystem and the evidence filesystem. This is deliberately
conservative when they are separate disks. A 24-hour Kafka run plus recovery needs
about 278 GiB; the archive alternative reserves additional input/output space. The
checks cannot reserve space against unrelated disk users. `--broker-volume` selects
[external broker storage](#broker-storage); `--output` relocates evidence separately. Output must
be writable. A network archive filesystem additionally needs the sync, rename and lock
semantics described in the [archive contract](archive.md).

Parallel execution uses four broker processes and shares the host's CPU, memory and
storage bandwidth. Check the container runtime's memory limit as well as host RAM.
Compare a single-job rehearsal with a paired rehearsal before increasing concurrency;
retain the input-rate and recovery gates in both. Pairing qualifies correctness under
the recorded co-load, not isolated performance. Run comparative benchmarks separately.
The executable's platform determines whether evidence covers native macOS or Linux.

## Compatibility matrix

[The version catalog](../../src/tests/qualification/matrix.json) pins five Apache Kafka images by digest. [The workflow](../../.github/workflows/compatibility.yml) runs the following plans:

| Tier | Broker copies | Authentication fixtures |
| --- | --- | --- |
| `pr` | Oldest/newest, both directions and same-version: four cases | Newest broker |
| `nightly` | Each version to itself: five cases | Newest broker |
| `full` | Every directed source/destination pair: 25 cases | All five versions |

```sh
python3 -m tools.qualification.qualify matrix --tier pr
python3 -m tools.qualification.qualify matrix --tier full --output build-results/full-matrix
```

Every tier also runs archive-capture endurance: the PR tier publishes for one minute; nightly, full and release matrices publish for two minutes. Each adds ten seconds without new input. A matrix runs sequentially locally; CI runs up to six cases in parallel on separate hosted runners. Use `--output` to choose a new evidence directory for each run; existing paths are refused so failed evidence cannot be overwritten.

The broker-version and security Go tests live in `src/tests/compatibility`. They import shared helpers from regular Go files in `src/tests/system`; unrelated system test files are not compiled into the compatibility package. On a PR, the workflow skips the matrix only when the diff is nonempty and every changed path is unrelated documentation, reviewer configuration or an excluded test file. Changes to application code, compatibility tests, shared helpers, broker fixtures, qualification tools, dependencies or workflow configuration run the matrix; unknown paths run it too. The fast PR check compiles the system and compatibility tests with the matrix tags; container-tag tests have their own CI workflow. PR path selection does not affect scheduled, manual or release invocations; scheduled runs may reuse evidence as described below.

Scheduled main runs reuse a suite or matrix tier only when its latest completed attempt passed on the same commit and its test executions are less than 30 days old. Nightly results cannot substitute for the full matrix. Failed or cancelled attempts require retry; skipped runs and rerunning only the aggregate report do not renew evidence age. Missing or unavailable history runs the tests.

The planner's GitHub Actions summary names each suite/tier, says whether it will run or reuse evidence, and links the original execution with its date. Weekly extended tests always include fresh fuzzing; scheduled vulnerability scans and performance A/A calibration also continue on unchanged code. Relevant PR changes, manual invocations and release invocations run their selected suites without scheduled-evidence reuse. These rules reuse scheduled evidence; they do not certify a newly packaged release executable.

Run an individual case:

```sh
python3 -m tools.qualification.qualify run broker --source 3.9.2 --destination 4.4.0
python3 -m tools.qualification.qualify run auth --source 4.4.0
```

The independent Python checker uses librdkafka; Kaferry uses franz-go. It seeds 96 records through all five compression codecs and checks both Kafka partitioning modes plus capture→clone→replay. Each route independently verifies seven records after its partial run, all 96 after resume, and unchanged output after its completed rerun. Python fastavro reads archive records independently of Kaferry. Reconciliation checks unique IDs, bytes, null versus empty, ordered duplicate headers, timestamps and preserved partition IDs. Counts come from consumed records, not Kafka offset differences or Kaferry's counters.

Security fixtures exercise PLAIN, SCRAM-SHA-256/512, signed OAuth client credentials, OAuth token files, Kerberos, server TLS, mTLS, denied access and replicated destination-leader failure. OAuth brokers validate RSA signatures, issuer, audience and expiry against an HTTP JWKS fixture; a live-copy test renews short-lived tokens while the issuer rotates keys. Token-file replacement is tested across invocations. The local KDC covers keytab login; prolonged ticket renewal and deployment-specific identity-provider behavior need separate evidence. Internal broker/controller listeners use plaintext.

Hosted authentication matrix jobs first render the default and external-volume Compose configurations and check all three brokers' data and metadata paths, volume mounts and preserved certificate/KDC configuration. This check starts no containers. Authentication runs capture and validate broker image identities after startup, before running tests. Failed or empty image collection fails qualification. Valid image receipts remain in the manifest if tests or cleanup fail.

These commands define test targets. A configuration is qualified only by a completed passing receipt for its exact source, destination and executable; the catalog alone is not evidence that it passed.

## Other suites

Every command attempts fixture cleanup and returns nonzero on failure or interruption:

```sh
python3 -m tools.qualification.qualify run integration
python3 -m tools.qualification.qualify run fault
python3 -m tools.qualification.qualify run load
python3 -m tools.qualification.qualify run resources
python3 -m tools.qualification.qualify run model
python3 -m tools.qualification.qualify run fuzz
python3 -m tools.qualification.qualify run benchmark
python3 -m tools.qualification.qualify run smoke
```

Failed Compose startup also retains `startup-health.json`: label-verified fixture
container state and healthcheck history captured before teardown. This supplements
`brokers.log` and `brokers-up.log`.

`resources` runs copy, capture, checkpoint and archive-adapter component tests, including injected failures at filesystem durability boundaries. `fault` includes abrupt process death and restart; its independent reconciliation permits replay duplicates only within the observed fault and recovery windows described below. `model` compares randomized ownership, delivery, gap and save sequences against a reference model. The same lifecycle target participates in `make fuzz`; `FUZZ_ITERATIONS` sets its execution budget.

System-test topic helpers admit historical record timestamps and disable time-based retention unless a scenario overrides it. Source truncation cases delete offsets explicitly.

`integration` requires two CLI cases for the [source truncation contract](../reference.md#record-error-policies). A protocol fixture rejects an observed source fetch with an out-of-range error; the running CLI must fail explicitly without output or progress beyond its saved initial position. A real-broker case first exits the CLI at observed saved progress, then deletes unread offsets. Restart must copy exactly the retained records, save their end and report the old position, log start and offset gap. These cases distinguish broker-error handling from reported restart skips; they do not measure retention scheduling or claim recovery of deleted records.

### Load workload profiles

`load` runs the existing bounded Kafka and archive copies plus these Kafka profiles. They reuse the streaming record fixture and verifier, with four source and five destination partitions and key routing. Each profile copies 3,200 records; near-limit uses 256 larger records. The source producer buffers at most 64 records. Every profile requires exact counts, matching submission/receipt fingerprints, no duplicates or corruption, preserved ordered headers and timestamps, expected key routing, and the configured uncommitted-record/byte caps.

| Profile | Input |
| --- | --- |
| `near-limit-v1` | Deterministic high-entropy values of 999,000–999,127 bytes, with source and destination `max.message.bytes` set to 1,000,000. The remaining space accommodates record/batch framing. |
| `hot-keys-v1` | Zipf keys over 257 ranks, exponent 1.25 and offset 1; deterministic PCG seed 42 with each record ID. No null keys. |
| `headers-v1` | 64 ordered headers with repeated names, null and empty values, and 256-byte values on the remaining 62 headers. |
| `tombstones-v1` | 32 cyclic non-null keys and a null value every fifth record. A header identifies tombstones independently of their null values. Both topics use compaction with a ten-minute minimum compaction lag and tombstone retention. Current record timestamps keep every version available for bounded reconciliation; this case does not test deletion by the cleaner. |
| `source-none-v1`, `source-gzip-v1`, `source-snappy-v1`, `source-lz4-v1`, `source-zstd-v1` | Cyclic keys and repeated-byte values, published with each source compression codec. Source topics retain producer compression. This compressible input is fixed regardless of archive entropy overrides, so successful source-batch observations must account for every record using the named codec. |

Measurements retain a versioned `workload.name` and its payload, key, header, seed, compaction and source-codec parameters. `source_compression_records` records record counts observed in source batches, grouped by codec, for these profiles. The release-results generator validates the complete versioned parameters and rejects unknown metadata. Compare rates only for matching workloads and parameters. These bounded profiles do not change the fixed endurance workload or its gates.

If Docker itself is unavailable, cleanup can fail. The receipt preserves both suite and cleanup errors. After restoring Docker, remove only these lab projects:

```sh
docker compose -f src/tests/compose.yaml -p kaferry-compat --profile cross-cluster down -v --remove-orphans
docker compose -f src/tests/compose.yaml -p kaferry-endurance --profile cross-cluster down -v --remove-orphans
KAFERRY_QUAL_CERT_DIR=/tmp docker compose -p kaferry-qualification -f src/tests/qualification/compose.yaml down -v --remove-orphans
```

The temporary certificate path satisfies Compose interpolation during teardown; it does not create new brokers. Restarting Docker can interrupt other workloads and is outside the lab's automatic cleanup.

For a stopped replicated run, read its project and run ID from its manifest. When using an external volume, first set `KAFERRY_REPLICATED_BROKER_VOLUME` to the same volume name and `KAFERRY_REPLICATED_VOLUME_EXTERNAL=true`; leave both unset for the default disposable volume. Stop writers before checking the marker and removing their data:

```sh
(
  set -eu
  project=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["fixture_project"])' build-results/replicated-smoke/manifest.json)
  KAFERRY_REPLICATED_RUN_ID=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["fixture_storage"]["run_id"])' build-results/replicated-smoke/manifest.json)
  export KAFERRY_REPLICATED_RUN_ID
  storage_script=$(cat src/tests/replicated/storage.sh)
  docker compose -f src/tests/replicated/compose.yaml -p "$project" --profile replicated stop --timeout 10
  docker compose -f src/tests/replicated/compose.yaml -p "$project" --profile replicated run --rm --no-deps --entrypoint /bin/sh storage -ec "$storage_script" storage clean
  docker compose -f src/tests/replicated/compose.yaml -p "$project" --profile replicated down -v --remove-orphans
)
```

If marker validation fails, preserve the volume and investigate; do not continue with volume removal.

For a parallel run without `--broker-volume`, use the exact `fixture_project` from each child manifest. For example,
clean up the endurance job below, then repeat with the `recovery/manifest.json` or
`capture/manifest.json` from that same run. Check that the run has stopped first.

```sh
project=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["fixture_project"])' build-results/parallel-24h/endurance/manifest.json)
docker compose -f src/tests/compose.yaml -p "$project" --profile cross-cluster down -v --remove-orphans
```

For paired or authentication brokers started with `--broker-volume`, restore that volume name and the run identity before cleanup. Select that run's manifest below; use the child manifest for each parallel job. Stop writers before deleting the owned directory, and preserve storage if any command fails:

```sh
(
  set -eu
  manifest=build-results/external-rehearsal/manifest.json
  export KAFERRY_BROKER_VOLUME=LAB_VOLUME
  project=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["fixture_project"])' "$manifest")
  KAFERRY_BROKER_RUN_ID=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["fixture_storage"]["run_id"])' "$manifest")
  KAFERRY_SOURCE_IMAGE=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["requested_images"][0])' "$manifest")
  export KAFERRY_BROKER_RUN_ID KAFERRY_SOURCE_IMAGE
  suite=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["suite"])' "$manifest")
  if [ "$suite" = auth ]; then
    export KAFERRY_AUTH_IMAGE="$KAFERRY_SOURCE_IMAGE" KAFERRY_QUAL_CERT_DIR=/tmp
    set -- -f src/tests/qualification/compose.yaml -f src/tests/qualification/broker-storage.compose.yaml
  else
    set -- -f src/tests/compose.yaml -f src/tests/broker-storage.compose.yaml --profile cross-cluster
  fi
  storage_script=$(cat src/tests/replicated/storage.sh)
  docker compose "$@" -p "$project" stop --timeout 10
  docker compose "$@" -p "$project" run --rm --no-deps --entrypoint /bin/sh storage -ec "$storage_script" storage clean
  docker compose "$@" -p "$project" down --remove-orphans
)
```

For authentication, `/tmp` satisfies certificate-path interpolation during teardown; it does not regenerate credentials or start brokers.

## Paired performance

For paired performance, use the existing standalone runner with two source revisions:

```sh
sh tools/qualification/performance_run.sh origin/main HEAD --pairs 6 --archives
```

The wrapper tears down its disposable brokers after success, failure or interruption. Compare measurements on the same quiet machine; shared-runner throughput has no regression gate.

[Performance runs](../../.github/workflows/performance.yml) are separate from required PR checks. They reuse Kafka, disabled-metrics Avro file-copy, and optional archive capture/replay/file-copy assertions, with larger inputs, a warm-up per binary and six interleaved pairs, alternating which binary runs first on one runner. Summaries show median head/base throughput and the pair range; raw metrics, binary/toolchain identities and broker images stay in the `performance` artifact for 30 days. Kafka input is capped at 160,000 records; file/archive input at 200,000. Archive comparisons require Avro support in both revisions.

Nightly runs start with A/A calibration (the same binary on both sides). Review at least 20 calibration runs before setting the repository variable `PERFORMANCE_MODE=comparison`; this enables changed-main comparisons against the last retained compatible scheduled measurement. No compatible baseline starts another calibration. Expired data requires a fresh baseline. Manual dispatch accepts a base revision; adding `ci:performance` opts a PR into the configured mode. In comparison mode, PRs compare against the merge base. No performance threshold blocks merging; choose warnings only after measuring A/A noise.

## Recovery and endurance coverage

Safety assertions run before long qualification. `make check` exercises confirmed-prefix
tracking, stale deliveries, partial saves, shutdown deadlines and injected archive I/O
failures with the race detector. `make fault` adds real-process recovery on the disposable
source/destination brokers; start them with `make broker`. Code PRs run this fault suite.
Run `make qualification-setup` first, with the same Python major/minor version used
for `make fault`, `run fault` or `run recovery`. Missing independent-checker dependencies
fail before qualification starts; they do not skip reconciliation.

Before allowing a copier to start, the independent observer initializes its broker and
coordinator connections within the existing ready deadline. `bootstrap.jsonl` retains
each setup attempt, error and duration separately from measured qualification. Only
transient driver timeouts, transport failures and coordinator discovery failures may
repeat during setup. Readiness requires an exact independent fixed-cut baseline with
fresh saved group progress and an empty destination; source records may already exist.
Unsafe progress or changed records fail immediately. Measured qualification begins
after this baseline, and a failed undeclared run-time query remains fatal on its first
attempt. Query deadlines remain one second. Snapshot consumers use zero broker-side
fetch wait with the one-second socket budget and pause after each fixed cut.

The blocked-output cases first save a prefix on two partitions, pause the destination,
and wait for the copier to report new submissions. They then kill it, signal it, wait
for its delivery timeout, or transfer ownership. Saved offsets must stay at the delivered
prefix. A separate case keeps output healthy, establishes confirmed but unsaved records,
then pauses the source and requires shutdown to time out while saving progress. Restart must recover every retained
record, and every received copy is checked, including permitted duplicates. The small
byte caps exercise backpressure without a large dataset. CLI logs go to temporary files
rather than accumulating in the test process's memory.

Every blocked-output case, crash/resume and rebalance-churn run also starts a separate
librdkafka consumer for each side. About once per second, and at explicit before/after
fault observations, it reads saved group positions, captures destination end offsets,
then consumes only through those captured ends. For every source partition, the saved
position must be at most the contiguous prefix independently matched at the destination.
An observer catching up to the captured ends avoids comparing current saves against a
stale verifier. Output appended beyond those ends cannot repair an unsafe sample.
These are ordered cross-cluster observations, not atomic snapshots: a transient unsafe
save repaired before the destination snapshot can be missed. Capture spans, idle intervals,
per-partition sample counts and first/last observations are retained; the one-second cadence
is a scheduling target, not an atomic or uninterrupted safety guarantee. Intervals between
samples are outside the sampled claim. Broker outages that prevent
an observation produce explicit gaps, never passing samples. Final observation must be
available. This Kafka-group observer does not cover archive checkpoints.

Final reconciliation independently consumes all retained source records and all output
through the final destination ends after fixture writers stop. It checks full key/value
bytes, null versus empty, ordered headers, timestamps and preserved partition IDs for
every copy. Fixture records must have unique full identities within their partition;
ambiguous identical source records fail rather than disguising loss as replay. Record
identities and copy history live in a disk-backed index, rather than growing Python heap
state across repeated recovery cycles. The blocking-commit adapter probe also records
observations, but its empty topic establishes a commit-deadline claim only; it cannot
establish a no-loss claim for a candidate executable.

Each fault records its resolved target, successful injection observations and observed
heal. Duplicate attribution uses per-partition destination offset cuts captured before
injection and after recovery, so independent-consumer lag cannot move an old delivery
inside a window. The recovery allowance is the sum of the scenario's configured session,
delivery and shutdown timeout budgets: expiration, redelivery and stopping can occur in
sequence. The adapter-only probe uses its configured session budget. Both receipts of
every consecutive duplicate pair outside all such windows fail qualification. After
fixture writers stop, independent consumption and sampling continue through the complete
recovery allowance, followed by a healthy post-recovery observation and final reconciliation.
A shortened or interrupted observation cannot qualify. These windows conservatively include the pre-injection
offset observation boundary, not an independently measurable broker acknowledgement time.

`fault-results.json` maps stable case IDs to checksummed `samples.jsonl` observations,
per-partition coverage, fault events, independent final reconciliation and binary/source
identity. Repeated recovery includes a receipt for every successfully injected case in
every completed cycle. Successful setup attempts that miss the source-outage injection
do not produce a qualified case. Evidence collection rejects missing cases, failed
reconciliation, unsafe observations, missing injection/heal proof and binary mismatches.
Each case must be registered in the shared timeout catalog; producer windows and
receipt validation both require that case's exact timeout keys and values. Adding a
scenario requires registering the budgets used by its actual fixture configuration.
`make fault` retains this evidence under `build-results/`; standalone runs use `--output`.

The `fault` Go process allowance preserves its existing 600 seconds for execution and
adds 473 seconds for the required recovery horizons. The shared fixture timeout catalog
supplies both actual scenario settings and observation windows: churn 135 seconds,
crash 96, blocking commit 45, and the five recovery cases 39, 39, 11, 69 and 39. Churn's
three ownership windows overlap, so only its final 135-second horizon extends the case.
The resulting outer budget is 1,073 seconds; individual CLI, fault and query deadlines
stay unchanged. Repeated `recovery` keeps its requested duration plus the existing
600-second allowance for completing its final cycle.

Run repeated recovery separately from uninterrupted uptime:

```sh
python3 -m tools.qualification.qualify run recovery --duration 2h --output build-results/recovery-2h
```

`recovery` defaults to two hours and accepts one minute through 24 hours. It repeats
complete cycles of the same five cases with fresh topics and processes. A final cycle
may finish after the requested duration; the runner allows ten extra minutes. Each
completed subtest releases its fixture resources. `recovery.json` records completed
cycles, elapsed time and failure status; it does not claim continuous process uptime
or a sustained publishing rate. Required broker settings cannot produce a successful
empty run. Short CI cases and long repetitions use the same assertions.

The longer runs below are a recommended local qualification schedule. Hosted scheduled
CI continues to run one fault cycle; it does not launch the two-hour recovery suite.

| Execution | Purpose |
| --- | --- |
| Every code PR: component tests and one fault cycle | Catch unsafe progress, corrupt replay and shutdown regressions quickly |
| Manually or locally scheduled before release: `recovery` for two hours | Exercise repeated failure, reassignment and restart on the tested executable |
| Manually before release: `resources`, `capture` and `auth` | Check archive storage cuts, continuous capture, authentication and replicated failover separately |
| Manually for a release candidate: `endurance` for 24 hours | Keep one copier alive through sustained input and recoverable destination stalls |

Use the exact candidate executable for CLI qualification and retain its digest in each
manifest. Component tests compile the checkout. A recovery pass cannot substitute for
an endurance pass; a shorter rehearsal cannot establish 24-hour uptime. Broker-version
compatibility remains the separate matrix above. Broader failure combinations and
longer runs should follow changes to the relevant adapters or lifecycle behavior.

## Endurance on a persistent host

Keep a process alive continuously for the duration being claimed. Separate short runs do not add up to continuous uptime. The runner accepts publishing durations from one minute through seven days and keeps the process running for ten seconds after the later of the scheduled publishing end and final source submission. Endurance may submit overdue scheduled records during the first five seconds after the publishing boundary. The Go test timeout allows for publishing, the quiet period and cleanup.

Prerequisites: Python 3.11+, Go, Docker and Compose, a host that stays awake, and exclusive use of the selected disposable brokers. Default `endurance` owns the separate `kaferry-endurance` project on localhost ports 39192 and 40192; other plain-broker suites use `kaferry-compat` on 39092 and 40092. The replicated variant uses the [six-broker fixture](#plain-replicated-smoke), its ports, storage checks and cleanup. The runner starts and removes its project and pauses its destination. Do not run another job against that project while it is active. Other host workloads can affect measurements.

```sh
make build
python3 -m tools.qualification.qualify run endurance --output build-results/endurance-24h
# Optional short rehearsal; this does not qualify 24 hours of uptime.
python3 -m tools.qualification.qualify run endurance --duration 1m --output build-results/endurance-rehearsal
# Replicated topology: newest catalog image, RF=3 and minimum ISR=2.
python3 -m tools.qualification.qualify run endurance --topology replicated --output build-results/replicated-endurance-24h
```

For a topology comparison, hold broker versions constant as well: run the single-broker baseline with `--source 4.4.0 --destination 4.4.0`. The replicated fixture accepts only the newest catalog entry; the default single-broker run uses the oldest.

The dedicated `endurance` suite uses the following fixed application workload. Full-duration runs stay out of PR, nightly and weekly matrices; the replicated fixture workflow runs a one-minute variant rehearsal when its relevant paths change. Smoke and rehearsal use separate hosted jobs and the same candidate executable; the rehearsal verifies its digest before use. Short `soak` and `capture` settings remain available. Use a fresh output directory for each run. The runner preserves evidence on failure and removes its disposable brokers in cleanup. Interrupt with Ctrl-C; an interrupted run cannot qualify. A fresh run starts fresh topics and progress rather than resuming an interrupted qualification.

| Setting | Purpose and acceptance |
| --- | --- |
| 24 hours of scheduled publishing, up to five seconds to finish overdue submissions, then ten seconds without new input; one continuous CLI process | Demonstrates this process stays alive for the measured interval. `--duration` accepts one minute through seven days; durations below 24 hours are labeled rehearsals in the manifest. |
| 24 source / 32 destination partitions, `PARTITIONING=KEY` | Exercises assignment, per-partition saved progress and fan-out in one process. These are chosen benchmark counts, not a measured production average. No group scaling or rebalance claim. |
| 200 records/second plus 400 extra records at 10 seconds and every minute afterward | Moderate sustained load, with two seconds of extra input per burst. Scheduling follows elapsed time. Each minute and the final window must acknowledge at least 95% of scheduled input, including bursts; the final window is checked after the five-second grace. Submission debt may not exceed 1,400 records (five seconds plus a burst); every scheduled record must be submitted and acknowledged within five seconds after the publishing boundary. |
| 1,024–5,119 byte values, deterministic high entropy bytes, 257 cyclic keys with every seventh key null | Roughly 3 KiB average; exercises bytes and modest key reuse without compression making the workload trivial. This distribution does not model hot keys, large messages or every application schema. Source writes use no compression. |
| Separate source/destination clusters: single-broker RF=1 by default, or three brokers per side with RF=3/minimum ISR=2 under `--topology replicated` | Isolates destination stalls from source publication. Pause the entire destination for one second at 30 seconds and every minute afterward during publishing; the replicated variant pauses all three destination brokers. After unpause, acknowledged input minus unique receipts must return to at most 20 records within 15 seconds; it must reach zero by the end of the ten-second consume-only period. |
| Fidelity, ordering and saved progress | Require no missing, duplicate, corrupted, unexpected or reordered records and matching submission/receipt hashes. Non-null keys must reach their Kafka key-hash partition; misplaced records count as corrupted. Null keys use sticky placement. Ordering is checked within each source/destination partition pair, not globally across key routes. After shutdown each source partition must have its exact final next offset saved. This successful run requires zero duplicates; Kaferry's at-least-once contract still permits duplicates after a crash or failed progress save. |
| Shutdown and bounded work | SIGINT must exit successfully within 30 seconds after the drain. Final summary counts must match input, with at most 1,024 uncommitted records and 128 MiB. This run does not restart the process or inject source failure. |
| Memory and measurement | Sample CLI RSS once per second: peak at most 512 MiB, final median growth at most 32 MiB over the warm median. Endurance checks growth both just before publishing ends and after drain. Warm median uses samples 11–30; the loaded final median uses the last 20 samples. The idle final median requires five samples after publishing stops; fewer samples fail the run. |

The rate checks make producer/verifier backpressure visible. The fixture uses nonblocking producer admission (rejection fails the run), a 2,048-record producer buffer and up to 512 receipts per verifier poll. Outstanding input and unresolved verification/digest history have an 8,192-ID bound; hitting a gate cannot reset the clock or reduce the target. Rate/debt failures invalidate load even when all records eventually reconcile. The last scheduled batch may be submitted during the five-second boundary grace; no additional input is scheduled then. A slow verifier can invalidate the harness; that failure alone does not establish a Kaferry throughput limit.

`manifest.json` records the workload, topology, exact binary, images, duration and result. `capacity.json` records preflight space. The metrics file's `endurance` object records topology and replication factor, targets, submissions/acknowledgements at the publishing boundary and after grace, minimum window ratio, gate iterations, maximum scheduling debt, stall recoveries, loaded final RSS, and wall time spent submitting, verifying and sampling. Existing metrics retain CLI CPU seconds, sampled lag, RSS and final reconciliation. CPU and operation times are descriptive, with no hardware-independent CPU threshold. Sampling time includes admin queries and RSS collection; verifier time includes polling waits and byte/hash work. These times measure harness occupancy, not the overhead relative to an uninstrumented run. Rate gates reject a run that instrumentation underloads; profiling and an instrumentation A/B comparison remain separate work.

`soak.stalls.jsonl` retains timestamps around the existing ownership checks and pause/unpause commands, the one-second hold deadline and local fixture pause observations. Command completion and cached fixture state do not independently establish broker epochs, ISR membership or consumer ownership. Failed commands remain failed; an incomplete operation has no successful completion claim. This diagnostic file stays in the run directory. CLI logs are collected before temporary files are removed, including on failed runs.

Endurance disables topic age/size retention until reconciliation. Default preflight reserves three times the record count times 5,376 bytes, plus 4 GiB on **each** broker data filesystem: two full maximum-size copies with 50% overhead, with no compression credit and allowance for a shared Docker disk. The replicated variant reserves nine times the same record bytes, plus 4 GiB on its shared broker volume, before starting brokers. The 24-hour workload schedules 17,856,000 records and requires about 272 GiB for the default topology or 809 GiB for RF=3. Evidence needs at least 1 GiB or 8 KiB per publishing second, whichever is greater. These are planning allowances, not guarantees against other disk users; monitor host/Docker capacity during the run. The runner does not prune active records. Record-ID overflow is rejected before intake.

This is application endurance under the recorded topology and load. The replicated variant requires all three replicas in sync and minimum ISR=2 before copying, with the same workload, process and gates as the default. It is neither a saturation benchmark nor evidence of resilience to broker failures or ISR loss. Use `load` for bounded throughput comparisons and `auth` for replicated leader-failure scenarios; neither substitutes for long continuous uptime. A passing rehearsal, an underloaded run or several short runs cannot support a multi-day uptime claim. Claims exclude multiple Kaferry processes, network partitions, ownership changes, authentication overhead and arbitrary payload/key distributions. `parallel` retains its default topology; the replicated variant runs separately. Match a real deployment with separately measured workloads before generalizing these results.

Replicated endurance configures every proxy route enabled and retains only the existing one-second broker stalls. The separate cut/heal proxy proof runs under `run replicated` and `run fault-replicated`.

Archive capture and the smaller Kafka soak remain available:

```sh
python3 -m tools.qualification.qualify run soak --duration 24h --rate 50
python3 -m tools.qualification.qualify run capture --duration 72h --rate 50
```

`soak` and `endurance` verify topic output as it arrives and pauses the separate disposable destination once per minute during publishing. All three modes keep the same copy process alive for ten seconds after the later of the scheduled publishing end and final source submission. After shutdown, Kafka modes consume through the destination's final offsets; `capture` scans all completed archive files. Replay/file-to-file load tests are finite jobs; their repeated execution is not reported as continuous streaming uptime.

Metrics and JSONL samples record submitted input, broker-acknowledged publications, total and unique received records, verified records, pending IDs, duplicates, corruption, unexpected IDs and reordering. Final missing counts are available only after all publications are acknowledged and the final output scan completes; incomplete evidence reports `missing: null`. Failure reports preserve observed counters. Verification stops with partial totals if unresolved ID history exceeds 8,192 entries. Rates include the consume-only period and reconciliation.

Both Kafka and archive verifiers compare complete key/value bytes, ordered headers (including duplicate names and null versus empty values) and timestamps against the fixture. Archive verification also compares source cluster, topic ID, partition and offset. `delivery.integrity` adds SHA-256 evidence from actual submissions and first unique receipts, retaining the byte comparisons. Its `sha256-canonical-records-v1` encoding length-prefixes key/value bytes and ordered header names/values, distinguishes null from empty bytes and includes the header count. Each source partition hashes record fingerprints in logical-ID order; a final SHA-256 combines all source-partition digests in partition order. This permits different arrival orders between destination partitions. Only unresolved receipt digests are buffered, with the same 8,192-ID limit. Metrics and GitHub summaries retain both hashes, hashed counts and pending gaps; `matched` is null until final reconciliation. The digest covers key/value/headers; timestamps, provenance, missing records, duplicates and reordering have separate checks. Archive hashes are completed by the scan after shutdown; their live JSONL snapshots remain unresolved.

For Kafka output, destination lag is acknowledged source records minus unique IDs received by the verifier, bounded below by zero. Destination consumer lag is the sum of destination end offsets minus the verifier's next consumed offsets. JSONL samples retain instantaneous lags; metrics retain sampled peaks and, after reconciliation, final values. Consumer-offset sampling pauses while the destination fixture is paused; record-lag sampling continues. Offset distance is a backlog measure, not an independently verified record count. Archive output has no live destination-lag measurement.

Kafka endurance also records `delivery.observed`: source acknowledgements and first unique destination receipts update a synchronized tracker immediately, matching record IDs even when receipt precedes acknowledgement. `peak_backlog` is the maximum number of acknowledged IDs not yet received at those observation points; it is updated per event rather than sampled. `max_latency_seconds` measures every tracked first receipt from test submission, using the same host's monotonic clock. This includes producer queueing, copying and verifier delay; it does not isolate broker arrival or Kaferry processing time. `timed_receipts` states the measurement count; no receipts gives a null maximum. Completed timing state is released, leaving only unsettled IDs. These observations appear in metrics, JSONL snapshots and the GitHub summary. Absent observation fields mean unmeasured. Broker-offset peaks remain sampled, and archive verification after shutdown does not provide these live observations.

`p95_latency_seconds`, `p99_latency_seconds` and `p999_latency_seconds` report the nearest-rank 95th, 99th and 99.9th percentiles over every timed first receipt. A fixed 432 KiB histogram rounds percentile durations upward by less than 0.1%, capped at the exact maximum; it never samples or drops timings. No receipts gives null percentiles. Small populations can put p99.9 at the maximum. The histogram covers nonnegative 64-bit nanosecond durations, and its size does not grow with uptime.

The verifier releases contiguous record IDs and bounds outstanding streaming input; RSS monitoring retains fixed warm/final windows and a peak. JSONL samples go to disk. Checks cover record fidelity, saved progress, worker work caps, a 512 MiB sampled RSS ceiling and at most 32 MiB final-median RSS growth after warm-up. These measurements do not include Go heap or goroutine profiles, and sampling can miss short peaks.

Start at a moderate rate: 50 records/second with this fixture's roughly 3 KiB average payload is about 40 GB of raw input over 72 hours, before Kafka replicas, destination output and archives. Payloads compress heavily by default. Set `KAFERRY_ARCHIVE_PAYLOAD=random` for deterministic high-entropy values. Bound the test duration/rate to available disk; the script does not prune active records or archives during verification. Source retention must exceed the maximum test backlog or interruption. Record-ID overflow is rejected before intake.

Use a local scheduler such as launchd/cron to invoke these commands against approved commits or release executables, with one coordinator at a time (which may own a parallel pair) and host sleep disabled for the run. No scheduler is installed automatically. Keep public PR execution on disposable hosted runners. GitHub-hosted jobs cannot remain alive for a multi-day run.

### Publish endurance results

Every standalone `endurance` run writes `summary.md` and a `publication/` directory after cleanup, including failed or interrupted runs. The package contains `endurance-results.md`, `endurance-results.json`, `endurance-evidence.tar.gz` and `endurance.SHA256SUMS`. The JSON keeps the complete manifest and measurements; the archive keeps the manifest, metrics, logs and JSONL samples. Missing observations remain unavailable. Short runs are labeled rehearsals. Existing publication directories are refused.

To package an older run, or retry report generation into a new directory:

```sh
python3 -m tools.qualification.endurance_results build-results/endurance-24h --output build-results/endurance-package
```

To publish evidence, run against a verified release executable using `--binary` from the start. After reviewing the package, use an authenticated `gh` CLI with permission to upload release assets:

```sh
python3 -m tools.qualification.endurance_results build-results/endurance-24h --publish vX.Y.Z
```

If you prepared a custom package directory, pass the same `--output` when publishing: `python3 -m tools.qualification.endurance_results build-results/endurance-24h --output build-results/endurance-package --publish vX.Y.Z`.

The command downloads the selected release's platform archive and requires its executable SHA-256 to match the measured binary. It also checks the package checksums and requires a clean fixture checkout. Runs of a newer development executable cannot be published as an older release's evidence. `--repo OWNER/REPO` selects another repository. Failed runs with measured identity can be published and retain their failure status; preflight failures without metrics cannot be uploaded.

Each upload uses platform/date-prefixed asset names and its own checksums; existing assets, tags and release notes remain unchanged. The command prints a link to the release containing the uploaded reports and evidence.

Uploading is explicit; running qualification does not contact GitHub. The existing release-results generator covers release load/soak evidence and does not import this dedicated profile.

## Published-version contracts

[The release catalog](../../src/tests/qualification/releases.json) states the expected contract for each selected release: successful resume or explicit rejection. It is intentionally explicit rather than assuming all older versions share state formats.

```sh
python3 -m tools.qualification.qualify releases --output build-results/released-versions
```

This downloads native release archives and checksums, verifies archive attestations with `gh`, then runs each cataloged contract. Downloads remain in the ignored local cache. A rejected-state case also verifies that completed old Avro records remain readable with a fresh group. Update the catalog when the supported release set changes; the v0.9.0 rejection follows its documented unsupported state contract.

For a binary already downloaded and verified:

```sh
python3 -m tools.qualification.qualify run migration --previous-binary /path/to/previous/kaferry
python3 -m tools.qualification.qualify run migration --previous-binary /path/to/incompatible/kaferry --state-contract reject
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
python3 -m tools.qualification.qualify setup --cloud
python3 -m tools.qualification.qualify run cloud --config /private/path/qualification.json
```

One cluster can qualify source and destination authentication using separate topics. Cross-cluster or cross-region claims require those actual endpoints. The generated credential configuration is private and temporary, outside evidence uploads. CLI error logs are retained; use sandbox credentials and inspect logs before sharing them.

Use a same-VPC client for private MSK access. Keep certification batches short and low-volume; compare total provisioned/serverless pricing, and delete temporary infrastructure even after a failed test. Three brokers on one host demonstrate logical failover, not loss of independent machines or zones. Physical power-loss and particular shared-filesystem durability claims require the corresponding deployment.

## Evidence and release gates

CLI scenarios use the supplied executable; component/model/fuzz/benchmark tests compile checkout code and are labeled separately. Each suite writes `manifest.json` before execution and updates it atomically afterward. It records source revision/dirty state, executable digest/version, requested images, actual broker image evidence, platform/toolchain, required test names, elapsed time and result. `binary_platform` comes from the executable's Go build settings; `execution_platform` identifies the checkout Go target independently. Both use canonical OS/architecture names. Broker copies include independent reconciliation; endurance includes metrics and `soak.samples.jsonl`. Missing credentials, skipped required tests, empty selections and interrupted runs cannot qualify a case.

```sh
python3 -m tools.qualification.qualify report build-results/full-matrix --tier full
```

The report requires every planned case exactly once and the same source revision and executable digest. It writes `qualification.json` and `qualification.md`, returning nonzero for incomplete evidence. Keep failed runs; compare follow-ups in new directories.

The release workflow tests the exact Linux AMD64 archive across the full matrix before publication. It verifies matrix identities against the packaged executable and retains checksummed `compatibility.json`/`compatibility.md` assets alongside the existing qualification summary. Detailed CI logs retain their workflow-specific lifetime. A long-run receipt for a different build does not certify the published binary.
