# Client compatibility — 2026-10-08

## Verified baseline

- Kafka `apache/kafka:3.9.0`, independent single-broker KRaft clusters and a three-broker/controller replicated cluster; `confluent-kafka-go/v2 v2.15.0`, bundled librdkafka `2.15.0`.
- macOS ARM64 with Go 1.27.1; Linux ARM64 and AMD64 with Go 1.26, CGO, bundled static library. AMD64 ran under Docker emulation. Hosted CI remains unrun.
- Lowest verified broker version: 3.9.0. Earlier feature introduction dates do not establish support.

## Probe results

| Assumption | Result |
| --- | --- |
| Raw record fidelity | Null/empty/binary keys and values, ordered duplicate/null/empty headers, millisecond timestamps survive |
| Topic IDs | Nonzero IDs through `DescribeTopics`; refuse zero/missing IDs |
| Cooperative ownership | Incremental assign/unassign and synchronous commit inside revocation work; empty assignments must avoid an empty native commit |
| Ownership loss | `AssignmentLost()` is true after exceeding max poll interval; fence outcomes and release without committing |
| Commit timeout | With socket timeout 1 s and broker paused 2.5 s, commit returned successfully after 2.7–3.2 s; socket timeout/Go context are not a total operation deadline |
| Foreign/non-owned progress | Admin fetch retrieves all requested topic partitions independently of ownership |
| Pre-join members | Client IDs and assigned topics are exposed; subscribed topics of unassigned members are not |
| Read-committed lookup | Open transaction yields no timestamp match; explicit read-committed end gives LSO rather than H |
| Transactional tails | Commit/abort markers advance EOF/position and saved cursor; next consumer resumes safely; aborted records remain invisible |
| Non-monotonic timestamps | Lookup returns first qualifying offset; no-match returns END; offset order does not imply timestamp order |
| Out-of-range offset | `auto.offset.reset=error` surfaces a failure rather than resetting |
| Size bounds | Synchronous oversize rejection; key/header batches passed with none/gzip/snappy/lz4/zstd at topic/producer limits |
| Batch rejection | One future timestamp rejects the mixed batch; delivery reports do not identify the guilty record. All asynchronous record failures stop under every SKIP policy |

The [Confluent API](https://docs.confluent.io/platform/current/clients/confluent-kafka-go/index.html) documents asynchronous delivery and cooperative operations; the pinned source and real probes establish behavior here. [Native properties](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html) remain the configuration vocabulary.

## Binding limits

- Producer timestamp conversion uses Unix nanoseconds, with zero delegated to the producer clock. Reject known unpreservable CreateTime timestamps before submission; a known LogAppendTime destination receives an unset timestamp and restamps it.
- Header name conversion uses C strings; embedded NUL suffixes disappear on Kafka consumption. Kafka endpoints require names without NUL. Files preserve arbitrary names; file→Kafka rejects known NUL names.
- The subscription guard uses the available client-ID/assignment checks and repeats progress validation at positioning. No protocol extension/client fork was introduced. This does not verify unassigned subscriptions.
- A ten-second write-drain budget does not bound blocking commits, filesystem calls, or native close. Never launch a commit goroutine and release ownership while it may still succeed.

## Permissions and deployment qualification

The disposable qualification cluster uses SASL/PLAIN over TLS with a generated CA. Its copier has only source-topic READ, destination-topic WRITE and prefixed-group READ ACLs (Kafka grants the associated DESCRIBE operations). Cluster-ID discovery, group inspection, offset fetch/commit and copying passed. Denied topic configuration description warned and disabled dependent checks. Wrong credentials, an unauthorized identity and an untrusted CA failed under `ON_ERROR=SKIP`, without exposing the supplied passwords in command diagnostics.

A destination-leader broker/controller stop passed with replication factor 3 and minimum ISR 2: 64 records retained order/bytes/timestamp, shutdown succeeded and committed all 64. Fixture clients must tolerate transient connections to the stopped broker; a stale admin-offset request is not a useful failover deadline. Internal broker/controller listeners are private Docker plaintext listeners with an anonymous superuser; this fixture does not qualify encrypted internal traffic.

Load/soak measurements belong in build results. Reproduce with `make load` and `KFWD_SOAK_DURATION=30m make soak`; generated reports and logs should be retained as CI artifacts.

Additional broker versions, SCRAM/mTLS/other authentication providers, physical AMD64 deployments, longer production workloads and realistic latency/retention still require deployment-specific qualification. No broader platform/version claim is implied.

## Native coordinator timeout defect observed — 2026-10-04

Fresh security qualification exposed an `AdminClient.Close` hang after repeatedly cancelled coordinator requests, then a native assertion abort (`rd_kafka_enq_once_del_source_return`) with four-second admin deadlines. The abort run's intermediate fixture source was not archived; its logs and recorded timeout changes remain the available evidence.

Follow-up stock mock tests reproduced the abort in both pinned 2.15.0 and official [2.16.0](https://github.com/confluentinc/confluent-kafka-go/releases/tag/v2.16.0): request group inspection or offsets with a 100 ms native deadline, restore brokers after 500 ms. Guard Malloc and native samples establish use-after-free in the response callback: native timeout releases the owner; late coordinator discovery releases the last reference, then queues a reply with the freed pointer. A longer native deadline and metadata-only controls exit cleanly; Go cancellation alone is not the trigger. The same defect can spin indefinitely on macOS and block Close. Clean runs do not establish safety.

Opus 5.5 at maximum effort also reproduced a leaked coordinator-request reference when native retry is refused during bootstrap decommissioning. A leaked broker reference could explain another Close hang, but that hang has not been reproduced. The original cold-fixture hang has no native stacks and remains unidentified. [Issue 4605](https://github.com/confluentinc/librdkafka/issues/4605) and [proposal 5397](https://github.com/confluentinc/librdkafka/pull/5397) concern the callback defect; neither a reference-order fix nor the proposal is qualified here. Upgrading to official 2.16.0 does not fix the reproduced defect.

Startup still uses `Inspector.CheckMembers` (`DescribeConsumerGroups`) and `Inspector.Progress` (`ListConsumerGroupOffsets`). Their ten-second Go contexts do not cancel native requests. Assignment callbacks use joined consumer `Committed` plus metadata-only identity discovery, avoiding coordinator-admin calls during copying and rebalances. The security fixture's Java coordinator warmup permits ACL/failover testing; it is not a production remedy.

The user elected to release v0.1.0 with these unfixed native limitations. Proposed long native deadlines, bounded read-only inspector Close, process isolation and native fixes are deferred; none is implemented. Startup checks, permissions and the official pinned client remain unchanged. This release does not establish reliable recovery from coordinator errors or bounded native shutdown.


## Permission baseline and admin inventory — 2026-10-04

For existing topics and a named consumer group under Kafka 3.9 StandardAuthorizer, a normal grouped consumer/nontransactional producer and this tool need the same grants: source topic READ, exact source group READ, destination topic WRITE. READ/WRITE imply DESCRIBE. Source/destination credentials can be scoped separately. Destination DESCRIBE_CONFIGS is optional; no source-config, destination-READ or cluster-DESCRIBE grant is needed by these paths. [Kafka authorization](https://kafka.apache.org/39/security/authorization-and-acls/).

The copier passed a focused SASL/TLS fixture using only those three LITERAL grants and the real candidate CLI; denied-config warning and key/value/null/header/timestamp fidelity were asserted. Parent test: 4.47 s; Go run: 6.059 s; fixture setup/run/cleanup: 22.836 s. Child authentication-negative tests were filtered for this run. A Go overlay changed only the fixture's group ACL; shipped code stayed unchanged. Same-cluster/one-version evidence does not qualify other authorizers or split-cluster credentials.

ONE_OFF needs authorization covering generated `kafka-fwd2-<hex>` group IDs, usually a prefix grant. Named GROUP allows an exact grant. A groupless reader that never commits can omit group READ; this tool's agreed grouped/resumable semantics retain it. Nontransactional idempotence needs only topic WRITE on standard Kafka >=2.8; older/custom authorizers may require cluster IDEMPOTENT_WRITE ([KIP-679](https://cwiki.apache.org/confluence/display/KAFKA/KIP-679%3A+Producer+will+enable+the+strongest+delivery+guarantee+by+default)); older brokers are unqualified here.

### Production

Factory.Open creates one Inspector/admin handle per Kafka endpoint; files create none. All direct runtime admin calls are in `internal/kafka/metadata.go`, constructed through `internal/kafka/clients.go`; `internal/app/engine.go` orchestrates endpoint and progress checks. Interfaces in clients.go/ports.go/factory.go support injection.

| Inspector method / API | Purpose / timing |
| --- | --- |
| NewInspector / DescribeTopics | Startup: require existing topic, nonzero topic ID and partition topology; source and destination |
| ClusterID / rd_kafka_clusterid | Startup: cluster identity for query fingerprint; cached Metadata header, no DescribeCluster RPC |
| currentPartitions / DescribeTopics | Source startup/progress and assignments: detect topic replacement and partition growth |
| Progress / ListConsumerGroupOffsets | Source pre-join: saved offsets and commit metadata for query/topic/window/file-progress checks |
| CheckMembers / DescribeConsumerGroups | Source pre-join: reject foreign active members or assignment to another topic |
| Offsets / ListOffsets | Source positioning: earliest/latest bounds, expired/out-of-range checks and read-committed end resolution |
| Config / DescribeConfigs | Destination startup only: optional topic size and timestamp preflight |
| Close / AdminClient.Close | Release each endpoint's native handle, including startup failure cleanup |

Group progress and membership APIs use the suspect coordinator path. DescribeTopics, DescribeConfigs and ListOffsets use other native admin paths. Consumer operations handle runtime committed-position fetches, commits, timestamp lookup and live watermarks; the producer refreshes destination routing metadata.

### DescribeConfigs choice — pending

It reads destination `max.message.bytes`, timestamp type and before/after timestamp bounds (with legacy difference fallback). Size config sets the producer record/batch ceiling and queue budget. Known LogAppendTime permits unset/restamped timestamps; known CreateTime permits pre-submit age checks under ON_REJECTED_TIMESTAMP.

Removing the call is possible but saves no required ACL. Without it, producer size is the default 1,000,000 or explicit `message.max.bytes`; operators should align that with destination limits. Small valid records can form a broker-rejected oversized batch. Timestamp-bound violations can reject mixed batches; unattributed asynchronous failures always stop, including under SKIP. Unknown LogAppendTime also conservatively rejects SDK-unpreservable timestamps that a known LogAppendTime destination could restamp. Native NUL/header and conversion validations remain.

Inspection is a one-time best-effort check. Changed config or client/broker clock skew can cause disagreement in either direction, including local SKIP of a broker-acceptable timestamp. Codex/Opus recommendation: retain automatic optional inspection, explain fallback behavior; user may choose removal with narrower behavior. No implementation change made. Removing DescribeConfigs would leave the coordinator startup calls.

### Tests

Tests use separate fixture administrators for setup/assertions; these grants are not CLI runtime requirements.

| tests/compat file | Admin purpose, directly or through shared helpers |
| --- | --- |
| client_test.go | Shared client creation/close, topic setup/removal and readiness; config visibility, topic-ID, group/offset, transaction-end and size probes |
| acceptance_test.go | Resume/commit checks, topic recreation, commit seeding, group inspection/deletion and retention truncation |
| churn_test.go | Topic fixtures, group readiness, committed progress and destination-end assertions during churn |
| clusters_test.go | Source/destination fixture admins, cluster IDs and cross-cluster topic lifecycle |
| directions_test.go | Topic fixtures and destination end assertions for file/topic permutations |
| edges_test.go | Foreign metadata/commit seeding, membership/offset assertions and retention truncation |
| fault_test.go | Topic/group readiness around broker outages and ownership loss |
| load_test.go | Topic fixtures and worker/group readiness for bounded load |
| qualification_test.go | TLS/SASL topic/ACL setup, restricted-principal topic visibility, replication/leader discovery and cleanup |
| review_test.go | Topic fixtures and legacy timestamp-setting changes |
| signal_test.go | Topic fixtures, partition creation, group commits and log-end assertions |
| soak_test.go | Source/destination fixture admins, topic lifecycle, group readiness and progress/lag assertions |
| timestamps_test.go | LogAppendTime and timestamp-index fixture configuration |
| transactions_test.go | Topic fixtures and committed-position assertions across transaction/control-record tails |
| workers_test.go | Group member/assignment readiness and shared fixtures |

`internal/kafka/fixtures_test.go` supplies mocked AdminClient/Clients implementations for independent component tests. Test setup can create/alter/delete resources; production uses only the inspection operations listed above.
