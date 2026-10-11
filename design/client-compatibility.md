# Client compatibility and permissions

Deployment limits, minimum permissions and verified client behavior. Last reviewed: 2026-10-10.

## Known native defects

Kaferry ships with unfixed native startup crashes and possible indefinite close hangs. It does not establish reliable recovery from coordinator errors or bounded shutdown.

Symptoms were first observed on 2026-10-04 during fresh security qualification: `AdminClient.Close` hung after repeatedly cancelled coordinator requests, then a native assertion aborted (`rd_kafka_enq_once_del_source_return`) with four-second admin deadlines. The abort fixture's intermediate source was not archived; logs and recorded timeout changes are the available evidence.

Stock mock tests reproduced the abort in pinned 2.15.0 and official [2.16.0](https://github.com/confluentinc/confluent-kafka-go/releases/tag/v2.16.0): request group inspection or offsets with a 100 ms native deadline, then restore brokers after 500 ms. Guard Malloc and native samples establish use-after-free: timeout releases the owner; late coordinator discovery releases the last reference, then queues a reply with the freed pointer. The defect can also spin indefinitely on macOS and block Close. Longer native deadlines and metadata-only controls exited cleanly; Go cancellation alone is not the trigger, and clean runs do not establish safety.

A leaked coordinator-request reference was also reproduced when native retry is refused during bootstrap decommissioning. It could explain another Close hang, but that hang has not been reproduced. The original cold-fixture hang has no native stacks and remains unidentified. [Issue 4605](https://github.com/confluentinc/librdkafka/issues/4605) and [proposal 5397](https://github.com/confluentinc/librdkafka/pull/5397) concern the callback defect; neither a reference-order fix nor the proposal is qualified here. Official 2.16.0 does not fix the reproduced defect.

Startup uses `Inspector.CheckMembers` (`DescribeConsumerGroups`) and `Inspector.Progress` (`ListConsumerGroupOffsets`). Their ten-second Go contexts do not cancel native requests. Assignment callbacks use joined consumer `Committed` plus metadata-only identity discovery, avoiding coordinator-admin calls during copying and rebalances.

Fixture Java coordinator warmup permits compatibility/ACL/failover testing; it is not a production remedy. Long native deadlines, bounded inspector Close, process isolation and native fixes are not implemented. Startup checks, permissions and the official pinned client are unchanged.

## Verified baseline

- Broker: `apache/kafka:3.9.0`, independent single-broker KRaft clusters and a three-broker/controller replicated cluster.
- Client: `confluent-kafka-go/v2 v2.15.0`, bundled librdkafka `2.15.0`.
- macOS ARM64 with Go 1.27.1; Linux ARM64/AMD64 with Go 1.26, CGO and bundled static library. AMD64 was tested under Docker emulation and GitHub hosted runners. See [installation](../README.md#install) for release binaries.
- Lowest verified broker version: 3.9.0. Earlier feature introduction dates do not establish support.

The qualification cluster uses SASL/PLAIN over TLS with a generated CA. Wrong credentials, an unauthorized identity and an untrusted CA failed under `ON_ERROR=SKIP`, without exposing supplied passwords in command diagnostics.

A destination-leader broker/controller stop passed with replication factor 3 and minimum ISR 2: 64 records retained order/bytes/timestamp, shutdown succeeded and committed all 64. Internal broker/controller listeners are private Docker plaintext listeners with an anonymous superuser; this fixture does not qualify encrypted internal traffic.

Additional broker versions, SCRAM/mTLS/other authentication providers, physical AMD64 production deployments, longer production workloads and realistic latency/retention need deployment-specific qualification. The successful fixtures do not qualify the native defect paths above.

[Hosted checks](https://github.com/pagrus7/kaferry/actions/workflows/ci.yml) and [qualification](https://github.com/pagrus7/kaferry/actions/workflows/qualification.yml) retain build artifacts. See [test targets and results](components.md#test-targets) to reproduce load/soak measurements.

## Permissions

For existing topics and a named group under Kafka 3.9 StandardAuthorizer, this tool and a normal grouped consumer/nontransactional producer require the same grants:

| Resource | Required grant |
| --- | --- |
| Source topic | READ |
| Named source group | READ on the exact group |
| Destination topic | WRITE |

READ/WRITE imply the associated DESCRIBE operations. Source/destination credentials can be scoped separately. Destination DESCRIBE_CONFIGS is optional; these paths require no source-config, destination-READ or cluster-DESCRIBE grant. [Kafka authorization](https://kafka.apache.org/39/security/authorization-and-acls/).

CI qualification grants source/destination topic permissions literally and group READ by fixture prefix, then copies with a named group. Cluster-ID discovery, group inspection, offset fetch/commit and copying passed; denied config description warned and disabled dependent checks. A one-off exact-group LITERAL variant passed on 2026-10-04, asserting denied-config fallback and record fidelity, but that variant is not in the repository. Same-cluster/one-version evidence does not qualify other authorizers or split-cluster credentials.

`GROUP = ONE_OFF` needs authorization for generated `kaferry-<hex>` IDs, usually a `kaferry-` prefix grant. This follows the naming rule; generated-ID authorization was not tested by those fixtures. Named `GROUP` permits an exact grant. A groupless reader without commits could omit group READ; this tool's grouped/resumable behavior requires it.

Nontransactional idempotence needs only topic WRITE on standard Kafka >=2.8; older/custom authorizers may require cluster IDEMPOTENT_WRITE ([KIP-679](https://cwiki.apache.org/confluence/display/KAFKA/KIP-679%3A+Producer+will+enable+the+strongest+delivery+guarantee+by+default)). Older brokers are unqualified here.

### Optional destination configuration

Startup best-effort `DescribeConfigs` reads destination size, timestamp type and acceptance bounds. Unreadable configuration warns and disables dependent checks; it adds no required ACL.

| Setting | Used for | Fallback when unreadable |
| --- | --- | --- |
| `max.message.bytes` | Producer size ceiling and queue budget | Default 1,000,000 or explicit `message.max.bytes`; operator aligns it with destination limits |
| `message.timestamp.type` | Known LogAppendTime sends an unset timestamp for restamping | Conservatively reject client-unpreservable timestamps even if the broker could restamp them |
| Timestamp before/after bounds, with legacy difference fallback | CreateTime age validation before submission under `ON_REJECTED_TIMESTAMP` | Broker may reject a mixed batch; asynchronous failures stop under every policy |

Size limits are not strict because of framing/compression; broker batch rejections remain possible even with preflight. Changed configuration or clock skew can make local checks disagree with the broker, including skipping a timestamp the broker would accept. Native NUL/header and timestamp-conversion validation remains.

Removing inspection saves no required ACL and leaves coordinator startup calls in place. The shipped behavior is automatic optional inspection.

## Production admin inventory

[Factory.Open](../internal/kafka/factory.go) creates one Inspector/admin handle per Kafka endpoint; files create none. Runtime admin calls are in [metadata.go](../internal/kafka/metadata.go), constructed through [clients.go](../internal/kafka/clients.go); [Engine](../internal/app/engine.go) orchestrates endpoint/progress checks. Interfaces support injection.

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

Group progress/membership APIs use the suspect coordinator path. DescribeTopics, DescribeConfigs and ListOffsets use other native admin paths. Consumer operations handle runtime committed-position fetches, commits, timestamp lookup and live watermarks; the producer refreshes destination routing metadata.

[Broker tests](../tests/compat) use separate fixture administrators for setup, assertions and cleanup; they can create/alter/delete resources, and their grants are not CLI requirements. [Independent component tests](../internal/kafka/fixtures_test.go) mock AdminClient/Clients; production uses only the inspection operations listed above.

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

- Producer timestamp conversion uses Unix nanoseconds; zero delegates to the producer clock. Unless the destination is known LogAppendTime, timestamps ≤ 0 or beyond the client's range are rejected before acceptance for delivery. Known LogAppendTime sends an unset timestamp and restamps it.
- Header name conversion uses C strings; embedded NUL suffixes disappear on Kafka consumption. Kafka endpoints require names without NUL. Files preserve arbitrary names; file→Kafka rejects known NUL names.
- The subscription guard checks client IDs/assignments and repeats progress validation at positioning. It cannot verify unassigned subscriptions; no protocol extension or client fork is used.
- A ten-second write-drain budget does not bound blocking commits, filesystem calls or native close. Never launch a commit goroutine and release ownership while it may still succeed.
