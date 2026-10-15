# Client compatibility

Native-client and deployment constraints. See [permissions](../docs/usage.md#permissions), [copy behavior](decisions.md), and [installation](../README.md#install).

## Known issues

When starting a copy from Kafka, Kaferry closes its client on a group coordinator timeout so it can exit with error code `1`. Cleanup can hang; slow cleanup or other coordinator errors can still cause a [librdkafka crash](https://github.com/confluentinc/librdkafka/pull/5397).

**Scope and impact:** these [coordinator requests](#production-admin-inventory) run before Kaferry creates the consumer or opens output. File sources and runs already copying do not send them. The affected process copies nothing and leaves saved progress unchanged, whether startup fails normally, hangs or crashes. Rerun the same query and group after the coordinator recovers.

**Timeout handling:** each request has a 10 s Go deadline and a 20 s librdkafka timeout. The Go deadline returns `context deadline exceeded` without cancelling the native request. Startup begins closing the source admin client immediately, giving cleanup about 10 s before native expiry. Native close has no enforced overall deadline, so this does not bound total startup time.

**Remaining risk:** bundled librdkafka 2.16.0 lacks the upstream use-after-free fix. Cleanup delayed past native expiry can still crash, and other error paths, including dropped connections, are not all verified. Crash frequency in production is unknown.

**Test coverage:** five [isolated mock-cluster cases](../internal/kafka/native_test.go) pass with shortened timeouts: four cover both coordinator requests timing out while brokers stay down or recover after the deadline; one covers offsets succeeding after early recovery. Broker integration warms the coordinator and does not exercise cold startup.

## Tested configurations

| Area | Tested configuration |
| --- | --- |
| Broker | Apache Kafka 3.9.0, KRaft; single-broker and three-broker clusters |
| Client | `confluent-kafka-go/v2 v2.16.0`, bundled librdkafka 2.16.0, CGO |
| Build/test platforms | Linux AMD64 (CI) and macOS ARM64; Go 1.26.9 |
| Client authentication | SASL/PLAIN over TLS |
| Failover | Replication factor 3, minimum ISR 2, destination-leader failure |

Test other brokers, authorizers, authentication mechanisms and deployment workloads before relying on them. Fixtures use plaintext internal broker/controller listeners; encrypted internal traffic is untested.

[CI](https://github.com/pagrus7/kaferry/actions/workflows/ci.yml) and [extended tests](https://github.com/pagrus7/kaferry/actions/workflows/qualification.yml) retain results for 30 days. [Test commands](components.md#test-targets).

## Permissions

Required grants and optional destination access are listed in the [usage guide](../docs/usage.md#permissions). The ACL fixture tests literal topic grants and a prefixed group grant with Kafka 3.9 StandardAuthorizer. READ/WRITE include the related DESCRIBE operations; see the [Kafka ACL reference](https://kafka.apache.org/39/security/authorization-and-acls/).

### Optional destination configuration

Startup attempts `DescribeConfigs` on the destination topic. DESCRIBE_CONFIGS is optional under every error policy: denial or failure logs a warning and disables checks that require the configuration. Checks against client limits still run.

| Setting | Use | Fallback when unreadable |
| --- | --- | --- |
| `max.message.bytes` | Cap producer size and queue budget | Default 1,000,000 or explicit `message.max.bytes`; broker can reject oversized batches |
| `message.timestamp.type` | Known LogAppendTime sends an unset timestamp for broker restamping | Reject timestamps the client cannot preserve, even if the broker could restamp them |
| Timestamp before/after bounds; legacy difference bound | Check CreateTime timestamps before submission, allowing record-level SKIP | Broker rejection stops the run under every policy |

Size preflight is approximate because of framing and compression. Clock skew or changed broker settings can invalidate timestamp preflight, including skipping a record the broker would accept.

## Production admin inventory

[Factory.Open](../internal/kafka/factory.go) creates one admin handle per Kafka endpoint. Files create none. Inspection is implemented in [metadata.go](../internal/kafka/metadata.go).

| API | Purpose / timing | Coordinator path |
| --- | --- | --- |
| DescribeTopics | Startup: require existing topics, nonzero IDs and partition topology. Source assignments: recheck source identity and partitions | No |
| ClusterID (`rd_kafka_clusterid`) | Startup: fingerprint cluster identity from metadata; no DescribeCluster RPC | No |
| DescribeConsumerGroups | Source startup: check active group members and assigned topics | Yes |
| ListConsumerGroupOffsets | Source startup, after membership check: read and validate saved commits | Yes |
| ListOffsets | Source positioning and completion: retained/log-end bounds, read-committed end and the log end including open transactions | No |
| DescribeConfigs | Destination startup: optional size and timestamp checks | No |
| AdminClient.Close | Release handles, including startup-failure cleanup | — |

Assignment uses consumer `Committed` for progress; the consumer also handles commits and timestamp lookup. Completion samples the admin client's read-uncommitted log end, since a read-committed consumer's watermark excludes open transactions. The producer refreshes routing metadata.

Fixture administrators create, alter and delete test resources; those permissions are not CLI requirements.

## Binding limits

- Transient inspection errors can fail startup or assignment; they are not retried. Restart after broker recovery.
- Native commit and close have no enforced overall deadline. The ten-second write-drain limit does not bound total shutdown; use a supervisor stop timeout when a hard limit is required.
- Kafka output uses the destination's timestamp type. Known LogAppendTime restamps records. Otherwise timestamps ≤ 0 or above 9,223,372,036,854 Unix milliseconds cannot be preserved by the Go client and follow the timestamp error policy. Files retain timestamp values and types.
- The client truncates consumed header names at NUL. Source Kafka headers must be NUL-free; lost suffixes cannot be detected or restored. File→Kafka rejects NUL names before publishing; files preserve arbitrary name bytes.
- The group topic check sees assigned partitions, so it cannot check the subscriptions of members with no partitions. [Topic identity checks and limits](decisions.md#identity).
- Consumer transient-error retries have no total deadline.
