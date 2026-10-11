# Client compatibility and permissions

Deployment limits and Kafka access requirements. See [copy behavior](decisions.md) and [installation](../README.md#install).

## Known native defects

**Kaferry is not production-qualified.** Its bundled client has these unresolved limits:

| Gap | Impact |
| --- | --- |
| Coordinator failures during Kafka-source startup | Native code can abort during `DescribeConsumerGroups` or `ListConsumerGroupOffsets` |
| Cleanup after startup coordinator failures | Native close can hang indefinitely |
| Blocking native commits are not bounded by Go contexts or socket timeout | The ten-second write-drain budget does not bound total shutdown |

No reliable mitigation is implemented. File-source runs do not perform startup group inspection.

## Verified baseline

| Area | Tested configuration |
| --- | --- |
| Broker | Apache Kafka 3.9.0, KRaft; single-broker and three-broker clusters |
| Client | `confluent-kafka-go/v2 v2.16.0`, bundled librdkafka 2.16.0, CGO |
| Build/test platforms | Linux AMD64 (CI) and macOS ARM64; Go 1.26.9 |
| Client authentication | SASL/PLAIN over TLS |
| Failover | Replication factor 3, minimum ISR 2, destination-leader failure |

Other brokers, authorizers, authentication mechanisms and deployment workloads require validation. Test fixtures use plaintext internal broker/controller listeners; encrypted internal traffic is untested. Passing fixtures do not establish reliable recovery from the native defects above.

[CI](https://github.com/pagrus7/kaferry/actions/workflows/ci.yml) and [qualification](https://github.com/pagrus7/kaferry/actions/workflows/qualification.yml) retain results for 30 days. [Test commands](components.md#test-targets).

## Permissions

With existing topics and Kafka 3.9 StandardAuthorizer, Kaferry needs the same grants as a grouped consumer and nontransactional idempotent producer:

| Direction | Required grants |
| --- | --- |
| Topic→topic | Source topic READ, source group READ, destination topic WRITE |
| Topic→file | Source topic READ, source group READ |
| File→topic | Destination topic WRITE |
| File→file | None |

READ/WRITE imply the associated DESCRIBE operations. No cluster-DESCRIBE, source-config or destination-READ grant is required. Source and destination credentials can be separate. [Kafka ACL reference](https://kafka.apache.org/39/security/authorization-and-acls/).

A named `GROUP` permits an exact group grant. Kafka-source `GROUP = ONE_OFF` generates `kaferry-<hex>` names; authorize that prefix. The ACL fixture tests literal topic grants and a prefixed group grant. Other authorizers or broker versions may require different grants, including cluster IDEMPOTENT_WRITE for idempotent production.

### Optional destination configuration

Startup attempts `DescribeConfigs` on the destination topic. DESCRIBE_CONFIGS is optional: denial or failure logs a warning and disables checks that require the configuration.

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
| ListOffsets | Source positioning: retained/log-end bounds and read-committed end | No |
| DescribeConfigs | Destination startup: optional size and timestamp checks | No |
| AdminClient.Close | Release handles, including startup-failure cleanup | — |

Assignment uses consumer `Committed` for progress; the consumer also handles commits, timestamp lookup and live watermarks. The producer refreshes routing metadata.

Fixture administrators create, alter and delete test resources; those permissions are not CLI requirements.

## Binding limits

- Kafka output uses the destination's timestamp type. Known LogAppendTime restamps records. Otherwise timestamps ≤ 0 or above 9,223,372,036,854 Unix milliseconds cannot be preserved by the Go client and follow the timestamp error policy. Files retain timestamp values and types.
- The client truncates consumed header names at NUL. Source Kafka headers must be NUL-free; lost suffixes cannot be detected or restored. File→Kafka rejects NUL names before publishing; files preserve arbitrary name bytes.
- Group checks cannot verify subscriptions of unassigned members. [Topic identity checks and limits](decisions.md#identity).
- Consumer transient-error retries have no total deadline.
