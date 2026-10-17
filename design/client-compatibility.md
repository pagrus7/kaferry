# Client compatibility

Kafka-client and deployment constraints. See [permissions](../docs/usage.md#permissions), [copy behavior](decisions.md), and [installation](../README.md#install).

## Known issues

Kafka output rejects file timestamps outside ±9,223,372,036,854 Unix milliseconds (roughly 1677–2262) unless the topic is known to use LogAppendTime, which replaces them with broker time. This check cannot catch out-of-range Kafka input: franz-go misreads those timestamps as other dates without warning, affecting copied timestamps and time filters. Ordinary modern timestamps are unaffected. For such sources, use another tool that preserves 64-bit millisecond timestamps. File-to-file copies retain the full signed millisecond value.

The pinned [decoder](https://github.com/twmb/franz-go/blob/v1.22.2/pkg/kgo/source.go#L2438) multiplies milliseconds into a signed nanosecond value. Overflow changes the timestamp before Kaferry receives it. [Kafka 4.x's default future-timestamp bound](https://kafka.apache.org/41/configuration/topic-configs/#message.timestamp.after.max.ms) reduces acceptance of new far-future records; it does not protect existing records, extreme past timestamps or permissive topic settings. Occurrence in production is unknown.

## Tested configurations

| Area | Tested configuration |
| --- | --- |
| Broker | Apache Kafka 3.9.0, KRaft; single-broker and three-broker clusters. Kafka 4.1.2: file paths, resume, cooperative ownership and transactions |
| Runtime client | `franz-go v1.22.2`, pure Go; Confluent is an independent test-fixture client only |
| Broker integration | Linux AMD64 (CI) and macOS ARM64 |
| Release builds and unit tests | Linux AMD64/ARM64 and macOS AMD64/ARM64 |
| Client authentication | SASL/PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512 over TLS; mTLS handshakes in component tests |
| Failover | Replication factor 3, minimum ISR 2, destination-leader failure |

Test other brokers, authorizers and workloads before relying on them. Fixtures use plaintext internal broker/controller listeners; encrypted internal traffic is untested.

[CI](https://github.com/pagrus7/kaferry/actions/workflows/ci.yml) and [extended tests](https://github.com/pagrus7/kaferry/actions/workflows/qualification.yml) retain results for 30 days. [Test commands](components.md#test-targets).

## Permissions

Required grants and optional destination access are listed in the [usage guide](../docs/usage.md#permissions). The Kafka 3.9 StandardAuthorizer fixture uses literal topic grants and a prefixed group grant. READ/WRITE include related DESCRIBE operations; see the [Kafka ACL reference](https://kafka.apache.org/39/security/authorization-and-acls/).

### Optional destination configuration

Startup attempts `DescribeConfigs` on the destination topic. Denial or failure logs a warning and disables checks that need those settings under every error policy. Client-limit checks still run.

| Setting | Use | Fallback when unreadable |
| --- | --- | --- |
| `max.message.bytes` | Set batch ceiling and queue allowance; cap an explicit client limit | Default 1,000,000 or explicit `message.max.bytes`; broker may reject oversized batches |
| `message.timestamp.type` | Known LogAppendTime lets the broker restamp records | Apply client-safe timestamp bounds even if the broker could restamp |
| Timestamp before/after bounds; legacy difference bound | Check CreateTime timestamps before submission, allowing record-level SKIP | Broker rejection stops the run under every policy |

Size preflight counts an uncompressed single-record batch and the client’s 4–5 byte outer framing. Compression, changed settings or clock skew can change broker acceptance. Size preflight can refuse a record up to 5 bytes below the broker limit; timestamp preflight can skip a record the broker would accept. Both refusals support SKIP. Kafka output rejects timestamps outside ±9,223,372,036,854 Unix milliseconds unless known LogAppendTime will restamp them; source decoding has the limit above.

## Kafka requests

[Factory.Open](../internal/franz/factory.go) creates one control client per Kafka endpoint. Files create none. [Inspector](../internal/franz/metadata.go) uses 10 s deadlines for startup and assignment, 2 s for input tip checks and 1 s for output partition checks.

| API | Purpose / timing |
| --- | --- |
| Metadata | Startup: require existing topics, cluster identity, nonzero topic IDs and partitions. Assignment: recheck source identity. Output: refresh partition count |
| DescribeGroups | Source startup: reject incompatible active members and assigned topics |
| OffsetFetch | Startup and assignment: read and validate saved progress; only absent commits use the start rule |
| ListOffsets | Batch retained/log-end bounds, timestamp starts, and stable ends |
| Fetch | Read-committed input; separate bounded gap probes prove compacted/control-only tails without skipping retained records |
| DescribeConfigs | Optional destination size and timestamp preflight |
| OffsetCommit | Consumer member saves confirmed positions with its generation and query metadata |
| JoinGroup / SyncGroup / Heartbeat / LeaveGroup | Ordinary consumer-group membership and ownership |
| Produce / InitProducerId | Idempotent output and delivery acknowledgements |

While partitions are assigned, input checks stable ends once per second. Each check can also request 1 MiB from one partition behind its stable end, including partitions still receiving records. Kafka can return a larger first batch. These probes count against client quotas; a throttled empty reply proves no progress and retries on the next check. When writing, output checks the partition count if at least 30 s have passed since the last check.

Completion samples the log end including open transactions. A stable offset alone never proves that records were consumed: gap probes parse batches and validate CRCs. Fixture administrators create, alter and delete test resources; those permissions are not CLI requirements.

## Operational limits

- Consumer polling can retry temporary network, leader and coordinator errors indefinitely. Required inspections and initial/periodic commits stop the run on error or deadline; writes use the delivery timeout. If Kafka reports that this worker is no longer a group member, it exits 1; rerun to rejoin.
- SIGINT/SIGTERM stop reads, allow up to ten seconds for outstanding writes, then save confirmed progress. The CLI's overall shutdown deadline defaults to 30 s and covers cleanup and blocked I/O. Expiry exits 1 without further cleanup; records after the last saved position can replay. Set `worker.shutdown_timeout` for your environment; a second signal forces termination.
- Leaving the group allows up to 5 s. A failed leave exits 1, even if progress was already saved. Other workers may wait for the session timeout before receiving its partitions. Resume with the same named group.
- If a gap check reports an unknown source topic or partition, the run stops; rerun after the topic is available. Source replica reassignment is untested and may trigger this.
- A delivery timeout can occur after the broker appended the record. The failed record stays unconfirmed and can duplicate on restart; later records cannot advance that partition's saved prefix past it.
- Raw key/value bytes, duplicate headers, NUL/non-UTF-8 header names, nulls and zero/negative timestamps are supported. Kafka's −1 timestamp means missing. Destination LogAppendTime replaces timestamps; retention can remove old records.
- Uncommitted-work caps bound tracked work. Fetch byte limits are soft; client buffers, decompression and one oversized record can add memory. [Connection mappings](decisions.md#connection-properties).
- Group checks see assigned partitions, so they cannot inspect subscriptions of members assigned none. [Identity limits](decisions.md#identity).
