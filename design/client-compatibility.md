# Client compatibility — 2026-10-03

## Verified baseline

- Kafka `apache/kafka:3.9.0`, single KRaft broker; `confluent-kafka-go/v2 v2.15.0`, bundled librdkafka `2.15.0`.
- macOS ARM64 with Go 1.27.1; Linux ARM64 with Go 1.26, CGO, bundled static library. Linux AMD64 is a CI target, not locally verified.
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

## Permissions and remaining qualification

Integration runs used an unrestricted plaintext test broker. Source requires read/topic description, group description and offset fetch/commit; destination requires write/topic description. Topic configuration description is optional: denied/unreadable config warns and disables dependent checks. Cluster-ID discovery must succeed. This permission inventory has not been tested against a restricted ACL broker.

Before qualifying additional deployments: restricted ACL/SASL/TLS matrices, additional broker versions, multiple brokers with failover, and sustained load/soak with realistic latency and retention. No broader platform/version qualification is implied.
