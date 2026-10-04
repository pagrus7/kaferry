# Client compatibility — 2026-10-03

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

The two-minute streaming soak verified 74,816 records, sampled commit lag/RSS and graceful shutdown. See [measurements](implementation-results.md#completeness-and-candidate-verification).

Additional broker versions, SCRAM/mTLS/other authentication providers, physical AMD64 deployments, longer production workloads and realistic latency/retention still require deployment-specific qualification. No broader platform/version claim is implied.
