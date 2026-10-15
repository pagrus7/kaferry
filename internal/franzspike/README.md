# franz-go feasibility spike

**Extreme-timestamp corruption is an accepted edge case for this evaluation.** Other migration checks remain open. This branch contains disposable adapters and capability tests; normal builds still use confluent-kafka-go. The experiment uses the existing copy engine, SQL planner and CBOR file components.

## Evidence

Tested locally on macOS ARM64 with Go 1.27.2 (module minimum 1.26), franz-go 1.22.2, kadm 1.19.0 and Apache Kafka 3.9. `deadline_test.go` also uses kfake for withheld replies.

| Area | Observed result |
| --- | --- |
| Raw records within the tested timestamp range | Key/value nulls versus empty bytes, ordered duplicate headers, NUL/non-UTF8 header names and ordinary negative/zero timestamps survive. |
| Saved progress | Public `PreCommitFnContext` retains member/generation fencing and custom metadata. `AdjustFetchOffsetsFn` positions before the first fetch. Departed-member commits are rejected. |
| Identities and routing | Existing topic IDs require unpadded standard Base64, not kadm's padded string. Cluster IDs and keyed Murmur2 partition routing match the native client. |
| Transactions | Read-committed consumption hides aborted data across small fetches. Retained control records advance the delivered cursor. An open transaction holds completion; later data beyond sampled H is excluded. |
| Engine and files | A repeated query resumes from its saved offset. Topic→file→topic works through the real v1 CBOR components. This does not test existing-file recovery. |
| Rebalance | Two real group members transfer a partition while one adapter holds an unsubmitted paused record. Reassignment clears client pauses; a held record does not block other partitions. |
| Permissions | PLAIN/TLS copying, progress commits and metadata/group inspection work with source READ, group READ and destination WRITE. DESCRIBE_CONFIGS remains optional and was denied in the fixture. |
| TLS/auth | CA validation remains on when hostname matching is disabled. Valid and invalid SAN/CA cases and SCRAM-SHA-512 pass. mTLS is not tested here. |
| Deadlines | Withheld coordinator and member-commit replies return the operation deadline. Forced client cancellation closes these clients and a blocked producer within the tested two-second bound, without detected goroutine leaks. The adapter also closes with an undrained delivery queue. Flush forces pending batches without blocking the core. This is not a universal shutdown guarantee. |

## Constraints that affect a migration

- [`RecordDeliveryTimeout`](https://github.com/twmb/franz-go/blob/v1.22.2/pkg/kgo/sink.go#L2153-L2157) in this client version measures a batch's age from its first record timestamp. It rejects a newly submitted historical record immediately. The prototype uses a per-record context measured from submission instead; `TestSpikeDeliveryTimeoutUsesRecordTimestamp` reproduces the difference.
- A record context does not normally cancel an ambiguous in-flight idempotent produce. [`AllowIdempotentProduceCancellation`](https://github.com/twmb/franz-go/blob/v1.22.2/pkg/kgo/config.go#L1228-L1258) permits that cancellation and may allow duplicates when the application retries. The test demonstrates both modes. The prototype retains the default mode; a production delivery-deadline policy is still required.
- Retaining control records handles the tested transaction gaps. General cursor advancement across empty/compacted fetches is not proven. Never substitute the broker's last stable offset for a cursor known to have been consumed.
- Full connection-property compatibility, authentication mapping, ONEOFF groups, relative windows, strict foreign/expired progress handling, existing file-output recovery, lost-assignment error classification and the full shutdown/rebalance fault matrix remain outside this prototype. Unsupported connection properties are rejected.

## Accepted timestamp edge case

Kafka 4.x defaults reject new CreateTime records more than one hour ahead of the broker, preventing the extreme-future input used here. Existing records and topics with a larger allowance remain exposed. Kafka 3.9 defaults allow such dates; the fixture explicitly sets unlimited allowances. See the [Kafka 4.0 settings](https://kafka.apache.org/40/configuration/topic-level-configs/#message.timestamp.after.max.ms) and [Kafka 3.9 settings](https://kafka.apache.org/39/configuration/topic-level-configs/#message.timestamp.after.max.ms).

The pinned client can silently change timestamps outside its nanosecond conversion range (about 1677–2262). A raw protocol producer writes `253402214400000` ms (9999-12-31) without using franz-go's timestamp encoder; the consumer returns `-4852202631934` ms. Its [millisecond decoder](https://github.com/twmb/franz-go/blob/v1.22.2/pkg/kgo/source.go#L2439) multiplies by 1,000,000 before calling `time.Unix`, overflowing int64. The wrapped value passes the write guard, so filtering and copied timestamps can be wrong. The producer independently uses `UnixNano` outside its safe range. Frequency in production is unknown.

This limitation is accepted and does not block migration. No decoder fix is required for this decision. Keep `TestSpikeRawTimestampDecodingAndRejection` as a diagnostic: its exact-timestamp assertion still fails and is excluded explicitly from the other checks below. Passing the separate observational round-trip test does not establish fidelity for extreme timestamps.

## Local load checks

The corrected adapter copies all 16,000 records in the default test and all 128 records in the slow-destination test. Work caps hold: peak uncommitted records are 9,707 and 32. Sampled peak RSS is 81.3 and 19.2 MiB; elapsed time is 0.494 and 1.104 s. These short CLI checks do not measure peak capacity or establish performance parity with the production adapter.

Earlier exploratory comparisons and one-minute controlled streams also passed fidelity, progress, work-cap and memory checks. Reports are retained in ignored `build-results/comparison-*`, `soak-*` and `post-review-load`. Controlled input rates differ; do not use them as saturation measurements. The prototype omits production checks and Fx composition. Fetch wait is 500 ms.

## Run

Use a disposable Kafka 3.9 broker on `127.0.0.1:39092`:

```sh
# Diagnostic for the accepted edge case: its fidelity assertion fails.
go test -race -count=1 -tags=integration,franzspike \
  -run '^TestSpikeRawTimestampDecodingAndRejection$' ./internal/franzspike
# Other checks explicitly exclude that known failure (security needs its fixture).
go test -race -count=1 -tags=integration,franzspike \
  -skip TestSpikeRawTimestampDecodingAndRejection ./internal/franzspike
CGO_ENABLED=0 go build -tags=franzspike -o bin/franz-spike ./cmd/franz-spike
KAFERRY_TEST_BINARY="$PWD/bin/franz-spike" go test -count=1 \
  -tags=integration,load -run TestLoadBoundedKafkaCopy ./tests/compat
```

The source and destination topics must exist. The CLI accepts only the narrow mapper described above. The permission result applies to Kafka 3.9; older brokers need a separate comparison against the existing idempotent producer. Security tests need the disposable three-broker qualification fixture and `FRANZ_SPIKE_CERT_DIR`; otherwise they skip. Do not interpret that skip as security coverage.

To include the security cases, start the existing disposable fixture with SCRAM enabled:

```sh
export KAFERRY_QUAL_CERT_DIR="$PWD/.cache/franz-security/certs"
sh tests/qualification/certificates.sh "$KAFERRY_QUAL_CERT_DIR"
docker compose -p kaferry-franz-security -f tests/qualification/compose.yaml \
  -f internal/franzspike/security.compose.yaml up -d --wait
FRANZ_SPIKE_CERT_DIR="$KAFERRY_QUAL_CERT_DIR" go test -race -count=1 \
  -tags=integration,franzspike -run 'TestSpikeTLS|TestSpikePlainTLS|TestSpikeScram' ./internal/franzspike
# Remove only this disposable fixture after testing.
docker compose -p kaferry-franz-security -f tests/qualification/compose.yaml \
  -f internal/franzspike/security.compose.yaml down -v
```

Before migration: resolve the delivery-deadline policy, prove invisible-gap cursor handling, map or explicitly approve unsupported working settings, and run the complete compatibility/security/rebalance/shutdown suite against the production adapter. Retain the accepted timestamp limitation in the compatibility documentation when adopting the client.
