# franz-go feasibility spike

**Do not migrate with this pinned client: it can silently change an out-of-range Kafka timestamp while consuming it.** This branch contains disposable adapters and capability tests. Normal builds still use confluent-kafka-go. Further evaluation needs a decoder fix or access to the original timestamp before conversion. The experiment uses the existing copy engine, SQL planner and CBOR file components.

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
- **The timestamp migration gate fails.** A raw protocol producer writes `253402214400000` ms (9999-12-31) without using franz-go's timestamp encoder. The consumer returns `-4852202631934` ms. Its [millisecond decoder](https://github.com/twmb/franz-go/blob/v1.22.2/pkg/kgo/source.go#L2439) multiplies by 1,000,000 before calling `time.Unix`, overflowing int64. The wrapped value falls inside the write guard, so the guard cannot reliably reject the original timestamp. `TestSpikeRawTimestampDecodingAndRejection` intentionally fails until this is fixed.
- The producer independently uses `UnixNano` internally. Its observational round-trip test reports corruption outside the safe range; passing that observation test is not a successful fidelity gate.
- Retaining control records handles the tested transaction gaps. General cursor advancement across empty/compacted fetches is not proven. Never substitute the broker's last stable offset for a cursor known to have been consumed.
- Full connection-property compatibility, authentication mapping, ONEOFF groups, relative windows, strict foreign/expired progress handling, existing file-output recovery, lost-assignment error classification and the full shutdown/rebalance fault matrix remain outside this prototype. Unsupported connection properties are rejected.

## Local load checks

The corrected adapter copies all 16,000 records in the default test and all 128 records in the slow-destination test. Work caps hold: peak uncommitted records are 9,707 and 32. Sampled peak RSS is 81.3 and 19.2 MiB; elapsed time is 0.494 and 1.104 s. These short CLI checks do not measure peak capacity or establish performance parity with the production adapter.

Earlier exploratory comparisons and one-minute controlled streams also passed fidelity, progress, work-cap and memory checks. Reports are retained in ignored `build-results/comparison-*`, `soak-*` and `post-review-load`. Controlled input rates differ; do not use them as saturation measurements. The prototype omits production checks and Fx composition. Fetch wait is 500 ms.

## Run

Use a disposable Kafka 3.9 broker on `127.0.0.1:39092`:

```sh
# This fails at the timestamp migration gate; that failure must stay visible.
go test -race -count=1 -tags=integration,franzspike ./internal/franzspike
# Run the other checks while diagnosing that blocker (security cases need the fixture).
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

Before migration: fix timestamp decoding and pass the retained failing gate; resolve the delivery-deadline policy, prove invisible-gap cursor handling, map or explicitly approve unsupported working settings, and run the complete compatibility/security/rebalance/shutdown suite against the production adapter.
