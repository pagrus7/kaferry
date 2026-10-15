# franz-go feasibility spike

**Continue evaluation; do not replace the production client yet.** This branch contains disposable adapters and capability tests. Normal builds still use confluent-kafka-go. The experiment uses the existing copy engine, SQL planner and CBOR file components.

## Evidence

Tested locally on macOS ARM64 with Go 1.27.2 (module minimum 1.26), franz-go 1.22.2, kadm 1.19.0 and Apache Kafka 3.9. `deadline_test.go` also uses kfake for withheld replies.

| Area | Observed result |
| --- | --- |
| Raw records | Key/value nulls versus empty bytes, ordered duplicate headers, NUL/non-UTF8 header names and ordinary negative/zero timestamps survive. |
| Saved progress | Public `PreCommitFnContext` retains member/generation fencing and custom metadata. `AdjustFetchOffsetsFn` positions before the first fetch. Departed-member commits are rejected. |
| Identities and routing | Existing topic IDs require unpadded standard Base64, not kadm's padded string. Cluster IDs and keyed Murmur2 partition routing match the native client. |
| Transactions | Read-committed consumption hides aborted data across small fetches. Retained control records advance the delivered cursor. An open transaction holds completion; later data beyond sampled H is excluded. |
| Engine and files | A repeated query resumes from its saved offset. Topic→file→topic works through the real v1 CBOR components. This does not test existing-file recovery. |
| Rebalance | Two real group members transfer a partition while one adapter holds an unsubmitted paused record. |
| Permissions | PLAIN/TLS copying, progress commits and metadata/group inspection work with source READ, group READ and destination WRITE. DESCRIBE_CONFIGS remains optional and was denied in the fixture. |
| TLS/auth | CA validation remains on when hostname matching is disabled. Valid and invalid SAN/CA cases and SCRAM-SHA-512 pass. mTLS is not tested here. |
| Deadlines | Withheld coordinator and member-commit replies return the operation deadline. Forced client cancellation closes these clients and a blocked producer within the tested two-second bound, without detected goroutine leaks. The adapter also closes with an undrained delivery queue. This is not a universal shutdown guarantee. |

## Constraints that affect a migration

- [`RecordDeliveryTimeout`](https://github.com/twmb/franz-go/blob/v1.22.2/pkg/kgo/sink.go#L2153-L2157) in this client version measures a batch's age from its first record timestamp. It rejects a newly submitted historical record immediately. The prototype uses a per-record context measured from submission instead; `TestSpikeDeliveryTimeoutUsesRecordTimestamp` reproduces the difference.
- A record context does not normally cancel an ambiguous in-flight idempotent produce. [`AllowIdempotentProduceCancellation`](https://github.com/twmb/franz-go/blob/v1.22.2/pkg/kgo/config.go#L1228-L1258) permits that cancellation and may allow duplicates when the application retries. The test demonstrates both modes. The prototype retains the default mode; a production delivery-deadline policy is still required.
- Extreme timestamps corrupt because the producer uses `UnixNano` internally. Keep checked rejection outside the client-safe range. The observational timestamp test reports this limitation; its passing status does not mean arbitrary int64 timestamps work.
- Retaining control records handles the tested transaction gaps. General cursor advancement across empty/compacted fetches is not proven. Never substitute the broker's last stable offset for a cursor known to have been consumed.
- Full connection-property compatibility, authentication mapping, ONEOFF groups, relative windows, strict foreign/expired progress handling, existing file-output recovery, lost-assignment error classification and the full shutdown/rebalance fault matrix remain outside this prototype. Unsupported connection properties are rejected.

## Local load comparison

Three interleaved runs of the unchanged load test used the same host and broker. Both clients preserved all records and stayed within the configured work caps.

| Case | Native median | Prototype median |
| --- | ---: | ---: |
| 16,000 records | 0.680 s | 0.163 s |
| 128 records, cap 32, destination linger 100 ms | 2.987 s | 1.085 s |

These short CLI measurements include startup. The prototype omits production checks and Fx composition; they do not isolate library throughput or establish sustained-load parity. Per-run logs and metrics are in ignored `build-results/comparison-*`. The native binary identifies baseline `b9f73fb` with local refactor edits; the prototype uses the same baseline copy engine. Normalized fetch wait is 500 ms.

One-minute controlled streaming runs also passed fidelity, saved-progress, work-cap and memory checks with one burst and no destination outage. Native/prototype peak RSS was 53.6/40.3 MiB and signal shutdown took 258/19 ms. Input rates differed (624/581 records/s); these are not saturation measurements. Reports are in `build-results/soak-*`.

## Run

Use a disposable Kafka 3.9 broker on `127.0.0.1:39092`:

```sh
go test -race -count=1 -tags=integration,franzspike ./internal/franzspike
CGO_ENABLED=0 go build -tags=franzspike -o bin/franz-spike ./cmd/franz-spike
KAFERRY_TEST_BINARY="$PWD/bin/franz-spike" go test -count=1 \
  -tags=integration,load -run TestLoadBoundedKafkaCopy ./tests/compat
```

The source and destination topics must exist. The CLI accepts only the narrow mapper described above. Security tests need the disposable three-broker qualification fixture and `FRANZ_SPIKE_CERT_DIR`; otherwise they skip. Do not interpret that skip as security coverage.

To include the security cases, start the existing disposable fixture with SCRAM enabled:

```sh
export KAFERRY_QUAL_CERT_DIR="$PWD/.cache/franz-security/certs"
sh tests/qualification/certificates.sh "$KAFERRY_QUAL_CERT_DIR"
docker compose -p kaferry-franz-security -f tests/qualification/compose.yaml \
  -f internal/franzspike/security.compose.yaml up -d --wait
FRANZ_SPIKE_CERT_DIR="$KAFERRY_QUAL_CERT_DIR" go test -race -count=1 \
  -tags=integration,franzspike ./internal/franzspike
# Remove only this disposable fixture after testing.
docker compose -p kaferry-franz-security -f tests/qualification/compose.yaml \
  -f internal/franzspike/security.compose.yaml down -v
```

Before migration: resolve the delivery-deadline policy, prove invisible-gap cursor handling, map or explicitly approve unsupported working settings, and run the complete compatibility/security/rebalance/shutdown suite against the production adapter.
