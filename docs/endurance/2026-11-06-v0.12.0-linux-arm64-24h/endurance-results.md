# v0.12.0: 24-hour endurance results

**PASSED** on Linux ARM64: one continuous Kaferry process copied and verified **17,856,000 records** during 24 hours of publishing followed by five minutes of draining. Final reconciliation found no missing, duplicate, corrupted, unexpected or reordered records, and the submitted and received SHA-256 fingerprints matched.

This measures application endurance under the workload below. It does not establish saturation capacity or resilience of replicated clusters.

| Workload | Configuration |
| --- | --- |
| Tested executable | Published v0.12.0 Linux ARM64 binary |
| Publishing / drain | 24 hours / five minutes |
| Input | 200 records/second plus 400-record bursts; 1,440 bursts |
| Routing | Key routing; 24 source and 32 destination partitions |
| Payloads | High entropy values of 1,024–5,119 bytes |
| Brokers | Separate Apache Kafka 3.9.2 single-broker source and destination clusters; replication factor 1 |
| Destination pauses | One second each; 1,440 pauses during publishing |
| Run interval (UTC) | November 6, 2026 at 04:45:54 through November 7 at 04:51:11, including fixture setup and cleanup |

| Measurement | Result |
| --- | ---: |
| Submitted / acknowledged / unique received / verified | 17,856,000 each |
| Missing / duplicate / corrupted / unexpected / reordered records | 0 each |
| Publish errors / final backlog | 0 / 0 |
| Destination pauses recovered | 1,440 of 1,440 |
| Sampled peak resident memory | 32.4 MiB |
| Final median memory growth over warm median | −7.4 MiB |
| Delivery time p95 / p99 / p99.9 | 27.4 / 491.0 / 1,037.6 ms |
| Maximum delivery time | 1,150.9 ms |
| Observed peak delivery backlog | 414 records |
| Shutdown | 0.034 seconds |

Delivery times run from test submission to first unique receipt and include producer queueing, copying and verifier delay. They cover all 17,856,000 records. Memory was sampled once per second and can miss short peaks. This single-process, replication-factor-1 run does not cover multiple Kaferry processes, replicated broker failures, network partitions or authentication overhead. [Workload and measurement definitions](../../../design/qualification.md#endurance-on-a-persistent-host).

The release executable came from commit `c7f65b8378f6bbb7b1216783269c669fcec1258a`; fixtures ran from clean commit `9c73d396032dc810ba4c4ca708714b0ad741fe1c`. The release archive checksum and attestation were verified before the run. Publication checked that the measured executable matched the release archive.

Binary SHA-256: `7f796a859277f60d10c7bd63465d0d6e854d3bab63aa56688089e53ef48425ea`.

[Full manifest and measurements](endurance-results.json) · [Published report](https://github.com/pagrus7/kaferry/releases/download/v0.12.0/endurance-linux-arm64-202611060445548614230000-endurance-results.md) · [Logs and samples](https://github.com/pagrus7/kaferry/releases/download/v0.12.0/endurance-linux-arm64-202611060445548614230000-endurance-evidence.tar.gz) · [Download checksums](https://github.com/pagrus7/kaferry/releases/download/v0.12.0/endurance-linux-arm64-202611060445548614230000-endurance.SHA256SUMS).

[All endurance results](../../endurance-results.md).
