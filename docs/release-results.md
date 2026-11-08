# Release results

The [latest release report](https://github.com/pagrus7/kaferry/releases/latest/download/release-results.md) identifies the measured executable by version and source. [JSON data](https://github.com/pagrus7/kaferry/releases/latest/download/release-results.json) retains all load/soak measurements and their environments. Reports are release assets, so their availability does not depend on Actions artifact retention.

The Kafka matrix covers source/destination broker pairs, authentication fixtures and capture endurance. Its case durations describe qualification runs. [Full compatibility evidence](https://github.com/pagrus7/kaferry/releases/latest/download/compatibility.json) records source and binary identities; [qualification](https://github.com/pagrus7/kaferry/releases/latest/download/qualification.md) describes the other exercised scenarios and limits.

## Throughput measurements

The report retains Linux AMD64 Kafka copying with key and preserved routing, capture, replay, archive file copying and the [load workload profiles](design/qualification.md#load-workload-profiles), with each input count. Timing starts when SQL input is released and ends when the process exits. These short checks exercise correctness and bounded resources on shared CI runners; their rates do not establish deployment capacity or a change against another release.

Each JSON measurement records its versioned `workload.name` and payload, key, header, seed and source-compression parameters. Profile measurements also retain the record counts observed in source batches for each codec. Receipts with workload metadata require complete identities, all profile cases and matching observed compression. Receipts without these fields remain usable for supplemental reports, labeled `workload.name: not recorded`; they establish no profile coverage. Compare rates only when the workload and its parameters match.

The 30-minute soak uses paced input and deliberate destination stalls. Its rate describes that workload; memory, lag, shutdown and work-cap measurements are in the JSON. Detailed logs and sample streams remain in the release workflow's `qualification-*` artifacts for 30 days. Individual `matrix-*` artifacts retain their logs and reconciliation data for 14 days.

## Release comparisons

Compare two revisions with the [paired performance workflow](https://github.com/pagrus7/kaferry/actions/workflows/performance.yml). It warms up each binary, runs six interleaved pairs per workload by default, and alternates which binary runs first on the same runner. The report gives the median latest/prior throughput ratio and pair range. Ratios above 1 mean higher throughput for the latest revision in that experiment; shared-runner measurements have no regression gate.

These comparisons build source at each revision using its pinned Go version and run the latest revision's correctness workloads against both binaries. They describe those builds and workload settings; they are separate from the packaged-binary release checks. [Method and retained evidence](design/qualification.md#paired-performance).
