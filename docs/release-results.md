# Release results

The [front-page image](https://github.com/pagrus7/kaferry/releases/latest/download/release-results.svg) follows the latest stable release. Its version and source identify the measured executable. Click it for [the full report](https://github.com/pagrus7/kaferry/releases/latest/download/release-results.md); [JSON data](https://github.com/pagrus7/kaferry/releases/latest/download/release-results.json) retains all load/soak measurements and their environments. Reports are release assets, so their availability does not depend on Actions artifact retention. GitHub's image proxy may cache an earlier image; check the displayed version.

The Kafka matrix covers source/destination broker pairs, authentication fixtures and capture endurance. Its case durations describe qualification runs. [Full compatibility evidence](https://github.com/pagrus7/kaferry/releases/latest/download/compatibility.json) records source and binary identities; [qualification](https://github.com/pagrus7/kaferry/releases/latest/download/qualification.md) describes the other exercised scenarios and limits.

## Throughput measurements

The image shows five Linux AMD64 load checks: Kafka copying with key and preserved routing, capture, replay and archive file copying. The report names each input count. Timing starts when SQL input is released and ends when the process exits. These short checks exercise correctness and bounded resources on shared CI runners; their rates do not establish deployment capacity or a change against another release.

The 30-minute soak uses paced input and deliberate destination stalls. Its rate describes that workload; memory, lag, shutdown and work-cap measurements are in the JSON. Detailed logs and sample streams remain in the release workflow's `qualification-*` artifacts for 30 days. Individual `matrix-*` artifacts retain their logs and reconciliation data for 14 days.

## Release comparisons

Compare two revisions with the [paired performance workflow](https://github.com/pagrus7/kaferry/actions/workflows/performance.yml). It warms up each binary, runs six interleaved pairs per workload by default, and alternates which binary runs first on the same runner. The report gives the median latest/prior throughput ratio and pair range. Ratios above 1 mean higher throughput for the latest revision in that experiment; shared-runner measurements have no regression gate.

These comparisons build source at each revision using its pinned Go version and run the latest revision's correctness workloads against both binaries. They describe those builds and workload settings; they are separate from the packaged-binary release checks. [Method and retained evidence](../design/components.md#paired-performance).
