# Release results

The [latest release report](https://github.com/pagrus7/kaferry/releases/latest/download/release-results.md) identifies the packaged Linux AMD64 executable and tested environments. Download the [load and soak measurements](https://github.com/pagrus7/kaferry/releases/latest/download/release-results.json), [compatibility matrix](https://github.com/pagrus7/kaferry/releases/latest/download/compatibility.json) or [qualification summary](https://github.com/pagrus7/kaferry/releases/latest/download/qualification.md).

## Throughput measurements

Load rates measure the interval from releasing SQL input until process exit on a shared CI runner. The 30-minute soak includes paced input and deliberate destination stalls. Interpret rates within the recorded workload and environment. A missing workload name means profile coverage is unknown. Release assets persist. Detailed `qualification-*` workflow artifacts expire after 30 days and `matrix-*` artifacts after 14 days.

## Release comparisons

To compare revisions on the same host, use the [paired performance procedure](design/qualification.md#paired-performance). Its median latest/prior ratio and pair range describe those source builds and workload settings.
