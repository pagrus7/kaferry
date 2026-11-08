# Kaferry documentation

Start with [installation](install.md), then [copy records with local Kafka](usage.md#local-kafka-playground) or [try the bundled Docker archive demo](usage.md#first-copy-with-docker).

| Reader’s question | Guide |
| --- | --- |
| How do I copy a backlog, offsets or selected partitions? | [Kafka examples](usage.md#copy-between-kafka-topics) |
| How do I copy records in a time range? | [Time-range examples](usage.md#copy-records-in-a-time-range) |
| How do I archive a topic and replay it? | [Archive examples](usage.md#archive-your-own-topic) |
| How do I run, stop, resume and check a copy? | [Usage](usage.md) |
| How do I recover or move an archive replay? | [Archive procedures](usage.md#archive-procedures) |
| Why is my copy idle, failing or repeating data? | [Troubleshooting](usage.md#troubleshooting) |
| What does a setting, result or exit code mean? | [Settings and results](reference.md) |
| Which SQL forms are accepted? | [Syntax](design/grammar.md) |
| How do delivery, ordering and restart affect my copy? | [Output rules](reference.md#kafka-sink) · [Resuming](usage.md#resume-or-start-over) |
| Which Kafka configurations and limitations apply? | [Compatibility](design/client-compatibility.md) |
| What files and metadata make up an archive? | [Archive format](design/archive.md) |
| How do I change or test Kaferry? | [Contributor guide](../CONTRIBUTING.md) |
| How are releases built and published? | [Release procedure](releases/README.md) |
