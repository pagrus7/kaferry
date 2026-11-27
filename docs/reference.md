# SQL, settings and results reference

Accepted SQL, exact settings, inspection output, exit codes and metrics. For operating steps, see [usage](usage.md); for restarting safely, see [resume or start over](usage.md#resume-or-start-over).

## SQL statements

Each input contains one `INSERT INTO ... SELECT * FROM ...` statement, with an optional final semicolon. The destination follows `INSERT INTO`; the source follows `FROM`. Either endpoint can be `TOPIC 'topic@alias'` or `ARCHIVE 'directory'`. A topic without `@alias` uses the required `default` connection profile. Archive paths name Kaferry archive directories. The destination must differ from the source archive directory or Kafka cluster/topic, including path or connection aliases.

Clauses appear in this order: optional `WITH CONNECTION` declarations, destination and its optional `WITH`, source and its optional `WITH`, optional `WHERE`, then optional `OPTIONS`. `SELECT *` copies whole records; field selection and joins are unsupported. [Endpoint settings](#endpoint-with-properties) define source progress/start rules and destination routing/rotation.

Keywords are case-insensitive. Connection aliases are case-sensitive and match `[A-Za-z_][A-Za-z0-9_]*`, as in TOML profile names. Strings use single quotes; `''` represents one quote. Comments use `--` to end of line or non-nested `/* ... */`. Present connection, `WITH` and `OPTIONS` clauses must be nonempty; settings, inline aliases and properties within one connection cannot repeat.

### Connections

Inline declarations precede `INSERT`; separate multiple declarations with commas, repeating `CONNECTION alias AS (...)`. Property names and literal values are strings. SQL properties override the matching TOML profile. `ENV('NAME')` resolves at startup and fails if unset; keep credentials in environment variables. Aliases without inline declarations need TOML profiles. TLS needs a trusted broker CA; set `ssl.ca.location` if system roots do not include it.

```sql
WITH CONNECTION dr AS (
  'bootstrap.servers' = 'dr-kafka:9093',
  'security.protocol' = 'SASL_SSL',
  'sasl.mechanism' = 'PLAIN',
  'sasl.username' = 'copier',
  'sasl.password' = ENV('DR_PASSWORD')
)
INSERT INTO TOPIC 'orders@dr'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-dr')
WHERE RECORD_OFFSET < LATEST;
```

### WHERE filters and bounds

An ungrouped `WHERE` combines predicates with `AND`, in any order. It permits one partition predicate and at most one lower and one upper bound for each of `RECORD_OFFSET` and `RECORD_TIMESTAMP`. General `OR`, equality comparisons and payload filters are unsupported.

| Predicate | Accepted forms and meaning |
| --- | --- |
| `PARTITION` | `IN (0, 2)`, `NOT IN (0, 2)`, `BETWEEN 0 AND 2`, `NOT BETWEEN 0 AND 2`; `BETWEEN` includes every ID from the lower through the upper end |
| `RECORD_OFFSET` | `>=`, `>`, `<`, `<=` with a nonnegative signed 64-bit integer; `BETWEEN lo AND hi`; or `< LATEST` |
| `RECORD_TIMESTAMP` | `>=`, `>`, `<`, `<=` with a time expression; or `BETWEEN lo AND hi`; uses Kafka record timestamps, not payload fields |

Partition IDs range from 0 to 2,147,483,647. Lists are nonempty; order and repeated IDs do not affect selection. Partition `BETWEEN` predicates may name at most 65,536 IDs across one statement. Every named ID must exist at startup, including exclusions. Archives use original Kafka IDs from completed-file headers across all represented source topics/clusters. `NOT` applies only to partition `IN` or `BETWEEN`.

Offset and timestamp `BETWEEN` include both ends. Internally, bounds use an inclusive lower and exclusive upper end. `> x` advances the lower end by one; `<= x` advances the upper end by one. That unit is one offset or one millisecond. Empty ranges and overflowing adjustments fail. For example, `RECORD_OFFSET BETWEEN 100 AND 199` equals `RECORD_OFFSET >= 100 AND RECORD_OFFSET < 200`.

An offset is a position within a partition. Kafka numeric starts must be within retained positions when initialized. Offset gaps mean a range's width can exceed its record count. Archive offsets refer to original Kafka positions. See [how a copy ends](#how-a-copy-ends) for upper bounds and `LATEST`.

Different offset ranges may use parenthesized branches joined by `OR`. Each branch needs `PARTITION IN` or `PARTITION BETWEEN` and an upper offset bound; selections must be disjoint. Unlisted partitions are excluded. Shared timestamp predicates may follow the branches with `AND`; shared partition/offset predicates and timestamps inside branches are unsupported:

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-offset-repair')
WHERE (PARTITION IN (0) AND RECORD_OFFSET BETWEEN 100 AND 199)
   OR (PARTITION IN (2) AND RECORD_OFFSET >= 900 AND RECORD_OFFSET < LATEST);
```

### Timestamp filters

A time expression starts with `TIMESTAMP '2026-10-01T09:00:00Z'` or `NOW()`, followed by optional `+ INTERVAL 'n' UNIT` or `- INTERVAL 'n' UNIT` adjustments. Timestamp literals require RFC 3339 with `Z` or an explicit offset and at most three fractional digits. Interval magnitudes are nonnegative integers; units are `SECOND`, `MINUTE`, `HOUR` or `DAY` (86,400 seconds).

Every `NOW()` in one resolved window uses the same instant. Saved progress keeps that resolved window on resume; separate workers can resolve different windows. Use fixed timestamps when cooperating workers need identical bounds.

```sql
INSERT INTO ARCHIVE '/data/recent-orders'
SELECT * FROM ARCHIVE '/data/orders' WITH (GROUP = 'recent-orders')
WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '7' DAY
  AND RECORD_TIMESTAMP < NOW();
```

This includes the lower timestamp and excludes the upper timestamp. `BETWEEN` includes its upper millisecond too: `BETWEEN TIMESTAMP '2026-10-01T09:00:00Z' AND TIMESTAMP '2026-10-01T09:30:00Z'` normalizes to `[09:00:00.000, 09:30:00.001)` UTC.

Timestamp bounds cannot be combined with explicit `START`. To stop a Kafka scan early based on timestamps, see [how a copy ends](#how-a-copy-ends).

### Final OPTIONS

Place `OPTIONS (...)` after `WHERE`, or after the source when there is no filter. Separate settings with commas. Source settings such as `GROUP` belong in source `WITH`.

| Option | Default | Values and limits |
| --- | --- | --- |
| `WORKER_STOP_AFTER_RECORDS` | No record limit | Integer from 1 to 9,007,199,254,740,992; caps accepted writes per worker per invocation, not records read or a shared group total |
| `ON_ERROR` | `FAIL` | `FAIL` or `SKIP`; applies to all skippable record errors; cannot be combined with either specific policy |
| `ON_MISSING_TIMESTAMP` | `FAIL` | `FAIL` or `SKIP`; requires timestamp bounds in `WHERE` |
| `ON_REJECTED_TIMESTAMP` | `FAIL` | `FAIL` or `SKIP`; requires a topic destination |

Filtered records, synchronous SKIP rejections and queue-full retries do not consume the worker allowance. Rerunning with the same group grants another allowance while retaining saved ends and progress. [Record-limit example](usage.md#try-a-small-copy-first) · [Skippable failures](#record-error-policies).

### SQL errors

SQL errors identify the first error found by one-based line and character (code point) column. LF ends a line; tabs count as one column. File and stdin input include their source name. Input values and SQL excerpts are omitted. Related declarations include their locations. Fix the reported error, then rerun.

```text
"copy.sql": line 3, column 29: expected FAIL or SKIP; found identifier
```

Common rejected forms include `PARTITION IN ()` (empty list), `OPTIONS (GROUP = 'g')` (wrong clause), `SELECT key` (only `SELECT *`), `INTERVAL '1' MONTH` (unsupported unit), and two statements separated by `;`. The [formal grammar](design/grammar.md) is a contributor reference.

## How a copy ends

- **Kafka readable end.** By default, or with `RECORD_OFFSET < LATEST`, Kaferry saves each partition's exclusive readable end on first assignment and reuses it on resume. Later appends are outside that end. An open transaction can hold the readable end below records already written. Each partition captures its end independently.
- **Kafka numeric end.** An explicit upper offset sets the end for that partition. The exclusive end must be within retained, readable positions on first assignment. With either Kafka end, a timestamp lookup with no match or a timestamp-derived start at or beyond the end yields an empty copy for that partition.
- **Earlier timestamp stop.** Timestamp bounds normally filter records throughout the offset range. On Kafka, `MAX_OUT_OF_ORDER` requires an upper timestamp bound and a `LATEST` end. A partition stops at its offset end or before the first visible record stamped strictly after the resolved exclusive upper bound plus the allowance, whichever comes first. A later in-range record can be missed if timestamps move backwards by more than the allowance. [Historical scan example](usage.md#shorten-a-historical-scan).
- **Following.** `FOLLOW = ON` keeps reading new Kafka records until interrupted or capped. It accepts partition filters and lower bounds. Remove upper bounds to follow.
- **Archive replay.** The end is the pinned completed-file selection's EOF, including with `< LATEST`. A numeric end can finish an original source partition early. Timestamp bounds filter records during replay. New files enter only a fresh selection.

A [worker record cap](#final-options) can end any copy before its source is finished. Bounded Kafka workers finish independently after the partitions they still hold stop and their progress is saved. See [exit codes](#exit-codes) for interruption and empty assignments, and [saved status](#saved-status) for the verdict across the selection.

## Inspection

`explain` previews a copy using current endpoint metadata and saved progress. It never joins the group, writes, locks files or checks write access. The source can change between preview and execution. For a fresh or incomplete archive selection, the preview reads headers and sizes without scanning payloads or recovering private files. For a completed checkpoint it reports saved file metadata even if those files have since disappeared. `ONE_OFF` previews a fresh group.

The text groups similar partitions and limits the windows, partition IDs per window and warnings shown to eight each. Use `--json` for all partitions, identifiers and warnings. See the [version 3 JSON example](../src/internal/app/testdata/explain.json).

- For Kafka, `start` is the selected offset. `log_end` includes open transactions. An offset difference is a distance in the log, so it cannot tell you how many records will be copied.
- `end_rule` is `LATEST`, `OFFSET`, `FOLLOW` or archive `EOF`. For bounded Kafka copies, `captured_end` is exclusive. An `end_origin` of `preview` means the end has yet to be saved. [How a copy ends](#how-a-copy-ends).
- Window bounds are signed Unix milliseconds, lower inclusive and upper exclusive. A null bound is open. Freshly resolved `NOW()` values are provisional. A null `max_out_of_order_ms` disables timestamp stopping.
- For archives, `position_records` counts confirmed, examined input records. Destination size and file count are not inspected. SQL text and connection properties are excluded from the report.

### Saved status

`kaferry status` accepts the same query and configuration as `run`; a named `GROUP` is required. It reads saved progress without joining, consuming, producing, committing, creating files or taking locks. It needs no permissions beyond existing source/group DESCRIBE and endpoint metadata access. `ONE_OFF` requires the actual logged group name; a deleted file checkpoint has no saved status.

| Status | Meaning |
| --- | --- |
| `unknown` | Saved progress is absent, invalid or incompatible, or the group has an incompatible active member. |
| `running` | Kafka group activity, unsettled state or a membership change was observed. |
| `incomplete` | Part of the selection has no saved stop. |
| `limit_reached` | All selected partitions have saved stops and at least one stopped at a worker record cap. |
| `complete_within_disorder` | All selected partitions finished and at least one stopped early under MAX_OUT_OF_ORDER. |
| `complete` | Source processing finished for the selected finite scope: saved offset ends or the saved archive selection. |

Identity conflicts take precedence over activity, then missing stops, worker caps and completion. Added eligible partitions count. Kafka activity can change during inspection and file activity is unobserved. An invalid checkpoint or selection yields `unknown`. Other operational read failures exit 1 without a verdict.

An idle FOLLOW group with saved progress normally reports `incomplete`. See [how a copy ends](#how-a-copy-ends) for the assumptions behind `complete_within_disorder`. Completion describes source processing. Filtered or skipped records can be absent, writes can repeat, and destination data may have been removed.

Text shows the verdict, its meaning and up to eight Kafka partitions or the archive's replay progress. `status --json` version 3 includes `version`, `verdict`, `meaning`, `group`, `fingerprint`, `source`, `destination`, `activity` and `partitions`. It omits `reason` (text), `notes` (an array of strings) and `next` (a suggested action) when empty. JSON lists all partitions. Each entry has `id` and may include `position`, `fingerprint`, `stop`, `window` and `captured_end`.

A Kafka `position` is the next offset to read. A file `position` counts confirmed examined input records. Archive sizes and counts describe the saved selection, or current completed files before the first run. Status checks headers and sizes for incomplete groups without verifying payloads or destination writes.

On a fingerprint mismatch, `unknown` can still report observed Kafka activity and source-validated positions, including partitions outside the current selection. Invalid metadata or a changed source UUID withholds positions. [Recovering a recreated destination](usage.md#recover-after-destination-topic-recreation).

## Configuration and CLI

Connections use TOML profiles or `WITH CONNECTION`; SQL keys override TOML. Endpoint `WITH` configures source progress/start rules or destination rotation/partitioning; optional final `OPTIONS` holds worker stops and all record-error policies. `ENV('NAME')` resolves at startup, fails if unset and is redacted. Worker settings are in TOML.

Profiles can be shared by sources and destinations. Supported connection keys are mapped to franz-go options; other keys fail before endpoints are opened, without displaying property values. A property name does not imply full librdkafka compatibility.

### Endpoint WITH properties

Source properties follow `FROM`; sink properties follow `INSERT INTO`. Unknown or inapplicable properties fail. [Final `OPTIONS`](#final-options) contains worker stops and record-error policies. [SQL syntax](#sql-statements).

#### Kafka source

| Property | Default | Meaning |
| --- | --- | --- |
| `GROUP` | `ONE_OFF` | Named consumer-group progress, or a generated independent group per invocation |
| `START` | `EARLIEST` | Explicit `START` requires `FOLLOW = ON` and no timestamp bounds. `LATEST` starts a fresh group at the readable end. Saved positions take precedence. New partitions in an established group start at the earliest retained offset. See [how a copy ends](#how-a-copy-ends) |
| `FOLLOW` | `OFF` | Set `ON` to [follow new records](#how-a-copy-ends) |
| `MAX_OUT_OF_ORDER` | `OFF` | Set a timestamp allowance to [stop a scan early](#how-a-copy-ends) |

For timestamp filters without saved progress, a positive lower bound starts at Kafka’s first matching offset. A non-positive or absent lower bound starts at the earliest retained offset. A FOLLOW partition with no timestamp match starts at the readable end.

Explicit group names must be nonblank. Quoted `GROUP = 'ONE_OFF'` is an ordinary named group. Omitting `GROUP` generates and logs a fresh `kaferry-<hex>` group each run. Quote that logged name to resume. Use the same named group for cooperating Kafka workers; unnamed runs copy independently.

#### Archive source

| Property | Default | Meaning |
| --- | --- | --- |
| `GROUP` | `ONE_OFF` | Checkpoint containing the pinned completed-file selection and physical replay cursors |

`FOLLOW`, `START` and `MAX_OUT_OF_ORDER` require a Kafka source. See [archive completion](#how-a-copy-ends) and [file replay](design/archive.md#replay-and-progress).

#### Kafka sink

| Property | Default | Meaning |
| --- | --- | --- |
| `PARTITIONING` | `PRESERVE` | Preserve original partition numbers, or use `KEY` for key-based routing |

The destination needs every selected source partition ID with `PRESERVE`. `KEY` uses Java-compatible Murmur2 routing for non-null keys and sticky routing for null keys; source offsets are never copied. Ordering holds within a source/destination partition pair during an uninterrupted producer session with stable routing. Rebalances and restarts can duplicate or reorder writes; no cross-partition order is guaranteed.

Topics must exist. Recreating a destination changes its UUID and prevents resume with the old group; running workers may write to its replacement before detecting the change. Destination timestamp settings may replace timestamps, and retention may delete old records soon after copying. [Recovery procedure](usage.md#recover-after-destination-topic-recreation).

#### Archive sink

| Property | Default | Meaning |
| --- | --- | --- |
| `ROTATE_AFTER_SIZE` | `1g` | Compressed container size target; case-insensitive binary k/m/g suffixes, or plain integer bytes |
| `ROTATE_AFTER_AGE` | `INTERVAL '1' HOUR` | Elapsed file age before rotation |

Rotation sizes and ages must be positive. [Rotation and publication](design/archive.md#rotation).

### Record-error policies

Final SQL `OPTIONS` defaults to `FAIL`. `ON_ERROR` cannot be combined with the specific policies below. SKIP drops only an identified record rejected before delivery acceptance:

| Policy | Skippable failure |
| --- | --- |
| `ON_MISSING_TIMESTAMP` or `ON_ERROR` | Missing timestamp required by a timestamp filter |
| `ON_REJECTED_TIMESTAMP` or `ON_ERROR` | Timestamp outside known destination bounds or the client’s supported range |
| `ON_ERROR` | Synchronous size refusal or supported record validation failure |

Asynchronous broker failures, authentication/authorization errors, out-of-range positions during copying, archive corruption and changed source files always stop copying. Earlier confirmed output remains; resume with the same group after fixing the cause. Counts appear in `copy summary`.

The stable diagnostic labels `poll source` and `save progress` identify source polling and saving confirmed progress. Polling can include assignment, position and destination-routing checks; these labels identify the operation, not the failed broker or network path.

On assignment or restart, saved Kafka progress below the retained log start resumes at that log start and logs the stable diagnostic label `expired source position` with `partition`, `saved`, `log_start` and `offset_gap`. The gap is an offset difference, not a lost-record count; deleted input cannot be recovered. Saved progress above log end fails. A saved captured end outside retained positions also fails. Fresh explicit offset bounds outside retained positions fail instead of skipping.

### Worker settings

Optional keys in TOML's `[worker]` table; byte values are integers:

| Key | Default | Meaning |
| --- | --- | --- |
| `partition_record_cap`, `partition_byte_cap` | 5000, 16777216 (16 MiB) | Unsaved work per partition |
| `record_cap`, `byte_cap` | 20000, 134217728 (128 MiB) | Unsaved work per worker; also sizes Kafka queues |
| `max_frame_bytes` | 67108864 (64 MiB) | Maximum decoded Avro block |
| `archive_capture_open_files` | 1024 | Open capture files; reaching this budget rotates the least recently used file |
| `archive_replay_open_files` | 32 | Open replay handles |
| `archive_replay_blocks` | 8 | Cached replay blocks; overlapping files beyond this budget repeat decoding |
| `delivery_timeout` | `'60s'` | Delivery deadline and queue-full wait |
| `shutdown_timeout` | `'30s'` | Whole-process stop deadline |
| `idle_exit` | `'30s'` | Wait after an empty assignment before exit 3 |
| `progress_interval` | '30s' | Progress logs; '0' disables |
| `checkpoint_dir` | `~/.kaferry/checkpoints` | File-source checkpoint directory; `run` creates the default directory. A configured directory must already exist |

Caps must be positive; `max_frame_bytes` is at most 4,294,967,295. Durations are positive Go duration strings, except `progress_interval` permits zero; `delivery_timeout` is 1 ms–2,147,483,647 ms. `checkpoint_dir` must not be empty. Unknown keys fail.

Replay can cache eight decoded blocks of up to 64 MiB each (512 MiB of raw block data at the defaults), independently of the worker work cap. Decoding, records and client buffers add memory; these settings do not cap total process memory. Lowering `archive_replay_blocks` reduces the cache count. Lowering `max_frame_bytes` can reject archives whose headers declare a larger block limit, even if their actual blocks are small.

Caps include delivered records whose source progress is not yet saved. Output acknowledgement precedes progress saving; a crash in between can repeat writes. At a cap, reading pauses until progress is saved. One oversized record may proceed alone.

### Connection properties

Values are strings in TOML and SQL. Unknown keys, unsupported values and contradictory aliases fail; they are never silently ignored. Authentication and TLS paths are validated before output opens.

| Keys | Meaning / limits |
| --- | --- |
| `bootstrap.servers` | Required comma-separated host[:port] entries, no protocol prefix |
| `security.protocol` | `PLAINTEXT` (default), `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`. `ssl.*` keys require SSL or SASL_SSL; `sasl.*` keys require SASL_PLAINTEXT or SASL_SSL |
| `sasl.mechanism` | PLAIN (default), SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER, AWS_MSK_IAM or GSSAPI |
| `sasl.username`, `sasl.password` | Both required for PLAIN and SCRAM; rejected for other mechanisms |
| `sasl.oauthbearer.token.endpoint.url`, `sasl.oauthbearer.client.id`, `sasl.oauthbearer.client.secret` | HTTPS token endpoint without redirects and client credentials for OAUTHBEARER. Tokens are reused until near expiration, then requested again when a broker authenticates; token requests have a 10 s timeout |
| `sasl.oauthbearer.scope` | Optional space-separated OAuth scopes |
| `sasl.oauthbearer.ssl.ca.location` | Optional PEM CA file for the token endpoint; otherwise system roots. Independent of Kafka TLS settings |
| `sasl.oauthbearer.token.file` | Alternative to OAuth client credentials: a file containing a bearer token, reread for each authentication. An external credential provider must replace it before expiry; cannot be combined with other OAuth settings |
| `sasl.kerberos.principal`, `sasl.kerberos.keytab` | Required GSSAPI principal including realm (`user@REALM`) and keytab file |
| `sasl.kerberos.config`, `sasl.kerberos.service.name` | Kerberos configuration file (default `/etc/krb5.conf`) and broker service name (default `kafka`); tickets are renewed while the Kafka client runs |
| `ssl.ca.location` | PEM CA file; otherwise system roots |
| `ssl.certificate.location`, `ssl.key.location` | PEM client certificate and unencrypted private key; supply both |
| `ssl.endpoint.identification.algorithm` | `https` (default) or `none`; `none` disables hostname checks but retains CA-chain checks |
| `client.id` | Optional suffix; source ID is `kaferry-<group>-<suffix>`, other clients `kaferry-<suffix>` |
| `compression.type` | `none` (default), gzip, snappy, lz4, zstd |
| `message.max.bytes` | Client batch ceiling including 4–5 bytes of outer framing, 1,000–1,000,000,000 bytes; default 1,000,000, adjusted by readable destination config |
| `message.timeout.ms`, `delivery.timeout.ms` | Submission deadline; positive milliseconds, aliases must agree; default worker delivery timeout |
| `queue.buffering.max.messages`, `queue.buffering.max.kbytes` | Sink admission bounds: at most worker records / the larger of worker bytes or the message limit (rounded up to KiB) |
| `queued.max.messages.kbytes`, `fetch.max.bytes`, `max.partition.fetch.bytes` | Soft per-fetch ceilings, capped by worker bytes; defaults worker bytes / 50 MiB / 1 MiB |
| `session.timeout.ms`, `heartbeat.interval.ms` | Defaults 45,000 / 3,000; heartbeat must be smaller than session |
| `max.poll.interval.ms` | Group rebalance timeout, default 300,000; at least session timeout and 30,000. Not an application poll watchdog |
| `request.timeout.ms` | Produce request timeout, default 30,000; not the record-delivery deadline |
| `socket.timeout.ms` | Request timeout overhead, default 2,000; 100–900,000; not a total-operation deadline |
| `linger.ms` | Batch wait, default 5; 0–60,000 |
| `metadata.max.age.ms`, `topic.metadata.refresh.interval.ms` | Metadata refresh age, default 300,000; aliases must agree; 10–3,600,000 |

Queue/fetch overrides must be positive. Byte queue settings use KiB. Large or compressed batches can exceed fetch ceilings; these do not cap total client memory. The ordinary consumer runs one fetch request at a time; separate requests checking offset gaps can overlap it. Kaferry's uncommitted-work caps remain independent.

`partitioner` is unsupported; select the copy's placement with destination `WITH (PARTITIONING = PRESERVE|KEY)`. Auto-commit/reset, group ID/protocol, read isolation, cooperative assignment, auto topic creation, idempotence, acknowledgements and delivery handling are owned by Kaferry. Kaferry sets these; the corresponding property names are rejected as unsupported. Sources use the SQL group, classic protocol, read-committed and no automatic offset reset. Producers use idempotence and all-ISR acknowledgements. Topic auto-creation is disabled.

### CLI and exit codes

`kaferry help` prints the overview, `kaferry help sql` prints grammar and defaults, and `kaferry help status` prints saved verdicts, all to stdout with exit code 0.

`run`, `explain` and `status` accept exactly one input: `--file/-f path`, `--sql/-e statement`, or `--file -`. `--config/-c path` loads TOML. `explain` and `status` write text or `--json` to stdout; `run` keeps stdout empty during copies. Events are JSON on stderr; errors also print a plain-text message after cleanup. Status verdicts use stdout, including nonzero verdicts. Shutdown timeout diagnostics name the operation still running.

#### Exit codes

| Normal exit | Meaning |
| --- | --- |
| 0 | This worker [finished its input or reached its record cap](#how-a-copy-ends) or a signal cleanly stopped uncapped FOLLOW. Status is `complete` or `complete_within_disorder`, or help or a preview printed. SKIP counts do not change the code |
| 1 | Copy, inspection, output, cleanup or the shutdown deadline failed, or a signal arrived before completion of startup, a file copy, or a bounded Kafka copy or capped FOLLOW copy. Conflicts after the first consumed record also exit 1 |
| 2 | Usage/parse/plan/configuration error, or a validation conflict before the first consumed record (Kafka topic validation, query/source identity, replay checkpoint/lock or self-copy) |
| 3 | For a bounded Kafka copy or capped FOLLOW copy, Kafka assigned this worker no partitions, and none arrived within `worker.idle_exit` (default 30 seconds). Other workers may still be copying. Configure whether your supervisor restarts this worker on code 3 |
| 4 | Status is `incomplete`: a partition lacks a saved stop |
| 5 | Status is `unknown`: progress is absent, invalid or incompatible, or the group has an incompatible active member |
| 6 | Status is `running`: group activity or a change was observed |
| 7 | Status is `limit_reached`: saved worker record caps leave source completion unestablished; rerun the same job for another allowance |

A busy replay-group lock exits 2. Errors while writing, publishing or recovering archive data exit 1. Startup Kafka topic-validation conflicts (for example, topic authorization denied) exit 2. The first failure determines the code; cleanup cannot replace it. A second signal or external forced termination can use system signal status.

A cleanup error can occur after all records are copied and progress is saved; the error names the close operation. Resume using the same named group to avoid starting over. `ONE_OFF` always creates a new group, so rerunning it can copy records again.

## Observability

JSON `progress` logs on stderr report local counters and buffer usage every `worker.progress_interval` (default 30 s; `0` disables). The final `copy summary` is independent of this setting.

Metric export is off by default. [Setup](usage.md#monitoring).

A Collector or compatible backend receives metrics; no scrape listener or Kafka requests are added. `explain` and `status` start no telemetry.

Supported exporter environment suffixes: `ENDPOINT`, `PROTOCOL` (`http/protobuf` only), `HEADERS`, `TIMEOUT`, `INSECURE`, `CERTIFICATE` (CA PEM), `CLIENT_CERTIFICATE` and `CLIENT_KEY` (PEM pair). Prefix them with `OTEL_EXPORTER_OTLP_` or `OTEL_EXPORTER_OTLP_METRICS_`; metric-specific settings take precedence. `INSECURE`, if set, must match the endpoint scheme: `true` for `http`, `false` for `https`. Certificate settings require an HTTPS endpoint. Client certificate/key settings must form a pair under the same prefix. A generic endpoint is a base URL with `/v1/metrics` appended; a metric-specific endpoint is the complete URL. Endpoint credentials, queries and fragments are rejected. Headers use comma-separated `name=value` entries with percent-encoded values.

`OTEL_METRIC_EXPORT_INTERVAL` defaults to 60000 ms; `OTEL_METRIC_EXPORT_TIMEOUT` to 30000 ms; exporter `TIMEOUT` to 10000 ms. Durations use positive integer milliseconds. `[metrics].enabled` is the only enable switch; `OTEL_METRICS_EXPORTER` does not control it. Counters use cumulative totals without exporter retries.

Resources include `service.name=kaferry`, version, a generated `service.instance.id` and `messaging.consumer.group.name`. `OTEL_RESOURCE_ATTRIBUTES` adds deployment/query attributes; built-in identity takes precedence. Automatic attributes exclude SQL, record contents, file paths and credentials. Attributes increase the number of series; use stable deployment values.

Metric names are experimental. All describe this worker's invocation; restarts and rebalances can replay records. Source partitions and topics are not metric attributes.

| Metric (`kaferry.` prefix) | Meaning |
| --- | --- |
| `records.submitted`, `records.confirmed` | Separate cumulative counts accepted and acknowledged by the destination; not unique records |
| `records.filtered`, `records.skipped` | Separate cumulative counts of examined records excluded by timestamp or offset filters, and dropped by error policy |
| `buffer.entries`, `buffer.entry_limit` | Unsaved tracked progress entries and their worker cap; entries can include cursor gaps and exclude a record deferred before tracking |
| `buffer.estimated_size`, `buffer.estimated_size_limit` | Tracked cap-accounting bytes and their cap; not payload/wire bytes or total memory |
| `partitions.held`, `partitions.stopped` | Current held partitions and held partitions at their stop rule, split by reason: `timestamp`, `tip`, `eof`, or `worker_record_cap`; not group completion |
| `progress.saves` | Nonempty save operations, including startup and metadata-only writes; `outcome=success|partial|failure` reflects requested partitions |
| `progress.save.duration` | Save duration in seconds |
| `progress.last_success` | Unix seconds of the last nonempty save where every requested partition succeeded; absent until then |
| `loop.iterations` | Completed loop steps, including quiet polls; not successful copying or group health |

Snapshots refresh at most once per second and once at exit. Gauges may lag current state; metric delivery is best effort. Invalid transport settings fail before output opens. Receiver failures produce a redacted, rate-limited warning and copying continues. Cleanup attempts final export for at most one second within the remaining process deadline. Lost exports, short runs, crashes and receiver outages can leave missing metrics; saved source progress is unaffected. Lag sampling and an HTTP health endpoint are not provided.
