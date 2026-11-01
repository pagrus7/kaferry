# Kaferry statement grammar

Syntax reference. See [copy behavior](decisions.md) for completion and restart rules.

## Lexical rules

- Keywords are case-insensitive. Connection aliases match `[A-Za-z_][A-Za-z0-9_]*` and are case-sensitive, matching TOML profile names.
- Strings are single-quoted; `''` inside a string is one quote.
- Comments: `--` to end of line, and non-nested `/* ... */`.
- Exactly one statement; the final `;` is optional.

## EBNF

```ebnf
statement   = [ connections ] insert select [ where ] [ options ] [ ";" ] ;
connections = "WITH" connection { "," connection } ;
connection  = "CONNECTION" alias "AS" "(" property { "," property } ")" ;
property    = string "=" ( string | env ) ;
env         = "ENV" "(" string ")" ;

insert      = "INSERT" "INTO" endpoint [ "WITH" "(" sink_option { "," sink_option } ")" ] ;
select      = "SELECT" "*" "FROM" endpoint [ "WITH" "(" source_option { "," source_option } ")" ] ;
endpoint    = "TOPIC" string                       (* 'topic' or 'topic@alias' *)
            | "ARCHIVE" string ;                   (* archive directory *)
source_option = "GROUP" "=" ( string | "ONE_OFF" )
            | "START" "=" ( "EARLIEST" | "LATEST" )
            | ( "MAX_STALENESS" | "MAX_OUT_OF_ORDER" ) "=" ( interval | "OFF" ) ;
sink_option = "PARTITIONING" "=" ( "PRESERVE" | "KEY" )
            | "ROTATE_AFTER_SIZE" "=" integer [ "k" | "m" | "g" ]
            | "ROTATE_AFTER_AGE" "=" interval ;

where       = "WHERE" ( predicates | branch { "OR" branch } [ "AND" predicates ] ) ;
predicates  = predicate { "AND" predicate } ;
branch      = "(" predicates ")" ;  (* disjoint PARTITION IN/BETWEEN plus offset bounds only *)
predicate   = between | bound | partition | offset ;
offset      = "RECORD_OFFSET" ( compare integer | "<" "LATEST"
            | "BETWEEN" integer "AND" integer ) ;
partition   = "PARTITION" [ "NOT" ] ( "IN" "(" integer { "," integer } ")"
            | "BETWEEN" integer "AND" integer ) ;
between     = "RECORD_TIMESTAMP" "BETWEEN" time "AND" time ;
bound       = "RECORD_TIMESTAMP" compare time ;
compare     = ">=" | ">" | "<" | "<=" ;
time        = instant { ( "+" | "-" ) interval } ;
instant     = "TIMESTAMP" string | "NOW" "(" ")" ;
interval    = "INTERVAL" string unit ;
unit        = "SECOND" | "MINUTE" | "HOUR" | "DAY" ;

options     = "OPTIONS" "(" copy_option { "," copy_option } ")" ;
copy_option = "WORKER_STOP_AFTER_RECORDS" "=" integer
            | "ON_ERROR" "=" ( "FAIL" | "SKIP" )
            | "ON_MISSING_TIMESTAMP" "=" ( "FAIL" | "SKIP" )
            | "ON_REJECTED_TIMESTAMP" "=" ( "FAIL" | "SKIP" ) ;
```

## Validation rules

Planning or startup enforces:

- Both endpoint `WITH` clauses and final `OPTIONS` are optional; if present, each must contain a setting. Omitted `GROUP` means `ONE_OFF`: generate and log a fresh group for each run. Explicit group names must be nonblank. Quoted `GROUP = 'ONE_OFF'` is an ordinary group name. Settings, inline connection aliases and a connection's properties cannot repeat.
- Archive rotation uses positive bytes (case-insensitive binary k/m/g suffixes) and positive intervals; see [settings and defaults](../docs/reference.md#archive-sink). `ARCHIVE 'directory'` names a filesystem directory containing a Kaferry archive.
- Source `WITH` precedes `WHERE`. Timestamp bounds supply the starting rule and cannot be combined with explicit `START`; see [Kafka start positions](decisions.md#kafka-start-positions). Archive sources take no `START`. `MAX_STALENESS` and `MAX_OUT_OF_ORDER` belong to source `WITH`; record-error policies and worker stops belong to final `OPTIONS`.
- `ON_ERROR` cannot be combined with any specific error policy.
- `WORKER_STOP_AFTER_RECORDS` is from 1 to 9,007,199,254,740,992; it caps accepted writes per worker per invocation, not records read or a shared group total.
- `PARTITIONING` requires a topic destination. See [Kafka output](decisions.md#kafka-output) for routing and destination layout requirements, and [settings](../docs/reference.md#kafka-sink) for its default.
- Intervals use nonnegative integers; `DAY` is 86,400 seconds. Both timestamp completion settings require an upper timestamp bound and reject an explicit offset end. `MAX_STALENESS` requires Kafka input. `OFF` disables the corresponding rule; zero is allowed. [Endpoint settings and defaults](../docs/reference.md#endpoint-with-properties).
- Offset literals are nonnegative signed 64-bit integers. `BETWEEN lo AND hi` includes both ends and is equivalent to `>= lo AND <= hi`. Comparisons normalize to an inclusive lower and exclusive upper bound; empty ranges and overflowing adjustments fail. Each grouped branch requires `PARTITION IN` or `PARTITION BETWEEN` and an upper offset bound; selectors must be disjoint. Unlisted partitions are excluded. Shared timestamp predicates follow the branches. Ungrouped timestamp `OR` is unsupported.
- Kafka explicit numeric starts/ends must be within retained positions at initialization. `LATEST` freezes the readable committed end on first assignment; resume reuses it. A timestamp-derived start beyond that end becomes an empty snapshot. File offsets refer to original source offsets, not replay cursors; `LATEST` means the pinned selection's EOF.
- `ON_REJECTED_TIMESTAMP` requires a topic destination; `ON_MISSING_TIMESTAMP` requires timestamp bounds in `WHERE`.
- An ungrouped filter may contain one partition predicate; each grouped branch requires its own. Use `PARTITION [NOT] IN (...)` or `PARTITION [NOT] BETWEEN lo AND hi`; combine an ungrouped predicate with timestamp bounds using `AND` in either order. IDs are integers from 0 to 2147483647; lists are nonempty, sorted and deduplicated. `BETWEEN` includes every ID from lo through hi, with at most 65,536 IDs across all partition `BETWEEN` predicates in one statement. Every named ID must exist in source metadata at startup, including exclusions; archives use completed-file headers. `IN` and `BETWEEN` select those IDs; `NOT IN` and `NOT BETWEEN` select all others. Kafka discovers additions while workers run; archives pin matching completed files once and keep that selection on resume.
- At most one lower and one upper bound. Timestamps are RFC 3339 with `Z` or an explicit offset and at most millisecond precision. The normalized `[lo, hi)` must be non-empty.
- At startup, reject a destination that is the same archive directory (including path aliases) or the same Kafka cluster ID and topic as the source.
- A topic without `@alias` uses the connection `default`, which must exist.

## Valid examples

Aliases not defined inline need TOML profiles; `ENV` variables must be set. TLS connections need a trusted broker CA; set `ssl.ca.location` if system roots do not include it.

Minimal topic copy: start a fresh group from earliest retained records, then follow. Rerunning copies them again; use a named `GROUP` to resume or share work:

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod';
```

Select source partitions, then follow; replace `IN` with `NOT IN` to copy every other partition, including later additions:

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-repair')
WHERE PARTITION IN (0, 2);
```

Archive partition selection uses the original Kafka IDs in file headers; it applies across all source topics/clusters represented in the directory:

```sql
INSERT INTO ARCHIVE '/data/repair'
SELECT * FROM ARCHIVE '/data/orders' WITH (GROUP = 'archive-repair')
WHERE PARTITION NOT IN (1, 3);
```

Minimal archive replay: copy the current completed files and exit:

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM ARCHIVE '/data/orders';
```

Topic→topic, fixed window, inline PLAIN over TLS connection:

```sql
WITH CONNECTION dr AS (
  'bootstrap.servers' = 'dr-kafka:9093',
  'security.protocol' = 'SASL_SSL',
  'sasl.mechanism'    = 'PLAIN',
  'sasl.username'     = 'copier',
  'sasl.password'     = ENV('DR_PASSWORD')
)
INSERT INTO TOPIC 'orders@dr'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-dr-2026-10-01')
WHERE RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T00:00:00Z'
  AND RECORD_TIMESTAMP <  TIMESTAMP '2026-10-02T00:00:00Z';
```

Topic→archive, follow from the earliest retained record, default connection:

```sql
INSERT INTO ARCHIVE '/data/audit'
SELECT * FROM TOPIC 'audit' WITH (GROUP = 'audit-archive');
```

Archive→topic, last seven days, skipping timestamps known to be outside the destination's accepted bounds:

```sql
INSERT INTO TOPIC 'orders@staging' WITH (PARTITIONING = KEY)
SELECT * FROM ARCHIVE '/data/orders' WITH (GROUP = 'orders-staging-load')
WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '7' DAY
OPTIONS (ON_REJECTED_TIMESTAMP = SKIP);
```

Archive→archive, inclusive range, one-off group (normalizes to `[09:00:00.000, 09:30:00.001)`):

```sql
INSERT INTO ARCHIVE '/data/incident'
SELECT * FROM ARCHIVE '/data/orders'
WHERE RECORD_TIMESTAMP BETWEEN TIMESTAMP '2026-10-01T09:00:00Z'
                           AND TIMESTAMP '2026-10-01T09:30:00Z';
```

Topic→topic, relative window with staleness. Saved progress keeps the resolved `NOW()` window for restarts; workers starting together can resolve different windows ([time window](decisions.md#time-window)):

```sql
INSERT INTO TOPIC 'clicks@analytics'
SELECT * FROM TOPIC 'clicks@prod' WITH (GROUP = 'clicks-backfill-1', MAX_STALENESS = INTERVAL '5' MINUTE)
WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '1' HOUR
  AND RECORD_TIMESTAMP <  NOW()
OPTIONS (ON_ERROR = SKIP);
```

Topic→topic, stop each worker after accepting 100 records for writing:

```sql
INSERT INTO TOPIC 'orders@staging'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-sample', START = EARLIEST)
OPTIONS (WORKER_STOP_AFTER_RECORDS = 100);
```

For `RECORD_OFFSET < LATEST`, see [copy the current backlog and stop](../docs/usage.md#copy-the-current-backlog-and-stop).

Use a shorter known disorder bound while allowing ten minutes for arrivals:

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod'
WITH (GROUP = 'orders-live-hour', MAX_STALENESS = INTERVAL '10' MINUTE,
      MAX_OUT_OF_ORDER = INTERVAL '10' SECOND)
WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '1' HOUR
  AND RECORD_TIMESTAMP < NOW();
```

Follow an ordered replay until timestamp progress establishes completion:

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod'
WITH (GROUP = 'orders-replay', MAX_STALENESS = OFF,
      MAX_OUT_OF_ORDER = INTERVAL '0' SECOND)
WHERE RECORD_TIMESTAMP >= TIMESTAMP '2026-10-22T09:00:00Z'
  AND RECORD_TIMESTAMP < TIMESTAMP '2026-10-22T10:00:00Z';
```

Select a different range for each source partition:

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-offset-repair')
WHERE (PARTITION BETWEEN 0 AND 1 AND RECORD_OFFSET BETWEEN 100 AND 499)
   OR (PARTITION IN (2) AND RECORD_OFFSET >= 900 AND RECORD_OFFSET < LATEST);
```

File replay can use the same timestamp-disorder assumption explicitly. Without it, the complete pinned selection is examined:

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM ARCHIVE '/archive/orders'
WITH (GROUP = 'orders-file-hour', MAX_OUT_OF_ORDER = INTERVAL '30' SECOND)
WHERE RECORD_TIMESTAMP >= TIMESTAMP '2026-10-22T09:00:00Z'
  AND RECORD_TIMESTAMP < TIMESTAMP '2026-10-22T10:00:00Z';
```

## Invalid examples

SQL errors identify the first error found by one-based line and character (code point) column. LF ends a line; tabs count as one column. File and stdin input include their source name. Input values and SQL excerpts are omitted. Related declarations include their locations. Fix the reported error, then rerun.

```text
"copy.sql": line 3, column 29: expected FAIL or SKIP; found identifier
```

| Statement or fragment | Rejected because |
| --- | --- |
| `WHERE PARTITION IN ()` | partition lists must be nonempty |
| `WHERE PARTITION IN (0) OR PARTITION IN (1)` | predicates combine only with `AND` |
| `OPTIONS (GROUP = 'g')` | `GROUP` belongs in source `WITH` |
| `FROM TOPIC 'a' WITH (GROUP='g', ON_ERROR=SKIP)` | error policies belong in final `OPTIONS` |
| `FROM ARCHIVE 'archive' WITH (GROUP='g', START=EARLIEST)` | `START` requires a topic source |
| `FROM ARCHIVE 'archive' WITH (GROUP='g', MAX_STALENESS=INTERVAL '1' MINUTE)` | `MAX_STALENESS` requires a topic source |
| `FROM TOPIC 'a' WITH (GROUP='g', START=EARLIEST) WHERE RECORD_TIMESTAMP < NOW()` | `START` cannot be combined with timestamp bounds |
| `FROM TOPIC 'a' WITH ()` | present clauses need at least one setting |
| `FROM TOPIC 'a' WITH (GROUP='')` | explicit group names must be nonblank |
| `WITH (GROUP='a', GROUP='b')` on a source | duplicate `GROUP` |
| `OPTIONS (ON_ERROR=SKIP, ON_MISSING_TIMESTAMP=FAIL)` | broad and specific policies cannot be mixed |
| `INSERT INTO ARCHIVE 'out' WITH (PARTITIONING=KEY)` | partitioning requires a topic destination |
| `INSERT INTO TOPIC 'a@x' SELECT * FROM TOPIC 'a@y' ...` where both aliases reach one cluster | destination is the source topic |
| `INSERT INTO ARCHIVE 'archive' SELECT * FROM ARCHIVE './archive' ...` | destination is the source archive |
| `SELECT key FROM TOPIC 'a' ...` | only `SELECT *` is supported |
| `WHERE RECORD_TIMESTAMP >= NOW() OR ...` | bounds combine only with `AND` |
| `WHERE RECORD_TIMESTAMP > TIMESTAMP '...' AND RECORD_TIMESTAMP >= TIMESTAMP '...'` | two lower bounds |
| `RECORD_TIMESTAMP >= NOW() AND RECORD_TIMESTAMP < NOW() - INTERVAL '1' HOUR` | empty range |
| `TIMESTAMP '2026-10-01T00:00:00'` | timestamp needs a zone |
| `TIMESTAMP '2026-10-01T00:00:00.0005Z'` | sub-millisecond precision |
| `INTERVAL '1' MONTH` | unsupported interval unit |
| `TOPIC 'orders'` without a defined `default` connection | no connection |
| two statements separated by `;` | exactly one statement |
