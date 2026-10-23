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
select      = "SELECT" "*" "FROM" endpoint "WITH" "(" source_option { "," source_option } ")" ;
endpoint    = "TOPIC" string                       (* 'topic' or 'topic@alias' *)
            | "FILE" "avro" "(" string ")" ;       (* archive directory *)
source_option = "GROUP" "=" ( string | "ONE_OFF" )
            | "START" "=" ( "EARLIEST" | "LATEST" )
            | "CUTOFF" "=" interval ;
sink_option = "PARTITIONING" "=" ( "PRESERVE" | "KEY" )
            | "ROTATE_AFTER_SIZE" "=" integer [ "k" | "m" | "g" ]
            | "ROTATE_AFTER_AGE" "=" interval ;

where       = "WHERE" ( between | bound [ "AND" bound ] ) ;
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

- Source `WITH` is required and must contain a nonblank `GROUP`. Options, inline connection aliases and a connection's properties cannot repeat. Quoted `GROUP = 'ONE_OFF'` is an ordinary group name; unquoted `ONE_OFF` generates one.
- Destination `WITH` is optional. Avro rotation uses positive bytes (case-insensitive binary k/m/g suffixes) and positive intervals; defaults are 1g and one hour. `avro(...)` takes only the directory.
- Source `WITH` precedes `WHERE`. A topic source needs exactly one of `WHERE` and `START`; a file source takes no `START`. `CUTOFF` belongs to source `WITH`; all record-error policies and worker stops belong to optional final `OPTIONS`.
- `ON_ERROR` cannot be combined with any specific error policy.
- `WORKER_STOP_AFTER_RECORDS` is from 1 to 9,007,199,254,740,992; it caps accepted writes per worker per invocation, not records read or a shared group total.
- `PARTITIONING` requires a topic destination and defaults to `PRESERVE`: original partition N goes to destination N. Every input partition ID must exist, including IDs whose records the filter excludes. Counts need not match. `KEY` uses original key bytes and the destination key partitioner. Missing partitions fail under every error policy.
- Intervals use nonnegative integers; `DAY` is 86,400 seconds. `CUTOFF` requires a topic source with an upper bound. `ON_REJECTED_TIMESTAMP` requires a topic destination; `ON_MISSING_TIMESTAMP` a `WHERE` clause.
- At most one lower and one upper bound. Timestamps are RFC 3339 with `Z` or an explicit offset and at most millisecond precision. The normalized `[lo, hi)` must be non-empty.
- At startup, reject a destination that is the same archive directory (including path aliases) or the same Kafka cluster ID and topic as the source.
- A topic without `@alias` uses the connection `default`, which must exist.

## Valid examples

Aliases not defined inline need TOML profiles; `ENV` variables must be set. TLS connections need a trusted broker CA; set `ssl.ca.location` if system roots do not include it.

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
  AND RECORD_TIMESTAMP <  TIMESTAMP '2026-10-02T00:00:00Z'
;
```

Topic→file, follow from the earliest retained record, default connection:

```sql
INSERT INTO FILE avro('/data/audit')
SELECT * FROM TOPIC 'audit' WITH (GROUP = 'audit-archive', START = EARLIEST)
;
```

File→topic, last seven days, skipping timestamps known to be outside the destination's accepted bounds:

```sql
INSERT INTO TOPIC 'orders@staging' WITH (PARTITIONING = KEY)
SELECT * FROM FILE avro('/data/orders') WITH (GROUP = 'orders-staging-load')
WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '7' DAY
OPTIONS (ON_REJECTED_TIMESTAMP = SKIP);
```

File→file, inclusive range, one-off group (normalizes to `[09:00:00.000, 09:30:00.001)`):

```sql
INSERT INTO FILE avro('/data/incident')
SELECT * FROM FILE avro('/data/orders') WITH (GROUP = ONE_OFF)
WHERE RECORD_TIMESTAMP BETWEEN TIMESTAMP '2026-10-01T09:00:00Z'
                           AND TIMESTAMP '2026-10-01T09:30:00Z'
;
```

Topic→topic, relative window with cutoff. Saved progress keeps the resolved `NOW()` window for restarts; workers starting together can resolve different windows ([time window](decisions.md#time-window)):

```sql
INSERT INTO TOPIC 'clicks@analytics'
SELECT * FROM TOPIC 'clicks@prod' WITH (GROUP = 'clicks-backfill-1', CUTOFF = INTERVAL '5' MINUTE)
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

## Invalid examples

SQL errors identify the first error found by one-based line and character (code point) column. LF ends a line; tabs count as one column. File and stdin input include their source name. Input values and SQL excerpts are omitted. Related declarations include their locations. Fix the reported error, then rerun.

```text
"copy.sql": line 3, column 29: expected FAIL or SKIP; found identifier
```

| Statement or fragment | Rejected because |
| --- | --- |
| `OPTIONS (GROUP = 'g')` | `GROUP` belongs in source `WITH` |
| `FROM TOPIC 'a' WITH (GROUP='g', ON_ERROR=SKIP)` | error policies belong in final `OPTIONS` |
| `FROM FILE avro('archive') WITH (GROUP='g', START=EARLIEST)` | `START` requires a topic source |
| `FROM FILE avro('archive') WITH (GROUP='g', CUTOFF=INTERVAL '1' MINUTE)` | `CUTOFF` requires a topic source |
| `FROM TOPIC 'a' WITH (GROUP='g', START=EARLIEST) WHERE RECORD_TIMESTAMP < NOW()` | `START` cannot be combined with `WHERE` |
| `FROM TOPIC 'a' WITH (GROUP='g')` | `START` is required without `WHERE` |
| `WITH (START=EARLIEST)` on a source | `GROUP` is required |
| `WITH (GROUP='a', GROUP='b')` on a source | duplicate `GROUP` |
| `OPTIONS (ON_ERROR=SKIP, ON_MISSING_TIMESTAMP=FAIL)` | broad and specific policies cannot be mixed |
| `INSERT INTO FILE avro('out') WITH (PARTITIONING=KEY)` | partitioning requires a topic destination |
| `INSERT INTO TOPIC 'a@x' SELECT * FROM TOPIC 'a@y' ...` where both aliases reach one cluster | destination is the source topic |
| `INSERT INTO FILE avro('archive') SELECT * FROM FILE avro('./archive') ...` | destination is the source archive |
| `SELECT key FROM TOPIC 'a' ...` | only `SELECT *` is supported |
| `WHERE RECORD_TIMESTAMP >= NOW() OR ...` | bounds combine only with `AND` |
| `WHERE RECORD_TIMESTAMP > TIMESTAMP '...' AND RECORD_TIMESTAMP >= TIMESTAMP '...'` | two lower bounds |
| `RECORD_TIMESTAMP >= NOW() AND RECORD_TIMESTAMP < NOW() - INTERVAL '1' HOUR` | empty range |
| `TIMESTAMP '2026-10-01T00:00:00'` | timestamp needs a zone |
| `TIMESTAMP '2026-10-01T00:00:00.0005Z'` | sub-millisecond precision |
| `INTERVAL '1' MONTH` | unsupported interval unit |
| `TOPIC 'orders'` without a defined `default` connection | no connection |
| two statements separated by `;` | exactly one statement |
