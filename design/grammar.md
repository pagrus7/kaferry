# kafka-fwd2 statement grammar

Syntax for the semantics in `decisions.md`; if they disagree, `decisions.md` wins and this file is fixed. Stage 1 uses the examples below as table-driven parser and planner cases.

## Lexical rules

- Keywords are case-insensitive. Connection aliases match `[A-Za-z_][A-Za-z0-9_]*`.
- Strings are single-quoted; `''` inside a string is one quote.
- Comments: `--` to end of line, and non-nested `/* ... */`.
- Exactly one statement; the final `;` is optional.

## EBNF

```ebnf
statement   = [ connections ] insert select [ where ] options [ ";" ] ;

connections = "WITH" connection { "," connection } ;
connection  = "CONNECTION" alias "AS" "(" property { "," property } ")" ;
property    = string "=" ( string | env ) ;        (* librdkafka key and value *)
env         = "ENV" "(" string ")" ;

insert      = "INSERT" "INTO" endpoint ;
select      = "SELECT" "*" "FROM" endpoint ;
endpoint    = "TOPIC" string                       (* 'topic' or 'topic@alias' *)
            | "FILE" "cbor" "(" string ")" ;       (* path *)

where       = "WHERE" ( between | bound [ "AND" bound ] ) ;
between     = "RECORD_TIMESTAMP" "BETWEEN" time "AND" time ;
bound       = "RECORD_TIMESTAMP" compare time ;
compare     = ">=" | ">" | "<" | "<=" ;
time        = instant { ( "+" | "-" ) interval } ;
instant     = "TIMESTAMP" string | "NOW" "(" ")" ;
interval    = "INTERVAL" string unit ;             (* string holds a non-negative integer *)
unit        = "SECOND" | "MINUTE" | "HOUR" | "DAY" ;

options     = "OPTIONS" "(" option { "," option } ")" ;
option      = "GROUP" "=" ( string | "ONE_OFF" )
            | "START" "=" ( "EARLIEST" | "LATEST" )
            | "CUTOFF" "=" interval
            | "ON_ERROR" "=" ( "FAIL" | "SKIP" )
            | "ON_MISSING_TIMESTAMP" "=" ( "FAIL" | "SKIP" )
            | "ON_REJECTED_TIMESTAMP" "=" ( "FAIL" | "SKIP" )
            | "ON_SOURCE_FILE_CHANGED" "=" ( "FAIL" | "CONTINUE" ) ;
```

## Planner rules

The grammar accepts some statements that planning rejects:

- `GROUP` is required; every option appears at most once.
- `START` cannot be combined with `WHERE`.
- `ON_ERROR` cannot be combined with any specific error policy.
- `CUTOFF` requires a topic source with an upper bound. `ON_SOURCE_FILE_CHANGED` requires a file source; `ON_REJECTED_TIMESTAMP` a topic destination; `ON_MISSING_TIMESTAMP` a `WHERE` clause.
- At most one lower and one upper bound. Timestamps are RFC 3339 with `Z` or an explicit offset and at most millisecond precision. The normalized `[lo, hi)` must be non-empty.
- Reject a destination that is the source: the same file (including path aliases), or the same connection alias and topic.
- A topic without `@alias` uses the connection `default`, which must exist.

## Valid examples

Topic→topic, fixed window, inline connection:

```sql
WITH CONNECTION dr AS (
  'bootstrap.servers' = 'dr-kafka:9092',
  'sasl.username'     = 'copier',
  'sasl.password'     = ENV('DR_PASSWORD')
)
INSERT INTO TOPIC 'orders@dr'
SELECT * FROM TOPIC 'orders@prod'
WHERE RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T00:00:00Z'
  AND RECORD_TIMESTAMP <  TIMESTAMP '2026-10-02T00:00:00Z'
OPTIONS (GROUP = 'orders-dr-2026-10-01');
```

Topic→file, follow from the earliest retained record, default connection:

```sql
INSERT INTO FILE cbor('/data/audit.cbor')
SELECT * FROM TOPIC 'audit'
OPTIONS (GROUP = 'audit-archive', START = EARLIEST);
```

File→topic, last seven days, skipping timestamps the destination rejects:

```sql
INSERT INTO TOPIC 'orders@staging'
SELECT * FROM FILE cbor('/data/orders.cbor')
WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '7' DAY
OPTIONS (GROUP = 'orders-staging-load', ON_REJECTED_TIMESTAMP = SKIP);
```

File→file, inclusive range, one-off group (normalizes to `[09:00:00.000, 09:30:00.001)`):

```sql
INSERT INTO FILE cbor('/data/incident.cbor')
SELECT * FROM FILE cbor('/data/orders.cbor')
WHERE RECORD_TIMESTAMP BETWEEN TIMESTAMP '2026-10-01T09:00:00Z'
                           AND TIMESTAMP '2026-10-01T09:30:00Z'
OPTIONS (GROUP = ONE_OFF);
```

Topic→topic, relative window with cutoff; the window is fixed on the group's first run:

```sql
INSERT INTO TOPIC 'clicks@analytics'
SELECT * FROM TOPIC 'clicks@prod'
WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '1' HOUR
  AND RECORD_TIMESTAMP <  NOW()
OPTIONS (GROUP = 'clicks-backfill-1', CUTOFF = INTERVAL '5' MINUTE, ON_ERROR = SKIP);
```

## Invalid examples

| Statement or fragment | Expected error |
| --- | --- |
| `... WHERE RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T00:00:00Z' OPTIONS (GROUP = 'g', START = EARLIEST)` | `START` cannot be combined with a time filter |
| `OPTIONS (GROUP = 'g', ON_ERROR = SKIP, ON_MISSING_TIMESTAMP = FAIL)` | `ON_ERROR` cannot be mixed with specific error policies |
| `... FROM FILE cbor('a.cbor') ... OPTIONS (GROUP = 'g', CUTOFF = INTERVAL '1' MINUTE)` | `CUTOFF` applies to topic sources |
| `OPTIONS (START = EARLIEST)` | `GROUP` is required |
| `OPTIONS (GROUP = 'a', GROUP = 'b')` | repeated option `GROUP` |
| `INSERT INTO TOPIC 'a@x' SELECT * FROM TOPIC 'a@x' ...` | destination is the source topic |
| `INSERT INTO FILE cbor('a.cbor') SELECT * FROM FILE cbor('./a.cbor') ...` | destination would overwrite the source file |
| `SELECT key FROM TOPIC 'a' ...` | only `SELECT *` is supported |
| `WHERE RECORD_TIMESTAMP >= NOW() OR ...` | expected `OPTIONS` |
| `WHERE RECORD_TIMESTAMP > TIMESTAMP '...' AND RECORD_TIMESTAMP >= TIMESTAMP '...'` | two lower bounds |
| `RECORD_TIMESTAMP >= NOW() AND RECORD_TIMESTAMP < NOW() - INTERVAL '1' HOUR` | empty time range |
| `TIMESTAMP '2026-10-01T00:00:00'` | timestamp needs `Z` or a UTC offset |
| `TIMESTAMP '2026-10-01T00:00:00.0005Z'` | sub-millisecond precision |
| `INTERVAL '1' MONTH` | unsupported interval unit |
| `TOPIC 'orders'` with no `default` connection | no connection for `orders` |
| two statements separated by `;` | exactly one statement |

## Open points

Grammar choices made here that `decisions.md` does not settle:

- Several inline connections are comma-separated after one `WITH`; property keys are quoted strings.
- Alias case sensitivity (proposed: case-sensitive, matching TOML profile names).
- Whether `START` applies to file sources.
- The destination retry duration is configurable only in TOML.
