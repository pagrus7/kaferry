# Kaferry statement grammar

Syntax reference. See [copy behavior](decisions.md) for completion and restart rules.

## Lexical rules

- Keywords are case-insensitive. Connection aliases match `[A-Za-z_][A-Za-z0-9_]*` and are case-sensitive, matching TOML profile names.
- Strings are single-quoted; `''` inside a string is one quote.
- Comments: `--` to end of line, and non-nested `/* ... */`.
- Exactly one statement; the final `;` is optional.

## EBNF

```ebnf
statement   = [ connections ] insert select [ where ] [ limit ] options [ ";" ] ;

connections = "WITH" connection { "," connection } ;
connection  = "CONNECTION" alias "AS" "(" property { "," property } ")" ;
property    = string "=" ( string | env ) ;        (* supported connection key and value *)
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
interval    = "INTERVAL" string unit ;             (* non-negative integer; DAY = 86,400 s *)
unit        = "SECOND" | "MINUTE" | "HOUR" | "DAY" ;
limit       = "LIMIT" integer ;                    (* unquoted positive decimal integer *)

options     = "OPTIONS" "(" option { "," option } ")" ;
option      = "GROUP" "=" ( string | "ONE_OFF" )
            | "START" "=" ( "EARLIEST" | "LATEST" )
            | "CUTOFF" "=" interval
            | "ON_ERROR" "=" ( "FAIL" | "SKIP" )
            | "ON_MISSING_TIMESTAMP" "=" ( "FAIL" | "SKIP" )
            | "ON_REJECTED_TIMESTAMP" "=" ( "FAIL" | "SKIP" ) ;
```

## Validation rules

Planning or startup enforces:

- `GROUP` is required and must not be blank. Options, inline connection aliases and a connection's properties cannot repeat. Quoted `GROUP = 'ONE_OFF'` is an ordinary group name; unquoted `ONE_OFF` generates one.
- A topic source needs exactly one of `WHERE` and `START`; a file source takes no `START`.
- `ON_ERROR` cannot be combined with any specific error policy.
- `LIMIT` is from 1 to 9,007,199,254,740,992; it caps accepted writes per worker per invocation, not records read or a shared group total.
- `CUTOFF` requires a topic source with an upper bound. `ON_REJECTED_TIMESTAMP` requires a topic destination; `ON_MISSING_TIMESTAMP` a `WHERE` clause.
- At most one lower and one upper bound. Timestamps are RFC 3339 with `Z` or an explicit offset and at most millisecond precision. The normalized `[lo, hi)` must be non-empty.
- Reject a destination that is the source: the same file (including path aliases), or, at startup, the same Kafka cluster ID and topic.
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

File→topic, last seven days, skipping timestamps known to be outside the destination's accepted bounds:

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

Topic→topic, relative window with cutoff. Saved progress keeps the resolved `NOW()` window for restarts; workers starting together can resolve different windows ([time window](decisions.md#time-window)):

```sql
INSERT INTO TOPIC 'clicks@analytics'
SELECT * FROM TOPIC 'clicks@prod'
WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '1' HOUR
  AND RECORD_TIMESTAMP <  NOW()
OPTIONS (GROUP = 'clicks-backfill-1', CUTOFF = INTERVAL '5' MINUTE, ON_ERROR = SKIP);
```

Topic→topic, sample at most 100 records with a fresh group:

```sql
INSERT INTO TOPIC 'orders@staging'
SELECT * FROM TOPIC 'orders@prod'
LIMIT 100
OPTIONS (GROUP = ONE_OFF, START = EARLIEST);
```

## Invalid examples

SQL errors identify the first error found by one-based line and character (code point) column. LF ends a line; tabs count as one column. File and stdin input include their source name. Input values and SQL excerpts are omitted. Related declarations include their locations. Fix the reported error, then rerun.

```text
"copy.sql": line 3, column 29: expected FAIL or SKIP; found identifier
```

| Statement or fragment | Rejected because |
| --- | --- |
| `... WHERE RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T00:00:00Z' OPTIONS (GROUP = 'g', START = EARLIEST)` | `START` cannot be combined with a time filter |
| `... WHERE RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T00:00:00Z' OPTIONS (GROUP = 'g', ON_ERROR = SKIP, ON_MISSING_TIMESTAMP = FAIL)` | `ON_ERROR` cannot be mixed with specific error policies |
| `... FROM FILE cbor('a.cbor') ... OPTIONS (GROUP = 'g', CUTOFF = INTERVAL '1' MINUTE)` | `CUTOFF` applies to topic sources |
| `... FROM FILE cbor('a.cbor') OPTIONS (GROUP = 'g', START = EARLIEST)` | `START` applies to topic sources |
| `... FROM TOPIC 'a' OPTIONS (GROUP = 'g')` | `START` is required without a time filter |
| `OPTIONS (START = EARLIEST)` | `GROUP` is required |
| `OPTIONS (GROUP = 'a', GROUP = 'b')` | repeated option `GROUP` |
| `INSERT INTO TOPIC 'a@x' SELECT * FROM TOPIC 'a@y' ...` where `x` and `y` reach one cluster | destination is the source topic |
| `INSERT INTO FILE cbor('a.cbor') SELECT * FROM FILE cbor('./a.cbor') ...` | destination is the source file |
| `SELECT key FROM TOPIC 'a' ...` | only `SELECT *` is supported |
| `WHERE RECORD_TIMESTAMP >= NOW() OR ...` | `OR` is unsupported; bounds combine with `AND` |
| `WHERE RECORD_TIMESTAMP > TIMESTAMP '...' AND RECORD_TIMESTAMP >= TIMESTAMP '...'` | two lower bounds |
| `RECORD_TIMESTAMP >= NOW() AND RECORD_TIMESTAMP < NOW() - INTERVAL '1' HOUR` | empty time range |
| `TIMESTAMP '2026-10-01T00:00:00'` | timestamp needs `Z` or a UTC offset |
| `TIMESTAMP '2026-10-01T00:00:00.0005Z'` | sub-millisecond precision |
| `INTERVAL '1' MONTH` | unsupported interval unit |
| `TOPIC 'orders'` with no `default` connection | no connection for `orders` |
| two statements separated by `;` | exactly one statement |
