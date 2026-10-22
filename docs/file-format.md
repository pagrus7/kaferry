# Archive formats

`FILE avro('directory')` stores full Kafka records in compressed files. [Format, rotation and recovery](../design/archive.md).

`FILE cbor('path')` retains the single-file format below. CBOR and Avro cannot be mixed in one file-to-file copy.

## CBOR archive v1

One file in submission order, uncompressed.

## Frames

Magic: eight bytes `4b 46 57 44 32 0d 0a 1a`. Then one framed header and framed records until EOF.

| Field | Encoding |
| --- | --- |
| Payload length | Nonzero unsigned 32-bit big-endian |
| Payload | CBOR map, exactly the declared length; writer uses canonical encoding |
| CRC | Unsigned 32-bit big-endian CRC32C/Castagnoli of length bytes **and** payload |

[Validation limits](../design/archive.md#cbor-validation-limits).

## Maps

Header: `version` (unsigned 1), `file_id` (random 128-bit hex text), `group` (text), `fingerprint` (SHA256 hex text).

| Record field | Type |
| --- | --- |
| `key`, `value` | Byte string or null; empty differs from null |
| `headers` | Ordered array of maps: `name` byte string, `value` byte string/null |
| `timestamp_ms` | Signed integer Unix milliseconds/null |
| `timestamp_type` | Text: `create_time`, `log_append_time`, `unknown` |
| `source` | Optional map: `topic` text, `partition` integer, `offset` integer; diagnostics only |

All record fields except `source` are required; `headers` is never null. Header names use bytes, so NUL and non-UTF-8 names round-trip. Duplicate headers retain sequence.

## Durability

Writes are acknowledged after `fsync`, then source progress is saved. Reading stops at a partial final frame; a complete corrupt frame stops the copy with an error. Restart can duplicate complete frames. [Output recovery](../design/decisions.md#output-recovery).
