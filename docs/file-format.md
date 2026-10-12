# CBOR archive v1

Wire format and durability reference. One binary file, in submission order, without compression or segmentation. Version 1 is frozen.

## Frames

Magic: eight bytes `4b 46 57 44 32 0d 0a 1a`. Then one framed header and framed records until EOF.

| Field | Encoding |
| --- | --- |
| Payload length | Nonzero unsigned 32-bit big-endian |
| Payload | CBOR map, exactly the declared length; writer uses canonical encoding |
| CRC | Unsigned 32-bit big-endian CRC32C/Castagnoli of length bytes **and** payload |

Header limit: 65,536 bytes. Record limit: `worker.max_frame_bytes`, default 64 MiB. Reject duplicate/unknown keys, indefinite lengths, tags, nesting above eight. The record's `headers` array limit is `max(16, max_frame_bytes/32)` (integer division); writer and reader enforce it.

## Maps

Header: `version` (unsigned 1), `file_id` (random 128-bit hex text), `group` (text), `fingerprint` (SHA256 hex text).

| Record field | Type |
| --- | --- |
| `key`, `value` | Byte string or null; empty differs from null |
| `headers` | Ordered array of maps: `name` byte string, `value` byte string/null |
| `timestamp_ms` | Signed integer Unix milliseconds/null |
| `timestamp_type` | Text: `create_time`, `log_append_time`, `unknown` |
| `source` | Optional map: `topic` text, `partition` integer, `offset` integer; diagnostics only |

All record fields except `source` are required; `headers` is never null. Header names use bytes, preserving arbitrary Go string contents. Duplicate headers retain sequence.

## Durability

Writes are acknowledged after `fsync`, then source progress is saved. Reading stops at a partial final frame; a complete corrupt frame stops the copy with an error. Restart can duplicate complete frames. [Output recovery](../design/decisions.md#output-recovery).
