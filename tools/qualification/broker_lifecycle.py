"""Bind broker worker measurements to immutable raw exit and lifecycle files."""

import hashlib
import json

from tools.qualification.fault_results import observation_path


SAMPLED_SCOPE = 'sampled progress only; forced copy shutdown omits final lifetime highwater'
FORCED_COPY_PHASES = {
    'shutdown timed out while finishing writes; exiting without further cleanup': 'finishing writes',
    'shutdown timed out while saving progress; exiting without further cleanup': 'saving progress',
}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def nonnegative(value):
    return type(value) is int and value >= 0


def progress(row):
    if (not nonnegative(row['submitted']) or not nonnegative(row['confirmed']) or
            row['confirmed'] > row['submitted']):
        raise ValueError('invalid broker raw progress counts')
    return {key: row.get(key, 0) for key in ('msg', 'submitted', 'confirmed', 'peak_uncommitted_records', 'peak_uncommitted_bytes')}


def raw_work(data):
    if not data or len(data) > 8*1024**2 or not data.endswith(b'\n'):
        raise ValueError('broker final log is empty, oversized or incomplete')
    terminals, samples, summary = [], [], None
    for line in data.decode('utf-8').splitlines():
        line = line.strip()
        if not line:
            continue
        if not line.startswith('{'):
            terminals.append(line)
            continue
        row = json.loads(line)
        if row.get('msg') == 'progress':
            progress(row)
            if not all(nonnegative(row[key]) for key in ('pending_entries', 'pending_estimated_bytes')):
                raise ValueError('invalid broker raw current work')
            samples.append(row)
        elif row.get('msg') == 'copy summary':
            progress(row)
            if summary is not None or not all(nonnegative(row[key]) for key in ('peak_uncommitted_records', 'peak_uncommitted_bytes')):
                raise ValueError('invalid broker raw summary')
            summary = {key: row[key] for key in ('msg', 'submitted', 'confirmed', 'peak_uncommitted_records', 'peak_uncommitted_bytes')}
    if not samples:
        raise ValueError('broker raw log lacks progress')
    return terminals, samples, summary


def load_lifecycle(directory, worker):
    try:
        return _load_lifecycle(directory, worker)
    except (KeyError, TypeError, IndexError, AttributeError, UnicodeError, OSError) as error:
        raise ValueError('unreadable or malformed broker lifecycle evidence') from error


def _load_lifecycle(directory, worker):
    number = worker['lifetime']
    if type(number) is not int or number <= 0:
        raise ValueError('invalid broker lifetime number')
    path = observation_path(directory, worker['lifecycle'], f'worker-{number}.lifecycle.json')
    if path.stat().st_size > 16*1024**2:
        raise ValueError('broker lifecycle snapshot exceeds its bounded evidence size')
    witness = json.loads(path.read_text())
    identity = dict(number=number, pid=worker['pid'], started_at=worker['started_at'], binary_sha256=worker['binary_sha256'])
    if (witness['lifetime'] != identity or witness['stopped_at'] != worker['stopped_at'] or
            witness['exit_code'] != worker['exit_code'] or witness['raw_log_error'] or
            type(witness['driver_intent_before_wait']) is not list or not isinstance(witness['termination_signal'], str)):
        raise ValueError('broker lifecycle differs from measured worker')
    raw_path = observation_path(directory, witness['raw_log'], f'worker-{number}.exited.cli.log')
    if raw_path.stat().st_size > 8*1024**2:
        raise ValueError('broker raw log exceeds its bounded evidence size')
    raw = raw_path.read_bytes()
    if witness['raw_log_bytes'] != len(raw):
        raise ValueError('broker raw log length differs from lifecycle')
    terminal, samples, summary = raw_work(raw)
    if (terminal != witness['terminal_lines'] or '\n'.join(terminal) != worker.get('terminal_error', '') or
            summary != worker['copy_summary'] or len(samples) != worker['progress_observations'] or
            max(row['pending_entries'] for row in samples) != worker['sampled_max_pending_entries'] or
            max(row['pending_estimated_bytes'] for row in samples) != worker['sampled_max_pending_estimated_bytes']):
        raise ValueError('broker raw log contradicts measured work or terminal')
    return witness, raw


def binds_before(witness, raw, before):
    prefix = before['worker']
    length = prefix['log_prefix_bytes']
    if (prefix['lifetime'] != witness['lifetime'] or type(length) is not int or not 0 < length <= len(raw) or
            not raw[:length].endswith(b'\n') or digest(raw[:length]) != prefix['log_prefix_sha256']):
        return False
    _, samples, summary = raw_work(raw[:length])
    return summary is None and progress(samples[-1]) == before['cli_progress']
