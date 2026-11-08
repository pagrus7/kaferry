"""Validate fault observation files before they become qualification evidence."""

import json
from collections import Counter
import math
from pathlib import Path
import subprocess
import sys
import uuid

from tools.qualification.fault_oracle import INITIALIZATION_ERRORS, checksum
from tools.ci.suites import FAULT_CASES, RECOVERY_CASES, fault_timeout_catalog


def nonnegative_int(value):
    if type(value) is not int or value < 0:
        raise ValueError('fault offsets and counts must be nonnegative integers')


def timestamp(value, start=0, finish=float('inf')):
    if type(value) not in (int, float) or not math.isfinite(value) or not start <= value <= finish:
        raise ValueError('fault timestamp must be finite and within its case interval')


def observation_path(root, reference, name):
    relative = Path(reference['observations'])
    if relative.is_absolute() or '..' in relative.parts:
        raise ValueError('fault observations must use a contained relative path')
    path = (Path(root)/relative).resolve()
    if not path.is_relative_to(Path(root).resolve()):
        raise ValueError('fault observation path escapes the evidence directory')
    if path.name != name or checksum(path) != reference['sha256']:
        raise ValueError('fault observation checksum mismatch')
    return path


def validate_bootstrap(case, root, partitions):
    setup = case['bootstrap']
    timestamp(setup['started_at'])
    timestamp(setup['ready_at'], setup['started_at'], case['started_at'])
    timestamp(setup['deadline_at'], setup['ready_at'])
    nonnegative_int(setup['attempts'])
    baseline, ready, errors, previous = None, None, 0, setup['started_at']
    with observation_path(root, setup, 'bootstrap.jsonl').open() as lines:
        for line in lines:
            row = json.loads(line)
            timestamp(row['started_at'], setup['started_at'], setup['ready_at'])
            timestamp(row['finished_at'], row['started_at'], setup['ready_at'])
            if row['kind'] == 'initialization-error':
                nonnegative_int(row['attempt'])
                if (baseline is not None or row['started_at'] < previous or
                        row['attempt'] != errors+1 or row['driver_error'] not in INITIALIZATION_ERRORS):
                    raise ValueError('invalid transient observer initialization evidence')
                errors += 1
                previous = row['finished_at']
            elif row['kind'] == 'sample':
                if baseline is not None or row['reason'] != 'bootstrap' or row['started_at'] < previous:
                    raise ValueError('observer initialization lacks one ordered baseline')
                if (len(row['partitions']) != len(partitions) or {p['partition'] for p in row['partitions']} != partitions or
                        len(row['destination_ends']) != len(partitions) or len(row['source_ends']) != len(partitions)):
                    raise ValueError('observer bootstrap baseline omitted partitions')
                for part in row['partitions']:
                    for key in ('partition', 'saved', 'confirmed_prefix'):
                        nonnegative_int(part[key])
                    if part['saved'] or part['confirmed_prefix']:
                        raise ValueError('observer bootstrap lacks fresh saved/destination state')
                for source, destination in zip(row['source_ends'], row['destination_ends']):
                    nonnegative_int(source)
                    nonnegative_int(destination)
                    if destination:
                        raise ValueError('observer bootstrap destination was not empty')
                for key in ('saved_observed_at', 'destination_snapshot_started_at', 'destination_snapshot_finished_at'):
                    timestamp(row[key], row['started_at'], row['finished_at'])
                if not row['started_at'] <= row['saved_observed_at'] <= row['destination_snapshot_started_at'] <= row['destination_snapshot_finished_at'] <= row['finished_at']:
                    raise ValueError('observer bootstrap baseline capture times are unordered')
                baseline = row
            elif row['kind'] == 'initialization-ready':
                nonnegative_int(row['attempt'])
                if (ready is not None or baseline is None or row['attempt'] != errors+1 or
                        row['started_at'] < previous or row['started_at'] > baseline['started_at'] or
                        row['finished_at'] < baseline['finished_at'] or row['finished_at'] != setup['ready_at'] or
                        row['deadline_at'] != setup['deadline_at']):
                    raise ValueError('observer ready preceded its exact initialization baseline')
                ready = row
            else:
                raise ValueError('observer initialization did not complete')
    if baseline is None or ready is None or errors+1 != setup['attempts']:
        raise ValueError('observer initialization baseline or attempt evidence is missing')


def configure(env):
    root = Path(__file__).resolve().parents[2]
    python = env.get('KAFERRY_ORACLE_PYTHON') or str(root / '.cache/qualification' /
        f'venv-{sys.version_info.major}.{sys.version_info.minor}/bin/python')
    if not Path(python).is_file():
        raise ValueError('fault/recovery require the independent checker; run python3 -m tools.qualification.qualify setup')
    subprocess.run([python, '-c', 'import confluent_kafka'], check=True, timeout=15)
    env['KAFERRY_ORACLE_PYTHON'] = python
    env['KAFERRY_ORACLE_RUN'] = uuid.uuid4().hex


def _validate_observations(case, root):
    """Validate and derive bounded proof from one case and its contained JSONL."""
    if type(case.get('schema')) is not int or case['schema'] != 1 or case.get('status') != 'passed':
        raise ValueError('missing successful independent fault case')
    timestamp(case['started_at'])
    timestamp(case['finished_at'], case['started_at'])
    timestamp(case['writers_stopped_at'], case['started_at'], case['finished_at'])
    timestamp(case['duration_seconds'])
    if case['duration_seconds'] != case['finished_at']-case['started_at']:
        raise ValueError('fault case duration differs from its observed interval')
    safety = case['safety']
    if type(safety['interval_seconds']) not in (int, float) or safety['interval_seconds'] != 1 or safety['ordering'] != 'saved-before-destination-snapshot':
        raise ValueError('fault observer cadence target or ordering differs')
    for key in ('samples', 'unavailable'):
        nonnegative_int(safety[key])
    observations = observation_path(root, safety, 'samples.jsonl')
    nonnegative_int(case['parameters']['source_partitions'])
    partitions = set(range(case['parameters']['source_partitions']))
    if not partitions:
        raise ValueError('fault observation requires source partitions')
    validate_bootstrap(case, root, partitions)
    events = case['fault_events']
    if not events:
        raise ValueError('fault case has no observed fault')
    registered = fault_timeout_catalog().get(case['id'])
    if registered is None:
        raise ValueError('fault case is not registered in the timeout catalog')
    required_timeouts = {key: milliseconds/1000 for key, milliseconds in registered.items()}
    phases = [dict(before=0, fault=0, recovery=0, after=0) for _ in events]
    for event in events:
        for key in ('started_at', 'healed_at', 'closed_at', 'recovery_until', 'destination_end_observed_at'):
            timestamp(event[key], case['started_at'], case['finished_at'])
        timestamp(event['recovery_seconds'])
        if event['timeout_basis'] != required_timeouts:
            raise ValueError('fault recovery timeout budgets differ from its registered scenario')
        for value in event['timeout_basis'].values():
            timestamp(value)
            if value <= 0:
                raise ValueError('fault recovery requires positive timeout budgets')
        for offset in event['destination_start']+event['destination_end']:
            nonnegative_int(offset)
        for injection in event['injections']:
            timestamp(injection['at'], case['started_at'], case['finished_at'])
        if (not isinstance(event['target'], dict) or not event['target'] or not event['injections'] or not event['start_observation'] or not event['heal_observation'] or
                any(not isinstance(injection.get('target'), dict) or not injection['target'] or not injection.get('kind') or not injection.get('observation') for injection in event['injections']) or
                event['started_at'] > min(injection['at'] for injection in event['injections']) or
                max(injection['at'] for injection in event['injections']) > event['healed_at'] or
                event['healed_at'] > event['closed_at'] or
                event['recovery_until'] != event['healed_at']+event['recovery_seconds'] or
                event['recovery_seconds'] != sum(event['timeout_basis'].values()) or
                len(event['destination_start']) != len(partitions) or len(event['destination_end']) != len(partitions) or
                any(a > b for a, b in zip(event['destination_start'], event['destination_end']))):
            raise ValueError('fault event lacks injection, target, heal or configured recovery proof')
        if (event['recovery_observation_complete'] is not True or
                event['destination_end_observed_at'] < event['recovery_until'] or
                event['closed_at'] < event['destination_end_observed_at']):
            raise ValueError('fault receipt lacks the complete configured recovery observation')
    count, gaps, first, last = 0, 0, None, None
    max_idle, max_capture, idle_seconds, unavailable_seconds, previous_finish = 0, 0, 0, 0, None
    final_parts, source_ends = None, None
    coverage = {p: dict(partition=p, samples=0, first_observed_at=None, last_observed_at=None) for p in partitions}
    with observations.open() as lines:
        for line in lines:
            sample = json.loads(line)
            start, finish = sample['started_at'], sample['finished_at']
            timestamp(start, case['started_at'], case['finished_at'])
            timestamp(finish, start, case['finished_at'])
            if start > finish or previous_finish is not None and start < previous_finish:
                raise ValueError('fault observations overlap or reverse time')
            max_idle = max(max_idle, start-previous_finish if previous_finish is not None else 0)
            idle_seconds += start-previous_finish if previous_finish is not None else 0
            max_capture = max(max_capture, finish-start)
            previous_finish = finish
            if sample['kind'] == 'unavailable':
                if not any(event['unavailable'] and event['started_at'] <= start and finish <= event['recovery_until'] for event in events):
                    raise ValueError('unavailable observation is outside a declared fault/recovery window')
                gaps += 1
                unavailable_seconds += finish-start
                continue
            if (sample['kind'] != 'sample' or len(sample['partitions']) != len(partitions) or
                    {p['partition'] for p in sample['partitions']} != partitions):
                raise ValueError('fault safety sample failed or omitted partitions')
            for key in ('saved_observed_at', 'destination_snapshot_started_at', 'destination_snapshot_finished_at'):
                timestamp(sample[key], start, finish)
            if not start <= sample['saved_observed_at'] <= sample['destination_snapshot_started_at'] <= sample['destination_snapshot_finished_at'] <= finish:
                raise ValueError('fault sample lacks ordered saved/destination capture times')
            if len(sample['source_ends']) != len(partitions) or len(sample['destination_ends']) != len(partitions):
                raise ValueError('fault sample end offsets omitted partitions')
            for offset in sample['source_ends']+sample['destination_ends']:
                nonnegative_int(offset)
            count += 1
            first = first or start
            last = finish
            for part in sample['partitions']:
                for key in ('partition', 'saved', 'confirmed_prefix'):
                    nonnegative_int(part[key])
                if (not 0 <= part['saved'] <= part['confirmed_prefix'] <= sample['source_ends'][part['partition']] or
                        part['confirmed_prefix'] > sample['destination_ends'][part['partition']]):
                    raise ValueError('unsafe saved progress in fault receipt')
                row = coverage[part['partition']]
                row.update(samples=row['samples']+1, first_observed_at=row['first_observed_at'] or start,
                           last_observed_at=finish, final_saved=part['saved'], final_confirmed_prefix=part['confirmed_prefix'],
                           min_saved=min(row.get('min_saved', part['saved']), part['saved']),
                           max_saved=max(row.get('max_saved', part['saved']), part['saved']),
                           min_confirmed_prefix=min(row.get('min_confirmed_prefix', part['confirmed_prefix']), part['confirmed_prefix']),
                           max_confirmed_prefix=max(row.get('max_confirmed_prefix', part['confirmed_prefix']), part['confirmed_prefix']))
            final_parts, source_ends = sample['partitions'], sample['source_ends']
            for phase, event in zip(phases, events):
                injected = min(injection['at'] for injection in event['injections'])
                baseline = case['id'] == 'fault/blocking-commit' or all(part['confirmed_prefix'] > 0 for part in sample['partitions'])
                phase['before'] += finish < injected and baseline
                phase['fault'] += start >= injected and finish <= event['healed_at']
                phase['recovery'] += start >= event['healed_at'] and finish <= event['recovery_until']
                phase['after'] += (sample.get('reason') == 'post-recovery' and start >= event['closed_at'] and
                                   sample['destination_snapshot_started_at'] >= event['destination_end_observed_at'])
    if count < 2 or count != safety['samples'] or gaps != safety['unavailable']:
        raise ValueError('fault receipt lacks matching before/after observation coverage')
    reconciliation = case['independent_reconciliation']
    if start < case['writers_stopped_at']:
        raise ValueError('final independent sample precedes writer exits')
    for key in ('expected', 'verified', 'deliveries', 'duplicates', 'missing', 'corrupted', 'unexpected', 'unattributed_duplicates'):
        nonnegative_int(reconciliation[key])
    if case['id'] != 'fault/blocking-commit' and (not case.get('writer_observations') or
            any(not writer.get('observation') or type(writer.get('pid')) is not int or writer['pid'] <= 0 or
                type(writer.get('exit_code')) is not int for writer in case['writer_observations'])):
        raise ValueError('fault receipt lacks registered writer exit observations')
    if (reconciliation['implementation'] != 'librdkafka' or
            reconciliation['expected'] != reconciliation['verified'] or
            reconciliation['deliveries'] < reconciliation['verified'] or
            reconciliation['duplicates'] != reconciliation['deliveries']-reconciliation['verified'] or
            any(reconciliation[key] for key in ('missing', 'corrupted', 'unexpected', 'unattributed_duplicates')) or
            case['id'] != 'fault/blocking-commit' and reconciliation['expected'] <= 0):
        raise ValueError('independent fault reconciliation is incomplete')
    if len(reconciliation['partitions']) != len(partitions) or {p['partition'] for p in reconciliation['partitions']} != partitions:
        raise ValueError('independent fault reconciliation omitted a partition')
    if sum(part['expected'] for part in reconciliation['partitions']) != reconciliation['expected']:
        raise ValueError('independent fault reconciliation partition totals differ')
    final = {part['partition']: part for part in reconciliation['partitions']}
    for part in reconciliation['partitions']:
        for key in ('partition', 'expected', 'confirmed_prefix'):
            nonnegative_int(part[key])
    for part in reconciliation['final_progress']:
        for key in ('partition', 'saved', 'confirmed_prefix'):
            nonnegative_int(part[key])
    for part in final_parts:
        if (part['saved'] != part['confirmed_prefix'] or part['saved'] != final[part['partition']]['expected'] or
                part['saved'] != final[part['partition']]['confirmed_prefix'] or part['saved'] != source_ends[part['partition']]):
            raise ValueError('fault final saved/confirmed/expected progress differs')
    if sorted(reconciliation['final_progress'], key=lambda p: p['partition']) != sorted(final_parts, key=lambda p: p['partition']):
        raise ValueError('fault final progress differs from the last independent observation')
    for phase, event in zip(phases, events):
        if not phase['before'] or not phase['recovery'] or not phase['after'] or not event['unavailable'] and not phase['fault']:
            raise ValueError('fault evidence lacks healthy baseline, fault, recovery or post-recovery observations')
    safety.update(first_observed_at=first, last_observed_at=last, partitions=list(coverage.values()),
                  max_idle_seconds=max_idle, max_capture_seconds=max_capture, idle_seconds=idle_seconds,
                  unavailable_seconds=unavailable_seconds, phase_observations=phases,
                  scope='ordered samples; intervals between samples and unavailable observations are excluded')
    return case


def validate_observations(case, root):
    """Return the case with proof derived from its checksummed observations."""
    try:
        return _validate_observations(case, root)
    except (KeyError, TypeError, IndexError) as error:
        raise ValueError('malformed fault observation evidence') from error


def validate_case(path):
    return validate_observations(json.loads(path.read_text()), path.parent)


def collect(directory, suite, identity=None, run=None):
    directory = Path(directory)
    base = directory/'fault-oracle'
    if run is not None:
        if not run or not run.isalnum():
            raise ValueError('invalid fault observation run identity')
        base = base/run
    paths = sorted(base.glob('*/observations/case.json'))
    cases = [validate_case(path) for path in paths]
    for case, path in zip(cases, paths):
        case['safety']['observations'] = (path.parent/'samples.jsonl').relative_to(directory).as_posix()
        case['bootstrap']['observations'] = (path.parent/'bootstrap.jsonl').relative_to(directory).as_posix()
    expected = RECOVERY_CASES if suite == 'recovery' else FAULT_CASES
    if {case['id'] for case in cases} != expected:
        raise ValueError('fault cases missing from independent evidence')
    recovery = json.loads((directory/'recovery.json').read_text())
    counts = Counter(case['id'] for case in cases)
    if (not recovery.get('passed') or recovery.get('completed_cycles', 0) <= 0 or
            any(counts[id] != recovery['completed_cycles'] for id in RECOVERY_CASES) or
            any(counts[id] != 1 for id in expected-RECOVERY_CASES)):
        raise ValueError('fault case receipts do not match completed recovery cycles')
    identities = {(case['source_commit'], case['binary_sha256']) for case in cases}
    if len(identities) != 1:
        raise ValueError('fault evidence mixes source commits or binaries')
    commit, digest = identities.pop()
    platforms = {(case['binary_platform'], case['execution_platform']) for case in cases}
    if len(platforms) != 1:
        raise ValueError('fault evidence mixes binary or execution platforms')
    binary_platform, execution_platform = platforms.pop()
    if identity and (commit != identity['source_commit'] or digest != identity['binary_sha256']):
        raise ValueError('fault evidence does not match the tested binary and source')
    report = dict(schema=1, status='passed', source_commit=commit, binary_sha256=digest, cases=cases,
                  oracle_run=run, binary_platform=binary_platform, execution_platform=execution_platform)
    if identity:
        for field in ('binary_platform', 'execution_platform'):
            if field in identity:
                if report[field] != identity[field]:
                    raise ValueError('fault platform does not match the tested binary and source')
    temporary = directory/'fault-results.json.tmp'
    temporary.write_text(json.dumps(report, indent=2)+'\n')
    temporary.replace(directory/'fault-results.json')
    return report
