"""Validate selected replicated fault exposures against independent safety proof."""

import json
from datetime import datetime
from pathlib import Path

from tools.ci.suites import REPLICATED_CASES, fault_timeout_catalog
from tools.qualification.fault_results import observation_path, validate_case


IDENTITY = ('source_commit', 'binary_sha256', 'binary_platform', 'execution_platform')


def stamp(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def vector(values):
    return isinstance(values, list) and len(values) == 24 and all(type(n) is int and n >= 0 for n in values)


def advances(after, before, delta=2):
    return vector(after) and vector(before) and all(a > b+delta for a, b in zip(after, before))


def topology(state, side, size):
    parts = state['topology'][side]
    return (len(parts) == 24 and all(type(p['Leader']) is int and 1 <= p['Leader'] <= 3 and
            len(p['ISR']) == size and len(set(p['ISR'])) == size and p['Leader'] in p['ISR'] and
            set(p['ISR']) <= {1, 2, 3} for p in parts))


def broker_case(directory, id, identity):
    root = Path(directory)
    case = json.loads((root/('broker-'+id.split('/')[1])/'replicated-broker.json').read_text())
    if (case['id'] != id or case['source_partitions'] != 24 or
            case['timeout_milliseconds'] != fault_timeout_catalog()[id]):
        raise ValueError('replicated broker case differs from its registered configuration')
    safety_path = observation_path(root, case['safety_case'], 'case.json')
    safety = validate_case(safety_path)
    if (safety['id'] != id or safety['execution'] != 'candidate CLI' or
            safety['parameters'] != dict(source_partitions=24, routing='PRESERVE') or
            any(safety[key] != identity[key] for key in IDENTITY)):
        raise ValueError('replicated broker proof differs from the selected executable identity')
    publication = case['publishing']
    if (not vector(publication['offered_by_partition']) or
            not 0 < publication['max_offer_gap_seconds'] <= 2 or
            publication['offered_by_partition'] != publication['acknowledged_by_partition'] or
            min(publication['acknowledged_by_partition']) <= 0 or
            not stamp(publication['started']) < stamp(publication['stopped']) <= safety['writers_stopped_at'] or
            publication['acknowledged_by_partition'] != [p['expected'] for p in sorted(
                safety['independent_reconciliation']['partitions'], key=lambda p: p['partition'])]):
        raise ValueError('source publications were not completely drained and reconciled')
    baseline = case['baseline']
    if not vector(baseline['saved']) or min(baseline['saved']) <= 0:
        raise ValueError('broker fault lacks a nonempty saved baseline on every partition')
    workers = case['worker_measurements']
    if (not workers or len(workers) > len(case['events'])+1 or case['restarts'] != len(workers)-1 or
            case['restart_scope'] != 'same selected binary, SQL, group and worker1 route'):
        raise ValueError('broker fault has an unsupported worker restart scope')
    for index, worker in enumerate(workers):
        if (type(worker['pid']) is not int or worker['pid'] <= 0 or
                worker['binary_sha256'] != identity['binary_sha256'] or worker['worker_route'] != 1 or
                worker['exit_code'] != (0 if index == len(workers)-1 else 1) or
                worker['rss_samples'] <= 0 or worker['rss_failures'] != 0 or
                not 0 < worker['sampled_peak_rss_bytes'] <= 512*1024**2 or
                worker['record_cap'] != 1024 or worker['byte_cap'] != 128*1024**2 or
                not 0 <= worker['peak_uncommitted_records'] <= 1024 or
                not 0 <= worker['peak_uncommitted_bytes'] <= 128*1024**2 or
                not safety['started_at'] <= stamp(worker['started_at']) < stamp(worker['stopped_at']) <= safety['writers_stopped_at'] or
                not worker['reason']):
            raise ValueError('broker fault worker exits or resource caps are invalid')
        if worker['exit_code'] == 1 and not worker['terminal_error']:
            raise ValueError('broker fault restart omitted the observed terminal failure')
    if {(w['pid'], w['exit_code']) for w in workers} != {(w['pid'], w['exit_code']) for w in safety['writer_observations']}:
        raise ValueError('broker fault worker measurements differ from independent exit observations')
    events, windows = case['events'], safety['fault_events']
    rolling = id.startswith('broker/rolling-')
    if len(events) != (3 if rolling else 1) or len(windows) != len(events):
        raise ValueError('broker fault omitted a required injection')
    previous = baseline
    for index, (event, window) in enumerate(zip(events, windows)):
        fault, recovered = event['fault'], event['recovered']
        role, targets = fault['role'], fault['targets']
        side = 'source' if id in ('broker/source-leader', 'broker/rolling-source') else 'destination'
        isr_loss = id == 'broker/destination-isr-loss'
        kind = 'restart' if rolling else 'cut' if isr_loss else 'kill'
        injection = window['injections']
        if (role['cluster'] != side or fault['fault'] != dict(kind=kind) or
                window['target'] != role or len(injection) != 1 or injection[0]['kind'] != kind or
                any(injection[0]['target'][key] != fault[key] for key in ('role', 'fault', 'targets', 'start')) or
                not stamp(previous['at']) < stamp(fault['start']) <= injection[0]['at'] <= stamp(fault['heal']) <= window['healed_at'] or
                not stamp(fault['heal']) <= stamp(recovered['at']) <= window['healed_at'] or
                not recovered['broker_healthy'] or not topology(recovered, side, 3) or
                not advances(recovered['saved'], previous['saved']) or
                not advances(recovered['publishing']['acknowledged_by_partition'], previous['publishing']['acknowledged_by_partition'])):
            raise ValueError('broker fault target, health, ISR or recovered progress is incomplete')
        broker = targets[0]['broker']
        if (not 1 <= broker <= 3 or len({t['container'] for t in targets}) != 1 or
                any(t['cluster'] != side or t['broker'] != broker or not t['container'] or
                    t['service'] != f'{side}-{broker}' for t in targets)):
            raise ValueError('broker fault targets do not identify one resolved owned broker')
        if rolling:
            if (role != dict(kind='broker', cluster=side, partition=0, broker=index+1) or broker != index+1 or
                    not topology(event['before'], side, 3) or
                    stamp(event['before']['at']) < stamp(previous['at'])):
                raise ValueError('rolling restart did not visit all three brokers after observed full ISR recovery')
        elif (role['kind'] != 'leader' or role['topic'] != case['destination_topic' if isr_loss or side == 'destination' else 'source_topic'] or role['partition'] != 0):
            raise ValueError('leader fault did not resolve the selected topic partition at injection')
        if isr_loss:
            lost, stalled = event['isr_lost'], event['stalled']
            controllers = event['controller_targets']
            lost_part, stalled_part = lost['topology']['destination'][0], stalled['topology']['destination'][0]
            if (len(workers) != 1 or len(targets) != 1 or targets[0]['listener'] != 'internal' or role['listener'] != 'internal' or
                    len(controllers) != 3 or {t['broker'] for t in controllers} != {1, 2, 3} or
                    len({t['container'] for t in controllers}) != 3 or
                    any(t['cluster'] != 'destination' or t['listener'] != 'controller' or not t['container'] for t in controllers) or
                    not lost['broker_healthy'] or event['min_insync_replicas'] != 2 or
                    lost_part != dict(Leader=broker, ISR=[broker]) or stalled_part != lost_part or
                    not stamp(fault['start']) < stamp(lost['at']) <= stamp(stalled['at']) < stamp(fault['heal']) or
                    stalled['destination_ends'][0] != lost['destination_ends'][0] or
                    stalled['saved'][0] > lost['destination_ends'][0] or
                    stalled['publishing']['acknowledged_by_partition'][0] <= lost['publishing']['acknowledged_by_partition'][0]+3 or
                    stalled['cli_progress']['submitted'] <= stalled['cli_progress']['confirmed']):
                raise ValueError('ISR loss lacks observed ISR1/minISR2, unresolved output and same-process retry')
        else:
            if len(targets) != 6 or {t['listener'] for t in targets} != {'internal', 'controller', 'admin', 'worker1', 'worker2', 'worker3'}:
                raise ValueError('container fault omitted an owned broker route')
            if not rolling:
                failover = event['failover']
                part = failover['topology'][side][0]
                if (part['Leader'] == broker or len(part['ISR']) != 2 or
                        not stamp(fault['start']) < stamp(failover['at']) < stamp(fault['heal']) or
                        not advances(failover['publishing']['acknowledged_by_partition'], previous['publishing']['acknowledged_by_partition'])):
                    raise ValueError('leader kill lacks a replacement leader before original broker healing')
        previous = recovered
    safety['broker_exposure'] = case
    for reference in (safety['safety'], safety['bootstrap']):
        reference['observations'] = (safety_path.parent/reference['observations']).resolve().relative_to(root.resolve()).as_posix()
    return safety


def collect(directory, selected, identity, run):
    if (not run or not run.isalnum() or not selected or len(selected) != len(set(selected)) or
            any(id not in REPLICATED_CASES for id in selected)):
        raise ValueError('invalid replicated fault selection')
    paths = sorted((Path(directory)/'fault-oracle'/run).glob('*/observations/case.json'))
    observed = [json.loads(path.read_text())['id'] for path in paths]
    if sorted(observed) != sorted(selected):
        raise ValueError('replicated evidence does not match the exact selected cases')
    cases = []
    for id in selected:
        if id == 'ownership/zombie-worker':
            from tools.qualification.ownership_audit import validate
            case = validate(directory, identity)
        else:
            try:
                case = broker_case(directory, id, identity)
            except (KeyError, TypeError, IndexError) as error:
                raise ValueError('malformed replicated broker evidence') from error
        observed_path = paths[observed.index(id)]
        if (Path(directory)/case['safety']['observations']).resolve() != (observed_path.parent/'samples.jsonl').resolve():
            raise ValueError('replicated proof references another observer run')
        cases.append(case)
    return cases
