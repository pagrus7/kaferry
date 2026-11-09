"""Validate selected replicated fault exposures against independent safety proof."""

import json
import re
from datetime import datetime
from pathlib import Path

from tools.ci.suites import REPLICATED_CASES, fault_timeout_catalog
from tools.qualification.fault_results import observation_path, validate_case
from tools.qualification.broker_readiness import validate as validate_readiness
from tools.qualification.broker_lifecycle import (FORCED_COPY_PHASES, SAMPLED_SCOPE, binds_before, load_lifecycle)


IDENTITY = ('source_commit', 'binary_sha256', 'binary_platform', 'execution_platform')
NEGOTIATION_EOF = ('broker closed the connection immediately during api versions negotiation, which often happens '
                   'when the broker requires TLS but the client is using plaintext: is TLS missing?')
FIRST_RESPONSE_EOF = ('broker closed the connection immediately after a request was issued, which often happens '
                      'when SASL is required but not provided: is SASL missing?')
SOURCE_OPERATION_EOFS = tuple(label+NEGOTIATION_EOF for label in ('poll source: ', 'save progress: ',
                            'poll source: save progress: ', 'poll source: unable to join group session: '))
EXACT_SCOPE = 'exact lifetime highwater from final copy summary'
BROKER_ERRORS = ('context deadline exceeded', 'REQUEST_TIMED_OUT', 'NOT_COORDINATOR', 'COORDINATOR_NOT_AVAILABLE',
                'UNKNOWN_MEMBER_ID', 'ILLEGAL_GENERATION', 'NOT_LEADER_OR_FOLLOWER', 'BROKER_NOT_AVAILABLE',
                'connection refused', 'i/o timeout', 'EOF', 'delivery timeout',
                'outstanding writes did not finish within 10 seconds')


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


def healthy_copy(state):
    progress = state['cli_progress']
    return (vector(state['saved']) and min(state['saved']) > 0 and
            vector(state['publishing']['acknowledged_by_partition']) and
            min(state['publishing']['acknowledged_by_partition']) > 0 and
            0 < progress['confirmed'] <= progress['submitted'] and
            all(topology(state, side, 3) for side in ('source', 'destination')))


def affected_failure(case, worker, side):
    proof = worker['failure_context']
    window = proof['window']
    if type(window) is not int or not 0 <= window < len(case['events']):
        return False
    event = case['events'][window]
    fault = event['fault']
    before = event.get('before', case['baseline'])
    return (proof['role'] == fault['role'] and proof['kind'] == fault['fault']['kind'] and
            proof['kind'] in ('kill', 'restart') and proof['role']['cluster'] == side and
            proof['transport'] == 'plaintext' and proof['before'] == before and healthy_copy(before) and
            proof['start'] == fault['start'] and proof['heal'] == fault['heal'] and
            proof['observed_exit_at'] == worker['stopped_at'] and
            stamp(before['at']) < stamp(proof['start']) < stamp(proof['observed_exit_at']) <= stamp(proof['heal']))


def forced_copy_failure(case, worker, witness, raw):
    proof = worker['failure_context']
    window = proof['window']
    if type(window) is not int or not 0 <= window < len(case['events']):
        return False
    event, active = case['events'][window], witness['active_fault']
    fault = event['fault']
    before = event.get('before', case['baseline'])
    registered = {
        'broker/source-leader': ('source', 'leader', 'kill'),
        'broker/destination-leader': ('destination', 'leader', 'kill'),
        'broker/rolling-source': ('source', 'broker', 'restart'),
        'broker/rolling-destination': ('destination', 'broker', 'restart'),
    }
    rule = registered.get(case['id'])
    recovery = sum(case['timeout_milliseconds'].values())
    if (not rule or (proof['role']['cluster'], proof['role']['kind'], proof['kind']) != rule or
            proof['case'] != case['id'] or proof['recovery_milliseconds'] != recovery or
            proof['role'] != fault['role'] or proof['kind'] != fault['fault']['kind'] or
            proof['start'] != fault['start'] or proof['heal'] != fault['heal'] or
            proof['observed_exit_at'] != witness['stopped_at'] or proof['before'] != before or
            proof['transport'] != 'plaintext' or proof['exit_witness'] != witness or
            ('\n'+worker['terminal_error']+'\n').encode() not in b'\n'+raw or
            witness['exit_code'] != 1 or witness['wait_error'] != 'exit status 1' or witness['termination_signal'] or
            witness['driver_intent_before_wait'] or not active or
            any(active[key] != proof[key] for key in ('case', 'recovery_milliseconds', 'window', 'role', 'kind', 'start', 'observed_exit_at', 'before', 'transport')) or
            active['heal'] not in ('0001-01-01T00:00:00Z', proof['heal']) or not healthy_copy(before) or
            not stamp(witness['lifetime']['started_at']) < stamp(before['at']) < stamp(proof['start']) < stamp(witness['stopped_at']) <= stamp(proof['heal'])+recovery/1000 or
            not binds_before(witness, raw, before)):
        return False
    return True


def source_operation_eof(terminal):
    lines = terminal.split('\n')
    if lines[0] not in SOURCE_OPERATION_EOFS:
        return False
    remaining = lines[1:]
    if remaining and remaining[0] == SOURCE_OPERATION_EOFS[1]:
        remaining = remaining[1:]
    return remaining in ([], ['close Kafka input: '+NEGOTIATION_EOF])


def worker_work(case, worker, witness, raw):
    available, summary = worker['lifetime_highwater_available'], worker['copy_summary']
    terminal = worker.get('terminal_error', '')
    terminal_lines = terminal.splitlines()
    if (type(available) is not bool or type(worker['progress_observations']) is not int or worker['progress_observations'] <= 0 or
            type(worker['sampled_max_pending_entries']) is not int or not 0 <= worker['sampled_max_pending_entries'] <= 1024 or
            type(worker['sampled_max_pending_estimated_bytes']) is not int or not 0 <= worker['sampled_max_pending_estimated_bytes'] <= 128*1024**2):
        raise ValueError('broker worker lacks observed current-work cap evidence')
    if not available:
        if (summary is not None or worker['peak_uncommitted_records'] is not None or worker['peak_uncommitted_bytes'] is not None or
                worker['cap_measurement_scope'] != SAMPLED_SCOPE or worker['exit_code'] != 1 or terminal not in FORCED_COPY_PHASES or
                worker['unavailable_highwater_phase'] != FORCED_COPY_PHASES[terminal] or
                not (network_forced_copy_failure(case, worker, witness, raw) if case['id'] == 'network/destination-outage'
                     else forced_copy_failure(case, worker, witness, raw))):
            raise ValueError('unavailable lifetime highwater requires the bound active forced-copy exit')
        return
    if (worker['unavailable_highwater_phase'] or worker['cap_measurement_scope'] != EXACT_SCOPE or not isinstance(summary, dict) or summary['msg'] != 'copy summary' or
            type(summary['submitted']) is not int or type(summary['confirmed']) is not int or
            not 0 <= summary['confirmed'] <= summary['submitted'] or
            type(worker['peak_uncommitted_records']) is not int or type(worker['peak_uncommitted_bytes']) is not int or
            not worker['sampled_max_pending_entries'] <= worker['peak_uncommitted_records'] <= 1024 or
            not worker['sampled_max_pending_estimated_bytes'] <= worker['peak_uncommitted_bytes'] <= 128*1024**2 or
            summary['peak_uncommitted_records'] != worker['peak_uncommitted_records'] or
            summary['peak_uncommitted_bytes'] != worker['peak_uncommitted_bytes']):
        raise ValueError('broker worker lacks exact lifetime work-cap evidence')
    if worker['exit_code'] == 0:
        if terminal:
            raise ValueError('completed broker worker has a terminal failure')
    elif source_operation_eof(terminal):
        if (summary['submitted'] <= 0 or summary['submitted'] != summary['confirmed'] or
                not source_operation_failure(case, worker, witness, raw)):
            raise ValueError('labelled source/save EOF lacks a bound all-confirmed active source fault')
    elif (terminal_lines and terminal_lines[0] in (NEGOTIATION_EOF, FIRST_RESPONSE_EOF) and
          all(line == terminal_lines[0] for line in terminal_lines)):
        diagnostic = terminal_lines[0]
        context = (network_outage_exit(case, worker, witness, raw) and
                   raw.split(b'\n').count(diagnostic.encode()) == len(terminal_lines)
                   if case['id'].startswith('network/') else diagnostic == NEGOTIATION_EOF and affected_failure(case, worker, 'destination'))
        if len(terminal_lines) != summary['submitted']-summary['confirmed'] or not context:
            raise ValueError('negotiation EOF lacks every failed write and an affected active plaintext destination fault')
    elif not terminal or any(diagnostic in terminal for diagnostic in (NEGOTIATION_EOF, FIRST_RESPONSE_EOF)) or any(line.startswith('shutdown timed out while ') or not any(message in line for message in BROKER_ERRORS) for line in terminal_lines):
        raise ValueError('broker worker has an unsupported terminal failure')


def source_operation_failure(case, worker, witness, raw):
    proof = worker['failure_context']
    if (case['id'] not in ('broker/source-leader', 'broker/rolling-source') or
            not forced_copy_failure(case, worker, witness, raw) or
            not stamp(proof['observed_exit_at']) < stamp(proof['heal']) or
            proof['role']['partition'] != 0 or any(proof['role'].get(key) for key in ('worker', 'listener', 'group'))):
        return False
    targets = case['events'][proof['window']]['fault']['targets']
    if (proof['targets'] != targets or witness['active_fault']['targets'] != targets or
            proof['timeout_milliseconds'] != case['timeout_milliseconds'] or
            witness['active_fault']['timeout_milliseconds'] != case['timeout_milliseconds'] or
            len(targets) != 6):
        return False
    first = targets[0]
    listeners = {'internal', 'controller', 'admin', 'worker1', 'worker2', 'worker3'}
    return (type(first['broker']) is int and 1 <= first['broker'] <= 3 and
            isinstance(first['container'], str) and bool(first['container']) and
            isinstance(first['proxy_container'], str) and bool(first['proxy_container']) and
            {target['listener'] for target in targets} == listeners and
            all(target['cluster'] == 'source' and type(target['broker']) is int and target['broker'] == first['broker'] and
                target['container'] == first['container'] and target['proxy_container'] == first['proxy_container'] and
                target['service'] == f'source-{first["broker"]}' and
                target['proxy'] == target['service']+'-'+target['listener'] for target in targets))


def copy_case(directory, id, identity):
    root = Path(directory)
    family, name = id.split('/')
    case = json.loads((root/(family+'-'+name)/('replicated-'+family+'.json')).read_text())
    if (case['id'] != id or case['source_partitions'] != 24 or
            case['timeout_milliseconds'] != fault_timeout_catalog()[id]):
        raise ValueError('replicated fault case differs from its registered configuration')
    safety_path = observation_path(root, case['safety_case'], 'case.json')
    safety = validate_case(safety_path)
    if (safety['id'] != id or safety['execution'] != 'candidate CLI' or
            safety['parameters'] != dict(source_partitions=24, routing='PRESERVE') or
            any(safety[key] != identity[key] for key in IDENTITY)):
        raise ValueError('replicated fault proof differs from the selected executable identity')
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
    if not healthy_copy(baseline):
        raise ValueError('replicated fault lacks healthy copying and a nonempty saved baseline on every partition')
    workers = case['worker_measurements']
    if (not workers or len(workers) > len(case['events'])+1 or case['restarts'] != len(workers)-1 or
            case['restart_scope'] != 'same selected binary, SQL, group and worker1 route'):
        raise ValueError('replicated fault has an unsupported worker restart scope')
    lifecycles = []
    for index, worker in enumerate(workers):
        if (type(worker['pid']) is not int or worker['pid'] <= 0 or
                worker['binary_sha256'] != identity['binary_sha256'] or worker['worker_route'] != 1 or
                worker['exit_code'] != (0 if index == len(workers)-1 else 1) or
                worker['rss_samples'] <= 0 or worker['rss_failures'] != 0 or
                not 0 < worker['sampled_peak_rss_bytes'] <= 512*1024**2 or
                worker['record_cap'] != 1024 or worker['byte_cap'] != 128*1024**2 or
                not safety['started_at'] <= stamp(worker['started_at']) < stamp(worker['stopped_at']) <= safety['writers_stopped_at'] or
                not worker['reason']):
            raise ValueError('replicated fault worker exits or resource caps are invalid')
        witness, raw = load_lifecycle(root/(family+'-'+name), worker)
        if family == 'network' and not network_rss_matches(worker, witness):
            raise ValueError('network RSS measurement differs from its immutable exit snapshot')
        if (worker['lifetime'] != index+1 or stamp(witness['recorded_at']) < stamp(worker['stopped_at'])):
            raise ValueError('broker lifecycle was not observed after its sole Wait')
        worker_work(case, worker, witness, raw)
        lifecycles.append((witness, raw))
    if case['exact_lifetime_work_caps_qualified'] is not all(w['lifetime_highwater_available'] for w in workers):
        raise ValueError('replicated case exact lifetime cap claim differs from available measurements')
    if {(w['pid'], w['exit_code']) for w in workers} != {(w['pid'], w['exit_code']) for w in safety['writer_observations']}:
        raise ValueError('replicated fault worker measurements differ from independent exit observations')
    for reference in (safety['safety'], safety['bootstrap']):
        reference['observations'] = (safety_path.parent/reference['observations']).resolve().relative_to(root.resolve()).as_posix()
    return case, safety, lifecycles


def network_rss_matches(worker, witness):
    rss = witness.get('rss')
    expected = dict(samples=worker['rss_samples'], failures=worker['rss_failures'],
                    sampled_peak_rss_bytes=worker['sampled_peak_rss_bytes'])
    return (isinstance(rss, dict) and rss == expected and
            all(type(value) is int for value in [*rss.values(), *expected.values()]))


def network_forced_copy_failure(case, worker, witness, raw):
    return (worker['terminal_error'] in FORCED_COPY_PHASES and witness['terminal_lines'] == [worker['terminal_error']] and
            ('\n'+worker['terminal_error']+'\n').encode() in b'\n'+raw and network_outage_exit(case, worker, witness, raw))


def network_outage_exit(case, worker, witness, raw):
    if case['id'] != 'network/destination-outage' or len(case['worker_measurements']) != 2 or worker['lifetime'] != 1:
        return False
    event = case['events'][0]
    fault, before = event['fault'], event['healthy']['after']
    expected = dict(case=case['id'], recovery_milliseconds=sum(case['timeout_milliseconds'].values()), window=0,
                    role=dict(kind='worker-destination', cluster='destination', worker=1, partition=0), kind='cut',
                    start=fault['start'], observed_exit_at=worker['stopped_at'], before=before, transport='plaintext',
                    targets=fault['targets'], timeout_milliseconds=fault_timeout_catalog()[case['id']])
    proof, active = worker['failure_context'], witness['active_fault']
    return (fault['role'] == expected['role'] and fault['fault'] == dict(kind='cut') and
            all(isinstance(context, dict) and all(context.get(key) == value for key, value in expected.items())
                for context in (proof, active)) and proof['exit_witness'] == witness and proof['heal'] == fault['heal'] and
            active['heal'] in ('0001-01-01T00:00:00Z', fault['heal']) and
            witness['exit_code'] == 1 and witness['wait_error'] == 'exit status 1' and not witness['termination_signal'] and
            not witness['driver_intent_before_wait'] and network_rss_matches(worker, witness) and
            witness['rss']['samples'] > 0 and witness['rss']['failures'] == 0 and 0 < witness['rss']['sampled_peak_rss_bytes'] <= 512*1024**2 and
            healthy_copy(before) and vector(before['destination_ends']) and
            all(saved <= end for saved,end in zip(before['saved'],before['destination_ends'])) and binds_before(witness, raw, before) and
            stamp(worker['started_at']) < stamp(before['at']) < stamp(fault['start']) < stamp(worker['stopped_at']) < stamp(fault['heal']))


def broker_case(directory, id, identity):
    case, safety, _ = copy_case(directory, id, identity)
    baseline, workers = case['baseline'], case['worker_measurements']
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
        validate_readiness(case, recovered, targets[:1], stamp(fault["heal"]), stamp(recovered["at"]))
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
            selected = event.get('isr_partition')
            if type(selected) is not int or not 0 <= selected < 24:
                raise ValueError('ISR loss omitted its observed affected partition')
            controllers = event['controller_targets']
            validate_readiness(case, lost, controllers, stamp(fault["start"]), stamp(fault["heal"]),
                               partition=selected, size=1, leader=broker)
            lost_part, stalled_part = lost['topology']['destination'][selected], stalled['topology']['destination'][selected]
            if (len(workers) != 1 or len(targets) != 1 or targets[0]['listener'] != 'internal' or role['listener'] != 'internal' or
                    targets[0]['proxy'] != f'destination-{broker}-internal' or event['before'] != previous or
                    previous['topology']['destination'][role['partition']]['Leader'] != broker or
                    previous['topology']['destination'][selected]['Leader'] != broker or
                    len(controllers) != 3 or {t['broker'] for t in controllers} != {1, 2, 3} or
                    len({t['container'] for t in controllers}) != 3 or
                    any(t['cluster'] != 'destination' or t['listener'] != 'controller' or not t['container'] for t in controllers) or
                    not lost['broker_healthy'] or event['min_insync_replicas'] != 2 or
                    lost_part != dict(Leader=broker, ISR=[broker]) or stalled_part != lost_part or
                    not stamp(fault['start']) < stamp(lost['at']) <= stamp(stalled['at']) < stamp(fault['heal']) or
                    lost['saved'][selected] > lost['destination_ends'][selected] or
                    lost['publishing']['acknowledged_by_partition'][selected] <= max(lost['destination_ends'][selected], lost['saved'][selected])+3 or
                    lost['cli_progress']['submitted'] <= lost['cli_progress']['confirmed'] or
                    stalled['destination_ends'][selected] != lost['destination_ends'][selected] or
                    stalled['saved'][selected] > lost['destination_ends'][selected] or
                    stalled['publishing']['acknowledged_by_partition'][selected] <= lost['publishing']['acknowledged_by_partition'][selected]+3 or
                    stalled['cli_progress']['submitted'] <= stalled['cli_progress']['confirmed'] or
                    not advances(recovered['saved'], stalled['saved']) or
                    not advances(recovered['publishing']['acknowledged_by_partition'], stalled['publishing']['acknowledged_by_partition'])):
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
    return safety


def interval(value, minimum):
    before, after = value['before'], value['after']
    seconds = stamp(after['at'])-stamp(before['at'])
    if seconds < minimum:
        raise ValueError('network observation shortened its configured interval')
    for state in (before, after):
        if (not all(vector(state[key]) for key in ('saved', 'destination_ends')) or
                not vector(state['publishing']['acknowledged_by_partition']) or
                not all(topology(state, side, 3) for side in ('source', 'destination'))):
            raise ValueError('network observation lacks independent healthy topology or progress')
    if (not advances(after['publishing']['acknowledged_by_partition'], before['publishing']['acknowledged_by_partition']) or
            (sum(after['publishing']['acknowledged_by_partition'])-sum(before['publishing']['acknowledged_by_partition']))/seconds < .95*240):
        raise ValueError('independent publisher did not sustain input during network exposure')
    return before, after, (sum(after['destination_ends'])-sum(before['destination_ends']))/seconds


def backlog(state):
    return max(0, sum(state['publishing']['acknowledged_by_partition'])-sum(state['destination_ends']))


def network_stamp_ns(value):
    # Go receipts retain nanoseconds; floating epoch seconds and Python's
    # microsecond truncation must not move an observation across a100ms slot.
    match = re.fullmatch(r'(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)', value)
    if not match:
        raise ValueError('network measurement start has an invalid publisher timestamp')
    whole = datetime.fromisoformat((match[1]+match[3]).replace('Z', '+00:00'))
    return int(whole.timestamp())*1_000_000_000+int((match[2] or '').ljust(9, '0'))


def network_measurement_start(case, state):
    publication = state['publishing']
    started, observed = (network_stamp_ns(publication[key]) for key in ('started', 'observed'))
    due = (observed-started)//100_000_000
    acknowledged, offered = (publication[key] for key in ('acknowledged_by_partition', 'offered_by_partition'))
    if (not healthy_copy(state) or backlog(state) > 24 or not vector(offered) or
            publication['started'] != case['publishing']['started'] or
            not started <= network_stamp_ns(state['at']) <= observed or
            any(not due-1 <= ack <= offer <= due or offer-ack > 1 for ack, offer in zip(acknowledged, offered))):
        raise ValueError('network measurement start retains past source debt, output backlog or inconsistent publisher proof')


def network_outage_lifetimes(case, event, lifecycles):
    first, replacement = case['worker_measurements']
    witness, raw = lifecycles[0]
    recovered_witness, recovered_raw = lifecycles[1]
    fault, healthy, recovery = event['fault'], event['healthy'], event['recovery']
    if (not stamp(first['started_at']) < stamp(healthy['before']['at']) < stamp(fault['start']) < stamp(first['stopped_at']) <=
            stamp(event['connection_effect']['exit_observed_at']) < stamp(fault['heal']) < stamp(replacement['started_at']) <
            stamp(recovery['before']['at']) < stamp(recovery['after']['at']) < stamp(replacement['stopped_at']) or
            witness['wait_error'] != 'exit status 1' or witness['termination_signal'] or witness['driver_intent_before_wait'] or
            recovered_witness['wait_error'] or recovered_witness['termination_signal'] or
            not all(binds_before(witness, raw, state) for state in healthy.values()) or
            not all(binds_before(recovered_witness, recovered_raw, state) for state in recovery.values())):
        raise ValueError('destination outage lacks an unintentional sole-Wait fault exit and live recovered lifetime')
    expected = dict(case=case['id'], recovery_milliseconds=sum(case['timeout_milliseconds'].values()), window=0,
                    role=fault['role'], kind=fault['fault']['kind'], start=fault['start'], observed_exit_at=first['stopped_at'],
                    before=healthy['after'], transport='plaintext', targets=fault['targets'],
                    timeout_milliseconds=fault_timeout_catalog()[case['id']])
    proof, active = first['failure_context'], witness['active_fault']
    if (any(not isinstance(context, dict) or any(context.get(key) != value for key, value in expected.items()) for context in (proof, active)) or
            proof['heal'] != fault['heal'] or proof['exit_witness'] != witness or
            active['heal'] not in ('0001-01-01T00:00:00Z', fault['heal'])):
        raise ValueError('destination outage lifecycle contradicts its current worker and active fault context')


def network_case(directory, id, identity):
    case, safety, lifecycles = copy_case(directory, id, identity)
    if not id.startswith('network/') or id not in REPLICATED_CASES:
        raise ValueError('unknown network fault exposure')
    if id != 'network/destination-outage' and case['exact_lifetime_work_caps_qualified'] is not True:
        raise ValueError('same-process network cases require exact lifetime work-cap evidence for every worker')
    events, windows, workers = case['events'], safety['fault_events'], case['worker_measurements']
    if len(events) != 1 or len(windows) != 1:
        raise ValueError('network case requires exactly one fault window')
    event, window = events[0], windows[0]
    fault = event['fault']
    role, targets, injections = fault['role'], fault['targets'], window['injections']
    name = id.split('/')[1]
    budgets = {key: value/1000 for key, value in fault_timeout_catalog()[id].items()}
    leader, outage = name == 'destination-leaders-outage', name == 'destination-outage'
    expected_role = dict(kind='leader' if leader else 'worker-destination', cluster='destination', worker=1, partition=0)
    if leader:
        expected_role['topic'] = case['destination_topic']
    expected_fault = dict(kind='cut')
    if name in ('latency', 'bandwidth', 'slow-close'):
        expected_fault = dict(kind={'slow-close':'slow_close'}.get(name, name),
                              stream='downstream' if name == 'slow-close' else 'upstream',
                              value={'latency':500, 'bandwidth':32, 'slow-close':int(budgets['delivery_timeout']*1000)}[name])
    if (role != expected_role or fault['fault'] != expected_fault or window['target'] != role or
            window['unavailable'] is not False or len(targets) != (1 if leader else 3) or
            {t['broker'] for t in targets} != ({targets[0]['broker']} if leader else {1, 2, 3}) or
            len({t['container'] for t in targets}) != len(targets) or
            len({t['proxy_container'] for t in targets}) != 1 or
            any(t['cluster'] != 'destination' or t['listener'] != 'worker1' or
                type(t['broker']) is not int or not 1 <= t['broker'] <= 3 or not t['container'] or not t['proxy_container'] or
                t['service'] != f'destination-{t["broker"]}' or t['proxy'] != f'destination-{t["broker"]}-worker1' for t in targets)):
        raise ValueError('network fault did not isolate the exact owned worker destination routes')
    if (len(injections) != (2 if name == 'slow-close' else 1) or
            injections[0]['kind'] != expected_fault['kind'] or
            any(injections[0]['target'][key] != fault[key] for key in ('role', 'fault', 'targets', 'start'))):
        raise ValueError('network exposure differs from the independent fault window')
    for phase in ('healthy', 'recovery'):
        network_measurement_start(case, event[phase]['before'])
    healthy_before, healthy_after, healthy_rate = interval(event['healthy'], budgets['session.timeout.ms'])
    during_before, during_after, during_rate = interval(event['during'], budgets['session.timeout.ms']+(budgets['delivery_timeout'] if outage else 0))
    recovery_before, recovery_after, recovery_rate = interval(event['recovery'], budgets['session.timeout.ms'])
    if (healthy_before != case['baseline'] or healthy_rate < .95*240 or recovery_rate < .95*healthy_rate or
            not stamp(healthy_after['at']) < stamp(fault['start']) <= injections[0]['at'] <= stamp(during_before['at']) or
            not stamp(during_after['at']) < stamp(fault['heal']) <= stamp(recovery_before['at']) < stamp(recovery_after['at']) <= window['healed_at'] or
            not advances(recovery_after['saved'], recovery_before['saved']) or
            not advances(recovery_before['saved'], during_after['saved']) or backlog(recovery_before) > 24):
        raise ValueError('network exposure lacks ordered injection, complete hold, heal or recovered throughput')
    brokers = {t['broker'] for t in targets}
    affected = [n for n, p in enumerate(during_before['topology']['destination']) if p['Leader'] in brokers]
    unaffected = sorted(set(range(24))-set(affected))
    if (not affected or event['affected_partitions'] != affected or event['unaffected_partitions'] != unaffected or
            leader and (not unaffected or during_before['topology']['destination'][0]['Leader'] != targets[0]['broker']) or
            not leader and affected != list(range(24)) or
            any(state['topology']['destination'] != during_before['topology']['destination'] for state in
                (healthy_after, during_after, recovery_before, recovery_after))):
        raise ValueError('network targets do not match the independently observed destination leaders')
    if name not in ('latency', 'bandwidth') and any(
            during_after['destination_ends'][p] != during_before['destination_ends'][p] or
            during_after['saved'][p] > during_after['destination_ends'][p] for p in affected):
        raise ValueError('blocked destination path acknowledged output or saved beyond the delivered prefix')
    if any(during_after['destination_ends'][p] <= during_before['destination_ends'][p]+2 or
           during_after['saved'][p] <= during_before['saved'][p]+2 for p in unaffected):
        raise ValueError('leader-only outage lacks independent unaffected saved progress')
    pending = during_after['cli_progress']
    if name in ('latency', 'bandwidth', 'slow-close') and pending['submitted'] <= pending['confirmed']:
        raise ValueError('network degradation did not expose unresolved CLI writes')
    if name in ('latency', 'bandwidth') and backlog(during_after) <= backlog(healthy_after)+24:
        raise ValueError('network toxic did not measurably increase actual output backlog')
    if name == 'bandwidth' and during_rate >= .75*healthy_rate:
        raise ValueError('bandwidth toxic did not reduce measured output throughput')
    if (len(workers) != (2 if outage else 1) or
            not 0 < case['publishing']['max_acknowledgement_gap_seconds'] <= 2):
        raise ValueError('network fault restarted an unsupported worker or lost steady independent input')
    connection = event['connection_effect']
    if outage:
        network_outage_lifetimes(case, event, lifecycles)
    elif name == 'slow-close':
        stimulus = connection['stimulus']
        if (stimulus['role'] != role or stimulus['targets'] != targets or
                stimulus['fault'] != dict(kind='timeout', stream='upstream', value=1) or
                injections[1]['kind'] != 'timeout' or
                any(injections[1]['target'][key] != stimulus[key] for key in ('role', 'fault', 'targets', 'start')) or
                connection['proxy'] not in {t['proxy'] for t in targets} or not connection['read_error'] or
                connection['minimum_delay_seconds'] != budgets['session.timeout.ms'] or
                not injections[0]['at'] <= stamp(stimulus['start']) <= injections[1]['at'] <= stamp(connection['started_at']) <= stamp(during_before['at']) or
                stamp(connection['delayed_until'])-stamp(connection['started_at']) < budgets['session.timeout.ms'] or
                not stamp(during_after['at']) <= stamp(connection['delayed_until']) < stamp(stimulus['heal']) <= stamp(fault['heal']) <= stamp(connection['eof_observed_at']) <= stamp(recovery_before['at'])):
            raise ValueError('slow-close lacks actual delayed EOF and removal on the original worker routes')
    elif connection is not None:
        raise ValueError('network case contains an unexpected connection stimulus')
    safety['network_exposure'] = case
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
                case = network_case(directory, id, identity) if id.startswith('network/') else broker_case(directory, id, identity)
            except (KeyError, TypeError, IndexError) as error:
                raise ValueError('malformed replicated fault evidence') from error
        observed_path = paths[observed.index(id)]
        if (Path(directory)/case['safety']['observations']).resolve() != (observed_path.parent/'samples.jsonl').resolve():
            raise ValueError('replicated proof references another observer run')
        cases.append(case)
    return cases
