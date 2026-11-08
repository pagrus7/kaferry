"""Recheck deterministic ownership exposure against raw Kafka and OS evidence."""

import base64
from collections import Counter
import json
from pathlib import Path
import posixpath
import re
import struct
import tomllib

from tools.ci.suites import fault_timeout_catalog
from tools.qualification.fault_results import observation_path, validate_case
from tools.qualification.ownership_audit import commits, integer, logging_configuration, records, segments, stamp
from tools.qualification.replicated_results import advances, vector


IDENTITY = ('source_commit', 'binary_sha256', 'binary_platform', 'execution_platform')
NAMES = ('worker-kill-rebalance', 'coordinator-kill-rebalance', 'worker-scale')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def assignment(value, source):
    """Decode the classic consumer assignment independently of franz-go."""
    data = base64.b64decode(value or '', validate=True)
    if not data:
        return []
    at = 0
    def number(format):
        nonlocal at
        result = struct.unpack_from(format, data, at)[0]
        at += struct.calcsize(format)
        return result
    require(number('>h') == 0, 'unknown consumer assignment version')
    count = number('>i')
    require(0 <= count <= 1, 'assignment contains unexpected topics')
    result = []
    for _ in range(count):
        size = number('>h')
        require(0 <= size <= len(data)-at, 'assignment topic is truncated')
        topic = data[at:at+size].decode('utf-8')
        at += size
        require(topic == source, 'assignment names another source topic')
        parts = number('>i')
        require(0 <= parts <= 24, 'assignment has invalid partition count')
        result.extend(number('>i') for _ in range(parts))
    size = number('>i')
    require(size >= -1 and at+max(0, size) == len(data), 'assignment user data is truncated or has a suffix')
    require(len(result) == len(set(result)) and set(result) <= set(range(24)), 'assignment repeats or invents a partition')
    return sorted(result)


def route(client):
    match = re.search(r'-worker([123])$', client)
    require(match is not None, 'audit client does not identify a selected worker route')
    return int(match[1])


def members(items):
    return sorted((item['client_id'], item['member_id'], sorted(item['partitions'] or [])) for item in items)


def audit(directory, case):
    rows = []
    for broker in range(1, 4):
        raw = directory/f'source-{broker}.log4j2.yaml'
        logging_configuration(raw, directory/f'source-{broker}.log4j2.json')
        flags = dict(line.split('=', 1) for line in (directory/f'source-{broker}.jvm-logging.txt').read_text().splitlines())
        require(set(flags) == {'-Dlog4j2.configurationFile', '-Dkafka.logs.dir'} and
                posixpath.normpath(flags['-Dlog4j2.configurationFile']) == '/opt/kafka/config/log4j2.yaml' and
                posixpath.normpath(flags['-Dkafka.logs.dir']) == '/opt/kafka/logs', 'unverified broker logging JVM settings')
        console = (directory/f'source-{broker}.console.log').read_text()
        require(console and not re.search(r'(?im)(ERROR.*(?:StatusLogger|StatusConsoleListener)|'
                r'(?:StatusLogger|StatusConsoleListener).*ERROR|AppenderLoggingException|Unable to write to stream|'
                r'Unable to create file|Error writing to stream)', console), 'broker logging failed or diagnostics are missing')
        before = segments(directory/f'source-{broker}.baseline.requests.log')
        after = segments(directory/f'source-{broker}.final.requests.log')
        require(all(not data or sum(part.startswith(data) for part in after.values()) == 1 for data in before.values()),
                'broker audit lost, duplicated or truncated its baseline')
        initial, final = records(before), records(after)
        guard = lambda row: row['requestHeader']['clientId'] == case['audit_guard'] and row['requestHeader']['requestApiKeyName'] == 'API_VERSIONS'
        require(sum(map(guard, final)) > sum(map(guard, initial)) >= 1, 'broker audit lacks start and finish guards')
        for phase, items in (('baseline', initial), ('final', final)):
            mark = case['audit_marks'][phase]
            require(any(guard(row) and int(stamp(mark['started'])*1000)/1000 <= row['completed_at'] <= stamp(mark['finished'])
                        for row in items), 'audit guard clock or capture interval differs')
        for row in final:
            row['broker'] = broker
        rows.extend(final)
    return rows


def snapshot(case, state, rows, routes=None, previous=None, progress_before=None):
    begin, end = stamp(state['started']), stamp(state['finished'])
    require(begin <= end and not state.get('error') and type(state['coordinator']) is int and state['coordinator'] in (1, 2, 3), 'ownership observation failed')
    selected = members(state['members'])
    # Millisecond broker timestamps round down. Require a complete successful
    # response inside the actual independent observation, not a nearby summary.
    descriptions = []
    for row in rows:
        if (row['requestHeader']['requestApiKeyName'] != 'DESCRIBE_GROUPS' or row['broker'] != state['coordinator'] or
                not int(begin*1000)/1000 <= row['completed_at'] <= end):
            continue
        for group in row['response']['groups']:
            if group['groupId'] != case['group'] or group['errorCode'] != 0 or group['groupState'] != state['state']:
                continue
            observed = [(member['clientId'], member['memberId'], assignment(member['memberAssignment'], case['source_topic'])) for member in group['members']]
            if sorted(observed) == selected:
                descriptions.append(row)
    require(descriptions, 'ownership state lacks its raw successful broker group description')
    if routes is None:
        return
    require(state['state'] == 'Stable' and len(selected) == len(routes) and
            {route(client) for client, _, _ in selected} == set(routes), 'stable membership differs from the selected routes')
    parts = [part for _, member, owned in selected for part in owned]
    require(all(member and owned for _, member, owned in selected) and all(type(part) is int for part in parts) and sorted(parts) == list(range(24)),
            'ownership does not cover all 24 partitions exactly once')
    before = progress_before or previous
    require(vector(state['saved']) and advances(state['saved'], [0]*24 if before is None else before['saved']),
            'saved progress did not advance on every partition')
    epochs = state['accepted_generations']
    require(set(epochs) == {client for client, _, _ in selected} and len(set(epochs.values())) == 1 and
            all(type(value) is int and value > 0 for value in epochs.values()), 'current accepted generation is incomplete')
    if previous:
        require(min(epochs.values()) > max(previous['accepted_generations'].values()) and
                advances(state['publishing']['acknowledged_by_partition'], before['publishing']['acknowledged_by_partition']) and
                stamp(before['finished']) <= stamp(state['started']),
                'membership generation or publication did not advance')
    for client, member, owned in selected:
        joins = [row for row in rows if row['requestHeader']['clientId'] == client and
                 row['requestHeader']['requestApiKeyName'] == 'JOIN_GROUP' and row['request']['groupId'] == case['group'] and
                 row['response']['errorCode'] == 0 and row['response']['memberId'] == member and row['completed_at'] <= end]
        require(joins and max(integer(row['response']['generationId']) for row in joins) == epochs[client],
                'reported generation differs from the actual selected member join')
        require(all(row['request']['sessionTimeoutMs'] == case['timeout_milliseconds']['session.timeout.ms'] and
                    row['request']['rebalanceTimeoutMs'] == case['timeout_milliseconds']['max.poll.interval.ms'] for row in joins),
                'actual group session or rebalance timeout differs from the configured window')
        syncs = [row for row in rows if row['requestHeader']['clientId'] == client and
                 row['requestHeader']['requestApiKeyName'] == 'SYNC_GROUP' and row['request']['groupId'] == case['group'] and
                 row['request']['memberId'] == member and integer(row['request']['generationId']) == epochs[client] and
                 row['response']['errorCode'] == 0 and row['completed_at'] <= end]
        require(syncs and assignment(max(syncs, key=lambda row: row['completed_at'])['response']['assignment'], case['source_topic']) == sorted(owned),
                'current generation does not assign the described partitions to this selected member')
        synced_at = max(row['completed_at'] for row in syncs)
        accepted = set()
        topic_ids = {topic['topicId'] for row in rows if row['requestHeader']['requestApiKeyName'] == 'METADATA'
                     for topic in row['response']['topics'] if topic['name'] == case['source_topic'] and topic['errorCode'] == 0}
        require(len(topic_ids) == 1, 'raw source metadata does not bind one topic identity')
        for row in rows:
            if (row['requestHeader']['clientId'] == client and row['requestHeader']['requestApiKeyName'] == 'OFFSET_COMMIT' and
                    row['request']['groupId'] == case['group'] and row['request']['memberId'] == member and
                    integer(row['request']['generationIdOrMemberEpoch']) == epochs[client] and synced_at <= row['completed_at'] <= end):
                require(row['listener'] == 'WORKER'+str(route(client)), 'selected CLI bypassed its worker route')
                require({topic['topicId'] for topic in row['request']['topics']} == topic_ids and
                        {topic['topicId'] for topic in row['response']['topics']} == topic_ids,
                        'accepted commit names another source topic identity')
                accepted.update(part for part, offset, error in commits(row) if error == 0 and
                                offset > (0 if before is None else before['saved'][part]+2))
        require(set(owned) <= accepted, 'actual current-generation commits do not cover the observed assignment')


def work(log):
    require(not log or log.endswith('\n'), 'selected CLI log ends with an incomplete row')
    peak_records = peak_bytes = samples = 0
    summary = False
    terminal = []
    for line in log.splitlines():
        if not line.strip():
            continue
        if not line.lstrip().startswith('{'):
            terminal.append(line.strip())
            continue
        row = json.loads(line)
        if row.get('msg') not in ('progress', 'copy summary'):
            continue
        keys = ('peak_uncommitted_records', 'peak_uncommitted_bytes') if row['msg'] == 'copy summary' else ('pending_entries', 'pending_estimated_bytes')
        require(all(type(row[key]) is int and row[key] >= 0 for key in (*keys, 'submitted', 'confirmed')) and
                row['confirmed'] <= row['submitted'], 'CLI cap measurements are incomplete or inconsistent')
        peak_records, peak_bytes = max(peak_records, row[keys[0]]), max(peak_bytes, row[keys[1]])
        samples += 1
        summary |= row['msg'] == 'copy summary'
    return peak_records, peak_bytes, samples, summary, '\n'.join(terminal)


def workers(directory, case, safety, identity):
    values = case['worker_measurements']
    require(values and len({w['pid'] for w in values}) == len(values) and len({w['log'] for w in values}) == len(values) and
            {(w['pid'], w['exit_code']) for w in values} == {(w['pid'], w['exit_code']) for w in safety['writer_observations']},
            'selected worker waits differ from independent writer observations')
    for worker in values:
        require(type(worker['pid']) is int and worker['pid'] > 0 and type(worker['route']) is int and worker['route'] in (1, 2, 3) and
                worker['binary_sha256'] == identity['binary_sha256'] and
                safety['started_at'] <= stamp(worker['started_at']) < stamp(worker['stopped_at']) <= safety['writers_stopped_at'] and
                stamp(case['audit_marks']['baseline']['finished']) < stamp(worker['started_at']) and
                stamp(worker['stopped_at']) < stamp(case['audit_marks']['final']['started']), 'worker identity or audited lifetime differs')
        require(type(worker['rss_samples']) is int and worker['rss_samples'] > 0 and worker['rss_failures'] == 0 and
                type(worker['sampled_peak_rss_bytes']) is int and 0 < worker['sampled_peak_rss_bytes'] <= 512*1024**2 and
                worker['record_cap'] == 1024 and worker['byte_cap'] == 128*1024**2, 'worker resource observations are invalid')
        counts = work((directory/worker['log']).read_text())
        require(counts[:4] == (worker['peak_uncommitted_records'], worker['peak_uncommitted_bytes'], worker['progress_observations'], worker['final_summary']) and
                counts[2] > 0 and counts[0] <= 1024 and counts[1] <= 128*1024**2 and
                (worker['forced_kill'] is True or counts[3]), 'recorded caps differ from raw CLI measurements')
        if worker['forced_kill'] is True:
            require(not counts[3] and worker['cap_measurement_scope'] == 'live progress observations before SIGKILL; forced exit cannot emit a final summary',
                    'killed worker was already stopping or omitted its sampled cap scope')
        status = worker['wait_status']
        require(type(status) is int and type(worker['exit_code']) is int and
                ((status == 9 and worker['exit_code'] == -1 and worker['forced_kill'] is True) or
                 (status == worker['exit_code'] << 8 and worker['exit_code'] in (0, 1) and worker['forced_kill'] is False)),
                'raw OS wait status differs from the recorded exit')
        require(worker['reason'], 'worker exit omitted its scope')
        if worker['exit_code'] == 1:
            causes = ('NOT_COORDINATOR', 'COORDINATOR_NOT_AVAILABLE', 'COORDINATOR_LOAD_IN_PROGRESS', 'UNKNOWN_MEMBER_ID',
                      'ILLEGAL_GENERATION', 'REBALANCE_IN_PROGRESS', 'consumer group ownership unavailable for commit', 'context deadline exceeded')
            require(counts[4] and counts[4] == worker['terminal_error'] and all(any(cause in line for cause in causes) for line in counts[4].splitlines()),
                    'restart lacks the exact bounded terminal coordinator cause')
        else:
            require(not counts[4] and not worker['terminal_error'], 'successful or killed worker has an unexplained terminal error')
    return values


def signal(case, number, pid):
    found = [event for event in case['events'] if event.get('signal_number') == number and event.get('pid') == pid]
    require(len(found) == 1, 'required OS signal was omitted or repeated')
    event = found[0]
    require(stamp(event['started']) <= stamp(event['finished']), 'OS signal interval is reversed')
    return event


def broker_inspection(directory, case, name, target, running, interval):
    observed = json.loads((directory/name).read_text())
    require(observed['id'] == target['container'] and observed['project'] == case['fixture_project'] and
            observed['service'] == target['service'] and observed['running'] is running and
            (not running or observed['health'] == 'healthy'), 'raw Docker inspection does not prove the resolved broker state')
    events = [event for event in case['events'] if event.get('broker_inspection') == name]
    require(len(events) == 1 and events[0]['container'] == target['container'] and
            stamp(interval['started']) <= stamp(events[0]['started']) <= stamp(events[0]['finished']) <= stamp(interval['finished']),
            'broker inspection was not recorded during the required state')


def validate(directory, id, identity):
    try:
        return _validate(Path(directory).resolve(), id, identity)
    except (KeyError, TypeError, IndexError, AttributeError, OSError, struct.error, UnicodeError) as error:
        raise ValueError('malformed deterministic ownership evidence') from error


def _validate(root, id, identity):
    require(id in {'ownership/'+name for name in NAMES}, 'unknown deterministic ownership case')
    directory = root/('ownership-'+id.split('/')[1])
    case = json.loads((directory/'replicated-ownership-case.json').read_text())
    require(type(case['schema']) is int and case['schema'] == 1 and case['id'] == id and case['complete'] is True and case['case_failed'] is False and
            type(case['source_partitions']) is int and case['source_partitions'] == 24 and case['binary_sha256'] == identity['binary_sha256'] and
            case['timeout_milliseconds'] == fault_timeout_catalog()[id], 'ownership receipt differs from its selected configuration')
    paths = {}
    for reference in case['artifacts']:
        name = reference['observations']
        require(Path(name).name == name and name not in paths, 'ownership artifact name is unsafe or repeated')
        paths[name] = observation_path(directory, reference, name)
        require(paths[name].stat().st_size <= 16*1024**2, 'ownership raw artifact exceeds its bounded capture')
    required = {f'source-{n}.{suffix}' for n in range(1, 4) for suffix in
                ('log4j2.yaml', 'log4j2.json', 'jvm-logging.txt', 'console.log', 'baseline.requests.log', 'final.requests.log')}
    required |= {worker['log'] for worker in case['worker_measurements']} | {'ownership-observations.jsonl'}
    required |= {'worker-settings.toml'} | {f'worker-route-{worker["route"]}.sql' for worker in case['worker_measurements']}
    coordinator = id == 'ownership/coordinator-kill-rebalance'
    if coordinator:
        required |= {'coordinator-stopped.inspect.json', 'coordinator-restored.inspect.json'}
    require(set(paths) == required, 'ownership receipt omitted a required raw artifact')
    safety_path = observation_path(root, case['safety_case'], 'case.json')
    safety = validate_case(safety_path)
    require(safety['id'] == id and safety['execution'] == 'candidate CLI' and
            safety['parameters'] == dict(source_partitions=24, routing='PRESERVE') and
            all(safety[key] == identity[key] for key in IDENTITY), 'ownership safety proof differs from the selected executable or source')
    publication = case['publishing']
    require(vector(publication['offered_by_partition']) and publication['offered_by_partition'] == publication['acknowledged_by_partition'] and
            min(publication['acknowledged_by_partition']) > 0 and 0 < publication['max_offer_gap_seconds'] <= 2 and
            safety['started_at'] <= stamp(publication['started']) < stamp(publication['stopped']) <= safety['writers_stopped_at'] and
            publication['acknowledged_by_partition'] == [part['expected'] for part in sorted(safety['independent_reconciliation']['partitions'], key=lambda part: part['partition'])],
            'ownership input publication was not completely drained and reconciled')
    rows = audit(directory, case)
    settings = tomllib.loads((directory/'worker-settings.toml').read_text())
    require(settings == dict(worker=dict(record_cap=1024,partition_record_cap=128,byte_cap=134217728,partition_byte_cap=8388608,
            progress_interval='100ms',delivery_timeout='1m0s',shutdown_timeout='5s')), 'selected worker settings differ from the measured caps and deadlines')
    for worker_route in {worker['route'] for worker in case['worker_measurements']}:
        clients = {member['client_id'] for state in case['states'].values() for member in state['members'] if route(member['client_id']) == worker_route}
        prefix = 'kaferry-'+case['group']+'-'
        require(len(clients) == 1 and next(iter(clients)).startswith(prefix), 'worker client identity changed across its selected route')
        marker = next(iter(clients))[len(prefix):]
        source = ','.join(f'127.0.0.1:{46092+worker_route*100+n}' for n in range(3))
        destination = ','.join(f'127.0.0.1:{47092+worker_route*100+n}' for n in range(3))
        expected = (f"WITH CONNECTION default AS ('bootstrap.servers'='{source}','client.id'='{marker}','session.timeout.ms'='6000',"
                    f"'heartbeat.interval.ms'='1000','max.poll.interval.ms'='30000','socket.timeout.ms'='8000'), CONNECTION destination AS "
                    f"('bootstrap.servers'='{destination}','client.id'='{marker}') INSERT INTO TOPIC '{case['destination_topic']}@destination' "
                    f"SELECT * FROM TOPIC '{case['source_topic']}' WITH (GROUP='{case['group']}', START=EARLIEST)")
        require((directory/f'worker-route-{worker_route}.sql').read_text() == expected, 'selected route SQL or timeout configuration differs')
    values = workers(directory, case, safety, identity)
    states = case['states']
    require(case['observations'] == 'ownership-observations.jsonl', 'ownership journal path differs')
    journal = [json.loads(line) for line in (directory/case['observations']).read_text().splitlines()]
    require(journal and all(any(not row.get('error') and row['started'] == state['started'] and row['state'] == state['state'] and
            row['coordinator'] == state['coordinator'] and members(row['members']) == members(state['members']) for row in journal)
            for state in states.values()), 'ownership journal omitted a required observation')
    baseline, recovered = states['baseline'], states['recovered']
    snapshot(case, baseline, rows, [1])
    windows = safety['fault_events']
    budget = {key: value/1000 for key, value in case['timeout_milliseconds'].items()}
    require(case['required_recovery_seconds'] == sum(budget.values()) and all(window['timeout_basis'] == budget and
            window['recovery_seconds'] == sum(budget.values()) and len(window['injections']) == 1 for window in windows),
            'ownership windows differ from their complete configured horizons')
    original = [worker for worker in values if worker['route'] == 1][0]
    require(stamp(original['started_at']) < stamp(baseline['started']), 'baseline predates its original selected process')
    if id == 'ownership/worker-scale':
        require(Counter(w['route'] for w in values) == Counter({1:1, 2:1, 3:1}) and all(w['exit_code'] == 0 for w in values),
                'scale changed process identity or restarted a worker')
        up, down = states['scaled_up'], states['scaled_down']
        snapshot(case, up, rows, [1, 2, 3], baseline)
        snapshot(case, down, rows, [1], up)
        require(down == recovered and len(windows) == 2, 'scale omitted a required membership transition')
        for window, before, after, kind, target, injection_kind in zip(windows, (baseline, up), (up, down),
                ('scale-up', 'scale-down'), (dict(from_=1, to=3), dict(from_=3, to=1)), ('worker-start', 'worker-stop')):
            target['from'] = target.pop('from_')
            injection = window['injections'][0]
            require(window['kind'] == kind and window['target'] == target and window['unavailable'] is False and
                    stamp(before['finished']) <= window['started_at'] <= injection['at'] <= stamp(after['finished']) <= window['healed_at'] and
                    injection['kind'] == injection_kind, 'scale window does not bind its actual transition')
        for worker in values[1:]:
            stopped = signal(case, 2, worker['pid'])
            require(stamp(up['finished']) < stamp(stopped['started']) <= stamp(worker['stopped_at']) < stamp(down['finished']), 'scale removal did not occur after three-worker progress')
        require(stamp(down['finished']) < stamp(original['stopped_at']), 'original worker did not survive scaling')
    else:
        require(len(windows) == 1, 'kill case requires one complete held-worker window')
        window, injection = windows[0], windows[0]['injections'][0]
        held_number = 17 if identity['execution_platform'].startswith('darwin-') else 19
        held = signal(case, held_number, original['pid'])
        proof = [event for event in case['events'] if event.get('reason') == 'OS confirms held worker']
        require(len(proof) == 1 and proof[0]['pid'] == original['pid'] and
                proof[0]['command'] == ['ps', '-o', 'state=', '-p', str(original['pid'])] and
                proof[0]['stdout'].strip() == proof[0]['os_process_state'] and 'T' in proof[0]['stdout'] and
                window['started_at'] <= stamp(held['started']) <= stamp(held['finished']) <= stamp(proof[0]['observed_at']),
                'kill case lacks the actual held-worker OS observation')
        preparing, at_kill = states['preparing_rebalance'], states['at_kill']
        first = baseline['members'][0]
        for state in (preparing, at_kill):
            snapshot(case, state, rows)
            require(state['state'] == 'PreparingRebalance' and len(state['members']) == 2 and
                    (first['client_id'], first['member_id']) in {(m['client_id'], m['member_id']) for m in state['members']} and
                    {route(member['client_id']) for member in state['members']} == {1, 2} and
                    state['prior_accepted_generation'] == max(baseline['accepted_generations'].values()),
                    'kill trigger lacks both actual members and the prior accepted generation')
        require(stamp(proof[0]['observed_at']) <= stamp(preparing['started']) <= stamp(preparing['finished']) <= stamp(at_kill['started']),
                'rebalance was not independently observed after the old worker stopped')
        peer = [worker for worker in values if worker['route'] == 2][0]
        require(stamp(proof[0]['observed_at']) <= stamp(peer['started_at']) < stamp(preparing['finished']), 'selected peer did not join the held-worker rebalance')
        require(window['kind'] == 'held-worker-rebalance' and window['target'] == dict(held_worker=original['pid'],
                coordinator_role=dict(kind='coordinator', cluster='source', group=case['group'], partition=0)) and
                window['unavailable'] is coordinator, 'kill window differs from its held-worker stimulus')
        if coordinator:
            coordinator_case(directory, case, rows, values, window, identity)
            snapshot(case, recovered, rows, [1, 2], baseline, states['broker_restored'])
        else:
            require(Counter(w['route'] for w in values) == Counter({1:1, 2:1}) and original['exit_code'] == -1 and peer['exit_code'] == 0,
                    'worker kill restarted or terminated another worker')
            killed = signal(case, 9, original['pid'])
            require(stamp(at_kill['finished']) <= stamp(killed['started']) <= stamp(killed['finished']) <= stamp(original['stopped_at']) <= injection['at'] and
                    injection['kind'] == 'worker-sigkill' and injection['target'] == dict(pid=original['pid'], route=1, state=at_kill), 'worker kill lacks the actual rebalance and OS wait before injection proof')
            snapshot(case, recovered, rows, [2], baseline)
        require(injection['at'] <= stamp(recovered['finished']) <= window['healed_at'], 'kill recovery was not observed before heal')
    # Raw re-evaluation establishes positive ownership and accepted progress.
    # Physical coordinator death can lose in-flight request completions; this
    # case does not make the zombie audit's complete stale-fencing claim.
    safety['ownership_exposure'] = dict(case, audit_scope='raw group descriptions, successful joins and accepted commits; bounded OS/Docker faults and selected CLI exits')
    for reference in (safety['safety'], safety['bootstrap']):
        reference['observations'] = (safety_path.parent/reference['observations']).resolve().relative_to(root).as_posix()
    return safety


def coordinator_case(directory, case, rows, values, window, identity):
    states, injection = case['states'], window['injections'][0]
    physical = [event['fault'] for event in case['events'] if 'fault' in event]
    commands = [event for event in case['events'] if 'coordinator_kill_started' in event]
    require(len(physical) == len(commands) == 1 and injection['kind'] == 'coordinator-kill', 'coordinator kill lacks one actual physical fault')
    fault, command = physical[0], commands[0]
    role, targets = fault['role'], fault['targets']
    broker = states['at_kill']['coordinator']
    require(role == dict(kind='coordinator', cluster='source', group=case['group'], partition=0) and fault['fault'] == dict(kind='kill') and
            len(targets) == 6 and {t['listener'] for t in targets} == {'internal','controller','admin','worker1','worker2','worker3'} and
            len({t['container'] for t in targets}) == 1 and all(t['cluster'] == 'source' and t['broker'] == broker and
            t['service'] == f'source-{broker}' and t['container'] == command['container'] for t in targets) and
            all(injection['target'][key] == fault[key] for key in ('role','fault','targets','start')),
            'physical target differs from the actual coordinator and independent injection')
    require(stamp(fault['start']) <= stamp(states['at_kill']['started']) <= stamp(states['at_kill']['finished']) <=
            stamp(command['coordinator_kill_started']) <= stamp(command['coordinator_kill_finished']) <= injection['at'], 'coordinator command did not follow the actual rebalance recheck')
    replacement, restored = states['coordinator_replacement'], states['broker_restored']
    snapshot(case, replacement, rows)
    snapshot(case, restored, rows)
    require(injection['at'] <= stamp(replacement['started']) <= stamp(replacement['finished']) <= stamp(fault['heal']) <= stamp(restored['started']) and
            replacement['coordinator'] != broker and advances(replacement['publishing']['acknowledged_by_partition'], states['at_kill']['publishing']['acknowledged_by_partition']),
            'coordinator replacement was not observed while the original broker was stopped')
    broker_inspection(directory, case, 'coordinator-stopped.inspect.json', targets[0], False, replacement)
    broker_inspection(directory, case, 'coordinator-restored.inspect.json', targets[0], True, restored)
    topology = restored['source_topology']
    require(len(topology) == 24 and all(type(part['Leader']) is int and part['Leader'] in (1,2,3) and
            all(type(node) is int for node in part['ISR']) and sorted(part['ISR']) == [1,2,3] for part in topology), 'restored source topology lacks ISR3')
    metadata = [topic for row in rows if row['requestHeader']['requestApiKeyName'] == 'METADATA' and
                int(stamp(restored['started'])*1000)/1000 <= row['completed_at'] <= stamp(restored['finished'])
                for topic in row['response']['topics'] if topic['name'] == case['source_topic'] and topic['errorCode'] == 0]
    require(any(len(topic['partitions']) == 24 and {part['partitionIndex'] for part in topic['partitions']} == set(range(24)) and
                all(type(part['partitionIndex']) is int and part['errorCode'] == 0 and sorted(part['isrNodes']) == [1,2,3] and
                dict(Leader=part['leaderId'], ISR=part['isrNodes']) == topology[part['partitionIndex']] for part in topic['partitions']) for topic in metadata),
            'restored ISR3 lacks the actual raw Kafka metadata response')
    original = values[0]
    continued = signal(case, 19 if identity['execution_platform'].startswith('darwin-') else 18, original['pid'])
    require(stamp(command['coordinator_kill_finished']) <= stamp(continued['started']) <= stamp(continued['finished']) <= injection['at'], 'held worker did not resume after coordinator kill')
    counts = Counter(worker['route'] for worker in values)
    require(set(counts) == {1,2} and all(1 <= count <= 2 for count in counts.values()) and all(w['exit_code'] in (0,1) for w in values),
            'coordinator fault exceeded its one-restart-per-route scope')
    restarts = [event for event in case['events'] if 'restart_routes' in event]
    failed = [worker for worker in values if worker['exit_code'] == 1]
    require(len(restarts) == 1 and sorted(restarts[0]['restart_routes']) == sorted(worker['route'] for worker in failed), 'restart routes differ from actual fail-fast exits')
    for worker in failed:
        require(stamp(command['coordinator_kill_started']) <= stamp(worker['stopped_at']) <= stamp(restored['finished']), 'fail-fast exit was premature or after the recovery boundary')
        replacements = [other for other in values if other['route'] == worker['route'] and other['pid'] != worker['pid']]
        require(len(replacements) == 1 and replacements[0]['exit_code'] == 0 and stamp(restored['finished']) <= stamp(replacements[0]['started_at']), 'failed route was not restarted once after broker recovery')
