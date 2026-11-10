"""Independently verify raw ownership protocol; Go owns scenario and process gates."""

import base64
import json
from pathlib import Path
import posixpath
import re
import struct

from tools.ci.suites import fault_timeout_catalog
from tools.qualification.fault_results import observation_path, validate_case
from tools.qualification.ownership_audit import commits, integer, logging_configuration, records, segments, stamp


IDENTITY = ('source_commit', 'binary_sha256', 'binary_platform', 'execution_platform')
NAMES = ('worker-kill-rebalance', 'coordinator-kill-rebalance', 'worker-scale')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def vector(values):
    return isinstance(values, list) and len(values) == 24 and all(type(value) is int and value >= 0 for value in values)


def advances(after, before):
    return vector(after) and vector(before) and all(a > b+2 for a, b in zip(after, before))


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


def members(state):
    items = state['members']
    if items is None and state['state'] == 'Empty':
        items = []
    require(isinstance(items, list), 'ownership members are not an array')
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


def group_description(case, state, rows):
    begin, end = stamp(state['started']), stamp(state['finished'])
    require(begin <= end and not state.get('error') and type(state['coordinator']) is int and state['coordinator'] in (1, 2, 3), 'ownership observation failed')
    selected = members(state)
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
    return selected


def selected_lifetime(case, state, client):
    begin, end = stamp(state['started']), stamp(state['finished'])
    lifetimes = [worker for worker in case['worker_measurements'] if worker['route'] == route(client) and
                 stamp(worker['started_at']) <= begin <= end < stamp(worker['stopped_at'])]
    require(len(lifetimes) == 1, 'ownership snapshot is not covered by one retained selected route lifetime')
    return lifetimes[0]


def member_protocol(row, lifetime, client, end):
    require(row['listener'] == 'WORKER'+str(route(client)) and
            stamp(lifetime['started_at']) <= row['received_at'] <= row['completed_at'] <= end < stamp(lifetime['stopped_at']),
            'selected member protocol lies outside its actual route lifetime')


def snapshot(case, state, rows, routes=None, previous=None, progress_before=None):
    selected = group_description(case, state, rows)
    lifetimes = {client:selected_lifetime(case,state,client) for client,_,_ in selected}
    end = stamp(state['finished'])
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
        lifetime = lifetimes[client]
        joins = [row for row in rows if row['requestHeader']['clientId'] == client and
                 row['requestHeader']['requestApiKeyName'] == 'JOIN_GROUP' and row['request']['groupId'] == case['group'] and
                 row['response']['errorCode'] == 0 and row['response']['memberId'] == member and row['completed_at'] <= end]
        require(joins and max(integer(row['response']['generationId']) for row in joins) == epochs[client],
                'reported generation differs from the actual selected member join')
        for row in joins:
            if integer(row['response']['generationId']) == epochs[client]:
                member_protocol(row,lifetime,client,end)
        require(all(row['request']['sessionTimeoutMs'] == case['timeout_milliseconds']['session.timeout.ms'] and
                    row['request']['rebalanceTimeoutMs'] == case['timeout_milliseconds']['max.poll.interval.ms'] for row in joins),
                'actual group session or rebalance timeout differs from the configured window')
        syncs = [row for row in rows if row['requestHeader']['clientId'] == client and
                 row['requestHeader']['requestApiKeyName'] == 'SYNC_GROUP' and row['request']['groupId'] == case['group'] and
                 row['request']['memberId'] == member and integer(row['request']['generationId']) == epochs[client] and
                 row['response']['errorCode'] == 0 and row['completed_at'] <= end]
        require(syncs and assignment(max(syncs, key=lambda row: row['completed_at'])['response']['assignment'], case['source_topic']) == sorted(owned),
                'current generation does not assign the described partitions to this selected member')
        for row in syncs:
            member_protocol(row,lifetime,client,end)
        synced_at = max(row['completed_at'] for row in syncs)
        accepted = set()
        topic_ids = {topic['topicId'] for row in rows if row['requestHeader']['requestApiKeyName'] == 'METADATA'
                     for topic in row['response']['topics'] if topic['name'] == case['source_topic'] and topic['errorCode'] == 0}
        require(len(topic_ids) == 1, 'raw source metadata does not bind one topic identity')
        for row in rows:
            if (row['requestHeader']['clientId'] == client and row['requestHeader']['requestApiKeyName'] == 'OFFSET_COMMIT' and
                    row['request']['groupId'] == case['group'] and row['request']['memberId'] == member and
                    integer(row['request']['generationIdOrMemberEpoch']) == epochs[client] and synced_at <= row['completed_at'] <= end):
                member_protocol(row,lifetime,client,end)
                require({topic['topicId'] for topic in row['request']['topics']} == topic_ids and
                        {topic['topicId'] for topic in row['response']['topics']} == topic_ids,
                        'accepted commit names another source topic identity')
                accepted.update(part for part, offset, error in commits(row) if error == 0 and
                                offset > (0 if before is None else before['saved'][part]+2))
        require(set(owned) <= accepted, 'actual current-generation commits do not cover the observed assignment')


def validate(directory, id, identity):
    try:
        return _validate(Path(directory).resolve(), id, identity)
    except (KeyError, TypeError, IndexError, AttributeError, OSError, struct.error, UnicodeError) as error:
        raise ValueError('malformed deterministic ownership evidence') from error


def _validate(root, id, identity):
    require(id in {'ownership/'+name for name in NAMES}, 'unknown deterministic ownership case')
    directory = root/('ownership-'+id.split('/')[1])
    case = json.loads((directory/'replicated-ownership-case.json').read_text())
    require(type(case['schema']) is int and case['schema'] == 1 and case['id'] == id and
            case['complete'] is True and case['case_failed'] is False and
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
    required |= {worker['log'] for worker in case['worker_measurements']}
    required |= {f'worker-{index}.{suffix}' for index in range(1,len(case['worker_measurements'])+1) for suffix in ('lifecycle.json','exited.cli.log')}
    require(required <= paths.keys(), 'ownership receipt omitted a required raw artifact')
    safety_path = observation_path(root, case['safety_case'], 'case.json')
    safety = validate_case(safety_path)
    require(safety['id'] == id and safety['execution'] == 'candidate CLI' and
            safety['parameters'] == dict(source_partitions=24, routing='PRESERVE') and
            all(safety[key] == identity[key] for key in IDENTITY), 'ownership safety proof differs from the selected executable or source')
    workers = case['worker_measurements']
    require(workers and len({worker['pid'] for worker in workers}) == len(workers) and
            {(worker['pid'], worker['exit_code']) for worker in workers} ==
            {(worker['pid'], worker['exit_code']) for worker in safety['writer_observations']},
            'selected worker waits differ from independent writer observations')
    for index, worker in enumerate(workers, 1):
        require(type(worker['pid']) is int and worker['pid'] > 0 and type(worker['route']) is int and worker['route'] in (1, 2, 3) and
                type(worker['exit_code']) is int and worker['binary_sha256'] == identity['binary_sha256'] and
                safety['started_at'] <= stamp(worker['started_at']) < stamp(worker['stopped_at']) <= safety['writers_stopped_at'] and
                stamp(case['audit_marks']['baseline']['finished']) < stamp(worker['started_at']) and
                stamp(worker['stopped_at']) < stamp(case['audit_marks']['final']['started']), 'worker identity or audited lifetime differs')
        witness = json.loads(observation_path(directory, worker['lifecycle'], f'worker-{index}.lifecycle.json').read_text())
        require(witness['lifetime'] == dict(number=index, pid=worker['pid'], started_at=worker['started_at'], binary_sha256=worker['binary_sha256']) and
                witness['stopped_at'] == worker['stopped_at'] and witness['exit_code'] == worker['exit_code'],
                'ownership lifecycle differs from the selected route lifetime')
        raw = observation_path(directory, witness['raw_log'], f'worker-{index}.exited.cli.log').read_bytes()
        require(raw == paths[worker['log']].read_bytes() and len(raw) == witness['raw_log_bytes'], 'ownership raw lifetime log differs')
    rows = audit(directory, case)
    states = case['states']
    baseline = states['baseline']
    snapshot(case, baseline, rows, [1])
    if id == 'ownership/worker-scale':
        snapshot(case, states['scaled_up'], rows, [1, 2, 3], baseline)
        snapshot(case, states['scaled_down'], rows, [1], states['scaled_up'])
        require(states['scaled_down'] == states['recovered'], 'recovered ownership differs from the final scaled assignment')
    else:
        for name in ('preparing_rebalance', 'at_kill'):
            snapshot(case, states[name], rows)
        if id == 'ownership/coordinator-kill-rebalance':
            for name in ('coordinator_replacement', 'broker_restored'):
                group_description(case, states[name], rows)
            snapshot(case, states['recovered'], rows, [1, 2], baseline, states['broker_restored'])
        else:
            snapshot(case, states['recovered'], rows, [2], baseline)
    safety['ownership_exposure'] = dict(case, audit_scope='independent raw group descriptions, successful joins, assignments and accepted commits; Go owns fault and process gates')
    for reference in (safety['safety'], safety['bootstrap']):
        reference['observations'] = (safety_path.parent/reference['observations']).resolve().relative_to(root).as_posix()
    return safety
