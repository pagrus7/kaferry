"""Validate the selected CLI's correlated Kafka 4.4 request log for one case.

The pinned upstream contracts are Request.updateRequestMetrics,
RequestConvertToJson.requestDescMetrics and SocketServer.processDisconnected:
https://github.com/apache/kafka/tree/4.4.0/server/src/main/java/org/apache/kafka/network
https://github.com/apache/kafka/blob/4.4.0/core/src/main/scala/kafka/network/SocketServer.scala
The accepted logging configuration uses synchronous hourly RollingFile output.
Unknown configurations or incomplete observations cannot establish fencing.
Classic session expiry is also checked against the coordinator's own
expireClassicGroupMemberHeartbeat log and the independent ownership snapshot:
https://github.com/apache/kafka/blob/4.4.0/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java
"""

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import posixpath
import re

from tools.qualification.fault_results import observation_path, validate_case


PREFIX = 'KAFERRY_AUDIT_FILE '
LINE = re.compile(r'^\[([^]]+)\] (DEBUG|INFO) Completed request:')


def integer(value):
    if type(value) is int:
        return value
    if isinstance(value, str) and re.fullmatch(r'-?[0-9]+', value):
        return int(value)
    raise ValueError('audit contains a non-integer protocol field')


def stamp(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def logging_configuration(raw, normalized):
    decoded = json.loads(normalized.read_text())
    if decoded['raw_sha256'] != hashlib.sha256(raw.read_bytes()).hexdigest():
        raise ValueError('decoded logging configuration differs from captured YAML')
    configuration = decoded['configuration']['Configuration']
    if set(configuration) != {'Properties', 'Appenders', 'Loggers'}:
        raise ValueError('unknown broker logging configuration attributes')
    properties = configuration['Properties']['Property']
    patterns = [prop['value'] for prop in properties if prop['name'] == 'logPattern']
    appenders = configuration['Appenders']
    loggers = configuration['Loggers']
    if patterns != ['[%d] %p %m (%c)%n'] or set(appenders) != {'Console', 'RollingFile'} or set(loggers) != {'Root', 'Logger'}:
        raise ValueError('broker request log format or synchronous logging is unverified')
    if (appenders['Console'] != dict(name='STDOUT', PatternLayout=dict(pattern='${logPattern}')) or
            set(loggers['Root']) != {'level', 'AppenderRef'} or
            dict(ref='STDOUT') not in loggers['Root']['AppenderRef'] or
            any(set(logger)-{'name', 'level'} for logger in loggers['Logger'] if logger['name'] in
                ('org.apache.kafka', 'org.apache.kafka.network', 'org.apache.kafka.network.Request'))):
        raise ValueError('network request receipt logging must reach unfiltered synchronous console output')
    request = [appender for appender in appenders['RollingFile'] if appender['name'] == 'RequestAppender']
    selected = [logger for logger in loggers['Logger'] if logger['name'] == 'kafka.request.logger']
    expected = dict(name='RequestAppender', fileName='${sys:kafka.logs.dir}/kafka-request.log',
                    filePattern='${sys:kafka.logs.dir}/kafka-request.log.%d{yyyy-MM-dd-HH}',
                    PatternLayout=dict(pattern='${logPattern}'), TimeBasedTriggeringPolicy=dict(modulate=True, interval=1))
    if request != [expected] or len(selected) != 1 or set(selected[0]) != {'name', 'level', 'additivity', 'AppenderRef'} or \
            selected[0]['additivity'] is not False or selected[0]['AppenderRef'] != dict(ref='RequestAppender'):
        raise ValueError('request audit must use synchronous unfiltered hourly logging without deletion or size rotation')


def segments(path):
    result = {}
    name = None
    for line in path.read_bytes().splitlines(keepends=True):
        if line.startswith(PREFIX.encode()):
            name = line.decode().strip()[len(PREFIX):]
            if not re.fullmatch(r'kafka-request\.log(?:\.[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2})?', name) or name in result:
                raise ValueError('unknown or repeated broker request log segment')
            result[name] = bytearray()
        elif name is None:
            raise ValueError('broker audit lacks segment boundaries')
        else:
            result[name].extend(line)
    if not result or any(data and not data.endswith(b'\n') for data in result.values()):
        raise ValueError('broker audit is empty or ends with an incomplete line')
    return {name: bytes(data) for name, data in result.items()}


def records(parts):
    result = []
    keys = set()
    for data in parts.values():
        for line in data.decode().splitlines():
            match = LINE.match(line)
            if not match:
                raise ValueError('broker request log contains an unknown record')
            value, end = json.JSONDecoder().raw_decode(line[match.end():])
            if line[match.end()+end:].strip() != '(kafka.request.logger)':
                raise ValueError('broker request audit has an unknown suffix')
            when = datetime.strptime(match[1], '%Y-%m-%d %H:%M:%S,%f').replace(tzinfo=timezone.utc).timestamp()
            header = value['requestHeader']
            key = (value['connection'], integer(header['correlationId']))
            if key in keys:
                raise ValueError('duplicate broker audit correlation')
            keys.add(key)
            value['completed_at'] = when
            # The completion log includes total request time. A request already
            # accepted before transfer can finish sending its response afterward.
            value['received_at'] = when - value['totalTimeMs']/1000
            result.append(value)
    return result


def commits(record):
    request, response = record['request'], record['response']
    if request is None or response is None:
        raise ValueError('selected CLI commit omitted a full request or response')
    wanted = {}
    for topic in request['topics']:
        for partition in topic['partitions']:
            key = integer(partition['partitionIndex'])
            if key in wanted:
                raise ValueError('selected CLI commit repeated a partition')
            wanted[key] = integer(partition['committedOffset'])
    observed = {}
    for topic in response['topics']:
        for partition in topic['partitions']:
            key = integer(partition['partitionIndex'])
            if key in observed:
                raise ValueError('commit response repeated a partition')
            observed[key] = integer(partition['errorCode'])
    if wanted.keys() != observed.keys() or not wanted:
        raise ValueError('commit response omitted requested partitions')
    return [(part, offset, observed[part]) for part, offset in wanted.items()]


def received_commits(diagnostics, broker, workers):
    """Count network-received commits, including requests still queued to run.

    Request's constructor emits this trace before handing work to KafkaApis.
    Connection correlation IDs may repeat after reconnect; identity counters keep
    those receipts distinct, and completion logs additionally retain connections.
    """
    result = Counter()
    for line in diagnostics.splitlines():
        if 'received request: RequestHeader(apiKey=OFFSET_COMMIT,' not in line:
            continue
        match = re.fullmatch(r'\[[^]]+\] TRACE Processor [0-9]+ received request: '
            r'RequestHeader\(apiKey=OFFSET_COMMIT, apiVersion=[0-9]+, clientId=([^,]*), '
            r'correlationId=([0-9]+), headerVersion=[0-9]+\) -- (.*) \(org\.apache\.kafka\.network\.Request\)', line)
        if match is None:
            raise ValueError('network received commit has an unverified protocol log format')
        client, correlation, payload = match.groups()
        if client not in workers:
            continue
        group = re.search(r"\bgroupId='([^']*)'", payload)
        member = re.search(r"\bmemberId='([^']*)'", payload)
        generation = re.search(r'\bgenerationIdOrMemberEpoch=(-?[0-9]+)', payload)
        if group is None or member is None or generation is None:
            raise ValueError('network received commit omitted actual ownership identity')
        result[(broker, client, int(correlation), group[1], member[1], int(generation[1]))] += 1
    return result


def _validate(directory, identity=None):
    directory = Path(directory).resolve()
    case = json.loads((directory/'replicated-ownership.json').read_text())
    if case['id'] != 'ownership/zombie-worker' or case['source_partitions'] != 24:
        raise ValueError('unknown replicated ownership case')
    safety_path = observation_path(directory, case['safety_case'], 'case.json')
    safety = validate_case(safety_path)
    if (safety['id'] != case['id'] or safety['execution'] != 'candidate CLI' or
            safety['parameters'] != dict(source_partitions=24, routing='PRESERVE') or identity and any(safety[key] != identity[key] for key in
            ('source_commit', 'binary_sha256', 'binary_platform', 'execution_platform'))):
        raise ValueError('ownership proof differs from the selected case or executable identity')
    baseline, transfer = case['baseline'], case['transfer']
    worker1, worker2 = case['workers']
    first = {member['client_id']: member for member in baseline['members']}
    if set(first) != {worker1, worker2}:
        raise ValueError('baseline lacks both selected CLI workers')
    old_member = first[worker1]['member_id']
    old_parts = set(first[worker1]['partitions'])
    if (not old_parts or not first[worker2]['partitions'] or
            old_parts.intersection(first[worker2]['partitions']) or
            old_parts.union(first[worker2]['partitions']) != set(range(24))):
        raise ValueError('baseline does not assign all partitions exactly once')
    if (len(transfer['members']) != 1 or transfer['members'][0]['client_id'] != worker2 or
            set(transfer['members'][0]['partitions']) != set(range(24)) or
            stamp(transfer['started']) <= stamp(case['fault']['start']) or
            stamp(case['fault']['heal']) <= stamp(transfer['finished'])):
        raise ValueError('independent group state does not prove expiry and peer transfer before healing')
    peer = case['peer_progress']
    if (len(peer['members']) != 1 or peer['members'][0]['client_id'] != worker2 or
            set(peer['members'][0]['partitions']) != set(range(24)) or
            not stamp(transfer['finished']) <= stamp(peer['started']) <= stamp(peer['finished']) < stamp(case['heal_started']) <= stamp(case['fault']['heal']) or
            len(case['baseline_saved']) != 24 or len(case['peer_saved']) != 24 or
            any(before < 1 or after <= before+2 for before, after in zip(case['baseline_saved'], case['peer_saved']))):
        raise ValueError('peer progress was not independently observed while the route remained cut')
    targets = case['fault']['targets']
    fault = case['fault']['fault']
    if (len(targets) != 1 or targets[0]['cluster'] != 'source' or targets[0]['listener'] != 'worker1' or
            case['fault']['role']['kind'] != 'worker-coordinator' or
            case['fault']['role']['cluster'] != 'source' or case['fault']['role']['worker'] != 1 or
            set(fault)-{'kind', 'stream', 'value'} or fault['kind'] != 'timeout' or
            fault['stream'] != 'upstream' or integer(fault.get('value', 0)) != 0 or
            case['fault']['role']['group'] != case['group']):
        raise ValueError('fault did not blackhole only worker1 coordinator upstream without immediate close')
    windows = safety['fault_events']
    if len(windows) != 1 or len(windows[0]['injections']) != 1:
        raise ValueError('ownership proof requires exactly one fully observed coordinator blackhole')
    window = windows[0]
    injected = window['injections'][0]
    actual = injected['target']
    if (window['target'] != case['fault']['role'] or injected['kind'] != 'timeout' or
            any(actual[key] != case['fault'][key] for key in ('role', 'fault', 'targets', 'start')) or
            not window['started_at'] <= stamp(case['fault']['start']) <= injected['at'] <= stamp(transfer['finished']) or
            stamp(case['fault']['heal']) > window['healed_at'] or
            window['timeout_basis'] != {'session.timeout.ms': 6, 'socket.timeout.ms': 8, 'progress_save_timeout': 10,
                                        'delivery_timeout': 2, 'shutdown_timeout': 5} or
            window['recovery_seconds'] != 31):
        raise ValueError('ownership fault target, timing or configured recovery window differs from independent safety evidence')
    rows = []
    expiries = []
    received = Counter()
    files = [dict(path='replicated-ownership.json', sha256=hashlib.sha256((directory/'replicated-ownership.json').read_bytes()).hexdigest())]
    for broker in range(1, 4):
        config = directory/f'source-{broker}.log4j2.yaml'
        normalized = directory/f'source-{broker}.log4j2.json'
        logging_configuration(config, normalized)
        flags = directory/f'source-{broker}.jvm-logging.txt'
        options = dict(line.split('=', 1) for line in flags.read_text().splitlines())
        if (set(options) != {'-Dlog4j2.configurationFile', '-Dkafka.logs.dir'} or
                posixpath.normpath(options['-Dlog4j2.configurationFile']) != '/opt/kafka/config/log4j2.yaml' or
                posixpath.normpath(options['-Dkafka.logs.dir']) != '/opt/kafka/logs'):
            raise ValueError('broker JVM does not use the captured synchronous log configuration and directory')
        console = directory/f'source-{broker}.console.log'
        diagnostics = console.read_text()
        if not diagnostics or re.search(r'(?im)(ERROR.*(?:StatusLogger|StatusConsoleListener)|'
                r'(?:StatusLogger|StatusConsoleListener).*ERROR|AppenderLoggingException|'
                r'Unable to write to stream|Unable to create file|Error writing to stream)', diagnostics):
            raise ValueError('broker logging diagnostics are missing or report a logging failure')
        received.update(received_commits(diagnostics, broker, case['workers']))
        needle = f'Member {old_member} in group {case["group"]} has failed, removing it from the group.'
        for line in diagnostics.splitlines():
            if needle not in line:
                continue
            match = re.match(r'^\[([^]]+)\] INFO ', line)
            if not match:
                raise ValueError('broker session expiry log has an unverified timestamp')
            at = datetime.strptime(match[1], '%Y-%m-%d %H:%M:%S,%f').replace(tzinfo=timezone.utc).timestamp()
            if broker == targets[0]['broker'] and injected['at'] <= at <= stamp(transfer['finished']):
                expiries.append(at)
        before = directory/f'source-{broker}.baseline.requests.log'
        after = directory/f'source-{broker}.final.requests.log'
        start, finish = segments(before), segments(after)
        for data in start.values():
            if data and sum(part.startswith(data) for part in finish.values()) != 1:
                raise ValueError('broker request audit was truncated, lost or duplicated during rotation')
        initial, final = records(start), records(finish)
        guards = lambda items: sum(row['requestHeader']['clientId'] == case['audit_guard'] and
                                  row['requestHeader']['requestApiKeyName'] == 'API_VERSIONS' for row in items)
        if guards(initial) < 1 or guards(final) <= guards(initial):
            raise ValueError('broker audit lacks independent start and finish guards')
        for phase, items in (('baseline', initial), ('final', final)):
            mark = case['audit_marks'][phase]
            # Kafka's default timestamp rounds down to milliseconds. The guard
            # bounds also check that the broker used UTC and the host clock.
            lower = int(stamp(mark['started'])*1000)/1000
            if not any(row['requestHeader']['clientId'] == case['audit_guard'] and
                       row['requestHeader']['requestApiKeyName'] == 'API_VERSIONS' and
                       lower <= row['completed_at'] <= stamp(mark['finished']) for row in items):
                raise ValueError('broker audit clock or guard observation bounds differ')
        for row in final:
            row['broker'] = broker
        rows += final
        for path in (config, normalized, flags, console, before, after):
            files.append(dict(path=path.name, sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
    if not expiries:
        raise ValueError('independent ownership transfer lacks the coordinator session-expiry observation')
    selected = [row for row in rows if row['requestHeader']['clientId'] in case['workers']]
    if any(row['request'] is None or row['response'] is None for row in selected):
        raise ValueError('selected CLI traffic lacks correlated request/response observations')
    if any(row['listener'] != 'WORKER'+str(case['workers'].index(row['requestHeader']['clientId'])+1) for row in selected):
        raise ValueError('selected CLI traffic bypassed its isolated worker route')
    if any(row['requestHeader']['clientId'] == worker1 and row['requestHeader']['requestApiKeyName'] == 'LEAVE_GROUP' and
           row['response']['errorCode'] == 0 and stamp(case['fault']['start']) <= row['received_at'] <= stamp(transfer['finished'])
           for row in selected):
        raise ValueError('worker1 left the group rather than expiring behind its cut route')
    commit_rows = [row for row in selected if row['requestHeader']['requestApiKeyName'] == 'OFFSET_COMMIT']
    completed = Counter((row['broker'], row['requestHeader']['clientId'], integer(row['requestHeader']['correlationId']),
                         row['request']['groupId'], row['request']['memberId'],
                         integer(row['request']['generationIdOrMemberEpoch'])) for row in commit_rows)
    if received != completed:
        raise ValueError('network received commits lack complete correlated broker responses')
    joins = [row for row in selected if row['requestHeader']['requestApiKeyName'] == 'JOIN_GROUP' and
             integer(row['response'].get('errorCode')) == 0]
    baseline_joins = [row for row in joins if row['requestHeader']['clientId'] == worker1 and
                      row['response']['memberId'] == old_member and row['completed_at'] < stamp(case['fault']['start'])]
    if not baseline_joins:
        raise ValueError('broker audit lacks the actual selected CLI join generation')
    generation = max(integer(row['response']['generationId']) for row in baseline_joins)
    if generation != case['baseline_generation']:
        raise ValueError('fault was not armed against the actual selected CLI generation')
    before = [row for row in commit_rows if row['completed_at'] < stamp(case['fault']['start']) and
              row['requestHeader']['clientId'] == worker1 and row['request']['memberId'] == old_member and
              integer(row['request']['generationIdOrMemberEpoch']) == generation]
    if not old_parts.issubset({part for row in before for part, offset, error in commits(row) if error == 0 and offset > 0}):
        raise ValueError('broker audit lacks accepted worker1 baseline commits')
    post = [row for row in commit_rows if row['received_at'] >= stamp(transfer['finished'])]
    stale, peer_progress, rejoined = [], set(), []
    for row in post:
        request = row['request']
        if request['groupId'] != case['group']:
            raise ValueError('selected CLI committed another group')
        epoch = integer(request['generationIdOrMemberEpoch'])
        accepted = [(part, offset) for part, offset, error in commits(row) if error == 0]
        if row['requestHeader']['clientId'] == worker1:
            if request['memberId'] == old_member and epoch == generation:
                stale.append(row)
                if accepted:
                    raise ValueError('selected CLI stale generation committed progress after peer transfer')
            elif accepted:
                if not any(join['response']['memberId'] == request['memberId'] and
                           integer(join['response']['generationId']) == epoch and epoch > generation and
                           join['requestHeader']['clientId'] == worker1 and
                           stamp(case['heal_started']) <= join['completed_at'] <= row['completed_at'] for join in joins):
                    raise ValueError('worker1 accepted progress lacks an independently observed legitimate rejoin')
                rejoined.append(row)
        elif (epoch > generation and request['memberId'] == peer['members'][0]['member_id'] and
              row['completed_at'] < stamp(case['heal_started'])):
            peer_progress.update(part for part, offset in accepted if part in old_parts and offset > case['baseline_saved'][part]+2)
    if peer_progress != old_parts:
        raise ValueError('broker audit lacks accepted peer progress on every transferred partition')
    if any(len(case[key]) != 2 or any(value is not True for value in case[key])
           for key in ('processes_alive_after_transfer', 'processes_alive_before_heal')):
        raise ValueError('zombie interval lacks both live selected CLI processes through accepted peer progress before heal')
    if len(case['processes_alive_after_heal']) != 2 or case['processes_alive_after_heal'][1] is not True:
        raise ValueError('peer did not remain alive after heal')
    after_heal, final = case['publishing_after_heal'], case['publishing']
    if (stamp(after_heal['observed']) < stamp(case['fault']['heal']) or
            stamp(case['healed']['started']) < stamp(after_heal['observed']) or
            stamp(final['stopped']) < stamp(case['healed']['finished']) or
            len(after_heal['acknowledged_by_partition']) != 24 or len(final['acknowledged_by_partition']) != 24 or
            any(end <= start+24 for start, end in zip(after_heal['acknowledged_by_partition'], final['acknowledged_by_partition'])) or
            not 0 < final['max_acknowledgement_gap_seconds'] <= 2):
        raise ValueError('a distinct steady-publishing interval after healing was not observed')
    measurements = case['worker_measurements']
    if len(measurements) != 2 or any(not (0 < row['sampled_peak_rss_bytes'] <= 512*1024**2 and
            row['rss_samples'] > 0 and row['rss_failures'] == 0 and
            0 < row['peak_uncommitted_records'] <= row['record_cap'] == 1024 and
            0 < row['peak_uncommitted_bytes'] <= row['byte_cap'] == 134217728) for row in measurements):
        raise ValueError('selected CLI resource measurements are incomplete or exceed the configured caps')
    exits = {row['pid']: row['exit_code'] for row in safety['writer_observations']}
    if (len(exits) != 2 or {row['pid'] for row in measurements} != exits.keys() or
            any(type(row['exit_code']) is not int or row['exit_code'] != exits[row['pid']] for row in measurements) or
            measurements[1]['exit_code'] != 0 or measurements[0]['exit_code'] not in (0, 1) or
            any(not stamp(case['audit_marks']['baseline']['finished']) < stamp(row['started_at']) <
                    stamp(case['fault']['start']) < stamp(case['heal_started']) < stamp(row['stopped_at']) <
                    stamp(case['audit_marks']['final']['started']) for row in measurements)):
        raise ValueError('complete broker audit does not surround both observed selected CLI processes')
    # The product deliberately fails a periodic save on a fenced generation.
    # A nonzero exit qualifies only with its actual selected-CLI rejection,
    # observed after healing begins and before the OS exit observation. A
    # transport-only failure or traffic absence is never fencing evidence.
    rejected = [row for row in stale if all(error in (22, 25, 82) for _, _, error in commits(row)) and
                stamp(case['heal_started']) <= row['received_at'] <= row['completed_at'] <= stamp(measurements[0]['stopped_at'])]
    fenced_parts = {part for row in rejected for part, _, _ in commits(row)}
    rejected_owner = old_parts.issubset(fenced_parts)
    healed_worker = [member for member in case['healed']['members'] if member['client_id'] == worker1]
    current_parts = set(healed_worker[0]['partitions']) if len(healed_worker) == 1 else set()
    rejoin_parts = {part for row in rejoined if healed_worker and row['request']['memberId'] == healed_worker[0]['member_id'] and
                    stamp(case['heal_started']) <= row['received_at'] <= row['completed_at'] <= stamp(measurements[0]['stopped_at'])
                    for part, offset, error in commits(row) if error == 0 and offset > case['peer_saved'][part]}
    local_fenced_rejoin = bool(current_parts) and current_parts.issubset(rejoin_parts)
    if measurements[0]['exit_code'] == 1 and not rejected_owner:
        raise ValueError('worker1 failed without an independently audited stale-generation rejection before exit')
    if not rejected_owner and not local_fenced_rejoin:
        raise ValueError('traffic absence alone lacks positive fencing or legitimate current-generation progress')
    proof = dict(implementation='Kafka 4.4 correlated broker request log',
                session_expiry_observations=len(expiries), expiry_before_transfer_observed=True,
                received_commits=sum(received.values()), completed_commits=sum(completed.values()),
                stale_member=old_member, stale_generation=generation, stale_requests=len(stale),
                stale_rejections=len(rejected), accepted_stale_commits=0,
                fencing_mode='stale-generation-rejected' if rejected_owner else 'current-generation-rejoin',
                stale_fenced_partitions=sorted(fenced_parts), worker1_exit_code=measurements[0]['exit_code'],
                accepted_peer_partitions=sorted(peer_progress), accepted_rejoin_commits=len(rejoined),
                post_transfer_commits=len(post), source_partitions=24, audit_files=files,
                scope='network receipts and correlated broker completions around both selected CLI lifetimes; no rejection is inferred when a stale request is absent')
    for key in ('safety', 'bootstrap'):
        reference = safety[key]
        reference['observations'] = (safety_path.parent/reference['observations']).resolve().relative_to(directory).as_posix()
    return dict(safety, ownership_fencing=proof)


def validate(directory, identity=None):
    try:
        return _validate(directory, identity)
    except (KeyError, TypeError, IndexError, AttributeError) as error:
        raise ValueError('malformed replicated ownership evidence') from error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    print(json.dumps(validate(args.directory), sort_keys=True))


if __name__ == '__main__':
    main()
