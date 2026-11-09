"""Recheck coordinator-loss operation and pending-join phases against raw evidence."""

import json

from tools.qualification.replicated_results import NEGOTIATION_EOF, source_operation_eof
from tools.qualification.fault_results import observation_path
from tools.qualification.ownership_audit import commits, records, segments, stamp
from tools.qualification.ownership_cases import (group_description, member_protocol,
                                                 require, route, snapshot)

ESTABLISHED = 'established-source-operation'
JOINING = 'pending-group-join'
SAVE = 'save progress: '+NEGOTIATION_EOF
CLOSE = 'close Kafka input: '+NEGOTIATION_EOF
JOIN = 'poll source: unable to join group session: '+NEGOTIATION_EOF


def phase(log):
    terminal = []
    for line in log.splitlines():
        try:
            json.loads(line)
        except json.JSONDecodeError:
            terminal.append(line)
    if source_operation_eof('\n'.join(terminal)):
        return ESTABLISHED
    return JOINING if terminal in ([JOIN], [JOIN, CLOSE]) else None


def protocol_cut(directory, case, fault):
    cut = case['coordinator_exit_audit']
    start, end, deadline = (stamp(cut[key]) for key in ('started', 'finished', 'deadline'))
    require(stamp(case['states']['preparing_rebalance']['finished']) <= start < end <= deadline <= start+2 and
            end <= stamp(fault['start']), 'coordinator protocol cut did not precede the final physical-fault recheck')
    references = cut['brokers']
    require(len(references) == 3 and {entry['target']['broker'] for entry in references} == {1,2,3} and
            len({entry['target']['container'] for entry in references}) == 3, 'coordinator protocol cut lacks three owned targets')
    selected = case['states']['at_kill']['coordinator']
    admin = next(target for target in fault['targets'] if target['listener'] == 'admin')
    rows = []
    for entry in references:
        target, raw = entry['target'], entry['raw']
        broker = target['broker']
        require(type(broker) is int and target['cluster'] == 'source' and target['listener'] == 'admin' and
                target['service'] == f'source-{broker}' and target['proxy'] == f'source-{broker}-admin' and
                target['proxy_container'] == admin['proxy_container'] and target['container'] and
                (broker != selected or target == admin), 'coordinator protocol cut differs from actual physical targets')
        name = f'source-{broker}.coordinator-pre-kill.requests.log'
        path = observation_path(directory, raw, name)
        require(0 < path.stat().st_size <= 16*1024**2, 'coordinator protocol cut exceeds the capture cap')
        prefix, before, final = (segments(path), segments(directory/f'source-{broker}.baseline.requests.log'),
                                 segments(directory/f'source-{broker}.final.requests.log'))
        require(all(any(value.startswith(data) for value in prefix.values()) for data in before.values()) and
                all(any(value.startswith(data) for value in final.values()) for data in prefix.values()),
                'coordinator protocol cut lost baseline data or is not an immutable prefix of final raw logs')
        for row in records(prefix):
            require(row['completed_at'] <= end, 'coordinator cut contains a response after its recorded finish')
            if isinstance(row.get('response'),dict) and 'errorCode' in row['response']:
                require(type(row['response']['errorCode']) is int, 'coordinator protocol error code is not an integer')
            row['broker'] = broker
            rows.append(row)
    group_description(case, case['states']['preparing_rebalance'], rows)
    return rows, end


def no_joined_progress(rows, group):
    for row in rows:
        key = row['requestHeader']['requestApiKeyName']
        if key not in ('JOIN_GROUP', 'SYNC_GROUP', 'OFFSET_COMMIT'):
            continue
        require(row['request']['groupId'] == group, 'pending member used another group')
        require(not (key in ('JOIN_GROUP','SYNC_GROUP') and row['response']['errorCode'] == 0),
                'pending member completed joining or assignment before exit')
        require(key != 'OFFSET_COMMIT' or not any(error == 0 for _,_,error in commits(row)),
                'pending member committed before exit')


def joining_proof(case, worker, member, prefix, final, end, log):
    require(worker['route'] == 2 and not member['partitions'], 'joining EOF does not belong to the unassigned peer')
    summary = worker['copy_summary']
    require(all(type(summary.get(key)) is int and summary[key] == 0 for key in
                ('submitted','confirmed','peak_uncommitted_records','peak_uncommitted_bytes')),
            'joining EOF requires its exact zero-work summary')
    lines = log.splitlines()
    for line in lines:
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        require(value.get('msg') != 'partition assigned' and
                (value.get('msg') != 'progress' or value.get('submitted') == value.get('confirmed') == 0),
                'joining EOF contains positive work or an assignment event')
    client = member['client_id']
    history_prefix = [row for row in prefix if row['requestHeader']['clientId'] == client]
    for row in history_prefix:
        member_protocol(row, worker, client, end)
        require(type(row['totalTimeMs']) in (int, float) and row['totalTimeMs'] >= 0,
                'pending protocol has invalid request timing')
    selected = [row for row in history_prefix if row['broker'] == case['states']['at_kill']['coordinator']]
    joins = [row for row in selected if row['requestHeader']['requestApiKeyName'] == 'JOIN_GROUP' and
             row['request']['groupId'] == case['group'] and row['request']['memberId'] == '' and
             type(row['response']['errorCode']) is int and row['response']['errorCode'] == 79 and row['response']['memberId'] == member['member_id'] and
             row['request']['sessionTimeoutMs'] == case['timeout_milliseconds']['session.timeout.ms'] and
             row['request']['rebalanceTimeoutMs'] == case['timeout_milliseconds']['max.poll.interval.ms']]
    require(any(api['requestHeader']['requestApiKeyName'] == 'API_VERSIONS' and type(api['response']['errorCode']) is int and api['response']['errorCode'] == 0 and
                api['connection'] == join['connection'] and api['completed_at'] <= join['received_at']
                for join in joins for api in selected), 'pending EOF lacks successful API negotiation and assigned member-ID protocol')
    history = [row for row in final if row['requestHeader']['clientId'] == client and row['received_at'] < stamp(worker['stopped_at'])]
    for row in history:
        require(stamp(worker['started_at']) <= row['received_at'] < stamp(worker['stopped_at']) and row['received_at'] <= row['completed_at'] and
                row['listener'] == 'WORKER2', 'pending protocol lies outside its actual route lifetime')
    no_joined_progress(history, case['group'])


def validate(directory, case, rows, workers, fault, command):
    candidates = [worker for worker in workers if worker.get('coordinator_exit_phase')]
    if not candidates and not case.get('coordinator_exit_audit'):
        return
    prefix, end = protocol_cut(directory, case, fault)
    held = next(event for event in case['events'] if event.get('reason') == 'OS confirms held worker')
    for worker in candidates:
        log = (directory/worker['log']).read_text()
        kind = phase(log)
        require(kind == worker['coordinator_exit_phase'] and worker['exit_code'] == 1 and
                stamp(command['coordinator_kill_started']) < stamp(worker['stopped_at']) < stamp(fault['heal']),
                'coordinator EOF phase or strict physical exit window differs')
        member = next(member for member in case['states']['at_kill']['members'] if route(member['client_id']) == worker['route'])
        if kind == ESTABLISHED:
            summary = worker['copy_summary']
            require(worker['route'] == 1 and type(summary['submitted']) is int and
                    summary['submitted'] == summary['confirmed'] > 0 and
                    member == case['states']['baseline']['members'][0], 'established EOF lacks its same baseline member or all-confirmed work')
            snapshot(case, case['states']['baseline'], prefix, [1])
        else:
            require(kind == JOINING and stamp(held['observed_at']) < stamp(worker['started_at']) < stamp(command['coordinator_kill_started']),
                    'joining EOF lifetime did not begin in the held-worker stimulus')
            joining_proof(case, worker, member, prefix, rows, end, log)
