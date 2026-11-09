"""Bind broker recovery to a fresh selected ADMIN API response."""

import hashlib
import json
from datetime import datetime

CRITERION = 'target-admin-metadata-all-topics-v1'


def stamp(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def admin(side, broker):
    return f'127.0.0.1:{(46092 if side == "source" else 47092)+broker-1}'


def response_valid(response, cluster, side, topic, partition, size, leader):
    brokers = response['Brokers']
    if (type(response['Version']) is not int or response['Version'] < 1 or type(response['ErrorCode']) is not int or response['ErrorCode'] != 0 or response['ClusterID'] != cluster or len(brokers) != 3 or
            any(type(b['NodeID']) is not int or type(b['Port']) is not int for b in brokers) or
            {b['NodeID'] for b in brokers} != {1, 2, 3} or
            any(f'{b["Host"]}:{b["Port"]}' != admin(side, b['NodeID']) for b in brokers)):
        return False
    topics = [entry for entry in response['Topics'] if entry['Topic'] == topic]
    if len(topics) != 1 or topics[0]['ErrorCode'] != 0:
        return False
    parts = topics[0]['Partitions']
    if len(parts) != 24 or any(type(p['Partition']) is not int for p in parts) or {p['Partition'] for p in parts} != set(range(24)):
        return False
    for part in parts:
        isr = part['ISR']
        if (type(part['ErrorCode']) is not int or part['ErrorCode'] != 0 or
                type(part['Leader']) is not int or any(type(n) is not int for n in isr) or not isr or len(set(isr)) != len(isr) or
                not set(isr) <= {1, 2, 3} or part['Leader'] not in isr):
            return False
        if partition is None or part['Partition'] == partition:
            if len(isr) != size or leader is not None and part['Leader'] != leader:
                return False
    return True


def validate(case, state, targets, earliest, latest, partition=None, size=3, leader=None):
    """Require every resolved target under the same existing two-second query."""
    try:
        checks = state['broker_readiness']
        if (case['readiness_criterion'] != CRITERION or not case['fixture_project'] or
                set(case['cluster_ids']) != {'source', 'destination'} or
                any(not value for value in case['cluster_ids'].values()) or
                len(set(case['cluster_ids'].values())) != 2 or len(checks) != 1):
            raise ValueError('missing explicit readiness criterion or cluster identity')
        check = checks[0]
        at, deadline = stamp(check['at']), stamp(check['deadline'])
        if (check['criterion'] != CRITERION or check['passed'] is not True or check.get('error') or
                check['diagnostic_state'] != 'complete' or
                not earliest <= at < deadline <= at+2 or at > latest or
                check['containers'] != [target['container'] for target in targets] or
                len(check['details']) != len(targets) or len(check['api_probes']) != len(targets)):
            raise ValueError('readiness observation lacks one bounded successful target query')
        for target, docker, probe in zip(targets, check['details'], check['api_probes']):
            side, broker = target['cluster'], target['broker']
            address = admin(side, broker)
            cluster = case['cluster_ids'][side]
            topic = case['source_topic' if side == 'source' else 'destination_topic']
            health = docker['Health']
            if (side not in ('source', 'destination') or broker not in (1, 2, 3) or
                    target['service'] != f'{side}-{broker}' or
                    docker['ID'] != target['container'] or docker['Project'] != case['fixture_project'] or
                    docker['Service'] != target['service'] or docker['Running'] is not True or
                    not 0 < stamp(docker['StartedAt']) <= at or
                    not docker['Healthcheck']['Test'] or docker['Healthcheck']['Test'][0] == 'NONE' or
                    health['Status'] not in ('starting', 'healthy', 'unhealthy') or
                    type(health['FailingStreak']) is not int or health['FailingStreak'] < 0 or
                    len(health['Log'] or []) > 5 or docker['probe_history_state'] not in ('retained', 'unavailable')):
                raise ValueError('readiness Docker state differs from running owned target')
            for row in health['Log'] or []:
                if (stamp(row['Start']) > stamp(row['End']) or type(row['ExitCode']) is not int or
                        len(row['Output'].encode()) > 2060):
                    raise ValueError('invalid bounded Docker probe history')
            if (probe['target'] != target or probe['client_cluster'] != side or probe['route'] != 'admin' or
                    probe['api_key'] != 3 or probe['all_topics'] is not True or
                    probe['expected_address'] != address or probe['transport_broker'] != broker or
                    probe['transport_address'] != address or probe['remote_address'] != address or
                    probe['cluster_id'] != cluster or probe['topic'] != topic or probe['error'] or
                    probe['response_truncated'] is not False or
                    not at <= stamp(probe['started_at']) <= stamp(probe['response_at']) <= stamp(probe['finished_at']) <= min(deadline, latest) or
                    not 0 < stamp(probe['connected_at']) <= stamp(probe['response_at'])):
                raise ValueError('readiness lacks fresh pinned ADMIN transport/response evidence')
            raw = probe['response_json'].encode()
            if (not 0 < len(raw) <= 64*1024 or hashlib.sha256(raw).hexdigest() != probe['response_sha256'] or
                    not response_valid(json.loads(raw), cluster, side, topic, partition, size, leader)):
                raise ValueError('readiness response differs from required cluster, routes, workload or ISR')
    except (KeyError, TypeError, IndexError, OverflowError, json.JSONDecodeError) as error:
        raise ValueError('malformed broker readiness evidence') from error
