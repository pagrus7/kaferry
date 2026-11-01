#!/usr/bin/env python3
"""Independently seed and reconcile a real CLI through all four copy directions."""

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import tempfile
import uuid


def encoded(value):
    return None if value is None else base64.b64encode(value).decode('ascii')


def records(count=96):
    stamp = int(time.time() * 1000) - 60000
    result = []
    for identity in range(count):
        key = None if identity % 7 == 0 else b'' if identity % 7 == 1 else bytes([0, 255, identity])
        value = None if identity % 11 == 0 else b'' if identity % 11 == 1 else bytes(range(256)) * (256 if identity % 17 == 0 else 4)
        headers = [('qualification-id', str(identity).encode()), ('dup', None), ('dup', b''), ('binary', b'\x00\xff')]
        result.append({'id': identity, 'partition': identity % 4, 'key': key, 'value': value,
                       'headers': headers, 'timestamp_ms': stamp})
    return result


def logical(key, value, headers, timestamp):
    return {'key': encoded(key), 'value': encoded(value),
            'headers': [(name, encoded(data)) for name, data in headers], 'timestamp_ms': timestamp}


def reconcile(messages, expected, preserve):
    wanted = {r['id']: r for r in expected}
    seen = set()
    for message in messages:
        headers = message.headers() or []
        ids = [value for name, value in headers if name == 'qualification-id']
        if len(ids) != 1:
            raise ValueError('record lacks its unique qualification identity')
        identity = int(ids[0])
        if identity not in wanted or identity in seen:
            raise ValueError(f'unexpected or duplicated record ID {identity}')
        record = wanted[identity]
        if logical(message.key(), message.value(), headers, message.timestamp()[1]) != logical(
                record['key'], record['value'], record['headers'], record['timestamp_ms']):
            raise ValueError(f'record ID {identity} changed bytes, headers or timestamp')
        if preserve and message.partition() != record['partition']:
            raise ValueError(f'record ID {identity} changed its preserved partition')
        seen.add(identity)
    missing = set(wanted) - seen
    if missing:
        raise ValueError(f'{len(missing)} selected records missing; first IDs: {sorted(missing)[:5]}')
    return {'expected': len(wanted), 'verified': len(seen), 'missing': 0, 'duplicates': 0, 'corrupted': 0}


def client_properties(properties, region):
    result = dict(properties)
    if result.get('sasl.mechanism') == 'AWS_MSK_IAM':
        if not region:
            raise ValueError('AWS_MSK_IAM requires aws_region in the qualification configuration')
        try:
            from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
        except ImportError:
            raise ValueError('install tests/qualification/requirements-cloud.txt for the independent MSK client')
        result['sasl.mechanism'] = 'OAUTHBEARER'
        def token(_):
            value, expiration = MSKAuthTokenProvider.generate_auth_token(region)
            return value, expiration / 1000
        result['oauth_cb'] = token
    return result


def read_topic(consumer_type, topic_partition, properties, topic, partitions):
    consumer = consumer_type(dict(properties, **{'group.id': 'qualification-check-' + uuid.uuid4().hex,
                                                'enable.auto.commit': False, 'isolation.level': 'read_committed'}))
    try:
        ends = {p: consumer.get_watermark_offsets(topic_partition(topic, p), timeout=30)[1] for p in range(partitions)}
        positions = {p: 0 for p in ends}
        consumer.assign([topic_partition(topic, p, 0) for p in ends])
        deadline = time.monotonic() + 60
        while any(positions[p] < ends[p] for p in ends):
            if time.monotonic() > deadline:
                raise ValueError('independent reader did not reach the captured destination ends')
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise ValueError('independent consumer failed: ' + str(message.error()))
            positions[message.partition()] = message.offset() + 1
            yield message
    finally:
        consumer.close()


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def run_probe(args):
    try:
        import confluent_kafka as kafka
        from confluent_kafka.admin import AdminClient, NewTopic
    except ImportError:
        raise ValueError('run python3 scripts/qualify.py setup before broker/cloud qualification')
    config = json.loads(args.config.read_text()) if args.config else {}
    source = config.get('source', {'bootstrap.servers': args.source})
    destination = config.get('destination', {'bootstrap.servers': args.destination})
    fixture_source = client_properties(config.get('fixture_source', source), config.get('aws_region'))
    fixture_destination = client_properties(config.get('fixture_destination', destination), config.get('aws_region'))
    admins = [AdminClient(fixture_source), AdminClient(fixture_destination)]
    namespace = 'kaferry-qualification-' + uuid.uuid4().hex
    expected = records()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    fd, private_path = tempfile.mkstemp(prefix='kaferry-qualification-', suffix='.toml')
    configuration = Path(private_path)
    (output / 'checkpoints').mkdir(exist_ok=True)
    with os.fdopen(fd, 'w') as private:
        private.write(''.join('[connections.' + name + ']\n' + ''.join(
        json.dumps(key) + '=' + json.dumps(value) + '\n' for key, value in properties.items())
        for name, properties in [('source', source), ('destination', destination)]) +
        '[worker]\ncheckpoint_dir=' + json.dumps(str(output / 'checkpoints')) + '\n')
    report = {'schema': 1, 'status': 'running', 'checker': 'confluent-kafka ' + kafka.__version__,
              'librdkafka': kafka.libversion()[0], 'binary_sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest(),
              'cases': []}
    created = [[], []]

    def topic(side, suffix, partitions):
        name = namespace + '-' + suffix
        futures = admins[side].create_topics([NewTopic(name, num_partitions=partitions, replication_factor=int(config.get('replication_factor', -1)))], request_timeout=60)
        futures[name].result(timeout=70)
        created[side].append(name)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            metadata = admins[side].list_topics(name, timeout=10).topics.get(name)
            if metadata and not metadata.error and len(metadata.partitions) == partitions and all(
                    not p.error and p.leader >= 0 for p in metadata.partitions.values()):
                break
            time.sleep(0.1)
        else:
            raise ValueError('fixture topic leaders did not become ready')
        return name

    def copy(label, query):
        path = output / (label + '.sql')
        path.write_text(query + ';\n')
        with (output / (label + '.cli.log')).open('w') as log:
            result = subprocess.run([str(args.binary), 'run', '-c', str(configuration), '-f', str(path)],
                                    stdout=log, stderr=subprocess.STDOUT, timeout=120)
        if result.returncode:
            raise ValueError(f'{label}: CLI exited {result.returncode}; inspect {label}.cli.log')

    def copy_resume(label, query):
        copy(label + '-partial', query + ' OPTIONS(WORKER_STOP_AFTER_RECORDS=7)')
        copy(label + '-resume', query)
        copy(label + '-completed', query)

    def check(label, name, partitions, preserve):
        counts = reconcile(read_topic(kafka.Consumer, kafka.TopicPartition, fixture_destination, name, partitions), expected, preserve)
        report['cases'].append(dict(counts, name=label))
        print(label + ': ' + json.dumps(counts), flush=True)

    try:
        report['source_cluster'] = admins[0].list_topics(timeout=30).cluster_id
        report['destination_cluster'] = admins[1].list_topics(timeout=30).cluster_id
        src = topic(0, 'source', 4)
        # Different librdkafka producers exercise every supported compression codec.
        for number, codec in enumerate(('none', 'gzip', 'snappy', 'lz4', 'zstd')):
            producer = kafka.Producer(dict(fixture_source, **{'compression.type': codec, 'enable.idempotence': True}))
            errors = []
            for record in expected[number * 20:min((number + 1) * 20, len(expected))]:
                producer.produce(src, partition=record['partition'], key=record['key'], value=record['value'],
                                 headers=record['headers'], timestamp=record['timestamp_ms'],
                                 on_delivery=lambda error, message: errors.append(str(error)) if error else None)
            if producer.flush(60) or errors:
                raise ValueError('independent fixture producer did not deliver its complete input')
        def source_sql(group):
            return f"FROM TOPIC {sql_string(src + '@source')} WITH (GROUP={sql_string(namespace + '-' + group)}, START=EARLIEST) WHERE RECORD_OFFSET < LATEST"
        for mode, partitions in [('PRESERVE', 4), ('KEY', 5)]:
            dst = topic(1, mode.lower(), partitions)
            query = f"INSERT INTO TOPIC {sql_string(dst + '@destination')} WITH (PARTITIONING={mode}) SELECT * " + source_sql(mode)
            copy_resume('topic-' + mode.lower(), query)
            check('topic-' + mode.lower(), dst, partitions, mode == 'PRESERVE')
        archive, clone = output / 'captured', output / 'cloned'
        copy_resume('capture', f"INSERT INTO ARCHIVE {sql_string(archive)} SELECT * " + source_sql('capture'))
        copy_resume('clone', f"INSERT INTO ARCHIVE {sql_string(clone)} SELECT * FROM ARCHIVE {sql_string(archive)} WITH (GROUP='clone')")
        dst = topic(1, 'restored', 4)
        copy_resume('replay', f"INSERT INTO TOPIC {sql_string(dst + '@destination')} SELECT * FROM ARCHIVE {sql_string(clone)} WITH (GROUP='replay')")
        check('capture-clone-replay', dst, 4, True)
        report['status'] = 'passed'
    finally:
        try:
            for admin, names in zip(admins, created):
                for future in admin.delete_topics(names, request_timeout=30).values() if names else []:
                    future.result(timeout=40)
        except Exception:
            report['status'] = 'failed'
            raise
        finally:
            configuration.unlink(missing_ok=True)
            (output / 'reconciliation.json').write_text(json.dumps(report, indent=2) + '\n')
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--source', default='127.0.0.1:39092')
    parser.add_argument('--destination', default='127.0.0.1:40092')
    parser.add_argument('--config', type=Path, help='private JSON containing source/destination Kafka properties')
    args = parser.parse_args()
    args.binary = args.binary.resolve()
    run_probe(args)


if __name__ == '__main__':
    main()
