"""Retain release measurements as JSON and Markdown reports."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import re

from tools.qualification.qualify import verify_release
from tools.ci.suites import LOAD_PROFILES


LOAD_CASES = {
    'TestLoadBoundedKafkaCopy/default': 'Kafka → Kafka (key)',
    'TestLoadBoundedKafkaCopy/preserved_partitions': 'Kafka → Kafka (preserve)',
    'TestLoadArchiveCopy/capture': 'Kafka → archive',
    'TestLoadArchiveCopy/replay': 'Archive → Kafka',
    'TestLoadArchiveCopy/file': 'Archive → archive',
}
LOAD_CASES.update({'TestLoadBoundedKafkaCopy/' + name: 'Kafka → Kafka (' + name + ')' for name in LOAD_PROFILES})
SOAK = 'TestStreamingSoakFidelityCommitLagMemoryAndShutdown'


def collect(evidence, archive, matrix, commit, version):
    if not re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+(?:[.-][A-Za-z0-9.-]+)?', version):
        raise ValueError('invalid release version')
    verify_release(matrix, archive, commit)
    document = json.loads(matrix.read_text())
    if document['schema'] != 1 or document['tier'] != 'full' or document['status'] != 'passed':
        raise ValueError('a passing full compatibility matrix is required')
    digest = document['cases'][0]['binary_sha256']
    environments, measurements, seen = {}, [], set()
    recorded_workloads, unrecorded_workloads = set(), set()
    for suite in ('load', 'soak'):
        directory = evidence / f'qualification-{suite}'
        env = json.loads((directory / f'{suite}.environment.json').read_text())
        result = json.loads((directory / f'{suite}.result.json').read_text())
        if (env['commit'] != commit or env['binary_sha256'] != digest or
                env['working_tree_dirty'] is not False):
            raise ValueError(f'{suite}: source and binary evidence does not match')
        if env['system'] != 'Linux' or env['machine'] != 'x86_64':
            raise ValueError(f'{suite}: Linux AMD64 measurements are required')
        if result['exit_code'] != 0:
            raise ValueError(f'{suite}: suite did not pass')
        environments[suite] = env
        for path in sorted(directory.glob('*.metrics.json')):
            item = json.loads(path.read_text())
            key = (suite, item['test'])
            if key in seen:
                raise ValueError(f'duplicate measurement: {key}')
            seen.add(key)
            if item['binary_sha256'] != digest or not item['passed'] or item['skipped']:
                raise ValueError(f'{item["test"]}: measurement did not pass for this binary')
            workload = item.get('workload')
            if workload is None:
                unrecorded_workloads.add(key)
                workload = {'name': 'not recorded'}
                item['workload'] = workload
            else:
                recorded_workloads.add(key)
            if not isinstance(workload, dict) or not isinstance(workload.get('name'), str) or not workload['name']:
                raise ValueError(f'{item["test"]}: workload identity is missing')
            profile = item['test'].removeprefix('TestLoadBoundedKafkaCopy/')
            if profile in LOAD_PROFILES:
                codec = profile.removeprefix('source-').removesuffix('-v1') if profile.startswith('source-') else 'none'
                if (workload['name'] != profile or workload.get('source_compression') != codec or
                        item.get('source_compression_records') != {codec: item['records']}):
                    raise ValueError(f'{item["test"]}: workload or observed source compression does not match')
            for field in ('records', 'records_per_second', 'elapsed_seconds'):
                value = item[field]
                if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                    raise ValueError(f'{item["test"]}: invalid {field}')
            measurements.append(dict(item, suite=suite))
    if recorded_workloads and unrecorded_workloads:
        raise ValueError('workload identity is missing from some measurements')
    required = {('load', test) for test in LOAD_CASES
                if recorded_workloads or test.removeprefix('TestLoadBoundedKafkaCopy/') not in LOAD_PROFILES} | {('soak', SOAK)}
    if not required <= seen or not environments['soak'].get('soak_duration'):
        raise ValueError('required load or soak measurements are missing')
    return {'schema': 1, 'version': version, 'source_commit': commit,
            'binary_sha256': digest, 'platform': 'linux-amd64',
            'environments': environments, 'measurements': measurements,
            'compatibility': [{key: case[key] for key in
                ('id', 'suite', 'source_version', 'destination_version', 'status')}
                for case in document['cases']]}


def markdown(report):
    version, commit = report['version'], report['source_commit']
    download = f'https://github.com/pagrus7/kaferry/releases/download/{version}'
    rows = [f'# Kaferry {version} release results', '',
            f'Linux AMD64 · source `{commit}` · binary SHA-256 `{report["binary_sha256"]}`', '',
            f'[Workflow run]({report["environments"]["load"]["run_url"]}) · '
            f'[Full matrix]({download}/compatibility.json) · '
            f'[Qualification]({download}/qualification.md) · '
            f'[Measurement JSON]({download}/release-results.json)', '',
            '## Compatibility', '',
            f'All {len(report["compatibility"])} broker, authentication and capture cases passed. '
            'The matrix checks correctness and compatibility; its elapsed times are not throughput benchmarks.', '',
            '## Throughput and memory', '',
            'Each row is one release-check invocation on a shared CI runner. Bounded load timing includes '
            'SQL input release through process exit. The soak uses paced input and destination stalls; '
            'its rate does not measure maximum copying capacity. These figures do not establish a performance '
            'change against another release. Use paired, interleaved runs for that comparison.', '',
            '| Suite / scenario | Workload | Records | Records/s | Sampled peak RSS MiB |',
            '| --- | --- | ---: | ---: | ---: |']
    for item in report['measurements']:
        label = LOAD_CASES.get(item['test'], item['test'])
        rows.append(f'| {item["suite"]} / {label} | {item["workload"]["name"]} | {item["records"]:,} | '
                    f'{item["records_per_second"]:,.0f} | {item["sampled_peak_rss_bytes"] / (1 << 20):.1f} |')
    rows += ['', '## Environments', '']
    for suite, env in report['environments'].items():
        rows.append(f'- {suite}: {env["platform"]}; {env["go"]}; started {env["started_at"]}.')
    rows += ['', f'Configured soak duration: {report["environments"]["soak"]["soak_duration"]}. '
             'The JSON retains every load/soak measurement, environment, work cap, lag and shutdown field '
             'reported by these checks. Detailed CLI logs and sample streams remain in the workflow artifacts '
             'for their configured retention period.', '']
    return '\n'.join(rows)


def write_results(evidence, archive, matrix, commit, version, output):
    report = collect(evidence, archive, matrix, commit, version)
    contents = {'release-results.json': json.dumps(report, indent=2, sort_keys=True) + '\n',
                'release-results.md': markdown(report)}
    output.mkdir(parents=True, exist_ok=True)
    for name, content in contents.items():
        (output / name).write_text(content, encoding='utf-8')
    (output / 'release-results.SHA256SUMS').write_text(''.join(
        f'{hashlib.sha256(content.encode()).hexdigest()}  {name}\n' for name, content in sorted(contents.items())))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('evidence', type=Path)
    parser.add_argument('archive', type=Path)
    parser.add_argument('matrix', type=Path)
    parser.add_argument('commit')
    parser.add_argument('version')
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    write_results(args.evidence, args.archive, args.matrix, args.commit, args.version, args.output)
