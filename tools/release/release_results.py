"""Retain release measurements and render the SVG source for the README image."""

import argparse
import hashlib
from html import escape
import json
import math
from pathlib import Path
import re

from tools.qualification.qualify import verify_release


LOAD_CASES = {
    'TestLoadBoundedKafkaCopy/default': 'Kafka → Kafka (key)',
    'TestLoadBoundedKafkaCopy/preserved_partitions': 'Kafka → Kafka (preserve)',
    'TestLoadArchiveCopy/capture': 'Kafka → archive',
    'TestLoadArchiveCopy/replay': 'Archive → Kafka',
    'TestLoadArchiveCopy/file': 'Archive → archive',
}
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
            for field in ('records', 'records_per_second', 'elapsed_seconds'):
                value = item[field]
                if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                    raise ValueError(f'{item["test"]}: invalid {field}')
            measurements.append(dict(item, suite=suite))
    required = {('load', test) for test in LOAD_CASES} | {('soak', SOAK)}
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
            '| Suite / scenario | Records | Records/s | Sampled peak RSS MiB |',
            '| --- | ---: | ---: | ---: |']
    for item in report['measurements']:
        label = LOAD_CASES.get(item['test'], item['test'])
        rows.append(f'| {item["suite"]} / {label} | {item["records"]:,} | '
                    f'{item["records_per_second"]:,.0f} | {item["sampled_peak_rss_bytes"] / (1 << 20):.1f} |')
    rows += ['', '## Environments', '']
    for suite, env in report['environments'].items():
        rows.append(f'- {suite}: {env["platform"]}; {env["go"]}; started {env["started_at"]}.')
    rows += ['', f'Configured soak duration: {report["environments"]["soak"]["soak_duration"]}. '
             'The JSON retains every load/soak measurement, environment, work cap, lag and shutdown field '
             'reported by these checks. Detailed CLI logs and sample streams remain in the workflow artifacts '
             'for their configured retention period.', '']
    return '\n'.join(rows)


def svg(report):
    parts = ['<svg xmlns="http://www.w3.org/2000/svg" width="820" height="395" '
             'viewBox="0 0 820 395" role="img" aria-labelledby="title desc">',
             f'<title id="title">Kaferry {escape(report["version"])} release results</title>',
             '<desc id="desc">Passing Kafka compatibility matrix and observed Linux AMD64 load throughput. '
             'Shared-runner measurements; click through for inputs, scope and full data.</desc>',
             '<rect width="820" height="395" rx="12" fill="#f6f8fa"/>',
             '<g font-family="Arial, sans-serif" fill="#1f2937">']

    def text(x, y, value, size=13, anchor='start'):
        parts.append(f'<text x="{x}" y="{y}" font-size="{size}" text-anchor="{anchor}">'
                     f'{escape(str(value))}</text>')

    text(24, 33, f'Kaferry {report["version"]} · release results', 22)
    text(24, 55, f'Linux AMD64 · source {report["source_commit"][:12]} · '
         f'{len(report["compatibility"])}/{len(report["compatibility"])} compatibility cases passed')
    text(24, 88, 'Kafka brokers: source → destination', 15)
    brokers = [case for case in report['compatibility'] if case['suite'] == 'broker']
    versions = sorted({case['source_version'] for case in brokers},
                      key=lambda value: tuple(map(int, value.split('.'))))
    for column, version in enumerate(versions):
        text(124 + column * 57, 113, version, 11, 'middle')
    for row, version in enumerate(versions):
        text(80, 139 + row * 27, version, 12, 'end')
        for column, destination in enumerate(versions):
            case = next(case for case in brokers if case['source_version'] == version
                        and case['destination_version'] == destination)
            x, y = 98 + column * 57, 122 + row * 27
            parts.append(f'<rect x="{x}" y="{y}" width="52" height="23" rx="4" fill="#d1fae5"/>')
            text(x + 26, y + 16, case['status'], 10, 'middle')
    auth = sum(case['suite'] == 'auth' for case in report['compatibility'])
    text(24, 282, f'Authentication: {auth}/{auth} version cases passed')
    text(24, 305, f'Capture endurance passed · soak {report["environments"]["soak"]["soak_duration"]} passed')
    text(442, 88, 'Load throughput · records/s', 15)
    selected = [next(item for item in report['measurements']
                     if item['suite'] == 'load' and item['test'] == test) for test in LOAD_CASES]
    for row, item in enumerate(selected):
        text(442, 126 + row * 37, LOAD_CASES[item['test']])
        text(795, 126 + row * 37, f'{item["records_per_second"]:,.0f}', 16, 'end')
    counts = {item['records'] for item in selected}
    scope = f'{next(iter(counts)):,} records per load case' if len(counts) == 1 else 'Mixed load record counts; see full data'
    text(24, 341, scope + ' · one invocation per scenario')
    text(24, 362, 'Timing: SQL input release through process exit · shared CI runners.')
    text(24, 383, 'Click for measurement inputs, qualification scope and full data.')
    parts += ['</g>', '</svg>']
    return '\n'.join(parts) + '\n'


def write_results(evidence, archive, matrix, commit, version, output):
    report = collect(evidence, archive, matrix, commit, version)
    contents = {'release-results.json': json.dumps(report, indent=2, sort_keys=True) + '\n',
                'release-results.md': markdown(report), 'release-results.svg': svg(report)}
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
