"""Paired measurements using the existing Kafka/file correctness workloads."""

import argparse
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import re
import statistics
import subprocess
import tarfile
import tempfile
import zipfile

from ci import run


CASES = {'kafka': 'TestLoadBoundedKafkaCopy/default',
         'file': 'TestLoadObservability/round-0/disabled'}
ARCHIVE_CASES = {f'archive_{direction}': f'TestLoadArchiveCopy/{direction}'
                 for direction in ('capture', 'replay', 'file')}


def command(*args, **kwargs):
    return subprocess.check_output(list(args), text=True, **kwargs).strip()


def resolve(ref):
    return command('git', 'rev-parse', '--verify', '--end-of-options', ref + '^{commit}')


def previous(repository):
    runs = json.loads(command('gh', 'run', 'list', '--repo', repository, '--workflow',
                              'performance.yml', '--branch', 'main', '--event', 'schedule', '--status', 'success',
                              '--limit', '30', '--json', 'databaseId,headSha'))
    for item in runs:
        artifacts = json.loads(command('gh', 'api', f'repos/{repository}/actions/runs/{item["databaseId"]}/artifacts'))['artifacts']
        for artifact in artifacts:
            if artifact['name'] != 'performance' or artifact['expired']:
                continue
            archive = subprocess.check_output(['gh', 'api', f'repos/{repository}/actions/artifacts/{artifact["id"]}/zip'])
            with zipfile.ZipFile(io.BytesIO(archive)) as zipped:
                report = json.loads(zipped.read('performance.json'))
            if report.get('head') == item['headSha']:
                return report if report.get('compatible') else None
    return None


def plan():
    head = resolve('HEAD')
    requested = os.environ.get('BASE_INPUT', '')
    mode = os.environ.get('PERFORMANCE_MODE', 'calibration')
    if mode not in {'calibration', 'comparison'}:
        raise ValueError('PERFORMANCE_MODE must be calibration or comparison')
    event = os.environ.get('EVENT', '')
    baseline = None
    if requested:
        base = resolve(requested)
    elif mode == 'comparison' and event == 'pull_request':
        base = command('git', 'merge-base', resolve(os.environ['PR_BASE']), head)
    elif mode == 'comparison':
        baseline = previous(os.environ['GITHUB_REPOSITORY'])
        base = baseline['head'] if baseline else head
    else:
        base = head
    result = {'base': base, 'head': head,
              'run': not (event == 'schedule' and mode == 'comparison' and base == head and baseline is not None)}
    with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
        for key, value in result.items():
            output.write(f'{key}={str(value).lower()}\n')
    print(json.dumps(result, indent=2))


def build(ref, root):
    source = root / ref
    source.mkdir(parents=True, exist_ok=True)
    data = subprocess.check_output(['git', 'archive', ref])
    with tarfile.open(fileobj=io.BytesIO(data)) as archive:
        archive.extractall(source, filter='data')
    workflow = source / '.github/workflows/ci.yml'
    pin = re.search(r"go-version:\s*['\"]([0-9.]+)['\"]", workflow.read_text()) if workflow.exists() else None
    env = dict(os.environ, CGO_ENABLED='0', GOTOOLCHAIN='go' + pin[1] if pin else 'auto')
    binary = (source / 'kaferry').resolve()
    with (source / 'build.log').open('w') as log:
        subprocess.run(['go', 'build', '-buildvcs=false', '-trimpath', '-o', str(binary), './cmd/kaferry'],
                       cwd=source, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    return binary, {'sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
                    'build_info': command('go', 'version', '-m', str(binary)),
                    'scoped_sql': (source / 'internal/sql/options.go').exists()}


def pair_summary(pairs):
    ratios = [pair['head']['records_per_second'] / pair['base']['records_per_second'] for pair in pairs]
    return {'head_over_base': ratios, 'median': statistics.median(ratios),
            'minimum': min(ratios), 'maximum': max(ratios)}


def measure(harness, binary, case, directory, counts):
    directory.mkdir(parents=True, exist_ok=False)
    os.environ.update(KAFERRY_TEST_BINARY=str(binary),
                      KAFERRY_EXPECT_BINARY_SHA256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                      KAFERRY_LOAD_KAFKA_RECORDS=str(counts['kafka']), KAFERRY_LOAD_FILE_RECORDS=str(counts['file']))
    if case in ARCHIVE_CASES:
        os.environ["KAFERRY_LOAD_ARCHIVE_RECORDS"] = str(counts[case])
    test = ARCHIVE_CASES.get(case) or CASES[case]
    selector = '/'.join('^' + re.escape(part) + '$' for part in test.split('/'))
    code = run('copy', [str(harness), '-test.v', '-test.run', selector, '-test.timeout=10m'], directory)
    # Compiled Go test binaries write verbose text, not go test -json events.
    # The report is emitted only by the selected correctness test's cleanup.
    metrics = list(directory.glob('*.metrics.json'))
    if code or len(metrics) != 1:
        raise ValueError(f'{case} workload failed; see {directory}')
    result = json.loads(metrics[0].read_text())
    if result['test'] != test or not result['passed'] or result['skipped'] or result['records'] != counts[case] or result['records_per_second'] <= 0:
        raise ValueError(f'{case} workload has no valid measurement; see {directory}')
    if case != 'kafka' and result.get('archive_format') != 'avro':
        raise ValueError(f'{case} workload used the wrong archive format; see {directory}')
    return result


def execute(base, head, pairs, counts, archives=False, archive_payload="random"):
    base, head = resolve(base), resolve(head)
    counts = dict(counts)
    cases = dict(CASES)
    if archives:
        os.environ["KAFERRY_ARCHIVE_PAYLOAD"] = archive_payload
        cases.update(ARCHIVE_CASES)
        counts.update({case: counts['file'] for case in ARCHIVE_CASES})
    directory = Path('build-results').resolve()
    directory.mkdir(exist_ok=True)
    measurements = Path(tempfile.mkdtemp(prefix='performance-', dir=directory))
    binaries, identities = {}, {}
    report = {'base': base, 'head': head, 'phase': 'calibration' if base == head else 'comparison',
              'compatible': False, 'counts': counts, 'pairs': pairs, 'binaries': identities,
              'recorded_at': datetime.now(timezone.utc).isoformat(), 'platform': platform.platform(),
              'measurements': measurements.name,
              'archive_payload': archive_payload if archives else None, 'workloads': {}}
    try:
        report['broker_images'] = command('docker', 'compose', '-p', 'kaferry-compat', '--profile', 'cross-cluster', 'images', '--format', 'json')
        for ref in dict.fromkeys([head, base]):
            binaries[ref], identities[ref] = build(ref, Path('.cache/performance').resolve())
        legacy = [str(binaries[ref]) for ref in binaries if not identities[ref].get('scoped_sql', True)]
        if len(legacy) > 1:
            raise ValueError('comparison needs a current SQL workload on at least one side')
        os.environ['KAFERRY_LEGACY_SQL_BINARY'] = legacy[0] if legacy else ''
        harness = Path('.cache/performance/workload').resolve()
        subprocess.run(['go', 'test', '-c', '-tags=integration,load', '-o', str(harness), './tests/compat'], env=dict(os.environ, CGO_ENABLED='0'), check=True)
        for case in cases:
            for side in ('head', 'base'):
                try:
                    measure(harness, binaries[head if side == 'head' else base], case, measurements / f'{case}-warmup-{side}', counts)
                except ValueError:
                    if side == 'base' and base != head:
                        report['unavailable'] = 'The baseline failed the current workload (incompatibility or a transient failure); comparison is unavailable. See logs.'
                        return report
                    raise
            samples = []
            for index in range(pairs):
                order = ('base', 'head') if index % 2 == 0 else ('head', 'base')
                pair = {'first': order[0]}
                for side in order:
                    pair[side] = measure(harness, binaries[head if side == 'head' else base], case,
                                         measurements / f'{case}-{index}-{side}', counts)
                samples.append(pair)
            report['workloads'][case] = {'samples': samples, **pair_summary(samples)}
        report['compatible'] = True
        return report
    finally:
        if not report['compatible']:
            report.setdefault('unavailable', 'Measurements are incomplete and do not update the baseline.')
            report['workloads'] = {}
        (directory / 'performance.json').write_text(json.dumps(report, indent=2) + '\n')
        rows = ['## Paired performance', '', f'Base `{base}` · Head `{head}` · {report["phase"]}', '',
                report.get('unavailable', 'Same-runner measurements; ratios above 1 mean higher throughput. No regression gate.'), '',
                '| Workload | Median head/base | Pair min–max |', '| --- | ---: | ---: |']
        rows += [f'| {name} | {value["median"]:.3f} | {value["minimum"]:.3f}–{value["maximum"]:.3f} |' for name, value in report['workloads'].items()]
        if summary := os.environ.get('GITHUB_STEP_SUMMARY'):
            with open(summary, 'a') as output:
                output.write('\n'.join(rows) + '\n')
        print('\n'.join(rows))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('plan')
    measure_parser = sub.add_parser('run')
    measure_parser.add_argument('base')
    measure_parser.add_argument('head')
    measure_parser.add_argument('--pairs', type=int, choices=range(2, 11, 2), default=6)
    measure_parser.add_argument('--archives', action='store_true')
    measure_parser.add_argument("--archive-payload", choices=("compressible", "random"), default="random")
    args = parser.parse_args()
    if args.command == 'plan':
        plan()
    else:
        execute(args.base, args.head, args.pairs, {'kafka': 160000, 'file': 200000}, args.archives, args.archive_payload)
