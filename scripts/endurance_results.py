"""Package a standalone endurance run and optionally publish it to its tested release."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile

from ci import summarize


def sha256(path):
    with path.open('rb') as source:
        return stream_sha256(source)


def stream_sha256(source):
    digest = hashlib.sha256()
    for block in iter(lambda: source.read(1024 * 1024), b''):
        digest.update(block)
    return digest.hexdigest()


def prepare(evidence, output):
    evidence, output = Path(evidence).resolve(), Path(output).resolve()
    if output.exists():
        raise ValueError(f'publication directory already exists: {output}')
    receipt = json.loads((evidence / 'manifest.json').read_text())
    if receipt.get('suite') != 'endurance' or receipt.get('status') not in ('passed', 'failed', 'interrupted'):
        raise ValueError('a finished standalone endurance run is required')
    metrics = [json.loads(path.read_text()) for path in sorted(evidence.glob('*.metrics.json'))]
    if any(item.get('binary_sha256') != receipt.get('binary_sha256') for item in metrics):
        raise ValueError('metrics and manifest identify different binaries')
    if receipt['status'] == 'passed' and (len(metrics) != 1 or not metrics[0].get('passed') or metrics[0].get('skipped') or
            not metrics[0].get('endurance', {}).get('load_validated') or
            not metrics[0].get('delivery', {}).get('reconciliation_complete') or
            metrics[0].get('delivery', {}).get('integrity', {}).get('matched') is not True):
        raise ValueError('passing endurance evidence is missing or incomplete')
    body = summarize(evidence)
    rehearsal = receipt['requested_seconds'] < 86400
    title = 'Endurance rehearsal' if rehearsal else 'Application endurance'
    rows = [f'# {title} results', '',
            f"**{receipt['status'].upper()}** · started {receipt['started_at']} · "
            f"requested publishing {receipt['requested_seconds']} seconds plus {receipt['drain_seconds']} seconds draining.", '',
            f"Measured run time: {receipt['elapsed_seconds']:.1f} seconds, including fixture setup and cleanup.", '',
            f"Fixture source commit: `{receipt['source_commit']}` · tracked working tree dirty: `{receipt['working_tree_dirty']}`.", '',
            f"Tested executable: `{receipt.get('binary_version', 'unavailable')}`", '',
            f"Binary SHA-256: `{receipt.get('binary_sha256', 'unavailable')}`", '',
            f"Host: `{receipt['platform']}` · Go: `{receipt['go']}`.", '',
            '[Full manifest and measurements](endurance-results.json).', '',
            'This single-process workload uses 24 source and 32 destination partitions, key routing, '
            '200 records/second plus bursts, high entropy payloads and separate single-broker clusters with replication factor 1. '
            'It measures application endurance under this workload; it does not establish saturation capacity or replicated resilience.', '']
    if rehearsal:
        rows += ['This is a rehearsal; it does not establish 24 hours of continuous uptime.', '']
    if receipt['status'] != 'passed':
        rows += ['The run did not pass. Missing observations and unresolved reconciliation do not establish successful delivery.', '']
    if metrics:
        item = metrics[0]
        endurance = item.get('endurance', {})
        rows += ['| Additional measurement | Value |', '| --- | ---: |',
                 f"| CLI CPU seconds | {item.get('child_cpu_seconds', 'unavailable') if item.get('passed') else 'unavailable'} |",
                 f"| Destination stalls / recovered | {item.get('destination_stalls', 'unavailable')} / {endurance.get('recoveries', 'unavailable')} |",
                 f"| Maximum stall recovery seconds | {endurance.get('max_recovery_seconds', 'unavailable') if endurance.get('recoveries', 0) > 0 else 'unavailable'} |", '',
                 'Delivery times include producer queueing, copying and verifier delay. Memory peaks are sampled.', '']
    rows += [body]
    output.parent.mkdir(parents=True, exist_ok=True)
    # Publish the complete local package together; retries never replace a prior package.
    with tempfile.TemporaryDirectory(dir=output.parent) as temporary:
        stage = Path(temporary) / 'publication'
        stage.mkdir()
        (stage / 'endurance-results.md').write_text('\n'.join(rows))
        (stage / 'endurance-results.json').write_text(json.dumps({'schema': 1, 'manifest': receipt, 'measurements': metrics}, indent=2) + '\n')
        with tarfile.open(stage / 'endurance-evidence.tar.gz', 'w:gz') as archive:
            for path in sorted(evidence.iterdir()):
                if path.is_file() and not path.is_symlink() and (path.name in ('manifest.json', 'capacity.json', 'summary.md', 'soak.samples.jsonl') or
                        path.name.endswith(('.metrics.json', '.log'))):
                    archive.add(path, arcname=path.name)
        files = sorted(stage.iterdir())
        (stage / 'endurance.SHA256SUMS').write_text(''.join(f'{sha256(path)}  {path.name}\n' for path in files))
        stage.rename(output)
    print(f'Endurance publication package: {output}')
    return output


def publish(package, release, repository):
    if not re.fullmatch(r'v\d+\.\d+\.\d+', release):
        raise ValueError('select an existing stable version tag, such as v0.12.0')
    if not re.fullmatch(r'[\w.-]+/[\w.-]+', repository):
        raise ValueError('repository must be OWNER/REPO')
    package = Path(package)
    names = ('endurance-results.md', 'endurance-results.json', 'endurance-evidence.tar.gz')
    expected = ''.join(f'{sha256(package / name)}  {name}\n' for name in sorted(names))
    if (package / 'endurance.SHA256SUMS').read_text() != expected:
        raise ValueError('publication checksums do not match')
    report = json.loads((package / 'endurance-results.json').read_text())
    receipt = report['manifest']
    if receipt['working_tree_dirty'] or not report['measurements']:
        raise ValueError('publishing requires a clean fixture checkout and measured binary identity')
    platform = report['measurements'][0]['platform'].replace('/', '-')
    if platform not in ('linux-amd64', 'linux-arm64', 'darwin-amd64', 'darwin-arm64'):
        raise ValueError('unsupported release platform')
    with tempfile.TemporaryDirectory() as temporary:
        archive_name = f'kaferry-{release}-{platform}.tar.gz'
        subprocess.run(['gh', 'release', 'download', release, '--repo', repository, '--pattern', archive_name, '--dir', temporary], check=True)
        with tarfile.open(Path(temporary) / archive_name) as archive:
            members = [member for member in archive.getmembers() if member.isfile() and Path(member.name).name == 'kaferry']
            if len(members) != 1:
                raise ValueError('release archive must contain exactly one kaferry executable')
            with archive.extractfile(members[0]) as binary:
                digest = stream_sha256(binary)
        if digest != receipt['binary_sha256']:
            raise ValueError('tested executable does not match the selected release; no files uploaded')
    # A dated prefix preserves prior runs. gh refuses existing asset names without --clobber.
    stamp = re.sub(r'[^0-9]', '', receipt['started_at'])
    prefix = f'endurance-{platform}-{stamp}'
    with tempfile.TemporaryDirectory() as temporary:
        uploads = []
        for name in names:
            path = Path(temporary) / f'{prefix}-{name}'
            shutil.copyfile(package / name, path)
            if name == 'endurance-results.md':
                path.write_text(path.read_text().replace('(endurance-results.json)',
                    f'(https://github.com/{repository}/releases/download/{release}/{prefix}-endurance-results.json)'))
            uploads.append(str(path))
        checksums = Path(temporary) / f'{prefix}-endurance.SHA256SUMS'
        checksums.write_text(''.join(f'{sha256(Path(path))}  {Path(path).name}\n' for path in sorted(uploads)))
        uploads.append(str(checksums))
        subprocess.run(['gh', 'release', 'upload', release, '--repo', repository, *uploads], check=True)
    print(f'Published evidence: https://github.com/{repository}/releases/tag/{release}')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('evidence', type=Path)
    parser.add_argument('--output', type=Path, help='new package directory; defaults to EVIDENCE/publication')
    parser.add_argument('--publish', metavar='TAG', help='upload an existing package after matching the release binary')
    parser.add_argument('--repo', default='pagrus7/kaferry')
    args = parser.parse_args(argv)
    output = args.output or args.evidence / 'publication'
    if args.publish:
        publish(output, args.publish, args.repo)
    else:
        prepare(args.evidence, output)


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        sys.exit(str(error))
