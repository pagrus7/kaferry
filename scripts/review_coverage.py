"""Collect exact-revision CI evidence and changed coverage gaps; never decide exceptions."""

import argparse
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
from urllib.request import urlopen
import zipfile


def git(*args):
    return subprocess.check_output(['git', *args], text=True).strip()


def github(endpoint):
    return json.loads(subprocess.check_output(['gh', 'api', endpoint], text=True))


def verified_run(run, head):
    if run['head_sha'] != head or run['status'] != 'completed' or run['conclusion'] != 'success':
        raise ValueError('CI run must be successful and match the reviewed SHA')


def sum_blocks(profiles):
    blocks = {}
    for profile in profiles:
        lines = profile.splitlines()
        if not lines or lines[0] not in {'mode: atomic', 'mode: count', 'mode: set'}:
            raise ValueError('invalid coverage mode')
        for line in lines[1:]:
            match = re.fullmatch(r'(.+):(\d+)\.(\d+),(\d+)\.(\d+) (\d+) (\d+)', line)
            if not match:
                raise ValueError('invalid coverage block')
            name, *numbers = match.groups()
            start, column, end, end_column, statements, hits = map(int, numbers)
            key = (name, start, column, end, end_column)
            before, count = blocks.get(key, (statements, 0))
            if before != statements:
                raise ValueError('inconsistent duplicate coverage block')
            blocks[key] = (statements, count + hits)
    return blocks


def changed_lines(base, head, paths):
    result = {}
    for path in paths:
        patch = git('diff', '--no-ext-diff', '--no-textconv', '--unified=0', '--no-renames', base, head, '--', path)
        lines = set()
        for start, count in re.findall(r'^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@', patch, re.M):
            lines.update(range(int(start), int(start) + int(count or 1)))
        result[path] = lines
    return result


def gap_ledger(blocks, changed, codecov, module):
    go = {}
    for (name, start, _, end, end_column), (statements, hits) in blocks.items():
        if not statements:
            continue
        path = name.removeprefix(module + '/')
        for line in changed.get(path, set()):
            if start <= line < end + (end_column > 1):
                go.setdefault((path, line), set()).add(hits > 0)
    cc = {(f['name'], line): ('hit', 'miss', 'partial')[status]
          for f in codecov['report']['files'] for line, status in f['line_coverage']}
    ledger = []
    for path, lines in changed.items():
        for line in sorted(lines):
            states = go.get((path, line), set())
            status = 'partial' if len(states) == 2 else 'hit' if True in states else 'miss' if states else None
            remote = cc.get((path, line))
            if status in {'miss', 'partial'} or remote in {'miss', 'partial'} or status != remote:
                ledger.append({'file': path, 'line': line, 'go': status, 'codecov': remote,
                               'agreement': status == remote, 'decision': None})
    return ledger


def collect(base, head, repository, destination):
    if not all(re.fullmatch('[a-f0-9]{40}', value) for value in (base, head)):
        raise ValueError('base and head must be full commit SHAs')
    if not re.fullmatch(r'[\w.-]+/[\w.-]+', repository):
        raise ValueError('repository must be owner/name')
    if git('merge-base', base, head) != base:
        raise ValueError('base must be the merge base')
    paths = git('diff', '--no-ext-diff', '--name-only', '--no-renames', base, head).splitlines()
    production = [p for p in paths if p.startswith(('cmd/', 'internal/')) and p.endswith('.go') and not p.endswith('_test.go')]
    if (destination / 'manifest.json').exists():
        raise ValueError('completed evidence already exists; choose a new evidence directory')
    destination.mkdir(parents=True, exist_ok=True)
    record = {'base': base, 'head': head, 'changed_files': paths, 'patch_coverage': 'N/A', 'gaps': []}
    if not production:
        (destination / 'manifest.json').write_text(json.dumps(record, indent=2) + '\n')
        return record
    runs = github(f'repos/{repository}/actions/workflows/ci.yml/runs?head_sha={head}&status=success&per_page=100')['workflow_runs']
    if not runs:
        raise ValueError('successful checks run is unavailable')
    run = runs[0]
    verified_run(run, head)
    (destination / 'run.json').write_text(json.dumps(run, indent=2))
    artifacts = github(f'repos/{repository}/actions/runs/{run["id"]}/artifacts')['artifacts']
    (destination / 'artifacts.json').write_text(json.dumps(artifacts, indent=2))
    profiles = []
    tree = github(f'repos/{repository}/git/commits/{head}')['tree']['sha']
    for name, label in [('checks-fast', 'unit'), ('checks-kafka', 'integration')]:
        artifact = next((a for a in artifacts if a['name'] == name and not a['expired']), None)
        if artifact is None:
            raise ValueError(f'coverage artifact unavailable: {name}')
        archive = subprocess.check_output(['gh', 'api', f'repos/{repository}/actions/artifacts/{artifact["id"]}/zip'])
        if artifact.get('digest') != 'sha256:' + hashlib.sha256(archive).hexdigest():
            raise ValueError('artifact digest mismatch')
        target = destination / name
        target.mkdir(exist_ok=True)
        with zipfile.ZipFile(io.BytesIO(archive)) as zipped:
            for filename in ['coverage.out', f'{label}.environment.json']:
                (target / filename).write_bytes(zipped.read(filename))
        environment = json.loads((target / f'{label}.environment.json').read_text())
        if not re.fullmatch('[a-f0-9]{40}', environment['commit']):
            raise ValueError('tested commit must be a full SHA')
        tested = github(f'repos/{repository}/git/commits/{environment["commit"]}')
        if environment['working_tree_dirty'] or tested['tree']['sha'] != tree:
            raise ValueError('tested source tree differs from reviewed commit')
        profiles.append((target / 'coverage.out').read_text())
    with urlopen(f'https://api.codecov.io/api/v2/github/{repository.split("/")[0]}/repos/{repository.split("/")[1]}/commits/{head}/', timeout=30) as response:
        codecov = json.load(response)
    if codecov['commitid'] != head or codecov['state'] != 'complete' or codecov['totals']['sessions'] < 2:
        raise ValueError('final combined Codecov report is unavailable')
    (destination / 'codecov.json').write_text(json.dumps(codecov, indent=2))
    owner, name = repository.split('/')
    with urlopen(f'https://api.codecov.io/api/v2/github/{owner}/repos/{name}/compare/?base={base}&head={head}', timeout=30) as response:
        comparison = json.load(response)
    if comparison['base_commit'] != base or comparison['head_commit'] != head:
        raise ValueError('Codecov comparison does not match the reviewed base and head')
    (destination / 'codecov-compare.json').write_text(json.dumps(comparison, indent=2))
    blocks = sum_blocks(profiles)
    summed = 'mode: atomic\n' + ''.join(f'{name}:{start}.{column},{end}.{end_column} {statements} {hits}\n'
                                      for (name, start, column, end, end_column), (statements, hits) in sorted(blocks.items()))
    (destination / 'summed.out').write_text(summed)
    module = git('show', f'{head}:go.mod').splitlines()[0].split()[1]
    record.update(patch_coverage='review required', run_id=run['id'],
                  gaps=gap_ledger(blocks, changed_lines(base, head, production), codecov, module))
    for status, total in [('miss', 'misses'), ('partial', 'partials')]:
        if sum(g['codecov'] == status for g in record['gaps']) != comparison['totals']['patch'][total]:
            raise ValueError('changed gap ledger does not match the complete Codecov patch report')
    measured = {key[0].removeprefix(module + '/') for key in blocks}
    record['unmeasured_files'] = sorted(set(production) - measured)
    (destination / 'manifest.json').write_text(json.dumps(record, indent=2) + '\n')
    return record


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('base', help='full merge-base SHA')
    parser.add_argument('head', help='full reviewed SHA')
    args = parser.parse_args()
    repo = json.loads(subprocess.check_output(['gh', 'repo', 'view', '--json', 'nameWithOwner'], text=True))['nameWithOwner']
    output = Path('.cache/reviews') / args.head / args.base / 'collected'
    print(json.dumps(collect(args.base, args.head, repo, output), indent=2))
