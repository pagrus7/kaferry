"""Add integrity suites for changes to copy behavior, clients or test infrastructure."""

import json
import os
from pathlib import Path
import re
import subprocess


def suites(paths):
    prose = ('docs/', 'releases/', 'review/')
    extra = any(not (p == 'README.md' or p.startswith(prose) and p.endswith('.md')
                     or p.startswith('design/') and p.endswith('.md') and p != 'design/grammar.md')
                for p in paths)
    return ['fault', 'qualification'] if extra else []


def select(event):
    pr = event['pull_request']
    base, head = pr['base']['sha'], pr['head']['sha']
    if not all(re.fullmatch('[a-f0-9]{40}', sha) for sha in (base, head)):
        raise ValueError('Expected full base and head commit SHAs')
    paths = subprocess.check_output(['git', 'diff', '--no-renames', '--name-only',
                                     f'{base}...{head}'], text=True).splitlines()
    return {'base': base, 'head': head, 'paths': paths, 'suites': suites(paths)}


if __name__ == '__main__':
    result = select(json.loads(Path(os.environ['GITHUB_EVENT_PATH']).read_text()))
    with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
        output.write(f'suites={json.dumps(result["suites"])}\n')
        output.write(f'run={str(bool(result["suites"])).lower()}\n')
    with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as summary:
        summary.write('## Additional integrity checks\n\n')
        summary.write(f'Suites: {", ".join(result["suites"]) or "none (prose only)"}.\n\n')
        summary.write('Changed paths:\n\n' + '\n'.join(f'- `{p}`' for p in result['paths']) + '\n')
    print(json.dumps(result, indent=2))
