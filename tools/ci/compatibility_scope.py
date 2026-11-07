"""Run the PR compatibility matrix for changes to its inputs."""

import json
import os
from pathlib import Path
import re
import subprocess


PROSE = {'README.md', 'CONTRIBUTING.md', 'AGENTS.md', 'CLAUDE.md'}
# These system tests have tags that exclude them from both PR matrix Go suites.
OTHER_SYSTEM_SUITES = {
    'src/tests/system/churn_test.go',
    'src/tests/system/container_test.go',
    'src/tests/system/crash_test.go',
    'src/tests/system/fault_test.go',
    'src/tests/system/migration_test.go',
    'src/tests/system/recovery_test.go',
}


def unrelated(path):
    if path in PROSE or path in OTHER_SYSTEM_SUITES:
        return True
    if path.startswith('docs/') and path.endswith('.md'):
        return True
    if path.startswith('.claude/agents/') and path.endswith('.md'):
        return True
    if path.startswith('.claude/skills/') and path.endswith('/SKILL.md'):
        return True
    if path.startswith('.codex/agents/') and path.endswith('.toml'):
        return True
    if path.startswith(('src/cmd/', 'src/internal/')) and path.endswith('_test.go'):
        return True
    if path.startswith('tools/') and Path(path).name.startswith('test_') and path.endswith('.py'):
        return True
    return False


def changed_paths(event):
    pr = event['pull_request']
    base, head = pr['base']['sha'], pr['head']['sha']
    if not all(re.fullmatch('[a-f0-9]{40}', sha) for sha in (base, head)):
        raise ValueError('expected full base and head commit SHAs')
    output = subprocess.check_output(['git', 'diff', '--no-renames', '--name-only', '-z',
                                      f'{base}...{head}', '--'])
    return [os.fsdecode(path) for path in output.split(b'\0') if path]


def decide(event):
    try:
        paths = changed_paths(event)
    except (KeyError, OSError, TypeError, ValueError, subprocess.CalledProcessError):
        return True, [], 'diff unavailable; run matrix'
    if not paths:
        return True, [], 'empty diff; run matrix'
    relevant = [path for path in paths if not unrelated(path)]
    return bool(relevant), paths, ('relevant paths: ' + ', '.join(relevant) if relevant
                                  else 'only unrelated prose and tests changed')


def main():
    event = json.loads(Path(os.environ['GITHUB_EVENT_PATH']).read_text())
    run, paths, reason = decide(event)
    with Path(os.environ['GITHUB_OUTPUT']).open('a') as output:
        output.write(f'run={str(run).lower()}\n')
    with Path(os.environ['GITHUB_STEP_SUMMARY']).open('a') as summary:
        summary.write(f'## PR compatibility scope\n\n{reason}\n\n')
        summary.write('Changed paths:\n\n' + '\n'.join(f'- `{path}`' for path in paths) + '\n')
    print(reason)


if __name__ == '__main__':
    main()
