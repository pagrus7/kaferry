"""Run the PR compatibility matrix for changes to its inputs."""

import json
import os
from pathlib import Path
import subprocess

from tools.ci.pr_diff import changed_paths


PROSE = {'README.md', 'CONTRIBUTING.md', 'AGENTS.md', 'CLAUDE.md'}


def unrelated(path):
    if path in PROSE:
        return True
    suffixes = (('docs/', '.md'), ('.claude/agents/', '.md'),
                ('.claude/skills/', '/SKILL.md'), ('.codex/agents/', '.toml'),
                ('src/cmd/', '_test.go'), ('src/internal/', '_test.go'),
                ('src/tests/system/', '_test.go'))
    # Compatibility imports regular system Go files, but never its test files.
    if any(path.startswith(prefix) and path.endswith(suffix) for prefix, suffix in suffixes):
        return True
    if path.startswith('tools/') and Path(path).name.startswith('test_') and path.endswith('.py'):
        return True
    return False


def decide(event):
    try:
        paths = changed_paths(event)
    except (KeyError, OSError, TypeError, ValueError, subprocess.CalledProcessError):
        return True, [], 'diff unavailable; run matrix'
    if not paths:
        return True, [], 'empty diff; run matrix'
    relevant = [path for path in paths if not unrelated(path)]
    return bool(relevant), paths, ('relevant paths: ' + ', '.join(relevant) if relevant
                                  else 'only unrelated documentation, reviewer configuration or tests changed')


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
