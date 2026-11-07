"""Read every path in a pull request diff, including deleted and renamed paths."""

import os
import re
import subprocess


def changed_paths(event):
    pr = event['pull_request']
    base, head = pr['base']['sha'], pr['head']['sha']
    if not all(re.fullmatch('[a-f0-9]{40}', sha) for sha in (base, head)):
        raise ValueError('expected full base and head commit SHAs')
    output = subprocess.check_output(['git', 'diff', '--no-renames', '--name-only', '-z',
                                      f'{base}...{head}', '--'])
    return [os.fsdecode(path) for path in output.split(b'\0') if path]
