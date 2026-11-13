"""Shared qualification runner arguments for tool tests."""

import argparse


def suite_args(root, suite='recovery', **changes):
    binary = root / 'candidate'
    binary.write_bytes(b'candidate')
    values = dict(suite=suite, binary=str(binary), output=root / 'evidence', source='3.9.2',
                  destination='3.9.2', config=None, rate=200 if suite == 'endurance' else 50,
                  duration=60, state_contract='resume', previous_binary=binary)
    values.update(changes)
    return argparse.Namespace(**values)
