"""Retain diagnostic timing observations without creating qualification proof."""

from datetime import datetime, timezone
import json
from pathlib import Path
import platform
import re


def retain(directory):
    directory = Path(directory)
    clock = 'shared Linux CLOCK_REALTIME, UTC' if platform.system() == 'Linux' else 'UTC; Docker VM/controller clock alignment unverified'
    report = dict(schema=1, diagnostic_only=True, clock=clock,
                  activations=[], session_expiries=[], completions=[], missing=[], errors=[])

    def error(path, number, message):
        report['errors'].append(dict(source=str(path.relative_to(directory)), line=number, error=str(message)))

    def lines(path):
        try:
            return path.read_text(errors='replace').splitlines()
        except OSError as failure:
            error(path, None, failure)
            return []

    def decode(path, number, text):
        try:
            value = json.loads(text)
            if not isinstance(value, dict):
                raise ValueError('diagnostic record is not an object')
            return value
        except ValueError as failure:
            error(path, number, failure)
            return None

    for path in sorted(directory.rglob('fault-events.jsonl')):
        for number, line in enumerate(lines(path), 1):
            event = decode(path, number, line)
            if event is not None:
                report['completions'].append(dict(event, source=str(path.relative_to(directory))))
    report['fault_windows'], groups = [], set()
    for path in sorted(directory.rglob('replicated-fault-window.json')):
        window = decode(path, None, '\n'.join(lines(path)))
        if window is not None:
            report['fault_windows'].append(dict(window, source=str(path.relative_to(directory))))
            try:
                groups.add(window['event']['role']['group'])
            except (KeyError, TypeError) as failure:
                error(path, None, failure)
    logs = directory/'brokers.log'
    for number, line in enumerate(lines(logs) if logs.exists() else [], 1):
        if '"fault_active_at"' in line:
            event = decode(logs, number, line[line.find('{'):])
            if event is not None:
                report['activations'].append(event)
        match = re.search(r'\[([^]]+)\] INFO .*Member (\S+) in group (\S+) has failed, removing it from the group\.', line)
        if match and (not groups or match[3] in groups):
            try:
                at = datetime.strptime(match[1], '%Y-%m-%d %H:%M:%S,%f').replace(tzinfo=timezone.utc)
                report['session_expiries'].append(dict(at=at.isoformat(), member=match[2], group=match[3], source='brokers.log'))
            except ValueError as failure:
                error(logs, number, failure)
    for field in ('activations', 'session_expiries', 'completions'):
        if not report[field]:
            report['missing'].append(field)
    path = directory/'replicated-case-evidence.json'
    path.write_text(json.dumps(report, indent=2)+'\n')
    return report
