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
                  activations=[], session_expiries=[], completions=[], missing=[])
    for path in sorted(directory.rglob('fault-events.jsonl')):
        for line in path.read_text().splitlines():
            report['completions'].append(dict(json.loads(line), source=str(path.relative_to(directory))))
    report['fault_windows'] = [dict(json.loads(path.read_text()), source=str(path.relative_to(directory)))
                              for path in sorted(directory.rglob('replicated-fault-window.json'))]
    groups = {window['event']['role']['group'] for window in report['fault_windows']}
    logs = directory/'brokers.log'
    for line in logs.read_text(errors='replace').splitlines() if logs.exists() else []:
        if '"fault_active_at"' in line:
            event = json.loads(line[line.index('{'):])
            report['activations'].append(event)
        match = re.search(r'\[([^]]+)\] INFO .*Member (\S+) in group (\S+) has failed, removing it from the group\.', line)
        if match and (not groups or match[3] in groups):
            at = datetime.strptime(match[1], '%Y-%m-%d %H:%M:%S,%f').replace(tzinfo=timezone.utc)
            report['session_expiries'].append(dict(at=at.isoformat(), member=match[2], group=match[3], source='brokers.log'))
    for field in ('activations', 'session_expiries', 'completions'):
        if not report[field]:
            report['missing'].append(field)
    path = directory/'replicated-case-evidence.json'
    path.write_text(json.dumps(report, indent=2)+'\n')
    return report
