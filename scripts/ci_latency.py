"""Report PR CI wall time and runner minutes from recent Actions runs."""

from datetime import datetime
import json
import math
import subprocess


WORKFLOWS = {'checks', 'quality', 'integrity'}


def seconds(start, end):
    return (datetime.fromisoformat(end.replace('Z', '+00:00')) -
            datetime.fromisoformat(start.replace('Z', '+00:00'))).total_seconds()


def summary(runs):
    groups = {}
    for run in runs:
        if run['name'] in WORKFLOWS:
            groups.setdefault(run['head_sha'], {})[run['name']] = run
    complete = [g for g in groups.values() if set(g) == WORKFLOWS and
                all(r['status'] == 'completed' for r in g.values())]
    durations = sorted(seconds(min(r['created_at'] for r in g.values()),
                               max(r['updated_at'] for r in g.values())) for g in complete)
    return {'samples': len(durations), 'completed_heads': sorted({r['head_sha'] for g in complete for r in g.values()}),
            'failed_heads': sum(any(r['conclusion'] != 'success' for r in g.values()) for g in complete),
            'p50_seconds': durations[math.ceil(len(durations) * .5) - 1] if len(durations) >= 20 else None,
            'p95_seconds': durations[math.ceil(len(durations) * .95) - 1] if len(durations) >= 20 else None}


if __name__ == '__main__':
    repo = json.loads(subprocess.check_output(['gh', 'repo', 'view', '--json', 'nameWithOwner'], text=True))['nameWithOwner']
    runs = json.loads(subprocess.check_output(['gh', 'api', f'repos/{repo}/actions/runs?event=pull_request&per_page=100'], text=True))['workflow_runs']
    # API returns newest first; keep only the latest run of each workflow/head.
    unique = {}
    for run in runs:
        unique.setdefault((run['head_sha'], run['name']), run)
    result = summary(list(unique.values()))
    minutes = 0
    for run in unique.values():
        if run['head_sha'] in result['completed_heads'] and run['name'] in WORKFLOWS:
            jobs = json.loads(subprocess.check_output(['gh', 'api', f'repos/{repo}/actions/runs/{run["id"]}/jobs?per_page=100'], text=True))['jobs']
            minutes += sum(seconds(j['started_at'], j['completed_at']) / 60 for j in jobs if j.get('started_at') and j.get('completed_at'))
    result.update(runner_minutes=round(minutes, 1), scope='checks, quality and integrity; queue included; excludes local reviews and PR-description')
    print(json.dumps(result, indent=2))
