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
    rerun = [g for g in complete if any(r.get('run_attempt', 1) > 1 for r in g.values())]
    cancelled = [g for g in complete if any(r['conclusion'] == 'cancelled' for r in g.values())]
    clean = [g for g in complete if g not in rerun and g not in cancelled]
    durations = sorted(seconds(min(r['created_at'] for r in g.values()),
                               max(r['updated_at'] for r in g.values())) for g in clean)
    return {'samples': len(durations), 'completed_heads': sorted({r['head_sha'] for g in complete for r in g.values()}),
            'failed_heads': sum(any(r['conclusion'] != 'success' for r in g.values()) for g in complete if g not in cancelled),
            'cancelled_heads': len(cancelled),
            'rerun_heads': len(rerun),
            'p50_seconds': durations[math.ceil(len(durations) * .5) - 1] if len(durations) >= 20 else None,
            'p95_seconds': durations[math.ceil(len(durations) * .95) - 1] if len(durations) >= 20 else None}


if __name__ == '__main__':
    repo = json.loads(subprocess.check_output(['gh', 'repo', 'view', '--json', 'nameWithOwner'], text=True))['nameWithOwner']
    runs = []
    for page in range(1, 4):
        batch = json.loads(subprocess.check_output(['gh', 'api', f'repos/{repo}/actions/runs?event=pull_request&per_page=100&page={page}'], text=True))['workflow_runs']
        runs.extend(batch)
        if len(batch) < 100:
            break
    # API returns newest first; preserve the first retained invocation per head,
    # so later description edits do not add review idle time to the CI duration.
    unique = {}
    for run in runs:
        unique[(run['head_sha'], run['name'])] = run
    result = summary(list(unique.values()))
    minutes = 0
    for run in unique.values():
        if run['head_sha'] in result['completed_heads'] and run['name'] in WORKFLOWS:
            jobs = json.loads(subprocess.check_output(['gh', 'api', f'repos/{repo}/actions/runs/{run["id"]}/jobs?filter=all&per_page=100'], text=True))['jobs']
            minutes += sum(seconds(j['started_at'], j['completed_at']) / 60 for j in jobs if j.get('started_at') and j.get('completed_at'))
    result.update(runner_minutes=round(minutes, 1), scope='first retained checks/quality/integrity invocation per head; queue included; excludes PR-description and local review timing')
    print(json.dumps(result, indent=2))
