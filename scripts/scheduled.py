#!/usr/bin/env python3
"""Reuse recent scheduled test executions without renewing their evidence age."""

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import time


REFRESH = timedelta(days=30)
NIGHTLY = ['fault', 'qualification', 'fuzz', 'benchmark']
WEEKLY = ['load', 'soak', *NIGHTLY]


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError('execution timestamp is missing')
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.tzinfo is None:
        raise ValueError('execution timestamp has no timezone')
    return stamp


def completed_step(job, name):
    if job.get('conclusion') != 'success':
        return None
    step = next((s for s in job.get('steps', []) if s['name'] == name), {})
    if step.get('conclusion') != 'success':
        return None
    return timestamp(step['completed_at']) if step.get('completed_at') else None


def execution(kind, key, jobs):
    """Return None for no attempt, otherwise (passed, evidence time, attempt time)."""
    if kind == 'qualification':
        job = next((j for j in jobs if j['name'] == f'qualification / suite ({key})'), None)
        if job is None or job.get('conclusion') == 'skipped':
            return None
        stamp = completed_step(job, 'Run suite')
        return stamp is not None, stamp, timestamp(job.get('started_at') or job.get('completed_at'))
    report = next((j for j in jobs if j['name'] == f'report ({key})'), None)
    cases = [j for j in jobs if (j['name'] == 'cases' or j['name'].startswith('cases ('))
             and j.get('conclusion') != 'skipped']
    if report is None and (not cases or any(j['name'].startswith('report (') for j in jobs)):
        return None
    if report is not None and report.get('conclusion') == 'skipped' and not cases:
        return None
    stamps = [completed_step(j, 'Run standalone suite') for j in cases]
    passed = report is not None and completed_step(report, 'Require complete matching evidence') is not None
    passed = passed and bool(stamps) and all(stamps)
    # Rerunning only the aggregate report cannot make the case executions newer.
    attempted = list(cases)
    if not passed and report is not None:
        attempted.append(report)
    latest = max(timestamp(j.get('started_at') or j.get('completed_at')) for j in attempted)
    return bool(passed), min(stamps) if passed else None, latest


def latest_jobs(jobs):
    inventory = {}
    for job in sorted(jobs, key=lambda item: (item.get('run_attempt', 1), item.get('id', 0))):
        # Skipped rerun jobs do not replace the preceding real execution.
        if job.get('conclusion') != 'skipped' or job['name'] not in inventory:
            inventory[job['name']] = job
    return list(inventory.values())


def select(kind, keys, head, runs, load_jobs, now, weekly=False):
    decisions = {key: {'suite': key, 'run': True, 'reason': 'no matching execution within 30 days'} for key in keys}
    if kind == 'qualification' and weekly:
        decisions['fuzz']['reason'] = 'weekly fresh-input fuzz budget'
    eligible = set(keys) - ({'fuzz'} if kind == 'qualification' and weekly else set())
    latest = {}
    for run in runs:
        if run['head_sha'] != head or run['status'] != 'completed':
            continue
        # Old run IDs can be rerun later; list order alone does not identify the last attempt.
        if timestamp(run.get('updated_at')) < now - REFRESH:
            continue
        jobs = latest_jobs(load_jobs(run['id']))
        for key in eligible:
            result = execution(kind, key, jobs)
            if result is not None and (key not in latest or result[2] > latest[key][0][2]
                    or result[2] == latest[key][0][2] and not result[0]):
                latest[key] = result, run
    for key, ((passed, stamp, _), run) in latest.items():
        decision = decisions[key]
        decision['url'] = run['html_url']
        if not passed:
            decision['reason'] = 'latest attempt did not pass; retry required'
        elif not timedelta(0) <= now - stamp < REFRESH:
            decision['reason'] = 'execution is expired or its timestamp is in the future'
        else:
            decision.update(run=False, reason='unchanged commit; reuse actual execution', executed_at=stamp.isoformat())
    return list(decisions.values())


def gh(path, deadline, paginate=False):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError('history lookup budget exhausted')
    command = ['gh', 'api'] + (['--paginate', '--slurp'] if paginate else []) + [path]
    return json.loads(subprocess.check_output(command, text=True,
        stderr=subprocess.PIPE, timeout=min(15, remaining)))


def collect(pages, key):
    values = [value for page in pages for value in page[key]]
    if any(page.get('total_count', len(values)) > len(values) for page in pages):
        raise ValueError('history is truncated')
    return values


def plan(kind, tier, env):
    weekly = env.get('WEEKLY') == 'true'
    if kind == 'qualification' and env.get('EVENT') == 'workflow_dispatch':
        weekly = True
    keys = [tier] if kind == 'compatibility' else WEEKLY if weekly else NIGHTLY
    if env.get('EVENT') != 'schedule' or env.get('FORCE_RUN') == 'true':
        return [{'suite': key, 'run': True, 'reason': 'explicit, PR or release execution'} for key in keys]
    repository, head = env['GITHUB_REPOSITORY'], env['GITHUB_SHA']
    if not re.fullmatch(r'[\w.-]+/[\w.-]+', repository) or not re.fullmatch('[a-f0-9]{40}', head):
        raise ValueError('expected a repository and full source commit')
    deadline = time.monotonic() + 45
    try:
        pages = gh(f'repos/{repository}/actions/workflows/{kind}.yml/runs?branch=main&event=schedule&status=completed&per_page=100&head_sha={head}', deadline, paginate=True)
        runs = collect(pages, 'workflow_runs')
        def load_jobs(run):
            pages = gh(f'repos/{repository}/actions/runs/{int(run)}/jobs?per_page=100&filter=all', deadline, paginate=True)
            return collect(pages, 'jobs')
        return select(kind, keys, head, runs, load_jobs, datetime.now(timezone.utc), weekly)
    except (KeyError, OSError, TypeError, ValueError, subprocess.SubprocessError) as error:
        # Missing history cannot justify skipping a test. Do not expose CLI stderr.
        return [{'suite': key, 'run': True, 'reason': f'history unavailable ({type(error).__name__}); run tests'} for key in keys]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('kind', choices=['qualification', 'compatibility'])
    parser.add_argument('--tier', choices=['pr', 'nightly', 'full'], default='nightly')
    args = parser.parse_args(argv)
    decisions = plan(args.kind, args.tier, os.environ)
    output = {'run': str(any(d['run'] for d in decisions)).lower()}
    if args.kind == 'qualification':
        output['suites'] = json.dumps([d['suite'] for d in decisions if d['run']])
    if destination := os.environ.get('GITHUB_OUTPUT'):
        with Path(destination).open('a') as stream:
            stream.write(''.join(f'{key}={value}\n' for key, value in output.items()))
    rows = ['## Scheduled test decisions', '', f"Source: `{os.environ.get('GITHUB_SHA', 'local')}`", '',
            '| Suite / tier | Decision | Reason | Actual execution | Evidence |',
            '| --- | --- | --- | --- | --- |']
    for item in decisions:
        link = f"[run]({item['url']})" if item.get('url') else '—'
        rows.append(f"| {item['suite']} | {'Run' if item['run'] else 'Reuse; tests skipped'} | {item['reason']} | {item.get('executed_at', '—')} | {link} |")
    summary = '\n'.join(rows) + '\n'
    if destination := os.environ.get('GITHUB_STEP_SUMMARY'):
        with Path(destination).open('a') as stream:
            stream.write(summary)
    print(summary)


if __name__ == '__main__':
    main()
