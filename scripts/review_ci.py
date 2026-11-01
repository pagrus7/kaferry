"""Wait for exact-head CI observations; never decide review or coverage readiness."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import signal
import subprocess
import tempfile
import time


WORKFLOWS = {f'.github/workflows/{name}.yml' for name in
             ('ci', 'quality', 'risk', 'pr-description')}


class Incomplete(ValueError):
    pass


def complete_list(data, key):
    rows = data[key]
    if not isinstance(rows, list) or data['total_count'] != len(rows):
        raise Incomplete('list response is incomplete; inspect paginated evidence manually')
    return rows


def ci_state(observation, head):
    runs = complete_list(observation['runs'], 'workflow_runs')
    checks = complete_list(observation['checks'], 'check_runs')
    statuses = complete_list(observation['statuses'], 'statuses')
    if observation['statuses']['sha'] != head or any(r['head_sha'] != head for r in runs + checks):
        raise Incomplete('CI response does not match the frozen head')
    if any(r['event'] != 'pull_request' for r in runs):
        raise Incomplete('workflow response contains a different event')
    # Description edits and reruns may leave several results for the same name.
    latest_runs, latest_checks = {}, {}
    for run in sorted(runs, key=lambda r: r['id']):
        latest_runs[run['path']] = run
    for check in sorted(checks, key=lambda c: c['id']):
        latest_checks[(check['app']['id'], check['name'])] = check
    pending = sorted(WORKFLOWS - latest_runs.keys())
    failed = []
    for path, run in latest_runs.items():
        if run['status'] != 'completed':
            pending.append(path)
        elif run['conclusion'] != 'success' and not (
                path not in WORKFLOWS and run['conclusion'] == 'skipped'):
            failed.append(path)
    if not checks:
        pending.append('check runs unavailable')
    for check in latest_checks.values():
        if check['status'] != 'completed':
            pending.append(check['name'])
        elif check['conclusion'] not in ('success', 'neutral', 'skipped'):
            failed.append(check['name'])
    for status in statuses:
        if status['state'] == 'pending':
            pending.append(status['context'])
        elif status['state'] != 'success':
            failed.append(status['context'])
    return ('failed' if failed else 'pending' if pending else 'ci-complete',
            sorted(set(failed or pending)))


def local_head():
    return subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True, timeout=5).strip()


def watch(repository, number, head, output, timeout=1800, max_requests=160):
    started = time.monotonic()
    deadline = started + timeout
    requests, polls, errors = 0, 0, 0
    base, state, details = None, 'incomplete', []
    delay = 15

    def api(endpoint):
        nonlocal requests
        remaining = deadline - time.monotonic()
        if remaining <= 0 or requests >= max_requests:
            raise TimeoutError('time or request budget exhausted')
        requests += 1
        response = subprocess.run(['gh', 'api', f'repos/{repository}/{endpoint}'],
                                  capture_output=True, text=True, check=True,
                                  timeout=min(20, remaining))
        return json.loads(response.stdout)

    def matching_pr(pr):
        nonlocal base
        current_base = (pr['base']['repo']['full_name'], pr['base']['ref'], pr['base']['sha'])
        if base is None:
            base = current_base
        return (pr['state'] == 'open' and pr['head']['sha'] == head and
                current_base == base and local_head() == head)

    try:
        while True:
            if local_head() != head:
                state, details = 'stale', ['local HEAD changed']
                break
            try:
                pr = api(f'pulls/{number}')
                if not matching_pr(pr):
                    state, details = 'stale', ['PR head, base or open state changed']
                    break
                polls += 1
                observation = {
                    'pr': pr,
                    'runs': api(f'actions/runs?head_sha={head}&event=pull_request&per_page=100'),
                    'checks': api(f'commits/{head}/check-runs?filter=latest&per_page=100'),
                    'statuses': api(f'commits/{head}/status?per_page=100'),
                }
                # Save raw evidence without sending it through the coordinator's context.
                (output / 'observation.json').write_text(json.dumps(observation, indent=2) + '\n')
                state, details = ci_state(observation, head)
                if state != 'pending':
                    final_pr = api(f'pulls/{number}')
                    (output / 'final-pr.json').write_text(json.dumps(final_pr, indent=2) + '\n')
                    if not matching_pr(final_pr):
                        state, details = 'stale', ['PR head, base or open state changed during collection']
                    break
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired, json.JSONDecodeError) as error:
                errors += 1
                # A failed poll never reuses a previous successful response.
                state, details = 'incomplete', [f'poll unavailable: {type(error).__name__}']
            remaining = deadline - time.monotonic()
            if remaining <= 0 or requests >= max_requests:
                state, details = 'incomplete', ['time or request budget exhausted']
                break
            time.sleep(min(delay, remaining))
            delay = min(delay * 2, 60)
    except KeyboardInterrupt:
        state, details = 'interrupted', ['watch stopped; CI is unverified']
    except (TimeoutError, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        state, details = 'incomplete', [str(error)[:200]]
    result = {'state': state, 'repository': repository, 'pr': number, 'head': head,
              'base': base, 'details': details, 'requests': requests, 'polls': polls,
              'transient_errors': errors, 'elapsed_seconds': round(time.monotonic() - started, 2),
              'observed_at': datetime.now(timezone.utc).isoformat()}
    (output / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


def positive(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError('must be a positive integer')
    return number


def interrupt(*_):
    raise KeyboardInterrupt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('repository', help='owner/name')
    parser.add_argument('pr', type=positive)
    parser.add_argument('head', help='full frozen HEAD SHA')
    parser.add_argument('--timeout', type=positive, default=1800, help='total seconds (default: 1800)')
    parser.add_argument('--max-requests', type=positive, default=160, help='API call cap (default: 160)')
    args = parser.parse_args()
    if not re.fullmatch(r'[\w.-]+/[\w.-]+', args.repository) or not re.fullmatch('[a-f0-9]{40}', args.head):
        parser.error('expected owner/name and a full lowercase HEAD SHA')
    cache = Path('.cache/reviews') / args.head
    cache.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix='ci-', dir=cache))
    signal.signal(signal.SIGTERM, interrupt)
    result = watch(args.repository, args.pr, args.head, output, args.timeout, args.max_requests)
    print(json.dumps({**result, 'evidence': str(output)}, separators=(',', ':')))
    return {'ci-complete': 0, 'failed': 1, 'stale': 2, 'incomplete': 2, 'interrupted': 130}[result['state']]


if __name__ == '__main__':
    raise SystemExit(main())
