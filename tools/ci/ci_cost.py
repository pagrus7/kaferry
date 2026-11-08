"""Collect raw GitHub Actions timing, jobs and cache-transfer evidence."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import json
from pathlib import Path
import re
import statistics
import subprocess

REPO = 'pagrus7/kaferry'
WORKFLOWS = ('ci.yml', 'compatibility.yml', 'quality.yml', 'risk.yml', 'suites.yml', 'qualification.yml', 'parallel-lab.yml', 'replicated-lab.yml', 'containers.yml', 'performance.yml')

def gh(*args):
    return subprocess.check_output(['gh', *args], text=True, stderr=subprocess.PIPE)

def seconds(start, end):
    return (datetime.fromisoformat(end.replace('Z', '+00:00')) - datetime.fromisoformat(start.replace('Z', '+00:00'))).total_seconds()

def cache_metrics(log):
    jobs = {}
    pending = {}
    for line in log.splitlines():
        fields = line.split('\t', 2)
        if len(fields) != 3:
            continue
        job, step, message = fields
        item = jobs.setdefault(job, dict(restored_bytes=0, saved_bytes=0, cache_miss=False, cache_job=False))
        match = re.search(r'Cache Size:.*\((\d+) B\)', message)
        if match:
            item['restored_bytes'] += int(match[1])
            item['cache_job'] = True
        match = re.search(r'Sent \d+ of (\d+)', message)
        if match:
            pending[job, step] = int(match[1])
        if 'Cache saved with key:' in message:
            size = pending.pop((job, step), None)
            if size is None:
                raise ValueError(f'saved cache has no byte count: {job}: {step}')
            item['saved_bytes'] += size
            item['cache_job'] = True
        if 'Cache not found for input keys' in message:
            item.update(cache_miss=True, cache_job=True)
    return jobs

def collect(run, directory):
    target = directory / str(run['id']) / str(run['run_attempt'])
    target.mkdir(parents=True, exist_ok=True)
    (target / 'run.json').write_text(json.dumps(run, indent=2))
    job_file = target / 'jobs.json'
    if not job_file.exists():
        pages = json.loads(gh('api', '--paginate', '--slurp', f'repos/{REPO}/actions/runs/{run["id"]}/attempts/{run["run_attempt"]}/jobs?per_page=100'))
        job_file.write_text(json.dumps([j for page in pages for j in page['jobs']], indent=2))
    jobs = json.loads(job_file.read_text())
    # A completed run can still list a pending job; read that job directly.
    for index, job in enumerate(jobs):
        if job.get('conclusion') is None or job.get('completed_at') is None:
            jobs[index] = json.loads(gh('api', f'repos/{REPO}/actions/jobs/{job["id"]}'))
    job_file.write_text(json.dumps(jobs, indent=2))
    if any(j.get('conclusion') is None or not j.get('started_at') or not j.get('completed_at') for j in jobs):
        raise ValueError('job evidence is incomplete; retry collection')
    log_file = target / 'log.txt'
    if not log_file.exists():
        log_file.write_text(gh('run', 'view', str(run['id']), '--repo', REPO, '--attempt', str(run['run_attempt']), '--log'))
    metrics = cache_metrics(log_file.read_text())
    executed = [j for j in jobs if j['conclusion'] != 'skipped']
    cache_jobs = [metrics.get(j['name'], {}) for j in executed]
    result = dict(id=run['id'], workflow=run['name'], sha=run['head_sha'], branch=run['head_branch'],
                  event=run['event'], conclusion=run['conclusion'], attempt=run['run_attempt'],
                  wall_seconds=seconds(run['run_started_at'], max((j['completed_at'] for j in jobs), default=run['run_started_at'])),
                  runner_seconds=sum(seconds(j['started_at'], j['completed_at']) for j in executed),
                  job_count=len(executed), restored_bytes=sum(j.get('restored_bytes',0) for j in cache_jobs),
                  saved_bytes=sum(j.get('saved_bytes',0) for j in cache_jobs),
                  cache_jobs=sum(j.get('cache_job',False) for j in cache_jobs),
                  cache_misses=sum(j.get('cache_miss',False) for j in cache_jobs),
                  job_metrics=[dict(name=j['name'], seconds=seconds(j['started_at'], j['completed_at']),
                                    conclusion=j['conclusion'], **metrics.get(j['name'], {})) for j in executed])
    (target / 'metrics.json').write_text(json.dumps(result, indent=2))
    return result

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--branch')
    parser.add_argument('--limit', type=int, default=25)
    parser.add_argument('--workflows', nargs='+', default=WORKFLOWS)
    args=parser.parse_args()
    args.directory.mkdir(parents=True, exist_ok=True)
    runs=[]
    for workflow in args.workflows:
        query=f'repos/{REPO}/actions/workflows/{workflow}/runs?per_page={args.limit}&status=completed'
        if args.branch:
            query+='&branch='+args.branch
        response=json.loads(gh('api',query))
        (args.directory / (workflow+'.json')).write_text(json.dumps(response,indent=2))
        runs.extend(response['workflow_runs'])
    results=[]
    errors=[]
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures={pool.submit(collect,run,args.directory):run for run in runs}
        for future in as_completed(futures):
            run=futures[future]
            try:
                results.append(future.result())
                print(f'{run["name"]} {run["id"]}: collected',flush=True)
            except Exception as error:
                errors.append(dict(id=run['id'], attempt=run['run_attempt'], error=str(error)))
                print(f'{run["name"]} {run["id"]}: ERROR {error}',flush=True)
    (args.directory/'metrics.json').write_text(json.dumps(results,indent=2))
    write_summary(args.directory, results, errors)

def miss_rate(runs):
    jobs = sum(r['job_count'] for r in runs)
    return sum(r['cache_misses'] for r in runs)/jobs if jobs else None

def write_summary(directory, results, errors):
    (directory/'collection.json').write_text(json.dumps(dict(complete=not errors, errors=errors), indent=2))
    if errors:
        for name in ('summary.json', 'pushes.json'):
            (directory/name).unlink(missing_ok=True)
        raise SystemExit('incomplete collection; see collection.json')
    summary={}
    for workflow in sorted({r['workflow'] for r in results}):
        subset=[r for r in results if r['workflow']==workflow]
        summary[workflow]={name:statistics.median(r[name] for r in subset) for name in ('wall_seconds','runner_seconds','job_count','restored_bytes','saved_bytes')}
        summary[workflow].update(runs=len(subset),cache_miss_rate=miss_rate(subset))
    pushes={}
    heads={}
    for run in sorted(results, key=lambda r: (r['id'], r['attempt'])):
        heads[run['sha'], run['workflow'], run['event']] = run
    for run in heads.values():
        if run['event'] != 'pull_request':
            continue
        push=pushes.setdefault(run['sha'],dict(workflows=[],critical_path_seconds=0,runner_seconds=0,job_count=0,restored_bytes=0,saved_bytes=0,cache_misses=0,cache_jobs=0))
        push['workflows'].append(run['workflow'])
        push['critical_path_seconds']=max(push['critical_path_seconds'],run['wall_seconds'])
        for name in ('runner_seconds','job_count','restored_bytes','saved_bytes','cache_misses','cache_jobs'):
            push[name]+=run[name]
    (directory/'pushes.json').write_text(json.dumps(pushes,indent=2))
    (directory/'summary.json').write_text(json.dumps(summary,indent=2))
    print(json.dumps(summary,indent=2))

if __name__=='__main__':
    main()
