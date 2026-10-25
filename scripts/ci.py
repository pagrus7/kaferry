#!/usr/bin/env python3
"""Capture a CI command and environment; summarize optional load metrics."""

import hashlib
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time

from quality import benchmarks, markdown
from history import comparison, image_ids


def verify_tests(log, expected):
    passed = set()
    for line in log.splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if isinstance(event, dict) and event.get('Action') == 'pass' and event.get('Test'):
            passed.add(event['Test'])
    missing = [name for name in expected if name not in passed]
    if missing:
        raise ValueError('Required tests did not pass: ' + ', '.join(missing))


def capture(command):
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=10)
    except FileNotFoundError:
        return ""
    return result.stdout.strip()


def run(label, command, directory):
    directory.mkdir(parents=True, exist_ok=True)
    os.environ["KAFERRY_REPORT_DIR"] = str(directory.resolve())
    binary = Path(os.environ.get("KAFERRY_TEST_BINARY", "bin/kaferry"))
    environment = {
        "commit": capture(["git", "rev-parse", "HEAD"]),
        "started_at": datetime.now(timezone.utc).isoformat(),
        "working_tree_dirty": bool(capture(["git", "status", "--porcelain"])),
        "go": capture(["go", "version"]),
        "platform": platform.platform(),
        "system": platform.system(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "runner": os.environ.get("RUNNER_NAME"),
        "run_url": f"https://github.com/{os.environ.get('GITHUB_REPOSITORY', '')}/actions/runs/{os.environ.get('GITHUB_RUN_ID', '')}",
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest() if binary.exists() else None,
        "broker_images": capture(["docker", "compose", "-p", "kaferry-compat", "--profile", "cross-cluster", "images", "--format", "json"]),
        "soak_duration": os.environ.get("KAFERRY_SOAK_DURATION"),
    }
    (directory / f"{label}.environment.json").write_text(json.dumps(environment, indent=2) + "\n")
    start = time.monotonic()
    with (directory / f"{label}.log").open("w") as log:
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors="replace") as process:
            for line in process.stdout:
                sys.stdout.write(line)
                sys.stdout.flush()
                log.write(line)
                log.flush()
            code = process.wait()
    result = {"command": command, "exit_code": code, "elapsed_seconds": time.monotonic() - start}
    (directory / f"{label}.result.json").write_text(json.dumps(result, indent=2) + "\n")
    return code


def summarize(directory):
    rows = ["## Build results", ""]
    environments = sorted(directory.glob("*.environment.json"))
    if environments:
        env = json.loads(environments[0].read_text())
        rows += [f"Commit: `{env['commit']}` · Date: {env.get('started_at', 'unavailable')} · "
                 f"{env['go']}", ""]
    rows += ["| Command | Result | Seconds |", "| --- | --- | ---: |"]
    for filename in sorted(directory.glob("*.result.json")):
        result = json.loads(filename.read_text())
        rows.append(f"| {filename.stem.removesuffix('.result')} | {'PASS' if result['exit_code'] == 0 else 'FAIL'} | {result['elapsed_seconds']:.1f} |")
    timings = []
    for filename in sorted(directory.glob('*.log')):
        for line in filename.read_text(errors='replace').splitlines():
            if not line.startswith('{'):
                continue
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get('Action') in {'pass', 'fail'} and event.get('Test'):
                timings.append((event.get('Elapsed', 0), event['Test'], event.get('Package', '')))
    if timings:
        rows += ['', '### Slowest tests', '', '| Test | Package | Seconds |', '| --- | --- | ---: |']
        rows += [f'| {test} | {package} | {elapsed:.2f} |' for elapsed, test, package in sorted(timings, reverse=True)[:10]]
        rows += ['', 'Parent tests include subtest time; these durations must not be added together.', '']
    rows.extend(["", "| Test | Result | Records | Records/s | Sampled peak RSS MiB | RSS samples | RSS growth MiB | Peak uncommitted records / bytes | Sampled max lag | Shutdown s |",
                 "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"])
    for filename in sorted(directory.glob("*.metrics.json")):
        r = json.loads(filename.read_text())
        status = "SKIP" if r["skipped"] else "PASS" if r["passed"] else "FAIL"
        growth = ((r["final_median_rss_bytes"] - r["warm_median_rss_bytes"]) / (1 << 20)
                  if "final_median_rss_bytes" in r and "warm_median_rss_bytes" in r else None)
        shutdown = f"{r['shutdown_seconds']:.3f}" if "shutdown_seconds" in r else "—"
        rows.append(f"| {r['test']} | {status} | {r['records']} | {r['records_per_second']:.0f} | {r['sampled_peak_rss_bytes'] / (1 << 20):.1f} | {r.get('rss_samples', '—')} | {f'{growth:.1f}' if growth is not None else '—'} | {r['peak_uncommitted_records']} / {r['peak_uncommitted_bytes']} | {r.get('max_sampled_commit_lag', '—')} | {shutdown} |")
    benchmark = directory / "benchmark.log"
    if benchmark.exists():
        rows += ["", "### Codec microbenchmarks", "", "| Benchmark | ns/op | B/op | allocs/op |",
                 "| --- | ---: | ---: | ---: |"]
        for name, (ns, size, allocations) in benchmarks(benchmark.read_text()).items():
            rows.append(f"| {name} | {ns} | {size} | {allocations} |")
    rows.extend(markdown(directory))
    rows.extend(comparison(directory))
    rows.extend(["", "Throughput is informational on shared runners. Bounded load timing starts at SQL input release and ends at process exit. RSS includes one sample before SQL input; short copies may finish before another sample. JSON metrics, environment, coverage and logs are retained in this run's artifacts.", ""])
    text = "\n".join(rows)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "summary.md").write_text(text)
    if destination := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(destination, "a") as summary:
            summary.write(text)
    print(text)


def release_summary(evidence, archive, commit, version, output):
    """Retain the release's measured suite results after temporary artifacts expire."""
    import tarfile

    with tarfile.open(archive, 'r:gz') as package:
        binary = package.extractfile('kaferry').read()
    digest = hashlib.sha256(binary).hexdigest()
    scenarios = {
        'integration': 'Kafka and archive copy, resume, signals and rebalances',
        'load': 'Bounded Kafka/archive workloads, fidelity and work caps',
        'fault': 'Broker outage and ownership changes',
        'qualification': 'TLS/SASL, restricted ACLs and replicated leader failure',
        'soak': 'Streaming fidelity, lag, memory and shutdown with destination stalls',
    }
    rows = [f'# Kaferry {version} qualification', '', f'Source: `{commit}`', '',
            f'Linux AMD64 binary SHA-256: `{digest}`', '',
            '| Suite | Exercised scenarios | Result | Seconds |', '| --- | --- | --- | ---: |']
    environments = []
    for suite, description in scenarios.items():
        directory = evidence / f'qualification-{suite}'
        env = json.loads((directory / f'{suite}.environment.json').read_text())
        result = json.loads((directory / f'{suite}.result.json').read_text())
        if env['commit'] != commit or env['binary_sha256'] != digest:
            raise ValueError(f'{suite}: evidence does not match the release source and binary')
        if result['exit_code'] != 0:
            raise ValueError(f'{suite}: qualification did not pass')
        if suite == 'qualification':
            env['broker_images'] = (directory / 'qualification.broker-images.json').read_text().strip()
        if not image_ids(env.get('broker_images')):
            raise ValueError(f'{suite}: broker image evidence is unavailable')
        environments.append((suite, env))
        rows.append(f"| {suite} | {description} | PASS | {result['elapsed_seconds']:.1f} |")
    rows += ['', '## Test environments', '']
    for suite, env in environments:
        rows += [f"- {suite}: {env['platform']}; {env['go']}; started {env['started_at']}. "
                 f"[Workflow run]({env['run_url']})."]
        rows += ['', f'{suite} broker images:', '', '```text', env['broker_images'], '```', '']
        if suite == 'soak':
            rows += ['', f"Configured soak duration: {env['soak_duration']}.", '']
    rows += ['', 'These results cover the listed fixtures and scenarios. They do not establish behavior for every deployment. '
             'Platform build information, SBOMs and provenance are separate release assets. '
             'Detailed logs and measurements are retained temporarily in the linked workflow artifacts.', '']
    output.write_text('\n'.join(rows))


if __name__ == "__main__":
    output = Path(os.environ.get("KAFERRY_REPORT_DIR", "build-results"))
    if sys.argv[1] == "release-summary":
        release_summary(Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4], sys.argv[5], Path(sys.argv[6]))
    elif sys.argv[1] == "summary":
        summarize(output)
    elif sys.argv[1] == 'verify-tests':
        verify_tests((output / f'{sys.argv[2]}.log').read_text(), sys.argv[3:])
    else:
        sys.exit(run(sys.argv[1], sys.argv[2:], output))
