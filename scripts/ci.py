#!/usr/bin/env python3
"""Capture a CI command and environment; summarize optional load metrics."""

import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time


def capture(command):
    result = subprocess.run(command, capture_output=True, text=True, timeout=10)
    return result.stdout.strip()


def run(label, command, directory):
    directory.mkdir(parents=True, exist_ok=True)
    os.environ["KAFERRY_REPORT_DIR"] = str(directory.resolve())
    binary = Path("bin/kaferry")
    environment = {
        "commit": capture(["git", "rev-parse", "HEAD"]),
        "working_tree_dirty": bool(capture(["git", "status", "--porcelain"])),
        "go": capture(["go", "version"]),
        "platform": platform.platform(),
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
    rows = ["## Build results", "", "| Command | Result | Seconds |", "| --- | --- | ---: |"]
    for filename in sorted(directory.glob("*.result.json")):
        result = json.loads(filename.read_text())
        rows.append(f"| {filename.stem.removesuffix('.result')} | {'PASS' if result['exit_code'] == 0 else 'FAIL'} | {result['elapsed_seconds']:.1f} |")
    rows.extend(["", "| Test | Result | Records | Records/s | Sampled peak RSS MiB | Peak uncommitted records / bytes | Shutdown s |",
                 "| --- | --- | ---: | ---: | ---: | ---: | ---: |"])
    for filename in sorted(directory.glob("*.metrics.json")):
        r = json.loads(filename.read_text())
        status = "SKIP" if r["skipped"] else "PASS" if r["passed"] else "FAIL"
        rows.append(f"| {r['test']} | {status} | {r['records']} | {r['records_per_second']:.0f} | {r['sampled_peak_rss_bytes'] / (1 << 20):.1f} | {r['peak_uncommitted_records']} / {r['peak_uncommitted_bytes']} | {r.get('shutdown_seconds', 0):.3f} |")
    rows.extend(["", "Throughput is informational on shared runners. JSON metrics, environment, coverage and logs are retained in this run's artifacts.", ""])
    text = "\n".join(rows)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "summary.md").write_text(text)
    if destination := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(destination, "a") as summary:
            summary.write(text)
    print(text)


if __name__ == "__main__":
    output = Path(os.environ.get("KAFERRY_REPORT_DIR", "build-results"))
    if sys.argv[1] == "summary":
        summarize(output)
    else:
        sys.exit(run(sys.argv[1], sys.argv[2:], output))
