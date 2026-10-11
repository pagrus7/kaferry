#!/usr/bin/env python3
"""Fetch prior qualification artifacts; compare only matching test settings."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import shutil

from quality import benchmarks


def fetch(suite):
    destination = Path(".cache/previous")
    shutil.rmtree(destination, ignore_errors=True)
    runs = json.loads(subprocess.check_output([
        "gh", "run", "list", "--workflow", "qualification.yml", "--branch", "main",
        "--status", "success", "--limit", "5", "--json", "databaseId,headSha,url"], text=True))
    destination.mkdir(parents=True, exist_ok=True)
    for run in runs:
        with tempfile.TemporaryDirectory() as name:
            result = subprocess.run(["gh", "run", "download", str(run["databaseId"]),
                                     "--name", f"qualification-{suite}", "--dir", name])
            if result.returncode == 0:
                for path in Path(name).iterdir():
                    path.rename(destination / path.name)
                (destination / "baseline.json").write_text(json.dumps({**run, "suite": suite}))
                print(f"Previous successful qualification: {run['url']}")
                return
    print("No retained successful baseline; current measurements remain available.")


def image_ids(value):
    images = json.loads(value) if isinstance(value, str) and value else value
    return sorted(image["ID"] for image in images) if images else None


def comparison(directory, baseline=Path(".cache/previous")):
    if not (baseline / "baseline.json").exists():
        return []
    run = json.loads((baseline / "baseline.json").read_text())
    rows = ["", "### Previous qualification", "",
            f"Baseline: [`{run['headSha']}`]({run['url']}). Shared-runner comparisons are informational.", ""]
    suite = json.loads((baseline / "baseline.json").read_text())["suite"]
    current_env, prior_env = (root / f"{suite}.environment.json" for root in (directory, baseline))
    if not current_env.exists() or not prior_env.exists():
        return rows + ["Environment unavailable; no numeric comparison.", ""]
    current, previous = (json.loads(path.read_text()) for path in (current_env, prior_env))
    current["broker_images"], previous["broker_images"] = (image_ids(e.get("broker_images")) for e in (current, previous))
    settings = ("go", "broker_images", "soak_duration", "system", "machine")
    if any(current.get(key) != previous.get(key) for key in settings):
        return rows + ["Toolchain, platform, broker images or duration differ; no numeric comparison.", ""]
    if suite in {"load", "soak", "fault"} and not current["broker_images"]:
        return rows + ["Broker image identities unavailable; no numeric comparison.", ""]
    old = {r["test"]: r for p in baseline.glob("*.metrics.json") for r in [json.loads(p.read_text())]}
    rows += ["| Test / measure | Previous | Current |", "| --- | ---: | ---: |"]
    settings = ("platform", "librdkafka", "worker_record_cap", "worker_byte_cap", "source_partitions", "destination_partitions")
    measures = ("records_per_second", "sampled_peak_rss_bytes", "shutdown_seconds")
    for filename in sorted(directory.glob("*.metrics.json")):
        r = json.loads(filename.read_text())
        p = old.get(r["test"])
        if not p or not p["passed"] or p["skipped"] or not r["passed"] or r["skipped"]:
            continue
        if any(r.get(key) != p.get(key) for key in settings):
            rows.append(f"| {r['test']} | Settings differ | — |")
            continue
        for key in measures:
            if key in p and key in r:
                rows.append(f"| {r['test']} / {key} | {p[key]:.3f} | {r[key]:.3f} |")
    if (directory / "benchmark.log").exists() and (baseline / "benchmark.log").exists():
        old_benchmarks = benchmarks((baseline / "benchmark.log").read_text())
        for name, values in benchmarks((directory / "benchmark.log").read_text()).items():
            if name in old_benchmarks:
                for measure, before, after in zip(("ns/op", "B/op", "allocs/op"), old_benchmarks[name], values):
                    rows.append(f"| {name} / {measure} | {before:g} | {after:g} |")
    return rows + [""]


if __name__ == "__main__":
    try:
        fetch(sys.argv[1])
    except (OSError, subprocess.CalledProcessError, ValueError) as error:
        print(f"Previous measurements unavailable: {error}. Current suite result is unchanged.")
