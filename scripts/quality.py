#!/usr/bin/env python3
"""Produce statement coverage and production-Go maintainability reports."""

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile


def coverage(text):
    lines = text.splitlines()
    if not lines or lines[0] not in {"mode: set", "mode: count", "mode: atomic"}:
        raise ValueError("missing or invalid coverage mode")
    blocks = {}
    for line in lines[1:]:
        match = re.fullmatch(r"(.+):(\d+\.\d+,\d+\.\d+) (\d+) (\d+)", line)
        if not match:
            raise ValueError("invalid coverage block")
        name, position, statements, hits = match.groups()
        key, value = (name, position), (int(statements), int(hits) > 0)
        if key in blocks:
            count, covered = blocks[key]
            if count != value[0]:
                raise ValueError("inconsistent duplicate coverage block")
            value = count, covered or value[1]
        blocks[key] = value
    packages = {}
    for (name, _), (statements, covered) in blocks.items():
        package = name.rsplit("/", 1)[0]
        counts = packages.setdefault(package, {"covered": 0, "statements": 0})
        counts["statements"] += statements
        counts["covered"] += statements if covered else 0
    total = sum(p["statements"] for p in packages.values())
    if not total:
        raise ValueError("coverage profile contains no statements")
    covered = sum(p["covered"] for p in packages.values())
    return {"covered": covered, "statements": total, "percent": covered * 100 / total,
            "packages": packages}


def production_files(root):
    return sorted(p for folder in ("cmd", "internal") for p in (root / folder).rglob("*.go")
                  if not p.name.endswith("_test.go"))


def benchmarks(text):
    matches = re.findall(r"^(Benchmark\S+)-\d+\s+\d+\s+([\d.]+) ns/op\s+(?:[\d.]+ MB/s\s+)?(\d+) B/op\s+(\d+) allocs/op", text, re.M)
    return {name: (float(ns), int(size), int(allocations)) for name, ns, size, allocations in matches}


def complexity(root):
    files = production_files(root)
    if not files:
        raise ValueError("no production Go files")
    output = subprocess.check_output(["gocyclo", "-over", "0", *map(str, files)], text=True)
    rows = []
    for line in output.splitlines():
        score, package, function, location = line.split(maxsplit=3)
        filename, number, column = location.rsplit(":", 2)
        rows.append({"complexity": int(score), "package": package, "function": function,
                     "file": str(Path(filename).relative_to(root)), "line": int(number)})
    return rows


def base_complexity(ref):
    if not re.fullmatch(r"[a-f0-9]{40}", ref):
        raise ValueError("base must be a full commit SHA")
    with tempfile.TemporaryDirectory() as name:
        root = Path(name)
        files = subprocess.check_output(["git", "ls-tree", "-r", "--name-only", ref,
                                         "cmd", "internal"], text=True).splitlines()
        for filename in files:
            if filename.endswith(".go") and not filename.endswith("_test.go"):
                destination = root / filename
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(subprocess.check_output(["git", "show", f"{ref}:{filename}"]))
        return complexity(root)


def maintainability(directory):
    current = complexity(Path.cwd())
    base = os.environ.get("QUALITY_BASE_SHA", "")
    previous = base_complexity(base) if base and set(base) != {"0"} else []
    identity = lambda row: (row["file"], row["package"], row["function"])
    scores = {identity(row): row["complexity"] for row in previous}
    for row in current:
        old = scores.get(identity(row))
        row["change"] = row["complexity"] - old if old is not None else None
    duplicates = json.loads((directory / "duplication/jscpd-report.json").read_text())
    if duplicates["statistics"]["total"]["sources"] < 1:
        raise ValueError("duplication scan contains no sources")
    return {"base": base or None, "files": len(production_files(Path.cwd())),
            "functions": len(current), "max_complexity": max(r["complexity"] for r in current),
            "hotspots": sorted(current, key=lambda r: (-r["complexity"], r["file"], r["function"]))[:10],
            "duplication": duplicates["statistics"]["total"]}


def markdown(directory):
    rows = []
    profile = directory / "coverage.json"
    if profile.exists():
        r = json.loads(profile.read_text())
        rows += ["## Go coverage", "", f"**{r['percent']:.1f}% of statements** ({r['covered']}/{r['statements']}). "
                 "Race-enabled unit, mock and file-component tests; excludes native C/C++.", "",
                 "| Package | Statement coverage |", "| --- | ---: |"]
        for name, counts in sorted(r["packages"].items()):
            percent = f"{counts['covered'] * 100 / counts['statements']:.1f}%" if counts["statements"] else "—"
            rows.append(f"| {name.removeprefix('github.com/pagrus7/kaferry/')} | {percent} |")
        rows += ["", "The artifact includes the original profile and annotated HTML.", ""]
    report = directory / "quality.json"
    if report.exists():
        r = json.loads(report.read_text())
        d = r["duplication"]
        rows += ["## Maintainability", "", f"Production Go only: {r['files']} files, {r['functions']} functions; "
                 f"maximum cyclomatic complexity **{r['max_complexity']}**.", "",
                 f"Exact clones ≥100 tokens and ≥10 lines: **{d['clones']}**, "
                 f"{d['percentage']:.2f}% duplicated lines. New clones: {d.get('newClones', '—')}.", ""]
        if r["base"]:
            rows.append(f"Compared with `{r['base']}` using the same tools and production paths.")
        rows += ["", "| Function | Location | Complexity | Change |", "| --- | --- | ---: | ---: |"]
        for h in r["hotspots"]:
            change = f"{h['change']:+d}" if h["change"] is not None else "—"
            rows.append(f"| {h['package']}.{h['function']} | {h['file']}:{h['line']} | {h['complexity']} | {change} |")
        rows += ["", "These are review signals, not correctness scores or arbitrary merge thresholds.", ""]
    return ["", *rows] if rows else []


if __name__ == "__main__":
    directory = Path(os.environ.get("KAFERRY_REPORT_DIR", "build-results"))
    directory.mkdir(parents=True, exist_ok=True)
    operation = sys.argv[1]
    if operation == "coverage":
        result = coverage((directory / "coverage.out").read_text())
    elif operation == "maintainability":
        result = maintainability(directory)
    else:
        raise ValueError("expected coverage or maintainability")
    name = "coverage" if operation == "coverage" else "quality"
    (directory / f"{name}.json").write_text(json.dumps(result, indent=2) + "\n")
