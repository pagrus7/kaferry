#!/usr/bin/env python3
"""Report coverage and Python duplication."""

import json
import os
from pathlib import Path
import re
import sys

from tools.review.review_coverage import sum_blocks


def coverage(text):
    blocks = sum_blocks([text])
    packages = {}
    for (name, _, _, _, _), (statements, hits) in blocks.items():
        package = name.rsplit("/", 1)[0]
        counts = packages.setdefault(package, {"covered": 0, "statements": 0})
        counts["statements"] += statements
        counts["covered"] += statements if hits > 0 else 0
    total = sum(p["statements"] for p in packages.values())
    if not total:
        raise ValueError("coverage profile contains no statements")
    covered = sum(p["covered"] for p in packages.values())
    return {"covered": covered, "statements": total, "percent": covered * 100 / total,
            "packages": packages}


def benchmarks(text):
    matches = re.findall(r"^(Benchmark\S+)-\d+\s+\d+\s+([\d.]+) ns/op\s+(?:[\d.]+ \S+\s+)*?(\d+) B/op\s+(\d+) allocs/op", text, re.M)
    return {name: (float(ns), int(size), int(allocations)) for name, ns, size, allocations in matches}


def maintainability(directory):
    duplicates = json.loads((directory / "duplication/jscpd-report.json").read_text())
    if duplicates["statistics"]["total"]["sources"] < 1:
        raise ValueError("duplication scan contains no sources")
    return {"base": os.environ.get("QUALITY_BASE_SHA") or None,
            "duplication": duplicates["statistics"]["total"],
            "new_clones": [clone for clone in duplicates["duplicates"] if clone["isNew"]]}


def markdown(directory):
    rows = []
    profile = directory / "coverage.json"
    if profile.exists():
        r = json.loads(profile.read_text())
        rows += ["## Go coverage", "", f"**{r['percent']:.1f}% of statements** ({r['covered']}/{r['statements']}). "
                 "All Go packages instrumented; excludes native C/C++.", "",
                 "| Package | Statement coverage |", "| --- | ---: |"]
        for name, counts in sorted(r["packages"].items()):
            percent = f"{counts['covered'] * 100 / counts['statements']:.1f}%" if counts["statements"] else "—"
            rows.append(f"| {name.removeprefix('github.com/pagrus7/kaferry/')} | {percent} |")
        rows += ["", "The artifact includes the original profile and annotated HTML.", ""]
    report = directory / "quality.json"
    if report.exists():
        r = json.loads(report.read_text())
        d = r["duplication"]
        rows += ["## Duplication", "", f"Python under `src/` and `tools/`, including tests: {d['sources']} files scanned. "
                 f"Normalized clones ≥50 tokens and ≥5 lines: **{d['clones']}**, "
                 f"{d['percentage']:.2f}% duplicated lines. New clones: **{d['newClones']}**. "
                 "Comments, identifier names and literal values are ignored.", ""]
        if r["base"]:
            rows.append(f"Compared with `{r['base']}` using the same tools and scan settings.")
        if r["new_clones"]:
            rows += ["", "New duplication must be removed or explicitly reviewed and suppressed before merging.", "",
                     "| New clone | Matching location | Tokens |", "| --- | --- | ---: |"]
            for clone in r["new_clones"]:
                first, second = clone["firstFile"], clone["secondFile"]
                rows.append(f"| `{os.path.relpath(first['name'])}:{first['start']}-{first['end']}` | "
                            f"`{os.path.relpath(second['name'])}:{second['start']}-{second['end']}` | {clone['tokens']} |")
        rows += ["", "New duplication fails CI; existing clones remain review signals.", ""]
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
