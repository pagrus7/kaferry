#!/usr/bin/env python3
"""Report benchmarks and Python duplication."""

import json
import os
from pathlib import Path
import re
import sys


def benchmarks(text):
    output = []
    for line in text.splitlines(keepends=True):
        try:
            event = json.loads(line)
        except ValueError:
            output.append(line)
        else:
            if isinstance(event, dict) and event.get('Action') == 'output':
                output.append(event.get('Output', ''))
    text = ''.join(output)
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
    if operation != "maintainability":
        raise ValueError("expected maintainability")
    result = maintainability(directory)
    (directory / "quality.json").write_text(json.dumps(result, indent=2) + "\n")
