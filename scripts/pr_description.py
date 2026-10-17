"""Check PR description length, required sections and review-result format."""

import json
import os
from pathlib import Path
import re
import sys


SECTIONS = ("Changes", "Validation", "Docs review", "Tests review", "Simplicity review")
MAX_CHARACTERS = 2000


def sections(body):
    lines = re.sub(r"<!--.*?(?:-->|\Z)", "", body, flags=re.DOTALL).splitlines()
    headings = []
    fence = None
    for index, line in enumerate(lines):
        if fence:
            if re.fullmatch(r" {0,3}" + re.escape(fence[0]) + "{" + str(len(fence)) + r",}[ \t]*", line):
                fence = None
            continue
        opening = re.match(r" {0,3}(`{3,}|~{3,})", line)
        if opening:
            fence = opening[1]
            continue
        heading = re.fullmatch(r" {0,3}##[ \t]+(.+?)[ \t]*", line)
        if heading:
            name = re.sub(r"[ \t]+#+$", "", heading[1])
            headings.append((name, index))
    result = {}
    for position, (name, start) in enumerate(headings):
        end = headings[position + 1][1] if position + 1 < len(headings) else len(lines)
        result.setdefault(name, []).append("\n".join(lines[start + 1:end]).strip())
    return result


def validate(body):
    errors = []
    if len(body) > MAX_CHARACTERS:
        errors.append(f"description exceeds {MAX_CHARACTERS} characters (including comments)")
    content = sections(body)
    for name in SECTIONS:
        values = content.get(name, [])
        if len(values) != 1:
            errors.append(f"'## {name}' must appear exactly once")
        elif not values[0]:
            errors.append(f"'## {name}' needs content")
        elif name.endswith("review") and values[0] != "Passed" and not re.fullmatch(r"Failed:[ \t]+\S[^\n]*", values[0]):
            errors.append(f"'## {name}' must contain only 'Passed' or 'Failed: reason'")
    return errors


if __name__ == "__main__":
    try:
        event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text(encoding="utf-8"))
        errors = validate(event["pull_request"].get("body") or "")
    except (KeyError, OSError, ValueError):
        sys.exit("Cannot read the pull request event.")
    if errors:
        sys.exit("\n".join(errors))
    print("PR description passed.")
