"""Check human PR descriptions; retain Dependabot's generated release notes."""

import json
import os
from pathlib import Path
import re
import subprocess
import sys


SECTIONS = ("Changes", "Validation", "Docs review", "Tests review", "Simplicity review")
MAX_CHARACTERS = 2000
FORMATTING_ONLY = "Not required: formatting only"


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


def validate(body, author=None, draft=False, paths=None):
    if author and author.get("login") == "dependabot[bot]" and author.get("type") == "Bot":
        return []
    errors = []
    if len(body) > MAX_CHARACTERS:
        errors.append(f"description exceeds {MAX_CHARACTERS} characters (including comments)")
    content = sections(body)
    exemptions = {name for name in SECTIONS if content.get(name) == [FORMATTING_ONLY]}
    if exemptions:
        if exemptions != {"Tests review", "Simplicity review"}:
            errors.append("formatting-only requires both Tests and Simplicity exemptions and a Docs review")
        if not paths or not all(
            p == "README.md" or p.startswith(("docs/", "releases/")) and p.endswith(".md")
            and Path(p).name not in ("AGENTS.md", "AGENTS.override.md", "SKILL.md")
            for p in paths
        ):
            errors.append("formatting-only requires README.md or Markdown under docs/ or releases/")
    for name in SECTIONS:
        values = content.get(name, [])
        if len(values) != 1:
            errors.append(f"'## {name}' must appear exactly once")
        elif not values[0]:
            errors.append(f"'## {name}' needs content")
        elif name.endswith("review") and values[0] != "Passed" and name not in exemptions:
            if not draft:
                errors.append(f"'## {name}' must be 'Passed' before the PR is ready")
            elif values[0] != "Pending" and not re.fullmatch(r"Failed:[ \t]+\S[^\n]*", values[0]):
                errors.append(f"'## {name}' must contain only 'Passed', 'Pending' or 'Failed: reason'")
    return errors


if __name__ == "__main__":
    try:
        event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text(encoding="utf-8"))
        pull_request = event["pull_request"]
        body = pull_request.get("body") or ""
        paths = None
        if [FORMATTING_ONLY] in sections(body).values():
            base, head = pull_request["base"]["sha"], pull_request["head"]["sha"]
            if not all(re.fullmatch("[a-f0-9]{40}", sha) for sha in (base, head)):
                raise ValueError("Expected full base and head SHAs")
            paths = subprocess.check_output(
                ["git", "diff", "--no-ext-diff", "--no-renames", "--name-only", "-z", f"{base}...{head}"],
                text=True,
            ).split("\0")[:-1]
        errors = validate(body, pull_request.get("user"), pull_request.get("draft", False), paths)
    except (KeyError, OSError, ValueError, subprocess.CalledProcessError):
        sys.exit("Cannot read the pull request event.")
    if errors:
        sys.exit("\n".join(errors))
    print("PR description passed.")
