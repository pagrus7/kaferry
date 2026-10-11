"""Read authored notes from the checkout used to build a release."""

from pathlib import Path
import re
import sys


def read_notes(tag, root):
    if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+(?:[.-][A-Za-z0-9.-]+)?", tag):
        raise ValueError("release tag must be a version such as v0.3.0")
    body = (root / "releases" / f"{tag}.md").read_text(encoding="utf-8")
    if not body.startswith(f"# Kaferry {tag}\n\n"):
        raise ValueError(f"release notes must start with '# Kaferry {tag}' and a blank line")
    sections = re.split(r"^## ", body, flags=re.MULTILINE)
    changes = next((section for section in sections[1:] if section.startswith("Changes\n")), "")
    if not changes.removeprefix("Changes\n").strip():
        raise ValueError("release notes need a nonempty '## Changes' section")
    return body


if __name__ == "__main__":
    try:
        if len(sys.argv) != 2:
            raise ValueError("usage: python3 scripts/release_notes.py vX.Y.Z")
        print(read_notes(sys.argv[1], Path(__file__).resolve().parent.parent), end="")
    except (OSError, ValueError) as error:
        sys.exit(str(error))
