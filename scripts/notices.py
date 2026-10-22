#!/usr/bin/env python3
"""Collect release notices from linked Go packages and their license directories."""

import json
from pathlib import Path
import subprocess
import sys


LICENSE_NAMES = ("LICENSE", "LICENCE", "COPYING")
NOTICE_NAMES = LICENSE_NAMES + ("NOTICE", "PATENTS")

def license_text(directory, fallback=None):
    files = sorted(p for p in directory.iterdir() if p.is_file() and
                   p.name.upper().startswith(NOTICE_NAMES))
    if fallback and fallback.is_file() and not any(p.name.upper().startswith(LICENSE_NAMES) for p in files):
        files.append(fallback)
    if not any(p.name.upper().startswith(LICENSE_NAMES) and p.stat().st_size for p in files):
        raise ValueError(f"missing license in {directory}")
    return "\n\n".join(f"{p.name}\n\n{p.read_text(encoding='utf-8')}" for p in files)


def module_text(directory, packages):
    result = [license_text(directory)]
    parents = set()
    for package in packages:
        while package != directory:
            package.relative_to(directory)
            parents.add(package)
            package = package.parent
    for parent in sorted(parents):
        for path in sorted(parent.iterdir()):
            if path.is_file() and path.name.upper().startswith(NOTICE_NAMES):
                result.append(f"{path.relative_to(directory)}\n\n{path.read_text(encoding='utf-8')}")
    return "\n\n".join(result)


def collect(root, binary):
    template = "{{if .Module}}{{if not .Module.Main}}{{.Module.Path}}\t{{.Module.Version}}\t{{.Module.Dir}}\t{{.Dir}}{{end}}{{else}}{{.ImportPath}}\t\t{{.Dir}}\t{{.Dir}}{{end}}"
    packages = subprocess.check_output(["go", "list", "-deps", "-f", template, "./cmd/kaferry"], text=True, cwd=root)
    environment = json.loads(subprocess.check_output(["go", "env", "-json", "GOROOT", "GOVERSION"], text=True, cwd=root))
    built_version = subprocess.check_output(["go", "version", str(binary)], text=True, cwd=root).strip().rsplit(": ", 1)[-1]
    if built_version != environment["GOVERSION"]:
        raise ValueError("build the binary and collect its notices with the same Go toolchain")
    goroot = Path(environment["GOROOT"])
    entries = {"Go runtime and standard library": goroot}
    linked_packages = {}
    client = False
    for line in packages.splitlines():
        if not line:
            continue
        name, version, directory, package = line.split("\t")
        if version:
            label = f"{name} {version}"
            entries[label] = Path(directory)
            linked_packages.setdefault(label, set()).add(Path(package))
        elif name.startswith("vendor/"):
            module = "/".join(name.split("/")[:4])
            entries[f"Go standard-library {module}"] = goroot / "src" / module
        if name == "github.com/twmb/franz-go":
            client = True
    if not client:
        raise ValueError("Kafka client missing from linked packages")
    sections = ["Third-party software retains the licenses reproduced below."]
    for name, directory in sorted(entries.items()):
        text = (module_text(directory, linked_packages[name]) if name in linked_packages else
                license_text(directory, goroot.parent / "LICENSE" if directory == goroot else None))
        sections.append(f"{name}\n\n{text}")
    return "\n\n".join(sections) + "\n"


if __name__ == "__main__":
    root = Path(__file__).resolve().parent.parent
    Path(sys.argv[2]).write_text(collect(root, Path(sys.argv[1]).resolve()), encoding="utf-8")
