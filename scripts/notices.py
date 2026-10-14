#!/usr/bin/env python3
"""Collect release notices from linked Go packages and the reviewed native bundle."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys


def license_text(directory, fallback=None):
    files = sorted(p for p in directory.iterdir() if p.is_file() and
                   p.name.upper().startswith(("LICENSE", "COPYING", "NOTICE", "PATENTS")))
    if fallback and fallback.is_file() and not any(p.name.upper().startswith(("LICENSE", "COPYING")) for p in files):
        files.append(fallback)
    if not any(p.name.upper().startswith(("LICENSE", "COPYING")) and p.stat().st_size for p in files):
        raise ValueError(f"missing license in {directory}")
    return "\n\n".join(f"{p.name}\n\n{p.read_text(encoding='utf-8')}" for p in files)


def native_text(client, archive, root):
    pins = json.loads((root / "licenses/native.json").read_text(encoding="utf-8"))
    if client != pins["client"] or archive.name not in pins["archives"]:
        raise ValueError("native bundle changed; review licenses/native.txt and licenses/native.json")
    for name, expected in pins["archives"].items():
        if hashlib.sha256((archive.parent / name).read_bytes()).hexdigest() != expected:
            raise ValueError("native bundle changed; review licenses/native.txt and licenses/native.json")
    return license_text(archive.parent) + "\n\n" + (root / "licenses/native.txt").read_text(encoding="utf-8")


def collect(root, binary):
    template = "{{if .Module}}{{if not .Module.Main}}{{.Module.Path}}\t{{.Module.Version}}\t{{.Module.Dir}}{{end}}{{else}}{{.ImportPath}}\t\t{{.Dir}}{{end}}"
    packages = subprocess.check_output(["go", "list", "-deps", "-f", template, "./cmd/kaferry"], text=True, cwd=root)
    environment = json.loads(subprocess.check_output(["go", "env", "-json", "GOROOT", "GOVERSION"], text=True, cwd=root))
    built_version = subprocess.check_output(["go", "version", str(binary)], text=True, cwd=root).strip().rsplit(": ", 1)[-1]
    if built_version != environment["GOVERSION"]:
        raise ValueError("build the binary and collect its notices with the same Go toolchain")
    goroot = Path(environment["GOROOT"])
    entries = {"Go runtime and standard library": goroot}
    client = None
    for line in packages.splitlines():
        if not line:
            continue
        name, version, directory = line.split("\t")
        if version:
            entries[f"{name} {version}"] = Path(directory)
        elif name.startswith("vendor/"):
            module = "/".join(name.split("/")[:4])
            entries[f"Go standard-library {module}"] = goroot / "src" / module
        if name == "github.com/confluentinc/confluent-kafka-go/v2":
            client = version
    if client is None:
        raise ValueError("Kafka client missing from linked packages")
    flags = subprocess.check_output(["go", "list", "-f", "{{join .CgoLDFLAGS \"\\n\"}}",
                                     "github.com/confluentinc/confluent-kafka-go/v2/kafka"], text=True, cwd=root)
    archives = [Path(flag) for flag in flags.splitlines() if flag.endswith(".a")]
    if len(archives) != 1:
        raise ValueError("expected one bundled static librdkafka archive")
    sections = ["Third-party software retains the licenses reproduced below.\n"
                "Native bundle entries may include components not linked on this platform."]
    sections.extend(f"{name}\n\n{license_text(directory, goroot.parent / 'LICENSE' if directory == goroot else None)}"
                    for name, directory in sorted(entries.items()))
    sections.append(f"Native bundle ({client}, {archives[0].name})\n\n{native_text(client, archives[0], root)}")
    return "\n\n".join(sections) + "\n"


if __name__ == "__main__":
    root = Path(__file__).resolve().parent.parent
    Path(sys.argv[2]).write_text(collect(root, Path(sys.argv[1]).resolve()), encoding="utf-8")
