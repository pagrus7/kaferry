#!/usr/bin/env python3
"""Collect release notices from linked Go packages and the reviewed native bundle."""

import hashlib
from pathlib import Path
import subprocess
import sys


# Review licenses/native.txt when updating the bundled client or these archives.
NATIVE_CLIENT = "v2.16.0"
NATIVE_ARCHIVES = {
    "librdkafka_darwin_arm64.a": "b5d62b664ab72eaefc7afa710447f3883b33823b4ef3084061a5903eae799b93",
    "librdkafka_glibc_linux_amd64.a": "186e2f2bfe9f2a311dd3ddd842ec29085c1c4889932e71c9ca82647c474b4703",
    "librdkafka_glibc_linux_arm64.a": "33700c99e5e34323447c706836e25f3c58bf697e5158f5c0b8606c0d4e746e4b",
}


def license_text(directory):
    files = sorted(p for p in directory.iterdir() if p.is_file() and
                   p.name.upper().startswith(("LICENSE", "COPYING", "NOTICE", "PATENTS")))
    if not any(p.name.upper().startswith(("LICENSE", "COPYING")) and p.stat().st_size for p in files):
        raise ValueError(f"missing license in {directory}")
    return "\n\n".join(f"{p.name}\n\n{p.read_text(encoding='utf-8')}" for p in files)


def native_text(client, archive, root):
    expected = NATIVE_ARCHIVES.get(archive.name)
    if client != NATIVE_CLIENT or hashlib.sha256(archive.read_bytes()).hexdigest() != expected:
        raise ValueError("native bundle changed; review licenses/native.txt and NATIVE_ARCHIVES")
    return license_text(archive.parent) + "\n\n" + (root / "licenses/native.txt").read_text(encoding="utf-8")


def collect(root):
    template = "{{if .Module}}{{if not .Module.Main}}{{.Module.Path}}\t{{.Module.Version}}\t{{.Module.Dir}}{{end}}{{else}}{{.ImportPath}}\t\t{{.Dir}}{{end}}"
    packages = subprocess.check_output(["go", "list", "-deps", "-f", template, "./cmd/kaferry"], text=True)
    goroot = Path(subprocess.check_output(["go", "env", "GOROOT"], text=True).strip())
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
                                     "github.com/confluentinc/confluent-kafka-go/v2/kafka"], text=True)
    archives = [Path(flag) for flag in flags.splitlines() if flag.endswith(".a")]
    if len(archives) != 1:
        raise ValueError("expected one bundled static librdkafka archive")
    sections = ["Third-party software retains the licenses reproduced below.\n"
                "Native bundle entries may include components not linked on this platform."]
    sections.extend(f"{name}\n\n{license_text(directory)}" for name, directory in sorted(entries.items()))
    sections.append(f"Native bundle ({client}, {archives[0].name})\n\n{native_text(client, archives[0], root)}")
    return "\n\n".join(sections) + "\n"


if __name__ == "__main__":
    root = Path(__file__).resolve().parent.parent
    Path(sys.argv[1]).write_text(collect(root), encoding="utf-8")
