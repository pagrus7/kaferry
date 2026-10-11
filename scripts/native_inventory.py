#!/usr/bin/env python3
"""Add the selected static librdkafka build material to a binary SPDX SBOM."""

import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inventory(binary, sbom, destination):
    package = json.loads(subprocess.check_output(
        ["go", "list", "-json", "github.com/confluentinc/confluent-kafka-go/v2/kafka"], text=True))
    archives = [Path(flag) for flag in package["CgoLDFLAGS"] if flag.endswith(".a")]
    if len(archives) != 1:
        raise ValueError("expected one bundled static librdkafka archive")
    archive = archives[0]
    with tempfile.TemporaryDirectory() as name:
        probe = Path(name) / "main.go"
        probe.write_text('package main\nimport ("fmt"; k "github.com/confluentinc/confluent-kafka-go/v2/kafka")\n'
                         'func main() { _, v := k.LibraryVersion(); fmt.Println(v); fmt.Println(k.LibrdkafkaLinkInfo) }\n')
        version, link = subprocess.check_output(["go", "run", str(probe)], text=True).strip().splitlines()
    license_path = archive.parent / "LICENSES.txt"
    shutil.copyfile(license_path, destination.with_suffix(".licenses.txt"))
    # Strings are observations, not proof that all bundle components are linked.
    strings = subprocess.check_output(["strings", str(binary)], text=True, errors="replace")
    observations = sorted(set(re.findall(r"(?:OpenSSL \d+\.\d+\.\d+[^\n]*|libcurl/\d+\.\d+\.\d+)", strings)))
    result = {"binary_sha256": sha256(binary), "librdkafka": version, "link_info": link,
              "go_client": package["Module"]["Version"], "archive": archive.name,
              "archive_sha256": sha256(archive), "link_flags": package["CgoLDFLAGS"][1:],
              "binary_version_strings": observations,
              "scope": "Selected static librdkafka archive and observed binary version strings. "
                       "Bundle license entries do not establish which other components were linked; "
                       "this is not a complete native dependency or vulnerability analysis."}
    destination.write_text(json.dumps(result, indent=2) + "\n")
    document = json.loads(sbom.read_text())
    native_id = "SPDXRef-Package-librdkafka"
    document["packages"].append({"SPDXID": native_id, "name": "librdkafka", "versionInfo": version,
                                 "downloadLocation": "https://github.com/confluentinc/librdkafka",
                                 "filesAnalyzed": False, "licenseConcluded": "NOASSERTION",
                                 "licenseDeclared": "NOASSERTION", "copyrightText": "NOASSERTION",
                                 "sourceInfo": f"{link}; selected archive SHA256 {sha256(archive)}"})
    clients = [p["SPDXID"] for p in document["packages"]
               if p["name"] == "github.com/confluentinc/confluent-kafka-go/v2"]
    if len(clients) != 1:
        raise ValueError("SBOM must identify exactly one confluent-kafka-go module")
    document["relationships"].append({"spdxElementId": clients[0], "relatedSpdxElement": native_id,
                                      "relationshipType": "STATIC_LINK"})
    sbom.write_text(json.dumps(document, indent=2) + "\n")


if __name__ == "__main__":
    inventory(*map(Path, sys.argv[1:]))
