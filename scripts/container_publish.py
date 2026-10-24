"""Push an already-tested OCI archive, without replacing an existing version digest."""

import argparse
import hashlib
from pathlib import Path
import subprocess
import sys


def publish(archive, reference):
    source = "oci-archive:" + str(archive)
    expected = "sha256:" + hashlib.sha256(subprocess.check_output(["skopeo", "inspect", "--raw", source])).hexdigest()
    existing = subprocess.run(["skopeo", "inspect", "--raw", "docker://" + reference], capture_output=True)
    if existing.returncode == 0:
        digest = "sha256:" + hashlib.sha256(existing.stdout).hexdigest()
        if digest != expected:
            raise ValueError("version tag already exists with a different digest; do not replace published images")
        return expected
    error = existing.stderr.decode(errors="replace").lower()
    if not any(message in error for message in ("manifest unknown", "name unknown", "status code: 404")):
        raise RuntimeError("cannot check existing version: " + error)
    digest_file = Path(str(archive) + ".digest")
    subprocess.run(["skopeo", "copy", "--all", "--preserve-digests", "--digestfile", str(digest_file),
                    source, "docker://" + reference], check=True, stdout=sys.stderr)
    if digest_file.read_text().strip() != expected:
        raise ValueError("registry digest differs from the tested OCI archive")
    return expected


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("reference")
    args = parser.parse_args()
    print(publish(args.archive, args.reference))
