"""Prepare container inputs from a candidate checkout or verified published archives."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile


REPO = "pagrus7/kaferry"
FILES = ("kaferry", "LICENSE", "THIRD_PARTY_NOTICES.txt")


def verify_checksum(path, checksums):
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    rows = re.findall(r"^([a-f0-9]{64})  " + re.escape(path.name) + r"$", checksums, re.M)
    if rows != [digest]:
        raise ValueError(f"missing, duplicate or incorrect checksum: {path.name}")


def extract_archive(path, checksums, destination):
    verify_checksum(path, checksums)
    with tarfile.open(path) as archive:
        members = archive.getmembers()
        if sorted(m.name for m in members) != sorted(FILES) or not all(m.isfile() and m.size for m in members):
            raise ValueError("archive must contain exactly the binary and nonempty license/notice files")
        destination.mkdir(parents=True)
        for member in members:
            target = destination / member.name
            with archive.extractfile(member) as source, target.open("wb") as output:
                shutil.copyfileobj(source, output)
            target.chmod(0o755 if member.name == "kaferry" else 0o644)


def extract_tutorial(path, destination):
    with tarfile.open(path) as archive:
        members = archive.getmembers()
        names = [member.name for member in members]
        expected = {"archive-demo/copy.sql", "archive-demo/tutorial.toml"}
        inputs = [name for name in names if re.fullmatch(r"archive-demo/input/[a-f0-9]{32}\.[a-f0-9]{32}\.avro", name)]
        if (len(members) != 3 or len(inputs) != 1 or set(names) != expected | set(inputs)
                or not all(member.isfile() and member.size for member in members)):
            raise ValueError("tutorial must contain SQL, configuration and one archive input")
        for member in members:
            target = destination / member.name.removeprefix("archive-demo/")
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(member) as source, target.open("wb") as output:
                shutil.copyfileobj(source, output)
            target.chmod(0o644)
    (destination / "checkpoints").mkdir()


def release(tag, directory):
    if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", tag):
        raise ValueError("container releases require a stable version tag")
    metadata = json.loads(subprocess.check_output(["gh", "release", "view", tag, "--repo", REPO,
                                                  "--json", "tagName,isDraft,isPrerelease"], text=True))
    if metadata != {"tagName": tag, "isDraft": False, "isPrerelease": False}:
        raise ValueError("expected a published stable release")
    item = json.loads(subprocess.check_output(["gh", "api", f"repos/{REPO}/git/ref/tags/{tag}"], text=True))["object"]
    while item["type"] == "tag":
        item = json.loads(subprocess.check_output(["gh", "api", f"repos/{REPO}/git/tags/{item['sha']}"], text=True))["object"]
    if item["type"] != "commit" or not re.fullmatch("[a-f0-9]{40}", item["sha"]):
        raise ValueError("tag must resolve to a commit")
    revision = item["sha"]
    with tempfile.TemporaryDirectory(prefix="kaferry-image-") as name:
        downloaded = Path(name)
        subprocess.run(["gh", "release", "download", tag, "--repo", REPO, "--dir", name,
                        "--pattern", "SHA256SUMS", "--pattern", f"kaferry-{tag}-linux-*.tar.gz",
                        "--pattern", "provenance-linux-*.json", "--pattern", "archive-demo.tar.gz"], check=True)
        checksums = (downloaded / "SHA256SUMS").read_text()
        packages = [(f"kaferry-{tag}-linux-{arch}.tar.gz", arch) for arch in ("amd64", "arm64")]
        packages.append(("archive-demo.tar.gz", "amd64"))
        for filename, arch in packages:
            path = downloaded / filename
            subprocess.run(["gh", "attestation", "verify", str(path), "--repo", REPO,
                            "--bundle", str(downloaded / f"provenance-linux-{arch}.json"),
                            "--signer-workflow", f"{REPO}/.github/workflows/release.yml",
                            "--source-ref", f"refs/tags/{tag}", "--source-digest", revision,
                            "--deny-self-hosted-runners"], check=True)
            if filename == "archive-demo.tar.gz":
                verify_checksum(path, checksums)
                extract_tutorial(path, directory / "archive-demo")
            else:
                extract_archive(path, checksums, directory / "linux" / arch)
    return tag, revision


def candidate(directory):
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    version = "validation-" + revision
    for arch in ("amd64", "arm64"):
        target = directory / "linux" / arch
        target.mkdir(parents=True)
        env = dict(os.environ, CGO_ENABLED="0", GOOS="linux", GOARCH=arch)
        subprocess.run(["make", "build", f"KAFERRY_VERSION={version}", f"KAFERRY_REVISION={revision}"], env=env, check=True)
        subprocess.run(["python3", "scripts/notices.py", "bin/kaferry", "bin/THIRD_PARTY_NOTICES.txt"], env=env, check=True)
        for filename in FILES:
            shutil.copy2(Path(filename) if filename == "LICENSE" else Path("bin") / filename, target / filename)
    bundle = directory / "archive-demo.tar.gz"
    subprocess.run(["go", "run", "./scripts/package_examples", str(bundle)], check=True)
    extract_tutorial(bundle, directory / "archive-demo")
    bundle.unlink()
    return version, revision


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("candidate", "release"))
    parser.add_argument("directory", type=Path)
    parser.add_argument("--tag")
    args = parser.parse_args()
    args.directory.mkdir(parents=True, exist_ok=False)
    if args.mode == "release":
        if not args.tag:
            parser.error("release inputs need --tag")
        version, revision = release(args.tag, args.directory)
    else:
        version, revision = candidate(args.directory)
    record = {"version": version, "revision": revision}
    (args.directory / "release.json").write_text(json.dumps(record, indent=2) + "\n")
    print(json.dumps(record))


if __name__ == "__main__":
    main()
