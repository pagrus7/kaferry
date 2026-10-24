"""Check image identity, non-root execution, exact packaged bytes and file copying."""

import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import tempfile


def write_wrapper(path, root, image):
    command = ["docker", "run", "--rm", "-i", "--network=host", "--user", f"{os.getuid()}:{os.getgid()}",
               "--env", "HOME=/tmp", "--mount", f"type=bind,src={root},dst={root}"]
    path.write_text("#!/bin/sh\nexec " + shlex.join(command) + ' --workdir "$PWD" ' + shlex.quote(image) + ' "$@"\n')
    path.chmod(0o755)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("image")
    parser.add_argument("context", type=Path)
    parser.add_argument("architecture", choices=("amd64", "arm64"))
    args = parser.parse_args()
    record = json.loads((args.context / "release.json").read_text())
    config = json.loads(subprocess.check_output(["docker", "image", "inspect", args.image], text=True))[0]
    if config["Config"]["User"] != "65532:65532" or config["Architecture"] != args.architecture:
        raise ValueError("image architecture/non-root identity differs from the expected target")
    container = subprocess.check_output(["docker", "create", args.image], text=True).strip()
    try:
        with tempfile.TemporaryDirectory() as name:
            for filename in ("kaferry", "LICENSE", "THIRD_PARTY_NOTICES.txt"):
                source = "/usr/local/bin/kaferry" if filename == "kaferry" else "/usr/share/doc/kaferry/" + filename
                destination = Path(name) / filename
                subprocess.run(["docker", "cp", f"{container}:{source}", str(destination)], check=True)
                if destination.read_bytes() != (args.context / "linux" / args.architecture / filename).read_bytes():
                    raise ValueError(f"image changed the packaged {filename}")
            ca = Path(name) / "ca.pem"
            subprocess.run(["docker", "cp", f"{container}:/etc/ssl/certs/ca-certificates.crt", str(ca)], check=True)
            if not ca.stat().st_size:
                raise ValueError("container is missing its CA certificate bundle")
    finally:
        subprocess.run(["docker", "rm", container], check=True, stdout=subprocess.DEVNULL)
    output = subprocess.check_output(["docker", "run", "--rm", "--read-only", "--cap-drop=ALL",
                                      "--security-opt=no-new-privileges", args.image, "--version"], text=True)
    if not output.startswith(f"kaferry {record['version']} (commit {record['revision']}; "):
        raise ValueError(f"unexpected container version: {output!r}")
    root = Path.cwd().resolve()
    wrapper = root / ".cache/container-kaferry"
    write_wrapper(wrapper, root, args.image)
    temporary = root / ".cache/container-tmp"
    temporary.mkdir(exist_ok=True)
    env = dict(os.environ, TMPDIR=str(temporary), KAFERRY_TEST_BINARY=str(wrapper))
    # The wrapper is not the executable; exact image/binary identity was checked above.
    env.pop("KAFERRY_EXPECT_BINARY_SHA256", None)
    subprocess.run(["go", "test", "-count=1", "-tags=integration,load", "-run",
                    "^(TestArchiveTutorial|TestLoadObservability)$/^round-0$/^disabled$", "./tests/compat"], env=env, check=True)
    print("Container identity, licenses, CA certificates and file-copy fidelity verified")


if __name__ == "__main__":
    main()
