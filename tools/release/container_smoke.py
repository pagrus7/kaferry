"""Check image identity, non-root execution, exact packaged bytes and file copying."""

import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import time
import uuid


def write_wrapper(path, root, image):
    command = ["docker", "run", "--rm", "-i", "--network=host", "--user", f"{os.getuid()}:{os.getgid()}",
               "--env", "HOME=/tmp", "--mount", f"type=bind,src={root},dst={root}"]
    path.write_text("#!/bin/sh\nexec " + shlex.join(command) + ' --workdir "$PWD" ' + shlex.quote(image) + ' "$@"\n')
    path.chmod(0o755)


def check_tutorial(image):
    # Given the bundled tutorial and a fresh Docker-managed volume, using the image's default user.
    volume = "kaferry-demo-" + uuid.uuid4().hex
    command = ["docker", "run", "--rm", "--mount", f"source={volume},target=/demo", "--workdir", "/demo", image]
    try:
        for expected in (3, 0):
            # When separate containers run the same named copy group.
            result = subprocess.run(command + ["run", "-c", "tutorial.toml", "-f", "copy.sql"],
                                    text=True, capture_output=True, check=True)
            summaries = [entry for line in result.stderr.splitlines()
                         if (entry := json.loads(line)).get("msg") == "copy summary"]
            if len(summaries) != 1 or summaries[0]["confirmed"] != expected:
                raise ValueError(f"tutorial expected {expected} confirmations: {result.stderr}")
            status = json.loads(subprocess.check_output(command + ["status", "-c", "tutorial.toml", "-f", "copy.sql", "--json"], text=True))
            # Then progress survives container removal and a rerun confirms no additional records.
            if status["verdict"] != "complete" or status["partitions"][0]["position"] != 3:
                raise ValueError(f"tutorial did not save completion: {status}")
    finally:
        subprocess.run(["docker", "volume", "rm", volume], check=True, stdout=subprocess.DEVNULL)


def check_demo(image):
    # Given the published demo entrypoint with no host files, ports or external network.
    def run(arguments=()):
        name = "kaferry-demo-" + uuid.uuid4().hex
        try:
            return subprocess.run(["docker", "run", "--rm", "--network=none", "--name", name, *arguments, image],
                                  text=True, capture_output=True, timeout=210)
        finally:
            subprocess.run(["docker", "rm", "--force", "--volumes", name],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    for _ in range(2):
        # When each fresh container completes, then verify the copy and independently read records.
        result = run()
        if result.returncode:
            raise ValueError(f"demo failed: {result}")
        summaries = [json.loads(line) for line in result.stderr.splitlines() if line.startswith('{')]
        summaries = [entry for entry in summaries if entry.get("msg") == "copy summary"]
        if len(summaries) != 1 or summaries[0]["confirmed"] != 3 or summaries[0]["submitted"] != 3:
            raise ValueError(f"demo did not copy three fresh records: {result.stderr}")
        if "order-1\norder-2\norder-3\n" not in result.stdout or "Demo passed:" not in result.stdout:
            raise ValueError(f"demo did not verify its output: {result.stdout}")
        print(result.stdout, end="")
    # Given a consumer returning corrupted output, a successful copy alone must not pass the demo.
    with tempfile.TemporaryDirectory() as name:
        consumer = Path(name) / "consumer.sh"
        consumer.write_text("#!/bin/sh\nprintf 'order-1\\norder-2\\nWRONG\\n'\n")
        consumer.chmod(0o755)
        result = run(["--mount", f"type=bind,src={consumer},dst=/opt/kafka/bin/kafka-console-consumer.sh,readonly"])
        if result.returncode != 1 or "WRONG" not in result.stdout or "Demo passed:" in result.stdout:
            raise ValueError(f"demo accepted corrupted destination output: {result}")
    # Given a running broker, cancellation must stop the container promptly with a failed result.
    container = subprocess.check_output(["docker", "run", "--detach", "--network=none", image], text=True).strip()
    try:
        deadline = time.monotonic() + 120
        while True:
            logs = subprocess.check_output(["docker", "logs", container], stderr=subprocess.STDOUT, text=True)
            if "Creating orders" in logs:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(f"demo never reached broker readiness: {logs}")
            time.sleep(0.2)
        subprocess.run(["docker", "stop", "--time", "15", container], check=True, timeout=20, stdout=subprocess.DEVNULL)
        code = subprocess.check_output(["docker", "inspect", "--format", "{{.State.ExitCode}}", container], text=True).strip()
        if code != "143":
            raise ValueError(f"demo cancellation returned {code}, expected 143")
    finally:
        subprocess.run(["docker", "rm", "--force", "--volumes", container], check=True, stdout=subprocess.DEVNULL)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("image")
    parser.add_argument("context", type=Path)
    parser.add_argument("architecture", choices=("amd64", "arm64"))
    parser.add_argument("--demo", action="store_true")
    args = parser.parse_args()
    record = json.loads((args.context / "release.json").read_text())
    config = json.loads(subprocess.check_output(["docker", "image", "inspect", args.image], text=True))[0]
    user = "appuser" if args.demo else "65532:65532"
    if config["Config"]["User"] != user or config["Architecture"] != args.architecture:
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
        subprocess.run(["docker", "rm", "--volumes", container], check=True, stdout=subprocess.DEVNULL)
    output = subprocess.check_output(["docker", "run", "--rm", "--read-only", "--cap-drop=ALL",
                                      "--security-opt=no-new-privileges", "--entrypoint", "/usr/local/bin/kaferry", args.image, "--version"], text=True)
    if not output.startswith(f"kaferry {record['version']} (commit {record['revision']}; "):
        raise ValueError(f"unexpected container version: {output!r}")
    if args.demo:
        check_demo(args.image)
        print("Demo identity, fresh runs, output verification and cancellation verified")
        return
    check_tutorial(args.image)
    root = Path.cwd().resolve()
    wrapper = root / ".cache/container-kaferry"
    write_wrapper(wrapper, root, args.image)
    temporary = root / ".cache/container-tmp"
    temporary.mkdir(exist_ok=True)
    env = dict(os.environ, TMPDIR=str(temporary), KAFERRY_TEST_BINARY=str(wrapper))
    # The wrapper is not the executable; exact image/binary identity was checked above.
    env.pop("KAFERRY_EXPECT_BINARY_SHA256", None)
    subprocess.run(["go", "test", "-count=1", "-tags=integration,load", "-run",
                    "^(TestArchiveTutorial(Cancellation)?|TestLoadObservability)$/^round-0$/^disabled$", "./src/tests/system"], env=env, check=True)
    print("Container identity, licenses, CA certificates and file-copy fidelity verified")


if __name__ == "__main__":
    main()
