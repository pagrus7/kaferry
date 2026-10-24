"""Install the checkout through a local Go module proxy, without publishing a tag."""

import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import zipfile


MODULE = "github.com/pagrus7/kaferry"
VERSIONS = ("v0.0.0-install-smoke", "v0.0.0-20000101000000-0123456789ab")


def main():
    root = Path(__file__).resolve().parent.parent
    cache = Path(subprocess.check_output(["go", "env", "GOMODCACHE"], text=True).strip())
    go_version = subprocess.check_output(["go", "env", "GOVERSION"], text=True).strip()
    files = subprocess.check_output(["git", "ls-files", "-z", "--", "cmd", "internal", "go.mod", "go.sum", "LICENSE"], cwd=root).decode().split("\0")
    with tempfile.TemporaryDirectory(prefix="kaferry-install-") as name:
        work = Path(name).resolve()
        proxy = work / "proxy"
        versions = proxy / MODULE / "@v"
        versions.mkdir(parents=True)
        (versions / "list").write_text("\n".join(VERSIONS) + "\n")
        for version in VERSIONS:
            (versions / f"{version}.mod").write_bytes((root / "go.mod").read_bytes())
            (versions / f"{version}.info").write_text(json.dumps({"Version": version, "Time": "2000-01-01T00:00:00Z"}))
            with zipfile.ZipFile(versions / f"{version}.zip", "w", zipfile.ZIP_DEFLATED) as archive:
                for file in filter(None, files):
                    archive.write(root / file, f"{MODULE}@{version}/{file}")
        env = dict(os.environ, CGO_ENABLED="0", GOWORK="off", GOFLAGS="", GOBIN=str(work / "bin"),
                   GOMODCACHE=str(work / "modules"), GOSUMDB="off",
                   GOPROXY=proxy.as_uri() + "," + (cache / "cache/download").as_uri())
        # Only this synthetic local proxy test disables sumdb; dependency hashes stay in go.sum.
        for version in VERSIONS:
            subprocess.run(["go", "install", f"{MODULE}/cmd/kaferry@{version}"], env=env, cwd=work, check=True)
            output = subprocess.check_output([str(work / "bin/kaferry"), "--version"], text=True)
            expected = rf"kaferry {re.escape(version)} \(commit unknown; {re.escape(go_version)}; franz-go v[^ )]+\)\n"
            if not re.fullmatch(expected, output):
                raise ValueError(f"installed binary has unexpected identity: {output!r}")
            print(f"Installed module identity verified: {version}")


if __name__ == "__main__":
    main()
