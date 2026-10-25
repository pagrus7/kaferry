# Installing Kaferry

Choose a release binary, Homebrew, a source build or a container. After installation, check `kaferry --version`. [First copy](usage.md#first-copy-without-kafka).

## Release binaries

Download the archive for your OS and CPU from [Releases](https://github.com/pagrus7/kaferry/releases/latest). Verify its entry in `SHA256SUMS`, extract it, and put `kaferry` on `PATH`. Keep `LICENSE` and `THIRD_PARTY_NOTICES.txt` with a redistributed binary. [Provenance verification](../design/components.md#release-evidence). When supplied, `qualification.md` records the release’s tested environments and scenarios; passing tests do not cover every deployment.

## Homebrew

```sh
brew install pagrus7/tap/kaferry
kaferry --version
```

## Source installation

Requires Go 1.26+; no C compiler.

`CGO_ENABLED=0 go install github.com/pagrus7/kaferry/cmd/kaferry@latest` builds the latest tagged release with the installer's Go toolchain. Use `@vX.Y.Z` for a specific release, or `@main` for development source. `GOBIN` selects the executable directory; otherwise Go uses `$(go env GOPATH)/bin`. The supported source-build profile disables CGO. `GOFLAGS` and the selected toolchain can change the result; a locally compiled executable is not an attested release binary.

The Go module cache and `GOROOT` retain dependency and toolchain source licenses. `go install` installs only the executable, without the release archive's license sidecars. Our supported binary redistribution package is the release archive: keep `LICENSE` and `THIRD_PARTY_NOTICES.txt` with its binary. For a custom build, use the matching checkout and Go toolchain with `scripts/notices.py`; do not reuse notices from a different toolchain. This packaging policy does not change any upstream license.

From a checkout, run `make build`. From the repository root, invoke `./bin/kaferry`; to run examples in another directory, add the checkout’s absolute `bin` path to `PATH`.

## Container images

[Try the bundled Docker demo](usage.md#first-copy-with-docker) without installing Kaferry or setting up Kafka.

Images package the tested Linux release binaries without recompiling them. The container workflow downloads the published archives, verifies SHA-256 checksums and archive provenance against the exact release tag, commit and release workflow, and retains `LICENSE` and `THIRD_PARTY_NOTICES.txt` in `/usr/share/doc/kaferry`. A digest-pinned Distroless base supplies CA certificates and its own package files. The image runs as UID/GID 65532, with `HOME=/tmp` and `/work` as its working directory; bind mounts need suitable ownership or an explicit `--user`.

Verify a published digest with GitHub CLI:

```sh
gh attestation verify oci://ghcr.io/pagrus7/kaferry@sha256:DIGEST \
  --repo pagrus7/kaferry \
  --signer-workflow pagrus7/kaferry/.github/workflows/containers.yml
```

Use a registry login if needed. The image also carries SBOM/provenance records for inspection; attestations establish build origin, not application correctness. `ghcr.io/pagrus7/kaferry:latest` follows the current stable release after its image passes testing and attestation. Version tags and digests identify fixed images.

[Run with mounted files and persistent checkpoints](usage.md#containers).
