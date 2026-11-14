# Installing Kaferry

Choose a release binary, Homebrew, a source build or a container. After installation, check `kaferry --version`. [First copy](usage.md#local-kafka-playground).

## Release binaries

Download the archive for your OS and CPU from [Releases](https://github.com/pagrus7/kaferry/releases/latest). Verify its entry in `SHA256SUMS`, extract it, and put `kaferry` on `PATH`. Keep `LICENSE` and `THIRD_PARTY_NOTICES.txt` with a redistributed binary. When supplied, `qualification.md` records the release’s tested environments and scenarios; passing tests do not cover every deployment.

When the release provides attestations, verify the archive's build origin with GitHub CLI:

```sh
gh attestation verify kaferry-VERSION-PLATFORM.tar.gz --repo pagrus7/kaferry
```

## Homebrew

```sh
brew install pagrus7/tap/kaferry
kaferry --version
```

## Source installation

Requires Go 1.27+; no C compiler.

`CGO_ENABLED=0 go install github.com/pagrus7/kaferry/src/cmd/kaferry@latest` builds the latest tagged release with the installer's Go toolchain. Use `@vX.Y.Z` for a specific published release. Tags v0.12.0 and older use `github.com/pagrus7/kaferry/cmd/kaferry@vX.Y.Z`. For development source, use `CGO_ENABLED=0 go install github.com/pagrus7/kaferry/src/cmd/kaferry@main`. `GOBIN` selects the executable directory; otherwise Go uses `$(go env GOPATH)/bin`. The supported source-build profile disables CGO. `GOFLAGS` and the selected toolchain can change the result; a locally compiled executable is not an attested release binary.

The Go module cache and `GOROOT` retain dependency and toolchain source licenses. `go install` installs only the executable, without the release archive's license sidecars. Our supported binary redistribution package is the release archive: keep `LICENSE` and `THIRD_PARTY_NOTICES.txt` with its binary. For a custom build, use the matching checkout and Go toolchain with `tools/release/notices.py`; do not reuse notices from a different toolchain. This packaging policy does not change any upstream license.

From a checkout, run `make build`. From the repository root, invoke `./bin/kaferry`; to run examples in another directory, add the checkout’s absolute `bin` path to `PATH`.

## Container images

[Try the disposable Kafka demo](../README.md#docker-quick-start) without installing Kaferry or preparing files. `ghcr.io/pagrus7/kaferry-demo` includes Apache Kafka and the release's Kaferry binary, runs as Kafka's non-root `appuser`, and verifies a three-record topic-to-topic copy. Its broker listens only on container loopback. The demo has a three-minute deadline plus up to ten seconds for forced shutdown; it requires more download space and memory than the CLI image. Ctrl+C cancels the demo. With `--rm`, the container and its data are removed after exit; Docker retains the image cache.

Both images support Linux AMD64/ARM64 and package tested release binaries without recompiling them. The container workflow downloads the published archives, verifies SHA-256 checksums and archive provenance against the exact release tag, commit and release workflow, and retains `LICENSE` and `THIRD_PARTY_NOTICES.txt` in `/usr/share/doc/kaferry`.

Use `ghcr.io/pagrus7/kaferry` for your own jobs. Its digest-pinned Distroless base supplies CA certificates and its own package files. This CLI image runs as UID/GID 65532, with `HOME=/tmp` and `/work` as its working directory; bind mounts need suitable ownership or an explicit `--user`. It also includes the [archive tutorial](usage.md#first-copy-with-docker).

Verify a published digest with GitHub CLI:

```sh
gh attestation verify oci://ghcr.io/pagrus7/kaferry@sha256:DIGEST \
  --repo pagrus7/kaferry \
  --signer-workflow pagrus7/kaferry/.github/workflows/containers.yml
```

For the demo image, replace `kaferry` with `kaferry-demo` in the image reference. Use a registry login if needed. Both images carry SBOM/provenance records for inspection; attestations establish build origin, not application correctness. Their `latest` tags follow the current stable release after testing and attestation. Version tags and digests identify fixed images.

[Run with mounted files and persistent checkpoints](usage.md#containers).
