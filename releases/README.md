# Releasing Kaferry

## Release notes

`releases/vX.Y.Z.md` is the source for the GitHub release body. Write it before tagging; the workflow reads the file from the tagged commit and fails if it is missing, has the wrong title or lacks a nonempty `Changes` section. The script omits the file's heading because GitHub supplies the release title. Keep notes concise and useful to someone installing or upgrading.

```markdown
# Kaferry vX.Y.Z

One sentence describing this release.

## Changes

- User-visible features, fixes or behavior changes.

## Upgrade

- Required actions, compatibility changes and migration risks.

## Known issues

- Briefly name the affected dependency, limitation and scope; link an upstream issue when one exists. For mitigated failures, state the protection and remaining risk together.

## Downloads

Supported platforms, prerequisites and available verification files.

[Installation](https://github.com/pagrus7/kaferry/blob/vX.Y.Z/README.md#install) · [Changes since vA.B.C](https://github.com/pagrus7/kaferry/compare/vA.B.C...vX.Y.Z)
```

`Changes` is required. Omit other sections when unnecessary. No review transcripts, implementation history, test logs or commit lists. Link version-specific docs for detail. Mention SBOMs or attestations only if that release includes them. Separate a shipped workaround from later source fixes; never infer failure frequency from passing tests.

## Procedure

1. Inspect the intended tag's source, dependency versions and upgrade changes. Write its notes and validate with `python3 scripts/release_notes.py vX.Y.Z`. Update living known-issue docs with current behavior, without release chronology; do not copy the release's change narrative into them.
2. Reread changed docs, comments and diagnostics as a user of the current version. Check each claim against the tagged implementation; remove repetition, investigation history and unnecessary detail. Include all three local reviews required by [AGENTS.md](../AGENTS.md) in the PR body. Run the checks appropriate to the changed behavior. See [test targets](../design/components.md#test-targets) and [release evidence](../design/components.md#release-evidence).
3. Commit the notes with the release changes. Before tagging, dispatch the release workflow on the candidate branch with publishing disabled. Check that the completed run succeeded and its `headSha` equals the full reviewed commit SHA, including the notes. A version tag makes the source available to `go install` before archive publication; do not tag a candidate that has not passed this validation. Push an annotated `vX.Y.Z` tag on that exact commit. The workflow tests each extracted archive, then runs Linux Kafka, fault, load, security/failover and a 30-minute soak before publishing. Manual dispatch without publishing exercises the same gates. Manual publishing must select that version tag.
4. Check the GitHub release body, assets and checksums after publishing. Each archive must contain `kaferry`, `LICENSE` and `THIRD_PARTY_NOTICES.txt`. The workflow verifies version/commit and file copying on all four platforms. CLI tests use the Linux AMD64 archive and verify its binary digest; component and in-process application tests execute checkout code. Keep notes aligned with that tag. If a tagged build fails, fix the problem and use a new version tag.

Build with `CGO_ENABLED=0` and verify all four release targets. Linux binaries must not need a dynamic linker. `scripts/notices.py` collects licenses, including subdirectory licenses, from packages linked by `cmd/kaferry` in the checkout; it fails if the binary was built with a different Go version. Run it on the checkout that built the binary and review the notices. Do not derive notices from `go.mod`, which also lists test-only dependencies.

To correct published prose, edit the tracked note file, then validate before updating the body. Keep tags and assets unchanged:

```sh
release_notes_file=$(mktemp)
python3 scripts/release_notes.py vX.Y.Z > "$release_notes_file" &&
  gh release edit vX.Y.Z --notes-file "$release_notes_file"
rm "$release_notes_file"
```

## Source installation

`CGO_ENABLED=0 go install github.com/pagrus7/kaferry/cmd/kaferry@latest` builds the latest tagged release with the installer's Go toolchain. Use `@vX.Y.Z` for a specific release, or `@main` for development source. `GOBIN` selects the executable directory; otherwise Go uses `$(go env GOPATH)/bin`. The supported source-build profile disables CGO. `GOFLAGS` and the selected toolchain can change the result; a locally compiled executable is not an attested release binary.

The Go module cache and `GOROOT` retain dependency and toolchain source licenses. `go install` installs only the executable, without the release archive's license sidecars. Our supported binary redistribution package is the release archive: keep `LICENSE` and `THIRD_PARTY_NOTICES.txt` with its binary. For a custom build, use the matching checkout and Go toolchain with `scripts/notices.py`; do not reuse notices from a different toolchain. This packaging policy does not change any upstream license.

CI installs synthetic tagged and pseudo-versioned module archives through a local proxy and checks the resulting executable's identity. It does not publish a tag. Module installs that include version metadata report that version; a commit is `unknown` when Go supplies no VCS metadata. The source-install check requires cached dependency archives and access to Go's checksum database. It authenticates published dependencies through that database and exempts only the synthetic Kaferry module.

## Container images

Images package the tested Linux release binaries without recompiling them. The container workflow downloads the published archives, verifies SHA-256 checksums and archive provenance against the exact release tag, commit and release workflow, and retains `LICENSE` and `THIRD_PARTY_NOTICES.txt` in `/usr/share/doc/kaferry`. A digest-pinned Distroless base supplies CA certificates and its own package files. The image runs as UID/GID 65532, with `HOME=/tmp` and `/work` as its working directory; bind mounts need suitable ownership or an explicit `--user`.

The workflow builds one multi-platform OCI archive containing image-specific SPDX SBOM and BuildKit provenance. Native AMD64/ARM64 jobs load that archive without rebuilding, compare its binary and license bytes with the inputs, exercise file copying, and check Kafka checkpointing/resume after Docker sends SIGTERM. Only after both jobs pass can the publish job copy the same OCI archive to GHCR and attest its image-index digest. Version tags are immutable: publishing a different digest under an existing version fails. Retry a failed publish job with its retained artifact; do not rebuild a published version.

The release workflow calls image publishing after the GitHub release is published. Manual dispatch of `containers.yml` takes an existing stable release tag and defaults to validation without publishing. Publishing needs the repository's package-write and attestation permissions. A newly created GHCR package is private by default; set its visibility to public in package settings before advertising pulls. This changes package visibility, not the image bytes.

Verify a published digest with GitHub CLI:

```sh
gh attestation verify oci://ghcr.io/pagrus7/kaferry@sha256:DIGEST \
  --repo pagrus7/kaferry \
  --signer-workflow pagrus7/kaferry/.github/workflows/containers.yml
```

Use a registry login if needed. The image also carries SBOM/provenance records for inspection; attestations establish build origin, not application correctness. No `latest` tag is published.
