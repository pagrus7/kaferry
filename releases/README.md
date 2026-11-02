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

## Example download

Each platform build generates `archive-demo.tar.gz` from the SQL, TOML and JSON under `examples/archive-demo`, using the archive encoder in the tagged source. The packaged-binary smoke test runs the tutorial against that generated bundle. After the test passes, the Linux AMD64 job retains its exact bundle for publication with provenance and `SHA256SUMS`; the publish job does not regenerate it. Generated Avro data and download archives are not stored in Git.

## Qualification summary

The full broker/authentication matrix also produces checksummed `compatibility.json` and `compatibility.md` assets. Publication requires their source and binary identities to match the Linux AMD64 archive. [Standalone qualification commands](../design/qualification.md).

Publishing saves `qualification.md` as a checksummed release asset. It identifies the tagged source, tested Linux AMD64 binary, fixture environments, major scenarios, suite results and configured soak duration. The generator rejects missing, failed or mismatched source/binary evidence. Per-platform build information and attestations remain separate assets. The summary describes the exercised configurations; detailed workflow artifacts expire after 30 days.

## Build and validation

The workflow builds Linux AMD64/ARM64 and macOS AMD64/ARM64 binaries with `CGO_ENABLED=0`. Linux binaries must not need a dynamic linker. Each platform archive contains `kaferry`, `LICENSE` and `THIRD_PARTY_NOTICES.txt`; separate assets provide SPDX SBOMs, archive provenance and binary SBOM attestations.

Each extracted archive passes version/commit and file-copy checks. Linux Kafka, fault, load, security/failover and a 30-minute soak must pass before publishing. CLI tests use the Linux AMD64 archive and verify its binary digest; component and in-process application tests execute checkout code.

[Notices generation](../scripts/notices.py) collects licenses for linked Go packages, including subdirectory licenses, and the Go runtime; it fails if required licenses are missing or the binary was built with a different Go version. Run it on the checkout that built the binary and review the notices. Do not derive notices from `go.mod`, which also lists test-only dependencies.

## Procedure

1. Inspect the intended tag's source, dependency versions and upgrade changes. Write its notes and validate with `python3 scripts/release_notes.py vX.Y.Z`. Update living known-issue docs with current behavior, without release chronology; do not copy the release's change narrative into them.
2. Reread changed docs, comments and diagnostics as a user of the current version. Check each claim against the tagged implementation; remove repetition, investigation history and unnecessary detail. Include all three local reviews required by [AGENTS.md](../AGENTS.md) in the PR body. Run the checks appropriate to the changed behavior. See [test targets](../design/components.md#test-targets) and [release evidence](../design/components.md#release-evidence).
3. Commit the notes with the release changes. Before tagging, dispatch the release workflow on the candidate branch with publishing disabled. Check that the completed run succeeded and its `headSha` equals the full reviewed commit SHA, including the notes. A version tag makes the source available to `go install` before archive publication; do not tag a candidate that has not passed this validation. Push an annotated `vX.Y.Z` tag on that exact commit. Manual dispatch defaults to validation without publishing and exercises the same gates. Manual publishing must select that version tag; the workflow verifies that the built commit matches it.
4. Check the GitHub release body, assets and checksums after publishing. Verify the [platform archive contents](#build-and-validation) and that `archive-demo.tar.gz` contains the tutorial SQL, configuration and generated input. Keep notes aligned with that tag. If a tagged build fails, fix the problem and use a new version tag.

To correct published prose, edit the tracked note file, then validate before updating the body. Keep tags and assets unchanged:

```sh
release_notes_file=$(mktemp)
python3 scripts/release_notes.py vX.Y.Z > "$release_notes_file" &&
  gh release edit vX.Y.Z --notes-file "$release_notes_file"
rm "$release_notes_file"
```

## Source-installation checks

[User installation and redistribution instructions](../docs/install.md).

CI installs synthetic tagged and pseudo-versioned module archives through a local proxy and checks the resulting executable's identity. It does not publish a tag. Module installs that include version metadata report that version; a commit is `unknown` when Go supplies no VCS metadata. The source-install check requires cached dependency archives and access to Go's checksum database. It authenticates published dependencies through that database and exempts only the synthetic Kaferry module.

## Container publishing

Images include the release's verified `archive-demo.tar.gz` contents under `/demo`, with an empty checkpoint directory owned by the image user. A fresh volume mounted at `/demo` receives these files from Docker. Candidate images generate the same bundle from the checkout. Each native image test runs the bundled copy and checks saved completion and a zero-record rerun across separate containers.

The workflow builds one multi-platform OCI archive containing image-specific SPDX SBOM and BuildKit provenance. Native AMD64/ARM64 jobs load that archive without rebuilding, compare its binary and license bytes with the inputs, exercise file copying, and check Kafka checkpointing/resume after Docker sends SIGTERM. Only after both jobs pass can the publish job copy the same OCI archive to GHCR and attest its image-index digest. Version tags are immutable: publishing a different digest under an existing version fails. After attestation, a serialized publish job updates `latest` only when its version matches the current stable GitHub release. Retry a failed publish job with its retained artifact; do not rebuild a published version.

The release workflow calls image publishing after the GitHub release is published. Manual dispatch of `containers.yml` takes an existing stable release tag and defaults to validation without publishing. Publishing needs the repository's package-write and attestation permissions. A newly created GHCR package is private by default; set its visibility to public in package settings before advertising pulls. This changes package visibility, not the image bytes.
