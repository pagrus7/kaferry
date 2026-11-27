# Releasing Kaferry

## Release notes

Write concise notes for installers and upgraders in `docs/releases/unreleased.md`. The workflow uses the tagged commit’s copy as the initial release body, stripping the heading because GitHub supplies the title. GitHub Releases holds the published notes.

```markdown
# Kaferry unreleased

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

`Changes` must be nonempty. Omit other sections when unnecessary. No review transcripts, implementation history, test logs or commit lists. Link version-specific docs for detail. Mention SBOMs or attestations only if that release includes them. Separate a shipped workaround from later source fixes; never infer failure frequency from passing tests.

## Example download

Each platform build generates `archive-demo.tar.gz` from the SQL, TOML and JSON under `examples/archive-demo`, using the archive encoder in the tagged source. The packaged-binary smoke test runs the tutorial against that generated bundle. After the test passes, the Linux AMD64 job retains its exact bundle for publication with provenance and `SHA256SUMS`; the publish job does not regenerate it. Generated Avro data and download archives are not stored in Git.

## Qualification summary

The full broker/authentication matrix also produces checksummed `compatibility.json` and `compatibility.md` assets. Publication requires their source and binary identities to match the Linux AMD64 archive. [Standalone qualification commands](../design/qualification.md).

Publishing saves `qualification.md` as a checksummed release asset. It identifies the tagged source, tested Linux AMD64 binary, fixture environments, major scenarios, suite results and configured soak duration. The generator rejects missing, failed or mismatched source/binary evidence. Per-platform build information and attestations remain separate assets. The summary describes the exercised configurations; detailed workflow artifacts have limited retention.

`release-results.md` and `release-results.json` retain the compatibility matrix, observed throughput and every load/soak measurement with its environment. These assets are generated from passing evidence for the exact Linux AMD64 binary before checksums and publication. [Measurement scope and comparisons](../release-results.md).

To supplement an existing release, download and verify the archive, `compatibility.json` and original `qualification-load`/`qualification-soak` workflow artifacts. Extract both artifact directories under `EVIDENCE`, then generate and review the reports:

```sh
python3 -m tools.release.release_results EVIDENCE LINUX_AMD64_ARCHIVE COMPATIBILITY_JSON FULL_COMMIT vX.Y.Z OUTPUT
gh release upload vX.Y.Z OUTPUT/release-results.*
```

Upload the three files without `--clobber`. Their `release-results.SHA256SUMS` verifies the supplemental files; existing tags, binaries and `SHA256SUMS` stay unchanged. [Artifact retention](../release-results.md#throughput-measurements).

## Build and validation

The workflow builds Linux AMD64/ARM64 and macOS AMD64/ARM64 binaries with `CGO_ENABLED=0`. Linux binaries must not need a dynamic linker. Each platform archive contains `kaferry`, `LICENSE` and `THIRD_PARTY_NOTICES.txt`; separate assets provide SPDX SBOMs, archive provenance and binary SBOM attestations.

Each extracted archive passes version/commit and file-copy checks. Linux Kafka, fault, load, security/failover and a 30-minute soak must pass before publishing. CLI tests use the Linux AMD64 archive and verify its binary digest; component and in-process application tests execute checkout code.

[Notices generation](../../tools/release/notices.py) collects licenses for linked Go packages, including subdirectory licenses, and the Go runtime; it fails if required licenses are missing or the binary was built with a different Go version. Run it on the checkout that built the binary and review the notices. Do not derive notices from `go.mod`, which also lists test-only dependencies.

## Procedure

1. Check the candidate's source, dependencies and upgrade steps. Finish `unreleased.md`, then validate with `python3 -m tools.release.release_notes vX.Y.Z`. Keep living known-issue docs focused on current behavior.
2. Follow the [review process](../development/reviews/README.md) for required reviews, formatting-only exceptions and PR statuses. Validate changed claims against the tagged implementation and run the appropriate [checks](../../CONTRIBUTING.md#test-targets).
3. Commit the notes with the release changes. Before tagging, dispatch the release workflow on the candidate branch with publishing disabled and `candidate_version` set to the intended `vX.Y.Z`. This validates the authored notes and embeds the intended version using the publishing build flags. Check that the completed run succeeded and its `headSha` equals the full reviewed commit SHA, including the notes. Manual dispatch defaults to validation without publishing; omitting `candidate_version` uses `validation-COMMIT` instead. The pinned Go toolchain derives main-module version metadata from Git tags, so adding the version tag can change the binary digest even with the same source and explicit version/revision flags. Validation artifacts do not establish qualification of the published binary.
4. Push an annotated `vX.Y.Z` tag on that exact validated commit. A version tag makes the source available to `go install` before archive publication; do not tag a candidate that has not passed validation. The workflow publishes a GitHub prerelease without changing the latest release. Manual publishing does the same and must select that version tag; the workflow verifies that the built commit matches it and rejects a different supplied candidate version.
5. Check the prerelease body, assets and checksums. Verify the [platform archive contents](#build-and-validation) and that `archive-demo.tar.gz` contains the tutorial SQL, configuration and generated input. Complete [lab preparation](../design/qualification.md#prepare) from a clean checkout of the tag. Download the published assets and verify the archive's provenance against the reviewed commit before extracting it. Below, replace `PLATFORM` with your native platform and `FULL_REVIEWED_COMMIT_SHA` with that commit; use a new `DOWNLOAD` directory. Complete the release's required qualification using this extracted executable through `--binary`, retaining its SHA-256 in the evidence. The example runs the full compatibility matrix; other required qualification scenarios must also pass.

   ```sh
   gh release download vX.Y.Z --repo pagrus7/kaferry --dir DOWNLOAD &&
   (cd DOWNLOAD && shasum -a 256 -c SHA256SUMS) &&
   gh attestation verify DOWNLOAD/kaferry-vX.Y.Z-PLATFORM.tar.gz \
     --repo pagrus7/kaferry \
     --signer-workflow pagrus7/kaferry/.github/workflows/release.yml \
     --source-ref refs/tags/vX.Y.Z --source-digest FULL_REVIEWED_COMMIT_SHA \
     --deny-self-hosted-runners &&
   tar -xzf DOWNLOAD/kaferry-vX.Y.Z-PLATFORM.tar.gz -C DOWNLOAD &&
   shasum -a 256 DOWNLOAD/kaferry &&
   python3 -m tools.qualification.qualify matrix --tier full \
     --binary "$PWD/DOWNLOAD/kaferry" --output build-results/published-matrix
   ```

6. After the published executable passes all required qualification, promote it with `gh release edit vX.Y.Z --prerelease=false --latest`. Dispatch [container publishing](#container-publishing) for that stable tag. If the tagged build or qualification fails, fix it under a new patch tag. Never move tags or replace published binaries.
7. In the first PR after promotion, reset `unreleased.md` to `# Kaferry unreleased`, a blank line and an empty `## Changes` section.

For corrections, new known issues or later test results for the released binary, edit the current GitHub release body.

```sh
gh release view vX.Y.Z --json body --jq .body > notes.md &&
  vi notes.md &&
  gh release edit vX.Y.Z --notes-file notes.md
```

## Source-installation checks

[User installation and redistribution instructions](../install.md).

CI installs synthetic tagged and pseudo-versioned module archives through a local proxy and checks the resulting executable's identity. It does not publish a tag. The CLI and metrics prefer linker-supplied values. Otherwise they report the embedded module version (including pseudo-versions), or `dev` for an absent or `(devel)` version. The commit comes from embedded VCS metadata, with `-dirty` for a modified checkout, or `unknown` when unavailable. The source-install check requires cached dependency archives and access to Go's checksum database. It authenticates published dependencies through that database and exempts only the synthetic Kaferry module.

## Container publishing

The CLI image includes the release's verified `archive-demo.tar.gz` contents under `/demo`, with an empty checkpoint directory owned by the image user. A fresh volume mounted at `/demo` receives these files from Docker. Candidate CLI images generate the same bundle from the checkout. Each native CLI image test runs the bundled copy and checks saved completion and a zero-record rerun across separate containers.

The workflow builds separate multi-platform OCI archives for `kaferry` and `kaferry-demo`, each containing image-specific SPDX SBOM and BuildKit provenance. The demo adds a digest-pinned Apache Kafka image and an entrypoint that starts an isolated broker, seeds records, runs the packaged Kaferry binary, and verifies destination values and record count. It has a three-minute deadline and ten-second forced-shutdown allowance.

Native AMD64/ARM64 jobs load both archives without rebuilding and compare their binary and license bytes with the inputs. CLI tests exercise file copying and Kafka checkpointing/resume after SIGTERM. The [smoke helper](../../tools/release/container_smoke.py) runs demo tests with `--demo`: four independent containers run concurrently for two fresh runs without external networking, rejection of corrupted destination output, and cancellation while the broker is running. Every failure is reported after all four checks finish. Only after both native jobs pass can publishing copy the same OCI archives to GHCR and attest their image-index digests. Version tags are immutable: publishing a different digest under an existing version fails. After attestation, a serialized publish job updates `latest` only when its version matches the current stable GitHub release. Retry a failed publish job with its retained artifacts; do not rebuild a published version.

Container publication is a separate manual dispatch after the qualified GitHub prerelease is promoted to stable. `containers.yml` takes an existing stable release tag and defaults to validation without publishing. Dispatch it with publishing enabled to run the native checks above and then publish those tested OCI archives:

```sh
gh workflow run containers.yml --ref main -f tag=vX.Y.Z -f publish=true
```

Publishing needs the repository's package-write and attestation permissions. A newly created GHCR package is private by default; set its visibility to public in package settings before advertising pulls. This changes package visibility, not the image bytes.
