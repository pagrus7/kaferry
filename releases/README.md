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
3. Commit the notes with the release changes. Push an annotated `vX.Y.Z` tag on the reviewed commit. The workflow tests each extracted archive, then runs Linux Kafka, fault, load, security/failover and a 30-minute soak before publishing. Manual dispatch without publishing exercises the same gates. Manual publishing must select that version tag.
4. Check the GitHub release body, assets and checksums after publishing. Each archive must contain `kaferry`, `LICENSE` and `THIRD_PARTY_NOTICES.txt`. The workflow verifies version/commit and file copying on all four platforms; Kafka tests use the Linux AMD64 archive with its binary digest enforced. Keep notes aligned with that tag. If a tagged build fails, fix the problem and use a new version tag.

Build with `CGO_ENABLED=0` and verify all four release targets. Linux binaries must not need a dynamic linker. `scripts/notices.py` collects licenses, including subdirectory licenses, from packages linked by `cmd/kaferry` in the checkout; it fails if the binary was built with a different Go version. Run it on the checkout that built the binary and review the notices. Do not derive notices from `go.mod`, which also lists test-only dependencies.

To correct published prose, edit the tracked note file, then validate before updating the body. Keep tags and assets unchanged:

```sh
release_notes_file=$(mktemp)
python3 scripts/release_notes.py vX.Y.Z > "$release_notes_file" &&
  gh release edit vX.Y.Z --notes-file "$release_notes_file"
rm "$release_notes_file"
```
