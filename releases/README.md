# Releasing Kaferry

## Release notes

`releases/vX.Y.Z.md` is the source for the GitHub release body. Write it before tagging; the workflow reads the file from the tagged commit and fails if it is missing or has the wrong title. The script omits the file's heading because GitHub supplies the release title. Keep notes concise and useful to someone installing or upgrading.

```markdown
# Kaferry vX.Y.Z

One sentence describing this release.

## Changes

- User-visible features, fixes or behavior changes.

## Upgrade

- Required actions, compatibility changes and migration risks.

## Known issues

- Affected behavior, impact, mitigation and remaining risk; link upstream issues.

## Downloads

Supported platforms, prerequisites and available verification files.

[Installation](https://github.com/pagrus7/kaferry/blob/vX.Y.Z/README.md#install) · [Changes since vA.B.C](https://github.com/pagrus7/kaferry/compare/vA.B.C...vX.Y.Z)
```

`Changes` is required. Omit other sections when unnecessary. No review transcripts, implementation history, test logs or commit lists. Link version-specific docs for detail. Mention SBOMs or attestations only if that release includes them. Separate a shipped workaround from later source fixes; never infer failure frequency from passing tests.

## Procedure

1. Inspect the intended tag's source, dependency versions and upgrade changes. Write its notes, update current known-issue docs, and validate with `python3 scripts/release_notes.py vX.Y.Z`.
2. Review the notes and run the checks appropriate to the changed behavior. See [test targets](../design/components.md#test-targets) and [release evidence](../design/components.md#release-evidence).
3. Commit the notes with the release changes. Push an annotated `vX.Y.Z` tag on the reviewed commit; the release workflow builds and publishes. Manual dispatch without publishing validates builds. Manual publishing must select that version tag.
4. Check the GitHub release body, assets and checksums after publishing. Keep notes aligned with that tag. If a tagged build fails, fix the problem and use a new version tag.

To correct published prose, edit the tracked note file, then validate before updating the body. Keep tags and assets unchanged:

```sh
release_notes_file=$(mktemp)
python3 scripts/release_notes.py vX.Y.Z > "$release_notes_file" &&
  gh release edit vX.Y.Z --notes-file "$release_notes_file"
rm "$release_notes_file"
```

The tracked v0.1.0/v0.2.0 notes correct existing release bodies; their historical tags remain unchanged. New releases must contain their notes in the tag itself.
