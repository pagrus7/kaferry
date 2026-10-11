# Releasing Kaferry

## Release notes

`releases/vX.Y.Z.md` is the source for the GitHub release body. Write it before tagging; the workflow reads the file from the tagged commit and fails if it is missing or has the wrong title. Keep notes concise and useful to someone installing or upgrading.

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
```

`Changes` is required. Omit other sections when unnecessary. No review transcripts, implementation history, test logs or commit lists. Link version-specific docs for detail. Mention SBOMs or attestations only if that release includes them. Separate a shipped workaround from later source fixes; never infer failure frequency from passing tests.

## Procedure

1. Inspect the intended tag's source, dependency versions and upgrade changes. Write its notes, update current known-issue docs, and validate with `python3 scripts/release_notes.py vX.Y.Z`.
2. Review the notes and run the checks appropriate to the changed behavior. See [test targets](../design/components.md#test-targets) and [release evidence](../design/components.md#release-evidence).
3. Commit the notes with the release changes. Push an annotated `vX.Y.Z` tag on the reviewed commit; the release workflow builds and publishes. Manual dispatch without publishing validates builds. Manual publishing must select that version tag.
4. Check the GitHub release body, assets and checksums after publishing. Keep notes aligned with that tag. To correct prose, edit the tracked note file, then run `gh release edit vX.Y.Z --notes-file releases/vX.Y.Z.md`; leave tags and assets unchanged.

The tracked v0.1.0/v0.2.0 notes correct existing release bodies; their historical tags remain unchanged. New releases must contain their notes in the tag itself.
