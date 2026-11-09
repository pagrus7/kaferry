"""Run explicitly: python3 -m unittest tools.qualification.storage_census_images_test."""

import subprocess
import time
import unittest
import uuid

from tools.qualification.qualify import IMAGES, VERSIONS, replicated_storage_command


CENSUS_IMAGES = tuple(IMAGES[version] for version in (VERSIONS[0], VERSIONS[-1]))


FAULT = r'''
set -eu
export KAFERRY_STORAGE_ROOT=$(mktemp -d)
export KAFERRY_FIXTURE_PROJECT=kaferry-replicated
export KAFERRY_REPLICATED_RUN_ID=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
export KAFERRY_STORAGE_UID=$(id -u) KAFERRY_STORAGE_GID=$(id -g)
/bin/sh -ec "$2" storage prepare
export KAFERRY_TEST_RUN=$KAFERRY_STORAGE_ROOT/kaferry-replicated/kaferry-replicated/$KAFERRY_REPLICATED_RUN_ID
export KAFERRY_REMOVALS=$KAFERRY_STORAGE_ROOT/removals
marker=$(cat "$KAFERRY_TEST_RUN/.kaferry-owned")
target=$KAFERRY_TEST_RUN/destination-1
mkdir "$KAFERRY_STORAGE_ROOT/foreign" "$KAFERRY_STORAGE_ROOT/tools"
printf 'external payload must stay private' > "$KAFERRY_STORAGE_ROOT/foreign/external-secret"
printf 'broker payload must stay private' > "$target/data"
ln "$target/data" "$target/hard-link"
ln -s "$KAFERRY_STORAGE_ROOT/foreign" "$target/link"
mkfifo "$target/pipe"
mkdir -p "$target/one/two/three/four"
touch "$target/one/two/three/four/hidden-depth-five"
cat > "$KAFERRY_STORAGE_ROOT/tools/rm" <<'SH'
#!/bin/sh
for target do :; done
if [ "$target" = "$KAFERRY_TEST_RUN/destination-1" ]; then
    echo attempt >> "$KAFERRY_REMOVALS"
    echo 'injected first deletion failure' >&2
    exit 7
fi
exec /bin/rm "$@"
SH
chmod 700 "$KAFERRY_STORAGE_ROOT/tools/rm"
case "$KAFERRY_CENSUS_CASE" in
    truncated)
        for n in $(seq 1 900); do
            touch "$target/entry-$n-abcdefghijklmnopqrstuvwxyz-abcdefghijklmnopqrstuvwxyz"
        done ;;
    timeout)
        printf '#!/bin/sh\nsleep 10\nexec /usr/bin/find "$@"\n' > "$KAFERRY_STORAGE_ROOT/tools/find"
        chmod 700 "$KAFERRY_STORAGE_ROOT/tools/find" ;;
esac
export PATH=$KAFERRY_STORAGE_ROOT/tools:$PATH
set +e
/bin/sh "$@" 2> "$KAFERRY_STORAGE_ROOT/diagnostic"
status=$?
set -e
cat "$KAFERRY_STORAGE_ROOT/diagnostic" >&2
test "$status" -eq 7
test "$(wc -l < "$KAFERRY_REMOVALS")" -eq 1
test "$(cat "$KAFERRY_TEST_RUN/.kaferry-owned")" = "$marker"
test "$(cat "$target/data")" = 'broker payload must stay private'
test "$(cat "$KAFERRY_STORAGE_ROOT/foreign/external-secret")" = 'external payload must stay private'
test -d "$KAFERRY_TEST_RUN/source-3"
echo 'verified: one deletion attempt, original status 7, ownership and data preserved' >&2
exit "$status"
'''


class StorageCensusImagesTest(unittest.TestCase):
    def cleanup(self, image, case):
        # Given fresh container-local owned storage and the actual rendered helper.
        rendered = replicated_storage_command([], 'clean')
        self.assertEqual(['-ec', rendered[-3], 'storage', 'clean'], rendered[-4:])
        name = 'kaferry-census-' + uuid.uuid4().hex
        command = ['docker', 'run', '--rm', '--pull=never', '--name', name,
                   '--network', 'none', '--cpus', '1', '--memory', '256m',
                   '--env', 'KAFERRY_CENSUS_CASE=' + case, '--entrypoint', '/bin/sh',
                   image, '-ec', FAULT, 'fixture', *rendered[-4:]]
        started = time.monotonic()
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=15)
        except subprocess.TimeoutExpired:
            subprocess.run(['docker', 'rm', '--force', name], capture_output=True, timeout=10)
            raise
        # When its first data deletion fails, then diagnostics cannot change that failure.
        self.assertEqual(7, result.returncode, result.stderr)
        self.assertEqual('', result.stdout)
        self.assertIn('verified: one deletion attempt, original status 7, ownership and data preserved', result.stderr)
        self.assertNotIn('payload must stay private', result.stderr)
        self.assertNotIn('external-secret', result.stderr)
        self.assertNotIn('hidden-depth-five', result.stderr)
        self.assertLess(len(result.stderr.encode()), 66560)
        return result.stderr, time.monotonic() - started

    def test_pinned_default_and_newest_images_capture_metadata_without_following_links(self):
        for image in CENSUS_IMAGES:
            with self.subTest(image=image):
                error, _ = self.cleanup(image, 'metadata')
                self.assertIn('cleanup census exit=0', error)
                self.assertIn('type=directory device=', error)
                self.assertIn('type=symbolic link device=', error)
                self.assertIn('type=fifo device=', error)
                self.assertRegex(error, r'inode=\d+ bytes=\d+ links=2 path=.*?/data\n')
                self.assertIn('/one/two/three/four\n', error)

    def test_pinned_default_and_newest_images_bound_output_and_metadata_waits(self):
        for image in CENSUS_IMAGES:
            for case, expected in [('truncated', 'truncated; metadata is incomplete'),
                                   ('timeout', 'timed out; metadata is incomplete')]:
                with self.subTest(image=image, case=case):
                    error, elapsed = self.cleanup(image, case)
                    self.assertIn('cleanup census ' + expected, error)
                    self.assertLess(elapsed, 6)


if __name__ == '__main__':
    unittest.main()
