//go:build integration && load

package compat

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/inspect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArchiveTutorial(t *testing.T) {
	// Given a release bundle, or one generated from editable sources for a local run.
	directory := t.TempDir()
	bundle := os.Getenv("KAFERRY_TUTORIAL_BUNDLE")
	if bundle == "" {
		bundle = filepath.Join(directory, "archive-demo.tar.gz")
		generate := exec.Command("go", "run", "./scripts/package_examples", bundle)
		generate.Dir = "../.."
		output, err := generate.CombinedOutput()
		require.NoError(t, err, "%s", output)
	}
	unpack := exec.Command("tar", "-xzf", bundle, "-C", directory)
	output, err := unpack.CombinedOutput()
	require.NoError(t, err, "%s", output)
	directory = filepath.Join(directory, "archive-demo")
	for _, name := range []string{"copy.sql", "tutorial.toml"} {
		expected, err := os.ReadFile(filepath.Join("../../examples/archive-demo", name))
		require.NoError(t, err)
		actual, err := os.ReadFile(filepath.Join(directory, name))
		require.NoError(t, err)
		require.Equal(t, expected, actual, "download bundle must match example source: %s", name)
	}
	binary, err := filepath.Abs(testBinary(t))
	require.NoError(t, err)
	commands := t.TempDir()
	require.NoError(t, os.Symlink(binary, filepath.Join(commands, "kaferry")))
	guide, err := os.ReadFile("../../docs/usage.md")
	require.NoError(t, err)
	_, tutorial, found := strings.Cut(string(guide), "<!-- archive-tutorial-commands -->")
	require.True(t, found, "tutorial commands must be marked in the guide")
	_, tutorial, found = strings.Cut(tutorial, "```sh\n")
	require.True(t, found)
	script, following, found := strings.Cut(tutorial, "```")
	require.True(t, found)
	_, following, found = strings.Cut(following, "```json\n")
	require.True(t, found, "tutorial must show a status example")
	example, _, found := strings.Cut(following, "```")
	require.True(t, found)
	var expected inspect.StatusReport
	require.NoError(t, json.Unmarshal([]byte(example), &expected))
	require.Len(t, expected.Partitions, 1)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	command := tutorialShell(ctx, script)
	command.Dir = directory
	command.Env = append(os.Environ(), "PATH="+commands+string(os.PathListSeparator)+os.Getenv("PATH"))
	// When executing the guide's exact commands, including status and the same-group rerun.
	output, err = command.CombinedOutput()
	require.NoError(t, err, "%s", output)
	// Then the known input is copied once, saved status matches the guide, and rerunning adds nothing.
	first, err := os.ReadFile(filepath.Join(directory, "first-run.log"))
	require.NoError(t, err)
	summary := summaryFrom(t, string(first))
	require.Equal(t, 3, summary.Submitted)
	require.Equal(t, 3, summary.Confirmed)
	rerun, err := os.ReadFile(filepath.Join(directory, "rerun.log"))
	require.NoError(t, err)
	summary = summaryFrom(t, string(rerun))
	require.Zero(t, summary.Submitted)
	require.Zero(t, summary.Confirmed)
	data, err := os.ReadFile(filepath.Join(directory, "status.json"))
	require.NoError(t, err)
	var status inspect.StatusReport
	require.NoError(t, json.Unmarshal(data, &status))
	require.Equal(t, expected.Verdict, status.Verdict)
	require.Equal(t, expected.Reason, status.Reason)
	require.Len(t, status.Partitions, 1)
	require.Equal(t, expected.Partitions[0].Position, status.Partitions[0].Position)
	require.Equal(t, expected.Partitions[0].Stop, status.Partitions[0].Stop)
	input := archiveRecords(t, filepath.Join(directory, "input"))
	require.Len(t, input, 3)
	values, err := os.ReadFile("../../examples/archive-demo/records.json")
	require.NoError(t, err)
	var expectedValues []string
	require.NoError(t, json.Unmarshal(values, &expectedValues))
	require.Len(t, expectedValues, 3)
	for i, value := range expectedValues {
		record := input[i]
		require.Equal(t, []byte(value), record.Value)
		require.EqualValues(t, i, record.Source.Offset)
	}
	require.Equal(t, input, archiveRecords(t, filepath.Join(directory, "output")), "rerun must preserve exactly the three original records")
}

func TestArchiveTutorialCancellation(t *testing.T) {
	if pidFile := os.Getenv("KAFERRY_TUTORIAL_HANG_PID"); pidFile != "" {
		// The child holds a lock and the inherited output pipes while it hangs.
		lock, err := os.OpenFile(pidFile+".lock", os.O_CREATE|os.O_RDWR, 0o600)
		require.NoError(t, err)
		defer lock.Close()
		require.NoError(t, syscall.Flock(int(lock.Fd()), syscall.LOCK_EX))
		require.NoError(t, os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0o600))
		time.Sleep(time.Minute)
		return
	}
	// Given a tutorial shell whose child holds a lock and keeps its output pipes open.
	pidFile := filepath.Join(t.TempDir(), "child.pid")
	lock, err := os.OpenFile(pidFile+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lock.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	command := tutorialShell(ctx, `"$KAFERRY_TUTORIAL_TEST_BINARY" -test.run '^TestArchiveTutorialCancellation$'; :`)
	command.Env = append(os.Environ(), "KAFERRY_TUTORIAL_HANG_PID="+pidFile, "KAFERRY_TUTORIAL_TEST_BINARY="+os.Args[0])
	process := &cliProcess{command: command, done: make(chan struct{})}
	process.start(t)
	t.Cleanup(func() {
		if !process.finished() {
			_ = syscall.Kill(-process.pid(), syscall.SIGKILL)
		}
		if errors.Is(syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB), syscall.EWOULDBLOCK) {
			data, _ := os.ReadFile(pidFile)
			if pid, err := strconv.Atoi(string(data)); err == nil {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	})
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(pidFile)
		return err == nil && len(data) > 0
	}, 5*time.Second, 10*time.Millisecond, "child must acquire its lock before cancellation")
	require.ErrorIs(t, syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB), syscall.EWOULDBLOCK)
	// When the tutorial context is cancelled, stop the shell and its child together.
	cancel()
	select {
	case <-process.done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled tutorial kept waiting for its child or output pipes")
	}
	// Then waiting is bounded and the child has exited, releasing its operating-system lock.
	require.Error(t, process.wait())
	require.EventuallyWithT(t, func(check *assert.CollectT) {
		assert.NoError(check, syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB), "cancelled tutorial left its child running")
	}, 5*time.Second, 10*time.Millisecond)
}
