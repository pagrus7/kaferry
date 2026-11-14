//go:build integration && container

package compat

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	core "github.com/pagrus7/kaferry/src/internal/copy"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func markedContainerShell(document, marker string) (string, error) {
	marker = "<!-- " + marker + " -->\n```sh\n"
	if strings.Count(document, marker) != 1 {
		return "", fmt.Errorf("expected one shell block marked %q", marker)
	}
	_, rest, _ := strings.Cut(document, marker)
	script, _, found := strings.Cut(rest, "```")
	if !found || strings.TrimSpace(script) == "" {
		return "", fmt.Errorf("missing shell block end or command")
	}
	return script, nil
}

func readmeContainerCommand(document string) (string, error) {
	script, err := markedContainerShell(document, "container-run")
	if err != nil {
		return "", err
	}
	for _, required := range []string{
		"docker run --rm --stop-timeout 35 ",
		`--user "$(id -u):$(id -g)"`,
		`--mount "type=bind,src=$PWD,dst=/work"`,
		"ghcr.io/pagrus7/kaferry:latest",
		"run -c /work/connections.toml -f /work/copy.sql",
	} {
		if strings.Count(script, required) != 1 {
			return "", fmt.Errorf("container command must include %q exactly once", required)
		}
	}
	return script, nil
}

func testContainerCommand(t *testing.T) {
	// Given the authoritative command and mutations that remove required behavior.
	readme, err := os.ReadFile("../../../README.md")
	require.NoError(t, err)
	document := string(readme)
	command, err := readmeContainerCommand(document)
	require.NoError(t, err)
	require.Contains(t, command, "docker run")
	for _, removed := range []string{
		`--user "$(id -u):$(id -g)"`,
		`--mount "type=bind,src=$PWD,dst=/work"`,
		"<!-- container-run -->",
		"--stop-timeout 35",
		"run -c /work/connections.toml -f /work/copy.sql",
	} {
		t.Run(removed, func(t *testing.T) {
			require.Contains(t, document, removed)
			// When the extracted example loses a required flag or its marker.
			_, err := readmeContainerCommand(strings.Replace(document, removed, "", 1))
			// Then extraction fails instead of silently testing a weaker command.
			require.Error(t, err)
		})
	}
	_, err = readmeContainerCommand(document + document)
	require.Error(t, err, "duplicate markers must not select a stale command")
	_, err = readmeContainerCommand("<!-- container-run -->\n```sh\ndocker run")
	require.Error(t, err, "an unterminated block must not execute")
}

// Match the unique bind source without adding --name or --cidfile to the example.
func mountedContainers(ctx context.Context, directory string) ([]string, error) {
	data, err := exec.CommandContext(ctx, "docker", "ps", "--all", "--quiet", "--no-trunc", "--filter", "volume=/work").CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("list containers: %w: %s", err, data)
	}
	ids := strings.Fields(string(data))
	if len(ids) == 0 {
		return nil, nil
	}
	data, err = exec.CommandContext(ctx, "docker", append([]string{"inspect"}, ids...)...).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("inspect containers: %w: %s", err, data)
	}
	var containers []struct {
		ID     string
		Mounts []struct{ Type, Source, Destination string }
	}
	if err := json.Unmarshal(data, &containers); err != nil {
		return nil, err
	}
	var matched []string
	for _, container := range containers {
		for _, mount := range container.Mounts {
			if mount.Type == "bind" && mount.Source == directory && mount.Destination == "/work" {
				matched = append(matched, container.ID)
			}
		}
	}
	return matched, nil
}

func TestContainerShutdownAndResume(t *testing.T) {
	t.Run("command-contract", testContainerCommand)
	// The contract subtest runs without Docker or an image when selected alone.
	t.Run("copies", func(t *testing.T) {
		// Given the exact playground setup and README command in a writable host directory.
		ctx, cancel := context.WithTimeout(t.Context(), 150*time.Second)
		defer cancel()
		image := os.Getenv("KAFERRY_TEST_IMAGE")
		require.Regexp(t, `^[a-zA-Z0-9][a-zA-Z0-9_.:/@-]*$`, image, "set KAFERRY_TEST_IMAGE to the tested image")
		listener, err := net.Listen("tcp4", "127.0.0.1:9092")
		require.NoError(t, err, "the documented playground needs port 9092")
		require.NoError(t, listener.Close())
		names, err := exec.CommandContext(ctx, "docker", "ps", "-aq", "--filter", "name=^/kaferry-kafka-demo$").CombinedOutput()
		require.NoError(t, err, "%s", names)
		require.Empty(t, strings.TrimSpace(string(names)), "playground name is already in use")
		guide, err := os.ReadFile("../../../docs/usage.md")
		require.NoError(t, err)
		setup, err := markedContainerShell(string(guide), "kafka-quickstart-setup")
		require.NoError(t, err)
		data, setupErr := tutorialShell(ctx, setup).CombinedOutput()
		brokerID := regexp.MustCompile(`(?m)^[a-f0-9]{64}$`).FindString(string(data))
		t.Cleanup(func() {
			if brokerID != "" {
				cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				out, err := exec.CommandContext(cleanup, "docker", "rm", "-fv", brokerID).CombinedOutput()
				require.NoError(t, err, "%s", out)
			}
		})
		require.NoError(t, setupErr, "%s", data)
		require.NotEmpty(t, brokerID, "setup must identify its broker: %s", data)
		a := kadm.NewClient(fixtureClient(t, "localhost:9092"))
		producer := fixtureClient(t, "localhost:9092")
		readme, err := os.ReadFile("../../../README.md")
		require.NoError(t, err)
		original, err := readmeContainerCommand(string(readme))
		require.NoError(t, err)
		const published = "ghcr.io/pagrus7/kaferry:latest"
		const network = "--network container:kaferry-kafka-demo"
		require.Contains(t, string(guide), network, "test the documented network option")
		// Only the image and network option differ from the extracted command.
		script := strings.Replace(original, published, image, 1)
		script = strings.Replace(script, "docker run ", "docker run "+network+" ", 1)
		require.Equal(t, 1, strings.Count(script, network))
		require.Equal(t, 1, strings.Count(script, image))
		require.Equal(t, original, strings.Replace(strings.Replace(script, network+" ", "", 1), image, published, 1))
		config := "[connections.default]\n'bootstrap.servers' = 'localhost:9092'\n"
		writeInputs := func(t *testing.T, directory, statement, settings string) {
			t.Helper()
			require.NoError(t, os.WriteFile(filepath.Join(directory, "connections.toml"), []byte(config+settings), 0600))
			require.NoError(t, os.WriteFile(filepath.Join(directory, "copy.sql"), []byte(statement), 0600))
		}
		launch := func(t *testing.T, directory string) *cliProcess {
			t.Helper()
			process := &cliProcess{command: tutorialShell(ctx, script), done: make(chan struct{})}
			process.command.Dir = directory
			process.start(t)
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				ids, err := mountedContainers(cleanup, directory)
				require.NoError(t, err)
				for _, id := range ids {
					out, err := exec.CommandContext(cleanup, "docker", "rm", "--force", id).CombinedOutput()
					require.NoError(t, err, "%s", out)
				}
			})
			return process
		}
		removed := func(t *testing.T, directory string) {
			t.Helper()
			require.Eventually(t, func() bool {
				ids, err := mountedContainers(ctx, directory)
				return err == nil && len(ids) == 0
			}, 5*time.Second, 100*time.Millisecond, "--rm must remove the previous container")
		}
		t.Run("kafka", func(t *testing.T) {
			directory, err := filepath.EvalSymlinks(t.TempDir())
			require.NoError(t, err)
			// When the mounted TOML/SQL copy is stopped and restarted in separate containers.
			const group = "container-copy"
			writeInputs(t, directory, "INSERT INTO TOPIC 'orders-copy' SELECT * FROM TOPIC 'orders' WITH (GROUP='"+group+"')", "")
			var previous string
			for round := range 2 {
				process := launch(t, directory)
				var id string
				require.Eventually(t, func() bool {
					ids, err := mountedContainers(ctx, directory)
					if err == nil && len(ids) == 1 {
						id = ids[0]
						return true
					}
					return false
				}, 10*time.Second, 100*time.Millisecond, "find the bind-mounted container")
				require.NotEqual(t, previous, id, "resume must use a new container")
				previous = id
				waitMembers(t, a, group, 1, 1, nil)
				if round == 1 {
					publish(t, producer, &kgo.Record{Topic: "orders", Value: []byte("order-4"), Timestamp: time.Now()})
				}
				waitEnd(t, a, "orders-copy", int64(3+round))
				out, err := exec.CommandContext(ctx, "docker", "stop", id).CombinedOutput()
				require.NoError(t, err, "%s", out)
				require.NoError(t, process.wait(), "%s", process.logs())
				// Then SIGTERM saves exactly the confirmed progress and resume never recopies it.
				summary := summaryFrom(t, process.logs())
				require.Equal(t, []int{3, 1}[round], summary.Confirmed)
				progress := committedPartitions(t, a, group, "orders", 1)
				require.EqualValues(t, 3+round, progress[0].At)
				require.EqualValues(t, 3+round, topicRecordCount(t, a, "orders-copy", 1), "clean restart must not recopy")
				removed(t, directory)
			}
		})
		t.Run("archive", func(t *testing.T) {
			directory, err := filepath.EvalSymlinks(t.TempDir())
			require.NoError(t, err)
			// Given an archive and a host-owned checkpoint directory outside that input.
			destination := topic(t, a, 1)
			records := make([]core.Record, 3)
			for id := range records {
				records[id] = archiveLoadRecordPartitions(id, 1)
			}
			writeArchiveRecords(t, filepath.Join(directory, "archive"), records)
			checkpoints := filepath.Join(directory, "checkpoints")
			require.NoError(t, os.Mkdir(checkpoints, 0700))
			writeInputs(t, directory, fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM ARCHIVE '/work/archive' WITH (GROUP='container-replay')", destination), "[worker]\ncheckpoint_dir = '/work/checkpoints'\n")
			var saved string
			for round := range 2 {
				// When the same README command replays in two successive --rm containers.
				process := launch(t, directory)
				require.NoError(t, process.wait(), "%s", process.logs())
				removed(t, directory)
				// Then the host reads persisted checkpoint files and the second run confirms zero.
				summary := summaryFrom(t, process.logs())
				require.Equal(t, []int{3, 0}[round], summary.Submitted)
				require.Equal(t, []int{3, 0}[round], summary.Confirmed)
				require.EqualValues(t, 3, topicRecordCount(t, a, destination, 1))
				files, err := filepath.Glob(filepath.Join(checkpoints, "*.json"))
				require.NoError(t, err)
				require.Len(t, files, 1, "checkpoint must survive container removal on the host")
				checkpoint, err := os.ReadFile(files[0])
				require.NoError(t, err, "files written by --user must be host-readable")
				require.True(t, json.Valid(checkpoint))
				if round == 0 {
					saved = files[0]
				} else {
					require.Equal(t, saved, files[0])
				}
			}
		})
	})
}
