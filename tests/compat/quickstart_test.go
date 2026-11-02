//go:build integration

package compat

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaQuickStart(t *testing.T) {
	if os.Getenv("KAFERRY_QUICKSTART") != "1" {
		t.Skip("set KAFERRY_QUICKSTART=1 to run the documented local Kafka playground")
	}
	// Given the documented commands, an unused demo name/port, and an installed binary.
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	listener, err := net.Listen("tcp4", "127.0.0.1:9092")
	require.NoError(t, err, "the playground needs a free local port 9092")
	require.NoError(t, listener.Close())
	names, err := exec.CommandContext(ctx, "docker", "container", "ls", "-aq", "--filter", "name=^/kaferry-kafka-demo$").CombinedOutput()
	require.NoError(t, err, "%s", names)
	require.Empty(t, strings.TrimSpace(string(names)), "the playground container name is already in use")
	guide, err := os.ReadFile("../../docs/usage.md")
	require.NoError(t, err)
	compose, err := os.ReadFile("../../compose.yaml")
	require.NoError(t, err)
	image := regexp.MustCompile(`apache/kafka:[0-9.]+`).FindString(string(guide))
	require.NotEmpty(t, image)
	for _, setting := range []string{"KAFERRY_SOURCE_IMAGE", "KAFERRY_DESTINATION_IMAGE"} {
		require.Contains(t, string(compose), setting+":-"+image+"@", "the playground and default fixtures must use the same Kafka version")
	}
	binary, err := filepath.Abs(testBinary(t))
	require.NoError(t, err)
	commands := t.TempDir()
	require.NoError(t, os.Symlink(binary, filepath.Join(commands, "kaferry")))
	directory := t.TempDir()
	command := func(block string) *exec.Cmd {
		_, rest, found := strings.Cut(string(guide), "<!-- kafka-quickstart-"+block+" -->")
		require.True(t, found, "missing playground block %s", block)
		_, rest, found = strings.Cut(rest, "```sh\n")
		require.True(t, found)
		script, _, found := strings.Cut(rest, "```")
		require.True(t, found)
		cmd := tutorialShell(ctx, script)
		cmd.Dir = directory
		cmd.Env = append(os.Environ(), "PATH="+commands+string(os.PathListSeparator)+os.Getenv("PATH"))
		return cmd
	}
	setup, err := command("setup").CombinedOutput()
	container := regexp.MustCompile(`(?m)^[a-f0-9]{64}$`).FindString(string(setup))
	t.Cleanup(func() {
		if container == "" {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		logs, _ := exec.CommandContext(cleanup, "docker", "logs", container).CombinedOutput()
		if t.Failed() {
			t.Logf("playground broker: %s", logs)
		}
		out, err := exec.CommandContext(cleanup, "docker", "rm", "-fv", container).CombinedOutput()
		require.NoError(t, err, "remove owned playground container: %s", out)
	})
	require.NoError(t, err, "%s", setup)
	require.NotEmpty(t, container, "docker run must identify the owned container: %s", setup)
	client := fixtureClient(t, "127.0.0.1:9092", kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	admin := kadm.NewClient(client)
	topics, err := admin.ListTopicsWithInternal(ctx)
	require.NoError(t, err)
	require.NotContains(t, topics, "__consumer_offsets", "the example must exercise the first group lookup on a fresh broker")
	run := func(block string) string {
		out, err := command(block).CombinedOutput()
		require.NoError(t, err, "playground %s: %s", block, out)
		return string(out)
	}
	// When the exact copy, destination-read and same-group rerun commands execute.
	first := summaryFrom(t, run("copy"))
	visible := run("read")
	rerun := summaryFrom(t, run("rerun"))
	// Then both runs finish, only three records are written, and an independent client reads them.
	require.Equal(t, 3, first.Submitted)
	require.Equal(t, 3, first.Confirmed)
	require.Contains(t, visible, "order-1\norder-2\norder-3\n")
	require.Zero(t, rerun.Submitted)
	require.Zero(t, rerun.Confirmed)
	ends, err := admin.ListEndOffsets(ctx, "orders", "orders-copy")
	require.NoError(t, err)
	require.NoError(t, ends.Error())
	require.EqualValues(t, 3, ends["orders"][0].Offset)
	require.EqualValues(t, 3, ends["orders-copy"][0].Offset, "the rerun must not append duplicate records")
	assign(t, client, "orders-copy", 1)
	for _, expected := range []string{"order-1", "order-2", "order-3"} {
		require.Equal(t, expected, string(read(t, client).Value))
	}
	client.Close()
	run("cleanup")
	container = ""
}
