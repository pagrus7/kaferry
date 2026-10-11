package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Native requests run in subprocesses so an abort or hang cannot take down the test runner.
type nativeScenario struct {
	Operation    string // groups | offsets
	GoTimeout    time.Duration
	RecoverAfter time.Duration // zero leaves the broker down
	CloseAfter   time.Duration // delay after the request returns
}

const (
	nativeScenarioEnv = "KAFERRY_NATIVE_SCENARIO"
	nativeGroup       = "kaferry-native-probe"
	nativeTopic       = "native-probe"
	nativeDeadline    = 10 * time.Second
)

func nativeScenarioFromEnvironment(t *testing.T) (nativeScenario, bool) {
	t.Helper()
	raw := os.Getenv(nativeScenarioEnv)
	if raw == "" {
		return nativeScenario{}, false
	}
	var s nativeScenario
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		t.Fatal(err)
	}
	return s, true
}

func TestNativeHelperProcess(t *testing.T) {
	if s, ok := nativeScenarioFromEnvironment(t); ok {
		runNativeScenario(s, nativeRequest)
		os.Exit(0)
	}
}

func runNativeScenario(s nativeScenario, request func(nativeScenario, *Inspector) error) {
	start := time.Now()
	mark := func(format string, args ...any) {
		fmt.Printf("%7.3f %s\n", time.Since(start).Seconds(), fmt.Sprintf(format, args...))
	}
	fail := func(err error) {
		if err != nil {
			mark("setup failed: %v", err)
			os.Exit(3)
		}
	}
	mc, err := k.NewMockCluster(1)
	fail(err)
	fail(mc.CreateTopic(nativeTopic, 1, 1))
	cfg := k.ConfigMap{"bootstrap.servers": mc.BootstrapServers(), "reconnect.backoff.ms": 10, "reconnect.backoff.max.ms": 10, "socket.timeout.ms": 1000}
	admin, err := k.NewAdminClient(&cfg)
	fail(err)
	fail(mc.SetBrokerDown(-1))
	mark("request start: %s", s.Operation)
	if s.RecoverAfter > 0 {
		go func() {
			time.Sleep(s.RecoverAfter)
			fail(mc.SetBrokerUp(-1))
			mark("brokers restored")
		}()
	}
	inspector := &Inspector{admin: admin, topic: nativeTopic, log: quietLogger(), timeout: s.GoTimeout}
	err = request(s, inspector)
	mark("request returned: %v", err)
	time.Sleep(s.CloseAfter)
	mark("close start")
	inspector.Close()
	mark("close completed")
	mc.Close()
}

func nativeRequest(s nativeScenario, inspector *Inspector) error {
	if s.Operation == "groups" {
		return inspector.CheckMembers(nativeGroup)
	}
	// MockCluster cannot serve DescribeTopics; exercise Progress's coordinator step directly.
	ctx, cancel := inspector.context()
	defer cancel()
	topic := nativeTopic
	_, err := inspector.groupOffsets(ctx, nativeGroup, []k.TopicPartition{{Topic: &topic, Partition: 0}})
	return err
}

type nativeOutcome struct {
	Kind    string // clean | abort | hang | error
	Elapsed time.Duration
	Output  string
}

func (o nativeOutcome) reached(marker string) bool { return strings.Contains(o.Output, marker) }

func (o nativeOutcome) phase(t *testing.T, marker string) time.Duration {
	t.Helper()
	for _, line := range strings.Split(o.Output, "\n") {
		fields := strings.SplitN(strings.TrimSpace(line), " ", 2)
		if len(fields) == 2 && strings.HasPrefix(fields[1], marker) {
			if seconds, err := strconv.ParseFloat(fields[0], 64); err == nil {
				return time.Duration(seconds * float64(time.Second))
			}
		}
	}
	t.Fatalf("missing phase %q:\n%s", marker, o.Output)
	return 0
}

func runNativeSubprocess(t *testing.T, s nativeScenario, helper string) nativeOutcome {
	t.Helper()
	encoded, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), nativeDeadline)
	defer cancel()
	start := time.Now()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+helper+"$")
	command.Env = append(os.Environ(), nativeScenarioEnv+"="+string(encoded))
	output, err := command.CombinedOutput()
	outcome := nativeOutcome{Kind: "clean", Elapsed: time.Since(start), Output: string(output)}
	var exitErr *exec.ExitError
	switch {
	case ctx.Err() != nil:
		outcome.Kind = "hang"
	case errors.As(err, &exitErr):
		outcome.Kind = "error"
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() && status.Signal() == syscall.SIGABRT {
			outcome.Kind = "abort"
		}
	case err != nil:
		t.Fatal(err)
	}
	return outcome
}

func TestInspectorCoordinatorTimeoutAndCleanup(t *testing.T) {
	scenarios := map[string]nativeScenario{}
	for _, operation := range []string{"groups", "offsets"} {
		// The Inspector overrides the configured 1 s socket timeout with a 4 s native expiry.
		recovered := nativeScenario{Operation: operation, GoTimeout: 2 * time.Second, RecoverAfter: 2200 * time.Millisecond, CloseAfter: time.Second}
		scenarios[operation+" timeout, brokers recover before close"] = recovered
		scenarios[operation+" timeout, brokers never recover"] = nativeScenario{Operation: operation, GoTimeout: 2 * time.Second}
	}
	// The mock does not implement group descriptions; offsets can demonstrate successful recovery.
	scenarios["offsets succeed after brokers recover within the deadline"] = nativeScenario{Operation: "offsets", GoTimeout: 3 * time.Second, RecoverAfter: 300 * time.Millisecond}
	for name, s := range scenarios {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			// Given an unavailable broker and the Inspector's timeout policy.
			// When the request runs and the handle is closed in a subprocess.
			outcome := runNativeSubprocess(t, s, "TestNativeHelperProcess")

			// Then the Go deadline bounds the request and cleanup completes without an abort or hang.
			if outcome.Kind != "clean" || !outcome.reached("close completed") {
				t.Fatalf("%s after %.2fs:\n%s", outcome.Kind, outcome.Elapsed.Seconds(), outcome.Output)
			}
			returned := outcome.phase(t, "request returned") - outcome.phase(t, "request start")
			if returned > s.GoTimeout+time.Second {
				t.Fatalf("request exceeded Go deadline %s:\n%s", s.GoTimeout, outcome.Output)
			}
			expected := "request returned: context deadline exceeded"
			if s.RecoverAfter > 0 && s.RecoverAfter < s.GoTimeout {
				expected = "request returned: <nil>"
			}
			if !outcome.reached(expected) {
				t.Fatalf("expected %q:\n%s", expected, outcome.Output)
			}
			started := outcome.phase(t, "close start")
			completed := outcome.phase(t, "close completed")
			if completed-started > 2*time.Second {
				t.Fatalf("close took %s:\n%s", completed-started, outcome.Output)
			}
		})
	}
}
