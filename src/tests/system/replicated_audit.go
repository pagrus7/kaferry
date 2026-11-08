//go:build integration && fault

package compat

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.yaml.in/yaml/v3"
)

const requestLogger = "kafka.request.logger"
const receivedLogger = "org.apache.kafka.network.Request"
const auditCaptureLimit = 16 << 20

const requestLogFiles = `
for f in /opt/kafka/logs/kafka-request.log*; do
  test -f "$f" || continue
  printf 'KAFERRY_AUDIT_FILE %s\n' "${f##*/}"
  head -c 16777217 "$f"
done`

type replicatedAudit struct {
	client         *kgo.Client
	targets        []faults.Target
	levels         map[int32]map[string]string
	output         string
	guard          string
	guards         map[int32]int
	marks          map[string]auditMark
	faultAttempted bool
	failureCommand func(context.Context, ...string) ([]byte, error)
}

type auditMark struct {
	Started  time.Time `json:"started"`
	Finished time.Time `json:"finished"`
}

// startReplicatedAudit enables the pinned broker's synchronous request log
// before starting either selected CLI. The independent validator requires full
// request/response JSON, including actual CLI member IDs and generations.
func startReplicatedAudit(ctx context.Context, t *testing.T, lab *faults.Lab, output string) *replicatedAudit {
	t.Helper()
	targets, err := lab.Resolve(ctx, faults.Role{Kind: "cluster", Cluster: "source", Listener: "admin"})
	require.NoError(t, err)
	guard := fmt.Sprintf("kaferry-audit-%d", time.Now().UnixNano())
	client := fixtureClient(t, strings.Join(faults.Endpoints("source", 0), ","), kgo.ClientID(guard))
	audit := &replicatedAudit{client: client, targets: targets, levels: make(map[int32]map[string]string), output: output, guard: guard,
		guards: make(map[int32]int), marks: make(map[string]auditMark)}
	t.Cleanup(func() {
		// Failed cases retain diagnostics before logger restoration. They never
		// create the final files consumed by the strict qualification validator.
		audit.retainFailure(t)
		cleanup, done := context.WithTimeout(context.Background(), 15*time.Second)
		defer done()
		for broker, levels := range audit.levels {
			for logger, level := range levels {
				if err := audit.level(cleanup, broker, logger, level); err != nil {
					t.Error("restore broker request logger:", err)
				}
			}
		}
	})
	for _, target := range targets {
		broker := target.Broker
		request := kmsg.NewPtrDescribeConfigsRequest()
		request.Resources = []kmsg.DescribeConfigsRequestResource{{ResourceType: kmsg.ConfigResourceTypeBrokerLogger,
			ResourceName: fmt.Sprint(broker), ConfigNames: []string{requestLogger, receivedLogger}}}
		result, err := client.Broker(int(broker)).Request(ctx, request)
		require.NoError(t, err)
		response := result.(*kmsg.DescribeConfigsResponse)
		require.Len(t, response.Resources, 1)
		require.NoError(t, kerr.ErrorForCode(response.Resources[0].ErrorCode))
		require.Len(t, response.Resources[0].Configs, 2)
		audit.levels[broker] = make(map[string]string)
		for _, setting := range response.Resources[0].Configs {
			require.Contains(t, []string{requestLogger, receivedLogger}, setting.Name)
			require.NotNil(t, setting.Value)
			audit.levels[broker][setting.Name] = *setting.Value
		}
		config, err := faults.Docker(ctx, "exec", target.Container, "cat", "/opt/kafka/config/log4j2.yaml")
		require.NoError(t, err, "pinned broker logging configuration must be inspectable")
		require.NoError(t, os.WriteFile(filepath.Join(output, fmt.Sprintf("source-%d.log4j2.yaml", broker)), config, 0600))
		normalized, err := requestLogConfiguration(config)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(output, fmt.Sprintf("source-%d.log4j2.json", broker)), normalized, 0600))
		flags, err := faults.Docker(ctx, "exec", target.Container, "sh", "-ec", `
test -z "${JAVA_TOOL_OPTIONS:-}${_JAVA_OPTIONS:-}${JDK_JAVA_OPTIONS:-}${LOG4J_CONTEXT_SELECTOR:-}${LOG4J2_CONTEXT_SELECTOR:-}"
tr '\000' '\n' </proc/1/cmdline | sed -n '/^-D[Ll]og4j/p;/^-Dkafka.logs.dir=/p'`)
		require.NoError(t, err, "unaccounted JVM logging options prevent a complete synchronous audit")
		require.NoError(t, os.WriteFile(filepath.Join(output, fmt.Sprintf("source-%d.jvm-logging.txt", broker)), flags, 0600))
		require.NoError(t, audit.level(ctx, broker, requestLogger, "DEBUG"))
		require.NoError(t, audit.level(ctx, broker, receivedLogger, "TRACE"))
	}
	audit.mark(ctx, t, "baseline")
	return audit
}

type failedAuditArtifact struct {
	Broker   int32     `json:"broker"`
	Path     string    `json:"path"`
	Started  time.Time `json:"started"`
	Finished time.Time `json:"finished"`
	Bytes    int       `json:"bytes"`
	SHA256   string    `json:"sha256,omitempty"`
	Saved    bool      `json:"saved"`
	Errors   []string  `json:"errors,omitempty"`
}

type failedAuditCapture struct {
	CaseFailed     bool                  `json:"case_failed"`
	DiagnosticOnly bool                  `json:"diagnostic_only"`
	Partial        bool                  `json:"partial"`
	Started        time.Time             `json:"started"`
	Finished       time.Time             `json:"finished"`
	Artifacts      []failedAuditArtifact `json:"artifacts"`
}

func (a *replicatedAudit) retainFailure(t *testing.T) {
	t.Helper()
	if !a.faultAttempted || !t.Failed() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := a.captureFailure(ctx); err != nil {
		t.Error("retain failed broker audit (diagnostics only):", err)
	}
}

// captureFailure makes one attempt per artifact, with a shared cleanup budget.
// Even command errors retain available bytes. Missing/truncated output is an
// explicit error, never evidence that no broker request occurred.
func (a *replicatedAudit) captureFailure(ctx context.Context) error {
	run := a.failureCommand
	if run == nil {
		run = boundedAuditDocker
	}
	receipt := failedAuditCapture{CaseFailed: true, DiagnosticOnly: true, Started: time.Now().UTC()}
	var failures []error
	for _, target := range a.targets {
		for _, kind := range []string{"requests", "console"} {
			artifact := a.captureFailedArtifact(ctx, target, kind, run)
			for _, message := range artifact.Errors {
				failures = append(failures, fmt.Errorf("%s: %s", artifact.Path, message))
			}
			receipt.Artifacts = append(receipt.Artifacts, artifact)
		}
	}
	receipt.Partial = len(failures) > 0
	receipt.Finished = time.Now().UTC()
	encoded, err := json.MarshalIndent(receipt, "", "  ")
	if err == nil {
		err = os.WriteFile(filepath.Join(a.output, "replicated-audit.failure.json"), append(encoded, '\n'), 0600)
	}
	return errors.Join(append(failures, err)...)
}

func (a *replicatedAudit) captureFailedArtifact(ctx context.Context, target faults.Target, kind string, run func(context.Context, ...string) ([]byte, error)) failedAuditArtifact {
	artifact := failedAuditArtifact{Broker: target.Broker, Started: time.Now().UTC(),
		Path: fmt.Sprintf("source-%d.failure.%s.log", target.Broker, kind)}
	args := []string{"exec", target.Container, "sh", "-ec", requestLogFiles}
	if kind == "console" {
		args = []string{"logs", target.Container}
	}
	attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
	data, err := run(attempt, args...)
	cancel()
	if err != nil {
		artifact.Errors = append(artifact.Errors, err.Error())
	}
	if len(data) > auditCaptureLimit {
		data = data[:auditCaptureLimit]
		artifact.Errors = append(artifact.Errors, "capture exceeded bounded evidence size")
	}
	artifact.Bytes = len(data)
	if len(data) == 0 {
		artifact.Errors = append(artifact.Errors, "capture returned no bytes; absence of requests is not established")
	} else if err := os.WriteFile(filepath.Join(a.output, artifact.Path), data, 0600); err != nil {
		artifact.Errors = append(artifact.Errors, err.Error())
	} else {
		artifact.Saved = true
		artifact.SHA256 = fmt.Sprintf("%x", sha256.Sum256(data))
	}
	artifact.Finished = time.Now().UTC()
	return artifact
}

type boundedAuditOutput struct {
	data     []byte
	exceeded bool
}

func (b *boundedAuditOutput) Write(data []byte) (int, error) {
	n := min(len(data), auditCaptureLimit-len(b.data))
	b.data = append(b.data, data[:n]...)
	b.exceeded = b.exceeded || n < len(data)
	return len(data), nil
}

func boundedAuditDocker(ctx context.Context, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, "docker", args...)
	var output boundedAuditOutput
	command.Stdout, command.Stderr = &output, &output
	command.WaitDelay = 250 * time.Millisecond
	err := command.Run()
	if output.exceeded {
		err = errors.Join(err, errors.New("capture exceeded bounded evidence size"))
	}
	return output.data, err
}

// KafkaDockerWrapper rewrites YAML on every container start, including logger
// order. Preserve the original bytes and their decoded meaning without relying
// on a hash of the distribution's differently formatted source YAML.
func requestLogConfiguration(data []byte) ([]byte, error) {
	var configuration map[string]any
	if err := yaml.Unmarshal(data, &configuration); err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"raw_sha256": fmt.Sprintf("%x", sha256.Sum256(data)), "configuration": configuration})
}

func (a *replicatedAudit) level(ctx context.Context, broker int32, logger, level string) error {
	request := kmsg.NewPtrIncrementalAlterConfigsRequest()
	request.Resources = []kmsg.IncrementalAlterConfigsRequestResource{{ResourceType: kmsg.ConfigResourceTypeBrokerLogger,
		ResourceName: fmt.Sprint(broker), Configs: []kmsg.IncrementalAlterConfigsRequestResourceConfig{{Name: logger, Value: &level}}}}
	result, err := a.client.Broker(int(broker)).Request(ctx, request)
	if err != nil {
		return err
	}
	response := result.(*kmsg.IncrementalAlterConfigsResponse)
	if len(response.Resources) != 1 {
		return fmt.Errorf("logger alteration returned %d resources", len(response.Resources))
	}
	return kerr.ErrorForCode(response.Resources[0].ErrorCode)
}

func (a *replicatedAudit) mark(ctx context.Context, t *testing.T, phase string) {
	t.Helper()
	a.marks[phase] = auditMark{Started: time.Now().UTC()}
	for _, target := range a.targets {
		_, err := a.client.Broker(int(target.Broker)).Request(ctx, kmsg.NewPtrApiVersionsRequest())
		require.NoError(t, err, "audit guard request")
	}
}

type ownershipAuditRecord struct {
	Header  struct{ ClientID, RequestAPIKeyName string } `json:"requestHeader"`
	Request struct {
		GroupID, MemberID string
		Generation        int32 `json:"generationIdOrMemberEpoch"`
		Topics            []struct {
			Partitions []struct {
				PartitionIndex  int32
				CommittedOffset json.Number
			}
		}
	} `json:"request"`
	Response *struct {
		ErrorCode    int16
		MemberID     string
		GenerationID int32
		Topics       []struct {
			Partitions []struct {
				PartitionIndex int32
				ErrorCode      int16
			}
		}
	} `json:"response"`
}

// currentGenerationSaved gates injection on actual accepted commits in the
// worker's latest successful join generation. Old committed group offsets alone
// cannot show that the worker has saved in its current generation.
func currentGenerationSaved(data []byte, client, group, member string, owned []int32, minimum map[int32]int64) (int32, bool, error) {
	rows, generation, err := currentGenerationRows(data, client, group, member)
	if err != nil {
		return 0, false, err
	}
	accepted := make(map[int32]bool)
	for _, row := range rows {
		if row.Header.RequestAPIKeyName != "OFFSET_COMMIT" || row.Request.MemberID != member || row.Request.Generation != generation {
			continue
		}
		parts, parseErr := auditAcceptedParts(row, minimum)
		if parseErr != nil {
			return 0, false, parseErr
		}
		maps.Copy(accepted, parts)
	}
	for _, partition := range owned {
		if !accepted[partition] {
			return generation, false, nil
		}
	}
	return generation, generation >= 0 && len(owned) > 0, nil
}

func currentGenerationRows(data []byte, client, group, member string) ([]ownershipAuditRecord, int32, error) {
	var rows []ownershipAuditRecord
	generation := int32(-1)
	for _, line := range strings.Split(string(data), "\n") {
		_, payload, ok := strings.Cut(line, "Completed request:")
		if !ok {
			continue
		}
		var row ownershipAuditRecord
		if err := json.NewDecoder(strings.NewReader(payload)).Decode(&row); err != nil {
			return nil, 0, err
		}
		if row.Header.ClientID != client || row.Request.GroupID != group || row.Response == nil {
			continue
		}
		rows = append(rows, row)
		if row.Header.RequestAPIKeyName == "JOIN_GROUP" && row.Response.ErrorCode == 0 && row.Response.MemberID == member {
			generation = max(generation, row.Response.GenerationID)
		}
	}
	return rows, generation, nil
}

func auditAcceptedParts(row ownershipAuditRecord, minimum map[int32]int64) (map[int32]bool, error) {
	positive, accepted := make(map[int32]bool), make(map[int32]bool)
	for _, topic := range row.Request.Topics {
		for _, part := range topic.Partitions {
			offset, err := part.CommittedOffset.Int64()
			if err != nil {
				return nil, err
			}
			positive[part.PartitionIndex] = offset > max(0, minimum[part.PartitionIndex])
		}
	}
	for _, topic := range row.Response.Topics {
		for _, part := range topic.Partitions {
			if part.ErrorCode == 0 && positive[part.PartitionIndex] {
				accepted[part.PartitionIndex] = true
			}
		}
	}
	return accepted, nil
}

func (a *replicatedAudit) ready(ctx context.Context, client, group, member string, owned []int32, minimum map[int32]int64) (int32, bool, error) {
	var all []byte
	for _, target := range a.targets {
		data, err := faults.Docker(ctx, "exec", target.Container, "sh", "-ec", requestLogFiles)
		if err != nil || len(data) > 16<<20 {
			return 0, false, fmt.Errorf("bounded broker request audit unavailable: %w", err)
		}
		all = append(all, data...)
	}
	return currentGenerationSaved(all, client, group, member, owned, minimum)
}

// capture preserves every hourly segment; no size rotation is accepted by the
// validator. A hard byte ceiling fails the case instead of silently dropping
// observations. A guard in every broker's log is checked independently.
func (a *replicatedAudit) capture(ctx context.Context, t *testing.T, phase string) {
	t.Helper()
	for _, target := range a.targets {
		poll, cancel := context.WithTimeout(ctx, 5*time.Second)
		tick := time.NewTicker(50 * time.Millisecond)
		var data []byte
		for {
			var err error
			data, err = faults.Docker(poll, "exec", target.Container, "sh", "-ec", requestLogFiles)
			require.NoError(t, err)
			require.LessOrEqual(t, len(data), 16<<20, "broker request audit exceeds bounded evidence size")
			count := strings.Count(string(data), `"clientId":"`+a.guard+`"`)
			if count > a.guards[target.Broker] && len(data) > 0 && data[len(data)-1] == '\n' {
				a.guards[target.Broker] = count
				break
			}
			select {
			case <-poll.Done():
				t.Fatal("broker request audit did not retain its guard within 5s")
			case <-tick.C:
			}
		}
		tick.Stop()
		cancel()
		require.NoError(t, os.WriteFile(filepath.Join(a.output, fmt.Sprintf("source-%d.%s.requests.log", target.Broker, phase)), data, 0600))
		if phase == "final" {
			logs, err := exec.CommandContext(ctx, "docker", "logs", target.Container).CombinedOutput()
			require.NoError(t, err)
			require.LessOrEqual(t, len(logs), 16<<20, "broker diagnostics exceed bounded evidence size")
			require.NoError(t, os.WriteFile(filepath.Join(a.output, fmt.Sprintf("source-%d.console.log", target.Broker)), logs, 0600))
		}
	}
	mark := a.marks[phase]
	mark.Finished = time.Now().UTC()
	a.marks[phase] = mark
}

func verifyOwnershipAudit(ctx context.Context, t *testing.T, directory string) {
	t.Helper()
	root, err := filepath.Abs("../../..")
	require.NoError(t, err)
	command := exec.CommandContext(ctx, os.Getenv("KAFERRY_ORACLE_PYTHON"), "-m", "tools.qualification.ownership_audit", directory)
	command.Dir = root
	output, err := command.CombinedOutput()
	require.NoError(t, err, "independent selected-CLI fencing audit failed: %s", output)
	require.NoError(t, os.WriteFile(filepath.Join(directory, "ownership-proof.json"), output, 0600))
}
