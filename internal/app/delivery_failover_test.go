package app

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/checkpoint"
	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/delivery"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

func TestRealPreparedCommitSurvivesBrokerRestart(t *testing.T) {
	testPreparedCommitSurvivesBrokerRestart(t, "PLAIN")
}

func TestRealGSSAPIPreparedCommitSurvivesBrokerRestart(t *testing.T) {
	testPreparedCommitSurvivesBrokerRestart(t, "GSSAPI")
}

func testPreparedCommitSurvivesBrokerRestart(t *testing.T, mechanism string) {
	t.Helper()
	brokers, ca, containers := os.Getenv("KAFERRY_QUAL_BROKERS"), os.Getenv("KAFERRY_QUAL_CA"), strings.Split(os.Getenv("KAFERRY_LAB_FAILOVER_CONTAINERS"), ",")
	if brokers == "" || ca == "" || len(containers) != 3 {
		t.Skip("requires secured three-broker fixture and KAFERRY_LAB_FAILOVER_CONTAINERS in broker-ID order")
	}
	for index, name := range containers {
		require.True(t, strings.HasPrefix(name, "kaferry-eos-"), "only disposable kaferry-eos-* containers may be stopped")
		probeContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		raw, err := exec.CommandContext(probeContext, "docker", "inspect", "--format", "{{json .Config.Env}}", name).Output()
		cancel()
		require.NoError(t, err)
		var environment []string
		require.NoError(t, json.Unmarshal(raw, &environment))
		found := ""
		for _, value := range environment {
			if strings.HasPrefix(value, "KAFKA_NODE_ID=") {
				found = strings.TrimPrefix(value, "KAFKA_NODE_ID=")
			}
		}
		require.Equal(t, fmt.Sprint(index+1), found, "container broker ID")
		probeContext, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		command, err := exec.CommandContext(probeContext, "docker", "inspect", "--format", "{{json .Config.Cmd}}", name).Output()
		cancel()
		require.NoError(t, err)
		require.Contains(t, string(command), "while true", "restart fixture requires lab/compose.init.yaml")

	}
	roots := x509.NewCertPool()
	certificate, err := os.ReadFile(ca)
	require.NoError(t, err)
	require.True(t, roots.AppendCertsFromPEM(certificate))
	options := []kgo.Opt{kgo.DialTLSConfig(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}), kgo.SASL(plain.Auth{User: "admin", Pass: "fixture-admin-secret"}.AsMechanism())}
	// Probe each advertised listener directly; a healthy peer can otherwise hide
	// an unavailable seed while fixture processes finish starting.
	for _, address := range strings.Split(brokers, ",") {
		require.Eventually(t, func() bool {
			probe, err := kgo.NewClient(append(append([]kgo.Opt{}, options...), kgo.SeedBrokers(address))...)
			if err != nil {
				return false
			}
			defer probe.Close()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			_, err = probe.SeedBrokers()[0].Request(ctx, kmsg.NewPtrMetadataRequest())
			return err == nil
		}, 30*time.Second, 100*time.Millisecond, "secured fixture listener %s", address)
	}
	properties := map[string]string{"bootstrap.servers": brokers, "security.protocol": "SASL_SSL", "sasl.mechanism": "PLAIN", "sasl.username": "admin", "sasl.password": "fixture-admin-secret", "ssl.ca.location": ca, "session.timeout.ms": "6000", "heartbeat.interval.ms": "500", "metadata.max.age.ms": "100"}
	if mechanism == "GSSAPI" {
		properties["sasl.mechanism"] = mechanism
		delete(properties, "sasl.username")
		delete(properties, "sasl.password")
		properties["sasl.kerberos.principal"] = "copier@KAFERRY.TEST"
		properties["sasl.kerberos.keytab"] = os.Getenv("KAFERRY_QUAL_KEYTAB")
		properties["sasl.kerberos.config"] = os.Getenv("KAFERRY_QUAL_KRB_CONFIG")
		require.NotEmpty(t, properties["sasl.kerberos.keytab"])
		require.NotEmpty(t, properties["sasl.kerberos.config"])
		admin := kadm.NewClient(client(t, strings.Split(brokers, ","), options...))
		for _, acl := range []*kadm.ACLBuilder{
			kadm.NewACLs().Topics("input", "output", "progress").Operations(kadm.OpRead, kadm.OpWrite, kadm.OpDescribeConfigs, kadm.OpAlter),
			kadm.NewACLs().Groups("broker-restart-").ResourcePatternType(kadm.ACLPatternPrefixed).Operations(kadm.OpRead),
			kadm.NewACLs().TransactionalIDs("kaferry-", "kaferry-publication-").ResourcePatternType(kadm.ACLPatternPrefixed).Operations(kadm.OpWrite),
		} {
			results, err := admin.CreateACLs(testContext(t), acl.Allow("User:copier").AllowHosts("*"))
			require.NoError(t, err)
			for _, result := range results {
				require.NoError(t, result.Err)
			}
		}
	}
	controls := make([]func(string) (string, error), len(containers))
	for index, container := range containers {
		controls[index] = openBrokerControl(t, container)
	}
	for _, faultKind := range []string{"leader", "group-coordinator", "transaction-coordinator"} {
		for _, direction := range []string{"TOPIC_to_TOPIC", "TOPIC_to_ARCHIVE", "ARCHIVE_to_TOPIC"} {
			if faultKind == "group-coordinator" && direction == "ARCHIVE_to_TOPIC" {
				continue
			}
			t.Run(faultKind+"/"+direction, func(t *testing.T) {
				admin := kadm.NewClient(client(t, strings.Split(brokers, ","), options...))
				// Given replicated input, output and progress with two replicas required for writes.
				created, err := admin.CreateTopics(testContext(t), 1, 3, map[string]*string{"min.insync.replicas": kadm.StringPtr("2")}, "input")
				require.NoError(t, err)
				require.NoError(t, created.Error())
				created, err = admin.CreateTopics(testContext(t), 2, 3, map[string]*string{"min.insync.replicas": kadm.StringPtr("2")}, "output")
				require.NoError(t, err)
				require.NoError(t, created.Error())
				created, err = admin.CreateTopics(testContext(t), 1, 3, map[string]*string{"cleanup.policy": kadm.StringPtr("compact"), "min.insync.replicas": kadm.StringPtr("2")}, "progress")
				require.NoError(t, err)
				require.NoError(t, created.Error())
				awaitReplicatedTopics(t, strings.Split(brokers, ","), options, "input", "output", "progress")
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					_, _ = admin.DeleteTopics(ctx, "input", "output", "progress")
				})
				input, target := "TOPIC 'input@secure'", "TOPIC 'output@secure'"
				root := ""
				expected := map[string]int{}
				if strings.HasPrefix(direction, "ARCHIVE") {
					path := t.TempDir()
					expected = archiveInput(t, path)
					input = "ARCHIVE '" + path + "'"
				} else {
					seedClient := client(t, strings.Split(brokers, ","), options...)
					for offset := range 6 {
						value := fmt.Sprint(offset)
						expected[value] = 1
						require.NoError(t, seedClient.ProduceSync(testContext(t), recordFor("input", 0, value)).FirstErr())
					}
				}
				if strings.HasSuffix(direction, "ARCHIVE") {
					root = t.TempDir()
					target = "ARCHIVE '" + root + "' WITH(ROTATE_AFTER_SIZE=1000)"
				}
				ast, err := sql.NewParser().Parse(fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WITH(GROUP='broker-restart-%d') WHERE RECORD_OFFSET < LATEST", target, input, time.Now().UnixNano()))
				require.NoError(t, err)
				p, err := plan.NewPlanner(environment{}).Build(ast, plan.Profiles{"secure": properties})
				require.NoError(t, err)
				cfg := config.Defaults("")
				state := t.TempDir()
				cfg.Worker.CheckpointDir = &state
				cfg.Worker.RecordCap = 1
				cfg.Worker.PartitionRecordCap = 1
				topic := "output"
				if root != "" {
					topic = "progress"
				}
				// Topic creation can return before the controller elects a partition leader.
				var metadata kadm.Metadata
				var leader int32
				require.Eventually(t, func() bool {
					var err error
					metadata, err = admin.Metadata(testContext(t), topic)
					if err != nil || metadata.Topics[topic].Err != nil {
						return false
					}
					partition, exists := metadata.Topics[topic].Partitions[0]
					leader = partition.Leader
					return exists && partition.Err == nil && leader >= 1 && leader <= 3
				}, 30*time.Second, 100*time.Millisecond)
				selected := leader
				oldPID := ""
				awaitRestart := func() {
					exchange := controls[selected-1]
					address := ""
					for _, broker := range metadata.Brokers {
						if broker.NodeID == selected {
							address = net.JoinHostPort(broker.Host, strconv.Itoa(int(broker.Port)))
						}
					}
					require.NotEmpty(t, address)
					require.Eventually(t, func() bool { pid, err := exchange("pid"); return err == nil && pid != "NONE" && pid != oldPID }, 30*time.Second, 100*time.Millisecond)
					// A JVM process exists before its secured listener accepts requests.
					require.Eventually(t, func() bool {
						probe, err := kgo.NewClient(append(append([]kgo.Opt{}, options...), kgo.SeedBrokers(address))...)
						if err != nil {
							return false
						}
						defer probe.Close()
						ctx, cancel := context.WithTimeout(context.Background(), time.Second)
						defer cancel()
						_, err = probe.SeedBrokers()[0].Request(ctx, kmsg.NewPtrMetadataRequest())
						return err == nil
					}, 30*time.Second, 100*time.Millisecond)
				}
				var fault error
				var prepared atomic.Bool
				injected := false
				killPoint := "prepared"
				if root != "" && faultKind == "transaction-coordinator" {
					killPoint = "checkpoint-chunks"
				}
				attempt := Options{ProgressTopic: "progress", Hook: func(point string, partition int32) {
					if point == "flushed" {
						prepared.Store(true)
					}
					if point == killPoint && prepared.Load() && !injected {
						injected = true
						var coordinator kadm.FindCoordinatorResponse
						if faultKind == "group-coordinator" {
							coordinator = admin.FindGroupCoordinators(testContext(t), p.Group)[p.Group]
						} else if faultKind == "transaction-coordinator" {
							identity := Identity{SourceCluster: metadata.Cluster}
							if p.Source.Kind == "ARCHIVE" {
								data, err := os.ReadFile(checkpoint.Path(state, p.Group) + ".eos")
								if err != nil {
									fault = err
									return
								}
								binding, err := checkpoint.Decode[replayBinding](data, 32<<10)
								if err != nil {
									fault = err
									return
								}
								identity = binding.Identity
							}
							j := delivery.New(p, cfg.Worker)
							j.Identity = identity
							id := j.TransactionID(partition, "progress")
							coordinator = admin.FindTxnCoordinators(testContext(t), id)[id]
						}
						if faultKind != "leader" {
							if coordinator.Err != nil {
								fault = coordinator.Err
								return
							}
							if coordinator.NodeID < 1 || coordinator.NodeID > 3 {
								fault = fmt.Errorf("invalid coordinator broker ID")
								return
							}
							selected = coordinator.NodeID
						}
						oldPID, fault = controls[selected-1]("crash")
						if fault == nil && oldPID == "ERROR" {
							fault = fmt.Errorf("fixture broker process was not running")
						}

					}
				}}
				// When the selected leader or coordinator dies after preparation, its supervisor restarts the same broker storage.
				if err := Run(testContext(t), p, cfg, attempt); err != nil {
					t.Logf("interrupted attempt: %v", err)
				}
				require.True(t, injected)
				require.NoError(t, fault)
				awaitRestart()
				resumeContext, cancel := context.WithTimeout(t.Context(), 90*time.Second)
				defer cancel()
				require.NoError(t, Run(resumeContext, p, cfg, Options{ProgressTopic: "progress"}))
				// Then committed output has exactly the authored input occurrences, including archive overlap.
				if root != "" {
					require.Equal(t, expected, archiveValues(t, root))
					return
				}
				require.Equal(t, expected, recordValues(committedRecords(t, strings.Split(brokers, ","), 2, options...)))
			})
		}
	}
}

// Replicated creation can precede follower metadata propagation and ISR catchup.
// Prepare every broker before testing failures in the copy path.
func awaitReplicatedTopics(t *testing.T, brokers []string, options []kgo.Opt, topics ...string) {
	t.Helper()
	probes := make([]*kgo.Client, len(brokers))
	for index, address := range brokers {
		probes[index] = client(t, []string{address}, options...)
	}
	require.Eventually(t, func() bool {
		ids := make(map[string][16]byte)
		for _, probe := range probes {
			request := kmsg.NewPtrMetadataRequest()
			for _, name := range topics {
				request.Topics = append(request.Topics, kmsg.MetadataRequestTopic{Topic: &name})
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			raw, err := probe.SeedBrokers()[0].Request(ctx, request)
			cancel()
			if err != nil {
				return false
			}
			response := raw.(*kmsg.MetadataResponse)
			if len(response.Topics) != len(topics) {
				return false
			}
			for _, topic := range response.Topics {
				if topic.ErrorCode != 0 || topic.Topic == nil || topic.TopicID == [16]byte{} || len(topic.Partitions) == 0 {
					return false
				}
				if id, exists := ids[*topic.Topic]; exists && id != topic.TopicID {
					return false
				}
				ids[*topic.Topic] = topic.TopicID
				for _, part := range topic.Partitions {
					if part.ErrorCode != 0 || part.Leader < 1 || part.Leader > 3 || len(part.ISR) != 3 {
						return false
					}
				}
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond, "fixture topics did not reach all three in-sync replicas")
}

// Keep the control channel open before intake. Docker exec setup can be slow;
// the fault itself must occur while the checkpoint transaction is prepared.
func openBrokerControl(t *testing.T, container string) func(string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	script := `pid() { for d in /proc/[0-9]*; do [ "$(cat "$d/comm" 2>/dev/null)" = java ] || continue; tr '\000' '\n' < "$d/cmdline" | grep -Fxq kafka.Kafka || continue; echo "${d##*/}"; return 0; done; return 1; }
echo READY
while IFS= read -r command; do
 case "$command" in
 pid) pid || echo NONE ;;
 crash) p=$(pid) || { echo ERROR; continue; }; kill -KILL "$p"; while kill -0 "$p" 2>/dev/null; do sleep 0.01; done; echo "$p" ;;
 esac
done`
	command := exec.CommandContext(ctx, "docker", "exec", "-i", container, "sh", "-c", script)
	stdin, err := command.StdinPipe()
	require.NoError(t, err)
	stdout, err := command.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, command.Start())
	done := make(chan struct{})
	go func() { _ = command.Wait(); close(done) }()
	t.Cleanup(func() {
		_ = stdin.Close()
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("broker control did not stop")
		}
	})
	reader := bufio.NewReader(stdout)
	type response struct {
		line string
		err  error
	}
	exchange := func(message string, budget time.Duration) (string, error) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		reply := make(chan response, 1)
		go func() {
			if message != "" {
				if _, err := io.WriteString(stdin, message+"\n"); err != nil {
					reply <- response{err: err}
					return
				}
			}
			line, err := reader.ReadString('\n')
			reply <- response{line: strings.TrimSpace(line), err: err}
		}()
		timer := time.NewTimer(budget)
		defer timer.Stop()
		var failure error
		select {
		case result := <-reply:
			return result.line, result.err
		case <-ctx.Done():
			failure = ctx.Err()
		case <-timer.C:
			failure = fmt.Errorf("broker control exchange: %w", context.DeadlineExceeded)
		}
		_ = stdin.Close()
		cancel()
		select {
		case <-done:
			return "", failure
		case <-time.After(5 * time.Second):
			return "", fmt.Errorf("%w; control process did not stop", failure)
		}
	}
	ready, err := exchange("", time.Minute)
	require.NoError(t, err)
	require.Equal(t, "READY", ready)
	return func(message string) (string, error) { return exchange(message, 5*time.Second) }
}
