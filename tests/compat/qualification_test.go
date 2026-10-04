//go:build integration && qualification

package compat

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func secureProperties(t *testing.T, username, password string) k.ConfigMap {
	t.Helper()
	brokers, ca := os.Getenv("KFWD_QUAL_BROKERS"), os.Getenv("KFWD_QUAL_CA")
	if brokers == "" || ca == "" {
		t.Skip("set KFWD_QUAL_BROKERS and KFWD_QUAL_CA for disposable qualification cluster")
	}
	return k.ConfigMap{"bootstrap.servers": brokers, "security.protocol": "SASL_SSL", "sasl.mechanism": "PLAIN", "sasl.username": username, "sasl.password": password, "ssl.ca.location": ca}
}

func secureConfig(t *testing.T, cfg k.ConfigMap) string {
	t.Helper()
	var text strings.Builder
	text.WriteString("[connections.default]\n")
	keys := make([]string, 0, len(cfg))
	for key := range cfg {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(&text, "%q=%q\n", key, fmt.Sprint(cfg[key]))
	}
	path := filepath.Join(t.TempDir(), "connection.toml")
	if err := os.WriteFile(path, []byte(text.String()), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func secureFixture(t *testing.T) (*k.AdminClient, *k.Producer, string, string, string) {
	t.Helper()
	cfg := secureProperties(t, "admin", "fixture-admin-secret")
	a, err := k.NewAdminClient(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	prefix := fmt.Sprintf("kfwd2-security-%d", time.Now().UnixNano())
	src, dst, group := prefix+"-src", prefix+"-dst", prefix+"-copy"
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	created, err := a.CreateTopics(ctx, []k.TopicSpecification{{Topic: src, NumPartitions: 1, ReplicationFactor: 3, Config: map[string]string{"min.insync.replicas": "2"}}, {Topic: dst, NumPartitions: 1, ReplicationFactor: 3, Config: map[string]string{"min.insync.replicas": "2"}}})
	if err != nil || len(created) != 2 {
		t.Fatal("create secure fixtures", created, err)
	}
	for _, result := range created {
		if result.Error.Code() != k.ErrNoError {
			t.Fatal(result)
		}
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = a.DeleteTopics(ctx, []string{src, dst})
	})
	bindings := k.ACLBindings{
		{Type: k.ResourceTopic, Name: src, ResourcePatternType: k.ResourcePatternTypeLiteral, Operation: k.ACLOperationRead},
		{Type: k.ResourceTopic, Name: dst, ResourcePatternType: k.ResourcePatternTypeLiteral, Operation: k.ACLOperationWrite},
		{Type: k.ResourceGroup, Name: prefix, ResourcePatternType: k.ResourcePatternTypePrefixed, Operation: k.ACLOperationRead},
	}
	for i := range bindings {
		bindings[i].Principal = "User:copier"
		bindings[i].Host = "*"
		bindings[i].PermissionType = k.ACLPermissionTypeAllow
	}
	results, err := a.CreateACLs(ctx, bindings)
	if err != nil || len(results) != len(bindings) {
		t.Fatal("create restricted ACLs", results, err)
	}
	for _, result := range results {
		if result.Error.Code() != k.ErrNoError {
			t.Fatal(result)
		}
	}
	waitSecureFixture(t, a, src, dst, group)
	cfg["enable.idempotence"] = true
	cfg["message.timeout.ms"] = 20000
	p, err := k.NewProducer(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return a, p, src, dst, group
}

// Listening sockets precede topic replication and cold group-coordinator loading.
func waitSecureFixture(t *testing.T, a *k.AdminClient, src, dst, group string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		request, cancel := context.WithTimeout(ctx, 2*time.Second)
		topics, err := a.DescribeTopics(request, k.NewTopicCollectionOfTopicNames([]string{src, dst}))
		ready := err == nil && len(topics.TopicDescriptions) == 2
		for _, topic := range topics.TopicDescriptions {
			ready = ready && topic.Error.Code() == k.ErrNoError && len(topic.Partitions) == 1 && len(topic.Partitions[0].Isr) == 3
		}
		progress, err := a.ListConsumerGroupOffsets(request, []k.ConsumerGroupTopicPartitions{{Group: group, Partitions: []k.TopicPartition{{Topic: &src, Partition: 0}}}})
		cancel()
		if ready && err == nil && len(progress.ConsumerGroupsTopicPartitions) == 1 && len(progress.ConsumerGroupsTopicPartitions[0].Partitions) == 1 && progress.ConsumerGroupsTopicPartitions[0].Partitions[0].Error == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("secure fixture did not replicate or load its coordinator", err)
		case <-tick.C:
		}
	}
}

func secureReader(t *testing.T, topic string) *k.Consumer {
	t.Helper()
	cfg := secureProperties(t, "admin", "fixture-admin-secret")
	cfg["group.id"] = topic + "-verify"
	cfg["enable.auto.commit"] = false
	c, err := k.NewConsumer(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if err := c.Assign([]k.TopicPartition{{Topic: &topic, Partition: 0, Offset: 0}}); err != nil {
		t.Fatal(err)
	}
	return c
}

func waitSecureEnd(t *testing.T, topic string, end int64) {
	t.Helper()
	c := secureReader(t, topic)
	deadline := time.Now().Add(45 * time.Second)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	var high int64
	var err error
	for time.Now().Before(deadline) {
		_, high, err = c.QueryWatermarkOffsets(topic, 0, 1000)
		if err == nil && high >= end {
			return
		}
		<-tick.C
	}
	t.Fatal("secure destination did not reach expected end", high, err)
}

func secureRead(t *testing.T, reader *k.Consumer) *k.Message {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		switch event := reader.Poll(100).(type) {
		case *k.Message:
			if event.TopicPartition.Error != nil {
				t.Fatal(event.TopicPartition.Error)
			}
			return event
		case k.Error:
			if event.IsFatal() || event.Code() != k.ErrTransport && event.Code() != k.ErrAllBrokersDown && event.Code() != k.ErrTimedOut {
				t.Fatal(event)
			}
		}
	}
	t.Fatal("secure record read timed out")
	return nil
}

func TestSaslTlsRestrictedAclCopyAndDeniedConfigPreflight(t *testing.T) {
	_, p, src, dst, group := secureFixture(t)
	stamp := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: 0}, Key: []byte{0, 255}, Value: []byte{}, Headers: []k.Header{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}, Timestamp: stamp})
	statement := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", dst, src, stamp.Add(time.Second).Format(time.RFC3339Nano), group)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	result := loggedWorker(ctx, statement, secureConfig(t, secureProperties(t, "copier", "fixture-copy-secret")))
	if result.code != 0 || !strings.Contains(result.detail, "topic configuration unreadable; dependent preflight checks disabled") || strings.Contains(result.detail, "fixture-copy-secret") {
		t.Fatal("restricted TLS copy failed, omitted config warning or leaked credential", result)
	}
	r := read(t, secureReader(t, dst))
	if !bytes.Equal(r.Key, []byte{0, 255}) || r.Value == nil || len(r.Value) != 0 || !r.Timestamp.Equal(stamp) || len(r.Headers) != 2 || r.Headers[0].Value != nil || r.Headers[1].Value == nil {
		t.Fatal("restricted TLS copy changed the record", r)
	}
	for _, failure := range []string{"authorization", "password", "trust"} {
		t.Run(failure, func(t *testing.T) {
			cfg := secureProperties(t, "copier", "fixture-copy-secret")
			switch failure {
			case "authorization":
				cfg["sasl.username"], cfg["sasl.password"] = "denied", "fixture-denied-secret"
			case "password":
				cfg["sasl.password"] = "unique-wrong-secret"
			case "trust":
				cfg["ssl.ca.location"] = os.Getenv("KFWD_QUAL_WRONG_CA")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			start := time.Now()
			result := loggedWorker(ctx, strings.Replace(statement, "CUTOFF=INTERVAL '0' SECOND", "CUTOFF=INTERVAL '0' SECOND,ON_ERROR=SKIP", 1), secureConfig(t, cfg))
			if result.code == 0 || time.Since(start) > 14*time.Second || strings.Contains(result.detail, fmt.Sprint(cfg["sasl.password"])) {
				t.Fatal("security failure was skipped, retried indefinitely or exposed credential", result)
			}
		})
	}
}

func TestReplicatedClusterLeaderFailurePreservesCopy(t *testing.T) {
	a, p, src, dst, group := secureFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	described, err := a.DescribeTopics(ctx, k.NewTopicCollectionOfTopicNames([]string{dst}))
	if err != nil || len(described.TopicDescriptions) != 1 || len(described.TopicDescriptions[0].Partitions) != 1 {
		t.Fatal("destination leader discovery", described, err)
	}
	leader := described.TopicDescriptions[0].Partitions[0].Leader.ID
	container := fmt.Sprintf("kafka-fwd2-qualification-kafka-secure-%d-1", leader)
	label, err := exec.Command("docker", "inspect", "--format", `{{index .Config.Labels "com.docker.compose.project"}}`, container).Output()
	if err != nil || strings.TrimSpace(string(label)) != "kafka-fwd2-qualification" {
		t.Fatal("failover target is not the disposable qualification broker", err)
	}
	statement := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' OPTIONS(GROUP='%s',START=EARLIEST)", dst, src, group)
	properties := secureProperties(t, "copier", "fixture-copy-secret")
	properties["socket.timeout.ms"] = "5000"
	properties["session.timeout.ms"] = "6000"
	config := secureConfig(t, properties)
	cmd := exec.CommandContext(ctx, testBinary(t), "run", "-e", statement, "-c", config)
	var output bytes.Buffer
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	finished := false
	t.Cleanup(func() {
		cancel()
		if !finished {
			_ = cmd.Process.Kill()
			<-done
		}
		if t.Failed() {
			t.Logf("copy process stderr: %s", output.String())
		}
	})
	committedPartitions(t, a, group, src, 1)
	stamp := time.Now().Truncate(time.Millisecond)
	for i := range 16 {
		publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: 0}, Key: []byte("stable"), Value: []byte(fmt.Sprint(i)), Timestamp: stamp})
	}
	waitEnd(t, a, dst, 16)
	if data, err := exec.Command("docker", "stop", "-t", "1", container).CombinedOutput(); err != nil {
		t.Fatalf("stop owned leader: %v %s", err, data)
	}
	defer func() {
		if data, err := exec.Command("docker", "start", container).CombinedOutput(); err != nil {
			t.Errorf("restore owned broker: %v %s", err, data)
		}
	}()
	for i := 16; i < 64; i++ {
		publish(t, p, &k.Message{TopicPartition: k.TopicPartition{Topic: &src, Partition: 0}, Key: []byte("stable"), Value: []byte(fmt.Sprint(i)), Timestamp: stamp})
	}
	waitSecureEnd(t, dst, 64)
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		finished = true
		t.Fatalf("copy failed on broker failover: %v %s", err, output.String())
	}
	finished = true
	reader := secureReader(t, dst)
	for i := range 64 {
		message := secureRead(t, reader)
		if string(message.Value) != fmt.Sprint(i) || !bytes.Equal(message.Key, []byte("stable")) || !message.Timestamp.Equal(stamp) {
			t.Fatal("failover lost, duplicated, reordered or changed a record", i, message)
		}
	}
	progress := committedPartitions(t, a, group, src, 1)
	if progress[0].Offset != 64 {
		t.Fatal("failover shutdown did not save confirmed progress", progress)
	}
}
