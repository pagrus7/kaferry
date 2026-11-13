//go:build integration && qualification

package compat

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/internal/inspect"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func secureProperties(t *testing.T, username, password string) map[string]string {
	t.Helper()
	brokers, ca := os.Getenv("KAFERRY_QUAL_BROKERS"), os.Getenv("KAFERRY_QUAL_CA")
	if brokers == "" || ca == "" {
		t.Skip("set KAFERRY_QUAL_BROKERS and KAFERRY_QUAL_CA for disposable qualification cluster")
	}
	return map[string]string{"bootstrap.servers": brokers, "security.protocol": "SASL_SSL", "sasl.mechanism": "PLAIN", "sasl.username": username, "sasl.password": password, "ssl.ca.location": ca}
}

func secureConfig(t *testing.T, cfg map[string]string) string {
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
	require.NoError(t, os.WriteFile(path, []byte(text.String()), 0600))
	return path
}

func secureClient(t *testing.T, cfg map[string]string, options ...kgo.Opt) *kgo.Client {
	t.Helper()
	pem, err := os.ReadFile(cfg["ssl.ca.location"])
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(pem), "fixture CA")
	auth := sasl.Mechanism(plain.Auth{User: cfg["sasl.username"], Pass: cfg["sasl.password"]}.AsMechanism())
	switch cfg["sasl.mechanism"] {
	case "SCRAM-SHA-256":
		auth = scram.Auth{User: cfg["sasl.username"], Pass: cfg["sasl.password"]}.AsSha256Mechanism()
	case "SCRAM-SHA-512":
		auth = scram.Auth{User: cfg["sasl.username"], Pass: cfg["sasl.password"]}.AsSha512Mechanism()
	}
	return fixtureClient(t, cfg["bootstrap.servers"], append([]kgo.Opt{kgo.DialTLSConfig(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}), kgo.SASL(auth)}, options...)...)
}

func secureFixture(t *testing.T) (*kadm.Client, *kgo.Client, string, string, string) {
	t.Helper()
	cfg := secureProperties(t, "admin", "fixture-admin-secret")
	a := kadm.NewClient(secureClient(t, cfg))
	prefix := fmt.Sprintf("kaferry-security-%d", time.Now().UnixNano())
	src, dst, group := prefix+"-src", prefix+"-dst", prefix+"-copy"
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	created, err := a.CreateTopics(ctx, 1, 3, map[string]*string{"min.insync.replicas": kadm.StringPtr("2")}, src, dst)
	require.NoError(t, err)
	require.Len(t, created, 2)
	require.NoError(t, created.Error())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = a.DeleteTopics(ctx, src, dst)
	})
	for _, acl := range []*kadm.ACLBuilder{
		kadm.NewACLs().Topics(src).Operations(kadm.OpRead),
		kadm.NewACLs().Topics(dst).Operations(kadm.OpWrite),
		kadm.NewACLs().Groups("kaferry-").Operations(kadm.OpRead).ResourcePatternType(kadm.ACLPatternPrefixed),
	} {
		results, err := a.CreateACLs(ctx, acl.Allow("User:copier").AllowHosts("*"))
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.NoError(t, results[0].Err)
	}
	for _, mechanism := range []kadm.ScramMechanism{kadm.ScramSha256, kadm.ScramSha512} {
		credentials, err := a.AlterUserSCRAMs(ctx, nil, []kadm.UpsertSCRAM{{User: "copier", Mechanism: mechanism, Iterations: 4096, Password: "fixture-copy-secret"}})
		require.NoError(t, err)
		require.Len(t, credentials, 1)
		require.NoError(t, credentials.Error())
	}
	waitTopicReady(t, a, src, 1)
	waitTopicReady(t, a, dst, 1)
	waitCopierPermissions(t, src, dst)
	waitSecureFixture(t, a, src, dst, group)
	p := secureClient(t, cfg, kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.RetryTimeout(20*time.Second))
	return a, p, src, dst, group
}

// Listening sockets precede topic replication and cold coordinator loading.
func waitSecureFixture(t *testing.T, a *kadm.Client, src, dst, group string) {
	t.Helper()
	reader := kadm.NewClient(secureClient(t, secureProperties(t, "copier", "fixture-copy-secret")))
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		topics, err := a.ListTopics(ctx, src, dst)
		ready := err == nil && len(topics) == 2
		for _, topic := range topics {
			ready = ready && topic.Err == nil && len(topic.Partitions) == 1 && len(topic.Partitions[0].ISR) == 3
		}
		progress, offsetErr := reader.FetchOffsetsForTopics(ctx, group, src)
		if ready && offsetErr == nil && progress.Error() == nil && len(progress[src]) == 1 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("secure fixture not ready: topics=%+v topic_error=%v offsets=%+v offset_error=%v", topics, err, progress, offsetErr)
		case <-tick.C:
		}
	}
}

// Controller ACL changes reach serving brokers asynchronously.
func waitCopierPermissions(t *testing.T, src, dst string) {
	t.Helper()
	a := kadm.NewClient(secureClient(t, secureProperties(t, "copier", "fixture-copy-secret")))
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	consecutive := 0
	for {
		topics, err := a.ListTopics(ctx, src, dst)
		if err == nil && len(topics) == 2 && topics[src].Err == nil && topics[dst].Err == nil {
			consecutive++
		} else {
			consecutive = 0
		}
		if consecutive == 3 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("copier topic permissions not ready: %v %+v", err, topics)
		case <-tick.C:
		}
	}
}

func secureReader(t *testing.T, topic string) *kgo.Client {
	t.Helper()
	client := secureClient(t, secureProperties(t, "admin", "fixture-admin-secret"), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	assign(t, client, topic, 1)
	return client
}

func RunSaslTlsRestrictedAclCopyAndDeniedConfigPreflight(t *testing.T) {
	// Given
	_, p, src, dst, group := secureFixture(t)
	stamp := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
	publish(t, p, &kgo.Record{Topic: src, Partition: 0, Key: []byte{0, 255}, Value: []byte{}, Headers: []kgo.RecordHeader{{Key: "dup", Value: nil}, {Key: "dup", Value: []byte{}}}, Timestamp: stamp})
	statement := fmt.Sprintf("INSERT INTO TOPIC '%s' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC '%s' WITH (GROUP='%s', MAX_STALENESS=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' ", dst, src, group, stamp.Add(time.Second).Format(time.RFC3339Nano))
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	// When
	result := binaryWorker(t, ctx, statement, secureConfig(t, secureProperties(t, "copier", "fixture-copy-secret")))

	// Then
	if result.code != 0 || !strings.Contains(result.detail, "topic configuration unreadable; dependent preflight checks disabled") || strings.Contains(result.detail, "fixture-copy-secret") {
		t.Fatal("restricted TLS copy failed, omitted config warning or leaked credential", result)
	}
	r := read(t, secureReader(t, dst))
	if !bytes.Equal(r.Key, []byte{0, 255}) || r.Value == nil || len(r.Value) != 0 || !r.Timestamp.Equal(stamp) || len(r.Headers) != 2 || r.Headers[0].Value != nil || r.Headers[1].Value == nil {
		t.Fatal("restricted TLS copy changed the record", r)
	}
	for _, failure := range []struct {
		name    string
		code    int
		message string
	}{{"authorization", 2, "authorization"}, {"password", 1, "authentication"}, {"trust", 1, "x509"}} {
		t.Run(failure.name, func(t *testing.T) {
			cfg := secureProperties(t, "copier", "fixture-copy-secret")
			switch failure.name {
			case "authorization":
				cfg["sasl.username"], cfg["sasl.password"] = "denied", "fixture-denied-secret"
			case "password":
				cfg["sasl.password"] = "unique-wrong-secret"
			case "trust":
				cfg["ssl.ca.location"] = os.Getenv("KAFERRY_QUAL_WRONG_CA")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			start := time.Now()
			result := binaryWorker(t, ctx, statement+" OPTIONS(ON_ERROR=SKIP)", secureConfig(t, cfg))
			require.Equal(t, failure.code, result.code, "security failure must reach the broker/client: %s", result.detail)
			require.Contains(t, strings.ToLower(result.detail), failure.message)
			require.Less(t, time.Since(start), 14*time.Second, "security failures must stop promptly")
			require.NotContains(t, result.detail, cfg["sasl.password"], "credentials stay redacted")
		})
	}
}

func RunSaslTlsRestrictedAclInspectionAndUnnamedCopy(t *testing.T) {
	// Given a completed finite copy using only source/group READ and destination WRITE.
	a, producer, src, dst, group := secureFixture(t)
	publish(t, producer, &kgo.Record{Topic: src, Partition: 0, Value: []byte("restricted-acl")})
	config := secureConfig(t, secureProperties(t, "copier", "fixture-copy-secret"))
	statement := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WITH (GROUP='%s') WHERE RECORD_OFFSET < LATEST", dst, src, group)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	result := binaryWorker(t, ctx, statement, config)
	require.Zero(t, result.code, result.detail)
	require.Equal(t, 1, summaryFrom(t, result.detail).Confirmed)
	waitMembers(t, a, group, 0, 0, nil)

	inspectQuery := func(t *testing.T, verb string) []byte {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, testBinary(t), verb, "--sql", statement, "--config", config, "--json")
		var stdout, stderr bytes.Buffer
		command.Stdout, command.Stderr = &stdout, &stderr
		require.NoError(t, command.Run(), stderr.String())
		require.NoError(t, ctx.Err(), stderr.String())
		require.NotContains(t, stderr.String(), "fixture-copy-secret")
		return stdout.Bytes()
	}
	t.Run("status", func(t *testing.T) {
		// When the copier inspects saved status, then the original finite scope is complete.
		var report inspect.StatusReport
		require.NoError(t, json.Unmarshal(inspectQuery(t, "status"), &report))
		require.Equal(t, "complete", report.Verdict)
		require.Equal(t, group, report.Group)
		require.Len(t, report.Partitions, 1)
		require.Equal(t, new(int64(1)), report.Partitions[0].Position)
		require.Equal(t, new(int64(1)), report.Partitions[0].End)
	})
	t.Run("explain", func(t *testing.T) {
		// When the copier previews the same job, then its saved position is available.
		var report inspect.Report
		require.NoError(t, json.Unmarshal(inspectQuery(t, "explain"), &report))
		require.Equal(t, group, report.Group)
		require.Len(t, report.Partitions, 1)
		require.True(t, report.Partitions[0].Saved)
		require.EqualValues(t, 1, report.Partitions[0].Start)
	})
	t.Run("unnamed-group", func(t *testing.T) {
		// Given group READ on the kaferry- prefix, when GROUP is omitted, then a fresh copy succeeds.
		statement := fmt.Sprintf("INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' WHERE RECORD_OFFSET < LATEST", dst, src)
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		result := binaryWorker(t, ctx, statement, config)
		require.Zero(t, result.code, result.detail)
		require.Equal(t, 1, summaryFrom(t, result.detail).Confirmed)
		var generatedGroup string
		for _, line := range strings.Split(result.detail, "\n") {
			var event struct {
				Message string `json:"msg"`
				Group   string `json:"group"`
			}
			if json.Unmarshal([]byte(line), &event) == nil && event.Message == "one-off group generated" {
				generatedGroup = event.Group
			}
		}
		require.True(t, strings.HasPrefix(generatedGroup, "kaferry-"), "generated group: %q", generatedGroup)
		require.NotEqual(t, group, generatedGroup)
		require.EqualValues(t, 2, topicRecordCount(t, a, dst, 1))
		reader := secureReader(t, dst)
		for range 2 {
			require.Equal(t, []byte("restricted-acl"), read(t, reader).Value)
		}
	})
}

func RunSaslTlsScramMechanisms(t *testing.T) {
	for _, mechanism := range []string{"SCRAM-SHA-256", "SCRAM-SHA-512"} {
		t.Run(mechanism, func(t *testing.T) {
			// Given a record and a restricted SCRAM identity over TLS.
			_, producer, src, dst, group := secureFixture(t)
			stamp := time.Now().Add(-time.Minute).Truncate(time.Millisecond)
			publish(t, producer, &kgo.Record{Topic: src, Partition: 0, Value: []byte(mechanism), Timestamp: stamp})
			cfg := secureProperties(t, "copier", "fixture-copy-secret")
			cfg["sasl.mechanism"] = mechanism
			waitScramReady(t, cfg, src)
			statement := fmt.Sprintf("INSERT INTO TOPIC '%s' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC '%s' WITH (GROUP='%s', MAX_STALENESS=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' ", dst, src, group, stamp.Add(time.Second).Format(time.RFC3339Nano))
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()

			// When the CLI copies with that mechanism.
			result := binaryWorker(t, ctx, statement, secureConfig(t, cfg))

			// Then the destination contains the original bytes and timestamp.
			if result.code != 0 || strings.Contains(result.detail, "fixture-copy-secret") {
				t.Fatal("SCRAM copy failed or exposed credentials", result)
			}
			record := read(t, secureReader(t, dst))
			if string(record.Value) != mechanism || !record.Timestamp.Equal(stamp) {
				t.Fatal("SCRAM copy changed the record", record)
			}

			// Given an incorrect password, when retried with SKIP, then authentication still fails.
			cfg["sasl.password"] = "unique-wrong-scram-secret"
			failureCtx, failureCancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer failureCancel()
			result = binaryWorker(t, failureCtx, statement+" OPTIONS(ON_ERROR=SKIP)", secureConfig(t, cfg))
			require.Equal(t, 1, result.code, "wrong SCRAM password must fail: %s", result.detail)
			require.NotContains(t, result.detail, "unique-wrong-scram-secret", "credentials must stay redacted")
		})
	}
}

// Credential changes must reach the serving brokers before testing the CLI.
func waitScramReady(t *testing.T, cfg map[string]string, topic string) {
	t.Helper()
	a := kadm.NewClient(secureClient(t, cfg))
	waitTopicReady(t, a, topic, 1)
}

func RunReplicatedClusterLeaderFailurePreservesCopy(t *testing.T) {
	// Given
	a, p, src, dst, group := secureFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	described, err := a.ListTopics(ctx, dst)
	if err != nil || len(described) != 1 || described[dst].Err != nil || len(described[dst].Partitions) != 1 {
		t.Fatal("destination leader discovery", described, err)
	}
	leader := described[dst].Partitions[0].Leader
	container := fmt.Sprintf("kaferry-qualification-kafka-secure-%d-1", leader)
	owned := ownBroker(t, container, "kaferry-qualification", "")
	statement := fmt.Sprintf("INSERT INTO TOPIC '%s' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC '%s' WITH (GROUP='%s', START=EARLIEST) ", dst, src, group)
	properties := secureProperties(t, "copier", "fixture-copy-secret")
	properties["socket.timeout.ms"] = "5000"
	properties["session.timeout.ms"] = "6000"
	config := secureConfig(t, properties)
	process := startCLI(t, ctx, testBinary(t), "", "run", "-e", statement, "-c", config)
	committedPartitions(t, a, group, src, 1)
	stamp := time.Now().Truncate(time.Millisecond)
	for i := range 16 {
		publish(t, p, &kgo.Record{Topic: src, Partition: 0, Key: []byte("stable"), Value: []byte(fmt.Sprint(i)), Timestamp: stamp})
	}
	waitEnd(t, a, dst, 16)

	// When the destination leader stops while the copy is running.
	require.NoError(t, owned.stop(t))
	for i := 16; i < 64; i++ {
		publish(t, p, &kgo.Record{Topic: src, Partition: 0, Key: []byte("stable"), Value: []byte(fmt.Sprint(i)), Timestamp: stamp})
	}
	waitEnd(t, a, dst, 64)

	signalErr := process.signal(os.Interrupt)

	require.NoError(t, signalErr)
	if err := process.wait(); err != nil {
		t.Fatalf("copy failed on broker failover: %v %s", err, process.logs())
	}

	// Then remaining replicas delivered every record and shutdown committed the stream.
	reader := secureReader(t, dst)
	for i := range 64 {
		message := read(t, reader)
		if string(message.Value) != fmt.Sprint(i) || !bytes.Equal(message.Key, []byte("stable")) || !message.Timestamp.Equal(stamp) {
			t.Fatal("failover lost, duplicated, reordered or changed a record", i, message)
		}
	}
	progressReader := kadm.NewClient(secureClient(t, secureProperties(t, "admin", "fixture-admin-secret")))
	progress := committedPartitions(t, progressReader, group, src, 1)
	require.Equal(t, int64(64), progress[0].At, "failover shutdown did not save confirmed progress")
	// Restore full replication before another security test creates its topics.
	require.NoError(t, owned.recover())
	waitSecureFixture(t, a, src, dst, group)
}
