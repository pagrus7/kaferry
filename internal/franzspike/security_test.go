//go:build integration && franzspike

package franzspike

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func securityTLS(t *testing.T, caFile, serverName string, hostnameOff bool) *tls.Config {
	t.Helper()
	data, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		t.Fatal("invalid fixture CA")
	}
	cfg := &tls.Config{RootCAs: roots, ServerName: serverName, MinVersion: tls.VersionTLS12}
	if hostnameOff {
		cfg.InsecureSkipVerify = true
		cfg.VerifyConnection = func(state tls.ConnectionState) error {
			intermediates := x509.NewCertPool()
			for _, c := range state.PeerCertificates[1:] {
				intermediates.AddCert(c)
			}
			_, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates})
			return err
		}
	}
	return cfg
}
func secureOpts(t *testing.T, user, password string) []kgo.Opt {
	t.Helper()
	directory := os.Getenv("FRANZ_SPIKE_CERT_DIR")
	if directory == "" {
		t.Skip("set FRANZ_SPIKE_CERT_DIR for disposable security fixture")
	}
	return []kgo.Opt{kgo.SeedBrokers("127.0.0.1:41092", "127.0.0.1:42092", "127.0.0.1:43092"), kgo.DialTLSConfig(securityTLS(t, filepath.Join(directory, "ca.pem"), "", false)), kgo.SASL(plain.Auth{User: user, Pass: password}.AsMechanism())}
}

func TestSpikeTLSHostnameAndCAAreIndependent(t *testing.T) {
	// Given a broker certificate signed by the fixture CA for localhost, not wrong.invalid.
	directory := os.Getenv("FRANZ_SPIKE_CERT_DIR")
	if directory == "" {
		t.Skip("set FRANZ_SPIKE_CERT_DIR")
	}
	for _, tc := range []struct {
		name, ca, server string
		off, want        bool
	}{
		{"valid", "ca.pem", "localhost", false, true},
		{"wrong SAN rejected", "ca.pem", "wrong.invalid", false, false},
		{"wrong SAN allowed only when disabled", "ca.pem", "wrong.invalid", true, true},
		{"untrusted CA remains rejected", "wrong-ca.pem", "wrong.invalid", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := securityTLS(t, filepath.Join(directory, tc.ca), tc.server, tc.off)
			// When performing the actual TLS handshake.
			deadline, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			connection, err := (&tls.Dialer{Config: cfg}).DialContext(deadline, "tcp", "127.0.0.1:41092")
			if connection != nil {
				connection.Close()
			}
			// Then turning off hostname matching never disables chain verification.
			if (err == nil) != tc.want {
				t.Fatal(tc.name, err)
			}
		})
	}
}

func TestSpikePlainTLSMinimumACLs(t *testing.T) {
	// Given only source READ, group READ and destination WRITE for User:copier.
	super := client(t, secureOpts(t, "admin", "fixture-admin-secret")...)
	admin := kadm.NewClient(super)
	prefix := fmt.Sprintf("franz-acl-%d-%d", os.Getpid(), sequence.Add(1))
	src, dst, group := prefix+"-src", prefix+"-dst", prefix+"-group"
	for _, name := range []string{src, dst} {
		if _, err := admin.CreateTopic(ctx(t), 1, 3, map[string]*string{"min.insync.replicas": kadm.StringPtr("2")}, name); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = admin.DeleteTopics(deadline, src, dst)
	})
	for _, rule := range []*kadm.ACLBuilder{
		kadm.NewACLs().Allow("User:copier").Topics(src).Operations(kadm.OpRead),
		kadm.NewACLs().Allow("User:copier").Topics(dst).Operations(kadm.OpWrite),
		kadm.NewACLs().Allow("User:copier").Groups(group).Operations(kadm.OpRead),
	} {
		results, err := admin.CreateACLs(ctx(t), rule)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range results {
			if r.Err != nil {
				t.Fatal(r.Err)
			}
		}
	}
	if err := super.ProduceSync(ctx(t), &kgo.Record{Topic: src, Key: []byte("key"), Value: []byte("payload")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	writer := client(t, secureOpts(t, "copier", "fixture-copy-secret")...)
	inspect := kadm.NewClient(writer)
	deadline := ctx(t)
	for {
		metadata, err := inspect.Metadata(deadline, src, dst)
		_, groupErr := inspect.FetchOffsets(deadline, group)
		if err == nil && metadata.Topics[src].Err == nil && metadata.Topics[dst].Err == nil && (groupErr == nil || errors.Is(groupErr, kerr.GroupIDNotFound)) {
			break
		}
		select {
		case <-deadline.Done():
			t.Fatal("ACL propagation failed", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	opts := append(secureOpts(t, "copier", "fixture-copy-secret"), kgo.ConsumerGroup(group), kgo.ConsumeTopics(src), kgo.DisableAutoCommit(), kgo.ConsumeResetOffset(kgo.NoResetOffset().AtStart()), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	reader := client(t, opts...)
	// When copying a record and saving progress using the restricted clients.
	record := readN(t, reader, 1)[0]
	record.Topic = dst
	if err := writer.ProduceSync(ctx(t), record).FirstErr(); err != nil {
		t.Fatal(err)
	}
	record.Topic = src
	if err := reader.CommitRecords(ctx(t), record); err != nil {
		t.Fatal(err)
	}
	if _, err := inspect.BrokerMetadata(ctx(t)); err != nil {
		t.Fatal("cluster metadata requires extra permission", err)
	}
	if _, err := inspect.ListStartOffsets(ctx(t), src); err != nil {
		t.Fatal(err)
	}
	if _, err := inspect.ListEndOffsets(ctx(t), src); err != nil {
		t.Fatal(err)
	}
	if _, err := inspect.FetchOffsets(ctx(t), group); err != nil {
		t.Fatal(err)
	}
	groups, err := inspect.DescribeGroups(ctx(t), group)
	if err != nil || groups[group].Err != nil {
		t.Fatal("group inspection requires extra permission", err, groups[group].Err)
	}
	configs, err := inspect.DescribeTopicConfigs(ctx(t), dst)
	denied := errors.Is(err, kerr.TopicAuthorizationFailed)
	if err != nil && !denied {
		t.Fatal(err)
	}
	// Then the copy/inspection works, while destination DESCRIBE_CONFIGS is denied.
	if !denied && (len(configs) != 1 || configs[0].Err != kerr.TopicAuthorizationFailed) {
		t.Fatal("optional config permission was not denied", configs)
	}
	t.Log("PLAIN/TLS copy and metadata inspection succeeded with source READ, group READ, destination WRITE; DESCRIBE_CONFIGS denied")
}

func TestSpikeScramSHA512OverTLS(t *testing.T) {
	// Given a SCRAM credential in the disposable broker metadata and a trusted CA.
	super := client(t, secureOpts(t, "admin", "fixture-admin-secret")...)
	admin := kadm.NewClient(super)
	results, err := admin.AlterUserSCRAMs(ctx(t), nil, []kadm.UpsertSCRAM{{User: "admin", Mechanism: kadm.ScramSha512, Iterations: 4096, Password: "fixture-scram-secret"}})
	if err != nil {
		t.Fatal(err)
	}
	if results["admin"].Err != nil {
		t.Fatal(results["admin"].Err)
	}
	opts := secureOpts(t, "admin", "fixture-admin-secret")
	opts = append(opts, kgo.SASL(scram.Auth{User: "admin", Pass: "fixture-scram-secret"}.AsSha512Mechanism()))
	// When using SCRAM-SHA-512, not the PLAIN mechanism.
	c := client(t, opts...)
	if err := c.Ping(ctx(t)); err != nil {
		t.Fatal(err)
	}
	// Then an authenticated metadata request succeeds.
	meta, err := kadm.NewClient(c).BrokerMetadata(ctx(t))
	if err != nil || strings.TrimSpace(meta.Cluster) == "" {
		t.Fatal(err, meta.Cluster)
	}
}
