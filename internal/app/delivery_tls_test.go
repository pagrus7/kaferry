package app

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func TestAuthenticatedMetadataProducerConsumerAndInspection(t *testing.T) {
	for _, protocol := range []string{"SSL", "SASL_PLAINTEXT", "SASL_SSL"} {
		methods := []string{""}
		if protocol != "SSL" {
			methods = []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}
		}
		for _, method := range methods {
			t.Run(protocol+"_"+method, func(t *testing.T) {
				// Given independently secured source/destination brokers.
				settings := map[string]string{"security.protocol": protocol}
				serverOptions := []kfake.Opt{kfake.NumBrokers(1)}
				clientOptions := []kgo.Opt{}
				if method != "" {
					serverOptions = append(serverOptions, kfake.EnableSASL(), kfake.Superuser(method, "copier", "fixture-secret"))
					settings["sasl.mechanism"] = method
					settings["sasl.username"] = "copier"
					settings["sasl.password"] = "fixture-secret"
					var mechanism sasl.Mechanism = plain.Auth{User: "copier", Pass: "fixture-secret"}.AsMechanism()
					if method == "SCRAM-SHA-256" {
						mechanism = scram.Auth{User: "copier", Pass: "fixture-secret"}.AsSha256Mechanism()
					} else if method == "SCRAM-SHA-512" {
						mechanism = scram.Auth{User: "copier", Pass: "fixture-secret"}.AsSha512Mechanism()
					}
					clientOptions = append(clientOptions, kgo.SASL(mechanism))
				}
				if protocol != "SASL_PLAINTEXT" {
					issuer := httptest.NewTLSServer(nil)
					defer issuer.Close()
					cert := issuer.TLS.Certificates[0]
					serverOptions = append(serverOptions, kfake.TLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}))
					roots := x509.NewCertPool()
					roots.AddCert(issuer.Certificate())
					clientOptions = append(clientOptions, kgo.DialTLSConfig(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}))
					path := filepath.Join(t.TempDir(), "ca.pem")
					require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: issuer.Certificate().Raw}), 0600))
					settings["ssl.ca.location"] = path
				}
				source, destination := kfake.MustCluster(serverOptions...), kfake.MustCluster(serverOptions...)
				defer source.Close()
				defer destination.Close()
				sourceClient, destinationClient := client(t, source.ListenAddrs(), clientOptions...), client(t, destination.ListenAddrs(), clientOptions...)
				for _, item := range []struct {
					client *kgo.Client
					topics []string
				}{{sourceClient, []string{"input"}}, {destinationClient, []string{"output", "progress"}}} {
					created, err := kadm.NewClient(item.client).CreateTopics(testContext(t), 1, 1, nil, item.topics...)
					require.NoError(t, err)
					require.NoError(t, created.Error())
				}
				value := "compact"
				changed, err := kadm.NewClient(destinationClient).AlterTopicConfigs(testContext(t), []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "cleanup.policy", Value: &value}}, "progress")
				require.NoError(t, err)
				require.NoError(t, changed[0].Err)
				records := []*kgo.Record{}
				for offset := range 3 {
					records = append(records, recordFor("input", 0, fmt.Sprint(offset)))
				}
				require.NoError(t, sourceClient.ProduceSync(testContext(t), records...).FirstErr())
				profiles := plan.Profiles{}
				for alias, brokers := range map[string][]string{"src": source.ListenAddrs(), "dst": destination.ListenAddrs()} {
					values := map[string]string{}
					for key, value := range settings {
						values[key] = value
					}
					values["bootstrap.servers"] = strings.Join(brokers, ",")
					profiles[alias] = values
				}
				ast, err := sql.NewParser().Parse("INSERT INTO TOPIC 'output@dst' SELECT * FROM TOPIC 'input@src' WITH(GROUP='auth') WHERE RECORD_OFFSET < LATEST")
				require.NoError(t, err)
				p, err := plan.NewPlanner(environment{}).Build(ast, profiles)
				require.NoError(t, err)
				// When copying and resuming, then every extra transaction/scan client authenticates too.
				for range 2 {
					require.NoError(t, Run(testContext(t), p, config.Defaults(""), Options{ProgressTopic: "progress"}))
				}
				status, err := (Inspection{Options: Options{ProgressTopic: "progress"}}).Status(testContext(t), p, config.Defaults("").Worker, core.NoopStopObserver{})
				require.NoError(t, err)
				require.Equal(t, "stopped", status.Verdict)
				// Read committed output through an independent authenticated consumer.
				got := recordValues(committedRecords(t, destination.ListenAddrs(), 1, clientOptions...))
				require.Equal(t, map[string]int{"0": 1, "1": 1, "2": 1}, got)
			})
		}
	}
}
