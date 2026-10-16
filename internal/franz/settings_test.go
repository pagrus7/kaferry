package franz

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/twmb/franz-go/pkg/kgo"
)

type noEnvironment struct{}

func (noEnvironment) LookupEnv(string) (string, bool) { return "", false }
func testPlan(t *testing.T, properties map[string]string) plan.Plan {
	t.Helper()
	values := map[string]string{"bootstrap.servers": "127.0.0.1:1"}
	for key, value := range properties {
		values[key] = value
	}
	statement, err := sql.NewParser().Parse("INSERT INTO TOPIC 'output' SELECT * FROM TOPIC 'input' OPTIONS(GROUP='test',START=EARLIEST)")
	if err != nil {
		t.Fatal(err)
	}
	p, err := plan.NewPlanner(noEnvironment{}).Build(statement, plan.Profiles{"default": values})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestSettingsRejectUnsupportedAndInvalidValuesWithoutPrintingSecrets(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"unknown.property", "secret-value"}, {"go.events.channel.enable", "secret-value"}, {"enable.auto.commit", "secret-value"},
		{"message.timeout.ms", "secret-value"}, {"message.max.bytes", "secret-value"}, {"partitioner", "secret-value"},
		{"sasl.mechanism", "secret-value"}, {"ssl.key.password", "secret-value"}, {"socket.timeout.ms", "1"},
		{"metadata.max.age.ms", "3600001"}, {"linger.ms", "60001"}, {"bad\nproperty", "secret-value"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			// Given an unsupported key or malformed value containing a credential-like string.
			p := testPlan(t, map[string]string{tc.key: tc.value})
			// When parsing the connection, then the diagnostic names a safe key and never echoes the value.
			_, err := parseSettings(p.Source.Properties)
			if err == nil || strings.Contains(err.Error(), "secret-value") || strings.Contains(err.Error(), "bad\nproperty") {
				t.Fatal("unsafe or missing settings diagnostic", err)
			}
		})
	}
}

func TestSettingsAliasesAndWorkerLimits(t *testing.T) {
	// Given equivalent timeout aliases, a fast metadata refresh, and small producer/fetch caps.
	p := testPlan(t, map[string]string{"message.timeout.ms": "01000", "delivery.timeout.ms": "1000", "metadata.max.age.ms": "500", "queue.buffering.max.messages": "16", "queue.buffering.max.kbytes": "1024", "queued.max.messages.kbytes": "1024"})
	s, err := parseSettings(p.Source.Properties)
	if err != nil {
		t.Fatal(err)
	}
	limits := config.Defaults("").Worker
	limits.RecordCap, limits.ByteCap = 32, 2<<20
	s, err = s.bounded(limits)
	if err != nil {
		t.Fatal(err)
	}
	// When constructing the actual pinned client options without starting consumption.
	common, err := s.commonOptions()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	options := append(common, s.producerOptions()...)
	options = append(options, s.consumerOptions()...)
	c, err := kgo.NewClient(append(options, kgo.WithContext(ctx))...)
	if err != nil {
		t.Fatal("valid settings rejected by client", err)
	}
	defer c.Close()
	// Then metadata aliases work below the client's default refresh floor and delivery uses submission deadlines.
	if c.OptValue(kgo.MetadataMaxAge) != (500*time.Millisecond) || c.OptValue(kgo.RecordDeliveryTimeout) != time.Duration(0) || s.delivery != time.Second || c.OptValue(kgo.MaxBufferedRecords) != int64(17) {
		t.Fatal("unsafe timeout or buffer mapping", c.OptValue(kgo.RecordDeliveryTimeout))
	}
	// When a buffer exceeds the worker bound, then it cannot weaken the configured cap.
	s.queueRecords = 33
	if _, err := s.bounded(limits); err == nil {
		t.Fatal("worker cap overridden")
	}
	// When aliases disagree, then the connection fails explicitly.
	p = testPlan(t, map[string]string{"message.timeout.ms": "1000", "delivery.timeout.ms": "2000"})
	if _, err := parseSettings(p.Source.Properties); err == nil {
		t.Fatal("conflicting timeout aliases accepted")
	}
}

func TestSettingsFormattingNeverRevealsCredentials(t *testing.T) {
	// Given valid SASL credentials.
	s, err := parseSettings(testPlan(t, map[string]string{"security.protocol": "SASL_SSL", "sasl.username": "private-user", "sasl.password": "private-password"}).Source.Properties)
	if err != nil {
		t.Fatal(err)
	}
	// When formatted with common logging verbs, then neither credentials nor their structure are exposed.
	for _, verb := range []string{"%v", "%+v", "%#v"} {
		text := fmt.Sprintf(verb, s)
		if strings.Contains(text, "private-") || !strings.Contains(text, "redacted") {
			t.Fatal("credentials printed", text)
		}
	}
}

func TestValidationEnforcesExplicitMessageAndWorkerQueueBounds(t *testing.T) {
	// Given an explicit small record ceiling and an oversized producer queue.
	p := testPlan(t, map[string]string{"message.max.bytes": "1000", "queue.buffering.max.kbytes": "2"})
	limits := config.Defaults("").Worker
	limits.ByteCap = 1024
	// When validating before endpoint acquisition, then the queue cannot borrow an unknown topic limit.
	if err := componentFactory().Validate(p, limits); err == nil {
		t.Fatal("explicit queue bound deferred until output opened")
	}
}
