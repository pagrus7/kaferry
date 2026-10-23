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
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
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
	statement, err := sql.NewParser().Parse("INSERT INTO TOPIC 'output' WITH (PARTITIONING = KEY) SELECT * FROM TOPIC 'input' WITH (GROUP='test', START=EARLIEST) ")
	require.NoError(t, err)
	p, err := plan.NewPlanner(noEnvironment{}).Build(statement, plan.Profiles{"default": values})
	require.NoError(t, err)
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
	require.NoError(t, err)
	limits := config.Defaults("").Worker
	limits.RecordCap, limits.ByteCap = 32, 2<<20
	s, err = s.bounded(limits)
	require.NoError(t, err)
	// When constructing the actual pinned client options without starting consumption.
	common, err := s.commonOptions()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	options := append(common, s.producerOptions()...)
	options = append(options, s.consumerOptions()...)
	c, err := kgo.NewClient(append(options, kgo.WithContext(ctx))...)
	require.NoError(t, err, "valid settings rejected by client")
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
	require.NoError(t, err)
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

func TestFetchQueueBoundsReachClient(t *testing.T) {
	// Given a fetch queue of one KiB within the worker's allowance.
	s, err := parseSettings(testPlan(t, map[string]string{"queued.max.messages.kbytes": "1"}).Source.Properties)
	require.NoError(t, err)
	limits := config.Defaults("").Worker
	limits.ByteCap = 1024
	s, err = s.bounded(limits)
	require.NoError(t, err)
	options, err := s.commonOptions()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	// When applying the real consumer options without connecting.
	client, err := kgo.NewClient(append(append(options, s.consumerOptions()...), kgo.WithContext(ctx))...)
	require.NoError(t, err)
	defer client.Close()
	// Then both fetch sizes and concurrent fetches honor the queue allowance.
	require.Equal(t, int32(1024), client.OptValue(kgo.FetchMaxBytes))
	require.Equal(t, int32(1024), client.OptValue(kgo.FetchMaxPartitionBytes))
	require.Equal(t, 1, client.OptValue(kgo.MaxConcurrentFetches))
	// When the queue exceeds the worker allowance by one KiB, then validation rejects it.
	s.fetchQueueBytes = 2048
	_, err = s.bounded(limits)
	var conflict *kafkaport.Conflict
	require.ErrorAs(t, err, &conflict)
	require.Contains(t, err.Error(), "queued.max.messages.kbytes")
}

func TestSettingsRejectConflictingSecurityAndTimingPropertiesSafely(t *testing.T) {
	for _, tc := range []struct {
		name, property string
		values         map[string]string
	}{
		{"heartbeat equals session", "heartbeat.interval.ms", map[string]string{"heartbeat.interval.ms": "45000"}},
		{"missing SASL username", "sasl.username/sasl.password", map[string]string{"security.protocol": "SASL_SSL", "sasl.password": "private-password"}},
		{"missing SASL password", "sasl.username/sasl.password", map[string]string{"security.protocol": "SASL_SSL", "sasl.username": "private-user"}},
		{"plaintext SASL", "sasl.password", map[string]string{"sasl.password": "private-password"}},
		{"plaintext TLS", "ssl.ca.location", map[string]string{"ssl.ca.location": "/private/certificate.pem"}},
		{"missing client key", "ssl.certificate.location/ssl.key.location", map[string]string{"security.protocol": "SSL", "ssl.certificate.location": "/private/certificate.pem"}},
		{"missing client certificate", "ssl.certificate.location/ssl.key.location", map[string]string{"security.protocol": "SSL", "ssl.key.location": "/private/key.pem"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given an inconsistent connection containing private credentials or file paths.
			p := testPlan(t, tc.values)
			// When parsing, then the conflict identifies the setting and omits private values.
			_, err := parseSettings(p.Source.Properties)
			var conflict *kafkaport.Conflict
			require.ErrorAs(t, err, &conflict)
			require.Contains(t, err.Error(), tc.property)
			require.NotContains(t, err.Error(), "private")
		})
	}
}

func TestFactoryOpenRedactsMalformedBrokerPort(t *testing.T) {
	// Given a seed broker whose port causes the client constructor to fail.
	p := testPlan(t, map[string]string{"bootstrap.servers": "localhost:private-port"})
	// When opening before any network request, then the public conflict hides constructor details.
	connection, err := componentFactory().Open(t.Context(), p.Source)
	require.Nil(t, connection)
	var conflict *kafkaport.Conflict
	require.ErrorAs(t, err, &conflict)
	require.EqualError(t, err, "invalid Kafka connection configuration")
	require.NotContains(t, err.Error(), "private-port")
}

func TestPartitionerPropertyPointsToSQLWithoutEchoingItsValue(t *testing.T) {
	// Given the retired connection property, when validating, then show the endpoint clause that replaces it.
	p := testPlan(t, map[string]string{"partitioner": "secret-value"})
	_, err := parseSettings(p.Destination.Properties)
	require.ErrorContains(t, err, "use WITH (PARTITIONING = PRESERVE) or WITH (PARTITIONING = KEY) after the INTO endpoint")
	require.NotContains(t, err.Error(), "secret-value")
}
