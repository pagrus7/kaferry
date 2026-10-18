package observability

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/http/httpguts"
)

type Environment interface {
	LookupEnv(string) (string, bool)
	ReadFile(string) ([]byte, error)
}

// validate rejects settings for which the SDK would silently use a fallback.
// The SDK retains responsibility for standard endpoint paths and transport options.
func validate(env Environment) error {
	for _, check := range []func(Environment) error{validateEndpoint, validateDurations, validateHeaders, validateTLS} {
		if err := check(env); err != nil {
			return err
		}
	}
	return nil
}

func validateEndpoint(env Environment) error {
	name, endpoint := setting(env, "ENDPOINT")
	u, err := url.Parse(endpoint)
	if err != nil || u == nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return invalid(name)
	}
	if name, value := setting(env, "PROTOCOL"); value != "" && value != "http/protobuf" {
		return invalid(name)
	}
	if name, value := setting(env, "INSECURE"); value != "" {
		if !strings.EqualFold(value, "true") && !strings.EqualFold(value, "false") {
			return invalid(name)
		}
		if strings.EqualFold(value, "true") != (u.Scheme == "http") {
			return invalid(name)
		}
	}
	return nil
}

func validateDurations(env Environment) error {
	name, timeout := setting(env, "TIMEOUT")
	if err := milliseconds(name, timeout); err != nil {
		return err
	}
	for _, name := range []string{"OTEL_METRIC_EXPORT_INTERVAL", "OTEL_METRIC_EXPORT_TIMEOUT"} {
		if value, _ := env.LookupEnv(name); value != "" {
			if err := milliseconds(name, value); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateHeaders(env Environment) error {
	if name, value := setting(env, "HEADERS"); value != "" {
		for _, pair := range strings.Split(value, ",") {
			key, value, ok := strings.Cut(strings.TrimSpace(pair), "=")
			decoded, err := url.PathUnescape(strings.TrimSpace(value))
			if !ok || key == "" || err != nil || !httpguts.ValidHeaderFieldValue(decoded) || !httpguts.ValidHeaderFieldName(strings.TrimSpace(key)) {
				return invalid(name)
			}
		}
	}
	return nil
}

func validateTLS(env Environment) error {
	name, path := setting(env, "CERTIFICATE")
	if path != "" {
		data, err := env.ReadFile(path)
		if err != nil || !x509.NewCertPool().AppendCertsFromPEM(data) {
			return invalid(name)
		}
	}
	prefix := "OTEL_EXPORTER_OTLP_"
	for _, suffix := range []string{"CLIENT_CERTIFICATE", "CLIENT_KEY"} {
		if value, _ := env.LookupEnv("OTEL_EXPORTER_OTLP_METRICS_" + suffix); strings.TrimSpace(value) != "" {
			prefix = "OTEL_EXPORTER_OTLP_METRICS_"
		}
	}
	certName, keyName := prefix+"CLIENT_CERTIFICATE", prefix+"CLIENT_KEY"
	cert, _ := env.LookupEnv(certName)
	key, _ := env.LookupEnv(keyName)
	cert, key = strings.TrimSpace(cert), strings.TrimSpace(key)
	if cert == "" && key == "" {
		return nil
	}
	certData, err := env.ReadFile(cert)
	if cert == "" || err != nil {
		return invalid(certName)
	}
	keyData, err := env.ReadFile(key)
	if key == "" || err != nil {
		return invalid(keyName)
	}
	if _, err := tls.X509KeyPair(certData, keyData); err != nil {
		return invalid(certName)
	}
	return nil
}

func milliseconds(name, value string) error {
	if value == "" {
		return nil
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n <= 0 || n > int64((1<<63-1)/time.Millisecond) {
		return invalid(name)
	}
	return nil
}

func setting(env Environment, suffix string) (string, string) {
	name := "OTEL_EXPORTER_OTLP_METRICS_" + suffix
	if value, _ := env.LookupEnv(name); value != "" {
		return name, strings.TrimSpace(value)
	}
	name = "OTEL_EXPORTER_OTLP_" + suffix
	value, _ := env.LookupEnv(name)
	return name, strings.TrimSpace(value)
}

func invalid(name string) error { return fmt.Errorf("metrics: invalid or missing %s", name) }
