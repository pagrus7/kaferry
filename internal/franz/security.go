package franz

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func (s settings) securityOptions() ([]kgo.Opt, error) {
	var options []kgo.Opt
	if strings.HasSuffix(s.protocol, "SSL") {
		cfg, err := s.tlsConfig()
		if err != nil {
			return nil, err
		}
		options = append(options, kgo.DialTLSConfig(cfg))
	} else if s.ca != "" || s.certificate != "" || s.key != "" {
		return nil, invalidProperty("security.protocol", "TLS files require SSL or SASL_SSL")
	}
	if strings.HasPrefix(s.protocol, "SASL_") {
		switch s.mechanism {
		case "PLAIN":
			options = append(options, kgo.SASL(plain.Auth{User: s.username, Pass: s.password}.AsMechanism()))
		case "SCRAM-SHA-256":
			options = append(options, kgo.SASL(scram.Auth{User: s.username, Pass: s.password}.AsSha256Mechanism()))
		case "SCRAM-SHA-512":
			options = append(options, kgo.SASL(scram.Auth{User: s.username, Pass: s.password}.AsSha512Mechanism()))
		}
	} else if s.username != "" || s.password != "" {
		return nil, invalidProperty("security.protocol", "SASL credentials require SASL_PLAINTEXT or SASL_SSL")
	}
	return options, nil
}

func (s settings) tlsConfig() (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if s.ca != "" {
		data, err := os.ReadFile(s.ca)
		if err != nil {
			return nil, invalidProperty("ssl.ca.location", "cannot read CA certificates")
		}
		cfg.RootCAs = x509.NewCertPool()
		if !cfg.RootCAs.AppendCertsFromPEM(data) {
			return nil, invalidProperty("ssl.ca.location", "contains no CA certificates")
		}
	}
	if s.certificate != "" {
		certificate, err := tls.LoadX509KeyPair(s.certificate, s.key)
		if err != nil {
			return nil, invalidProperty("ssl.certificate.location/ssl.key.location", "cannot load client certificate and unencrypted private key")
		}
		cfg.Certificates = []tls.Certificate{certificate}
	}
	if s.hostname == "none" {
		// Only hostname matching is disabled. Certificate chains remain verified.
		cfg.InsecureSkipVerify = true
		cfg.VerifyConnection = func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return fmt.Errorf("TLS peer did not provide a certificate")
			}
			intermediates := x509.NewCertPool()
			for _, certificate := range state.PeerCertificates[1:] {
				intermediates.AddCert(certificate)
			}
			_, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: cfg.RootCAs, Intermediates: intermediates})
			return err
		}
	}
	return cfg, nil
}
