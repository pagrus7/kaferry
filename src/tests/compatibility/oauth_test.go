//go:build integration && qualification

package compatibility

import (
	"testing"

	system "github.com/pagrus7/kaferry/src/tests/system"
)

func TestSaslTlsOAuthTokenFile(t *testing.T) {
	system.RunSaslTlsOAuthTokenFile(t)
}

func TestSaslTlsOAuthRenewalAndKeyRotation(t *testing.T) {
	system.RunSaslTlsOAuthRenewalAndKeyRotation(t)
}
