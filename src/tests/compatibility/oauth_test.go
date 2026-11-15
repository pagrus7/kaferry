//go:build integration && qualification

package compatibility

import (
	"testing"

	system "github.com/pagrus7/kaferry/src/tests/system"
)

//kaferry:guarantee security
func TestSaslTlsOAuthTokenFile(t *testing.T) {
	system.RunSaslTLSOAuthTokenFile(t)
}

//kaferry:guarantee security
func TestSaslTlsOAuthRenewalAndKeyRotation(t *testing.T) {
	system.RunSaslTLSOAuthRenewalAndKeyRotation(t)
}
