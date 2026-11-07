//go:build integration && qualification

package compatibility

import (
	"testing"

	system "github.com/pagrus7/kaferry/src/tests/system"
)

func TestSaslTlsExternalAuthentication(t *testing.T) {
	system.RunSaslTlsExternalAuthentication(t)
}

func TestSaslTlsRestrictedAclCopyAndDeniedConfigPreflight(t *testing.T) {
	system.RunSaslTlsRestrictedAclCopyAndDeniedConfigPreflight(t)
}

func TestSaslTlsScramMechanisms(t *testing.T) {
	system.RunSaslTlsScramMechanisms(t)
}
