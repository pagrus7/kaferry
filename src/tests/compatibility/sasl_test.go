//go:build integration && qualification

package compatibility

import (
	"testing"

	system "github.com/pagrus7/kaferry/src/tests/system"
)

//kaferry:guarantee security
func TestSaslTlsExternalAuthentication(t *testing.T) {
	system.RunSaslTlsExternalAuthentication(t)
}

//kaferry:guarantee security
func TestSaslTlsRestrictedAclCopyAndDeniedConfigPreflight(t *testing.T) {
	system.RunSaslTlsRestrictedAclCopyAndDeniedConfigPreflight(t)
}

//kaferry:guarantee security
func TestSaslTlsRestrictedAclInspectionAndUnnamedCopy(t *testing.T) {
	system.RunSaslTlsRestrictedAclInspectionAndUnnamedCopy(t)
}

//kaferry:guarantee security
func TestSaslTlsScramMechanisms(t *testing.T) {
	system.RunSaslTlsScramMechanisms(t)
}
