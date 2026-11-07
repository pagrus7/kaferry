//go:build integration && qualification

package compatibility

import (
	"testing"

	system "github.com/pagrus7/kaferry/src/tests/system"
)

func TestSaslTlsExternalAuthentication(t *testing.T) {
	system.RunSaslTlsExternalAuthentication(t)
}

func TestSaslTlsOAuthTokenFile(t *testing.T) {
	system.RunSaslTlsOAuthTokenFile(t)
}

func TestSaslTlsOAuthRenewalAndKeyRotation(t *testing.T) {
	system.RunSaslTlsOAuthRenewalAndKeyRotation(t)
}

func TestMutualTlsCopy(t *testing.T) {
	system.RunMutualTlsCopy(t)
}

func TestServerTlsCopy(t *testing.T) {
	system.RunServerTlsCopy(t)
}

func TestSaslTlsRestrictedAclCopyAndDeniedConfigPreflight(t *testing.T) {
	system.RunSaslTlsRestrictedAclCopyAndDeniedConfigPreflight(t)
}

func TestSaslTlsScramMechanisms(t *testing.T) {
	system.RunSaslTlsScramMechanisms(t)
}

func TestReplicatedClusterLeaderFailurePreservesCopy(t *testing.T) {
	system.RunReplicatedClusterLeaderFailurePreservesCopy(t)
}
