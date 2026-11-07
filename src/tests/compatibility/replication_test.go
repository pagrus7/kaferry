//go:build integration && qualification

package compatibility

import (
	"testing"

	system "github.com/pagrus7/kaferry/src/tests/system"
)

func TestReplicatedClusterLeaderFailurePreservesCopy(t *testing.T) {
	system.RunReplicatedClusterLeaderFailurePreservesCopy(t)
}
