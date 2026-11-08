//go:build integration && load

package compatibility

import (
	"testing"

	system "github.com/pagrus7/kaferry/src/tests/system"
)

func TestStreamingSoakFidelityCommitLagMemoryAndShutdown(t *testing.T) {
	system.RunStreamingSoakFidelityCommitLagMemoryAndShutdown(t)
}
