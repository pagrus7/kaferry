//go:build integration && qualification

package compatibility

import (
	"testing"

	system "github.com/pagrus7/kaferry/src/tests/system"
)

func TestMutualTlsCopy(t *testing.T) {
	system.RunMutualTlsCopy(t)
}

func TestServerTlsCopy(t *testing.T) {
	system.RunServerTlsCopy(t)
}
