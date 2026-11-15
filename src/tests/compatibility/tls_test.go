//go:build integration && qualification

package compatibility

import (
	"testing"

	system "github.com/pagrus7/kaferry/src/tests/system"
)

//kaferry:guarantee security
func TestMutualTlsCopy(t *testing.T) {
	system.RunMutualTlsCopy(t)
}

//kaferry:guarantee security
func TestServerTlsCopy(t *testing.T) {
	system.RunServerTlsCopy(t)
}
