// Package checkpoint contains shared checkpoint naming and wire-format helpers.
package checkpoint

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"
)

func Path(directory, group string) string {
	var slug strings.Builder
	for _, r := range group {
		if slug.Len() >= 48 {
			break
		}
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			slug.WriteRune(r)
		} else {
			slug.WriteByte('_')
		}
	}
	hash := sha256.Sum256([]byte(group))
	return filepath.Join(directory, slug.String()+"-"+hex.EncodeToString(hash[:8])+".json")
}
