//go:build integration

package compat

import (
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func testBinary(t *testing.T) string {
	t.Helper()
	path := os.Getenv("KAFERRY_TEST_BINARY")
	if path != "" {
		require.NoError(t, verifyBinary(path, os.Getenv("KAFERRY_EXPECT_BINARY_SHA256")))
		return path
	}
	path = filepath.Join(t.TempDir(), "kaferry")
	cmd := exec.Command("go", "build", "-trimpath", "-o", path, "../../cmd/kaferry")
	cmd.Env = append(cmd.Environ(), "CGO_ENABLED=0")
	if data, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, data)
	}
	require.NoError(t, verifyBinary(path, os.Getenv("KAFERRY_EXPECT_BINARY_SHA256")))
	return path
}

func verifyBinary(path, expected string) error {
	if expected == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) != expected {
		return fmt.Errorf("test binary SHA-256 does not match the release artifact")
	}
	return nil
}
