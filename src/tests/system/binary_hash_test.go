//go:build integration

package compat

import (
	"crypto/sha256"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExpectedBinaryHashRejectsSubstitution(t *testing.T) {
	// Given a test binary whose expected digest was recorded after archive extraction.
	binary, err := os.CreateTemp(t.TempDir(), "binary-")
	require.NoError(t, err)
	path := binary.Name()
	require.NoError(t, binary.Close())
	data := []byte("release binary")
	require.NoError(t, os.WriteFile(path, data, 0600))
	expected := fmt.Sprintf("%x", sha256.Sum256(data))
	require.NoError(t, verifyBinary(path, expected))
	// When the path is replaced, then the artifact tests reject the substitute.
	require.NoError(t, os.WriteFile(path, []byte("rebuilt binary"), 0600))
	require.ErrorContains(t, verifyBinary(path, expected), "does not match")
	require.Error(t, verifyBinary(path+"-missing", expected), "missing artifact must fail")
}
