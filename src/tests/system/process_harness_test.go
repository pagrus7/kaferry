//go:build integration && harness

package compat

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHarnessBrokerCurrentLogChild(t *testing.T) {
	if os.Getenv("KAFERRY_BROKER_LOG_CHILD") != "1" {
		return
	}
	_, err := fmt.Fprint(os.Stdout, brokerProgressRow)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, os.Stdin)
	require.NoError(t, err)
	if terminal := os.Getenv("KAFERRY_BROKER_LOG_CHILD_ERROR"); terminal != "" {
		_, err = fmt.Fprintln(os.Stderr, terminal)
		require.NoError(t, err)
		os.Exit(1)
	}
}
