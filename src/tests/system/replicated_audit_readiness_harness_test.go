//go:build integration && fault && harness

package compat

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHarnessReplicatedAuditReadinessRejectsIncompleteBrokerReads(t *testing.T) {
	// Given complete positive observations on one broker, another broker's
	// missing, failed, oversized or malformed raw data must still prevent proof.
	for _, mode := range []string{"failed", "empty", "oversized", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			data, readErr := readAuditRequests(ctx, readinessTargets(), func(_ context.Context, args ...string) ([]byte, error) {
				if args[1] != "three" {
					return bytes.Join(readinessRows(), nil), nil
				}
				switch mode {
				case "failed":
					return bytes.Join(readinessRows(), nil), errors.New("log command failed")
				case "empty":
					return nil, nil
				case "oversized":
					return bytes.Repeat([]byte("x"), auditCaptureLimit+1), nil
				default:
					return []byte("Completed request:{\n"), nil
				}
			})
			// When complete logs are required, then partial broker success cannot
			// mask failed collection, and malformed records still reach parsing.
			if mode != "malformed" {
				require.ErrorContains(t, readErr, "broker=3")
				require.Contains(t, readErr.Error(), "elapsed=")
				require.Contains(t, readErr.Error(), "bytes=")
				require.Contains(t, readErr.Error(), "context=<nil>")
				require.Nil(t, data, "failed collection cannot expose a partial passing audit")
				return
			}
			require.NoError(t, readErr)
			_, ready, parseErr := currentGenerationSaved(data, "worker", "group", "member", []int32{0, 1}, nil, time.Time{})
			require.Error(t, parseErr)
			require.False(t, ready)
		})
	}
}
