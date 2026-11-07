//go:build integration

package compat

import (
	"os"
	"testing"
)

func broker(t *testing.T) string {
	t.Helper()
	value := os.Getenv("KAFKA_TEST_BROKERS")
	if value == "" {
		t.Skip("set KAFKA_TEST_BROKERS to a disposable broker")
	}
	return value
}
