//go:build integration

package compat

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func pairedTopics(t *testing.T, sourcePartitions, destinationPartitions int) (*kadm.Client, string, string, *kgo.Client) {
	t.Helper()
	a := admin(t)
	source := topic(t, a, sourcePartitions)
	destination := topic(t, a, destinationPartitions)
	return a, source, destination, producer(t)
}
