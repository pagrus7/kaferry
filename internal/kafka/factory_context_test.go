package kafka

import (
	"context"
	"errors"
	"testing"
	"time"
)

type waitingMetadata struct {
	AdminClient
	deadline time.Time
}

func (m *waitingMetadata) ClusterID(ctx context.Context) (string, error) {
	m.deadline, _ = ctx.Deadline()
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(time.Second):
		return "", errors.New("metadata transport was not canceled")
	}
}

func TestConnectionMetadataHonorsCallerDeadline(t *testing.T) {
	// Given a metadata transport that never replies and a longer adapter timeout.
	transport := &waitingMetadata{}
	connection := &connection{inspector: &Inspector{admin: transport, timeout: time.Hour}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	wantDeadline, _ := ctx.Deadline()
	// When a caller with a short deadline requests the cluster identity.
	_, err := connection.ClusterID(ctx)
	// Then the caller deadline reaches the transport and ends the request.
	if !errors.Is(err, context.DeadlineExceeded) || transport.deadline.After(wantDeadline) {
		t.Fatal("metadata ignored its caller deadline", err, transport.deadline, wantDeadline)
	}
}
