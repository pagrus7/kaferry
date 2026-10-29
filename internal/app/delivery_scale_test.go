package app

import (
	"fmt"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/config"
	"github.com/stretchr/testify/require"
)

func TestPartitionScaleAndAdmissionCaps(t *testing.T) {
	// Given 64 source partitions and a worker cap smaller than their combined backlog.
	source, destination := clusters(t, 64)
	seed(t, source, 64, 4)
	p := query(t, source, destination, "WHERE RECORD_OFFSET < LATEST", "")
	cfg := config.Defaults("")
	cfg.Worker.RecordCap = 48
	cfg.Worker.PartitionRecordCap = 3
	observer := &observations{}
	started := time.Now()
	acquired, committed := 0, 0
	// When streaming and resuming, then all 256 occurrences appear once under the configured cap.
	require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress", Observer: observer, Hook: func(point string, _ int32) {
		if point == "acquired" {
			acquired++
		}
		if point == "committed" {
			committed++
		}
		if (point == "acquired" && acquired%16 == 0) || (point == "committed" && committed%64 == 0) {
			t.Logf("%s: acquired=%d committed=%d elapsed=%s", point, acquired, committed, time.Since(started))
		}
	}}))
	require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
	got := output(t, destination, 64)
	require.Len(t, got, 256)
	for part := range 64 {
		for offset := range 4 {
			require.Equal(t, 1, got[fmt.Sprintf("%d:%d", part, offset)])
		}
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	require.LessOrEqual(t, observer.peak, int64(48))
}
