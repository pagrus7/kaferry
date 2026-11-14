//go:build integration && fault

package compat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// These summaries describe captured bytes, not complete ownership proof. Each
// record is appended immediately so earlier observations survive later failure.
type auditGrowthRecord struct {
	Observed          time.Time        `json:"observed_at"`
	Elapsed           float64          `json:"elapsed_seconds"`
	Broker            int32            `json:"broker"`
	Reason            string           `json:"reason"`
	Bytes             int              `json:"captured_bytes"`
	Limit             int              `json:"capture_limit_bytes"`
	CapHit            bool             `json:"cap_hit"`
	Complete          bool             `json:"captured_records_complete"`
	ClientBytes       map[string]int64 `json:"client_bytes"`
	RequestBytes      map[string]int64 `json:"request_type_bytes"`
	HeaderBytes       int64            `json:"segment_header_bytes"`
	UnclassifiedBytes int64            `json:"unclassified_bytes"`
	Error             string           `json:"error,omitempty"`
}

type auditGrowth struct {
	started time.Time
	path    string
	mu      sync.Mutex
	stop    sync.Once
	closed  bool
	cancel  context.CancelFunc
	done    chan struct{}
}

func (a *replicatedAudit) startGrowth() {
	ctx, cancel := context.WithCancel(context.Background())
	growth := &auditGrowth{started: time.Now(), path: filepath.Join(a.output, "replicated-audit.growth.jsonl"), cancel: cancel, done: make(chan struct{})}
	a.growth = growth
	a.recordGrowth(0, "start", nil, nil)
	go func() {
		defer close(growth.done)
		tick := time.NewTicker(10 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				a.sampleGrowth(ctx, "interval")
			}
		}
	}()
}

func (a *replicatedAudit) stopGrowth() {
	if a.growth == nil {
		return
	}
	a.growth.stop.Do(func() {
		a.growth.cancel()
		<-a.growth.done
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		defer cancel()
		a.sampleGrowth(ctx, "cleanup")
		a.growth.mu.Lock()
		a.growth.closed = true
		a.growth.mu.Unlock()
	})
}

func (a *replicatedAudit) sampleGrowth(ctx context.Context, reason string) {
	for _, target := range a.targets {
		attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
		data, err := boundedAuditDocker(attempt, "exec", target.Container, "sh", "-ec", requestLogFiles)
		cancel()
		a.recordGrowth(target.Broker, reason, data, err)
	}
}

func (a *replicatedAudit) recordGrowth(broker int32, reason string, data []byte, captureErr error) {
	if a.growth == nil {
		return
	}
	record := summarizeAuditGrowth(broker, reason, data, captureErr)
	record.Observed = time.Now().UTC()
	record.Elapsed = time.Since(a.growth.started).Seconds()
	encoded, err := json.Marshal(record)
	if err != nil {
		return
	}
	a.growth.mu.Lock()
	defer a.growth.mu.Unlock()
	if a.growth.closed {
		return
	}
	file, err := os.OpenFile(a.growth.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err == nil {
		_, _ = file.Write(append(encoded, '\n'))
		_ = file.Close()
	}
}

func summarizeAuditGrowth(broker int32, reason string, data []byte, captureErr error) auditGrowthRecord {
	row := auditGrowthRecord{Broker: broker, Reason: reason, Bytes: len(data), Limit: auditCaptureLimit,
		ClientBytes: make(map[string]int64), RequestBytes: make(map[string]int64), Complete: len(data) > 0}
	row.CapHit = len(data) >= auditCaptureLimit || captureErr != nil && strings.Contains(captureErr.Error(), "exceed")
	for len(data) > 0 {
		line, remaining, newline := bytes.Cut(data, []byte{'\n'})
		size := int64(len(line))
		if newline {
			size++
		}
		data = remaining
		if bytes.HasPrefix(line, []byte("KAFERRY_AUDIT_FILE ")) && newline {
			row.HeaderBytes += size
			continue
		}
		_, payload, found := bytes.Cut(line, []byte("Completed request:"))
		var header struct {
			Header struct{ ClientID, RequestAPIKeyName string } `json:"requestHeader"`
		}
		err := errors.New("missing or incomplete request record")
		if found && newline {
			err = json.NewDecoder(bytes.NewReader(payload)).Decode(&header)
		}
		if err != nil || header.Header.ClientID == "" || header.Header.RequestAPIKeyName == "" {
			row.Complete = false
			row.UnclassifiedBytes += size
			continue
		}
		row.ClientBytes[header.Header.ClientID] += size
		row.RequestBytes[header.Header.RequestAPIKeyName] += size
	}
	if captureErr != nil {
		row.Error = captureErr.Error()
	}
	if !row.Complete {
		row.Error = errors.Join(captureErr, errors.New("capture includes unclassified or incomplete bytes")).Error()
	}
	row.Complete = row.Complete && captureErr == nil && !row.CapHit
	return row
}
