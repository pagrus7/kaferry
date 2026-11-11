package faults

import (
	"fmt"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// SDKDiagnostics retains recent SDK response and transport diagnostics for the
// synthetic fixture. It does not log record payloads and bounds rows and bytes.
type SDKDiagnostics struct {
	mu      sync.Mutex
	lines   []string
	dropped int
}

// Level includes the SDK's per-response retry diagnostics.
func (*SDKDiagnostics) Level() kgo.LogLevel { return kgo.LogLevelDebug }

// Log retains the latest 128 rows, including errors after steady publishing.
func (d *SDKDiagnostics) Log(level kgo.LogLevel, message string, values ...any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	line := fmt.Sprintf("%s %s %v", level, message, values)
	if len(line) > 2048 {
		line = line[:2048] + " [truncated]"
	}
	if len(d.lines) == 128 {
		copy(d.lines, d.lines[1:])
		d.lines[len(d.lines)-1] = line
		d.dropped++
		return
	}
	d.lines = append(d.lines, line)
}

// Snapshot returns a chronological tail and its omitted-row count.
func (d *SDKDiagnostics) Snapshot() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return fmt.Sprintf("%s\nSDK diagnostic rows omitted: %d", strings.Join(d.lines, "\n"), d.dropped)
}
