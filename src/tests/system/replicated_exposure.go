//go:build integration && fault && load

package compat

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/src/tests/system/faults"
	"github.com/stretchr/testify/require"
)

// The product's periodic save uses a fixed 10s context. The midpoint leaves a
// pending RPC across the 6s broker session without extending that save bound.
// Upstream loss prevents heartbeats from reaching Kafka; downstream-only loss
// would leave the member alive at the broker. Removing this toxic closes its
// connections, so a pending sync commit can retry with its original generation.
func zombieCoordinatorFault(timeouts map[string]time.Duration) (faults.Fault, error) {
	session, save := timeouts["session.timeout.ms"], timeouts["progress_save_timeout"]
	rpc := timeouts["socket.timeout.ms"]
	if len(timeouts) != 5 || session != 6*time.Second || save != 10*time.Second ||
		timeouts["delivery_timeout"] != 2*time.Second || timeouts["shutdown_timeout"] != 5*time.Second ||
		rpc != session+(save-session)/2 || session >= rpc || rpc >= save {
		return faults.Fault{}, fmt.Errorf("zombie exposure requires registered 6s session, 8s midpoint RPC, 10s save, 2s delivery and 5s shutdown")
	}
	return faults.Fault{Kind: "timeout", Stream: "upstream", Value: 0}, nil
}

func zombieWorkerSettings(timeouts map[string]time.Duration) string {
	return fmt.Sprintf("record_cap=1024\npartition_record_cap=128\nbyte_cap=134217728\npartition_byte_cap=8388608\nprogress_interval='100ms'\ndelivery_timeout=%q\nshutdown_timeout=%q\n",
		timeouts["delivery_timeout"], timeouts["shutdown_timeout"])
}

func zombieWorkerConnection(timeouts map[string]time.Duration) map[string]int64 {
	return map[string]int64{"session.timeout.ms": timeouts["session.timeout.ms"].Milliseconds(),
		"heartbeat.interval.ms": 1000, "socket.timeout.ms": timeouts["socket.timeout.ms"].Milliseconds()}
}

func zombieWorkerSQL(timeouts map[string]time.Duration, worker int, marker, src, dst, group string) string {
	connection := zombieWorkerConnection(timeouts)
	return fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s','client.id'='%s','session.timeout.ms'='%d','heartbeat.interval.ms'='%d','socket.timeout.ms'='%d'), CONNECTION destination AS ('bootstrap.servers'='%s','client.id'='%s') INSERT INTO TOPIC '%s@destination' SELECT * FROM TOPIC '%s' WITH (GROUP='%s', START=EARLIEST)",
		strings.Join(faults.Endpoints("source", worker), ","), marker, connection["session.timeout.ms"], connection["heartbeat.interval.ms"], connection["socket.timeout.ms"],
		strings.Join(faults.Endpoints("destination", worker), ","), marker, dst, src, group)
}

// Stop RSS sampling when this process exits. A failed sample after an independently
// observed exit is outside its live interval; errors while it lives still fail.
// Existing endurance sampling and its failure thresholds remain unchanged.
func watchReplicatedRSS(t *testing.T, process *cliProcess) func() rssPeak {
	return watchReplicatedSamples(t, process, func() (int64, error) { return rssSample(process.pid()) })
}

func watchReplicatedSamples(t *testing.T, process *cliProcess, sample func() (int64, error)) func() rssPeak {
	t.Helper()
	first, err := sample()
	require.NoError(t, err)
	require.Positive(t, first)
	stop, result := make(chan struct{}), make(chan rssPeak, 1)
	go func() {
		stats := rssPeak{peak: first, samples: 1}
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				result <- stats
				return
			case <-process.done:
				result <- stats
				return
			case <-tick.C:
				rss, err := sample()
				if err != nil {
					// ESRCH proves the task is gone even before the sole Wait
					// goroutine publishes completion through process.done.
					if process.finished() || errors.Is(err, syscall.ESRCH) {
						result <- stats
						return
					}
					stats.failures++
				} else {
					stats.samples++
					stats.peak = max(stats.peak, rss)
				}
			}
		}
	}()
	finish := sync.OnceValue(func() rssPeak { close(stop); return <-result })
	t.Cleanup(func() { finish() })
	return finish
}

func observeReplicatedExit(process *cliProcess) <-chan time.Time {
	exited := make(chan time.Time, 1)
	go func() {
		<-process.done // published only after the process's sole OS Wait completed
		exited <- time.Now().UTC()
	}()
	return exited
}
