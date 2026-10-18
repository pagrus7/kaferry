package file

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
)

func TestSourceCheckpointDetectsChangedLastFrameAndShortenedSource(t *testing.T) {
	// Given
	for _, change := range []string{"last-frame", "shortened"} {
		t.Run(change, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "source.cbor")

			// When
			out, err := NewOutput(NewPublisher(), path, "fixture", "fixture", "", 0, codec(t), 10, logger())

			// Then
			require.NoError(t, err)
			require.NoError(t, out.Submit(sample(), core.Token{}))
			require.NoError(t, out.Flush())
			out.Close()
			cp, err := NewCheckpoints(dir, "g", DurableWriter{})
			require.NoError(t, err)
			source, err := NewSource(path, "fp", plan.WindowSpec{}, codec(t), cp, core.RealClock{})
			require.NoError(t, err)
			require.NoError(t, source.Start(&lifecycle{}, nil))
			event, err := source.Poll(0)
			if err != nil || event.Input == nil {
				t.Fatal(err)
			}
			if _, err := cp.Save(context.Background(), []core.Cursor{{Partition: 0, Position: event.Input.Next}}); err != nil {
				t.Fatal(err)
			}
			saved := cp.Existing()
			source.Close()
			cp.Close()
			if change == "shortened" {
				err = os.Truncate(path, saved.Position-1)
			} else {
				f, openErr := os.OpenFile(path, os.O_RDWR, 0600)
				require.NoError(t, openErr)
				_, err = f.WriteAt([]byte{0, 0, 0, 0}, saved.LastFrame.Position)
				f.Close()
			}
			require.NoError(t, err)
			cp, err = NewCheckpoints(dir, "g", DurableWriter{})
			require.NoError(t, err)
			defer cp.Close()
			if source, err := NewSource(path, "fp", plan.WindowSpec{}, codec(t), cp, core.RealClock{}); source != nil || err == nil {
				t.Fatal("changed source was silently resumed", err)
			}
		})
	}
}

func TestOutputRequiresExistingIdentityAndFrameBoundaryExtent(t *testing.T) {
	// Given
	path := filepath.Join(t.TempDir(), "out.cbor")

	// When
	newOutputOut, newOutputErr := NewOutput(NewPublisher(), path, "g", "fp", "missing-id", 100, codec(t), 10, logger())

	// Then
	if newOutputOut != nil || newOutputErr == nil {
		t.Fatal("missing output with stored progress was recreated")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("failed identity check created an output", err)
	}
	out, err := NewOutput(NewPublisher(), path, "g", "fp", "", 0, codec(t), 10, logger())
	require.NoError(t, err)
	require.NoError(t, out.Submit(sample(), core.Token{}))
	require.NoError(t, out.Flush())
	id, extent := out.DurableOutput()
	out.Close()
	if out, err := NewOutput(NewPublisher(), path, "g", "fp", id, extent-1, codec(t), 10, logger()); out != nil || err == nil {
		t.Fatal("extent inside a frame was accepted")
	}
	if info, err := os.Stat(path); err != nil || info.Size() != extent {
		t.Fatal("invalid extent caused destructive recovery", err)
	}
}

type simultaneousPublisher struct {
	Publisher
	ready   chan<- struct{}
	release <-chan struct{}
}

func (p simultaneousPublisher) Create(path, group, fingerprint string, codec ArchiveEncoder) (*os.File, bool, error) {
	p.ready <- struct{}{}
	<-p.release
	return p.Publisher.Create(path, group, fingerprint, codec)
}

func TestSimultaneousOutputCreatorsCannotReplaceWinner(t *testing.T) {
	// Given
	path := filepath.Join(t.TempDir(), "out.cbor")
	ready, release := make(chan struct{}, 2), make(chan struct{})
	releaseCreators := sync.OnceFunc(func() { close(release) })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	type result struct {
		out *Output
		err error
	}
	results := make(chan result, 2)
	remaining := 2
	t.Cleanup(func() {
		releaseCreators()
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		for remaining > 0 {
			select {
			case result := <-results:
				remaining--
				if result.out != nil {
					result.out.Close()
				}
			case <-timer.C:
				t.Error("creators did not finish during cleanup")
				return
			}
		}
	})
	for range 2 {
		c := codec(t)
		go func() {

			// When
			out, err := NewOutput(simultaneousPublisher{NewPublisher(), ready, release}, path, "g", "fp", "", 0, c, 10, logger())
			results <- result{out, err}
		}()
	}
	// Both creators finish orphan cleanup before either creates a temporary file.
	// A header-encoding barrier can deadlock when cleanup wins the pre-lock race.
	for range 2 {
		select {
		case <-ready:
		case result := <-results:
			remaining--
			if result.out != nil {
				result.out.Close()
			}
			t.Fatalf("creator exited before publication: %v", result.err)
		case <-ctx.Done():
			t.Fatal("creators did not reach publication")
		}
	}
	releaseCreators()
	successes := 0
	for range 2 {
		var completed result
		select {
		case completed = <-results:
			remaining--
		case <-ctx.Done():
			t.Fatal("creators did not finish publication")
		}
		if completed.err != nil {
			continue
		}
		successes++
		t.Cleanup(completed.out.Close)
		id, extent := completed.out.DurableOutput()
		f, err := os.Open(path)

		// Then
		require.NoError(t, err)
		info, _ := f.Stat()
		header, end, err := codec(t).Header(f, info.Size())
		f.Close()
		if err != nil || header.ID != id || extent != end || end != info.Size() {
			t.Fatal("winner's locked inode was replaced", err)
		}
	}
	if successes != 1 {
		t.Fatal("concurrent creators did not have exactly one owner", successes)
	}
}
