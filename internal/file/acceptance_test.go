package file

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	core "kafka-fwd2/internal/copy"
	"kafka-fwd2/internal/plan"
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
			if err != nil {
				t.Fatal(err)
			}
			if err := out.Submit(sample(), core.Token{}); err != nil {
				t.Fatal(err)
			}
			if err := out.Flush(); err != nil {
				t.Fatal(err)
			}
			out.Close()
			cp, err := NewCheckpoints(dir, "g", DurableWriter{})
			if err != nil {
				t.Fatal(err)
			}
			source, err := NewSource(path, "fp", plan.WindowSpec{}, codec(t), cp, core.RealClock{})
			if err != nil {
				t.Fatal(err)
			}
			if err := source.Start(&lifecycle{}); err != nil {
				t.Fatal(err)
			}
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
				if openErr != nil {
					t.Fatal(openErr)
				}
				_, err = f.WriteAt([]byte{0, 0, 0, 0}, saved.LastFrame.Position)
				f.Close()
			}
			if err != nil {
				t.Fatal(err)
			}
			cp, err = NewCheckpoints(dir, "g", DurableWriter{})
			if err != nil {
				t.Fatal(err)
			}
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
	if err != nil {
		t.Fatal(err)
	}
	if err := out.Submit(sample(), core.Token{}); err != nil {
		t.Fatal(err)
	}
	if err := out.Flush(); err != nil {
		t.Fatal(err)
	}
	id, extent := out.DurableOutput()
	out.Close()
	if out, err := NewOutput(NewPublisher(), path, "g", "fp", id, extent-1, codec(t), 10, logger()); out != nil || err == nil {
		t.Fatal("extent inside a frame was accepted")
	}
	if info, err := os.Stat(path); err != nil || info.Size() != extent {
		t.Fatal("invalid extent caused destructive recovery", err)
	}
}

type simultaneousHeader struct {
	*Codec
	ready   chan<- struct{}
	release <-chan struct{}
}

func (c simultaneousHeader) EncodeHeader(header Header) ([]byte, error) {
	data, err := c.Codec.EncodeHeader(header)
	c.ready <- struct{}{}
	<-c.release
	return data, err
}

func TestSimultaneousOutputCreatorsCannotReplaceWinner(t *testing.T) {
	// Given
	path := filepath.Join(t.TempDir(), "out.cbor")
	ready, release := make(chan struct{}, 2), make(chan struct{})
	type result struct {
		out *Output
		err error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {

			// When
			out, err := NewOutput(NewPublisher(), path, "g", "fp", "", 0, simultaneousHeader{codec(t), ready, release}, 10, logger())
			results <- result{out, err}
		}()
	}
	<-ready
	<-ready
	close(release)
	successes := 0
	for range 2 {
		result := <-results
		if result.err != nil {
			continue
		}
		successes++
		defer result.out.Close()
		id, extent := result.out.DurableOutput()
		f, err := os.Open(path)

		// Then
		if err != nil {
			t.Fatal(err)
		}
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
