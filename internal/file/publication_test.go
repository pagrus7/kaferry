package file

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/plan"
)

type interruptedPublication struct {
	nativePublication
	stage   string
	crash   bool
	failure error
	calls   []string
}

func (p *interruptedPublication) after(stage string, err error) error {
	p.calls = append(p.calls, stage)
	if err == nil && p.stage == stage {
		if p.crash {
			os.Exit(99)
		}
		return p.failure
	}
	return err
}
func (p *interruptedPublication) CreateTemp(dir, pattern string) (*os.File, error) {
	f, err := p.nativePublication.CreateTemp(dir, pattern)
	if err = p.after("temporary", err); err != nil && f != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, err
	}
	return f, err
}
func (p *interruptedPublication) Write(f *os.File, data []byte) error {
	if p.stage == "short-write" {
		_, _ = f.Write(data[:len(data)/2])
		return io.ErrShortWrite
	}
	return p.after("header-write", p.nativePublication.Write(f, data))
}
func (p *interruptedPublication) Sync(f *os.File) error {
	return p.after("header-sync", p.nativePublication.Sync(f))
}
func (p *interruptedPublication) Link(old, new string) error {
	return p.after("publish", p.nativePublication.Link(old, new))
}
func (p *interruptedPublication) Remove(path string) error {
	return p.after("temporary-unlink", p.nativePublication.Remove(path))
}
func (p *interruptedPublication) SyncDirectory(path string) error {
	return p.after("directory-sync", p.nativePublication.SyncDirectory(path))
}

func TestPublicationFailuresNeverPermitUnownedOutput(t *testing.T) {
	// Given
	for _, stage := range []string{"temporary", "header-write", "short-write", "header-sync", "publish", "temporary-unlink", "directory-sync"} {
		t.Run(stage, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "out.cbor")
			failure := errors.New("publication I/O failure")
			if stage == "short-write" {
				failure = io.ErrShortWrite
			}
			fs := &interruptedPublication{stage: stage, failure: failure}

			// When
			out, err := NewOutput(&publisher{fs: fs}, path, "group", "fp", "", 0, codec(t), 10, logger())

			// Then
			if out != nil || !errors.Is(err, failure) {
				t.Fatal("failed publication became writable", out, err)
			}
			_, exists := os.Stat(path)
			restarted, err := NewOutput(NewPublisher(), path, "group", "fp", "", 0, codec(t), 10, logger())
			if exists == nil {
				if restarted != nil || err == nil {
					t.Fatal("unowned published header was adopted")
				}
			} else {
				if err != nil {
					t.Fatal("unpublished failure prevented clean creation", err)
				}
				restarted.Close()
			}
		})
	}
}

type initializationWriter struct {
	stage string
}

func (w initializationWriter) Replace(path string, data []byte) error {
	if strings.HasPrefix(w.stage, "checkpoint-") {
		return replaceCheckpoint(&interruptedCheckpoint{stage: w.stage}, path, data)
	}
	if w.stage == "before-checkpoint" {
		os.Exit(99)
	}
	err := (DurableWriter{}).Replace(path, data)
	if err == nil && w.stage == "after-checkpoint" {
		os.Exit(99)
	}
	return err
}

type interruptedCheckpoint struct {
	nativeReplacement
	stage string
}

func (w interruptedCheckpoint) after(stage string, err error) error {
	if err == nil && stage == w.stage {
		os.Exit(99)
	}
	return err
}
func (w interruptedCheckpoint) CreateTemp(dir, pattern string) (*os.File, error) {
	f, err := w.nativeReplacement.CreateTemp(dir, pattern)
	return f, w.after("checkpoint-temporary", err)
}
func (w interruptedCheckpoint) Write(f *os.File, data []byte) error {
	return w.after("checkpoint-write", w.nativeReplacement.Write(f, data))
}
func (w interruptedCheckpoint) Sync(f *os.File) error {
	return w.after("checkpoint-sync", w.nativeReplacement.Sync(f))
}
func (w interruptedCheckpoint) Rename(old, new string) error {
	return w.after("checkpoint-rename", w.nativeReplacement.Rename(old, new))
}
func (w interruptedCheckpoint) SyncDirectory(path string) error {
	return w.after("checkpoint-directory-sync", w.nativeReplacement.SyncDirectory(path))
}

func TestCrashAtEveryOutputInitializationBoundary(t *testing.T) {
	// Given
	if os.Getenv("KAFERRY_INITIALIZATION_CRASH") == "1" {
		args := os.Args[len(os.Args)-4:]
		stage, src, dst, directory := args[0], args[1], args[2], args[3]
		cp, err := NewCheckpoints(directory, "initialization", initializationWriter{stage})
		if err != nil {
			t.Fatal(err)
		}
		source, err := NewSource(src, "fp", plan.WindowSpec{}, codec(t), cp, core.RealClock{})
		if err != nil {
			t.Fatal(err)
		}
		pub := &publisher{fs: &interruptedPublication{stage: stage, crash: true}}
		out, err := NewOutput(pub, dst, "initialization", "fp", "", 0, codec(t), 10, logger())
		if err != nil {
			t.Fatal(err)
		}
		cp.BindOutput(out)
		if err := source.Start(&lifecycle{}); err != nil {
			t.Fatal(err)
		}
		t.Fatal("initialization crash boundary not reached")
	}
	for _, stage := range []string{"temporary", "header-write", "header-sync", "publish", "temporary-unlink", "directory-sync", "before-checkpoint", "checkpoint-temporary", "checkpoint-write", "checkpoint-sync", "checkpoint-rename", "checkpoint-directory-sync", "after-checkpoint"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			src, dst, cpdir := filepath.Join(dir, "source.cbor"), filepath.Join(dir, "out.cbor"), t.TempDir()
			fixture, err := NewOutput(NewPublisher(), src, "fixture", "fixture", "", 0, codec(t), 10, logger())
			if err != nil {
				t.Fatal(err)
			}
			for _, value := range []string{"a", "b", "c"} {
				if err := fixture.Submit(core.Record{Value: []byte(value), TimestampType: "unknown"}, core.Token{}); err != nil {
					t.Fatal(err)
				}
			}
			if err := fixture.Flush(); err != nil {
				t.Fatal(err)
			}
			fixture.Close()
			child := exec.Command(os.Args[0], "-test.run=^TestCrashAtEveryOutputInitializationBoundary$", "--", stage, src, dst, cpdir)
			child.Env = append(os.Environ(), "KAFERRY_INITIALIZATION_CRASH=1")

			// When
			data, err := child.CombinedOutput()

			// Then
			if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 99 {
				t.Fatalf("initialization child: %v %s", err, data)
			}
			cp, err := NewCheckpoints(cpdir, "initialization", DurableWriter{})
			if err != nil {
				t.Fatal(err)
			}
			defer cp.Close()
			source, err := NewSource(src, "fp", plan.WindowSpec{}, codec(t), cp, core.RealClock{})
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			id, extent := "", int64(0)
			if saved := cp.Existing(); saved != nil {
				id, extent = saved.DestinationID, saved.Extent
			} else if _, err := os.Stat(dst); err == nil {
				if out, err := NewOutput(NewPublisher(), dst, "initialization", "fp", "", 0, codec(t), 10, logger()); out != nil || err == nil {
					t.Fatal("restart guessed identity after a creation crash")
				}
				// The documented operator remedy is safe only for this header-only fixture.
				f, err := os.Open(dst)
				if err != nil {
					t.Fatal(err)
				}
				info, _ := f.Stat()
				_, end, err := codec(t).Header(f, info.Size())
				f.Close()
				if err != nil || end != info.Size() {
					t.Fatal("initialization published record data before its checkpoint", err)
				}
				if err := os.Remove(dst); err != nil {
					t.Fatal(err)
				}
			}
			out, err := NewOutput(NewPublisher(), dst, "initialization", "fp", id, extent, codec(t), 10, logger())
			if err != nil {
				t.Fatal(err)
			}
			defer out.Close()
			cp.BindOutput(out)
			r := core.NewRunner(source, out, cp, core.RealClock{}, logger(), plan.Plan{Source: plan.Endpoint{Kind: "FILE"}}, config.Defaults(dir).Worker, core.NewTracker(), core.NoopStopObserver{})
			if err := r.Run(context.Background()); err != nil {
				t.Fatal(err)
			}
			if values := outputValues(t, dst); !reflect.DeepEqual(values, []string{"a", "b", "c"}) {
				t.Fatal("initialization recovery lost or duplicated records", values)
			}
			stale, _ := filepath.Glob(filepath.Join(dir, temporaryPrefix(dst)+"*"))
			if len(stale) != 0 {
				t.Fatal("orphan header files remained after recovery", stale)
			}
		})
	}
}

func outputValues(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, _ := f.Stat()
	c := codec(t)
	_, position, err := c.Header(f, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	var values []string
	for position < info.Size() {
		payload, next, _, err := readFrame(f, position, info.Size(), c.FrameLimit())
		if err != nil {
			t.Fatal(err)
		}
		record, err := c.DecodeRecord(payload)
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, string(record.Value))
		position = next
	}
	return values
}

func TestZeroFilledOutputTailIsTruncatedOnlyBeyondDurableExtent(t *testing.T) {
	// Given
	for _, acknowledged := range []bool{false, true} {
		t.Run(map[bool]string{false: "unacknowledged", true: "acknowledged"}[acknowledged], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "out.cbor")

			// When
			out, err := NewOutput(NewPublisher(), path, "g", "fp", "", 0, codec(t), 10, logger())

			// Then
			if err != nil {
				t.Fatal(err)
			}
			id, start := out.DurableOutput()
			if err := out.Submit(sample(), core.Token{}); err != nil {
				t.Fatal(err)
			}
			if err := out.Flush(); err != nil {
				t.Fatal(err)
			}
			_, extent := out.DurableOutput()
			out.Close()
			f, err := os.OpenFile(path, os.O_RDWR, 0600)
			if err != nil {
				t.Fatal(err)
			}
			position := extent
			if acknowledged {
				position = start
			}
			if _, err := f.WriteAt(make([]byte, 32), position); err != nil {
				t.Fatal(err)
			}
			f.Close()
			out, err = NewOutput(NewPublisher(), path, "g", "fp", id, extent, codec(t), 10, logger())
			if acknowledged {
				if out != nil || err == nil {
					t.Fatal("zero-filled acknowledged record accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			out.Close()
			info, _ := os.Stat(path)
			if info.Size() != extent {
				t.Fatal("unacknowledged zeros were retained", info.Size(), extent)
			}
		})
	}
}

type blockedRename struct{ target string }

func (w blockedRename) Replace(_ string, data []byte) error {
	return (DurableWriter{}).Replace(w.target, data)
}

func TestCheckpointRenameFailurePreservesSavedPositionAndPendingFrames(t *testing.T) {
	// Given
	dir := t.TempDir()
	cp, err := NewCheckpoints(dir, "g", DurableWriter{})
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close()
	cp.Initialize(Checkpoint{Version: 2, OpeningSize: 10, Fingerprint: "fp", SourceID: "source", Position: 0})

	// When
	_, saveErr := cp.Save(context.Background(), []core.Cursor{{Partition: 0}})

	// Then
	if saveErr != nil {
		t.Fatal(saveErr)
	}
	before, err := os.ReadFile(cp.path)
	if err != nil {
		t.Fatal(err)
	}
	cp.RememberFrame(10, FrameIdentity{Position: 0, CRC: 123})
	blocked := filepath.Join(dir, "directory-target")
	if err := os.Mkdir(blocked, 0700); err != nil {
		t.Fatal(err)
	}
	cp.writer = blockedRename{target: blocked}
	if _, err := cp.Save(context.Background(), []core.Cursor{{Partition: 0, Position: 10}}); err == nil {
		t.Fatal("invalid checkpoint rename succeeded")
	}
	after, err := os.ReadFile(cp.path)
	if err != nil || !reflect.DeepEqual(before, after) || cp.Existing().Position != 0 || cp.frames[10] == nil {
		t.Fatal("rename failure advanced progress or discarded replay state", err)
	}
	cp.writer = DurableWriter{}
	if _, err := cp.Save(context.Background(), []core.Cursor{{Partition: 0, Position: 10}}); err != nil || cp.Existing().Position != 10 {
		t.Fatal("retry could not persist the preserved frame", err)
	}
	stale, _ := filepath.Glob(filepath.Join(dir, ".checkpoint-*"))
	if len(stale) != 0 {
		t.Fatal("failed rename leaked temporary checkpoints", stale)
	}
}
