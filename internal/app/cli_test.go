package app

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	core "kafka-fwd2/internal/copy"
	"kafka-fwd2/internal/file"
)

func TestCLIInputsAndExitCodes(t *testing.T) {
	for _, args := range [][]string{{}, {"bad"}, {"run"}, {"run", "--sql", "SELECT *", "--file", "x"}, {"run", "--sql", "SELECT *"}, {"run", "--file", "-"}, {"run", "--unknown"}} {
		var out bytes.Buffer
		if code := Execute(context.Background(), args, nil, &out); code != 2 {
			t.Fatalf("args=%v code=%d", args, code)
		}
	}
	var out bytes.Buffer
	if code := Execute(context.Background(), []string{"run", "--help"}, nil, &out); code != 0 {
		t.Fatal("help failed")
	}
}
func TestFileCLIAndOneOffCleanup(t *testing.T) {
	dir := t.TempDir()
	src, dst := filepath.Join(dir, "source.cbor"), filepath.Join(dir, "destination.cbor")
	codec, err := file.NewCodec(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	o, err := file.NewOutput(src, "fixture", "fixture", "", 0, codec, 10, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Submit(core.Record{Key: []byte{}, Value: nil, TimestampType: "unknown"}, core.Token{}); err != nil {
		t.Fatal(err)
	}
	if err := o.Flush(); err != nil {
		t.Fatal(err)
	}
	o.Close()
	cpdir := filepath.Join(dir, "checkpoints")
	cfg := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(cfg, []byte("[worker]\ncheckpoint_dir = '"+cpdir+"'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	statement := "INSERT INTO FILE cbor('" + dst + "') SELECT * FROM FILE cbor('" + src + "') OPTIONS(GROUP=ONE_OFF)"
	var out bytes.Buffer
	if code := Execute(context.Background(), []string{"run", "-f", "-", "-c", cfg}, strings.NewReader(statement), &out); code != 0 {
		t.Fatalf("exit=%d: %s", code, out.String())
	}
	files, err := filepath.Glob(filepath.Join(cpdir, "*.json"))
	if err != nil || len(files) != 0 {
		t.Fatal("successful one-off checkpoint not removed")
	}
	statement = "INSERT INTO FILE cbor('" + src + "') SELECT * FROM FILE cbor('" + src + "') OPTIONS(GROUP='self')"
	if code := Execute(context.Background(), []string{"run", "-e", statement, "-c", cfg}, nil, &out); code != 2 {
		t.Fatal("file self-copy was allowed")
	}
}
