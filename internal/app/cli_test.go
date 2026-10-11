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

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/file"
)

func TestCLIInputsAndExitCodes(t *testing.T) {
	// Given
	for _, args := range [][]string{{}, {"bad"}, {"run"}, {"run", "--sql", "SELECT *", "--file", "x"}, {"run", "--sql", "SELECT *"}, {"run", "--file", "-"}, {"run", "--unknown"}} {
		var out bytes.Buffer

		// When
		executeCode := Execute(context.Background(), args, nil, &out)

		// Then
		if executeCode != 2 {
			t.Fatalf("args=%v code=%d", args, executeCode)
		}
	}
	var out bytes.Buffer
	if code := Execute(context.Background(), []string{"run", "--help"}, nil, &out); code != 0 {
		t.Fatal("help failed")
	}
}
func TestFileCLIAndOneOffCleanup(t *testing.T) {
	// Given
	t.Setenv("HOME", "") // Explicit configuration must not require a home directory.
	dir := t.TempDir()
	src, dst := filepath.Join(dir, "source.cbor"), filepath.Join(dir, "destination.cbor")
	codec, err := file.NewCodec(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	o, err := file.NewOutput(file.NewPublisher(), src, "fixture", "fixture", "", 0, codec, 10, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
	cpdir := t.TempDir()
	cfg := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(cfg, []byte("[worker]\ncheckpoint_dir = '"+cpdir+"'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	statement := "INSERT INTO FILE cbor('" + dst + "') SELECT * FROM FILE cbor('" + src + "') OPTIONS(GROUP=ONE_OFF)"
	var out bytes.Buffer

	// When
	executeCode := Execute(context.Background(), []string{"run", "-f", "-", "-c", cfg}, strings.NewReader(statement), &out)

	// Then
	if executeCode != 0 {
		t.Fatalf("exit=%d: %s", executeCode, out.String())
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

func TestCLIMissingHomeReportsConfigurationErrors(t *testing.T) {
	for _, configured := range []bool{false, true} {
		t.Run(map[bool]string{false: "default directory", true: "missing configuration"}[configured], func(t *testing.T) {
			// Given a valid file-copy statement and no home directory.
			t.Setenv("HOME", "")
			statement := "INSERT INTO FILE cbor('/unused-output.cbor') SELECT * FROM FILE cbor('/unused-source.cbor') OPTIONS(GROUP='restore')"
			args := []string{"run", "--sql", statement}
			want := "set HOME or worker.checkpoint_dir"
			if configured {
				args = append(args, "--config", filepath.Join(t.TempDir(), "missing.toml"))
				want = "read configuration"
			}
			var out bytes.Buffer

			// When the CLI loads configuration and opens the source.
			code := Execute(context.Background(), args, nil, &out)

			// Then startup diagnostics identify the required configuration.
			if code != 2 || !strings.Contains(out.String(), want) || strings.Contains(out.String(), "composition failed") {
				t.Fatalf("exit=%d: %s", code, out.String())
			}
		})
	}
}
