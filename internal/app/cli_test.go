package app

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/file"
)

func TestCLIInputsAndExitCodes(t *testing.T) {
	// Given
	for _, args := range [][]string{{}, {"bad"}, {"--version", "run"}, {"run"}, {"run", "--sql", "SELECT *", "--file", "x"}, {"run", "--sql", "SELECT *"}, {"run", "--file", "-"}, {"run", "--unknown"}} {
		var out bytes.Buffer

		// When
		executeCode := Execute(context.Background(), args, nil, io.Discard, &out)

		// Then
		if executeCode != 2 {
			t.Fatalf("args=%v code=%d", args, executeCode)
		}
	}
	var out bytes.Buffer
	if code := Execute(context.Background(), []string{"run", "--help"}, nil, io.Discard, &out); code != 0 {
		t.Fatal("help failed")
	}
}

func TestCLIVersionWithoutConfigurationOrInput(t *testing.T) {
	// Given no home directory and a reader that cannot supply SQL.
	t.Setenv("HOME", "")
	var stdout, stderr bytes.Buffer
	_, native := kafka.LibraryVersion()
	want := "kaferry " + version + " (commit " + revision + "; " + runtime.Version() + "; librdkafka " + native + ")\n"

	// When requesting build information.
	code := Execute(t.Context(), []string{"--version"}, forbiddenInput{}, &stdout, &stderr)

	// Then report the build on stdout without loading configuration or reading SQL.
	if code != 0 || stdout.String() != want || stderr.Len() != 0 {
		t.Fatalf("exit=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

type forbiddenInput struct{}

func (forbiddenInput) Read([]byte) (int, error) { panic("version must not read SQL") }

func TestCLIVersionOutputFailure(t *testing.T) {
	// Given an output stream that has closed.
	var stderr bytes.Buffer

	// When requesting build information.
	code := Execute(t.Context(), []string{"--version"}, nil, closedOutput{}, &stderr)

	// Then report a failure instead of claiming the version was printed.
	if code != 1 || !strings.Contains(stderr.String(), "output closed") {
		t.Fatalf("exit=%d stderr=%q", code, stderr.String())
	}
}

type closedOutput struct{}

func (closedOutput) Write([]byte) (int, error) { return 0, errors.New("output closed") }
func TestFileCLIAndOneOffCleanup(t *testing.T) {
	for _, input := range []string{"stdin", "file"} {
		t.Run(input, func(t *testing.T) {
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

			inputPath := "-"
			var stdin io.Reader = strings.NewReader(statement)
			if input == "file" {
				stdin = nil // The SQL file must supply the statement independently.
				inputPath = filepath.Join(dir, "copy.sql")
				if err := os.WriteFile(inputPath, []byte(statement), 0600); err != nil {
					t.Fatal(err)
				}
			}
			// When running a file-to-file copy through the selected SQL input.
			executeCode := Execute(context.Background(), []string{"run", "-f", inputPath, "-c", cfg}, stdin, io.Discard, &out)

			// Then
			if executeCode != 0 {
				t.Fatalf("exit=%d: %s", executeCode, out.String())
			}
			files, err := filepath.Glob(filepath.Join(cpdir, "*.json"))
			if err != nil || len(files) != 0 {
				t.Fatal("successful one-off checkpoint not removed")
			}
			statement = "INSERT INTO FILE cbor('" + src + "') SELECT * FROM FILE cbor('" + src + "') OPTIONS(GROUP='self')"
			if code := Execute(context.Background(), []string{"run", "-e", statement, "-c", cfg}, nil, io.Discard, &out); code != 2 {
				t.Fatal("file self-copy was allowed")
			}
		})
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
			code := Execute(context.Background(), args, nil, io.Discard, &out)

			// Then startup diagnostics identify the required configuration.
			if code != 2 || !strings.Contains(out.String(), want) || strings.Contains(out.String(), "composition failed") {
				t.Fatalf("exit=%d: %s", code, out.String())
			}
		})
	}
}

func TestCLIRejectsUnavailableOrOversizedStatementInputs(t *testing.T) {
	for _, tc := range []struct {
		name  string
		args  []string
		stdin io.Reader
		want  string
	}{
		{"missing file", []string{"run", "-f", filepath.Join(t.TempDir(), "missing.sql")}, nil, "missing.sql"},
		{"oversized inline", []string{"run", "-e", strings.Repeat("x", (1<<20)+1)}, nil, "statement exceeds 1 MiB"},
		{"oversized stdin", []string{"run", "-f", "-"}, strings.NewReader(strings.Repeat("x", (1<<20)+1)), "statement exceeds 1 MiB"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given an inaccessible SQL file or input beyond the statement limit.
			var diagnostics bytes.Buffer

			// When executing the supplied statement input.
			code := Execute(t.Context(), tc.args, tc.stdin, io.Discard, &diagnostics)

			// Then fail as an input error before opening endpoints or creating progress.
			if code != 2 || !strings.Contains(diagnostics.String(), tc.want) {
				t.Fatalf("exit=%d: %s", code, diagnostics.String())
			}
		})
	}
}
