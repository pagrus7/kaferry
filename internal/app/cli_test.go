package app

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"testing/iotest"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
)

func TestCLIInputsAndExitCodes(t *testing.T) {
	// Given
	for _, args := range [][]string{{}, {"bad"}, {"--version", "run"}, {"run"}, {"run", "--sql", "SELECT *", "--file", "x"}, {"run", "--sql", "SELECT *"}, {"run", "--file", "-"}, {"run", "--unknown"}} {
		var out bytes.Buffer

		// When
		executeCode := Execute(context.Background(), args, nil, io.Discard, &out)

		// Then
		require.EqualValues(t, 2, executeCode)
	}
	var out bytes.Buffer
	require.Zero(t, Execute(context.Background(), []string{"run", "--help"}, nil, io.Discard, &out))
}

func TestCLIVersionWithoutConfigurationOrInput(t *testing.T) {
	// Given release metadata, no home directory and a reader that cannot supply SQL.
	originalVersion, originalRevision := version, revision
	t.Cleanup(func() { version, revision = originalVersion, originalRevision })
	version, revision = "v0.2.3", "release-commit"
	t.Setenv("HOME", "")
	var stdout, stderr bytes.Buffer
	want := "kaferry v0.2.3 (commit release-commit; " + runtime.Version() + "; franz-go "

	// When requesting build information.
	code := Execute(t.Context(), []string{"--version"}, forbiddenInput{}, &stdout, &stderr)

	// Then report the build on stdout without loading configuration or reading SQL.
	require.Zero(t, code)
	require.True(t, strings.HasPrefix(stdout.String(), want))
	require.True(t, strings.HasSuffix(stdout.String(), ")\n"))
	require.Zero(t, stderr.Len())
}

type forbiddenInput struct{}

func (forbiddenInput) Read([]byte) (int, error) { panic("version must not read SQL") }

func TestCLIVersionOutputFailure(t *testing.T) {
	// Given an output stream that has closed.
	var stderr bytes.Buffer

	// When requesting build information.
	code := Execute(t.Context(), []string{"--version"}, nil, closedOutput{}, &stderr)

	// Then report a failure instead of claiming the version was printed.
	require.EqualValues(t, 1, code)
	require.Contains(t, stderr.String(), "output closed")
}

type closedOutput struct{}

func (closedOutput) Write([]byte) (int, error) { return 0, errors.New("output closed") }
func TestFileCLIAndOneOffCleanup(t *testing.T) {
	for _, input := range []string{"stdin", "file"} {
		t.Run(input, func(t *testing.T) {
			// Given
			t.Setenv("HOME", "") // Explicit configuration must not require a home directory.
			dir := t.TempDir()
			src, dst := filepath.Join(dir, "source.avro"), filepath.Join(dir, "destination.avro")
			writeArchiveRecords(t, src, []core.Record{{Key: []byte{}, Value: nil, TimestampType: "unknown"}})
			cpdir := t.TempDir()
			cfg := filepath.Join(dir, "config.toml")
			require.NoError(t, os.WriteFile(cfg, []byte("[worker]\ncheckpoint_dir = '"+cpdir+"'\n"), 0600))
			statement := "INSERT INTO FILE avro('" + dst + "') SELECT * FROM FILE avro('" + src + "')"
			var out bytes.Buffer

			inputPath := "-"
			var stdin io.Reader = strings.NewReader(statement)
			if input == "file" {
				stdin = nil // The SQL file must supply the statement independently.
				inputPath = filepath.Join(dir, "copy.sql")
				require.NoError(t, os.WriteFile(inputPath, []byte(statement), 0600))
			}
			// When running a file-to-file copy through the selected SQL input.
			executeCode := Execute(context.Background(), []string{"run", "-f", inputPath, "-c", cfg}, stdin, io.Discard, &out)

			// Then
			require.Zero(t, executeCode)
			copied := archiveRecords(t, dst)
			require.Len(t, copied, 1)
			require.Equal(t, []byte{}, copied[0].Key, "empty keys must survive the default copy")
			require.Nil(t, copied[0].Value, "null values must survive the default copy")
			files, err := filepath.Glob(filepath.Join(cpdir, "*.json"))
			require.NoError(t, err)
			require.Len(t, files, 0)
			statement = "INSERT INTO FILE avro('" + src + "') SELECT * FROM FILE avro('" + src + "') WITH(GROUP='self')"
			require.EqualValues(t, 2, Execute(context.Background(), []string{"run", "-e", statement, "-c", cfg}, nil, io.Discard, &out))
		})
	}
}

func TestCLIMissingHomeReportsConfigurationErrors(t *testing.T) {
	for _, command := range []string{"run", "explain", "status"} {
		for _, configured := range []bool{false, true} {
			t.Run(command+"/"+map[bool]string{false: "default directory", true: "missing configuration"}[configured], func(t *testing.T) {
				// Given a valid file-copy statement and no home directory.
				t.Setenv("HOME", "")
				statement := "INSERT INTO FILE avro('/unused-output.avro') SELECT * FROM FILE avro('/unused-source.avro') WITH (GROUP='restore') "
				args := []string{command, "--sql", statement}
				want := "set HOME or worker.checkpoint_dir"
				if configured {
					args = append(args, "--config", filepath.Join(t.TempDir(), "missing.toml"))
					want = "read configuration"
				}
				var stdout, stderr bytes.Buffer
				// When executing, then report the configuration error without a status verdict or preview.
				code := Execute(t.Context(), args, nil, &stdout, &stderr)
				require.EqualValues(t, 2, code)
				require.Empty(t, stdout.String())
				require.Contains(t, stderr.String(), want)
				require.NotContains(t, stderr.String(), "composition failed")
			})
		}
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
			require.EqualValues(t, 2, code)
			require.Contains(t, diagnostics.String(), tc.want)
		})
	}
}

func TestCLISQLDiagnosticsIdentifyInputWithoutCreatingOutput(t *testing.T) {
	for _, input := range []string{"inline", "file", "stdin", "file validation"} {
		t.Run(input, func(t *testing.T) {
			// Given malformed SQL, explicit configuration and an output that does not exist.
			t.Setenv("HOME", "")
			dir := t.TempDir()
			destination := filepath.Join(dir, "output.avro")
			query := "INSERT INTO FILE avro('" + destination + "')\nSELECT 'sensitive_SENTINEL_5c27' FROM FILE avro('missing.avro')\nWITH(GROUP='g')"
			want := "line 2, column 8: expected \"*\"; found string literal"
			if input == "file validation" {
				query = strings.Replace(query, "'sensitive_SENTINEL_5c27'", "*", 1)
				query = strings.Replace(query, "GROUP='g'", "START=EARLIEST", 1)
				want = "line 3, column 6: START requires a topic source; remove START for FILE sources"
			}
			config := filepath.Join(dir, "config.toml")
			require.NoError(t, os.WriteFile(config, []byte("[worker]\ncheckpoint_dir = '"+dir+"'\n"), 0600))
			args := []string{"run", "-c", config}
			var stdin io.Reader
			source := ""
			switch input {
			case "inline":
				args = append(args, "-e", query)
			case "stdin":
				args = append(args, "-f", "-")
				stdin, source = strings.NewReader(query), "<stdin>"
			default:
				source = filepath.Join(dir, "copy.sql")
				require.NoError(t, os.WriteFile(source, []byte(query), 0600))
				args = append(args, "-f", source)
			}
			var stdout, stderr bytes.Buffer
			// When running the public command.
			code := Execute(t.Context(), args, stdin, &stdout, &stderr)
			// Then exit 2 with source/location and no credentials, copies or checkpoints.
			expected := want + "\n"
			if source != "" {
				expected = "\"" + source + "\": " + expected
			}
			require.EqualValues(t, 2, code)
			require.Zero(t, stdout.Len())
			require.Equal(t, expected, stderr.String())
			require.NotContains(t, stderr.String(), "sensitive_SENTINEL_5c27")
			{
				_, err := os.Stat(destination)
				require.True(t, os.IsNotExist(err))
			}
			checkpoints, err := filepath.Glob(filepath.Join(dir, "*.json"))
			require.NoError(t, err)
			require.Len(t, checkpoints, 0)
		})
	}
}

func TestSQLDiagnosticSourceLabelsEscapeControlCharacters(t *testing.T) {
	// Given a SQL input name containing quotes, a newline and terminal escape.
	source := "copy\"\n\x1b.sql"
	err := sql.At(sql.Position{Line: 2, Column: 8}, errors.New("invalid value"))
	var output bytes.Buffer
	// When rendering the diagnostic's source label.
	printCommandError(&output, source, err)
	// Then keep the error on one line without embedding terminal controls.
	want := strconv.Quote(source) + ": line 2, column 8: invalid value\n"
	require.Equal(t, want, output.String())
	require.EqualValues(t, 1, strings.Count(output.String(), "\n"))
	require.False(t, strings.ContainsRune(output.String(), '\x1b'))
}

func TestCLIRejectsReadFailureAfterValidSQL(t *testing.T) {
	// Given complete valid SQL followed by a reader failure.
	fixture := filePreview(t)
	before := fileTree(t, fixture.directory)
	failure := errors.New("SQL read sentinel")
	input := io.MultiReader(strings.NewReader(fixture.query), iotest.ErrReader(failure))
	var stdout, stderr bytes.Buffer
	// When reading SQL from stdin through the public command.
	code := Execute(t.Context(), []string{"run", "-f", "-", "-c", fixture.config}, input, &stdout, &stderr)
	// Then the read error prevents execution, output and checkpoint creation.
	require.Equal(t, 2, code)
	require.Empty(t, stdout.String())
	require.Equal(t, failure.Error()+"\n", stderr.String())
	require.Equal(t, before, fileTree(t, fixture.directory))
}
