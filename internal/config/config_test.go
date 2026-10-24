package config

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDocumentedConfiguration(t *testing.T) {
	// Given published TOML snippets, including partial overrides.
	data, err := os.ReadFile("../../docs/usage.md")
	require.NoError(t, err)
	examples := strings.Split(string(data), "```toml\n")[1:]
	require.NotEmpty(t, examples, "usage guide must contain TOML examples")
	for index, part := range examples {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			// When loading with the production decoder and defaults.
			input := strings.SplitN(part, "```", 2)[0]
			_, err := NewLoader(&readerMock{data: []byte(input)}, "/home/example").Load("example.toml")
			// Then invalid keys, types or worker settings fail the docs check.
			require.NoError(t, err)
		})
	}
}

func TestConfiguration(t *testing.T) {
	// Given
	input := "[connections.prod]\n'bootstrap.servers'='prod:9092'\n'sasl.password'='secret'\n[worker]\npartition_record_cap=100\nidle_exit='5s'\nmax_frame_bytes=8192\n"

	// When
	c, err := NewLoader(&readerMock{data: []byte(input)}, "/home/test").Load("config.toml")

	// Then
	require.NoError(t, err)
	require.Equal(t, "prod:9092", c.Connections["prod"]["bootstrap.servers"])
	require.EqualValues(t, 100, c.Worker.PartitionRecordCap)
	require.Equal(t, int64(16<<20), c.Worker.PartitionByteCap)
	require.Equal(t, "5s", c.Worker.IdleExit)
	require.EqualValues(t, 8192, c.Worker.ArchiveBlockMaxBytes)
	require.NotNil(t, c.Worker.CheckpointDir)
	require.Equal(t, filepath.Join("/home/test", ".kaferry", "checkpoints"), *c.Worker.CheckpointDir)
	for _, input := range []string{"unknown=1", "[worker]\nrecord_cap=0", "[worker]\nidle_exit='0s'", "[worker]\ncheckpoint_dir=''", "[worker]\nunknown=3", "[connections.prod]\n'sasl.password'= 'unique-secret' trailing", "[connections.prod]\n'bootstrap.servers'=123"} {
		_, err := NewLoader(&readerMock{data: []byte(input)}, "/home/test").Load("config.toml")
		require.Error(t, err)
		require.NotContains(t, err.Error(), "unique-secret")
	}
}

type readerMock struct {
	data  []byte
	err   error
	paths []string
}

func (r *readerMock) ReadFile(path string) ([]byte, error) {
	r.paths = append(r.paths, path)
	return r.data, r.err
}
func TestLoaderUsesInjectedReaderAndAvoidsReadsForDefaults(t *testing.T) {
	// Given
	files := &readerMock{data: []byte("[worker]\nrecord_cap=17\n")}
	var loader Reader = NewLoader(files, "home")

	// When
	_, loadErr := loader.Load("")

	// Then
	require.NoError(t, loadErr)
	require.Len(t, files.paths, 0)
	cfg, err := loader.Load("chosen.toml")
	require.NoError(t, err)
	require.EqualValues(t, 17, cfg.Worker.RecordCap)
	require.Len(t, files.paths, 1)
	require.Equal(t, "chosen.toml", files.paths[0])
	files.err = io.ErrUnexpectedEOF
	_, err = loader.Load("chosen.toml")
	require.ErrorIs(t, err, files.err)
}

func TestDeliveryTimeoutFitsNativeMillisecondRange(t *testing.T) {
	for _, scenario := range []struct {
		value string
		valid bool
	}{
		{"500us", false}, {"1ms", true}, {"60s", true},
		{"2147483647ms", true}, {"2147483648ms", false},
	} {
		t.Run(scenario.value, func(t *testing.T) {
			// Given a worker duration at a native producer boundary.
			worker := Defaults("home").Worker
			worker.DeliveryTimeout = scenario.value

			// When validating worker tuning.
			err := worker.Validate()

			// Then positive sub-millisecond and overflowing deadlines fail.
			require.Equal(t, scenario.valid, (err == nil))
		})
	}
}

func TestLoaderDefersDefaultCheckpointDirectoryWithoutHome(t *testing.T) {
	for _, input := range []string{"", "[worker]\nrecord_cap=17\n", "[worker]\ncheckpoint_dir='/chosen/checkpoints'\n", "[worker]\ncheckpoint_dir=''\n"} {
		t.Run(input, func(t *testing.T) {
			// Given configuration without an available home directory.
			loader := NewLoader(&readerMock{data: []byte(input)}, "")

			// When loading defaults or TOML overrides.
			cfg, err := loader.Load("chosen.toml")

			// Then omitted defaults stay unresolved; explicit empty settings fail.
			if strings.Contains(input, "checkpoint_dir=''") {
				require.Error(t, err)
				require.Contains(t, err.Error(), "worker.checkpoint_dir must not be empty")
				return
			}
			require.NoError(t, err)
			if strings.Contains(input, "checkpoint_dir") {
				require.NotNil(t, cfg.Worker.CheckpointDir)
				require.Equal(t, "/chosen/checkpoints", *cfg.Worker.CheckpointDir)
			} else {
				require.Nil(t, cfg.Worker.CheckpointDir)
			}
		})
	}
}

func TestObservabilityDefaultsAndOverrides(t *testing.T) {
	for _, tc := range []struct {
		input          string
		valid, enabled bool
		interval       string
	}{
		{"", true, false, "30s"},
		{"[metrics]\nenabled=true\n[worker]\nprogress_interval='0'", true, true, "0"},
		{"[worker]\nprogress_interval='5s'", true, false, "5s"},
		{"[worker]\nprogress_interval='-1s'", false, false, ""},
		{"[metrics]\nlisten='localhost:1234'", false, false, ""},
	} {
		t.Run(tc.input, func(t *testing.T) {
			// Given TOML controlling metrics independently from progress logs.
			cfg, err := NewLoader(&readerMock{data: []byte(tc.input)}, "home").Load("config.toml")
			// When loading, then defaults are safe and unsupported or negative settings fail.
			require.Equal(t, tc.valid, (err == nil))
			if tc.valid {
				require.Equal(t, tc.enabled, cfg.Metrics.Enabled)
				require.Equal(t, tc.interval, cfg.Worker.ProgressInterval)
			}
		})
	}
}

func TestLoaderEnforcesConfigurationSizeAtTheReadBoundary(t *testing.T) {
	for _, size := range []int{1 << 20, (1 << 20) + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			// Given valid TOML padded with a comment to the file size limit.
			content := "[worker]\nrecord_cap=17\n#"
			files := &readerMock{data: []byte(content + strings.Repeat("x", size-len(content)))}
			// When loading through the injected file boundary.
			cfg, err := NewLoader(files, "home").Load("config.toml")
			// Then exactly 1 MiB is accepted and one extra byte is rejected.
			require.Equal(t, []string{"config.toml"}, files.paths)
			if size > 1<<20 {
				require.EqualError(t, err, "configuration exceeds 1 MiB")
			} else {
				require.NoError(t, err)
				require.Equal(t, 17, cfg.Worker.RecordCap)
			}
		})
	}
}

func TestArchiveBlockSizeBoundaries(t *testing.T) {
	for _, size := range []int64{0, 1, 4294967295, 4294967296} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			// Given a decoded block size at each representable boundary.
			worker := Defaults("home").Worker
			worker.ArchiveBlockMaxBytes = size
			// When validating the worker.
			err := worker.Validate()
			// Then only positive values that fit the archive block limit are accepted.
			if size == 0 || size > 4294967295 {
				require.EqualError(t, err, "worker.max_frame_bytes must be between 1 and 4294967295 bytes")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestWorkerCapsMustEachBePositive(t *testing.T) {
	for _, setting := range []string{"partition_record_cap", "partition_byte_cap", "record_cap", "byte_cap"} {
		for _, value := range []int64{0, -1} {
			t.Run(fmt.Sprintf("%s=%d", setting, value), func(t *testing.T) {
				// Given one invalid cap and valid defaults for all other settings.
				worker := Defaults("home").Worker
				switch setting {
				case "partition_record_cap":
					worker.PartitionRecordCap = int(value)
				case "partition_byte_cap":
					worker.PartitionByteCap = value
				case "record_cap":
					worker.RecordCap = int(value)
				case "byte_cap":
					worker.ByteCap = value
				}
				// When validating the worker.
				err := worker.Validate()
				// Then each cap is independently required to be positive.
				require.EqualError(t, err, "worker record and byte caps must be positive")
			})
		}
	}
}
