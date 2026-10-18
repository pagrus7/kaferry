package config

import (
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfiguration(t *testing.T) {
	// Given
	input := "[connections.prod]\n'bootstrap.servers'='prod:9092'\n'sasl.password'='secret'\n[worker]\npartition_record_cap=100\nidle_exit='5s'\n"

	// When
	c, err := Decode(strings.NewReader(input), "/home/test")

	// Then
	if err != nil {
		t.Fatal(err)
	}
	if c.Connections["prod"]["bootstrap.servers"] != "prod:9092" || c.Worker.PartitionRecordCap != 100 || c.Worker.PartitionByteCap != 16<<20 || c.Worker.IdleExit != "5s" || c.Worker.CheckpointDir == nil || *c.Worker.CheckpointDir != filepath.Join("/home/test", ".kaferry", "checkpoints") {
		t.Fatalf("incorrect config defaults or overrides: %+v", c.Worker)
	}
	for _, input := range []string{"unknown=1", "[worker]\nrecord_cap=0", "[worker]\nidle_exit='0s'", "[worker]\ncheckpoint_dir=''", "[worker]\nunknown=3", "[connections.prod]\n'sasl.password'= 'unique-secret' trailing", "[connections.prod]\n'bootstrap.servers'=123"} {
		_, err := Decode(strings.NewReader(input), "/home/test")
		if err == nil || strings.Contains(err.Error(), "unique-secret") {
			t.Errorf("invalid or unsafe diagnostic: %v", err)
		}
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
	if loadErr != nil || len(files.paths) != 0 {
		t.Fatal("defaults unexpectedly read a file", loadErr)
	}
	cfg, err := loader.Load("chosen.toml")
	if err != nil || cfg.Worker.RecordCap != 17 || len(files.paths) != 1 || files.paths[0] != "chosen.toml" {
		t.Fatal("reader boundary failed", cfg.Worker, files.paths, err)
	}
	files.err = io.ErrUnexpectedEOF
	if _, err := loader.Load("chosen.toml"); !errors.Is(err, files.err) {
		t.Fatal("read failure lost", err)
	}
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
			if (err == nil) != scenario.valid {
				t.Fatal("incorrect timeout validation", err)
			}
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
				if err == nil || !strings.Contains(err.Error(), "worker.checkpoint_dir must not be empty") {
					t.Fatal("explicit empty directory accepted", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(input, "checkpoint_dir") {
				if cfg.Worker.CheckpointDir == nil || *cfg.Worker.CheckpointDir != "/chosen/checkpoints" {
					t.Fatal("explicit directory lost")
				}
			} else if cfg.Worker.CheckpointDir != nil {
				t.Fatalf("default resolved prematurely: %q", *cfg.Worker.CheckpointDir)
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
			cfg, err := Decode(strings.NewReader(tc.input), "home")
			// When loading, then defaults are safe and unsupported or negative settings fail.
			if (err == nil) != tc.valid {
				t.Fatal(cfg, err)
			}
			if tc.valid && (cfg.Metrics.Enabled != tc.enabled || cfg.Worker.ProgressInterval != tc.interval) {
				t.Fatal(cfg)
			}
		})
	}
}
