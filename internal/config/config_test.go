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
	if c.Connections["prod"]["bootstrap.servers"] != "prod:9092" || c.Worker.PartitionRecordCap != 100 || c.Worker.PartitionByteCap != 16<<20 || c.Worker.IdleExit != "5s" || c.Worker.CheckpointDir != filepath.Join("/home/test", ".kaferry", "checkpoints") {
		t.Fatalf("incorrect config defaults or overrides: %+v", c.Worker)
	}
	for _, input := range []string{"unknown=1", "[worker]\nrecord_cap=0", "[worker]\nidle_exit='0s'", "[worker]\nunknown=3", "[connections.prod]\n'sasl.password'= 'unique-secret' trailing", "[connections.prod]\n'bootstrap.servers'=123"} {
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
