// Package config loads connection profiles and worker limits from TOML.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pelletier/go-toml/v2"
)

type Worker struct {
	MaxFrameBytes      int64   `toml:"max_frame_bytes"`
	PartitionRecordCap int     `toml:"partition_record_cap"`
	PartitionByteCap   int64   `toml:"partition_byte_cap"`
	RecordCap          int     `toml:"record_cap"`
	ByteCap            int64   `toml:"byte_cap"`
	DeliveryTimeout    string  `toml:"delivery_timeout"`
	ShutdownTimeout    string  `toml:"shutdown_timeout"`
	IdleExit           string  `toml:"idle_exit"`
	TipWait            string  `toml:"tip_wait"`
	CheckpointDir      *string `toml:"checkpoint_dir"` // Nil selects the default when opening a file source.
	ProgressInterval   string  `toml:"progress_interval"`
}

type Metrics struct {
	Enabled bool `toml:"enabled"`
}

const DefaultShutdownTimeout = 30 * time.Second

type Config struct {
	Connections plan.Profiles `toml:"connections"`
	Worker      Worker        `toml:"worker"`
	Metrics     Metrics       `toml:"metrics"`
}

type FileReader interface{ ReadFile(string) ([]byte, error) }
type Reader interface{ Load(string) (Config, error) }
type Loader struct {
	files FileReader
	home  string
}

func NewLoader(files FileReader, home string) *Loader { return &Loader{files: files, home: home} }

func Defaults(home string) Config {
	var checkpointDir *string
	if home != "" {
		directory := filepath.Join(home, ".kaferry", "checkpoints")
		checkpointDir = &directory
	}
	return Config{Connections: make(plan.Profiles), Worker: Worker{MaxFrameBytes: 64 << 20, PartitionRecordCap: 5000, PartitionByteCap: 16 << 20, RecordCap: 20000, ByteCap: 128 << 20, DeliveryTimeout: "60s", ShutdownTimeout: DefaultShutdownTimeout.String(), IdleExit: "30s", TipWait: "15m", CheckpointDir: checkpointDir, ProgressInterval: "30s"}}
}

func (l *Loader) Load(path string) (Config, error) {
	config := Defaults(l.home)
	if path == "" {
		return config, nil
	}
	data, err := l.files.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read configuration: %w", err)
	}
	if len(data) > 1<<20 {
		return Config{}, fmt.Errorf("configuration exceeds 1 MiB")
	}
	err = toml.NewDecoder(bytes.NewReader(data)).DisallowUnknownFields().Decode(&config)
	if err != nil {
		var syntax *toml.DecodeError
		if errors.As(err, &syntax) {
			row, column := syntax.Position()
			return Config{}, fmt.Errorf("invalid TOML configuration at line %d, column %d", row, column)
		}
		// Decoder errors may include source excerpts and secret values.
		return Config{}, fmt.Errorf("invalid TOML configuration")
	}
	if err = config.Worker.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (w Worker) Validate() error {
	if d, err := time.ParseDuration(w.ProgressInterval); err != nil || d < 0 {
		return fmt.Errorf("worker.progress_interval must be a nonnegative duration")
	}
	if w.MaxFrameBytes < 1 || w.MaxFrameBytes > 4294967295 {
		return fmt.Errorf("worker.max_frame_bytes must be between 1 and 4294967295 bytes")
	}
	if w.PartitionRecordCap <= 0 || w.PartitionByteCap <= 0 || w.RecordCap <= 0 || w.ByteCap <= 0 {
		return fmt.Errorf("worker record and byte caps must be positive")
	}
	for _, setting := range []struct{ name, value string }{{"delivery_timeout", w.DeliveryTimeout}, {"shutdown_timeout", w.ShutdownTimeout}, {"idle_exit", w.IdleExit}, {"tip_wait", w.TipWait}} {
		duration, err := time.ParseDuration(setting.value)
		if err != nil || duration <= 0 {
			return fmt.Errorf("worker.%s must be a positive duration", setting.name)
		}
		if setting.name == "delivery_timeout" && (duration < time.Millisecond || duration > 2147483647*time.Millisecond) {
			return fmt.Errorf("worker.delivery_timeout must be between 1ms and 2147483647ms")
		}
	}
	if w.CheckpointDir != nil && *w.CheckpointDir == "" {
		return fmt.Errorf("worker.checkpoint_dir must not be empty")
	}
	return nil
}
