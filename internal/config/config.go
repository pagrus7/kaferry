// Package config loads connection profiles and worker limits from TOML.
package config

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pelletier/go-toml/v2"
)

type Worker struct {
	MaxFrameBytes      int64  `toml:"max_frame_bytes"`
	PartitionRecordCap int    `toml:"partition_record_cap"`
	PartitionByteCap   int64  `toml:"partition_byte_cap"`
	RecordCap          int    `toml:"record_cap"`
	ByteCap            int64  `toml:"byte_cap"`
	DeliveryTimeout    string `toml:"delivery_timeout"`
	IdleExit           string `toml:"idle_exit"`
	TipWait            string `toml:"tip_wait"`
	CheckpointDir      string `toml:"checkpoint_dir"`
	checkpointHome     string
}

type Config struct {
	Connections plan.Profiles `toml:"connections"`
	Worker      Worker        `toml:"worker"`
}

type FileReader interface{ ReadFile(string) ([]byte, error) }
type Reader interface{ Load(string) (Config, error) }
type Loader struct {
	files FileReader
	home  string
}

func NewLoader(files FileReader, home string) *Loader { return &Loader{files: files, home: home} }

func Defaults(home string) Config {
	return Config{Connections: make(plan.Profiles), Worker: Worker{MaxFrameBytes: 64 << 20, PartitionRecordCap: 5000, PartitionByteCap: 16 << 20, RecordCap: 20000, ByteCap: 128 << 20, DeliveryTimeout: "60s", IdleExit: "30s", TipWait: "15m", CheckpointDir: filepath.Join(home, ".kaferry", "checkpoints"), checkpointHome: home}}
}

// LegacyCheckpointDir enables the upgrade guard only at the default location.
func (w Worker) LegacyCheckpointDir() string {
	if w.checkpointHome == "" || filepath.Clean(w.CheckpointDir) != filepath.Join(w.checkpointHome, ".kaferry", "checkpoints") {
		return ""
	}
	return filepath.Join(w.checkpointHome, ".kafka-fwd2", "checkpoints")
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
	err = toml.NewDecoder(strings.NewReader(string(data))).DisallowUnknownFields().Decode(&config)
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
	if w.MaxFrameBytes < 1 || w.MaxFrameBytes > 4294967295 {
		return fmt.Errorf("worker.max_frame_bytes must fit a positive u32")
	}
	if w.PartitionRecordCap <= 0 || w.PartitionByteCap <= 0 || w.RecordCap <= 0 || w.ByteCap <= 0 {
		return fmt.Errorf("worker record and byte caps must be positive")
	}
	for _, setting := range []struct{ name, value string }{{"delivery_timeout", w.DeliveryTimeout}, {"idle_exit", w.IdleExit}, {"tip_wait", w.TipWait}} {
		duration, err := time.ParseDuration(setting.value)
		if err != nil || duration <= 0 {
			return fmt.Errorf("worker.%s must be a positive duration", setting.name)
		}
		if setting.name == "delivery_timeout" && (duration < time.Millisecond || duration > 2147483647*time.Millisecond) {
			return fmt.Errorf("worker.delivery_timeout must be between 1ms and 2147483647ms")
		}
	}
	if w.CheckpointDir == "" {
		return fmt.Errorf("worker.checkpoint_dir must not be empty")
	}
	return nil
}

// Decode is useful for config sources that already own a reader.
func Decode(reader io.Reader, home string) (Config, error) {
	data, err := io.ReadAll(io.LimitReader(reader, (1<<20)+1))
	if err != nil {
		return Config{}, err
	}
	return NewLoader(memoryFile(data), home).Load("config")
}

type memoryFile []byte

func (m memoryFile) ReadFile(string) ([]byte, error) { return m, nil }
