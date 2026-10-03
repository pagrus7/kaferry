package app

import (
	copy "kafka-fwd2/internal/copy"
	"kafka-fwd2/internal/file"
	"log/slog"
)

type FileFactory struct {
	clock  copy.Clock
	logger *slog.Logger
	writer file.AtomicWriter
}

func NewFileFactory(clock copy.Clock, logger *slog.Logger, writer file.AtomicWriter) *FileFactory {
	return &FileFactory{clock: clock, logger: logger, writer: writer}
}
