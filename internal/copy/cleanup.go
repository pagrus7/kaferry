package copy

import (
	"errors"
	"fmt"
)

// CleanupResult reports cleanup failures without replacing the first exit code.
func CleanupResult(result error, stage string, cleanup error) error {
	if cleanup == nil {
		return result
	}
	if result == nil {
		return &Exit{Code: 1, Err: fmt.Errorf("%s: %w", stage, cleanup)}
	}
	var coded interface{ ExitCode() int }
	if !errors.As(result, &coded) {
		result = &Exit{Code: 1, Err: result}
	}
	return errors.Join(result, fmt.Errorf("%s: %w", stage, cleanup))
}
