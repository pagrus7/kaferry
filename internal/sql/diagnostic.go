package sql

import (
	"errors"
	"fmt"
)

// Position uses one-based lines and rune columns; tabs count as one column.
type Position struct {
	Line   int
	Column int
}

// Diagnostic locates an error without retaining or displaying SQL source text.
type Diagnostic struct {
	Position Position
	Err      error
}

func (d *Diagnostic) Error() string {
	if d.Position.Line == 0 {
		return d.Err.Error()
	}
	return fmt.Sprintf("line %d, column %d: %s", d.Position.Line, d.Position.Column, d.Err)
}

func (d *Diagnostic) Unwrap() error { return d.Err }

// At preserves the original cause and the most specific existing location.
func At(position Position, err error) error {
	if err == nil {
		return nil
	}
	var diagnostic *Diagnostic
	if errors.As(err, &diagnostic) && diagnostic.Position.Line != 0 {
		return err
	}
	return &Diagnostic{Position: position, Err: err}
}
