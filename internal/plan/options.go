package plan

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pagrus7/kaferry/internal/sql"
)

func applyOptions(options []sql.Option, scope sql.Scope, apply func(sql.Option) error) error {
	seen := make(map[string]sql.Position)
	for _, option := range options {
		if err := sql.CheckScope(option.Name, scope); err != nil {
			return sql.At(option.Position, err)
		}
		if first, exists := seen[option.Name]; exists {
			return sql.At(option.Position, fmt.Errorf("option %s repeats; specify it once%s", option.Name, sql.Related(first, "first declaration")))
		}
		if first, conflict := conflictingPolicy(option.Name, seen); conflict {
			return sql.At(option.Position, fmt.Errorf("ON_ERROR cannot be combined with specific error policies; choose ON_ERROR or the specific policies%s", sql.Related(first, "conflicting declaration")))
		}
		seen[option.Name] = option.Position
		if err := apply(option); err != nil {
			return sql.At(option.ValuePosition, err)
		}
	}
	return nil
}

func conflictingPolicy(name string, seen map[string]sql.Position) (sql.Position, bool) {
	switch name {
	case "ON_ERROR":
		if first, exists := seen["ON_MISSING_TIMESTAMP"]; exists {
			return first, true
		}
		first, exists := seen["ON_REJECTED_TIMESTAMP"]
		return first, exists
	case "ON_MISSING_TIMESTAMP", "ON_REJECTED_TIMESTAMP":
		first, exists := seen["ON_ERROR"]
		return first, exists
	}
	return sql.Position{}, false
}

func optionPosition(options []sql.Option, name string, fallback sql.Position) sql.Position {
	for _, option := range options {
		if option.Name == name {
			return option.Position
		}
	}
	return fallback
}

func (p *Plan) applyOption(option sql.Option) error {
	switch option.Name {
	case "GROUP":
		return p.groupOption(option)
	case "START":
		if option.Value != "EARLIEST" && option.Value != "LATEST" {
			return fmt.Errorf("START must be EARLIEST or LATEST")
		}
		p.Start = option.Value
	case "MAX_STALENESS", "MAX_OUT_OF_ORDER":
		return p.stalenessOption(option)
	case "WORKER_STOP_AFTER_RECORDS":
		value, err := strconv.ParseUint(option.Value, 10, 64)
		if err != nil || value == 0 || value > 1<<53 {
			return fmt.Errorf("WORKER_STOP_AFTER_RECORDS must be an integer from 1 to 9007199254740992")
		}
		p.WorkerStopAfterRecords = value
	case "ON_ERROR", "ON_MISSING_TIMESTAMP", "ON_REJECTED_TIMESTAMP":
		return p.policyOption(option)
	}
	return nil
}

func (p *Plan) groupOption(option sql.Option) error {
	if strings.TrimSpace(option.Value) == "" {
		return fmt.Errorf("GROUP must not be blank; use a quoted name or ONE_OFF")
	}
	p.Group = option.Value
	p.OneOff = !option.Quoted && option.Value == "ONE_OFF"
	return nil
}

func (p *Plan) stalenessOption(option sql.Option) error {
	if option.Name == "MAX_OUT_OF_ORDER" {
		if p.Window.Upper == nil {
			return sql.At(option.Position, fmt.Errorf("MAX_OUT_OF_ORDER requires an upper timestamp bound in WHERE"))
		}
		if p.HasOffsetEnd() {
			return sql.At(option.Position, fmt.Errorf("MAX_OUT_OF_ORDER cannot be combined with an explicit offset end"))
		}
		value, err := allowance(option)
		if err == nil {
			p.MaxOutOfOrderMS = &value
		}
		return err
	}
	if p.Source.Kind != "TOPIC" {
		return sql.At(option.Position, fmt.Errorf("MAX_STALENESS requires a topic source; remove MAX_STALENESS for FILE sources"))
	}
	if p.Window.Upper == nil {
		return sql.At(option.Position, fmt.Errorf("MAX_STALENESS requires an upper timestamp bound in WHERE"))
	}
	if p.HasOffsetEnd() {
		return sql.At(option.Position, fmt.Errorf("MAX_STALENESS cannot be combined with an explicit offset end"))
	}
	var err error
	p.MaxStalenessMS, err = allowance(option)
	return err
}

func (p *Plan) policyOption(option sql.Option) error {
	if option.Value != "FAIL" && option.Value != "SKIP" {
		return fmt.Errorf("%s must be FAIL or SKIP", option.Name)
	}
	switch option.Name {
	case "ON_ERROR":
		p.Policies = Policies{option.Value, option.Value, option.Value}
	case "ON_MISSING_TIMESTAMP":
		if p.Window.Lower == nil && p.Window.Upper == nil {
			return sql.At(option.Position, fmt.Errorf("ON_MISSING_TIMESTAMP requires timestamp bounds in WHERE"))
		}
		p.Policies.MissingTimestamp = option.Value
	case "ON_REJECTED_TIMESTAMP":
		if p.Destination.Kind != "TOPIC" {
			return sql.At(option.Position, fmt.Errorf("ON_REJECTED_TIMESTAMP requires a topic destination"))
		}
		p.Policies.RejectedTimestamp = option.Value
	}
	return nil
}
