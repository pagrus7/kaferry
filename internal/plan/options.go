package plan

import (
	"fmt"
	"strings"

	"github.com/pagrus7/kaferry/internal/sql"
)

func (p *Plan) options(options []sql.Option, position sql.Position) error {
	p.Policies = Policies{MissingTimestamp: "FAIL", RejectedTimestamp: "FAIL", OtherRecord: "FAIL"}
	p.CutoffMS = 60000
	seen := make(map[string]bool)
	for _, option := range options {
		if seen[option.Name] {
			return sql.At(option.Position, fmt.Errorf("option %s repeats; specify it once", option.Name))
		}
		if conflictingPolicy(option.Name, seen) {
			return sql.At(option.Position, fmt.Errorf("ON_ERROR cannot be combined with specific error policies; choose ON_ERROR or the specific policies"))
		}
		seen[option.Name] = true
		if err := p.applyOption(option); err != nil {
			return sql.At(option.ValuePosition, err)
		}
	}
	if !seen["GROUP"] {
		return sql.At(position, fmt.Errorf("GROUP is required; add GROUP = 'name' or GROUP = ONE_OFF to OPTIONS"))
	}
	if p.Source.Kind != "TOPIC" || p.Window.Upper == nil {
		p.CutoffMS = 0
	}
	return nil
}

func conflictingPolicy(name string, seen map[string]bool) bool {
	switch name {
	case "ON_ERROR":
		return seen["ON_MISSING_TIMESTAMP"] || seen["ON_REJECTED_TIMESTAMP"]
	case "ON_MISSING_TIMESTAMP", "ON_REJECTED_TIMESTAMP":
		return seen["ON_ERROR"]
	}
	return false
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
	case "CUTOFF":
		return p.cutoffOption(option)
	case "ON_ERROR", "ON_MISSING_TIMESTAMP", "ON_REJECTED_TIMESTAMP":
		return p.policyOption(option)
	default:
		return sql.At(option.Position, fmt.Errorf("unsupported option; use GROUP, START, CUTOFF, ON_ERROR, ON_MISSING_TIMESTAMP, or ON_REJECTED_TIMESTAMP"))
	}
	return nil
}

func (p *Plan) groupOption(option sql.Option) error {
	if strings.TrimSpace(option.Value) == "" {
		return fmt.Errorf("GROUP must not be empty")
	}
	p.Group = option.Value
	p.OneOff = !option.Quoted && option.Value == "ONE_OFF"
	return nil
}

func (p *Plan) cutoffOption(option sql.Option) error {
	if p.Source.Kind != "TOPIC" || p.Window.Upper == nil {
		return sql.At(option.Position, fmt.Errorf("CUTOFF requires a topic source with an upper bound in WHERE"))
	}
	if option.Interval == nil {
		return fmt.Errorf("CUTOFF requires an interval")
	}
	var err error
	p.CutoffMS, err = intervalMS(*option.Interval)
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
			return sql.At(option.Position, fmt.Errorf("ON_MISSING_TIMESTAMP requires WHERE"))
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
