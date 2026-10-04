package plan

import (
	"fmt"
	"strings"

	"kafka-fwd2/internal/sql"
)

func (p *Plan) options(options []sql.Option) error {
	p.Policies = Policies{MissingTimestamp: "FAIL", RejectedTimestamp: "FAIL", OtherRecord: "FAIL"}
	p.CutoffMS = 60000
	seen := make(map[string]bool)
	for _, option := range options {
		if seen[option.Name] {
			return fmt.Errorf("option %s repeats", option.Name)
		}
		seen[option.Name] = true
		if err := p.applyOption(option); err != nil {
			return err
		}
	}
	if !seen["GROUP"] {
		return fmt.Errorf("GROUP is required")
	}
	if seen["ON_ERROR"] && (seen["ON_MISSING_TIMESTAMP"] || seen["ON_REJECTED_TIMESTAMP"]) {
		return fmt.Errorf("ON_ERROR cannot be combined with specific error policies")
	}
	if p.Source.Kind != "TOPIC" || p.Window.Upper == nil {
		p.CutoffMS = 0
	}
	return nil
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
		return fmt.Errorf("unsupported option")
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
		return fmt.Errorf("CUTOFF requires a topic source with an upper bound")
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
		return fmt.Errorf("error policy must be FAIL or SKIP")
	}
	switch option.Name {
	case "ON_ERROR":
		p.Policies = Policies{option.Value, option.Value, option.Value}
	case "ON_MISSING_TIMESTAMP":
		if p.Window.Lower == nil && p.Window.Upper == nil {
			return fmt.Errorf("ON_MISSING_TIMESTAMP requires WHERE")
		}
		p.Policies.MissingTimestamp = option.Value
	case "ON_REJECTED_TIMESTAMP":
		if p.Destination.Kind != "TOPIC" {
			return fmt.Errorf("ON_REJECTED_TIMESTAMP requires a topic destination")
		}
		p.Policies.RejectedTimestamp = option.Value
	}
	return nil
}
