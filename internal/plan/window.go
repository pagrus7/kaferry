package plan

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"time"

	"kafka-fwd2/internal/sql"
)

var timestampSyntax = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,3})?(Z|[+-][0-9]{2}:[0-9]{2})$`)

// Expression keeps NOW symbolic until initial progress for a partition is saved.
type Expression struct {
	Now          bool  `json:"now"`
	LiteralMS    int64 `json:"literal_ms"`
	AdjustmentMS int64 `json:"adjustment_ms"`
}

type WindowSpec struct {
	Lower *Expression `json:"lower"`
	Upper *Expression `json:"upper"`
}

type Window struct {
	Lower *int64 `json:"lower"`
	Upper *int64 `json:"upper"`
}

func (s WindowSpec) Relative() bool {
	return s.Lower != nil && s.Lower.Now || s.Upper != nil && s.Upper.Now
}

// Resolve uses one instant for every NOW() in this window.
func (s WindowSpec) Resolve(now time.Time) (Window, error) {
	var window Window
	for _, side := range []struct {
		expression *Expression
		target     **int64
	}{{s.Lower, &window.Lower}, {s.Upper, &window.Upper}} {
		if side.expression == nil {
			continue
		}
		e := side.expression
		base := e.LiteralMS
		if e.Now {
			base = now.UnixMilli()
		}
		value, err := checkedAdd(base, e.AdjustmentMS)
		if err != nil {
			return Window{}, err
		}
		*side.target = &value
	}
	if window.Lower != nil && window.Upper != nil && *window.Lower >= *window.Upper {
		return Window{}, fmt.Errorf("normalized time range is empty")
	}
	return window, nil
}

func (w Window) Contains(timestampMS int64) bool {
	return (w.Lower == nil || timestampMS >= *w.Lower) && (w.Upper == nil || timestampMS < *w.Upper)
}

// ValidateStored rejects incomplete or altered persisted windows before reading.
func (s WindowSpec) ValidateStored(w Window, cutoff int64) error {
	if (s.Lower == nil) != (w.Lower == nil) || (s.Upper == nil) != (w.Upper == nil) {
		return fmt.Errorf("stored time window has different bounds")
	}
	for _, side := range []struct {
		expression *Expression
		value      *int64
	}{{s.Lower, w.Lower}, {s.Upper, w.Upper}} {
		if side.expression != nil && !side.expression.Now && *side.value != side.expression.LiteralMS {
			return fmt.Errorf("stored literal time bound differs")
		}
	}
	if w.Lower != nil && w.Upper != nil && *w.Lower >= *w.Upper {
		return fmt.Errorf("stored time range is empty")
	}
	if w.Upper != nil {
		_, err := checkedAdd(*w.Upper, cutoff)
		return err
	}
	return nil
}

func normalize(bounds []sql.Bound) (WindowSpec, error) {
	var window WindowSpec
	for _, bound := range bounds {
		e, err := expression(bound.Time)
		if err != nil {
			return WindowSpec{}, err
		}
		var target **Expression
		switch bound.Operator {
		case ">", ">=":
			target = &window.Lower
		case "<", "<=":
			target = &window.Upper
		default:
			return WindowSpec{}, fmt.Errorf("unsupported timestamp comparison")
		}
		if *target != nil {
			return WindowSpec{}, fmt.Errorf("at most one lower and one upper bound are allowed")
		}
		if bound.Operator == ">" || bound.Operator == "<=" {
			e.AdjustmentMS, err = checkedAdd(e.AdjustmentMS, 1)
			if err != nil {
				return WindowSpec{}, err
			}
		}
		// Collapse literal arithmetic so equivalent literal queries fingerprint alike.
		if !e.Now {
			e.LiteralMS, err = checkedAdd(e.LiteralMS, e.AdjustmentMS)
			if err != nil {
				return WindowSpec{}, err
			}
			e.AdjustmentMS = 0
		}
		*target = &e
	}
	if window.Lower != nil && window.Upper != nil {
		lo, hi := window.Lower, window.Upper
		if lo.Now == hi.Now && (lo.Now && lo.AdjustmentMS >= hi.AdjustmentMS || !lo.Now && lo.LiteralMS >= hi.LiteralMS) {
			return WindowSpec{}, fmt.Errorf("normalized time range is empty")
		}
	}
	return window, nil
}

func expression(t sql.Time) (Expression, error) {
	e := Expression{Now: t.Now}
	if !t.Now {
		if !timestampSyntax.MatchString(t.Literal) {
			return Expression{}, fmt.Errorf("timestamp must be RFC 3339 with a zone and at most milliseconds")
		}
		if t.Literal[len(t.Literal)-1] != 'Z' {
			zone := t.Literal[len(t.Literal)-6:]
			hours, _ := strconv.Atoi(zone[1:3])
			minutes, _ := strconv.Atoi(zone[4:6])
			if hours > 23 || minutes > 59 {
				return Expression{}, fmt.Errorf("invalid timestamp zone")
			}
		}
		instant, err := time.Parse(time.RFC3339Nano, t.Literal)
		if err != nil {
			return Expression{}, fmt.Errorf("timestamp must be RFC 3339 with an explicit zone")
		}
		e.LiteralMS = instant.UnixMilli()
	}
	for _, adjustment := range t.Adjustments {
		delta, err := intervalMS(adjustment)
		if err != nil {
			return Expression{}, err
		}
		if adjustment.Subtract {
			delta = -delta
		}
		e.AdjustmentMS, err = checkedAdd(e.AdjustmentMS, delta)
		if err != nil {
			return Expression{}, err
		}
	}
	return e, nil
}

func intervalMS(interval sql.Interval) (int64, error) {
	if interval.Magnitude == "" {
		return 0, fmt.Errorf("interval must be a non-negative integer")
	}
	for _, c := range interval.Magnitude {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("interval must be a non-negative integer")
		}
	}
	n, err := strconv.ParseInt(interval.Magnitude, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("interval is too large")
	}
	units := map[string]int64{"SECOND": 1000, "MINUTE": 60000, "HOUR": 3600000, "DAY": 86400000}
	unit, ok := units[interval.Unit]
	if !ok {
		return 0, fmt.Errorf("unsupported interval unit")
	}
	if n > math.MaxInt64/unit {
		return 0, fmt.Errorf("interval arithmetic overflows milliseconds")
	}
	return n * unit, nil
}

func checkedAdd(a, b int64) (int64, error) {
	if b > 0 && a > math.MaxInt64-b || b < 0 && a < math.MinInt64-b {
		return 0, fmt.Errorf("timestamp arithmetic overflows milliseconds")
	}
	return a + b, nil
}
