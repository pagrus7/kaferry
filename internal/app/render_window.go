package app

import (
	"fmt"
	"io"
	"time"

	"github.com/pagrus7/kaferry/internal/inspect"
	"github.com/pagrus7/kaferry/internal/plan"
)

type windowKey struct {
	lower, upper       int64
	hasLower, hasUpper bool
	origin             string
}
type windowGroup struct {
	window     plan.Window
	origin     string
	partitions []int32
	fresh      bool
}

func hasTimeFilter(report inspect.Report) bool {
	if file := report.Source.File; file != nil {
		return file.Window.Lower != nil || file.Window.Upper != nil
	}
	for _, partition := range report.Partitions {
		if partition.Window.Lower != nil || partition.Window.Upper != nil {
			return true
		}
	}
	return false
}

func groupWindows(partitions []inspect.Partition) []windowGroup {
	var groups []windowGroup
	index := make(map[windowKey]int)
	for _, partition := range partitions {
		key := windowKey{hasLower: partition.Window.Lower != nil, hasUpper: partition.Window.Upper != nil, origin: partition.WindowOrigin}
		if key.hasLower {
			key.lower = *partition.Window.Lower
		}
		if key.hasUpper {
			key.upper = *partition.Window.Upper
		}
		n, exists := index[key]
		if !exists {
			n = len(groups)
			index[key] = n
			groups = append(groups, windowGroup{window: partition.Window, origin: partition.WindowOrigin})
		}
		groups[n].partitions = append(groups[n].partitions, partition.ID)
		groups[n].fresh = groups[n].fresh || !partition.Saved
	}
	return groups
}

func renderWindows(output io.Writer, report inspect.Report) error {
	fmt.Fprintln(output, "\nTime window")
	fmt.Fprintln(output, "  Uses: record timestamps, not payload event time.")
	groups := groupWindows(report.Partitions)
	if file := report.Source.File; file != nil {
		groups = []windowGroup{{window: *file.Window, origin: file.WindowOrigin}}
	}
	for _, group := range groups[:min(8, len(groups))] {
		if len(groups) > 1 {
			fmt.Fprintf(output, "  Partitions: %v", group.partitions[:min(8, len(group.partitions))])
			if len(group.partitions) > 8 {
				fmt.Fprintf(output, " (+%d more; see --json)", len(group.partitions)-8)
			}
			fmt.Fprintln(output)
		}
		renderBounds(output, group.window, group.origin)
		if report.StartRule == "WINDOW" && group.fresh {
			if group.window.Lower != nil && *group.window.Lower > 0 {
				fmt.Fprintln(output, "    Fresh start: first timestamp lookup match at/after From; otherwise the read-committed end.")
			} else {
				fmt.Fprintln(output, "    Fresh start: earliest retained offset; apply the time filter while reading.")
			}
		}
		if err := renderStop(output, report.Source.Kind, group.window, report.CutoffMS); err != nil {
			return err
		}
	}
	if len(groups) > 8 {
		fmt.Fprintf(output, "  %d more distinct windows; see --json.\n", len(groups)-8)
	}
	if report.Source.Kind == "TOPIC" && len(groups) > 0 && groups[0].window.Upper != nil {
		fmt.Fprintln(output, "  Stops apply to assigned partitions; they do not prove complete coverage of the time range.")
	}
	return nil
}

func renderBounds(output io.Writer, window plan.Window, origin string) {
	if window.Lower == nil && window.Upper == nil {
		fmt.Fprintln(output, "  Filter: none.")
		return
	}
	fmt.Fprintf(output, "  From: %s (inclusive)\n", formatBound(window.Lower))
	fmt.Fprintf(output, "  Until: %s (exclusive)\n", formatBound(window.Upper))
	switch origin {
	case "saved":
		fmt.Fprintln(output, "    Bounds: saved progress.")
	case "adopted":
		fmt.Fprintln(output, "    Bounds: adopted from the lowest saved partition.")
	case "resolved":
		fmt.Fprintln(output, "    Bounds: NOW() preview only; a run without saved progress resolves it again.")
	default:
		fmt.Fprintln(output, "    Bounds: SQL literals.")
	}
}

func renderStop(output io.Writer, source string, window plan.Window, cutoff int64) error {
	if source == "FILE" {
		fmt.Fprintln(output, "  Stop: end of input as observed when the run opens the file; the window filters records.")
	} else if window.Upper == nil {
		fmt.Fprintln(output, "  Stop: no end time; follow new records until stopped.")
	} else {
		threshold, err := window.StopThreshold(cutoff)
		if err != nil {
			return err
		}
		fmt.Fprintf(output, "  Stop threshold: %s (window end + %d ms cutoff).\n", formatBound(&threshold), cutoff)
		fmt.Fprintln(output, "    Per assigned partition: stop when a record timestamp exceeds the threshold, or the clock passes it and reading reaches a sampled log end.")
		fmt.Fprintln(output, "    Open transactions can delay reading that end; worker.tip_wait bounds the wait.")
	}
	return nil
}

func formatBound(value *int64) string {
	if value == nil {
		return "unbounded"
	}
	return time.UnixMilli(*value).UTC().Format(time.RFC3339Nano)
}
