package app

import (
	"fmt"
	"io"
	"time"

	"github.com/pagrus7/kaferry/src/internal/inspect"
	"github.com/pagrus7/kaferry/src/internal/plan"
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

func renderWindows(output io.Writer, report inspect.Report) {
	fmt.Fprintln(output, "\nTime window")
	fmt.Fprintln(output, "  Uses: record timestamps, not payload event time.")
	groups := groupWindows(report.Partitions)
	if file := report.Source.File; file != nil {
		groups = []windowGroup{{window: *file.Window, origin: file.WindowOrigin}}
	}
	for _, group := range groups[:min(8, len(groups))] {
		renderWindowGroup(output, report, group, len(groups) > 1)
	}
	if len(groups) > 8 {
		fmt.Fprintf(output, "  %d more distinct windows; see --json.\n", len(groups)-8)
	}
	if report.Source.Kind == "TOPIC" && len(groups) > 0 && groups[0].window.Upper != nil {
		fmt.Fprintln(output, "  Stops apply to assigned partitions; they do not prove complete coverage of the time range.")
	}
}

func renderWindowGroup(output io.Writer, report inspect.Report, group windowGroup, multiple bool) {
	if multiple {
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
	renderCompletion(output, report, group.window)
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

func formatBound(value *int64) string {
	if value == nil {
		return "unbounded"
	}
	return time.UnixMilli(*value).UTC().Format(time.RFC3339Nano)
}

// Inspection validates completion arithmetic before rendering the report.
func renderCompletion(output io.Writer, report inspect.Report, window plan.Window) {
	switch report.EndRule {
	case "FOLLOW":
		fmt.Fprintln(output, "  Stop: no source end (FOLLOW = ON).")
	case "EOF":
		fmt.Fprintln(output, "  Stop: end of this group's fixed selection of completed archive files.")
	case "OFFSET":
		fmt.Fprintln(output, "  Stop: saved exclusive offset end for each partition.")
	default:
		fmt.Fprintln(output, "  Stop: saved readable end per partition (default). Use FOLLOW = ON to keep copying new records.")
	}
	if report.MaxOutOfOrderMS != nil && window.Upper != nil {
		threshold := *window.Upper + *report.MaxOutOfOrderMS
		fmt.Fprintf(output, "    Stop earlier before the first visible record with timestamp after %s (MAX_OUT_OF_ORDER).\n", formatBound(&threshold))
	}
}
