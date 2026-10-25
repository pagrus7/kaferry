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
		if err := renderCompletion(output, report, group.window); err != nil {
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

func renderStop(output io.Writer, source inspect.Endpoint, window plan.Window, staleness int64, stopAfter uint64) error {
	if source.Kind == "FILE" {
		rule := "end of this group's fixed selection of completed archive files"
		if stopAfter > 0 {
			rule = "worker record threshold or " + rule + ", whichever comes first"
		}
		fmt.Fprintf(output, "  Stop: %s; the window filters records.\n", rule)
	} else if window.Upper == nil {
		if stopAfter > 0 {
			fmt.Fprintln(output, "  Stop: follow new records until the worker record threshold is reached or interrupted.")
		} else {
			fmt.Fprintln(output, "  Stop: no end time; follow new records until stopped.")
		}
	} else {
		if stopAfter > 0 {
			fmt.Fprintln(output, "  Stop: worker record threshold or all assigned partitions stopping, whichever comes first.")
		}
		threshold, err := window.StopThreshold(staleness)
		if err != nil {
			return err
		}
		fmt.Fprintf(output, "  Stop threshold: %s (window end + %d ms max staleness).\n", formatBound(&threshold), staleness)
		fmt.Fprintln(output, "    Per assigned partition: reuse a saved captured end. Otherwise, once the clock passes the threshold, save the current log end. Stop at that fixed end unless a record timestamp exceeds the threshold first.")
		fmt.Fprintln(output, "    Open transactions can delay reading that end; worker.tip_wait bounds a stalled readable tip, not ordinary backlog processing.")
	}
	return nil
}

func formatBound(value *int64) string {
	if value == nil {
		return "unbounded"
	}
	return time.UnixMilli(*value).UTC().Format(time.RFC3339Nano)
}

func renderCompletion(output io.Writer, report inspect.Report, window plan.Window) error {
	if report.Offsets != nil && report.Offsets.HasEnd() || len(report.PartitionOffsets) > 0 {
		if report.Source.Kind == "FILE" {
			fmt.Fprintln(output, "  Stop: selected archive EOF or the original source offset ends; timestamps filter records.")
		} else {
			fmt.Fprintln(output, "  Stop: fixed exclusive offset end per partition; timestamps filter records. LATEST previews can change before execution.")
		}
		return nil
	}
	d := report.MaxOutOfOrderMS
	if report.Source.Kind == "FILE" || window.Upper == nil || d != nil && *d == report.MaxStalenessMS && report.MaxStalenessMS >= 0 {
		if err := renderStop(output, report.Source, window, report.MaxStalenessMS, report.WorkerStopAfterRecords); err != nil {
			return err
		}
		if report.Source.Kind == "FILE" && d != nil && window.Upper != nil {
			threshold, err := window.StopThreshold(*d)
			if err != nil {
				return err
			}
			fmt.Fprintf(output, "    Each original source partition can finish when its timestamp exceeds %s (MAX_OUT_OF_ORDER).\n", formatBound(&threshold))
		}
		return nil
	}
	if report.WorkerStopAfterRecords > 0 {
		fmt.Fprintln(output, "  Stop: worker record threshold or all assigned partitions stopping, whichever comes first.")
	}
	if d == nil {
		fmt.Fprintln(output, "  Timestamp stopping: OFF.")
	} else {
		threshold, err := window.StopThreshold(*d)
		if err != nil {
			return err
		}
		fmt.Fprintf(output, "  Timestamp stop: record timestamp exceeds %s (window end + %d ms MAX_OUT_OF_ORDER).\n", formatBound(&threshold), *d)
	}
	if report.MaxStalenessMS < 0 {
		fmt.Fprintln(output, "  Wall-clock end capture: OFF; partitions without timestamp evidence keep following.")
	} else {
		threshold, err := window.StopThreshold(report.MaxStalenessMS)
		if err != nil {
			return err
		}
		fmt.Fprintf(output, "  Capture replicated ends after %s (window end + %d ms MAX_STALENESS), or reuse saved ends. Drain each fixed end; this is not a runtime deadline.\n", formatBound(&threshold), report.MaxStalenessMS)
	}
	return nil
}
