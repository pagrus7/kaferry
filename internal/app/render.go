package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/pagrus7/kaferry/internal/inspect"
)

func renderExplanation(output io.Writer, report inspect.Report, asJSON bool) error {
	var buffer bytes.Buffer
	if asJSON {
		encoder := json.NewEncoder(&buffer)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(report); err != nil {
			return err
		}
	} else {
		fmt.Fprintln(&buffer, "Endpoints")
		renderEndpoint(&buffer, "Source", report.Source)
		renderEndpoint(&buffer, "Destination", report.Destination)
		renderProgress(&buffer, report)
		renderWindows(&buffer, report)
		renderBehavior(&buffer, report)
		if len(report.Warnings) > 0 {
			fmt.Fprintln(&buffer, "\nAttention")
			for _, warning := range report.Warnings[:min(8, len(report.Warnings))] {
				fmt.Fprintf(&buffer, "  - %s\n", warning)
			}
			if len(report.Warnings) > 8 {
				fmt.Fprintf(&buffer, "  %d more warnings; see --json.\n", len(report.Warnings)-8)
			}
		}
		fmt.Fprintln(&buffer, "\nPreview limits")
		fmt.Fprintln(&buffer, "  Read-only; metadata and progress may change before the run.")
		fmt.Fprintln(&buffer, "  Write access is not checked. Records, output bytes and duration are not estimated.")
		if report.Source.File != nil || report.Destination.File != nil {
			fmt.Fprintln(&buffer, "  Archive records are not scanned; full archive integrity is not checked.")
		}
		fmt.Fprintln(&buffer, "  Full partition details and identities: --json.")
	}
	_, err := buffer.WriteTo(output)
	return err
}

func renderEndpoint(output io.Writer, label string, endpoint inspect.Endpoint) {
	fmt.Fprintf(output, "  %s: %s %q", label, endpoint.Kind, endpoint.Name)
	if endpoint.Kind == "TOPIC" {
		fmt.Fprintf(output, " (connection %q; %d partitions)\n", endpoint.Connection, endpoint.PartitionCount)
		fmt.Fprintf(output, "    Resolved cluster: %q\n", endpoint.ClusterID)
	} else {
		fmt.Fprintln(output)
	}
	if filter := endpoint.PartitionFilter; filter != nil {
		operator, additions := "IN", "Added partitions are excluded."
		if filter.Exclude {
			operator, additions = "NOT IN", "Added partitions are eligible while workers run."
		}
		if endpoint.Kind == "FILE" {
			additions = "New files require a new group; resume keeps the saved selection."
		}
		ids := filter.IDs[:min(8, len(filter.IDs))]
		fmt.Fprintf(output, "    Source selection: PARTITION %s %v. %s\n", operator, ids, additions)
		if len(ids) < len(filter.IDs) {
			fmt.Fprintf(output, "    %d more listed IDs; see --json.\n", len(filter.IDs)-len(ids))
		}
	}
}

func renderProgress(output io.Writer, report inspect.Report) {
	fmt.Fprintf(output, "\nProgress\n  Group: %q\n", report.Group)
	if report.OneOff {
		fmt.Fprintln(output, "  ONE_OFF: each run starts fresh. Resume an unfinished run with its logged generated group name.")
	}
	if file := report.Source.File; file != nil {
		if *file.CheckpointExists {
			fmt.Fprintln(output, "  Resume: saved file checkpoint.")
		} else {
			fmt.Fprintln(output, "  Start: current completed files; no saved checkpoint.")
		}
		fmt.Fprintf(output, "  Input position: %d records; snapshot: %d completed files, %d encoded bytes.\n", *file.PositionRecords, file.FileCount, file.SizeBytes)
		if file.Checkpoint != "" {
			fmt.Fprintf(output, "  Checkpoint: %q\n", file.Checkpoint)
		}
	} else {
		saved := 0
		for _, partition := range report.Partitions {
			if partition.Saved {
				saved++
			}
		}
		fmt.Fprintf(output, "  Resume: %d of %d partitions have saved offsets.\n", saved, len(report.Partitions))
		for _, partition := range report.Partitions[:min(8, len(report.Partitions))] {
			if report.Offsets != nil || len(report.PartitionOffsets) > 0 {
				fmt.Fprintf(output, "  Partition %d: start %d", partition.ID, partition.Start)
				if partition.End != nil {
					fmt.Fprintf(output, "; exclusive end %d (%s)", *partition.End, partition.EndOrigin)
				}
				fmt.Fprintln(output)
			} else if partition.End != nil {
				fmt.Fprintf(output, "  Partition %d: saved captured end %d.\n", partition.ID, *partition.End)
			}
		}
		if fresh := len(report.Partitions) - saved; fresh > 0 {
			fmt.Fprintf(output, "  Without saved offsets: %d partitions; %s\n", fresh, startRule(report.StartRule))
		}
	}
	if report.Destination.File != nil {
		fmt.Fprintln(output, "  Output: independent Avro files; worker completion publishes remaining files.")
	}
}

func startRule(rule string) string {
	switch rule {
	case "OFFSET":
		return "use the resolved per-partition offset positions."
	case "LATEST":
		return "start at the current read-committed end."
	case "WINDOW":
		return "use the time-window start rule below."
	default:
		return "start at the earliest retained offset."
	}
}

func renderBehavior(output io.Writer, report inspect.Report) {
	fmt.Fprintln(output, "\nBehavior")
	if report.WorkerStopAfterRecords > 0 {
		fmt.Fprintf(output, "  Worker stop: after %d records accepted for writing. Resuming gets a fresh allowance.\n", report.WorkerStopAfterRecords)
	}
	if report.Destination.Kind == "TOPIC" {
		if report.Destination.Partitioning == "KEY" {
			fmt.Fprintln(output, "  Routing: original key bytes; destination key partitioner.")
		} else {
			fmt.Fprintln(output, "  Routing: preserve original partition numbers; missing partitions fail.")
		}
		if report.Destination.TimestampType != "" {
			fmt.Fprintf(output, "  Destination timestamp policy: %q.\n", report.Destination.TimestampType)
		} else {
			fmt.Fprintln(output, "  Destination timestamp policy: unknown.")
		}
	}
	var skips []string
	for _, policy := range []struct {
		value, label string
		applies      bool
	}{
		{report.RecordErrors.MissingTimestamp, "missing timestamps", hasTimeFilter(report)},
		{report.RecordErrors.RejectedTimestamp, "rejected timestamps", report.Destination.Kind == "TOPIC"},
		{report.RecordErrors.OtherRecord, "other invalid records", true},
	} {
		if policy.applies && policy.value == "SKIP" {
			skips = append(skips, policy.label)
		}
	}
	if len(skips) == 0 {
		fmt.Fprintln(output, "  Record errors: stop; no deliberate skipping.")
	} else {
		fmt.Fprintf(output, "  Skip before writing: %s (records dropped).\n", strings.Join(skips, ", "))
		fmt.Fprintln(output, "  Delivery and unclassified errors: stop.")
	}
}
