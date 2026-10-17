package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/pagrus7/kaferry/internal/inspect"
	"github.com/pagrus7/kaferry/internal/plan"
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
		fmt.Fprintf(&buffer, "group %q  fingerprint %s\n", report.Group, report.Fingerprint)
		renderEndpoint(&buffer, "source", report.Source)
		renderEndpoint(&buffer, "destination", report.Destination)
		fmt.Fprintf(&buffer, "cutoff %d ms\n", report.CutoffMS)
		renderPartitions(&buffer, report.Partitions)
		fmt.Fprintf(&buffer, "\nRun permissions (not verified): %s\n", strings.Join(report.RunPermissions, "; "))
		for _, warning := range report.Warnings {
			fmt.Fprintf(&buffer, "Warning: %s\n", warning)
		}
	}
	n, err := output.Write(buffer.Bytes())
	if err == nil && n != buffer.Len() {
		return io.ErrShortWrite
	}
	return err
}

func renderEndpoint(output io.Writer, label string, endpoint inspect.Endpoint) {
	fmt.Fprintf(output, "%s %s %q", label, endpoint.Kind, endpoint.Name)
	if endpoint.Kind == "TOPIC" {
		fmt.Fprintf(output, "  connection %q  cluster %q  topic ID %q  partitions %d", endpoint.Connection, endpoint.ClusterID, endpoint.TopicID, endpoint.PartitionCount)
		if endpoint.TimestampType != "" {
			fmt.Fprintf(output, "  %q", endpoint.TimestampType)
		}
	}
	fmt.Fprintln(output)
	if file := endpoint.File; file != nil {
		fmt.Fprintf(output, "  exists %t  file ID %q  size %d bytes  record frames unknown\n", file.Exists, file.ID, file.SizeBytes)
		if file.StartBytes != nil {
			fmt.Fprintf(output, "  start %d bytes  checkpoint %q  saved %t  window %s\n", *file.StartBytes, file.Checkpoint, *file.CheckpointExists, formatWindow(*file.Window))
		}
		if file.AcknowledgedExtent != nil {
			fmt.Fprintf(output, "  acknowledged extent %d bytes\n", *file.AcknowledgedExtent)
		}
	}
}

func renderPartitions(output io.Writer, partitions []inspect.Partition) {
	if len(partitions) == 0 {
		return
	}
	bounded := partitions[0].Window.Upper != nil
	table := tabwriter.NewWriter(output, 0, 4, 2, ' ', 0)
	fmt.Fprint(table, "\npartition\tstart\tlog start\tlog end\tsaved\t")
	if bounded {
		fmt.Fprint(table, "lookup end\tspan\t")
	}
	fmt.Fprintln(table, "window [start, end)\twindow source")
	for _, p := range partitions {
		fmt.Fprintf(table, "%d\t%d\t%d\t%d\t%t\t", p.ID, p.Start, p.LogStart, p.LogEnd, p.Saved)
		if bounded {
			fmt.Fprintf(table, "%s\t%s\t", optionalOffset(p.LookupEnd), optionalOffset(p.Span))
		}
		fmt.Fprintf(table, "%s\t%s\n", formatWindow(p.Window), windowOrigin(p.WindowOrigin))
	}
	table.Flush() // The caller supplies an in-memory buffer.
}

func windowOrigin(origin string) string {
	switch origin {
	case "saved":
		return "saved progress"
	case "adopted":
		return "from saved partition"
	case "resolved":
		return "NOW() preview"
	default:
		return "SQL bounds"
	}
}

func optionalOffset(value *int64) string {
	if value == nil {
		return "none"
	}
	return fmt.Sprint(*value)
}
func formatWindow(window plan.Window) string {
	bound := func(value *int64) string {
		if value == nil {
			return "unbounded"
		}
		return time.UnixMilli(*value).UTC().Format(time.RFC3339Nano)
	}
	return "[" + bound(window.Lower) + ", " + bound(window.Upper) + ")"
}
