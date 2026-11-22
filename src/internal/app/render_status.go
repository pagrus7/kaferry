package app

import (
	"bytes"
	"fmt"
	"io"

	"github.com/pagrus7/kaferry/src/internal/inspect"
)

func renderStatus(output io.Writer, r inspect.StatusReport, asJSON bool) error {
	if asJSON {
		return renderJSON(output, r)
	}
	var b bytes.Buffer
	fmt.Fprintf(&b, "Status: %s\nGroup: %q\n", r.Verdict, r.Group)
	if r.Reason != "" {
		fmt.Fprintf(&b, "Reason: %s\n", r.Reason)
	}
	renderEndpoint(&b, "Source", r.Source)
	renderEndpoint(&b, "Destination", r.Destination)
	fmt.Fprintln(&b, "\nSaved progress")
	unit := "offset"
	if r.Source.Kind == "ARCHIVE" {
		unit = "input records"
	}
	for _, p := range r.Partitions[:min(8, len(r.Partitions))] {
		label := fmt.Sprintf("Partition %d", p.ID)
		if r.Source.Kind == "ARCHIVE" {
			label = "Replay"
		}
		if p.Position == nil {
			fmt.Fprintf(&b, "  %s: no saved progress\n", label)
			continue
		}
		fmt.Fprintf(&b, "  %s: %s %d", label, unit, *p.Position)
		if p.End != nil {
			fmt.Fprintf(&b, "; captured end %d", *p.End)
		}
		if p.Stop != nil {
			fmt.Fprintf(&b, "; %s at %d", inspect.StopDescription(p.Stop.Reason), p.Stop.At)
		} else {
			fmt.Fprint(&b, "; no saved stop")
		}
		fmt.Fprintln(&b)
	}
	if len(r.Partitions) > 8 {
		fmt.Fprintf(&b, "  %d more partitions; see --json.\n", len(r.Partitions)-8)
	}
	if r.Source.File != nil {
		fmt.Fprintf(&b, "  Snapshot: %d completed files, %d encoded bytes.\n", r.Source.File.FileCount, r.Source.File.SizeBytes)
	}
	renderStatusGuidance(&b, r)
	_, err := b.WriteTo(output)
	return err
}

func renderStatusGuidance(output io.Writer, r inspect.StatusReport) {
	fmt.Fprintf(output, "\nMeaning: %s\n", r.Meaning)
	for _, note := range r.Notes {
		fmt.Fprintln(output, note)
	}
	if r.Next != "" {
		fmt.Fprintf(output, "Next: %s\n", r.Next)
	}
	fmt.Fprintf(output, "Activity: %s.\nAll statuses: kaferry help status\n", r.Activity)
}
