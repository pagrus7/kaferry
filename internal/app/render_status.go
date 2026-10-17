package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/pagrus7/kaferry/internal/inspect"
)

func renderStatus(output io.Writer, r inspect.StatusReport, asJSON bool) error {
	var b bytes.Buffer
	if asJSON {
		e := json.NewEncoder(&b)
		e.SetIndent("", "  ")
		if err := e.Encode(r); err != nil {
			return err
		}
	} else {
		fmt.Fprintf(&b, "Status: %s\nGroup: %q\n", r.Verdict, r.Group)
		if r.Reason != "" {
			fmt.Fprintf(&b, "Reason: %s\n", r.Reason)
		}
		renderEndpoint(&b, "Source", r.Source)
		renderEndpoint(&b, "Destination", r.Destination)
		fmt.Fprintln(&b, "\nSaved progress")
		unit := "offset"
		if r.Source.Kind == "FILE" {
			unit = "byte position"
		}
		for _, p := range r.Partitions[:min(8, len(r.Partitions))] {
			if p.Position == nil {
				fmt.Fprintf(&b, "  Partition %d: no saved progress\n", p.ID)
				continue
			}
			fmt.Fprintf(&b, "  Partition %d: %s %d", p.ID, unit, *p.Position)
			if p.Stop != nil {
				fmt.Fprintf(&b, "; stopped by %s at %d", p.Stop.Reason, p.Stop.At)
			} else {
				fmt.Fprint(&b, "; no saved stop")
			}
			fmt.Fprintln(&b)
		}
		if len(r.Partitions) > 8 {
			fmt.Fprintf(&b, "  %d more partitions; see --json.\n", len(r.Partitions)-8)
		}
		if r.Source.File != nil {
			fmt.Fprintf(&b, "  Current source size: %d bytes\n", r.Source.File.SizeBytes)
		}
		fmt.Fprintf(&b, "\n%s\nActivity: %s.\n", r.Meaning, r.Activity)
	}
	n, err := output.Write(b.Bytes())
	if err == nil && n != b.Len() {
		return io.ErrShortWrite
	}
	return err
}
