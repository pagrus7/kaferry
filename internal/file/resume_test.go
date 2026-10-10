package file

import (
	"bytes"
	"testing"

	"github.com/pagrus7/kaferry/internal/plan"
)

func TestResumePointValidatesCheckpointWithoutOwningReader(t *testing.T) {
	for _, scenario := range []string{"valid", "fingerprint", "source identity", "past end", "before start", "CRC", "next position", "missing frame", "stored window"} {
		t.Run(scenario, func(t *testing.T) {
			// Given a checkpoint naming the last confirmed archive frame.
			data := append([]byte("header"), frame([]byte("record"))...)
			start, size := int64(6), int64(len(data))
			_, _, crc, err := readFrame(bytes.NewReader(data), start, size, 1024)
			if err != nil {
				t.Fatal(err)
			}
			saved := &Checkpoint{Fingerprint: "fp", SourceID: "archive", Position: size, LastFrame: &FrameIdentity{Position: start, CRC: crc}}
			switch scenario {
			case "fingerprint":
				saved.Fingerprint = "other"
			case "source identity":
				saved.SourceID = "replacement"
			case "past end":
				saved.Position++
			case "before start":
				saved.Position = 0
			case "CRC":
				saved.LastFrame.CRC++
			case "next position":
				saved.Position--
			case "missing frame":
				saved.LastFrame = nil
			case "stored window":
				lower := int64(1)
				saved.Window.Lower = &lower
			}
			reader := bytes.NewReader(data)

			// When validating resume progress with the independent helper.
			position, _, err := resumePoint(reader, size, start, "archive", saved, "fp", plan.WindowSpec{}, 1024)

			// Then only the matching frame identity resumes and the reader remains usable.
			if (err == nil) != (scenario == "valid") {
				t.Fatal("invalid checkpoint decision", err)
			}
			if scenario == "valid" && position != size {
				t.Fatal("wrong resume position", position)
			}
			if _, err := reader.ReadAt(make([]byte, 1), 0); err != nil {
				t.Fatal("reader unusable", err)
			}
		})
	}
}
