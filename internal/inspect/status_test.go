package inspect

import (
	"errors"
	"reflect"
	"testing"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/file"
	"github.com/pagrus7/kaferry/internal/kafkaport"
)

func TestKafkaStatusVerdictsAndReadOrder(t *testing.T) {
	for _, tc := range []struct {
		name, verdict string
		code          int
	}{
		{"no progress", "unknown", 5}, {"partial progress", "incomplete", 4}, {"no stop", "incomplete", 4}, {"stopped", "stopped", 0},
		{"active", "running", 6}, {"changed empty group state", "running", 6}, {"rebalance without members", "running", 6},
		{"foreign member", "unknown", 5}, {"identity conflict while active", "unknown", 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given two current partitions and independently observed membership/progress.
			s, p, f, _ := fixture()
			source := f.connections["input"]
			for _, id := range []int32{0, 1} {
				source.positions[id] = int64(id)
				source.metadata[id] = kafkaport.Progress{Stop: &core.Stop{Reason: "tip", At: int64(id)}}
			}
			switch tc.name {
			case "no progress":
				clear(source.positions)
				clear(source.metadata)
			case "partial progress":
				delete(source.positions, 1)
				delete(source.metadata, 1)
			case "no stop":
				source.metadata[1] = kafkaport.Progress{}
			case "active":
				source.memberships = []kafkaport.Membership{{Idle: true}, {Members: 1}}
			case "changed empty group state":
				source.memberships = []kafkaport.Membership{{Idle: true, State: "Empty"}, {Idle: true, State: "Dead"}}
			case "rebalance without members":
				source.memberships = []kafkaport.Membership{{Idle: false}}
			case "foreign member":
				source.memberships = []kafkaport.Membership{{Foreign: true, Members: 1}}
			case "identity conflict while active":
				source.fail = "saved"
				source.failure = &kafkaport.Conflict{Err: errors.New("different query")}
				source.memberships = []kafkaport.Membership{{Members: 1}}
			}
			// When observing without joining, consuming, positioning or committing.
			r, err := s.Status(t.Context(), p, config.Defaults("home").Worker, core.NoopStopObserver{})
			// Then identity conflicts win, activity prevents stopped, and only every durable stop yields 0.
			if err != nil || r.Verdict != tc.verdict || r.ExitCode() != tc.code || source.closed != 1 || f.connections["output"].closed != 1 || len(source.queries) > 0 {
				t.Fatal(r, err, source)
			}
			if tc.name != "identity conflict while active" && !reflect.DeepEqual(source.observations, []string{"describe", "members", "saved", "members", "describe"}) {
				t.Fatal(source.observations)
			}
			if tc.name == "stopped" && (len(r.Partitions) != 2 || r.Partitions[0].ID != 0 || r.Partitions[1].Position == nil || *r.Partitions[1].Position != 1) {
				t.Fatal(r)
			}
		})
	}
}
func TestStatusPreservesOperationalFailuresAndRejectsOneOff(t *testing.T) {
	for _, stage := range []string{"members", "saved", "close", "ONE_OFF"} {
		t.Run(stage, func(t *testing.T) {
			// Given a failed read/cleanup or an explicitly anonymous group.
			s, p, f, _ := fixture()
			source := f.connections["input"]
			source.fail = stage
			if stage == "ONE_OFF" {
				p.OneOff = true
			}
			// When checking status.
			_, err := s.Status(t.Context(), p, config.Worker{}, core.NoopStopObserver{})
			// Then unavailable state is not reported as successful absence, and ONE_OFF opens nothing.
			if stage == "ONE_OFF" {
				var code interface{ ExitCode() int }
				if !errors.As(err, &code) || code.ExitCode() != 2 || f.opened != 0 {
					t.Fatal(err, f.opened)
				}
			} else if !errors.Is(err, source.failure) {
				t.Fatal(err)
			}
		})
	}
}
func TestFileStatusUsesOpeningSizeNotEOFPosition(t *testing.T) {
	for _, tc := range []struct {
		name    string
		size    int64
		verdict string
	}{
		{"preexisting partial final frame", 150, "stopped"}, {"appended bytes", 151, "incomplete"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given a saved EOF before a partial final frame that was already present at open.
			s, p, _, files := fixture()
			p.Source.Kind = "FILE"
			p.Source.Name = "input.cbor"
			opening := int64(150)
			files.source = file.SourceInspection{ArchiveInspection: file.ArchiveInspection{Exists: true, ID: "source", Size: tc.size}, Saved: true, Position: 100, OpeningSize: &opening, Stop: &core.Stop{Reason: "eof", At: 100}}
			// When checking the saved stop against the current file size.
			r, err := s.Status(t.Context(), p, config.Worker{}, core.NoopStopObserver{})
			// Then only growth after opening invalidates that saved stop.
			if err != nil || r.Verdict != tc.verdict || r.Activity != "not checked for files" || r.Source.File.SizeBytes != tc.size {
				t.Fatal(r, err)
			}
		})
	}
}
