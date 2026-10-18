package file

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/stretchr/testify/require"
)

func TestOutputRejectsForeignQueryOrArchiveWithoutChangingBytes(t *testing.T) {
	for _, scenario := range []string{"fingerprint", "group", "archive identity"} {
		t.Run(scenario, func(t *testing.T) {
			// Given an archive with a durable record and saved identity/extent.
			path := filepath.Join(t.TempDir(), "archive.cbor")
			c := codec(t)
			output, err := NewOutput(NewPublisher(), path, "group", "fp", "", 0, c, 10, logger())
			require.NoError(t, err)
			if err := output.Submit(sample(), core.Token{}); err != nil {
				output.Close()
				t.Fatal(err)
			}
			if err := output.Flush(); err != nil {
				output.Close()
				t.Fatal(err)
			}
			id, extent := output.DurableOutput()
			originalID := id
			output.Close()
			before, err := os.ReadFile(path)
			require.NoError(t, err)
			before = append(before, []byte("unacknowledged tail")...)
			require.NoError(t, os.WriteFile(path, before, 0600))
			group, fp := "group", "fp"
			switch scenario {
			case "fingerprint":
				fp = "another-query"
			case "group":
				group = "another-group"
			case "archive identity":
				id = "another-archive"
			}

			// When a restart attempts to open it with incompatible saved identity.
			restarted, err := NewOutput(NewPublisher(), path, group, fp, id, extent, c, 10, logger())

			// Then identity fails before truncation or appending and releases the file lock.
			var conflict *Conflict
			if restarted != nil || !errors.As(err, &conflict) {
				t.Fatal("foreign output accepted", restarted, err)
			}
			after, readErr := os.ReadFile(path)
			if readErr != nil || !bytes.Equal(before, after) {
				t.Fatal("failed restart changed archive", readErr)
			}
			restarted, err = NewOutput(NewPublisher(), path, "group", "fp", originalID, extent, c, 10, logger())
			if err != nil {
				t.Fatal("failed restart retained output lock", err)
			}
			restarted.Close()
		})
	}
}
