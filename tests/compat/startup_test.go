//go:build integration

package compat

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConsumerConfigurationFailureDoesNotCreateArchiveOrBlockRetry(t *testing.T) {
	for _, setting := range []struct{ name, properties string }{
		{"protected isolation", ",'isolation.level'='read_uncommitted'"},
		{"consumer timeout validation", ",'session.timeout.ms'='6000','max.poll.interval.ms'='1000'"},
	} {
		t.Run(setting.name, func(t *testing.T) {
			// Given a new archive and a source configuration rejected before subscription.
			a := admin(t)
			src := topic(t, a, 1)
			path := filepath.Join(t.TempDir(), "out.avro")
			end := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
			query := func(properties string) string {
				return fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s'%s) INSERT INTO ARCHIVE '%s' SELECT * FROM TOPIC '%s' WITH (GROUP='%s', MAX_STALENESS=INTERVAL '0' SECOND) WHERE RECORD_TIMESTAMP < TIMESTAMP '%s' ", broker(t), properties, path, src, src+"-copy", end.Format(time.RFC3339Nano))
			}
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()

			// When rejecting the source settings.
			first := loggedWorker(ctx, query(setting.properties), "")

			// Then the output was never created and the corrected query can succeed.
			require.Equal(t, 2, first.code, "configuration exit=%d: %s", first.code, first.detail)
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatal("invalid source configuration created an archive", err)
			}
			corrected := loggedWorker(ctx, query(""), "")
			require.Zero(t, corrected.code, "corrected query blocked: %d %s", corrected.code, corrected.detail)
			if records := archiveRecords(t, path); len(records) != 0 {
				t.Fatal("empty source produced records", records)
			}
		})
	}
}
