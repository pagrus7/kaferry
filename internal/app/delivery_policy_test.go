package app

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
)

func TestRecordPoliciesPreserveResolvedPrefix(t *testing.T) {
	for _, kind := range []string{"missing", "timestamp", "size"} {
		for _, skip := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s_skip_%v", kind, skip), func(t *testing.T) {
				// Given an archive containing a bad record before one good record.
				_, destination := clusters(t, 1)
				input := t.TempDir()
				codec, err := avro.New(64 << 20)
				require.NoError(t, err)
				identity := avro.Identity{Version: 1, WriterID: strings.Repeat("a", 32), FileID: strings.Repeat("b", 32), Source: avro.Source{Cluster: "fixture", Topic: "input", TopicID: avro.TopicID{1}}, FirstOffset: 0}
				file, err := os.Create(filepath.Join(input, identity.WriterID+"."+identity.FileID+".avro"))
				require.NoError(t, err)
				writer, err := codec.NewWriter(file, identity)
				require.NoError(t, err)
				stamp := time.Now().Add(-time.Minute).UnixMilli()
				bad := avro.Record{Source: identity.Source, Offset: 0, Value: []byte("bad"), TimestampMS: &stamp, TimestampKind: "create_time"}
				predicate := ""
				properties := map[string]string{"bootstrap.servers": strings.Join(destination, ",")}
				policy := "ON_ERROR"
				switch kind {
				case "missing":
					bad.TimestampMS = nil
					bad.TimestampKind = "unknown"
					predicate = "WHERE RECORD_TIMESTAMP < NOW()"
					policy = "ON_MISSING_TIMESTAMP"
				case "timestamp":
					bad.TimestampMS = new(int64(math.MaxInt64))
					policy = "ON_REJECTED_TIMESTAMP"
				case "size":
					bad.Value = make([]byte, 2000)
					properties["message.max.bytes"] = "1000"
				}
				require.NoError(t, writer.Append(bad))
				require.NoError(t, writer.Append(avro.Record{Source: identity.Source, Offset: 1, Value: []byte("good"), TimestampMS: &stamp, TimestampKind: "create_time"}))
				require.NoError(t, writer.Flush())
				require.NoError(t, file.Close())
				action := "FAIL"
				if skip {
					action = "SKIP"
				}
				text := fmt.Sprintf("INSERT INTO TOPIC 'output@dst' SELECT * FROM ARCHIVE '%s' WITH(GROUP='policy') %s OPTIONS(%s=%s)", input, predicate, policy, action)
				ast, err := sql.NewParser().Parse(text)
				require.NoError(t, err)
				p, err := plan.NewPlanner(environment{}).Build(ast, plan.Profiles{"dst": properties})
				require.NoError(t, err)
				cfg := config.Defaults("")
				directory := t.TempDir()
				cfg.Worker.CheckpointDir = &directory
				// When copied and resumed with the same policy, then only deliberate skips advance past bad input.
				for range 2 {
					err = Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"})
					if skip {
						require.NoError(t, err)
					} else {
						require.Error(t, err)
					}
				}
				if skip {
					require.Equal(t, map[string]int{"good": 1}, output(t, destination, 1))
				} else {
					require.Empty(t, output(t, destination, 1))
				}
			})
		}
	}
}
