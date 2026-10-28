package app

import (
	"fmt"
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
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestStreamingFullRecordFidelityAndPartitionOrder(t *testing.T) {
	for _, sourceKind := range []string{"TOPIC", "ARCHIVE"} {
		for _, destinationKind := range []string{"TOPIC", "ARCHIVE"} {
			t.Run(sourceKind+"_to_"+destinationKind, func(t *testing.T) {
				// Given authored nil/empty/binary fields, duplicate header names, timestamps and source provenance.
				source, destination := clusters(t, 2)
				original := avro.Source{Cluster: "fixture-cluster", Topic: "original-topic", TopicID: avro.TopicID{9}}
				if sourceKind == "TOPIC" {
					metadata, err := kadm.NewClient(client(t, source)).Metadata(testContext(t), "input")
					require.NoError(t, err)
					original.Cluster = metadata.Cluster
					original.Topic = "input"
					original.TopicID = avro.TopicID(metadata.Topics["input"].ID)
				}
				expected := map[int32][]avro.Record{}
				for part := range 2 {
					identity := original
					identity.Partition = int32(part)
					for offset := range 3 {
						stamp := int64(1700000000000 + part*100 + offset)
						record := avro.Record{Source: identity, Offset: int64(offset), TimestampMS: &stamp, TimestampKind: "create_time", Headers: []avro.Header{}}
						switch offset {
						case 0:
							record.Key = nil
							record.Value = []byte{}
						case 1:
							record.Key = []byte{}
							record.Value = nil
						case 2:
							record.Key = []byte{0, 255, 1}
							record.Value = []byte{255, 0, 42}
							record.Headers = []avro.Header{{Key: []byte("x"), Value: nil}, {Key: []byte("x"), Value: []byte{}}, {Key: []byte{}, Value: []byte{0, 255}}}
						}
						expected[int32(part)] = append(expected[int32(part)], record)
					}
				}
				input, target := "TOPIC 'input@src'", "TOPIC 'output@dst'"
				if sourceKind == "TOPIC" {
					producer := client(t, source)
					records := []*kgo.Record{}
					for part := range 2 {
						for _, record := range expected[int32(part)] {
							wire := &kgo.Record{Topic: "input", Partition: int32(part), Key: record.Key, Value: record.Value, Timestamp: time.UnixMilli(*record.TimestampMS)}
							for _, header := range record.Headers {
								wire.Headers = append(wire.Headers, kgo.RecordHeader{Key: string(header.Key), Value: header.Value})
							}
							records = append(records, wire)
						}
					}
					require.NoError(t, producer.ProduceSync(testContext(t), records...).FirstErr())
				} else {
					root := t.TempDir()
					input = "ARCHIVE '" + root + "'"
					codec, err := avro.New(64 << 20)
					require.NoError(t, err)
					for part := range 2 {
						writerID, fileID := strings.Repeat("a", 32), fmt.Sprintf("%032x", part+1)
						file, err := os.Create(filepath.Join(root, writerID+"."+fileID+".avro"))
						require.NoError(t, err)
						writer, err := codec.NewWriter(file, avro.Identity{Version: 1, WriterID: writerID, FileID: fileID, Source: expected[int32(part)][0].Source, FirstOffset: 0})
						require.NoError(t, err)
						for _, record := range expected[int32(part)] {
							require.NoError(t, writer.Append(record))
						}
						require.NoError(t, writer.Flush())
						require.NoError(t, file.Close())
					}
				}
				outputRoot := ""
				if destinationKind == "ARCHIVE" {
					outputRoot = t.TempDir()
					target = "ARCHIVE '" + outputRoot + "' WITH(ROTATE_AFTER_SIZE=1000)"
				}
				ast, err := sql.NewParser().Parse(fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WITH(GROUP='fidelity-%d') WHERE RECORD_OFFSET < LATEST OPTIONS(WORKER_STOP_AFTER_RECORDS=3)", target, input, time.Now().UnixNano()))
				require.NoError(t, err)
				p, err := plan.NewPlanner(environment{}).Build(ast, plan.Profiles{"src": {"bootstrap.servers": strings.Join(source, ",")}, "dst": {"bootstrap.servers": strings.Join(destination, ",")}})
				require.NoError(t, err)
				cfg := config.Defaults("")
				directory := t.TempDir()
				cfg.Worker.CheckpointDir = &directory
				cfg.Worker.RecordCap = 2
				cfg.Worker.PartitionRecordCap = 1
				// When a capped streaming run resumes and repeats, then full records and per-partition order remain intact.
				require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
				p.WorkerStopAfterRecords = 0
				for range 2 {
					require.NoError(t, Run(testContext(t), p, cfg, Options{ProgressTopic: "progress"}))
				}
				if destinationKind == "ARCHIVE" {
					records, private, err := scanArchiveRecords(outputRoot)
					require.NoError(t, err)
					require.Zero(t, private)
					actual := map[int32][]avro.Record{}
					for _, record := range records {
						actual[record.Source.Partition] = append(actual[record.Source.Partition], record)
					}
					require.Equal(t, expected, actual)
				} else {
					actual := map[int32][]*kgo.Record{}
					for _, record := range committedRecords(t, destination, 2) {
						actual[record.Partition] = append(actual[record.Partition], record)
					}
					for part, want := range expected {
						require.Len(t, actual[part], len(want))
						for index, record := range want {
							got := actual[part][index]
							require.Equal(t, record.Key, got.Key)
							require.Equal(t, record.Value, got.Value)
							require.Equal(t, *record.TimestampMS, got.Timestamp.UnixMilli())
							headers := []avro.Header{}
							for _, header := range got.Headers {
								headers = append(headers, avro.Header{Key: []byte(header.Key), Value: header.Value})
							}
							require.Equal(t, record.Headers, headers)
						}
					}
				}
			})
		}
	}
}
