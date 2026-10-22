//go:build integration

package compat

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/pagrus7/kaferry/internal/app"
	"github.com/pagrus7/kaferry/internal/commitmeta"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestTimeBoundariesOutOfOrderAndTriggerReplay(t *testing.T) {
	// Given
	a := admin(t)
	src := topic(t, a, 1)
	p := producer(t)
	lo := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	hi := lo.Add(time.Second)
	for i, stamp := range []time.Time{lo.Add(-time.Millisecond), lo, hi.Add(-time.Millisecond), lo.Add(time.Millisecond), hi, hi.Add(time.Millisecond), hi.Add(2 * time.Millisecond), lo.Add(time.Millisecond)} {
		publish(t, p, &kgo.Record{Topic: src, Partition: 0, Value: []byte(fmt.Sprint(i)), Timestamp: stamp})
	}
	cfg := configuration(t)
	for _, test := range []struct {
		name, filter string
		want         []string
	}{{"halfopen", fmt.Sprintf("RECORD_TIMESTAMP >= TIMESTAMP '%s' AND RECORD_TIMESTAMP < TIMESTAMP '%s'", lo.Format(time.RFC3339Nano), hi.Format(time.RFC3339Nano)), []string{"1", "2", "3"}}, {"between", fmt.Sprintf("RECORD_TIMESTAMP BETWEEN TIMESTAMP '%s' AND TIMESTAMP '%s'", lo.Format(time.RFC3339Nano), hi.Format(time.RFC3339Nano)), []string{"1", "2", "3", "4"}}} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "out.avro")
			statement := fmt.Sprintf("INSERT INTO FILE avro('%s') SELECT * FROM TOPIC '%s' WHERE %s OPTIONS(GROUP='%s-%s',CUTOFF=INTERVAL '0' SECOND)", path, src, test.filter, src, test.name)

			// When
			execute(t, statement, cfg)
			execute(t, statement, cfg)
			records := archiveRecords(t, path)

			// Then
			require.Len(t, records, len(test.want), "records=%d want=%d", len(records), len(test.want))
			for i, r := range records {
				require.Equal(t, test.want[i], string(r.Value), "value=%s want=%s", r.Value, test.want[i])
			}
		})
	}
}

func TestForeignProgressAndSelfCopyFailBeforeJoining(t *testing.T) {
	// Given
	a := admin(t)
	src, dst := topic(t, a, 1), topic(t, a, 1)
	group := src + "-foreign"
	metadata := "another application's offsets"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := a.CommitOffsets(ctx, group, kadm.Offsets{src: {0: {Topic: src, Partition: 0, At: 0, Metadata: metadata}}})
	if err != nil || result.Error() != nil {
		t.Fatal("fixture offsets", err)
	}
	statement := fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' OPTIONS(GROUP='%s',START=EARLIEST)", broker(t), dst, src, group)
	var out bytes.Buffer

	// When
	executeCode := app.Execute(ctx, []string{"run", "-e", statement}, nil, io.Discard, &out)

	// Then
	require.Equal(t, 2, executeCode, "foreign progress exit=%d: %s", executeCode, out.String())
	description, err := a.DescribeGroups(ctx, group)
	require.NoError(t, err)
	require.NoError(t, description.Error())
	require.Contains(t, description, group)
	require.Len(t, description[group].Members, 0, "foreign group joined before validation")
	statement = fmt.Sprintf("WITH CONNECTION default AS ('bootstrap.servers'='%s') INSERT INTO TOPIC '%s' SELECT * FROM TOPIC '%s' OPTIONS(GROUP='%s-self',START=EARLIEST)", broker(t), src, src, src)
	if code := app.Execute(ctx, []string{"run", "-e", statement}, nil, io.Discard, &out); code != 2 {
		t.Fatal("Kafka self-copy permitted")
	}
}

func TestExpiredAndAboveEndCommits(t *testing.T) {
	// Given
	a := admin(t)
	src := topic(t, a, 1)
	p := producer(t)
	stamp := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	for i := range 6 {
		publish(t, p, &kgo.Record{Topic: src, Partition: 0, Value: []byte(fmt.Sprint(i)), Timestamp: stamp})
	}
	cfg := configuration(t)
	path := filepath.Join(t.TempDir(), "out.avro")
	group := src + "-expired"
	statement := fmt.Sprintf("INSERT INTO FILE avro('%s') SELECT * FROM TOPIC '%s' WHERE RECORD_TIMESTAMP >= TIMESTAMP '%s' AND RECORD_TIMESTAMP < TIMESTAMP '%s' OPTIONS(GROUP='%s',CUTOFF=INTERVAL '0' SECOND)", path, src, stamp.Add(-time.Millisecond).Format(time.RFC3339Nano), stamp.Add(time.Second).Format(time.RFC3339Nano), group)

	// When
	execute(t, statement, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	positions, err := a.FetchOffsets(ctx, group)

	// Then
	require.NoError(t, err)
	require.NoError(t, positions.Error())
	part := positions[src][0]
	deleted, err := a.DeleteRecords(ctx, kadm.Offsets{src: {0: {Topic: src, Partition: 0, At: 3}}})
	require.NoError(t, err)
	require.NoError(t, deleted.Error())
	// Rewinding an offset also clears the saved stop that referred to its old position.
	require.NotEmpty(t, part.Metadata, "missing metadata")
	saved, err := commitmeta.NewCodec().Decode(part.Metadata)
	require.NoError(t, err)
	saved.Stop = nil
	text, err := commitmeta.NewCodec().Encode(saved)
	require.NoError(t, err)
	part.Metadata = text
	part.At = 1
	result, err := a.CommitOffsets(ctx, group, kadm.Offsets{src: {0: part.Offset}})
	require.NoError(t, err)
	require.NoError(t, result.Error())
	execute(t, statement, cfg)
	if records := archiveRecords(t, path); len(records) != 9 {
		t.Fatal("expired commit did not replay from log start", len(records))
	}
	part.At = 999999
	result, err = a.CommitOffsets(ctx, group, kadm.Offsets{src: {0: part.Offset}})
	require.NoError(t, err)
	require.NoError(t, result.Error())
	var out bytes.Buffer
	if code := app.Execute(ctx, []string{"run", "-e", statement, "-c", cfg}, nil, io.Discard, &out); code != 2 {
		t.Fatalf("above-end offset exit=%d: %s", code, out.String())
	}
}
