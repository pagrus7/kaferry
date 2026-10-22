package archiveio

import (
	"errors"
	"github.com/pagrus7/kaferry/internal/archive/avro"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/stretchr/testify/require"
	"testing"
)

type captureMock struct {
	records                        []avro.Record
	appendErr, flushErr, finishErr error
	flushes, finishes, closes      int
}

func (m *captureMock) Append(r avro.Record) error {
	if m.appendErr != nil {
		return m.appendErr
	}
	m.records = append(m.records, r)
	return nil
}
func (m *captureMock) Flush() error  { m.flushes++; return m.flushErr }
func (m *captureMock) Finish() error { m.finishes++; return m.finishErr }
func (m *captureMock) Close() error  { m.closes++; return nil }
func TestSinkConfirmsBatchOnlyAfterDurabilityAndPreservesRecord(t *testing.T) {
	// Given a full logical record, including nulls and binary duplicate header names.
	m := &captureMock{}
	sink := NewSink(m, 2)
	record := core.Record{Source: &core.Provenance{Cluster: "cluster", TopicID: [16]byte{1}, Topic: "orders", Partition: 2, Offset: 42}, Key: []byte{}, TimestampType: "unknown", Headers: []core.Header{{Name: string([]byte{255}), Value: nil}, {Name: string([]byte{255}), Value: []byte{}}}}
	// When submitting, then no delivery is acknowledged before Flush.
	token := core.Token{Sequence: 7}
	require.NoError(t, sink.Submit(record, token))
	require.Empty(t, sink.Deliveries())
	require.Equal(t, record, fromArchive(m.records[0]))
	require.NoError(t, sink.Flush())
	require.Equal(t, core.Delivery{Token: token}, <-sink.Deliveries())
	// When finishing, then publication runs separately and Close releases ownership.
	require.NoError(t, sink.Finish())
	sink.Close()
	require.Equal(t, 1, m.finishes)
	require.Equal(t, 1, m.closes)
}
func TestSinkLatchesStorageFailuresButAllowsRecordLimitSkip(t *testing.T) {
	for _, stage := range []string{"append", "flush", "finish"} {
		t.Run(stage, func(t *testing.T) {
			// Given a storage failure at one durability stage.
			failure := errors.New("disk unavailable")
			m := &captureMock{}
			sink := NewSink(m, 2)
			record := core.Record{Source: &core.Provenance{}, TimestampType: "unknown"}
			switch stage {
			case "append":
				m.appendErr = failure
			case "flush":
				m.flushErr = failure
			case "finish":
				m.finishErr = failure
			}
			// When the stage fails, then later calls cannot acknowledge work or recover silently.
			if stage == "append" {
				require.ErrorIs(t, sink.Submit(record, core.Token{}), failure)
			} else {
				require.NoError(t, sink.Submit(record, core.Token{}))
				if stage == "flush" {
					require.ErrorIs(t, sink.Flush(), failure)
				} else {
					require.ErrorIs(t, sink.Finish(), failure)
				}
			}
			require.ErrorIs(t, sink.Submit(record, core.Token{}), failure)
			require.ErrorIs(t, sink.Flush(), failure)
			require.ErrorIs(t, sink.Finish(), failure)
			require.Empty(t, sink.Deliveries())
		})
	}
	// Given an individual format-limit rejection, when it is skipped, then the sink remains usable.
	m := &captureMock{appendErr: &avro.InvalidRecord{Err: errors.New("too large")}}
	sink := NewSink(m, 1)
	var classified *core.RecordError
	require.ErrorAs(t, sink.Submit(core.Record{Source: &core.Provenance{}}, core.Token{}), &classified)
	m.appendErr = nil
	require.NoError(t, sink.Submit(core.Record{Source: &core.Provenance{}}, core.Token{}))
	require.NoError(t, sink.Flush())
	require.Len(t, sink.Deliveries(), 1)
}
