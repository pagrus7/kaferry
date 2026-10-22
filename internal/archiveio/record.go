// Package archiveio adapts directory archives to the copy engine's ports.
package archiveio

import (
	"errors"

	"github.com/pagrus7/kaferry/internal/archive/avro"
	core "github.com/pagrus7/kaferry/internal/copy"
)

func toArchive(r core.Record) (avro.Record, error) {
	if r.Source == nil {
		return avro.Record{}, errors.New("archive output requires original Kafka source identity")
	}
	out := avro.Record{Source: avro.Source{Cluster: r.Source.Cluster, Topic: r.Source.Topic, TopicID: avro.TopicID(r.Source.TopicID), Partition: r.Source.Partition}, Offset: r.Source.Offset, TimestampMS: r.TimestampMS, TimestampKind: r.TimestampType, Key: r.Key, Value: r.Value, Headers: make([]avro.Header, len(r.Headers))}
	for i, h := range r.Headers {
		out.Headers[i] = avro.Header{Key: []byte(h.Name), Value: h.Value}
	}
	return out, nil
}

func fromArchive(r avro.Record) core.Record {
	out := core.Record{Source: &core.Provenance{Cluster: r.Source.Cluster, Topic: r.Source.Topic, TopicID: [16]byte(r.Source.TopicID), Partition: r.Source.Partition, Offset: r.Offset}, TimestampMS: r.TimestampMS, TimestampType: r.TimestampKind, Key: r.Key, Value: r.Value, Headers: make([]core.Header, len(r.Headers))}
	for i, h := range r.Headers {
		out.Headers[i] = core.Header{Name: string(h.Key), Value: h.Value}
	}
	return out
}
