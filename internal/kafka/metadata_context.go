package kafka

import (
	"context"

	k "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pagrus7/kaferry/internal/plan"
)

// scopedMetadata carries the request context while sharing one inspector's state.
type scopedMetadata struct {
	*Inspector
	parent context.Context
}

func (s *scopedMetadata) ClusterID() (string, error) { return s.clusterID(s.parent) }
func (s *scopedMetadata) CurrentPartitions() ([]k.TopicPartition, error) {
	return s.partitions(s.parent)
}
func (s *scopedMetadata) Progress(group string) ([]k.TopicPartition, error) {
	return s.progress(s.parent, group)
}
func (s *scopedMetadata) ReadProgress(group, fp string, window plan.WindowSpec, cutoff int64) (map[int32]Progress, error) {
	return s.readProgress(s.parent, group, fp, window, cutoff)
}
func (s *scopedMetadata) CheckMembers(group string) error { return s.checkMembers(s.parent, group) }
func (s *scopedMetadata) Offsets(ids []int32, kind k.OffsetSpec, committed bool) (map[int32]int64, error) {
	return s.offsets(s.parent, ids, kind, committed)
}
func (s *scopedMetadata) Config() map[string]string { return s.config(s.parent) }

// Direct native callers retain the fixed request deadlines. The application
// uses scopedMetadata to propagate each operation's caller context.
func (i *Inspector) ClusterID() (string, error) { return i.clusterID(context.Background()) }
func (i *Inspector) CurrentPartitions() ([]k.TopicPartition, error) {
	return i.partitions(context.Background())
}
func (i *Inspector) Progress(group string) ([]k.TopicPartition, error) {
	return i.progress(context.Background(), group)
}
func (i *Inspector) ReadProgress(group, fp string, window plan.WindowSpec, cutoff int64) (map[int32]Progress, error) {
	return i.readProgress(context.Background(), group, fp, window, cutoff)
}
func (i *Inspector) CheckMembers(group string) error {
	return i.checkMembers(context.Background(), group)
}
func (i *Inspector) Offsets(ids []int32, kind k.OffsetSpec, committed bool) (map[int32]int64, error) {
	return i.offsets(context.Background(), ids, kind, committed)
}
func (i *Inspector) Config() map[string]string { return i.config(context.Background()) }
