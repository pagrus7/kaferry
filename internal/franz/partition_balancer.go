package franz

import (
	"fmt"
	"slices"

	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// partitionBalancer translates sparse source IDs to dense IDs for the existing
// cooperative sticky algorithm, then translates its assignment back. Ownership
// and generation metadata undergo the same translation before balancing.
type partitionBalancer struct {
	kgo.GroupBalancer
	topic, protocol string
	filter          *plan.PartitionFilter
}

func sourceBalancer(p plan.Plan, fp string) kgo.GroupBalancer {
	base := kgo.CooperativeStickyBalancer()
	if p.Partitions == nil {
		return base
	}
	return &partitionBalancer{GroupBalancer: base, topic: p.Source.Name, protocol: kafkaport.GroupProtocol(p, fp), filter: p.Partitions}
}

func (b *partitionBalancer) ProtocolName() string { return b.protocol }

func (b *partitionBalancer) MemberBalancer(members []kmsg.JoinGroupResponseMember) (kgo.GroupMemberBalancer, map[string]struct{}, error) {
	consumer, err := kgo.NewConsumerBalancer(b, members)
	if err != nil {
		return nil, nil, err
	}
	return consumer, consumer.MemberTopics(), nil
}

func (b *partitionBalancer) Balance(consumer *kgo.ConsumerBalancer, topics map[string]int32) kgo.IntoSyncAssignment {
	result := consumer.NewPlan()
	fail := func(err error) kgo.IntoSyncAssignment { consumer.SetError(err); return result }
	count, ok := topics[b.topic]
	if !ok || count <= 0 || len(topics) != 1 {
		return fail(fmt.Errorf("partition selection requires metadata for its source topic"))
	}
	available := make([]int32, count)
	for i := range available {
		available[i] = int32(i)
	}
	selected, err := b.filter.Select(available)
	if err != nil {
		return fail(err)
	}
	members := slices.Clone(consumer.Members())
	for i := range members {
		_, meta := consumer.MemberAt(i)
		if len(meta.Topics) != 1 || meta.Topics[0] != b.topic {
			return fail(fmt.Errorf("GROUP member has an incompatible source subscription"))
		}
		owned := map[string][]int32{}
		for _, topic := range meta.OwnedPartitions {
			if topic.Topic != b.topic {
				return fail(fmt.Errorf("GROUP member owns another topic"))
			}
			for _, id := range topic.Partitions {
				index, found := slices.BinarySearch(selected, id)
				if !found {
					return fail(fmt.Errorf("GROUP member owns an excluded source partition"))
				}
				owned[b.topic] = append(owned[b.topic], int32(index))
			}
		}
		members[i].ProtocolMetadata = b.GroupBalancer.JoinGroupMetadata(meta.Topics, owned, meta.Generation)
	}
	delegate, _, err := b.GroupBalancer.MemberBalancer(members)
	if err != nil {
		return fail(err)
	}
	dense, err := delegate.(kgo.GroupMemberBalancerOrError).BalanceOrError(map[string]int32{b.topic: int32(len(selected))})
	if err != nil {
		return fail(err)
	}
	for _, assignment := range dense.IntoSyncAssignment() {
		var decoded kmsg.ConsumerMemberAssignment
		if err := decoded.ReadFrom(assignment.MemberAssignment); err != nil {
			return fail(err)
		}
		for _, topic := range decoded.Topics {
			for _, id := range topic.Partitions {
				result.AddPartition(&kmsg.JoinGroupResponseMember{MemberID: assignment.MemberID}, b.topic, selected[id])
			}
		}
	}
	return result
}
