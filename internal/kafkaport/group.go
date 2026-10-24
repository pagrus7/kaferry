package kafkaport

import "github.com/pagrus7/kaferry/internal/plan"

// GroupProtocol fences incompatible active workers before Kafka assigns work.
// Unfiltered copies retain their existing cooperative protocol.
func GroupProtocol(p plan.Plan, fingerprint string) string {
	if p.Partitions != nil {
		return "kaferry-partitions-v1-" + fingerprint
	}
	return "cooperative-sticky"
}
