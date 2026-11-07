package plan

import "fmt"

// PreservesPartitions resolves the topic destination's default routing policy.
func (e Endpoint) PreservesPartitions() bool {
	return e.Kind == "TOPIC" && e.Partitioning != Key
}

// CheckPartition rejects a source ID that cannot retain its number at output.
func CheckPartition(id int32, count int) error {
	if id < 0 || int64(id) >= int64(count) {
		return fmt.Errorf("cannot preserve original partition %d: destination has %d partitions; expand the destination or choose PARTITIONING = KEY", id, count)
	}
	return nil
}

func CheckPartitions(ids []int32, count int) error {
	if count < 1 {
		return fmt.Errorf("destination topic has no partitions")
	}
	for _, id := range ids {
		if err := CheckPartition(id, count); err != nil {
			return err
		}
	}
	return nil
}
