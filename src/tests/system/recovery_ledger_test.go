package compat

import (
	"fmt"
	"reflect"

	"github.com/twmb/franz-go/pkg/kgo"
)

// A finite recovery batch retains expectations independently of delivery order.
// Every copy is validated, including legitimate replay duplicates.
type recoveryLedger struct {
	expected   map[string]*kgo.Record
	seen       map[string]bool
	duplicates int
}

func (l *recoveryLedger) observe(record *kgo.Record) error {
	id := string(record.Key)
	want, ok := l.expected[id]
	if !ok {
		return fmt.Errorf("unexpected record ID %q", id)
	}
	if !reflect.DeepEqual(want.Value, record.Value) || !reflect.DeepEqual(want.Headers, record.Headers) ||
		!want.Timestamp.Equal(record.Timestamp) || want.Partition != record.Partition {
		return fmt.Errorf("corrupted record %q", id)
	}
	if l.seen[id] {
		l.duplicates++
	}
	l.seen[id] = true
	return nil
}
