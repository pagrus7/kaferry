package copy

import "time"

// Settings contains the runner's flow limits and timing budgets.
// The application supplies validated values from worker configuration.
type Settings struct {
	PartitionRecordCap int
	PartitionByteCap   int64
	RecordCap          int
	ByteCap            int64
	DeliveryTimeout    time.Duration
	IdleExit           time.Duration
	TipWait            time.Duration
	ProgressInterval   time.Duration
}
