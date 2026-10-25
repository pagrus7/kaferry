// Package inspect previews a copy through read-only Kafka and file ports.
package inspect

import "github.com/pagrus7/kaferry/internal/plan"

type Report struct {
	Version                int                         `json:"version"`
	Fingerprint            string                      `json:"fingerprint"`
	Group                  string                      `json:"group"`
	OneOff                 bool                        `json:"one_off"`
	MaxOutOfOrderMS        *int64                      `json:"max_out_of_order_ms"`
	Offsets                *plan.OffsetRange           `json:"offset_range,omitempty"`
	PartitionOffsets       []plan.PartitionOffsetRange `json:"partition_offset_ranges,omitempty"`
	MaxStalenessMS         int64                       `json:"max_staleness_ms"`
	WorkerStopAfterRecords uint64                      `json:"worker_stop_after_records,omitempty"`
	Source                 Endpoint                    `json:"source"`
	Destination            Endpoint                    `json:"destination"`
	Partitions             []Partition                 `json:"partitions"`
	StartRule              string                      `json:"start_rule,omitempty"`
	RecordErrors           ErrorPolicies               `json:"record_errors"`
	Warnings               []string                    `json:"warnings"`
}

type ErrorPolicies struct {
	MissingTimestamp  string `json:"missing_timestamp"`
	RejectedTimestamp string `json:"rejected_timestamp"`
	OtherRecord       string `json:"other_record"`
}

type Endpoint struct {
	Kind            string                `json:"kind"`
	Format          string                `json:"format,omitempty"`
	Name            string                `json:"name"`
	Connection      string                `json:"connection,omitempty"`
	ClusterID       string                `json:"cluster_id,omitempty"`
	TopicID         string                `json:"topic_id,omitempty"`
	PartitionCount  int                   `json:"partition_count,omitempty"`
	Partitioning    string                `json:"partitioning,omitempty"`
	PartitionFilter *plan.PartitionFilter `json:"partition_filter,omitempty"`
	TimestampType   string                `json:"timestamp_type,omitempty"`
	File            *FileSummary          `json:"file,omitempty"`
}

type FileSummary struct {
	FileCount        int          `json:"file_count,omitempty"`
	PositionRecords  *int64       `json:"position_records,omitempty"`
	Exists           bool         `json:"exists"`
	SizeBytes        int64        `json:"size_bytes"`
	Window           *plan.Window `json:"window,omitempty"`
	WindowOrigin     string       `json:"window_origin,omitempty"`
	Checkpoint       string       `json:"checkpoint,omitempty"`
	CheckpointExists *bool        `json:"checkpoint_exists,omitempty"`
}

type Partition struct {
	ID           int32       `json:"partition"`
	Start        int64       `json:"start"`
	LogStart     int64       `json:"log_start"`
	LogEnd       int64       `json:"log_end"`
	EndOrigin    string      `json:"end_origin,omitempty"`
	End          *int64      `json:"captured_end,omitempty"`
	Saved        bool        `json:"saved"`
	Window       plan.Window `json:"window"`
	WindowOrigin string      `json:"window_origin"`
}
