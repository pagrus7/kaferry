// Package inspect previews a copy through read-only Kafka and file ports.
package inspect

import "github.com/pagrus7/kaferry/internal/plan"

type Report struct {
	Version        int         `json:"version"`
	Fingerprint    string      `json:"fingerprint"`
	Group          string      `json:"group"`
	OneOff         bool        `json:"one_off"`
	CutoffMS       int64       `json:"cutoff_ms"`
	Source         Endpoint    `json:"source"`
	Destination    Endpoint    `json:"destination"`
	Partitions     []Partition `json:"partitions"`
	RunPermissions []string    `json:"run_permissions"`
	Warnings       []string    `json:"warnings"`
}

type Endpoint struct {
	Kind           string       `json:"kind"`
	Name           string       `json:"name"`
	Connection     string       `json:"connection,omitempty"`
	ClusterID      string       `json:"cluster_id,omitempty"`
	TopicID        string       `json:"topic_id,omitempty"`
	PartitionCount int          `json:"partition_count,omitempty"`
	TimestampType  string       `json:"timestamp_type,omitempty"`
	File           *FileSummary `json:"file,omitempty"`
}

type FileSummary struct {
	Exists             bool         `json:"exists"`
	ID                 string       `json:"id,omitempty"`
	SizeBytes          int64        `json:"size_bytes"`
	StartBytes         *int64       `json:"start_bytes,omitempty"`
	Window             *plan.Window `json:"window,omitempty"`
	Checkpoint         string       `json:"checkpoint,omitempty"`
	CheckpointExists   *bool        `json:"checkpoint_exists,omitempty"`
	AcknowledgedExtent *int64       `json:"acknowledged_extent_bytes,omitempty"`
}

type Partition struct {
	ID           int32       `json:"partition"`
	Start        int64       `json:"start"`
	LookupEnd    *int64      `json:"lookup_end"`
	Span         *int64      `json:"span"`
	LogStart     int64       `json:"log_start"`
	LogEnd       int64       `json:"log_end"`
	Saved        bool        `json:"saved"`
	Window       plan.Window `json:"window"`
	WindowOrigin string      `json:"window_origin"`
}
