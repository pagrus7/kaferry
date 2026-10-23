package inspect

import (
	"context"
	"time"

	"github.com/pagrus7/kaferry/internal/archiveio"
	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
)

type KafkaFactory interface {
	OpenInspection(context.Context, plan.Endpoint) (kafkaport.InspectionConnection, error)
}
type Files interface {
	Source(plan.Plan, string, config.Worker) (archiveio.SourceInspection, error)
	Destination(string) (archiveio.ArchiveInspection, error)
}
type Explainer interface {
	Explain(context.Context, plan.Plan, config.Worker, core.StopObserver) (Report, error)
}
type Service struct {
	kafka       KafkaFactory
	files       Files
	positioning kafkaport.PositionerFactory
}

func NewExplainer(kafka KafkaFactory, files Files, positioning kafkaport.PositionerFactory) *Service {
	return &Service{kafka: kafka, files: files, positioning: positioning}
}

func (s *Service) Explain(ctx context.Context, p plan.Plan, limits config.Worker, stop core.StopObserver) (report Report, result error) {
	if err := ctx.Err(); err != nil {
		return report, err
	}
	report = Report{Version: 1, Group: p.Group, OneOff: p.OneOff, CutoffMS: p.CutoffMS, WorkerStopAfterRecords: p.WorkerStopAfterRecords, StartRule: p.Start, RecordErrors: ErrorPolicies{p.Policies.MissingTimestamp, p.Policies.RejectedTimestamp, p.Policies.OtherRecord}, Partitions: []Partition{}, Warnings: []string{}}
	if p.OneOff {
		report.Group = "ONE_OFF"
	}
	stop.Stage("inspecting source metadata")
	source, sourceInfo, err := s.open(ctx, p.Source, false, stop)
	if err != nil {
		return report, err
	}
	defer closeConnection(source, stop, &result)
	report.Source = sourceInfo
	stop.Stage("inspecting destination metadata")
	destination, destinationInfo, err := s.open(ctx, p.Destination, true, stop)
	if err != nil {
		return report, err
	}
	defer closeConnection(destination, stop, &result)
	report.Destination = destinationInfo
	report.Fingerprint, err = p.Fingerprint(sourceInfo.ClusterID, destinationInfo.ClusterID)
	if err != nil {
		return report, &kafkaport.Conflict{Err: err}
	}
	stop.Stage("inspecting saved progress")
	ids, err := s.source(ctx, p, limits, source, &report)
	if err != nil {
		return report, err
	}
	if p.Destination.PreservesPartitions() {
		if err = plan.CheckPartitions(ids, report.Destination.PartitionCount); err != nil {
			return report, &kafkaport.Conflict{Err: err}
		}
	}
	stop.Stage("inspecting destination archive")
	if p.Destination.Kind == "FILE" {
		info, err := s.files.Destination(p.Destination.Name)
		if err != nil {
			return report, err
		}
		report.Destination.File = fileSummary(info)
	}
	addWarnings(&report)
	return report, ctx.Err()
}

func (s *Service) open(ctx context.Context, endpoint plan.Endpoint, configuration bool, stop core.StopObserver) (kafkaport.InspectionConnection, Endpoint, error) {
	info := Endpoint{Kind: endpoint.Kind, Name: endpoint.Name, Format: endpoint.Format, Connection: endpoint.Alias}
	if endpoint.Kind != "TOPIC" {
		return nil, info, nil
	}
	connection, err := s.kafka.OpenInspection(ctx, endpoint)
	if err != nil {
		return nil, info, err
	}
	topic, err := connection.Describe(ctx, configuration)
	if err != nil {
		closeConnection(connection, stop, &err)
		return nil, info, err
	}
	if configuration {
		info.Partitioning = endpoint.Partitioning
		if endpoint.PreservesPartitions() {
			info.Partitioning = plan.Preserve
		}
	}
	info.ClusterID, info.TopicID, info.PartitionCount, info.TimestampType = topic.ClusterID, topic.TopicID, len(topic.Partitions), topic.TimestampType
	return connection, info, nil
}

func closeConnection(connection kafkaport.InspectionConnection, stop core.StopObserver, result *error) {
	if connection == nil {
		return
	}
	stop.Begin()
	stop.Stage("closing inspection connection")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	*result = core.CleanupResult(*result, "close inspection connection", connection.Close(ctx))
}

func fileSummary(info archiveio.ArchiveInspection) *FileSummary {
	return &FileSummary{Exists: info.Exists, SizeBytes: info.Size, FileCount: info.FileCount}
}

func addWarnings(r *Report) {
	if r.Destination.Kind != "TOPIC" {
		return
	}
	switch r.Destination.TimestampType {
	case "LogAppendTime":
		r.Warnings = append(r.Warnings, "Destination LogAppendTime replaces copied timestamps with broker time.")
	case "":
		r.Warnings = append(r.Warnings, "Destination timestamp policy is unreadable; related checks remain unavailable.")
	}
}
