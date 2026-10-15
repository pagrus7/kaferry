//go:build franzspike

package franzspike

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/pagrus7/kaferry/internal/config"
	core "github.com/pagrus7/kaferry/internal/copy"
	archive "github.com/pagrus7/kaferry/internal/file"
	"github.com/pagrus7/kaferry/internal/plan"
)

// Execute is a preflight harness for literal windows and named groups only.
// The production engine and configuration mapper are deliberately not replaced.
func Execute(ctx context.Context, p plan.Plan, limits config.Worker, logger *slog.Logger) error {
	if p.OneOff || p.Window.Relative() {
		return fmt.Errorf("spike requires a named group and literal time bounds")
	}
	sourceCluster, sourceID, err := EndpointIdentity(ctx, p.Source)
	if err != nil {
		return err
	}
	destCluster, _, err := EndpointIdentity(ctx, p.Destination)
	if err != nil {
		return err
	}
	fp, err := p.Fingerprint(sourceCluster, destCluster)
	if err != nil {
		return err
	}
	files := archive.NewFactory(core.RealClock{}, logger, archive.DurableWriter{}, archive.NewPublisher())
	var source core.Source
	var store core.ProgressStore
	var input archive.Input
	var kafka *Source
	if p.Source.Kind == "TOPIC" {
		kafka, err = NewSource(p, fp, sourceID)
		if err != nil {
			return err
		}
		source, store = kafka, kafka
	} else {
		input, err = files.OpenSource(p, fp, limits)
		if err != nil {
			return err
		}
		source, store = input, input
	}
	var sink core.Sink
	if p.Destination.Kind == "TOPIC" {
		sink, err = NewSink(p, limits)
	} else {
		if _, statErr := os.Stat(p.Destination.Name); statErr == nil {
			source.Close()
			return fmt.Errorf("spike file-output recovery is not implemented; use a fresh output")
		}
		var output archive.OutputSink
		output, err = files.OpenOutput(p, fp, archive.DestinationProgress{}, limits)
		if err == nil {
			sink = output
			if kafka != nil {
				kafka.BindOutput(output)
			} else {
				input.BindOutput(output)
			}
		}
	}
	if err != nil {
		source.Close()
		return err
	}
	execution := core.NewRunner(source, sink, store, core.RealClock{}, logger, p, limits, core.NewTracker())
	result := execution.Run(ctx)
	closeErr := source.Close()
	sink.Close()
	if closeErr != nil && !errors.Is(closeErr, context.Canceled) {
		return errors.Join(result, closeErr)
	}
	return result
}
