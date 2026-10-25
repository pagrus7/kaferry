package app

import (
	"context"
	"testing"

	"github.com/pagrus7/kaferry/internal/kafkaport"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

type replacementLookup struct {
	calls   []string
	queries []kafkaport.TimestampOffsetQuery
}

func (r *replacementLookup) New(kafkaport.OffsetReader, kafkaport.TimeReader) kafkaport.OffsetLookup {
	return r
}
func (r *replacementLookup) Earliest(context.Context, []int32) (map[int32]int64, error) {
	r.calls = append(r.calls, "earliest")
	return map[int32]int64{0: 0}, nil
}
func (r *replacementLookup) ReplicatedEnd(context.Context, []int32) (map[int32]int64, error) {
	r.calls = append(r.calls, "replicated")
	return map[int32]int64{0: 10}, nil
}
func (r *replacementLookup) ReadableEnd(context.Context, []int32) (map[int32]int64, error) {
	r.calls = append(r.calls, "readable")
	return map[int32]int64{0: 9}, nil
}
func (r *replacementLookup) AtOrAfterTimestamp(_ context.Context, q []kafkaport.TimestampOffsetQuery) ([]kafkaport.TimestampOffsetMatch, error) {
	r.queries = q
	return []kafkaport.TimestampOffsetMatch{{Partition: 0, Offset: 7, Found: true}}, nil
}
func TestFxOffsetLookupCanBeReplacedIndependently(t *testing.T) {
	// Given a replacement lookup factory, without a real broker backend.
	fake := &replacementLookup{}
	var factory kafkaport.PositionerFactory
	app := fx.New(fx.NopLogger, ClockModule, LoggingModule, PositioningModule,
		fx.Decorate(func(kafkaport.OffsetLookupFactory) kafkaport.OffsetLookupFactory { return fake }), fx.Populate(&factory))
	require.NoError(t, app.Err())
	// When resolving a timestamp start, then every lookup uses the injected component and explicit timestamp parameter.
	p := plan.Plan{Window: plan.WindowSpec{Lower: &plan.Expression{LiteralMS: 1234}}}
	got, err := factory.New(nil, nil, p, "fp", "topic").Locate(t.Context(), []int32{0}, nil, nil)
	require.NoError(t, err)
	require.EqualValues(t, 7, got.Assignments[0].Cursor.Position)
	require.Equal(t, []string{"earliest", "replicated"}, fake.calls)
	require.Equal(t, []kafkaport.TimestampOffsetQuery{{Partition: 0, TimestampMS: 1234}}, fake.queries)
}
