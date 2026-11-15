// Package guarantees indexes Kaferry's user-visible guarantees and the tests
// that prove them. Each guarantee links to the design section that states it.
//
// Tests cite guarantees with a directive line directly above the test function:
//
//	//kaferry:guarantee delivery rebalance
//	func TestWorkerCrashBeforeDeliveryProcessingReplaysWithoutLoss(t *testing.T) {
//
// The package test fails when a guarantee has no evidence, a directive names an
// unknown guarantee, sits outside a test function's doc comment, or is placed
// on a harness test.
package guarantees

// Guarantee names one documented contract.
type Guarantee struct {
	ID  string
	Doc string // repository-relative Markdown path and heading anchor
}

const (
	decisions = "docs/design/decisions.md"
	archive   = "docs/design/archive.md"
)

// Registry lists every guarantee that needs test evidence.
var Registry = []Guarantee{
	{"fidelity", decisions + "#scope"},
	{"delivery", decisions + "#delivery-and-progress"},
	{"caps", decisions + "#delivery-and-progress"},
	{"shutdown", decisions + "#delivery-and-progress"},
	{"window", decisions + "#time-window"},
	{"start", decisions + "#kafka-start-positions"},
	{"partitions", decisions + "#source-partitions"},
	{"completion.timestamp", decisions + "#completion"},
	{"completion.offset-end", decisions + "#offset-ends"},
	{"assignment.added", decisions + "#added-assignments-and-lookups"},
	{"inspect", "docs/reference.md#inspection"},
	{"worker-stop", decisions + "#worker-record-stop"},
	{"exit", decisions + "#when-a-worker-exits"},
	{"groups", decisions + "#group-names"},
	{"rebalance", decisions + "#rebalances"},
	{"identity", decisions + "#identity"},
	{"commit-metadata", decisions + "#commit-metadata"},
	{"output.routing", decisions + "#kafka-output"},
	{"output.records", decisions + "#kafka-output"},
	{"errors", decisions + "#errors"},
	{"archive.capture", archive + "#capture-and-recovery"},
	{"archive.ownership", archive + "#layout-and-ownership"},
	{"archive.rotation", archive + "#rotation"},
	{"replay", archive + "#replay-and-progress"},
	{"security", "docs/design/client-compatibility.md#tested-configurations"},
	{"docs.examples", "README.md#docker-quick-start"},
}
