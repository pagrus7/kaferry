//go:build integration && franzspike

package franzspike

import (
	"encoding/base64"
	"errors"
	"testing"
	"time"

	core "github.com/pagrus7/kaferry/internal/copy"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func wireGap(t *testing.T, name string) *fetchGap {
	t.Helper()
	c := client(t)
	admin := kadm.NewClient(c)
	metadata, err := admin.Metadata(ctx(t), name)
	if err != nil {
		t.Fatal(err)
	}
	id := metadata.Topics[name].ID
	if metadata.Topics[name].Err != nil || id == ([16]byte{}) {
		t.Fatal("gap fixture has no valid topic identity", metadata.Topics[name].Err)
	}
	return &fetchGap{client: c, topic: name, topicID: base64.RawStdEncoding.EncodeToString(id[:])}
}

func TestSpikeGapProofReadsAbortedTransactions(t *testing.T) {
	// Given three aborted records followed by their broker-written control marker.
	src := topic(t, 1)
	transaction := client(t, kgo.TransactionalID(src+"-gap"))
	if err := transaction.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := transaction.ProduceSync(ctx(t), &kgo.Record{Topic: src, Value: []byte("aborted")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	if err := transaction.EndTransaction(ctx(t), kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	proof := wireGap(t, src)
	admin := kadm.NewClient(proof.client)
	deadline := ctx(t)
	var end int64
	for {
		high, err := admin.ListEndOffsets(deadline, src)
		if err != nil {
			t.Fatal(err)
		}
		stable, err := admin.ListCommittedOffsets(deadline, src)
		if err != nil {
			t.Fatal(err)
		}
		end = high[src][0].Offset
		if end > 3 && stable[src][0].Offset == end {
			break
		}
		select {
		case <-deadline.Done():
			t.Fatal("abort marker never reached stable end")
		case <-time.After(20 * time.Millisecond):
		}
	}
	// When the independent gap reader fetches and parses those actual broker batches.
	next, err := proof.Next(deadline, 0, 0)
	// Then aborted data and its marker advance the cursor without a user record.
	if err != nil || next != end {
		t.Fatal("abort gap was not proven", next, end, err)
	}
	// When a retained user record is appended after that gap.
	producer := client(t)
	if err := producer.ProduceSync(deadline, &kgo.Record{Topic: src, Value: []byte("retained")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	next, err = proof.Next(deadline, 0, 0)
	// Then a proof spanning the retained record cannot advance.
	if err != nil || next != 0 {
		t.Fatal("proof skipped retained record after abort", next, err)
	}
}

func TestSpikeGapProofRejectsRecreatedTopic(t *testing.T) {
	// Given a gap reader bound to the original source topic identity.
	src := topic(t, 1)
	proof := wireGap(t, src)
	admin := kadm.NewClient(proof.client)
	if _, err := admin.DeleteTopics(ctx(t), src); err != nil {
		t.Fatal(err)
	}
	deadline := ctx(t)
	for {
		_, err := admin.CreateTopic(deadline, 1, 1, nil, src)
		if err == nil {
			break
		}
		if !errors.Is(err, kerr.TopicAlreadyExists) {
			t.Fatal(err)
		}
		select {
		case <-deadline.Done():
			t.Fatal("deleted fixture topic did not become recreatable")
		case <-time.After(20 * time.Millisecond):
		}
	}
	// When a topic with the same name but a different identity is probed.
	for {
		next, err := proof.Next(deadline, 0, 0)
		// Then stale broker metadata cannot advance progress, and refreshed metadata fails permanently.
		if err == nil || next != 0 {
			t.Fatal("gap proof accepted a different topic", next, err)
		}
		var retry *core.Recoverable
		if !errors.As(err, &retry) {
			break
		}
		select {
		case <-deadline.Done():
			t.Fatal("recreated topic identity never became a terminal error")
		case <-time.After(20 * time.Millisecond):
		}
	}
}
