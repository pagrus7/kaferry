package franz

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

const maxValueBytes = 96 << 20

type valueHead struct {
	Version, Size, Parts int
	SHA256               string
}

func valueDigest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func writeValue(ctx context.Context, client TransactionClient, topic string, part int32, key string, value []byte, beforeHead func()) error {
	if len(value) > maxValueBytes {
		return errors.New("checkpoint exceeds 96 MiB")
	}
	batch := int(client.OptValue(kgo.ProducerBatchMaxBytes).(int32))
	size := min(512<<10, batch-len(key)-256)
	if size < 1 {
		return errors.New("producer batch limit cannot hold checkpoint framing")
	}
	parts := (len(value) + size - 1) / size
	for i := range parts {
		record := &kgo.Record{Topic: topic, Partition: part, Key: []byte(key + "/chunk/" + strconv.Itoa(i)), Value: value[i*size : min((i+1)*size, len(value))]}
		if err := produceCheckpoint(ctx, client, record); err != nil {
			return err
		}
	}
	if beforeHead != nil {
		beforeHead()
	}
	head, _ := json.Marshal(valueHead{Version: 1, Size: len(value), Parts: parts, SHA256: valueDigest(value)})
	return produceCheckpoint(ctx, client, &kgo.Record{Topic: topic, Partition: part, Key: []byte(key), Value: head})
}

// Transactional produces cannot cancel individual ambiguous records. Abandon
// this client on deadline; a replacement must fence it before loading progress.
func produceCheckpoint(ctx context.Context, client TransactionClient, record *kgo.Record) error {
	done := make(chan error, 1)
	client.Produce(ctx, record, func(_ *kgo.Record, err error) { done <- err })
	if err := client.Flush(ctx); err != nil {
		client.Close()
		return err
	}
	return <-done
}

type valueReader struct {
	head   *valueHead
	chunks map[int][]byte
	bytes  int
	seen   bool
	err    error
}

func (r *valueReader) accept(key string, record *kgo.Record) error {
	if record.Attrs.IsControl() {
		return nil
	}
	name := string(record.Key)
	if name == key {
		var head valueHead
		if err := json.Unmarshal(record.Value, &head); err != nil {
			return err
		}
		if head.Version != 1 || head.Size < 1 || head.Size > maxValueBytes || head.Parts < 1 || head.Parts > head.Size {
			return errors.New("invalid checkpoint chunk header")
		}
		r.head = &head
		return nil
	}
	prefix := key + "/chunk/"
	if !strings.HasPrefix(name, prefix) {
		return nil
	}
	index, err := strconv.Atoi(strings.TrimPrefix(name, prefix))
	if err != nil || index < 0 || index >= maxValueBytes {
		return errors.New("invalid checkpoint chunk index")
	}
	r.seen = true
	// Writers emit chunk zero first and the head last in one transaction.
	// Compaction can retain trailing chunks from older, smaller chunk sizes.
	if index == 0 {
		r.head, r.err, r.bytes = nil, nil, 0
		r.chunks = make(map[int][]byte)
	}
	if r.chunks == nil {
		return nil
	}
	r.bytes += len(record.Value) - len(r.chunks[index])
	if r.bytes > maxValueBytes {
		// A later chunk zero can replace this incomplete historical generation.
		// Drop its buffers now, and reject it if no replacement follows.
		r.chunks = nil
		r.err = errors.New("checkpoint chunks exceed 96 MiB")
		return nil
	}
	r.chunks[index] = append([]byte{}, record.Value...)
	return nil
}
func (r *valueReader) value() ([]byte, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.head == nil {
		if r.seen {
			return nil, errors.New("checkpoint chunks lack a committed header")
		}
		return nil, nil
	}
	value := make([]byte, 0, r.head.Size)
	for i := range r.head.Parts {
		chunk, ok := r.chunks[i]
		if !ok {
			return nil, fmt.Errorf("checkpoint chunk %d missing", i)
		}
		if len(chunk) > r.head.Size-len(value) {
			return nil, errors.New("checkpoint chunks exceed declared size")
		}
		value = append(value, chunk...)
	}
	if len(value) != r.head.Size || valueDigest(value) != r.head.SHA256 {
		return nil, errors.New("checkpoint chunk checksum mismatch")
	}
	return value, nil
}
