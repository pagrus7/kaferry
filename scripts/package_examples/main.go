// Command package_examples builds the downloadable tutorial from editable sources.
package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pagrus7/kaferry/internal/archive/avro"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: go run ./scripts/package_examples OUTPUT.tar.gz")
		os.Exit(2)
	}
	if err := build(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func build(path string) error {
	source := "examples/archive-demo"
	data, err := os.ReadFile(filepath.Join(source, "records.json"))
	if err != nil {
		return err
	}
	var values []string
	if err := json.Unmarshal(data, &values); err != nil {
		return err
	}
	codec, err := avro.New(64 << 20)
	if err != nil {
		return err
	}
	identity := avro.Identity{Version: avro.Version, WriterID: strings.Repeat("1", 32), FileID: strings.Repeat("2", 32),
		Source: avro.Source{Cluster: "tutorial", Topic: "orders", TopicID: avro.TopicID{1}, Partition: 0}}
	var input bytes.Buffer
	writer, err := codec.NewWriter(&input, identity)
	if err != nil {
		return err
	}
	for offset, value := range values {
		stamp := int64(1790845200000 + offset*1000)
		record := avro.Record{Source: identity.Source, Offset: int64(offset), TimestampMS: &stamp,
			TimestampKind: "create_time", Key: []byte("customer-1"), Value: []byte(value),
			Headers: []avro.Header{{Key: []byte("example"), Value: []byte("archive-demo")}}}
		if err := writer.Append(record); err != nil {
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	output, err := os.Create(path)
	if err != nil {
		return err
	}
	defer output.Close()
	compressed := gzip.NewWriter(output)
	archive := tar.NewWriter(compressed)
	add := func(name string, data []byte) error {
		if err := archive.WriteHeader(&tar.Header{Name: "archive-demo/" + name, Mode: 0o644, Size: int64(len(data))}); err != nil {
			return err
		}
		_, err := archive.Write(data)
		return err
	}
	for _, name := range []string{"copy.sql", "tutorial.toml"} {
		data, err := os.ReadFile(filepath.Join(source, name))
		if err != nil {
			return err
		}
		if err := add(name, data); err != nil {
			return err
		}
	}
	if err := add("input/"+identity.WriterID+"."+identity.FileID+".avro", input.Bytes()); err != nil {
		return err
	}
	if err := archive.Close(); err != nil {
		return err
	}
	if err := compressed.Close(); err != nil {
		return err
	}
	return output.Close()
}
