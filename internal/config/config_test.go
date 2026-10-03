package config

import (
	"strings"
	"testing"
)

func TestConfiguration(t *testing.T) {
	input := "[connections.prod]\n'bootstrap.servers'='prod:9092'\n'sasl.password'='secret'\n[worker]\npartition_record_cap=100\nidle_exit='5s'\n"
	c, err := Decode(strings.NewReader(input), "/home/test")
	if err != nil {
		t.Fatal(err)
	}
	if c.Connections["prod"]["bootstrap.servers"] != "prod:9092" || c.Worker.PartitionRecordCap != 100 || c.Worker.PartitionByteCap != 16<<20 || c.Worker.IdleExit != "5s" {
		t.Fatalf("incorrect config defaults or overrides: %+v", c.Worker)
	}
	for _, input := range []string{"unknown=1", "[worker]\nrecord_cap=0", "[worker]\nidle_exit='0s'", "[worker]\nunknown=3", "[connections.prod]\n'sasl.password'= 'unique-secret' trailing", "[connections.prod]\n'bootstrap.servers'=123"} {
		_, err := Decode(strings.NewReader(input), "/home/test")
		if err == nil || strings.Contains(err.Error(), "unique-secret") {
			t.Errorf("invalid or unsafe diagnostic: %v", err)
		}
	}
}
