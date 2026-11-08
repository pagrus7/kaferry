package franz

import (
	"github.com/pagrus7/kaferry/src/internal/kafka"
	"github.com/pagrus7/kaferry/src/internal/kafka/commitmeta"
)

func encodeMetadata(p kafka.Progress) string {
	s, err := commitmeta.NewCodec().Encode(p)
	if err != nil {
		panic(err)
	}
	return s
}
