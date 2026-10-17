package kafkaport

import (
	"github.com/pagrus7/kaferry/internal/commitmeta"
)

func encodeMetadata(p Progress) string {
	s, err := commitmeta.NewCodec().Encode(p)
	if err != nil {
		panic(err)
	}
	return s
}
