package franz

import (
	"github.com/pagrus7/kaferry/src/internal/commitmeta"
	"github.com/pagrus7/kaferry/src/internal/kafkaport"
)

func encodeMetadata(p kafkaport.Progress) string {
	s, err := commitmeta.NewCodec().Encode(p)
	if err != nil {
		panic(err)
	}
	return s
}
