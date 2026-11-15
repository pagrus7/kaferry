//go:build integration && fault && harness

package compat

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestHarnessReplicatedAuditArmsOnlyAfterCurrentGenerationAcceptedProgress(t *testing.T) {
	t.Run("post-transfer receipt", replicatedAuditRequiresPostTransferReceipt)
	// Given a worker whose earlier generation committed, then rejoined.
	rows := []string{
		`{"requestHeader":{"clientId":"one","requestApiKeyName":"JOIN_GROUP"},"request":{"groupId":"group"},"response":{"errorCode":0,"memberId":"member","generationId":1}}`,
		`{"requestHeader":{"clientId":"one","requestApiKeyName":"OFFSET_COMMIT"},"request":{"groupId":"group","memberId":"member","generationIdOrMemberEpoch":1,"topics":[{"partitions":[{"partitionIndex":0,"committedOffset":"5"},{"partitionIndex":1,"committedOffset":5}]}]},"response":{"topics":[{"partitions":[{"partitionIndex":0,"errorCode":0},{"partitionIndex":1,"errorCode":0}]}]}}`,
		`{"requestHeader":{"clientId":"one","requestApiKeyName":"JOIN_GROUP"},"request":{"groupId":"group"},"response":{"errorCode":0,"memberId":"member","generationId":2}}`,
	}
	encode := func() []byte {
		var data []byte
		for _, row := range rows {
			data = append(data, []byte("Completed request:"+row+" (kafka.request.logger)\n")...)
		}
		return data
	}
	generation, ready, err := currentGenerationSaved(encode(), "one", "group", "member", []int32{0, 1}, nil, time.Time{})
	require.NoError(t, err)
	require.EqualValues(t, 2, generation)
	require.False(t, ready, "earlier generation commits cannot arm a stale-ownership fault")

	// When current-generation progress is rejected, it still cannot arm the case.
	rows = append(rows, `{"requestHeader":{"clientId":"one","requestApiKeyName":"OFFSET_COMMIT"},"request":{"groupId":"group","memberId":"member","generationIdOrMemberEpoch":2,"topics":[{"partitions":[{"partitionIndex":0,"committedOffset":"6"},{"partitionIndex":1,"committedOffset":6}]}]},"response":{"topics":[{"partitions":[{"partitionIndex":0,"errorCode":22},{"partitionIndex":1,"errorCode":22}]}]}}`)
	_, ready, err = currentGenerationSaved(encode(), "one", "group", "member", []int32{0, 1}, nil, time.Time{})
	require.NoError(t, err)
	require.False(t, ready)

	// Then only actual accepted positive progress for every owned partition in
	// the latest joined generation permits injection.
	rows = append(rows, `{"requestHeader":{"clientId":"one","requestApiKeyName":"OFFSET_COMMIT"},"request":{"groupId":"group","memberId":"member","generationIdOrMemberEpoch":2,"topics":[{"partitions":[{"partitionIndex":0,"committedOffset":"6"},{"partitionIndex":1,"committedOffset":6}]}]},"response":{"topics":[{"partitions":[{"partitionIndex":0,"errorCode":0},{"partitionIndex":1,"errorCode":0}]}]}}`)
	_, ready, err = currentGenerationSaved(encode(), "one", "group", "member", []int32{0, 1}, nil, time.Time{})
	require.NoError(t, err)
	require.True(t, ready)
	_, ready, err = currentGenerationSaved(encode(), "one", "group", "member", []int32{0, 1}, map[int32]int64{0: 6, 1: 6}, time.Time{})
	require.NoError(t, err)
	require.False(t, ready, "peer progress must advance beyond the observed pre-fault prefix")
	_, ready, err = currentGenerationSaved(encode(), "one", "group", "member", []int32{0, 1, 2}, nil, time.Time{})
	require.NoError(t, err)
	require.False(t, ready, "an unobserved partition cannot be inferred from its peers")
}
