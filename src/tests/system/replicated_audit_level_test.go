//go:build integration && fault

package compat

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestReplicatedAuditLevelRetriesConnectionErrorOnce(t *testing.T) {
	other := errors.New("request rejected locally")
	for _, test := range []struct {
		name      string
		errors    []error
		code      int16
		wantError error
	}{
		{name: "success", errors: []error{nil}},
		{name: "EOF then success", errors: []error{io.EOF, nil}},
		{name: "wrapped reset then success", errors: []error{fmt.Errorf("read: %w", syscall.ECONNRESET), nil}},
		{name: "closed then success", errors: []error{net.ErrClosed, nil}},
		{name: "broken pipe then success", errors: []error{syscall.EPIPE, nil}},
		{name: "broker error", errors: []error{nil}, code: kerr.ClusterAuthorizationFailed.Code, wantError: kerr.ClusterAuthorizationFailed},
		{name: "request broker error", errors: []error{kerr.NotController}, wantError: kerr.NotController},
		{name: "two EOFs", errors: []error{io.EOF, io.EOF}, wantError: io.EOF},
		{name: "deadline", errors: []error{context.DeadlineExceeded}, wantError: context.DeadlineExceeded},
		{name: "canceled", errors: []error{context.Canceled}, wantError: context.Canceled},
		{name: "other error", errors: []error{other}, wantError: other},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Given a logger update whose request has the specified outcomes.
			calls := 0
			ctx := t.Context()
			audit := &replicatedAudit{levelRequest: func(query context.Context, request kmsg.Request) (kmsg.Response, error) {
				require.Equal(t, ctx, query)
				update := request.(*kmsg.IncrementalAlterConfigsRequest)
				require.Len(t, update.Resources, 1)
				resource := update.Resources[0]
				require.Equal(t, kmsg.ConfigResourceTypeBrokerLogger, resource.ResourceType)
				require.Equal(t, "2", resource.ResourceName)
				require.Len(t, resource.Configs, 1)
				require.Equal(t, requestLogger, resource.Configs[0].Name)
				require.Equal(t, "DEBUG", *resource.Configs[0].Value)
				require.Less(t, calls, len(test.errors), "unexpected extra request")
				err := test.errors[calls]
				calls++
				return &kmsg.IncrementalAlterConfigsResponse{Resources: []kmsg.IncrementalAlterConfigsResponseResource{{ErrorCode: test.code}}}, err
			}}
			// When that level is applied to the selected broker.
			err := audit.level(ctx, 2, requestLogger, "DEBUG")
			// Then only a connection failure permits one identical retry.
			if test.wantError == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, test.wantError)
			}
			require.Equal(t, len(test.errors), calls)
		})
	}
}
