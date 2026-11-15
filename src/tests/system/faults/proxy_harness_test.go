//go:build harness

package faults

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mutationTransport struct {
	base                           http.RoundTripper
	preflight, submitted, response time.Time
}

func (m *mutationTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/toxics") {
		m.submitted = time.Now().UTC()
	}
	response, err := m.base.RoundTrip(r)
	if r.Method == http.MethodGet && m.submitted.IsZero() {
		m.preflight = time.Now().UTC()
	} else if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/toxics") {
		m.response = time.Now().UTC()
	}
	return response, err
}

func TestHarnessToxicMutationReceiptExcludesPreflightAndRequiresAcceptance(t *testing.T) {
	for _, rejected := range []bool{false, true} {
		t.Run(fmt.Sprint(rejected), func(t *testing.T) {
			// Given route inspection and an API that accepts or rejects the toxic.
			lab, proxies, _ := newProxyStub(t)
			transport := &mutationTransport{base: lab.HTTP.Transport}
			lab.HTTP.Transport = transport
			if rejected {
				proxies.fail = "POST /proxies/source-2-worker1/toxics"
			}
			// When injecting, then submission follows preflight and precedes the POST.
			injection, err := lab.Inject(t.Context(), Role{Kind: "worker-coordinator", Cluster: "source", Group: "copy", Worker: 1},
				Fault{Kind: "timeout", Stream: "upstream"})
			require.Len(t, injection.Event.Mutations, 1)
			mutation := injection.Event.Mutations[0]
			require.Equal(t, injection.Event.Targets[0], mutation.Target)
			require.False(t, transport.preflight.IsZero())
			require.False(t, mutation.Started.Before(transport.preflight))
			require.False(t, mutation.Started.After(transport.submitted))
			// Then a failed mutation never fabricates an acceptance receipt.
			if rejected {
				require.Error(t, err)
				require.True(t, mutation.Accepted.IsZero())
			} else {
				require.NoError(t, err)
				require.False(t, mutation.Accepted.Before(transport.response))
				require.NoError(t, injection.Heal(t.Context()))
			}
		})
	}
}
