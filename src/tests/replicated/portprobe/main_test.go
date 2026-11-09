package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReservationsRequireValidBoundaries(t *testing.T) {
	// Given kernel reservations written as individual ports or inclusive ranges.
	for _, test := range []struct {
		value string
		want  []int
	}{
		{"", nil},
		{" 65535,1-3\n", []int{1, 2, 3, 65535}},
	} {
		// When parsed, then every reserved port is retained in numeric order.
		got, err := ports(test.value)
		require.NoError(t, err)
		require.Equal(t, test.want, got)
	}
	for _, value := range []string{"0", "65536", "2-1", "one", "1-x", "1-2-3"} {
		// Given invalid boundaries, when parsed, then no reservation is accepted.
		_, err := ports(value)
		require.Error(t, err, "accepted reservation %q", value)
	}
}

func TestProbeRejectsMalformedArguments(t *testing.T) {
	// Given incomplete or invalid probe arguments.
	for _, args := range [][]string{nil, {"46394"}, {"not-a-port", ""}, {"46394", "0"}} {
		// When invoked, then reject them before reading kernel state or changing proxies.
		_, err := run(args)
		require.Error(t, err, "accepted arguments %v", args)
	}
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestConfigureStopsAtRejectedMutation(t *testing.T) {
	// Given a proxy API that rejects either creation or disabling the target.
	for _, statuses := range [][]int{{503}, {201, 409}, {201, 201, 409}} {
		t.Run(fmt.Sprint(statuses), func(t *testing.T) {
			calls := 0
			client := &http.Client{Transport: roundTripper(func(request *http.Request) (*http.Response, error) {
				require.Less(t, calls, len(statuses), "continued mutating after rejection")
				var value proxy
				require.NoError(t, json.NewDecoder(request.Body).Decode(&value))
				require.Equal(t, http.MethodPost, request.Method)
				if calls == 2 {
					require.Equal(t, "/proxies/target", request.URL.Path)
					require.False(t, value.Enabled, "target was not disabled")
				} else {
					require.Equal(t, "/proxies", request.URL.Path)
					require.True(t, value.Enabled, "creation unexpectedly disabled the proxy")
				}
				status := statuses[calls]
				calls++
				return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("rejected"))}, nil
			})}
			// When configuration reaches that mutation, then report it and issue no later mutations.
			err := configure(client, proxy{Name: "target", Enabled: true}, proxy{Name: "other", Enabled: true})
			require.ErrorContains(t, err, "rejected")
			require.Equal(t, len(statuses), calls)
		})
	}
}

func TestConfigurePreservesTransportFailure(t *testing.T) {
	// Given an unavailable API, when configuring a proxy, then preserve its transport error.
	failure := errors.New("connection unavailable")
	client := &http.Client{Transport: roundTripper(func(*http.Request) (*http.Response, error) {
		return nil, failure
	})}
	require.ErrorIs(t, configure(client, proxy{}, proxy{}), failure)
}

func TestReadyWaitsForSuccessfulAPIResponse(t *testing.T) {
	// Given an API that first cannot connect, then reports unavailable, then serves a response.
	calls := 0
	client := &http.Client{Transport: roundTripper(func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("connection refused")
		}
		status := http.StatusServiceUnavailable
		if calls == 3 {
			status = http.StatusOK
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("version"))}, nil
	})}
	// When waiting for readiness, then continue until the successful response has been read.
	require.NoError(t, ready(client))
	require.Equal(t, 3, calls)
}
