package faults

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

type proxy struct {
	Name     string            `json:"name"`
	Listen   string            `json:"listen"`
	Upstream string            `json:"upstream"`
	Enabled  bool              `json:"enabled"`
	Toxics   []json.RawMessage `json:"toxics"`
}

func (l *Lab) request(ctx context.Context, method, path string, value, result any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, method, l.API+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	client := l.HTTP
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("proxy %s %s returned %d", method, path, response.StatusCode)
	}
	if result != nil {
		return json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(result)
	}
	_, err = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
	return err
}

func (l *Lab) ownedProxy(ctx context.Context, target Target, requireHealthy bool) error {
	owned, err := l.container(ctx, target.ProxyContainer, "toxiproxy")
	if err != nil {
		return err
	}
	if !owned.State.Running || owned.State.Paused {
		return errors.New("resolved proxy container is not running")
	}
	var current proxy
	if err := l.request(ctx, http.MethodGet, "/proxies/"+target.Proxy, nil, &current); err != nil {
		return err
	}
	port := 9092
	for index, name := range listeners {
		if name == target.Listener {
			port += index
		}
	}
	if current.Name != target.Proxy || current.Upstream != fmt.Sprintf("%s:%d", target.Service, port) ||
		current.Listen != fmt.Sprintf("0.0.0.0:%d", routePort(target.Cluster, target.Listener, target.Broker)) {
		return errors.New("proxy route does not belong to the resolved fixture target")
	}
	if requireHealthy && (!current.Enabled || len(current.Toxics) != 0) {
		return errors.New("proxy already has an active fault")
	}
	return nil
}

func (l *Lab) proxyFault(ctx context.Context, target Target, fault Fault) (restoration, error) {
	if err := l.ownedProxy(ctx, target, true); err != nil {
		return nil, err
	}
	path := "/proxies/" + target.Proxy
	if fault.Kind == "cut" {
		undo := func(heal context.Context) error {
			if err := l.ownedProxy(heal, target, false); err != nil {
				return err
			}
			return l.request(heal, http.MethodPost, path, map[string]bool{"enabled": true}, nil)
		}
		return undo, l.request(ctx, http.MethodPost, path, map[string]bool{"enabled": false}, nil)
	}
	name := "kaferry-" + rand.Text()
	stream := fault.Stream
	if stream == "" {
		stream = "downstream"
	}
	attribute := map[string]string{"timeout": "timeout", "reset_peer": "timeout", "latency": "latency", "bandwidth": "rate", "slow_close": "delay"}[fault.Kind]
	toxic := map[string]any{"name": name, "type": fault.Kind, "stream": stream, "toxicity": 1,
		"attributes": map[string]int64{attribute: fault.Value}}
	undo := func(heal context.Context) error { return l.removeToxic(heal, target, path, name) }
	return undo, l.request(ctx, http.MethodPost, path+"/toxics", toxic, nil)
}

func (l *Lab) removeToxic(ctx context.Context, target Target, path, name string) error {
	if err := l.ownedProxy(ctx, target, false); err != nil {
		return err
	}
	var toxics []struct{ Name string }
	if err := l.request(ctx, http.MethodGet, path+"/toxics", nil, &toxics); err != nil {
		return err
	}
	for _, toxic := range toxics {
		if toxic.Name == name {
			return l.request(ctx, http.MethodDelete, path+"/toxics/"+name, nil, nil)
		}
	}
	return nil
}
