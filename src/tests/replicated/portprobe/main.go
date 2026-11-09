// Command portprobe checks Toxiproxy listener reservation in a disposable network namespace.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

type proxy struct {
	Name     string `json:"name"`
	Listen   string `json:"listen"`
	Upstream string `json:"upstream"`
	Enabled  bool   `json:"enabled"`
}

type observation struct {
	ReservedPorts []int  `json:"reserved_ports"`
	SourcePorts   []int  `json:"upstream_source_ports"`
	RebindStatus  int    `json:"rebind_status"`
	SocketsHeld   int    `json:"sockets_held_at_rebind"`
	Exhausted     bool   `json:"finite_pool_exhausted"`
	Range         string `json:"ephemeral_range"`
}

func main() {
	value, err := run(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(value); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) (observation, error) {
	var observed observation
	if len(args) != 2 {
		return observed, fmt.Errorf("expected listener port and reservation")
	}
	port, err := strconv.Atoi(args[0])
	if err != nil {
		return observed, err
	}
	expected, err := ports(args[1])
	if err != nil {
		return observed, err
	}
	observed, err = kernel(expected, port)
	if err != nil {
		return observed, err
	}
	client := apiClient()
	defer client.CloseIdleConnections()
	if err = ready(client); err != nil {
		return observed, err
	}
	upstream, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 20000})
	if err != nil {
		return observed, err
	}
	defer upstream.Close()
	target := proxy{"reserved-listener", fmt.Sprintf("0.0.0.0:%d", port), "127.0.0.1:20000", true}
	other := proxy{"upstream-allocation", "0.0.0.0:20002", target.Upstream, true}
	if err = configure(client, target, other); err != nil {
		return observed, err
	}
	count := 4
	if len(expected) != 0 {
		count--
	}
	sockets, selected, err := exhaust(upstream, count)
	defer closeSockets(sockets)
	if err != nil {
		return observed, err
	}
	observed.SourcePorts, observed.SocketsHeld, observed.Exhausted = selected, len(selected), true
	status, body, err := update(client, "/proxies/"+target.Name, target)
	observed.RebindStatus = status
	if err != nil {
		return observed, err
	}
	if len(expected) == 0 && (status != 500 || !strings.Contains(body, "address already in use")) || len(expected) != 0 && status != 200 {
		return observed, fmt.Errorf("unexpected listener rebind: status %d, body %s", status, body)
	}
	return observed, nil
}

func ports(value string) ([]int, error) {
	var result []int
	for _, piece := range strings.Split(strings.TrimSpace(value), ",") {
		if piece == "" {
			continue
		}
		bounds := strings.Split(piece, "-")
		low, err := strconv.Atoi(bounds[0])
		if err != nil || len(bounds) > 2 {
			return nil, fmt.Errorf("invalid reservation %q", piece)
		}
		high := low
		if len(bounds) == 2 {
			high, err = strconv.Atoi(bounds[1])
		}
		if err != nil || low < 1 || low > high || high > 65535 {
			return nil, fmt.Errorf("invalid reservation %q", piece)
		}
		for port := low; port <= high; port++ {
			result = append(result, port)
		}
	}
	slices.Sort(result)
	return result, nil
}

func kernel(expected []int, port int) (observation, error) {
	var observed observation
	value, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_reserved_ports")
	if err != nil {
		return observed, err
	}
	observed.ReservedPorts, err = ports(string(value))
	if err != nil || !slices.Equal(expected, observed.ReservedPorts) {
		return observed, fmt.Errorf("kernel reservation differs: %q, expected %v", value, expected)
	}
	value, err = os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range")
	if err != nil {
		return observed, err
	}
	observed.Range = strings.Join(strings.Fields(string(value)), " ")
	if observed.Range != fmt.Sprintf("%d %d", port, port+3) {
		return observed, fmt.Errorf("kernel ephemeral range differs: %s", value)
	}
	return observed, nil
}

func apiClient() *http.Client {
	dialer := &net.Dialer{Timeout: time.Second, LocalAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 20001}}
	return &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{DialContext: dialer.DialContext, MaxConnsPerHost: 1}}
}

func ready(client *http.Client) error {
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		response, err := client.Get("http://127.0.0.1:8474/version")
		if err == nil {
			_, drainErr := io.Copy(io.Discard, response.Body)
			closeErr := response.Body.Close()
			if response.StatusCode == 200 && drainErr == nil && closeErr == nil {
				return nil
			}
		}
		select {
		case <-deadline.C:
			return fmt.Errorf("proxy API did not become available")
		case <-tick.C:
		}
	}
}

func configure(client *http.Client, target, other proxy) error {
	for _, value := range []proxy{target, other} {
		status, body, err := update(client, "/proxies", value)
		if err != nil || status != 201 {
			return errors.Join(fmt.Errorf("create proxy: status %d, body %s", status, body), err)
		}
	}
	target.Enabled = false
	status, body, err := update(client, "/proxies/"+target.Name, target)
	if err != nil || status != 200 {
		return errors.Join(fmt.Errorf("disable proxy: status %d, body %s", status, body), err)
	}
	return nil
}

func update(client *http.Client, path string, value proxy) (int, string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return 0, "", err
	}
	response, err := client.Post("http://127.0.0.1:8474"+path, "application/json", bytes.NewReader(data))
	if err != nil {
		return 0, "", err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 4096))
	return response.StatusCode, string(body), err
}

func exhaust(upstream *net.TCPListener, count int) ([]net.Conn, []int, error) {
	var sockets []net.Conn
	var selected []int
	for index := range count + 1 {
		dialer := net.Dialer{Timeout: time.Second, LocalAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 20010 + index}}
		client, err := dialer.Dial("tcp4", "127.0.0.1:20002")
		if err != nil {
			return sockets, selected, err
		}
		sockets = append(sockets, client)
		if index == count {
			if err = client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				return sockets, selected, err
			}
			var value [1]byte
			if _, err = client.Read(value[:]); !errors.Is(err, io.EOF) {
				return sockets, selected, errors.Join(errors.New("exhausted proxy connection did not close"), err)
			}
			break
		}
		if err = upstream.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			return sockets, selected, err
		}
		server, err := upstream.AcceptTCP()
		if err != nil {
			return sockets, selected, err
		}
		sockets = append(sockets, server)
		selected = append(selected, server.RemoteAddr().(*net.TCPAddr).Port)
	}
	slices.Sort(selected)
	return sockets, selected, nil
}

func closeSockets(sockets []net.Conn) {
	for _, socket := range sockets {
		_ = socket.Close()
	}
}
