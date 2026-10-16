//go:build integration && franzspike

package franzspike

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

// replyGate drops produce replies after the broker has processed the requests.
// Requests and all other replies still use the broker's real network path.
type replyGate struct {
	after   atomic.Int64
	seen    atomic.Int64
	dropped atomic.Int64
}

func (g *replyGate) listen(network, address string) (net.Listener, error) {
	listener, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}
	return &gatedListener{Listener: listener, gate: g}, nil
}

type gatedListener struct {
	net.Listener
	gate *replyGate
}

func (l *gatedListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &gatedConn{Conn: conn, gate: l.gate, produce: make(map[uint32]bool)}, nil
}

type gatedConn struct {
	net.Conn
	gate    *replyGate
	mu      sync.Mutex
	produce map[uint32]bool
	input   []byte
	output  []byte
}

func (c *gatedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.input = append(c.input, p[:n]...)
	for len(c.input) >= 4 {
		size := int(binary.BigEndian.Uint32(c.input[:4])) + 4
		if len(c.input) < size {
			break
		}
		if size >= 12 && binary.BigEndian.Uint16(c.input[4:6]) == 0 {
			c.mu.Lock()
			c.produce[binary.BigEndian.Uint32(c.input[8:12])] = true
			c.mu.Unlock()
		}
		c.input = c.input[size:]
	}
	return n, err
}

func (c *gatedConn) Write(p []byte) (int, error) {
	c.output = append(c.output, p...)
	for len(c.output) >= 4 {
		size := int(binary.BigEndian.Uint32(c.output[:4])) + 4
		if len(c.output) < size {
			break
		}
		frame := c.output[:size]
		c.output = c.output[size:]
		c.mu.Lock()
		correlation := binary.BigEndian.Uint32(frame[4:8])
		produce := c.produce[correlation]
		delete(c.produce, correlation)
		c.mu.Unlock()
		if after := c.gate.after.Load(); produce && after > 0 && c.gate.seen.Add(1) == after {
			c.gate.dropped.Add(1)
			continue
		}
		for len(frame) > 0 {
			n, err := c.Conn.Write(frame)
			if err != nil {
				return 0, err
			}
			if n == 0 {
				return 0, io.ErrShortWrite
			}
			frame = frame[n:]
		}
	}
	return len(p), nil
}
