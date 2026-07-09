/*
MIT License

Copyright (c) 2026 The Trzsz SSH Authors.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

package tsshd

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

// fakeDatagramConn records SendDatagram calls and enforces a fixed budget.
type fakeDatagramConn struct {
	mu      sync.Mutex
	maxSize uint16
	sent    [][]byte
	recvCh  chan []byte
}

func newFakeDatagramConn(maxSize uint16) *fakeDatagramConn {
	return &fakeDatagramConn{maxSize: maxSize, recvCh: make(chan []byte, 16)}
}

func (c *fakeDatagramConn) GetMaxDatagramSize() uint16 {
	return c.maxSize
}

func (c *fakeDatagramConn) SendDatagram(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent = append(c.sent, append([]byte(nil), data...))
	return nil
}

func (c *fakeDatagramConn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	select {
	case buf := <-c.recvCh:
		return buf, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *fakeDatagramConn) sentCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.sent)
}

// fakeStream is an in-memory Stream that records writes (the reliable
// stream fallback path).
type fakeStream struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *fakeStream) Read(p []byte) (int, error) {
	select {} // tests never read; block forever like an idle stream
}

func (s *fakeStream) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *fakeStream) Close() error                       { return nil }
func (s *fakeStream) CloseRead() error                   { return nil }
func (s *fakeStream) CloseWrite() error                  { return nil }
func (s *fakeStream) LocalAddr() net.Addr                { return nil }
func (s *fakeStream) RemoteAddr() net.Addr               { return nil }
func (s *fakeStream) SetDeadline(t time.Time) error      { return nil }
func (s *fakeStream) SetReadDeadline(t time.Time) error  { return nil }
func (s *fakeStream) SetWriteDeadline(t time.Time) error { return nil }

func (s *fakeStream) written() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Len()
}

func newTestPacketConn(t *testing.T, budget uint16, datagramOnly bool) (*packetConn, *fakeDatagramConn, *fakeStream) {
	t.Helper()
	dc := newFakeDatagramConn(budget)
	forwarder := &udpForwarder{conn: dc}
	stream := &fakeStream{}
	checker := newTimeoutChecker(0)
	t.Cleanup(checker.Close)
	conn := newPacketConn(stream, 1, forwarder, checker)
	conn.datagramOnly = datagramOnly
	t.Cleanup(func() { _ = conn.Close() })
	return conn, dc, stream
}

func TestPacketConn_DatagramOnlyDropsOversize(t *testing.T) {
	conn, dc, stream := newTestPacketConn(t, 100, true)

	// Fits the budget (payload + 8-byte channel tag): sent as a datagram.
	if err := conn.Write(make([]byte, 92)); err != nil {
		t.Fatalf("in-budget write failed: %v", err)
	}
	if dc.sentCount() != 1 {
		t.Fatalf("expected 1 datagram sent, got %d", dc.sentCount())
	}

	// Oversize: dropped silently, never the reliable stream.
	if err := conn.Write(make([]byte, 500)); err != nil {
		t.Fatalf("oversize write should drop, got error: %v", err)
	}
	if dc.sentCount() != 1 {
		t.Fatalf("oversize payload must not be sent as a datagram")
	}
	if stream.written() != 0 {
		t.Fatalf("oversize payload must not fall back to the stream, wrote %d bytes", stream.written())
	}
}

func TestPacketConn_DefaultKeepsStreamFallback(t *testing.T) {
	conn, dc, stream := newTestPacketConn(t, 100, false)

	if err := conn.Write(make([]byte, 500)); err != nil {
		t.Fatalf("fallback write failed: %v", err)
	}
	if dc.sentCount() != 0 {
		t.Fatalf("oversize payload must not be sent as a datagram")
	}
	if stream.written() == 0 {
		t.Fatalf("oversize payload should have fallen back to the stream")
	}
}

func TestPacketConn_DatagramOnlyDropsDuringReconnect(t *testing.T) {
	conn, dc, stream := newTestPacketConn(t, 100, true)
	conn.peerCheck.timeoutFlag.Store(true)

	// Must neither block waiting for reconnection nor use the stream.
	if err := conn.Write(make([]byte, 50)); err != nil {
		t.Fatalf("write during reconnect should drop, got error: %v", err)
	}
	if dc.sentCount() != 0 || stream.written() != 0 {
		t.Fatalf("write during reconnect must be dropped")
	}
}

func TestPacketConn_DatagramOnlyClosedForwarder(t *testing.T) {
	conn, _, _ := newTestPacketConn(t, 100, true)
	conn.forwarder.Close()

	if err := conn.Write(make([]byte, 50)); err == nil {
		t.Fatalf("write on closed forwarder should return an error")
	}
}
