// Package output serialises outbound messages and writes them to the client conn.
package output

import (
	"log/slog"
	"net"
	"sync"

	"github.com/caiusseverus/adsb-dashboard/radar-core/protocol"
)

// Writer holds the current client connection and writes framed msgpack messages.
// Thread-safe: multiple goroutines may call Send concurrently.
type Writer struct {
	mu   sync.Mutex
	conn net.Conn
}

func NewWriter() *Writer { return &Writer{} }

// SetConn replaces the active connection. Pass nil to disconnect.
func (w *Writer) SetConn(conn net.Conn) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.conn = conn
}

// Send encodes msg as msgpack and writes a length-prefixed frame.
// If no connection is active, the message is silently dropped.
func (w *Writer) Send(msg interface{}) {
	payload, err := protocol.Encode(msg)
	if err != nil {
		slog.Warn("output: encode failed", "err", err)
		return
	}
	w.mu.Lock()
	conn := w.conn
	w.mu.Unlock()
	if conn == nil {
		return
	}
	if err := protocol.WriteFrame(conn, payload); err != nil {
		slog.Warn("output: write failed", "err", err)
		// Don't clear conn here — ingest will detect the disconnect and call SetConn(nil).
	}
}

// SendBurstFired is a typed convenience wrapper.
func (w *Writer) SendBurstFired(msg *protocol.BurstFired) { w.Send(msg) }

// SendIIDState is a typed convenience wrapper.
func (w *Writer) SendIIDState(msg *protocol.IIDState) { w.Send(msg) }

// SendHealth is a typed convenience wrapper.
func (w *Writer) SendHealth(msg *protocol.Health) { w.Send(msg) }

// SendFrameReady is a typed convenience wrapper.
func (w *Writer) SendFrameReady(msg *protocol.FrameReady) { w.Send(msg) }

// SendSnapshotResp is a typed convenience wrapper.
func (w *Writer) SendSnapshotResp(msg *protocol.SnapshotResp) { w.Send(msg) }
