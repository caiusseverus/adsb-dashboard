// Package output serialises outbound messages and writes them to the client conn.
package output

import (
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/caiusseverus/adsb-dashboard/radar-core/protocol"
)

// Writer holds the current client connection and writes framed msgpack messages.
// Thread-safe: multiple goroutines may call Send concurrently.
type Writer struct {
	mu       sync.Mutex
	conn     net.Conn
	ipcDebug bool
}

func NewWriter() *Writer {
	return &Writer{ipcDebug: envDebugEnabled("RADAR_CORE_IPC_DEBUG")}
}

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
	defer w.mu.Unlock()
	conn := w.conn
	if conn == nil {
		return
	}
	if w.ipcDebug {
		slog.Debug(
			"output: write frame",
			"type", messageTypeLabel(msg),
			"payload_len", len(payload),
			"frame_len", len(payload)+4,
		)
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

// SendFMFrameResult is a typed convenience wrapper.
func (w *Writer) SendFMFrameResult(msg *protocol.FMFrameResult) { w.Send(msg) }

// SendFMState is a typed convenience wrapper.
func (w *Writer) SendFMState(msg *protocol.FMState) { w.Send(msg) }

// SendSnapshotResp is a typed convenience wrapper.
func (w *Writer) SendSnapshotResp(msg *protocol.SnapshotResp) { w.Send(msg) }

func envDebugEnabled(key string) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func messageTypeLabel(msg interface{}) string {
	switch msg.(type) {
	case *protocol.BurstFired:
		return "BURST_FIRED"
	case *protocol.FrameReady:
		return "FRAME_READY"
	case *protocol.IIDState:
		return "IID_STATE"
	case *protocol.SnapshotResp:
		return "SNAPSHOT_RESP"
	case *protocol.Health:
		return "HEALTH"
	case *protocol.FMFrameResult:
		return "FM_FRAME_RESULT"
	case *protocol.FMState:
		return "FM_STATE"
	case *protocol.RadarEvent:
		return "RADAR_EVENT"
	case *protocol.PositionUpdate:
		return "POSITION_UPDATE"
	case *protocol.ConfigUpdate:
		return "CONFIG_UPDATE"
	case *protocol.SnapshotReq:
		return "SNAPSHOT_REQ"
	case *protocol.ResetIID:
		return "RESET_IID"
	default:
		return "UNKNOWN"
	}
}
