// Package ingest listens on a Unix domain socket, accepts one client connection,
// and dispatches decoded messages to registered handlers.
//
// Only one client is expected (the Python app). If the client disconnects,
// the listener waits for a new connection without restarting radar-core.
package ingest

import (
	"log/slog"
	"net"

	"github.com/caiusseverus/adsb-dashboard/radar-core/protocol"
)

// Handlers holds callbacks for each inbound message type.
// Unset handlers are silently ignored.
type Handlers struct {
	OnConnect        func(conn net.Conn) // called when a client connects
	OnDisconnect     func()              // called when the client disconnects
	OnRadarEvent     func(msg *protocol.RadarEvent)
	OnPositionUpdate func(msg *protocol.PositionUpdate)
	OnConfigUpdate   func(msg *protocol.ConfigUpdate)
	OnSnapshotReq    func(msg *protocol.SnapshotReq)
	OnResetIID       func(msg *protocol.ResetIID)
}

// Listener accepts connections on a Unix socket and dispatches messages.
type Listener struct {
	socketPath string
	handlers   Handlers
	outbound   chan<- []byte // serialised outbound frames → output.Writer
}

func NewListener(socketPath string, h Handlers, outbound chan<- []byte) *Listener {
	return &Listener{socketPath: socketPath, handlers: h, outbound: outbound}
}

// Run binds the socket and serves clients sequentially.
// It blocks until ctx is cancelled (via the stop channel).
func (l *Listener) Run(stop <-chan struct{}) error {
	ln, err := net.Listen("unix", l.socketPath)
	if err != nil {
		return err
	}
	defer ln.Close()
	slog.Info("radar-core: listening", "socket", l.socketPath)

	go func() {
		<-stop
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-stop:
				return nil
			default:
				slog.Warn("radar-core: accept error", "err", err)
				continue
			}
		}
		slog.Info("radar-core: client connected", "remote", conn.RemoteAddr())
		if l.handlers.OnConnect != nil {
			l.handlers.OnConnect(conn)
		}
		l.serveConn(conn, stop)
		if l.handlers.OnDisconnect != nil {
			l.handlers.OnDisconnect()
		}
		slog.Info("radar-core: client disconnected")
	}
}

func (l *Listener) serveConn(conn net.Conn, stop <-chan struct{}) {
	defer conn.Close()

	// Register this conn as the outbound writer target.
	// The output goroutine drains the outbound channel and writes to conn.
	// We signal the output goroutine by sending the conn, then reading
	// frames in a loop until disconnect or stop.
	framer := protocol.NewFramer(conn)

	// Drain inbound frames in this goroutine (the caller's goroutine).
	for {
		select {
		case <-stop:
			return
		default:
		}

		payload, err := framer.Read()
		if err != nil {
			return // EOF or conn reset — caller will Accept a new client
		}
		msg, err := protocol.Decode(payload)
		if err != nil {
			slog.Warn("radar-core: decode error", "err", err)
			continue
		}
		l.dispatch(msg)
	}
}

func (l *Listener) dispatch(msg interface{}) {
	switch m := msg.(type) {
	case *protocol.RadarEvent:
		if l.handlers.OnRadarEvent != nil {
			l.handlers.OnRadarEvent(m)
		}
	case *protocol.PositionUpdate:
		if l.handlers.OnPositionUpdate != nil {
			l.handlers.OnPositionUpdate(m)
		}
	case *protocol.ConfigUpdate:
		if l.handlers.OnConfigUpdate != nil {
			l.handlers.OnConfigUpdate(m)
		}
	case *protocol.SnapshotReq:
		if l.handlers.OnSnapshotReq != nil {
			l.handlers.OnSnapshotReq(m)
		}
	case *protocol.ResetIID:
		if l.handlers.OnResetIID != nil {
			l.handlers.OnResetIID(m)
		}
	}
}
