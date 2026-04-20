// Package protocol implements length-prefixed framing over net.Conn.
//
// Wire format: 4-byte big-endian uint32 length, followed by that many bytes of
// msgpack-encoded payload. Maximum frame size is 1 MiB; anything larger is
// rejected with ErrFrameTooLarge to prevent runaway allocation.
package protocol

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
)

const maxFrameBytes = 1 << 20 // 1 MiB hard cap

var ErrFrameTooLarge = errors.New("protocol: frame exceeds maximum size")

// ReadFrame reads one length-prefixed frame from r.
// The returned slice is newly allocated each call.
func ReadFrame(r io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n > maxFrameBytes {
		return nil, ErrFrameTooLarge
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

// WriteFrame writes one length-prefixed frame to w.
func WriteFrame(w io.Writer, payload []byte) error {
	if len(payload) > maxFrameBytes {
		return ErrFrameTooLarge
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	if err := writeAll(w, lenBuf[:]); err != nil {
		return err
	}
	return writeAll(w, payload)
}

// Framer wraps a net.Conn with ReadFrame / WriteFrame helpers.
type Framer struct {
	conn net.Conn
}

func NewFramer(conn net.Conn) *Framer {
	return &Framer{conn: conn}
}

func (f *Framer) Read() ([]byte, error)      { return ReadFrame(f.conn) }
func (f *Framer) Write(payload []byte) error { return WriteFrame(f.conn, payload) }
func (f *Framer) Close() error               { return f.conn.Close() }

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
