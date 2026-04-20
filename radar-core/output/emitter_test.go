package output_test

import (
	"bytes"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caiusseverus/adsb-dashboard/radar-core/output"
	"github.com/caiusseverus/adsb-dashboard/radar-core/protocol"
)

type spyConn struct {
	buf bytes.Buffer
	mu  sync.Mutex

	activeWrites atomic.Int32
	maxWrites    atomic.Int32
}

func (c *spyConn) Write(p []byte) (int, error) {
	active := c.activeWrites.Add(1)
	for {
		prev := c.maxWrites.Load()
		if active <= prev || c.maxWrites.CompareAndSwap(prev, active) {
			break
		}
	}
	defer c.activeWrites.Add(-1)

	time.Sleep(250 * time.Microsecond)
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

func (c *spyConn) Read(_ []byte) (int, error) { return 0, io.EOF }
func (c *spyConn) Close() error               { return nil }
func (c *spyConn) LocalAddr() net.Addr        { return nil }
func (c *spyConn) RemoteAddr() net.Addr       { return nil }
func (c *spyConn) SetDeadline(_ time.Time) error {
	return nil
}
func (c *spyConn) SetReadDeadline(_ time.Time) error {
	return nil
}
func (c *spyConn) SetWriteDeadline(_ time.Time) error {
	return nil
}

func (c *spyConn) Bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]byte, c.buf.Len())
	copy(out, c.buf.Bytes())
	return out
}

func TestWriterSendSerializesFrameWrites(t *testing.T) {
	w := output.NewWriter()
	conn := &spyConn{}
	w.SetConn(conn)

	const goroutines = 16
	const sendsPerG = 40
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < sendsPerG; i++ {
				w.SendHealth(&protocol.Health{
					MsgType:       protocol.MsgHealth,
					UptimeS:       float64(g*sendsPerG + i),
					EventsIn:      uint64(g),
					BurstsFired:   uint64(i),
					FramesEmitted: uint64(i),
					QueueDepth:    1,
					DropCount:     0,
					ActiveIIDs:    2,
				})
			}
		}()
	}
	wg.Wait()

	if got := conn.maxWrites.Load(); got != 1 {
		t.Fatalf("concurrent conn.Write calls detected; max active writes=%d, want 1", got)
	}

	data := conn.Bytes()
	reader := bytes.NewReader(data)
	total := 0
	for reader.Len() > 0 {
		payload, err := protocol.ReadFrame(reader)
		if err != nil {
			t.Fatalf("ReadFrame failed after %d frames: %v", total, err)
		}
		decoded, err := protocol.Decode(payload)
		if err != nil {
			t.Fatalf("Decode failed for frame %d: %v", total, err)
		}
		if _, ok := decoded.(*protocol.Health); !ok {
			t.Fatalf("decoded message type %T, want *protocol.Health", decoded)
		}
		total++
	}

	want := goroutines * sendsPerG
	if total != want {
		t.Fatalf("decoded %d frames, want %d", total, want)
	}
}
