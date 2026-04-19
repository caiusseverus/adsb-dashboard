// radar-core — operational radar estimator for the ADS-B dashboard.
//
// Stage 1: ingest RADAR_EVENT messages, assemble bursts, emit BURST_FIRED.
// Stage 2: maintain per-IID burst records, run rotation model analysis, emit IID_STATE.
// Runs as an independently supervised process (systemd unit) on the Pi.
//
// Usage: radar-core [--socket /path/to/radar-core.sock]
package main

import (
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/caiusseverus/adsb-dashboard/radar-core/burst"
	rcconfig "github.com/caiusseverus/adsb-dashboard/radar-core/config"
	"github.com/caiusseverus/adsb-dashboard/radar-core/iid"
	"github.com/caiusseverus/adsb-dashboard/radar-core/ingest"
	"github.com/caiusseverus/adsb-dashboard/radar-core/output"
	"github.com/caiusseverus/adsb-dashboard/radar-core/protocol"
)

const (
	rotationAnalysisInterval = 5 * time.Second
	healthInterval           = 10 * time.Second
)

func main() {
	socketPath := flag.String("socket", rcconfig.DefaultSocketPath, "Unix socket path")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	engine := newEngine()

	stop := make(chan struct{})
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigs
		slog.Info("radar-core: shutting down", "signal", sig)
		close(stop)
	}()

	go engine.runRotationTicker(stop)
	go engine.runHealthTicker(stop)

	os.Remove(*socketPath)

	listener := ingest.NewListener(*socketPath, engine.handlers(), nil)
	if err := listener.Run(stop); err != nil {
		slog.Error("radar-core: listener failed", "err", err)
		os.Exit(1)
	}
	slog.Info("radar-core: stopped")
}

// engine wires the ingest, burst, output, and IID state stages.
type engine struct {
	writer   *output.Writer
	builders map[uint8]*burst.Builder  // per IID; only accessed from the ingest goroutine
	states   map[uint8]*iid.IIDState   // per IID; AddBurst called from ingest, analysis from ticker
	revisions map[uint8]uint32         // monotonic IID_STATE revision counter
	start    time.Time

	eventsIn    atomic.Uint64
	burstsFired atomic.Uint64
}

func newEngine() *engine {
	return &engine{
		writer:    output.NewWriter(),
		builders:  make(map[uint8]*burst.Builder),
		states:    make(map[uint8]*iid.IIDState),
		revisions: make(map[uint8]uint32),
		start:     time.Now(),
	}
}

func (e *engine) handlers() ingest.Handlers {
	return ingest.Handlers{
		OnConnect:        func(conn net.Conn) { e.writer.SetConn(conn) },
		OnDisconnect:     func() { e.writer.SetConn(nil) },
		OnRadarEvent:     e.onRadarEvent,
		OnPositionUpdate: e.onPositionUpdate,
		OnConfigUpdate:   e.onConfigUpdate,
		OnSnapshotReq:    e.onSnapshotReq,
		OnResetIID:       e.onResetIID,
	}
}

// onRadarEvent is called from the ingest goroutine — no lock needed for builders/states maps.
func (e *engine) onRadarEvent(msg *protocol.RadarEvent) {
	e.eventsIn.Add(1)

	b, ok := e.builders[msg.IID]
	if !ok {
		b = burst.NewBuilder(msg.IID)
		e.builders[msg.IID] = b
	}
	if _, ok := e.states[msg.IID]; !ok {
		e.states[msg.IID] = iid.NewIIDState(msg.IID)
	}

	var sigPtr *float64
	if msg.SignalDBFS != nil {
		v := float64(*msg.SignalDBFS)
		sigPtr = &v
	}

	fired := b.OnEvent(msg.ArrivalUS, msg.ICAO, sigPtr, rcconfig.Get().BurstGapUS)
	for i := range fired {
		f := &fired[i]
		e.emitBurstFired(f)
		// Record burst for rotation analysis.
		e.states[msg.IID].AddBurst(f.ICAO, f.CentroidUS, f.NReplies)
	}
}

func (e *engine) emitBurstFired(f *burst.FiredBurst) {
	e.burstsFired.Add(1)

	var sig *float32
	if f.SignalDBFS != nil {
		v := float32(*f.SignalDBFS)
		sig = &v
	}

	nReplies := f.NReplies
	if nReplies > 255 {
		nReplies = 255
	}

	e.writer.Send(&protocol.BurstFired{
		MsgType:    protocol.MsgBurstFired,
		IID:        f.IID,
		ICAO:       f.ICAO,
		CentroidUS: f.CentroidUS,
		NReplies:   uint8(nReplies),
		SignalDBFS: sig,
	})
}

func (e *engine) onPositionUpdate(_ *protocol.PositionUpdate) {
	// Stage 3: position cache will be populated here.
}

func (e *engine) onConfigUpdate(msg *protocol.ConfigUpdate) {
	rcconfig.Apply(msg.Key, msg.Value)
	slog.Info("radar-core: config updated", "key", msg.Key)
}

func (e *engine) onSnapshotReq(msg *protocol.SnapshotReq) {
	e.writer.Send(&protocol.SnapshotResp{
		MsgType: protocol.MsgSnapshotResp,
		ReqID:   msg.ReqID,
		Payload: map[string]interface{}{
			"uptime_s":     time.Since(e.start).Seconds(),
			"events_in":    e.eventsIn.Load(),
			"bursts_fired": e.burstsFired.Load(),
			"active_iids":  len(e.builders),
		},
	})
}

func (e *engine) onResetIID(msg *protocol.ResetIID) {
	if b, ok := e.builders[msg.IID]; ok {
		b.Reset()
	}
	if s, ok := e.states[msg.IID]; ok {
		s.Reset()
	}
	slog.Info("radar-core: IID reset", "iid", msg.IID)
}

// runRotationTicker runs every 5s and analyses dirty IID states.
// Runs in its own goroutine; IIDState.TakeIfDirty is thread-safe.
func (e *engine) runRotationTicker(stop <-chan struct{}) {
	ticker := time.NewTicker(rotationAnalysisInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			e.analyseAllIIDs()
		}
	}
}

func (e *engine) analyseAllIIDs() {
	for iidNum, s := range e.states {
		records := s.TakeIfDirty()
		if records == nil {
			continue
		}
		model := iid.AnalyseBurstRecords(records)
		s.ApplyRotation(model)

		e.revisions[iidNum]++
		rev := e.revisions[iidNum]

		status, periodS, rpm, _ := s.Snapshot()

		var rpmMsg *float32
		if rpm != nil {
			v := float32(*rpm)
			rpmMsg = &v
		}

		msg := &protocol.IIDState{
			MsgType:       protocol.MsgIIDState,
			IID:           iidNum,
			PeriodS:       periodS,
			RPM:           rpmMsg,
			Status:        status,
			NBurstRecords: uint16(len(records)),
			LastUpdated:   float64(time.Now().UnixMicro()) / 1e6,
			Revision:      rev,
		}
		e.writer.SendIIDState(msg)

		slog.Debug("radar-core: IID_STATE emitted",
			"iid", iidNum,
			"status", status,
			"period_s", periodS,
			"n_records", len(records),
			"revision", rev,
		)
	}
}

// runHealthTicker emits a Health message every 10s.
func (e *engine) runHealthTicker(stop <-chan struct{}) {
	ticker := time.NewTicker(healthInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			e.writer.SendHealth(&protocol.Health{
				MsgType:     protocol.MsgHealth,
				UptimeS:     time.Since(e.start).Seconds(),
				EventsIn:    e.eventsIn.Load(),
				BurstsFired: e.burstsFired.Load(),
				ActiveIIDs:  uint8(len(e.builders)),
			})
		}
	}
}
