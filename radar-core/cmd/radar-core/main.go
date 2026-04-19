// radar-core — operational radar estimator for the ADS-B dashboard.
//
// Stage 1: ingest RADAR_EVENT messages, assemble bursts, emit BURST_FIRED.
// Stage 2: maintain per-IID burst records, run rotation model analysis, emit IID_STATE.
// Stage 3: position cache, reference aircraft selection, sync epoch tracking.
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
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/caiusseverus/adsb-dashboard/radar-core/burst"
	rcconfig "github.com/caiusseverus/adsb-dashboard/radar-core/config"
	"github.com/caiusseverus/adsb-dashboard/radar-core/frame"
	"github.com/caiusseverus/adsb-dashboard/radar-core/iid"
	"github.com/caiusseverus/adsb-dashboard/radar-core/ingest"
	"github.com/caiusseverus/adsb-dashboard/radar-core/output"
	"github.com/caiusseverus/adsb-dashboard/radar-core/protocol"
)

const (
	rotationAnalysisInterval = 5 * time.Second
	healthInterval           = 10 * time.Second
	positionPruneInterval    = 60 * time.Second
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
	go engine.runPositionPruneTicker(stop)

	os.Remove(*socketPath)

	listener := ingest.NewListener(*socketPath, engine.handlers(), nil)
	if err := listener.Run(stop); err != nil {
		slog.Error("radar-core: listener failed", "err", err)
		os.Exit(1)
	}
	slog.Info("radar-core: stopped")
}

// engine wires the ingest, burst, output, IID state, position cache, and
// frame accumulator stages.
type engine struct {
	writer       *output.Writer
	builders     map[uint8]*burst.Builder      // per IID; only accessed from the ingest goroutine
	states       map[uint8]*iid.IIDState       // per IID; thread-safe via IIDState.mu
	accumulators map[uint8]*frame.Accumulator  // per IID; only accessed from the ingest goroutine
	revisions    map[uint8]uint32              // monotonic IID_STATE revision counter
	positions    *iid.PositionCache            // global ADS-B position cache (thread-safe)
	start        time.Time

	eventsIn    atomic.Uint64
	burstsFired atomic.Uint64
	framesEmitted atomic.Uint64
}

func newEngine() *engine {
	return &engine{
		writer:       output.NewWriter(),
		builders:     make(map[uint8]*burst.Builder),
		states:       make(map[uint8]*iid.IIDState),
		accumulators: make(map[uint8]*frame.Accumulator),
		revisions:    make(map[uint8]uint32),
		positions:    iid.NewPositionCache(),
		start:        time.Now(),
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
	s, ok := e.states[msg.IID]
	if !ok {
		s = iid.NewIIDState(msg.IID)
		e.states[msg.IID] = s
	}
	acc, ok := e.accumulators[msg.IID]
	if !ok {
		acc = frame.New(msg.IID, e.writer, e.positions)
		acc.OnFrameEmitted = func() { e.framesEmitted.Add(1) }
		e.accumulators[msg.IID] = acc
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
		s.AddBurst(f.ICAO, f.CentroidUS, f.NReplies)

		// Stage 3: advance sync epoch when reference ICAO fires.
		e.maybeUpdateSync(s, f)

		// Stage 4: feed the frame accumulator.
		acc.OnBurst(f.ICAO, f.CentroidUS, f.NReplies, f.SignalDBFS, s)
	}
}

// maybeUpdateSync advances the sync epoch when the reference aircraft fires.
func (e *engine) maybeUpdateSync(s *iid.IIDState, f *burst.FiredBurst) {
	syncQ, refICAO := s.SyncSnapshot()
	_ = syncQ
	if refICAO == nil || *refICAO != f.ICAO {
		return
	}

	// Look up the reference aircraft's ADS-B position for age estimate.
	refPosAgeS := 999.0
	pos := e.positions.Get(f.ICAO)
	if pos != nil {
		refPosAgeS = time.Since(pos.TS).Seconds()
	}

	// nAircraft estimate from builder's active ICAO count (includes the reference).
	nAircraft := 1
	if b, ok := e.builders[s.IID]; ok {
		nAircraft = b.ActiveICAOs()
	}

	// phaseOffsetDeg = 0 until radar position is known (Stage 4+).
	s.UpdateSyncEpoch(f.CentroidUS, 0.0, nAircraft, refPosAgeS)
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

func (e *engine) onPositionUpdate(msg *protocol.PositionUpdate) {
	e.positions.Update(msg.ICAO, msg.Lat, msg.Lon, msg.AltFt, msg.TS)
}

func (e *engine) onConfigUpdate(msg *protocol.ConfigUpdate) {
	rcconfig.Apply(msg.Key, msg.Value)
	slog.Info("radar-core: config updated", "key", msg.Key)
}

func (e *engine) onSnapshotReq(msg *protocol.SnapshotReq) {
	e.writer.Send(&protocol.SnapshotResp{
		MsgType: protocol.MsgSnapshotResp,
		ReqID:   msg.ReqID,
		Payload: e.buildSnapshotPayload(msg.Scope),
	})
}

func (e *engine) buildSnapshotPayload(scope string) map[string]interface{} {
	payload := map[string]interface{}{
		"uptime_s":       time.Since(e.start).Seconds(),
		"events_in":      e.eventsIn.Load(),
		"bursts_fired":   e.burstsFired.Load(),
		"frames_emitted": e.framesEmitted.Load(),
		"active_iids":    len(e.builders),
	}
	if scope == "health" {
		return payload
	}

	var targetIID *uint8
	scope = strings.TrimSpace(scope)
	if strings.HasPrefix(scope, "iid:") {
		if parsed, err := strconv.Atoi(strings.TrimPrefix(scope, "iid:")); err == nil && parsed >= 0 && parsed <= 255 {
			v := uint8(parsed)
			targetIID = &v
		}
	}

	iidsPayload := map[string]interface{}{}
	for iidNum, s := range e.states {
		if targetIID != nil && iidNum != *targetIID {
			continue
		}
		key := strconv.Itoa(int(iidNum))
		snap := s.DebugStateSnapshot()
		iidPayload := map[string]interface{}{
			"status":             snap.Status,
			"has_period":         snap.HasPeriod,
			"period_s":           nil,
			"has_reference_icao": snap.HasRefICAO,
			"reference_icao":     nil,
			"sync_state_present": snap.SyncPresent,
			"sync_quality":       snap.SyncQuality,
			"sync_state_usable":  snap.SyncUsable,
			"sync_holdover":      snap.SyncHoldover,
			"sync_n_frames":      snap.SyncNSyncFrames,
		}
		if snap.HasPeriod {
			iidPayload["period_s"] = snap.PeriodS
		}
		if snap.HasRefICAO {
			iidPayload["reference_icao"] = snap.RefICAO
		}

		if acc, ok := e.accumulators[iidNum]; ok {
			accDiag := acc.Diagnostics()
			iidPayload["frame_accumulator"] = map[string]interface{}{
				"completed_frames":      accDiag.FrameIndex,
				"open_frame":            accDiag.OpenFrame,
				"open_frame_ref_icao":   accDiag.OpenFrameRefICAO,
				"open_frame_n_aircraft": accDiag.OpenFrameNAircraft,
				"open_frame_n_obs":      accDiag.OpenFrameNObs,
				"last_frame_start_us":   accDiag.LastFrameStartUS,
				"last_gate_reason":      accDiag.LastGateReason,
				"last_gate_icao":        accDiag.LastGateICAO,
				"gate_counts":           accDiag.GateCounts,
			}
		}
		iidsPayload[key] = iidPayload
	}
	payload["iids"] = iidsPayload
	return payload
}

func (e *engine) onResetIID(msg *protocol.ResetIID) {
	if b, ok := e.builders[msg.IID]; ok {
		b.Reset()
	}
	if s, ok := e.states[msg.IID]; ok {
		s.Reset()
	}
	if a, ok := e.accumulators[msg.IID]; ok {
		a.Reset()
	}
	slog.Info("radar-core: IID reset", "iid", msg.IID)
}

// runRotationTicker runs every 5s and analyses dirty IID states.
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

		// Rotation model analysis.
		model := iid.AnalyseBurstRecords(records)
		s.ApplyRotation(model)

		// Reference aircraft selection — use the most recent centroid as nowUS.
		nowUS := 0.0
		if len(records) > 0 {
			nowUS = records[len(records)-1].CentroidUS
		}
		s.RefreshReference(records, nowUS)

		e.revisions[iidNum]++
		rev := e.revisions[iidNum]

		status, periodS, rpm, _ := s.Snapshot()
		syncQuality, refICAO := s.SyncSnapshot()

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
			RefICAO:       refICAO,
			SyncQuality:   syncQuality,
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
				MsgType:       protocol.MsgHealth,
				UptimeS:       time.Since(e.start).Seconds(),
				EventsIn:      e.eventsIn.Load(),
				BurstsFired:   e.burstsFired.Load(),
				FramesEmitted: e.framesEmitted.Load(),
				ActiveIIDs:    uint8(len(e.builders)),
			})
		}
	}
}

// runPositionPruneTicker evicts stale ADS-B positions every minute.
func (e *engine) runPositionPruneTicker(stop <-chan struct{}) {
	ticker := time.NewTicker(positionPruneInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			n := e.positions.Prune()
			if n > 0 {
				slog.Debug("radar-core: pruned stale positions", "count", n)
			}
		}
	}
}
