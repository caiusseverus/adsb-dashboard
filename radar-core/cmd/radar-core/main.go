// radar-core — operational radar estimator for the ADS-B dashboard.
//
// Stage 1: ingest RADAR_EVENT messages, assemble bursts, emit BURST_FIRED.
// Stage 2: maintain per-IID burst records, run rotation model analysis, emit IID_STATE.
// Stage 3: position cache, reference aircraft selection, sync epoch tracking.
// Runs either as a backend-managed subprocess or as an externally managed process.
//
// Usage: radar-core [--socket /path/to/radar-core.sock]
package main

import (
	"flag"
	"log/slog"
	"math"
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
	rcexport "github.com/caiusseverus/adsb-dashboard/radar-core/export"
	"github.com/caiusseverus/adsb-dashboard/radar-core/fm"
	"github.com/caiusseverus/adsb-dashboard/radar-core/frame"
	"github.com/caiusseverus/adsb-dashboard/radar-core/iid"
	"github.com/caiusseverus/adsb-dashboard/radar-core/ingest"
	"github.com/caiusseverus/adsb-dashboard/radar-core/output"
	rcprofile "github.com/caiusseverus/adsb-dashboard/radar-core/profile"
	"github.com/caiusseverus/adsb-dashboard/radar-core/protocol"
)

type syncPopulationDiagnostics struct {
	rawCandidateAircraftCount int
	positionedAircraftCount   int
	dominantAircraftCount     int
	frameObservationCount     int
	excludedMissingPosition   int
	excludedStalePosition     int
	excludedNotDominant       int
	excludedOutsideWindow     int
}

const (
	rotationAnalysisInterval = 5 * time.Second
	healthInterval           = 10 * time.Second
	positionPruneInterval    = 60 * time.Second
)

func main() {
	socketPath := flag.String("socket", rcconfig.DefaultSocketPath, "Unix socket path")
	flag.Parse()

	logLevel := slog.LevelError
	if envDebugEnabled("RADAR_CORE_DEBUG") {
		logLevel = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
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
	go engine.runFMWorker(stop)

	os.Remove(*socketPath)

	listener := ingest.NewListener(*socketPath, engine.handlers(), nil)
	if err := listener.Run(stop); err != nil {
		slog.Error("radar-core: listener failed", "err", err)
		os.Exit(1)
	}
	slog.Info("radar-core: stopped")
}

func envDebugEnabled(key string) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

// engine wires the ingest, burst, output, IID state, position cache, and
// frame accumulator stages.
type engine struct {
	writer       *output.Writer
	builders     map[uint8]*burst.Builder     // per IID; only accessed from the ingest goroutine
	states       map[uint8]*iid.IIDState      // per IID; thread-safe via IIDState.mu
	accumulators map[uint8]*frame.Accumulator // per IID; only accessed from the ingest goroutine
	fmState      *fm.State
	fmQueue      chan *protocol.FrameReady
	revisions    map[uint8]uint32   // monotonic IID_STATE revision counter
	positions    *iid.PositionCache // global ADS-B position cache (thread-safe)
	exports      *rcexport.Store
	profiler     *rcprofile.Collector
	start        time.Time

	eventsIn      atomic.Uint64
	burstsFired   atomic.Uint64
	framesEmitted atomic.Uint64
}

func newEngine() *engine {
	return &engine{
		writer:       output.NewWriter(),
		builders:     make(map[uint8]*burst.Builder),
		states:       make(map[uint8]*iid.IIDState),
		accumulators: make(map[uint8]*frame.Accumulator),
		fmState:      fm.NewState(rcconfig.Get().ReceiverLat, rcconfig.Get().ReceiverLon, rcconfig.Get().HasReceiver),
		fmQueue:      make(chan *protocol.FrameReady, 256),
		revisions:    make(map[uint8]uint32),
		positions:    iid.NewPositionCache(),
		exports:      rcexport.NewStore(4096, 8192, 512, 360.0),
		profiler:     rcprofile.NewCollector(),
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
		acc.ObserveStage = e.profiler.Observe
		acc.OnFrameEmitted = func(frameMsg *protocol.FrameReady) {
			e.framesEmitted.Add(1)
			e.recordSweepFrame(frameMsg)
			select {
			case e.fmQueue <- frameMsg:
			default:
				slog.Warn("radar-core: FM queue full; dropping frame", "iid", frameMsg.IID, "frame", frameMsg.FrameIndex)
			}
		}
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
		tBurst := time.Now()
		s.AddBurst(f.ICAO, f.CentroidUS, f.NReplies, b.ActiveICAOs())

		// Stage 3: advance sync epoch when reference ICAO fires.
		tSync := time.Now()
		e.maybeUpdateSync(s, f)
		e.profiler.Observe("sync_update", time.Since(tSync))
		e.maybeObserveRefinementResidual(s, f)

		// Stage 4: feed the frame accumulator.
		acc.OnBurst(f.ICAO, f.CentroidUS, f.NReplies, f.SignalDBFS, s)
		e.recordObservationExports(s, f)
		e.emitBurstFired(s, f)

		e.profiler.Observe("burst_processing", time.Since(tBurst))
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
	// Use GetRaw (no staleness filter) so we can distinguish "not in cache"
	// (sentinel -1.0) from a genuinely stale position (positive age > 8s).
	refPosAgeS := -1.0 // sentinel: not in Go position cache
	refPosMissingReason := "not_in_cache"
	posRaw, posExists := e.positions.GetRaw(f.ICAO)
	if posExists {
		rawAge := time.Since(posRaw.TS).Seconds()
		refPosAgeS = rawAge
		if rawAge > 8.0 {
			refPosMissingReason = "stale"
		} else {
			refPosMissingReason = ""
		}
	}

	popDiag := e.computeSyncPopulationDiagnostics(s, f.ICAO, f.CentroidUS)
	nAircraft := popDiag.positionedAircraftCount
	if nAircraft < 1 {
		nAircraft = 1
	}
	if s.Sync != nil {
		s.Sync.SetLastUpdateEpochInputDiagnostics(
			popDiag.rawCandidateAircraftCount,
			popDiag.positionedAircraftCount,
			popDiag.dominantAircraftCount,
			popDiag.frameObservationCount,
			f.ICAO,
			f.CentroidUS,
			refPosAgeS,
			popDiag.excludedMissingPosition,
			popDiag.excludedStalePosition,
			popDiag.excludedNotDominant,
			popDiag.excludedOutsideWindow,
		)
	}

	phaseOffsetDeg := 0.0
	cfg := rcconfig.Get()
	if posExists && posRaw != nil && cfg.HasReceiver {
		phaseOffsetDeg = bearingDeg(cfg.ReceiverLat, cfg.ReceiverLon, posRaw.Lat, posRaw.Lon)
	}
	if s.Sync != nil {
		s.Sync.LastUpdateEpochRefPosMissingReason = refPosMissingReason
	}
	s.UpdateSyncEpoch(f.CentroidUS, phaseOffsetDeg, nAircraft, refPosAgeS)
	snap := s.DebugStateSnapshot()
	e.emitIIDState(s.IID, s, uint16(minInt(snap.BurstRecordsTotal, 65535)))
}

func (e *engine) computeSyncPopulationDiagnostics(s *iid.IIDState, refICAO uint32, refEpochUS float64) syncPopulationDiagnostics {
	diag := syncPopulationDiagnostics{
		rawCandidateAircraftCount: 1,
		positionedAircraftCount:   1,
		dominantAircraftCount:     1,
	}
	periodS := 4.0
	if p := s.OperationalPeriodSnapshot(); p != nil && *p > 0 {
		periodS = *p
	}
	halfWindowUS := math.Max(periodS*1_000_000.0, 2_000_000.0)
	records := s.BurstRecordsWindow(refEpochUS, halfWindowUS)
	stateSnap := s.DebugStateSnapshot()
	seenRaw := map[uint32]struct{}{}
	seenPositioned := map[uint32]struct{}{}
	seenDominant := map[uint32]struct{}{}
	family := s.FamilySnapshot()
	for _, rec := range records {
		if _, ok := seenRaw[rec.ICAO]; ok {
			continue
		}
		seenRaw[rec.ICAO] = struct{}{}
		if family != nil && family.FoldedICAOs != nil {
			if _, ok := family.FoldedICAOs[rec.ICAO]; !ok {
				diag.excludedNotDominant++
				continue
			}
		}
		seenDominant[rec.ICAO] = struct{}{}
		pos := e.positions.Get(rec.ICAO)
		if pos == nil {
			diag.excludedMissingPosition++
			continue
		}
		ageS := time.Since(pos.TS).Seconds()
		if ageS > 8.0 {
			diag.excludedStalePosition++
			continue
		}
		seenPositioned[rec.ICAO] = struct{}{}
	}
	diag.rawCandidateAircraftCount = len(seenRaw)
	diag.positionedAircraftCount = len(seenPositioned)
	diag.dominantAircraftCount = len(seenDominant)
	if diag.rawCandidateAircraftCount == 0 {
		diag.rawCandidateAircraftCount = 1
	}
	if diag.positionedAircraftCount == 0 {
		diag.positionedAircraftCount = 1
	}
	if diag.dominantAircraftCount == 0 {
		diag.dominantAircraftCount = 1
	}
	if stateSnap.BurstRecordsICAOs > len(seenRaw) {
		diag.excludedOutsideWindow = stateSnap.BurstRecordsICAOs - len(seenRaw)
	}
	if acc, ok := e.accumulators[s.IID]; ok {
		accDiag := acc.Diagnostics()
		diag.frameObservationCount = accDiag.OpenFrameNObs
	}
	return diag
}

func (e *engine) maybeObserveRefinementResidual(s *iid.IIDState, f *burst.FiredBurst) {
	cfg := rcconfig.Get()
	if !cfg.HasReceiver {
		return
	}
	pos := e.positions.Get(f.ICAO)
	if pos == nil {
		return
	}
	observedBearingDeg := bearingDeg(cfg.ReceiverLat, cfg.ReceiverLon, pos.Lat, pos.Lon)
	refPosAgeS := time.Since(pos.TS).Seconds()
	nAircraft := 1
	if b, ok := e.builders[s.IID]; ok {
		nAircraft = b.ActiveICAOs()
	}
	dominantFamily := false
	if family := s.FamilySnapshot(); family != nil && family.FoldedICAOs != nil {
		_, dominantFamily = family.FoldedICAOs[f.ICAO]
	} else {
		// If family gating is unavailable, treat observations as eligible-soft
		// rather than deadlocking refinement to zero observations.
		dominantFamily = true
	}
	s.RecordBurstResidualObservation(f.CentroidUS, f.ICAO, observedBearingDeg, nAircraft, refPosAgeS, dominantFamily)
}

func bearingDeg(lat1, lon1, lat2, lon2 float64) float64 {
	phi1 := lat1 * math.Pi / 180.0
	phi2 := lat2 * math.Pi / 180.0
	dlam := (lon2 - lon1) * math.Pi / 180.0
	y := math.Sin(dlam) * math.Cos(phi2)
	x := math.Cos(phi1)*math.Sin(phi2) - math.Sin(phi1)*math.Cos(phi2)*math.Cos(dlam)
	b := math.Atan2(y, x) * 180.0 / math.Pi
	b = math.Mod(b+360.0, 360.0)
	if b < 0 {
		b += 360.0
	}
	return b
}

func (e *engine) emitBurstFired(s *iid.IIDState, f *burst.FiredBurst) {
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

	var latPtr, lonPtr *float64
	var posAgePtr *float32
	if pos := e.positions.Get(f.ICAO); pos != nil {
		lat, lon := pos.Lat, pos.Lon
		latPtr, lonPtr = &lat, &lon
		posAge := float32(time.Since(pos.TS).Seconds())
		posAgePtr = &posAge
	}
	dominantFamily := false
	if family := s.FamilySnapshot(); family != nil && family.FoldedICAOs != nil {
		_, dominantFamily = family.FoldedICAOs[f.ICAO]
	}
	simpleCentroid := f.SimpleCentroidUS
	centroidDelta := f.CentroidDeltaUS
	firstReply := f.FirstReplyUS
	lastReply := f.LastReplyUS
	spanUS := f.SpanUS
	var peakAmplitude *float32
	if f.PeakAmplitude != nil {
		v := float32(*f.PeakAmplitude)
		peakAmplitude = &v
	}

	e.writer.Send(&protocol.BurstFired{
		MsgType:            protocol.MsgBurstFired,
		IID:                f.IID,
		ICAO:               f.ICAO,
		CentroidUS:         f.CentroidUS,
		SimpleCentroidUS:   &simpleCentroid,
		WeightedCentroidUS: f.WeightedCentroidUS,
		CentroidDeltaUS:    &centroidDelta,
		FirstReplyUS:       &firstReply,
		StrongestReplyUS:   f.StrongestReplyUS,
		MidStrongWindowUS:  f.MidStrongWindowUS,
		LastReplyUS:        &lastReply,
		SpanUS:             &spanUS,
		PeakAmplitude:      peakAmplitude,
		NReplies:           uint8(nReplies),
		SignalDBFS:         sig,
		Lat:                latPtr,
		Lon:                lonPtr,
		PosAgeS:            posAgePtr,
		DominantFamily:     dominantFamily,
	})
}

func (e *engine) recordObservationExports(s *iid.IIDState, f *burst.FiredBurst) {
	var sig *float32
	if f.SignalDBFS != nil {
		v := float32(*f.SignalDBFS)
		sig = &v
	}
	var latPtr, lonPtr *float64
	var posAgePtr *float32
	assoc := float32(0.0)
	if pos := e.positions.Get(f.ICAO); pos != nil {
		lat, lon := pos.Lat, pos.Lon
		latPtr, lonPtr = &lat, &lon
		posAge := float32(time.Since(pos.TS).Seconds())
		posAgePtr = &posAge
		assoc = 1.0
	}
	dominantFamily := false
	if family := s.FamilySnapshot(); family != nil && family.FoldedICAOs != nil {
		_, dominantFamily = family.FoldedICAOs[f.ICAO]
	}
	wallTS := float64(time.Now().UnixNano()) / float64(time.Second)
	track := rcexport.TrackObservation{
		IID:                   f.IID,
		ICAO:                  f.ICAO,
		ArrivalUS:             f.CentroidUS,
		WallTS:                wallTS,
		SignalDBFS:            sig,
		TruthLat:              latPtr,
		TruthLon:              lonPtr,
		PositionAgeS:          posAgePtr,
		AssociationConfidence: assoc,
		DominantFamily:        dominantFamily,
	}
	tExport := time.Now()
	e.exports.RecordTrackObservation(track)
	e.exports.RecordEvidenceEvent(rcexport.EvidenceEvent{
		Kind:               "burst_fired",
		IID:                f.IID,
		ICAO:               f.ICAO,
		ArrivalUS:          f.CentroidUS,
		SimpleCentroidUS:   &f.SimpleCentroidUS,
		WeightedCentroidUS: f.WeightedCentroidUS,
		CentroidDeltaUS:    &f.CentroidDeltaUS,
		FirstReplyUS:       &f.FirstReplyUS,
		StrongestReplyUS:   f.StrongestReplyUS,
		MidStrongWindowUS:  f.MidStrongWindowUS,
		LastReplyUS:        &f.LastReplyUS,
		SpanUS:             &f.SpanUS,
		PeakAmplitude: func() *float32 {
			if f.PeakAmplitude == nil {
				return nil
			}
			v := float32(*f.PeakAmplitude)
			return &v
		}(),
		WallTS:                wallTS,
		NReplies:              uint8(minInt(f.NReplies, 255)),
		SignalDBFS:            sig,
		TruthLat:              latPtr,
		TruthLon:              lonPtr,
		PositionAgeS:          posAgePtr,
		DominantFamily:        dominantFamily,
		AssociationConfidence: assoc,
	})
	e.profiler.Observe("evidence_export", time.Since(tExport))
}

func (e *engine) recordSweepFrame(frameMsg *protocol.FrameReady) {
	obs := make([]rcexport.SweepFrameObservation, 0, len(frameMsg.Observations))
	for _, entry := range frameMsg.Observations {
		obs = append(obs, rcexport.SweepFrameObservation{
			ICAO:      entry.ICAO,
			Lat:       entry.Lat,
			Lon:       entry.Lon,
			ArrivalUS: entry.ArrivalUS,
			NReplies:  entry.NReplies,
			PosAgeS:   entry.PosAgeS,
		})
	}
	e.exports.RecordSweepFrame(rcexport.SweepFrame{
		IID:          frameMsg.IID,
		FrameIndex:   frameMsg.FrameIndex,
		PeriodS:      frameMsg.PeriodS,
		RefICAO:      frameMsg.RefICAO,
		RefLat:       frameMsg.RefLat,
		RefLon:       frameMsg.RefLon,
		RefArrivalUS: frameMsg.RefArrivalUS,
		RefPosAgeS:   frameMsg.RefPosAgeS,
		Quality:      frameMsg.Quality,
		ExportedAt:   float64(time.Now().UnixNano()) / float64(time.Second),
		Observations: obs,
	})
}

func (e *engine) onPositionUpdate(msg *protocol.PositionUpdate) {
	e.positions.Update(msg.ICAO, msg.Lat, msg.Lon, msg.AltFt, msg.TS)
}

func (e *engine) onConfigUpdate(msg *protocol.ConfigUpdate) {
	if strings.HasPrefix(msg.Key, "IID_BASE_PERIOD_S:") {
		rawIID := strings.TrimPrefix(msg.Key, "IID_BASE_PERIOD_S:")
		parsed, err := strconv.Atoi(rawIID)
		if err != nil || parsed < 0 || parsed > 255 {
			slog.Warn("radar-core: invalid IID base period key", "key", msg.Key)
			return
		}
		period, ok := numericConfigValue(msg.Value)
		if !ok {
			slog.Warn("radar-core: invalid IID base period value", "key", msg.Key)
			return
		}
		iidNum := uint8(parsed)
		s, ok := e.states[iidNum]
		if !ok {
			s = iid.NewIIDState(iidNum)
			e.states[iidNum] = s
		}
		s.SetBasePeriod(period)
		e.emitIIDState(iidNum, s, 0)
		slog.Info("radar-core: IID DF base period updated", "iid", iidNum, "base_period_s", period)
		return
	}
	rcconfig.Apply(msg.Key, msg.Value)
	cfg := rcconfig.Get()
	if cfg.HasReceiver {
		e.fmState.SetReceiver(cfg.ReceiverLat, cfg.ReceiverLon)
	}
	slog.Info("radar-core: config updated", "key", msg.Key)
}

func numericConfigValue(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint64:
		return float64(x), true
	default:
		return 0, false
	}
}

func (e *engine) onSnapshotReq(msg *protocol.SnapshotReq) {
	e.writer.Send(&protocol.SnapshotResp{
		MsgType: protocol.MsgSnapshotResp,
		ReqID:   msg.ReqID,
		Payload: e.buildSnapshotPayload(msg.Scope),
	})
}

func (e *engine) buildSnapshotPayload(scope string) map[string]interface{} {
	tSnapshot := time.Now()
	defer func() {
		e.profiler.Observe("snapshot_export", time.Since(tSnapshot))
	}()
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
	stableIIDs := map[string]rcexport.IIDSnapshot{}
	for iidNum, s := range e.states {
		if targetIID != nil && iidNum != *targetIID {
			continue
		}
		key := strconv.Itoa(int(iidNum))
		snap := s.DebugStateSnapshot()
		iidPayload := map[string]interface{}{
			"status":                                         snap.Status,
			"has_period":                                     snap.HasPeriod,
			"period_s":                                       nil,
			"has_reference_icao":                             snap.HasRefICAO,
			"reference_icao":                                 nil,
			"sync_state_present":                             snap.SyncPresent,
			"sync_quality":                                   snap.SyncQuality,
			"sync_state_usable":                              snap.SyncUsable,
			"sync_period_s":                                  nil,
			"sync_phase_epoch_us":                            nil,
			"sync_phase_offset_deg":                          nil,
			"sync_jitter_deg":                                nil,
			"sync_residual_ema_deg":                          nil,
			"sync_last_residual_deg":                         nil,
			"sync_holdover":                                  snap.SyncHoldover,
			"sync_n_frames":                                  snap.SyncNSyncFrames,
			"sync_n_rejected_frames":                         snap.SyncNRejectedFrames,
			"sync_last_updated":                              nil,
			"period_source":                                  snap.PeriodSource,
			"base_period_s":                                  nil,
			"period_delta_s":                                 snap.PeriodDeltaS,
			"effective_period_s":                             nil,
			"residual_slope_deg_per_s":                       snap.ResidualSlopeDegPerS,
			"slope_ema_deg_per_s":                            snap.ResidualSlopeEMADegPerS,
			"slope_std_deg_per_s":                            snap.ResidualSlopeStdDegPerS,
			"proposed_delta_s":                               snap.ProposedDeltaS,
			"applied_delta_s":                                snap.AppliedDeltaS,
			"last_slew_limited":                              snap.LastSlewLimited,
			"last_hard_bound":                                snap.LastHardBound,
			"hard_bound_reason":                              snap.HardBoundReason,
			"hard_bound_limit_s":                             snap.HardBoundLimitS,
			"hard_bound_limit_ppm":                           snap.HardBoundLimitPPM,
			"last_rejected_delta_s":                          snap.LastRejectedDeltaS,
			"last_rejected_delta_ppm":                        snap.LastRejectedDeltaPPM,
			"last_rejected_delta_reason":                     snap.LastRejectedDeltaReason,
			"last_rejected_delta_epoch_id":                   snap.LastRejectedDeltaEpochID,
			"requested_delta_s":                              snap.RequestedDeltaS,
			"requested_delta_ppm":                            snap.RequestedDeltaPPM,
			"current_delta_s":                                snap.CurrentDeltaS,
			"current_delta_ppm":                              snap.CurrentDeltaPPM,
			"delta_to_base_s":                                snap.DeltaToBaseS,
			"delta_to_base_ppm":                              snap.DeltaToBasePPM,
			"df_base_period_s":                               snap.DFBasePeriodS,
			"period_disagreement_s":                          snap.PeriodDisagreementS,
			"period_disagreement_ppm":                        snap.PeriodDisagreementPPM,
			"period_refinement_status":                       snap.PeriodRefinementStatus,
			"period_agrees_with_df":                          snap.PeriodAgreesWithDF,
			"period_reject_reason":                           snap.PeriodRejectReason,
			"holdover_reason":                                snap.HoldoverReason,
			"holdover_quality_gate_failed":                   snap.HoldoverQualityGateFailed,
			"holdover_missing_df_base_period":                snap.HoldoverMissingDFBasePeriod,
			"holdover_hard_residual_reject":                  snap.HoldoverHardResidualReject,
			"holdover_no_reference":                          snap.HoldoverNoReference,
			"holdover_stale_reference_position":              snap.HoldoverStaleReferencePosition,
			"holdover_period_disagreement":                   snap.HoldoverPeriodDisagreement,
			"holdover_insufficient_aircraft":                 snap.HoldoverInsufficientAircraft,
			"holdover_no_dominant_family":                    snap.HoldoverNoDominantFamily,
			"holdover_sync_state_missing":                    snap.HoldoverSyncStateMissing,
			"update_epoch_attempts":                          snap.UpdateEpochAttempts,
			"update_epoch_accepts":                           snap.UpdateEpochAccepts,
			"update_epoch_rejects":                           snap.UpdateEpochRejects,
			"last_update_epoch_reject_reason":                snap.LastUpdateEpochRejectReason,
			"last_update_epoch_n_aircraft":                   snap.LastUpdateEpochNAircraft,
			"last_update_epoch_ref_pos_age_s":                snap.LastUpdateEpochRefPosAgeS,
			"last_update_epoch_ref_icao":                     snap.LastUpdateEpochRefICAO,
			"last_update_epoch_residual_deg":                 snap.LastUpdateEpochResidualDeg,
			"last_update_epoch_predicted_deg":                snap.LastUpdateEpochPredictedDeg,
			"last_update_epoch_observed_deg":                 snap.LastUpdateEpochObservedDeg,
			"consecutive_hard_residual_rejects":              snap.ConsecutiveHardResidualRejects,
			"consecutive_hard_bound_rejects":                 snap.ConsecutiveHardBoundRejects,
			"last_accepted_epoch_age_s":                      snap.LastAcceptedEpochAgeS,
			"sync_epoch_age_s":                               snap.SyncEpochAgeS,
			"current_phase_epoch_us":                         snap.CurrentPhaseEpochUS,
			"candidate_epoch_us":                             snap.CandidateEpochUS,
			"sync_reacquired_provisional":                    snap.ReacquiredProvisional,
			"last_update_epoch_raw_candidate_aircraft_count": snap.LastUpdateEpochRawCandidateAircraftCount,
			"last_update_epoch_positioned_aircraft_count":    snap.LastUpdateEpochPositionedAircraftCount,
			"last_update_epoch_dominant_aircraft_count":      snap.LastUpdateEpochDominantAircraftCount,
			"last_update_epoch_frame_observation_count":      snap.LastUpdateEpochFrameObservationCount,
			"last_update_epoch_input_reference_icao":         snap.LastUpdateEpochInputReferenceICAO,
			"last_update_epoch_input_reference_burst_us":     snap.LastUpdateEpochInputReferenceBurstUS,
			"last_update_epoch_input_reference_pos_age_s":    snap.LastUpdateEpochInputReferencePosAgeS,
			"last_update_epoch_excluded_missing_position":    snap.LastUpdateEpochExcludedMissingPosition,
			"last_update_epoch_excluded_stale_position":      snap.LastUpdateEpochExcludedStalePosition,
			"last_update_epoch_excluded_not_dominant":        snap.LastUpdateEpochExcludedNotDominant,
			"last_update_epoch_excluded_outside_window":      snap.LastUpdateEpochExcludedOutsideWindow,
			"update_epoch_reject_quality_gate":               snap.UpdateEpochRejectQualityGate,
			"update_epoch_reject_missing_base":               snap.UpdateEpochRejectMissingBase,
			"update_epoch_reject_hard_residual":              snap.UpdateEpochRejectHardResidual,
			"update_epoch_reject_no_reference":               snap.UpdateEpochRejectNoReference,
			"update_epoch_reject_stale_ref_pos":              snap.UpdateEpochRejectStaleRefPos,
			"update_epoch_reject_insufficient_aircraft":      snap.UpdateEpochRejectInsufficientAC,
			"update_epoch_last_strict_gate_pass":             snap.UpdateEpochLastStrictGatePass,
			"period_refinement_plotted_count":                snap.RefinementPlottedCount,
			"period_refinement_eligible_count":               snap.RefinementEligibleCount,
			"period_refinement_rejected_count":               snap.RefinementRejectedCount,
			"sync_reference_update_count":                    snap.RefinementReferenceUpdates,
			"refinement_plotted_count":                       snap.RefinementPlottedCount,
			"refinement_eligible_count":                      snap.RefinementEligibleCount,
			"refinement_rejected_count":                      snap.RefinementRejectedCount,
			"refinement_reference_updates":                   snap.RefinementReferenceUpdates,
			"refinement_last_reject_reason":                  snap.RefinementLastRejectReason,
			"refinement_last_observation_age_s":              snap.RefinementLastObservationAgeS,
			"refinement_history_len":                         snap.RefinementHistoryLen,
			"fit_observation_count":                          snap.FitObservationCount,
			"fit_span_s":                                     snap.FitSpanS,
			"fit_icao_count":                                 snap.FitICAOCount,
			"fit_observations_per_icao_min":                  snap.FitObservationsPerICAOMin,
			"fit_observations_per_icao_median":               snap.FitObservationsPerICAOMedian,
			"fit_observations_per_icao_max":                  snap.FitObservationsPerICAOMax,
			"fit_retention_window_s":                         snap.FitRetentionWindowS,
			"fit_global_cap_hit":                             snap.FitGlobalCapHit,
			"fit_last_eviction_reason":                       snap.FitLastEvictionReason,
			"fit_inlier_ratio":                               snap.FitInlierRatio,
			"suspicious_icao_count":                          snap.SuspiciousICAOCount,
			"suspicious_icao_last_reason":                    snap.SuspiciousICAOLastReason,
			"fit_epoch_id":                                   snap.FitEpochID,
			"fit_epoch_started_ts":                           snap.FitEpochStartedUnix,
			"fit_epoch_reset_reason":                         snap.FitEpochResetReason,
			"fit_epoch_observation_count":                    snap.FitEpochObservationCount,
			"fit_epoch_span_s":                               snap.FitEpochSpanS,
			"fit_dropped_on_epoch_reset":                     snap.FitDroppedOnEpochReset,
			"fit_segment_count":                              snap.FitSegmentCount,
			"fit_epoch_reset_count_by_reason":               snap.FitEpochResetCountByReason,
			"last_fit_epoch_reset_age_s":                    snap.LastFitEpochResetAgeS,
			"fit_observations_added_since_reset":            snap.FitObservationsAddedSinceReset,
			"fit_observations_rejected_since_reset":         snap.FitObservationsRejectedSinceReset,
			"last_fit_observation_reject_reason":            snap.LastFitObservationRejectReason,
			"current_reference_icao":                        snap.CurrentReferenceICAO,
			"previous_reference_icao":                       snap.PreviousReferenceICAO,
			"reference_change_count":                        snap.ReferenceChangeCount,
			"reference_churn_rate":                          snap.ReferenceChurnRate,
			"observation_drop_counts_by_reason":              snap.ObservationDropCountsByReason,
			"holdover_missing_reference_position":            snap.HoldoverMissingReferencePosition,
			"update_epoch_reject_missing_ref_pos":            snap.UpdateEpochRejectMissingRefPos,
			"last_ref_sel_has_position":                      snap.LastRefSelHasPosition,
			"last_ref_sel_position_age_s":                    snap.LastRefSelPositionAgeS,
			"last_ref_sel_missing_reason":                    snap.LastRefSelMissingReason,
			"retained_state": map[string]interface{}{
				"active_aircraft_estimate":           snap.ActiveAircraftEstimate,
				"burst_records_total":                snap.BurstRecordsTotal,
				"burst_records_dynamic_cap":          snap.BurstRecordsDynamicCap,
				"burst_records_cap_hit":              snap.BurstRecordsCapHit,
				"burst_records_cap_hits_total":       snap.BurstRecordsCapHitsTotal,
				"burst_records_retained_span_s":      snap.BurstRecordsRetainedSpanS,
				"burst_records_icaos":                snap.BurstRecordsICAOs,
				"burst_records_per_icao_min":         snap.BurstRecordsPerICAOMin,
				"burst_records_per_icao_median":      snap.BurstRecordsPerICAOMedian,
				"burst_records_per_icao_max":         snap.BurstRecordsPerICAOMax,
				"reference_eligible_aircraft_count":  snap.ReferenceEligibleAircraft,
				"dominant_family_aircraft_count":     snap.DominantFamilyAircraft,
				"reference_selection_sparse_history": snap.ReferenceSelectionSparseHistory,
			},
		}
		if snap.HasPeriod {
			iidPayload["period_s"] = snap.PeriodS
		}
		if snap.HasRefICAO {
			iidPayload["reference_icao"] = snap.RefICAO
		}
		if snap.SyncPresent {
			iidPayload["sync_period_s"] = snap.SyncPeriodS
			iidPayload["sync_phase_epoch_us"] = snap.SyncPhaseEpochUS
			iidPayload["sync_phase_offset_deg"] = snap.SyncPhaseOffsetDeg
			iidPayload["sync_jitter_deg"] = snap.SyncJitterDeg
			iidPayload["sync_residual_ema_deg"] = snap.SyncResidualEMA
			iidPayload["sync_last_residual_deg"] = snap.SyncLastResidualDeg
			iidPayload["sync_last_updated"] = snap.SyncLastUpdatedUnix
		}
		if snap.BasePeriodS > 0 {
			iidPayload["base_period_s"] = snap.BasePeriodS
		}
		if snap.EffectivePeriodS > 0 {
			iidPayload["effective_period_s"] = snap.EffectivePeriodS
		}
		stableIIDs[key] = rcexport.IIDSnapshot{
			IID:    iidNum,
			Status: snap.Status,
			PeriodS: func() *float64 {
				if snap.HasPeriod {
					v := snap.PeriodS
					return &v
				}
				return nil
			}(),
			ReferenceICAO: func() *uint32 {
				if snap.HasRefICAO {
					v := snap.RefICAO
					return &v
				}
				return nil
			}(),
			SyncQuality:      snap.SyncQuality,
			SyncStateUsable:  snap.SyncUsable,
			SyncStatePresent: snap.SyncPresent,
			SyncPeriodS: func() *float64 {
				if snap.SyncPresent {
					v := snap.SyncPeriodS
					return &v
				}
				return nil
			}(),
			SyncPhaseEpochUS: func() *float64 {
				if snap.SyncPresent {
					v := snap.SyncPhaseEpochUS
					return &v
				}
				return nil
			}(),
			SyncPhaseOffsetDeg: func() *float64 {
				if snap.SyncPresent {
					v := snap.SyncPhaseOffsetDeg
					return &v
				}
				return nil
			}(),
			SyncJitterDeg: func() *float64 {
				if snap.SyncPresent {
					v := snap.SyncJitterDeg
					return &v
				}
				return nil
			}(),
			SyncResidualEMADeg: func() *float64 {
				if snap.SyncPresent {
					v := snap.SyncResidualEMA
					return &v
				}
				return nil
			}(),
			SyncLastResidualDeg: func() *float64 {
				if snap.SyncPresent {
					v := snap.SyncLastResidualDeg
					return &v
				}
				return nil
			}(),
			SyncNFrames:         snap.SyncNSyncFrames,
			SyncNRejectedFrames: snap.SyncNRejectedFrames,
			SyncHoldover:        snap.SyncHoldover,
			SyncLastUpdated: func() *float64 {
				if snap.SyncPresent {
					v := snap.SyncLastUpdatedUnix
					return &v
				}
				return nil
			}(),
			RetainedBurstSpan: snap.BurstRecordsRetainedSpanS,
		}

		if acc, ok := e.accumulators[iidNum]; ok {
			accDiag := acc.Diagnostics()
			iidPayload["frame_accumulator"] = map[string]interface{}{
				"completed_frames":                accDiag.FrameIndex,
				"open_frame":                      accDiag.OpenFrame,
				"open_frame_ref_icao":             accDiag.OpenFrameRefICAO,
				"open_frame_n_aircraft":           accDiag.OpenFrameNAircraft,
				"open_frame_n_obs":                accDiag.OpenFrameNObs,
				"last_frame_start_us":             accDiag.LastFrameStartUS,
				"last_gate_reason":                accDiag.LastGateReason,
				"last_gate_icao":                  accDiag.LastGateICAO,
				"gate_counts":                     accDiag.GateCounts,
				"centroid_history_icao_count":     accDiag.CentroidHistoryICAOCount,
				"centroid_history_total":          accDiag.CentroidHistoryTotal,
				"centroid_history_max_per_icao":   accDiag.CentroidHistoryMaxPerICAO,
				"centroid_history_cap_per_icao":   accDiag.CentroidHistoryCapPerICAO,
				"centroid_history_cap_hit":        accDiag.CentroidHistoryCapHit,
				"centroid_history_cap_hits_total": accDiag.CentroidHistoryCapHitsTotal,
			}
		}
		if fmState := e.fmState.Snapshot(iidNum); fmState != nil {
			iidPayload["forward_model"] = map[string]interface{}{
				"has_position":          fmState.CentroidAvailable,
				"lat":                   fmState.CentroidLat,
				"lon":                   fmState.CentroidLon,
				"cep_m":                 fmState.CentroidCEPM,
				"source":                "go_frame_accumulation",
				"n_observations":        fmState.CentroidInliers,
				"window_s":              0.0,
				"updated_ts":            fmState.UpdatedAt,
				"last_run":              map[string]interface{}{"ts": fmState.UpdatedAt, "success": fmState.LastFrameSuccess, "stage": fmState.LastSolveStatus, "reason": fmState.LastFrameReason, "source": "go_frame_accumulation", "lat": fmState.CentroidLat, "lon": fmState.CentroidLon, "cep_m": fmState.CentroidCEPM},
				"coincident_validation": nil,
			}
			iidPayload["frame_position_pipeline"] = map[string]interface{}{
				"frames_reaching_solver":           fmState.FramesReachingSolver,
				"solver_success":                   fmState.SolverSuccess,
				"candidate_positions":              fmState.CandidatePositions,
				"solver_no_candidate":              fmState.SolverNoCandidate,
				"accumulation_rejected":            fmState.AccumulationRejected,
				"accumulation_rejection_reasons":   fmState.AccumulationRejectionReasons,
				"accumulation_accepted":            fmState.AccumulationAccepted,
				"accumulation_acceptance_tiers":    fmState.AccumulationAcceptanceTiers,
				"accumulation_written":             fmState.AccumulationWritten,
				"storage_errors":                   0,
				"last_rejection_reason":            fmState.LastRejectionReason,
				"last_admission_tier":              fmState.LastAdmissionTier,
				"accumulated_frame_positions":      fmState.AccumulatedFramePositions,
				"centroid_available":               fmState.CentroidAvailable,
				"centroid_rejection_counts":        fmState.CentroidRejectionCounts,
				"centroid_inliers":                 fmState.CentroidInliers,
				"centroid_stage0_survivors":        fmState.CentroidStage0Survivors,
				"last_ambiguity_same_lobe_bypass":  fmState.LastAmbiguitySameLobeBypass,
				"last_cluster_member_count":        fmState.LastClusterMemberCount,
				"last_second_cluster_member_count": fmState.LastSecondClusterMemberCount,
				"last_support_dominance_ratio":     fmState.LastSupportDominanceRatio,
				"last_pairwise_rms_deg":            fmState.LastPairwiseRMSDeg,
			}
			if estimates := e.fmState.SnapshotFrameEstimates(iidNum); len(estimates) > 0 {
				framePositions := make([]map[string]interface{}, 0, len(estimates))
				for _, est := range estimates {
					framePositions = append(framePositions, map[string]interface{}{
						"frame_index":         est.FrameIndex,
						"sweep_start_us":      est.SweepStartUS,
						"lat":                 est.Lat,
						"lon":                 est.Lon,
						"cep_km":              est.CEPKM,
						"n_contributing_arcs": est.NContributingArcs,
						"azimuth_spread_deg":  est.AzimuthSpreadDeg,
						"weight":              est.Weight,
					})
				}
				iidPayload["frame_positions"] = framePositions
			}
		}
		iidsPayload[key] = iidPayload
	}
	payload["iids"] = iidsPayload
	trackObservations, evidenceEvents, sweepFrames := e.exports.Snapshot(targetIID)
	stableSnapshot := rcexport.RadarSnapshot{
		GeneratedAt:       float64(time.Now().UnixNano()) / float64(time.Second),
		UptimeS:           payload["uptime_s"].(float64),
		EventsIn:          payload["events_in"].(uint64),
		BurstsFired:       payload["bursts_fired"].(uint64),
		FramesEmitted:     payload["frames_emitted"].(uint64),
		ActiveIIDs:        payload["active_iids"].(int),
		IIDs:              stableIIDs,
		SweepFrames:       sweepFrames,
		EvidenceEvents:    evidenceEvents,
		TrackObservations: trackObservations,
		Profile:           e.profiler.Snapshot(),
	}
	payload["radar_snapshot"] = stableSnapshot
	payload["track_observations"] = trackObservations
	payload["evidence_events"] = evidenceEvents
	payload["sweep_frames"] = sweepFrames
	payload["profile"] = stableSnapshot.Profile
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
	e.fmState.Reset(msg.IID)
	e.exports.ResetIID(msg.IID)
	slog.Info("radar-core: IID reset", "iid", msg.IID)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (e *engine) emitIIDState(iidNum uint8, s *iid.IIDState, nBurstRecords uint16) {
	e.revisions[iidNum]++
	rev := e.revisions[iidNum]

	status, periodS, rpm, _ := s.Snapshot()
	syncQuality, refICAO := s.SyncSnapshot()
	syncPresent, syncUsable, syncPeriodS, syncPhaseEpochUS, syncPhaseOffsetDeg, syncJitterDeg, syncResidualEMA, syncLastResidual, syncNFrames, syncNRejected, syncHoldover := s.SyncProtocolSnapshot()

	var rpmMsg *float32
	if rpm != nil {
		v := float32(*rpm)
		rpmMsg = &v
	}

	msg := &protocol.IIDState{
		MsgType:                         protocol.MsgIIDState,
		IID:                             iidNum,
		PeriodS:                         periodS,
		RPM:                             rpmMsg,
		Status:                          status,
		RefICAO:                         refICAO,
		SyncQuality:                     syncQuality,
		SyncStatePresent:                syncPresent,
		SyncStateUsable:                 syncUsable,
		SyncPeriodS:                     syncPeriodS,
		SyncPhaseEpochUS:                syncPhaseEpochUS,
		SyncPhaseOffsetDeg:              syncPhaseOffsetDeg,
		SyncJitterDeg:                   syncJitterDeg,
		SyncResidualEMA:                 syncResidualEMA,
		SyncLastResidual:                syncLastResidual,
		SyncNFrames:                     syncNFrames,
		SyncNRejected:                   syncNRejected,
		SyncHoldover:                    syncHoldover,
		PeriodSource:                    snapPeriodSource(s),
		BasePeriodS:                     snapBasePeriod(s),
		PeriodDeltaS:                    snapPeriodDelta(s),
		EffectivePeriodS:                snapEffectivePeriod(s),
		ResidualSlopeDegPS:              snapResidualSlope(s),
		ResidualSlopeEMADegPS:           snapResidualSlopeEMA(s),
		ResidualSlopeStdDegPS:           snapResidualSlopeStd(s),
		ProposedDeltaS:                  snapProposedDelta(s),
		AppliedDeltaS:                   snapAppliedDelta(s),
		LastSlewLimited:                 snapLastSlewLimited(s),
		LastHardBound:                   snapLastHardBound(s),
		HardBoundReason:                 snapHardBoundReason(s),
		HardBoundLimitS:                 snapHardBoundLimitS(s),
		HardBoundLimitPPM:               snapHardBoundLimitPPM(s),
		RequestedDeltaS:                 snapRequestedDeltaS(s),
		RequestedDeltaPPM:               snapRequestedDeltaPPM(s),
		CurrentDeltaS:                   snapCurrentDeltaS(s),
		CurrentDeltaPPM:                 snapCurrentDeltaPPM(s),
		DeltaToBaseS:                    snapDeltaToBaseS(s),
		DeltaToBasePPM:                  snapDeltaToBasePPM(s),
		DFBasePeriodS:                   snapDFBasePeriodS(s),
		PeriodDisagreementS:             snapPeriodDisagreementS(s),
		PeriodDisagreementPPM:           snapPeriodDisagreementPPM(s),
		PeriodRefineStatus:              snapPeriodRefineStatus(s),
		PeriodAgreesWithDF:              snapPeriodAgrees(s),
		PeriodRejectReason:              snapPeriodRejectReason(s),
		FitObservationCount:             snapFitObservationCount(s),
		FitSpanS:                        snapFitSpanS(s),
		FitICAOCount:                    snapFitICAOCount(s),
		FitPerICAOMin:                   snapFitPerICAOMin(s),
		FitPerICAOMedian:                snapFitPerICAOMedian(s),
		FitPerICAOMax:                   snapFitPerICAOMax(s),
		FitRetentionWindowS:             snapFitRetentionWindowS(s),
		FitGlobalCapHit:                 snapFitGlobalCapHit(s),
		FitLastEvictionReason:           snapFitLastEvictionReason(s),
		FitInlierRatio:                  snapFitInlierRatio(s),
		SuspiciousICAOCount:             snapSuspiciousICAOCount(s),
		SuspiciousICAOLastReason:        snapSuspiciousICAOLastReason(s),
		FitEpochID:                      snapFitEpochID(s),
		FitEpochStartedUnix:             snapFitEpochStartedUnix(s),
		FitEpochResetReason:             snapFitEpochResetReason(s),
		FitEpochObservationCount:        snapFitEpochObservationCount(s),
		FitEpochSpanS:                   snapFitEpochSpanS(s),
		FitDroppedOnEpochReset:          snapFitDroppedOnEpochReset(s),
		FitSegmentCount:                 snapFitSegmentCount(s),
		SlopeSignConvention:             snapSlopeSignConvention(s),
		HoldoverReason:                  snapHoldoverReason(s),
		HoldoverQualityGateFailed:       snapHoldoverCount(s, "quality_gate_failed"),
		HoldoverMissingDFBasePeriod:     snapHoldoverCount(s, "missing_df_base_period"),
		HoldoverHardResidualReject:      snapHoldoverCount(s, "hard_residual_reject"),
		HoldoverNoReference:             snapHoldoverCount(s, "no_reference"),
		HoldoverStaleReferencePosition:  snapHoldoverCount(s, "stale_reference_position"),
		HoldoverPeriodDisagreement:      snapHoldoverCount(s, "period_disagreement"),
		HoldoverInsufficientAircraft:    snapHoldoverCount(s, "insufficient_aircraft"),
		HoldoverNoDominantFamily:        snapHoldoverCount(s, "no_dominant_family"),
		HoldoverSyncStateMissing:        snapHoldoverCount(s, "sync_state_missing"),
		UpdateEpochAttempts:             snapUpdateEpochAttempts(s),
		UpdateEpochAccepts:              snapUpdateEpochAccepts(s),
		UpdateEpochRejects:              snapUpdateEpochRejects(s),
		LastUpdateEpochRejectReason:     snapLastUpdateEpochRejectReason(s),
		LastUpdateEpochNAircraft:        snapLastUpdateEpochNAircraft(s),
		LastUpdateEpochRefPosAgeS:       snapLastUpdateEpochRefPosAgeS(s),
		LastUpdateEpochRefICAO:          snapLastUpdateEpochRefICAO(s),
		LastUpdateEpochResidualDeg:      snapLastUpdateEpochResidualDeg(s),
		LastUpdateEpochPredictedDeg:     snapLastUpdateEpochPredictedDeg(s),
		LastUpdateEpochObservedDeg:      snapLastUpdateEpochObservedDeg(s),
		ConsecutiveHardResidualRejects:  snapConsecutiveHardResidualRejects(s),
		LastAcceptedEpochAgeS:           snapLastAcceptedEpochAgeS(s),
		SyncEpochAgeS:                   snapSyncEpochAgeS(s),
		CurrentPhaseEpochUS:             snapCurrentPhaseEpochUS(s),
		CandidateEpochUS:                snapCandidateEpochUS(s),
		SyncReacquiredProvisional:       snapSyncReacquiredProvisional(s),
		UpdateEpochRejectQualityGate:    snapUpdateEpochRejectCount(s, "quality_gate_failed"),
		UpdateEpochRejectMissingBase:    snapUpdateEpochRejectCount(s, "missing_df_base_period"),
		UpdateEpochRejectHardResidual:   snapUpdateEpochRejectCount(s, "hard_residual_reject"),
		UpdateEpochRejectNoReference:    snapUpdateEpochRejectCount(s, "no_reference"),
		UpdateEpochRejectStaleRefPos:    snapUpdateEpochRejectCount(s, "stale_reference_position"),
		UpdateEpochRejectInsufficientAC: snapUpdateEpochRejectCount(s, "insufficient_aircraft"),
		UpdateEpochLastStrictGatePass:   snapUpdateEpochLastStrictGatePass(s),
		ReacquireSupportObsCount:       snapReacquireSupportObsCount(s),
		ReacquireSupportICAOCount:      snapReacquireSupportICAOCount(s),
		NBurstRecords:                   nBurstRecords,
		LastUpdated:                     float64(time.Now().UnixMicro()) / 1e6,
		Revision:                        rev,
	}
	e.writer.SendIIDState(msg)

	slog.Debug("radar-core: IID_STATE emitted",
		"iid", iidNum,
		"status", status,
		"period_s", periodS,
		"n_records", nBurstRecords,
		"sync_present", syncPresent,
		"revision", rev,
	)
}

func snapBasePeriod(s *iid.IIDState) *float64 {
	snap := s.DebugStateSnapshot()
	if snap.BasePeriodS <= 0 {
		return nil
	}
	v := snap.BasePeriodS
	return &v
}

func snapPeriodDelta(s *iid.IIDState) *float64 {
	snap := s.DebugStateSnapshot()
	v := snap.PeriodDeltaS
	return &v
}

func snapEffectivePeriod(s *iid.IIDState) *float64 {
	snap := s.DebugStateSnapshot()
	if snap.EffectivePeriodS <= 0 {
		return nil
	}
	v := snap.EffectivePeriodS
	return &v
}

func snapPeriodSource(s *iid.IIDState) string {
	return s.DebugStateSnapshot().PeriodSource
}

func snapPeriodAgrees(s *iid.IIDState) bool {
	return s.DebugStateSnapshot().PeriodAgreesWithDF
}

func snapResidualSlope(s *iid.IIDState) *float64 {
	snap := s.DebugStateSnapshot()
	v := snap.ResidualSlopeDegPerS
	return &v
}

func snapPeriodRefineStatus(s *iid.IIDState) string {
	return s.DebugStateSnapshot().PeriodRefinementStatus
}

func snapPeriodRejectReason(s *iid.IIDState) string {
	return s.DebugStateSnapshot().PeriodRejectReason
}
func snapResidualSlopeEMA(s *iid.IIDState) *float64 {
	v := s.DebugStateSnapshot().ResidualSlopeEMADegPerS
	return &v
}
func snapResidualSlopeStd(s *iid.IIDState) *float64 {
	v := s.DebugStateSnapshot().ResidualSlopeStdDegPerS
	return &v
}
func snapProposedDelta(s *iid.IIDState) *float64 {
	v := s.DebugStateSnapshot().ProposedDeltaS
	return &v
}
func snapAppliedDelta(s *iid.IIDState) *float64 { v := s.DebugStateSnapshot().AppliedDeltaS; return &v }
func snapLastSlewLimited(s *iid.IIDState) bool  { return s.DebugStateSnapshot().LastSlewLimited }
func snapLastHardBound(s *iid.IIDState) bool    { return s.DebugStateSnapshot().LastHardBound }
func snapHardBoundReason(s *iid.IIDState) string {
	return s.DebugStateSnapshot().HardBoundReason
}
func snapHardBoundLimitS(s *iid.IIDState) *float64 { v := s.DebugStateSnapshot().HardBoundLimitS; return &v }
func snapHardBoundLimitPPM(s *iid.IIDState) *float64 {
	v := s.DebugStateSnapshot().HardBoundLimitPPM
	return &v
}
func snapRequestedDeltaS(s *iid.IIDState) *float64 { v := s.DebugStateSnapshot().RequestedDeltaS; return &v }
func snapRequestedDeltaPPM(s *iid.IIDState) *float64 {
	v := s.DebugStateSnapshot().RequestedDeltaPPM
	return &v
}
func snapCurrentDeltaS(s *iid.IIDState) *float64 { v := s.DebugStateSnapshot().CurrentDeltaS; return &v }
func snapCurrentDeltaPPM(s *iid.IIDState) *float64 {
	v := s.DebugStateSnapshot().CurrentDeltaPPM
	return &v
}
func snapDeltaToBaseS(s *iid.IIDState) *float64 { v := s.DebugStateSnapshot().DeltaToBaseS; return &v }
func snapDeltaToBasePPM(s *iid.IIDState) *float64 {
	v := s.DebugStateSnapshot().DeltaToBasePPM
	return &v
}
func snapDFBasePeriodS(s *iid.IIDState) *float64 { v := s.DebugStateSnapshot().DFBasePeriodS; return &v }
func snapPeriodDisagreementS(s *iid.IIDState) *float64 {
	v := s.DebugStateSnapshot().PeriodDisagreementS
	return &v
}
func snapPeriodDisagreementPPM(s *iid.IIDState) *float64 {
	v := s.DebugStateSnapshot().PeriodDisagreementPPM
	return &v
}
func snapFitObservationCount(s *iid.IIDState) uint16 {
	v := s.DebugStateSnapshot().FitObservationCount
	if v < 0 {
		return 0
	}
	if v > math.MaxUint16 {
		return math.MaxUint16
	}
	return uint16(v)
}
func snapFitSpanS(s *iid.IIDState) *float64 { v := s.DebugStateSnapshot().FitSpanS; return &v }
func snapFitICAOCount(s *iid.IIDState) uint16 {
	v := s.DebugStateSnapshot().FitICAOCount
	if v < 0 {
		return 0
	}
	if v > math.MaxUint16 {
		return math.MaxUint16
	}
	return uint16(v)
}
func snapFitPerICAOMin(s *iid.IIDState) uint16 {
	v := s.DebugStateSnapshot().FitObservationsPerICAOMin
	if v < 0 {
		return 0
	}
	if v > math.MaxUint16 {
		return math.MaxUint16
	}
	return uint16(v)
}
func snapFitPerICAOMedian(s *iid.IIDState) *float64 {
	v := s.DebugStateSnapshot().FitObservationsPerICAOMedian
	return &v
}
func snapFitPerICAOMax(s *iid.IIDState) uint16 {
	v := s.DebugStateSnapshot().FitObservationsPerICAOMax
	if v < 0 {
		return 0
	}
	if v > math.MaxUint16 {
		return math.MaxUint16
	}
	return uint16(v)
}
func snapFitRetentionWindowS(s *iid.IIDState) *float64 {
	v := s.DebugStateSnapshot().FitRetentionWindowS
	return &v
}
func snapFitGlobalCapHit(s *iid.IIDState) bool { return s.DebugStateSnapshot().FitGlobalCapHit }
func snapFitLastEvictionReason(s *iid.IIDState) string {
	return s.DebugStateSnapshot().FitLastEvictionReason
}
func snapFitInlierRatio(s *iid.IIDState) *float64 {
	v := s.DebugStateSnapshot().FitInlierRatio
	if v < 0.0 {
		return nil // sentinel for "no data" — Python maps to None
	}
	return &v
}
func snapSuspiciousICAOCount(s *iid.IIDState) uint16 {
	v := s.DebugStateSnapshot().SuspiciousICAOCount
	if v < 0 {
		return 0
	}
	if v > math.MaxUint16 {
		return math.MaxUint16
	}
	return uint16(v)
}
func snapSuspiciousICAOLastReason(s *iid.IIDState) string {
	return s.DebugStateSnapshot().SuspiciousICAOLastReason
}
func snapFitEpochID(s *iid.IIDState) uint32 {
	v := s.DebugStateSnapshot().FitEpochID
	if v > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}
func snapFitEpochStartedUnix(s *iid.IIDState) *float64 {
	v := s.DebugStateSnapshot().FitEpochStartedUnix
	return &v
}
func snapFitEpochResetReason(s *iid.IIDState) string {
	return s.DebugStateSnapshot().FitEpochResetReason
}
func snapFitEpochObservationCount(s *iid.IIDState) uint16 {
	v := s.DebugStateSnapshot().FitEpochObservationCount
	if v < 0 {
		return 0
	}
	if v > math.MaxUint16 {
		return math.MaxUint16
	}
	return uint16(v)
}
func snapFitEpochSpanS(s *iid.IIDState) *float64 {
	v := s.DebugStateSnapshot().FitEpochSpanS
	return &v
}
func snapFitDroppedOnEpochReset(s *iid.IIDState) uint32 {
	v := s.DebugStateSnapshot().FitDroppedOnEpochReset
	if v < 0 {
		return 0
	}
	if v > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}
func snapFitSegmentCount(s *iid.IIDState) uint32 {
	v := s.DebugStateSnapshot().FitSegmentCount
	if v > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}
func snapSlopeSignConvention(s *iid.IIDState) string {
	return s.DebugStateSnapshot().SlopeSignConvention
}
func snapHoldoverReason(s *iid.IIDState) string { return s.DebugStateSnapshot().HoldoverReason }
func snapHoldoverCount(s *iid.IIDState, reason string) uint32 {
	snap := s.DebugStateSnapshot()
	var v uint64
	switch reason {
	case "quality_gate_failed":
		v = snap.HoldoverQualityGateFailed
	case "missing_df_base_period":
		v = snap.HoldoverMissingDFBasePeriod
	case "hard_residual_reject":
		v = snap.HoldoverHardResidualReject
	case "no_reference":
		v = snap.HoldoverNoReference
	case "stale_reference_position":
		v = snap.HoldoverStaleReferencePosition
	case "period_disagreement":
		v = snap.HoldoverPeriodDisagreement
	case "insufficient_aircraft":
		v = snap.HoldoverInsufficientAircraft
	case "no_dominant_family":
		v = snap.HoldoverNoDominantFamily
	case "sync_state_missing":
		v = snap.HoldoverSyncStateMissing
	}
	if v > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}
func snapUpdateEpochAttempts(s *iid.IIDState) uint32 {
	v := s.DebugStateSnapshot().UpdateEpochAttempts
	if v > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}
func snapUpdateEpochAccepts(s *iid.IIDState) uint32 {
	v := s.DebugStateSnapshot().UpdateEpochAccepts
	if v > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}
func snapUpdateEpochRejects(s *iid.IIDState) uint32 {
	v := s.DebugStateSnapshot().UpdateEpochRejects
	if v > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}
func snapLastUpdateEpochRejectReason(s *iid.IIDState) string {
	return s.DebugStateSnapshot().LastUpdateEpochRejectReason
}
func snapLastUpdateEpochNAircraft(s *iid.IIDState) uint16 {
	v := s.DebugStateSnapshot().LastUpdateEpochNAircraft
	if v < 0 {
		return 0
	}
	if v > math.MaxUint16 {
		return math.MaxUint16
	}
	return uint16(v)
}
func snapLastUpdateEpochRefPosAgeS(s *iid.IIDState) *float64 {
	v := s.DebugStateSnapshot().LastUpdateEpochRefPosAgeS
	return &v
}
func snapLastUpdateEpochRefICAO(s *iid.IIDState) *uint32 {
	v := s.DebugStateSnapshot().LastUpdateEpochRefICAO
	if v == 0 {
		return nil
	}
	return &v
}
func snapLastUpdateEpochResidualDeg(s *iid.IIDState) *float64 {
	v := s.DebugStateSnapshot().LastUpdateEpochResidualDeg
	return &v
}
func snapLastUpdateEpochPredictedDeg(s *iid.IIDState) *float64 {
	v := s.DebugStateSnapshot().LastUpdateEpochPredictedDeg
	return &v
}
func snapLastUpdateEpochObservedDeg(s *iid.IIDState) *float64 {
	v := s.DebugStateSnapshot().LastUpdateEpochObservedDeg
	return &v
}
func snapConsecutiveHardResidualRejects(s *iid.IIDState) uint32 {
	v := s.DebugStateSnapshot().ConsecutiveHardResidualRejects
	if v > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}
func snapLastAcceptedEpochAgeS(s *iid.IIDState) *float64 {
	v := s.DebugStateSnapshot().LastAcceptedEpochAgeS
	return &v
}
func snapSyncEpochAgeS(s *iid.IIDState) *float64 {
	v := s.DebugStateSnapshot().SyncEpochAgeS
	return &v
}
func snapCurrentPhaseEpochUS(s *iid.IIDState) *float64 {
	v := s.DebugStateSnapshot().CurrentPhaseEpochUS
	if v <= 0 {
		return nil
	}
	return &v
}
func snapCandidateEpochUS(s *iid.IIDState) *float64 {
	v := s.DebugStateSnapshot().CandidateEpochUS
	if v <= 0 {
		return nil
	}
	return &v
}
func snapSyncReacquiredProvisional(s *iid.IIDState) bool {
	return s.DebugStateSnapshot().ReacquiredProvisional
}
func snapUpdateEpochRejectCount(s *iid.IIDState, reason string) uint32 {
	snap := s.DebugStateSnapshot()
	var v uint64
	switch reason {
	case "quality_gate_failed":
		v = snap.UpdateEpochRejectQualityGate
	case "missing_df_base_period":
		v = snap.UpdateEpochRejectMissingBase
	case "hard_residual_reject":
		v = snap.UpdateEpochRejectHardResidual
	case "no_reference":
		v = snap.UpdateEpochRejectNoReference
	case "stale_reference_position":
		v = snap.UpdateEpochRejectStaleRefPos
	case "insufficient_aircraft":
		v = snap.UpdateEpochRejectInsufficientAC
	}
	if v > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}
func snapUpdateEpochLastStrictGatePass(s *iid.IIDState) bool {
	return s.DebugStateSnapshot().UpdateEpochLastStrictGatePass
}

func snapReacquireSupportObsCount(s *iid.IIDState) uint16 {
	return uint16(s.DebugStateSnapshot().ReacquireSupportObservationCount)
}

func snapReacquireSupportICAOCount(s *iid.IIDState) uint16 {
	return uint16(s.DebugStateSnapshot().ReacquireSupportICAOCount)
}

func (e *engine) runFMWorker(stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case frameMsg := <-e.fmQueue:
			frameResult, state := e.fmState.ProcessFrame(frameMsg)
			if frameResult != nil {
				e.writer.SendFMFrameResult(frameResult)
			}
			if state != nil {
				e.writer.SendFMState(state)
			}
		}
	}
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
		s.RefreshReference(records, nowUS, e.positions)

		e.emitIIDState(iidNum, s, uint16(minInt(len(records), 65535)))
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
