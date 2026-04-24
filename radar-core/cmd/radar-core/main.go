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

type exportSyncEligibility struct {
	compactEligible             bool
	refinedPresent              bool
	refinedUsable               bool // relative sync: Present && Usable (timing/phase work)
	refinedAbsolutePhaseTrusted bool // geographic bearing: Present && AbsolutePhaseTrusted
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

		// Stage 4: feed the frame accumulator.
		acc.OnBurst(f.ICAO, f.CentroidUS, f.NReplies, f.SignalDBFS, s)
		e.recordObservationExports(s, f)
		e.emitBurstFired(s, f)

		// Multi-aircraft sync refinement: feed sync-eligible bursts and run solver.
		e.maybeUpdateMultiSync(s, f)

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
	snap := s.DebugStateSnapshot()
	e.emitIIDState(s.IID, s, uint16(minInt(snap.BurstRecordsTotal, 65535)))
}

// maybeUpdateMultiSync feeds sync-eligible bursts into the per-IID multi-aircraft
// sync solver and emits a MultiSyncState when a solver run completes.
func (e *engine) maybeUpdateMultiSync(s *iid.IIDState, f *burst.FiredBurst) {
	nowUnix := float64(time.Now().UnixMicro()) / 1e6
	if s.MultiSync == nil {
		s.RecordMultiSyncAdmission("no_solver", f.ICAO, nowUnix)
		return
	}
	// Only feed dominant-family bursts with a known ADS-B position.
	dominantFamily := false
	if family := s.FamilySnapshot(); family != nil && family.FoldedICAOs != nil {
		_, dominantFamily = family.FoldedICAOs[f.ICAO]
	}
	if !dominantFamily {
		s.RecordMultiSyncAdmission("not_dominant_family", f.ICAO, nowUnix)
		return
	}
	pos := e.positions.Get(f.ICAO)
	if pos == nil {
		s.RecordMultiSyncAdmission("no_adsb_position", f.ICAO, nowUnix)
		return
	}

	// Compute bearing and range from receiver to aircraft.
	cfg := rcconfig.Get()
	if !cfg.HasReceiver {
		s.RecordMultiSyncAdmission("no_receiver_config", f.ICAO, nowUnix)
		return
	}
	bearing, rangeNM := iid.BearingAndRangeNM(
		cfg.ReceiverLat, cfg.ReceiverLon, pos.Lat, pos.Lon,
	)

	posAgeS := float32(time.Since(pos.TS).Seconds())
	var sig *float32
	if f.SignalDBFS != nil {
		v := float32(*f.SignalDBFS)
		sig = &v
	}

	obs := iid.MultiSyncObs{
		CentroidUS: f.CentroidUS,
		ICAO:       f.ICAO,
		BearingDeg: bearing,
		RangeNM:    float32(rangeNM),
		PosAgeS:    posAgeS,
		NReplies:   f.NReplies,
		SignalDBFS: sig,
		WallTS:     float64(time.Now().UnixMicro()) / 1e6,
	}
	s.MultiSync.AddObs(obs)
	s.RecordMultiSyncAdmission("admitted", f.ICAO, nowUnix)

	// Run the solver if the throttle interval has elapsed.
	syncSnap, _ := s.SyncSnapshot()
	_ = syncSnap
	sync := s.SyncStateRef()
	dominantPeriodS := s.DominantPeriodSnapshot()
	if updated := s.MultiSync.TryUpdate(sync, dominantPeriodS); updated {
		e.emitMultiSyncState(s)
	} else {
		s.RecordMultiSyncAdmission("solver_throttled_or_no_update", f.ICAO, nowUnix)
	}
}

// emitMultiSyncState sends the current multi-aircraft sync state for one IID.
func (e *engine) emitMultiSyncState(s *iid.IIDState) {
	snap := s.MultiSync.Snapshot()
	if !snap.Present {
		return
	}
	candidates := make([]protocol.MultiSyncAnchorCandidate, 0, len(snap.AnchorCandidates))
	for _, row := range snap.AnchorCandidates {
		candidates = append(candidates, protocol.MultiSyncAnchorCandidate{
			ICAO:                row.ICAO,
			Score:               row.Score,
			SpreadDeg:           row.SpreadDeg,
			ObsCount:            uint16(row.ObsCount),
			FitEligibleCount:    uint16(row.FitEligibleCount),
			FitEligibleFraction: row.FitEligibleFraction,
			Status:              row.Status,
			RejectReasons:       row.RejectReasons,
		})
	}
	msg := &protocol.MultiSyncState{
		MsgType:               protocol.MsgMultiSyncState,
		IID:                   s.IID,
		Present:               snap.Present,
		Usable:                snap.Usable,
		PeriodS:               snap.PeriodS,
		PeriodBaseS:           snap.PeriodBaseS,
		PhaseEpochUS:          snap.PhaseEpochUS,
		PhaseOffsetDeg:        snap.PhaseOffsetDeg,
		JitterDeg:             snap.JitterDeg,
		ResidualEMADeg:        snap.ResidualEMADeg,
		NSyncUpdates:          uint32(snap.NSyncUpdates),
		Holdover:              snap.Holdover,
		PeriodReacquireActive: snap.PeriodReacquireActive,
		PeriodReacquireReason: snap.PeriodReacquireReason,
		AnchorICAO:            snap.AnchorICAO,
		AnchorPhaseDeg:        snap.AnchorPhaseDeg,
		AnchorScore:           snap.AnchorScore,
		UpdatedAt:             snap.LastUpdated,
		// Bootstrap / trust diagnostics.
		BootstrapPeriodS:                 snap.BootstrapPeriodS,
		TrustedBasePeriodS:               snap.TrustedBasePeriodS,
		TrustUpdateStreak:                uint16(snap.TrustUpdateStreak),
		BaseClamped:                      snap.BaseClamped,
		BaseClampDiffPPM:                 snap.BaseClampDiffPPM,
		WrongPeriodSuspect:               snap.WrongPeriodSuspect,
		ReacquireCandidatePeriod:         snap.ReacquireCandidatePeriod,
		ReacquireCandidateScore:          snap.ReacquireCandidateScore,
		FitTotalObservations:             uint32(snap.FitTotalObservations),
		FitEligibleObservations:          uint32(snap.FitEligibleObservations),
		FitRejectedObservations:          uint32(snap.FitRejectedObservations),
		FitContributingICAOs:             uint16(snap.FitContributingICAOs),
		FitWindowS:                       snap.FitWindowS,
		DisplayWindowS:                   snap.DisplayWindowS,
		FitSpanS:                         snap.FitSpanS,
		ResidualSlopeDegPerS:             snap.ResidualSlopeDegPerS,
		FitRejectReasons:                 snap.FitRejectReasons,
		AnchorCandidateCount:             uint16(snap.AnchorCandidateCount),
		AnchorNoCandidateReason:          snap.AnchorNoCandidateReason,
		AnchorCandidates:                 candidates,
		DominantPriorPeriodS:             snap.DominantPriorPeriodS,
		TrustedRefinedPeriodS:            snap.TrustedRefinedPeriodS,
		ActiveFamilyPriorPeriodS:         snap.ActiveFamilyPriorPeriodS,
		ActiveFamilyPriorSource:          snap.ActiveFamilyPriorSource,
		DominantPriorActive:              snap.DominantPriorActive,
		CompactPeriodS:                   snap.CompactPeriodS,
		PeriodDeltaToDominantS:           snap.PeriodDeltaToDominantS,
		PeriodDeltaToDominantPPM:         snap.PeriodDeltaToDominantPPM,
		CompactDeltaToDominantS:          snap.CompactDeltaToDominantS,
		CompactDeltaToDominantPPM:        snap.CompactDeltaToDominantPPM,
		CompactSyncUnreliable:            snap.CompactSyncUnreliable,
		RecoveryModeActive:               snap.RecoveryModeActive,
		RecoveryTriggerReasons:           snap.RecoveryTriggerReasons,
		CompactGatingBypassed:            snap.CompactGatingBypassed,
		RecoveryRelaxedAdmissions:        uint32(snap.RecoveryRelaxedAdmissions),
		ActiveAuthorityMode:              snap.ActiveAuthorityMode,
		AuthoritySwitchCount:             uint32(snap.AuthoritySwitchCount),
		LastAuthoritySwitchTS:            snap.LastAuthoritySwitchTS,
		LastAuthoritySwitchReason:        snap.LastAuthoritySwitchReason,
		AuthorityEnterStreak:             uint16(snap.AuthorityEnterStreak),
		AuthorityExitStreak:              uint16(snap.AuthorityExitStreak),
		AnchorSwitchCount:                uint32(snap.AnchorSwitchCount),
		LastAnchorSwitchTS:               snap.LastAnchorSwitchTS,
		LastAnchorSwitchReason:           snap.LastAnchorSwitchReason,
		AnchorHoldUpdates:                uint16(snap.AnchorHoldUpdates),
		CandidatePeriodS:                 snap.CandidatePeriodS,
		AuthoritativePeriodS:             snap.AuthoritativePeriodS,
		CandidatePhaseOffsetDeg:          snap.CandidatePhaseOffsetDeg,
		AuthoritativePhaseOffsetDeg:      snap.AuthoritativePhaseOffsetDeg,
		CandidateAnchorICAO:              snap.CandidateAnchorICAO,
		AuthoritativeAnchorICAO:          snap.AuthoritativeAnchorICAO,
		CandidateValidationScore:         snap.CandidateValidationScore,
		AuthoritativeValidationScore:     snap.AuthoritativeValidationScore,
		AuthoritativeStateAgeS:           snap.AuthoritativeStateAgeS,
		CandidatePromotionStreak:         uint16(snap.CandidatePromotionStreak),
		AuthoritativePeriodUpdateGain:    snap.AuthoritativePeriodUpdateGain,
		AuthoritativePhaseUpdateGain:     snap.AuthoritativePhaseUpdateGain,
		PeriodFrozenDueToPhaseValidation: snap.PeriodFrozenDueToPhaseValidation,
		BranchAmbiguityScore:             snap.BranchAmbiguityScore,
		CircularDispersionDeg:            snap.CircularDispersionDeg,
		ValidatorAgreementCount:          uint16(snap.ValidatorAgreementCount),
		ValidatorDisagreementCount:       uint16(snap.ValidatorDisagreementCount),
		CandidateValidationStatus:        snap.CandidateValidationStatus,
		CandidateApplicationBlockReason:  snap.CandidateApplicationBlockReason,
		CandidateMode:                    snap.CandidateMode,
		AuthoritativeMode:                snap.AuthoritativeMode,
		AbsolutePhaseTrusted:             snap.AbsolutePhaseTrusted,
		DominantPriorInconsistent:        snap.DominantPriorInconsistent,
		PeriodFitAcceptedObservations:    uint32(snap.PeriodFitAcceptedObservations),
		PeriodFitRejectedObservations:    uint32(snap.PeriodFitRejectedObservations),
		PeriodFitRejectedAfterUnwrap:     uint32(snap.PeriodFitRejectedAfterUnwrap),
		SlopeWindowDeg:                   snap.SlopeWindowDeg,
		SlopePromotionGatePassed:         snap.SlopePromotionGatePassed,
		AuthorityPromotionBlockReason:    snap.AuthorityPromotionBlockReason,
		ResidualCorrectionBasis:          snap.ResidualCorrectionBasis,
		MotionGuardDegraded:              snap.MotionGuardDegraded,
	}
	e.writer.Send(msg)
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
	var assoc float32
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
	eligibility := e.exportSyncEligibility(s, dominantFamily, assoc)
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
		MsgType:                         protocol.MsgBurstFired,
		IID:                             f.IID,
		ICAO:                            f.ICAO,
		CentroidUS:                      f.CentroidUS,
		SimpleCentroidUS:                &simpleCentroid,
		WeightedCentroidUS:              f.WeightedCentroidUS,
		CentroidDeltaUS:                 &centroidDelta,
		FirstReplyUS:                    &firstReply,
		StrongestReplyUS:                f.StrongestReplyUS,
		MidStrongWindowUS:               f.MidStrongWindowUS,
		LastReplyUS:                     &lastReply,
		SpanUS:                          &spanUS,
		PeakAmplitude:                   peakAmplitude,
		NReplies:                        uint8(nReplies),
		SignalDBFS:                      sig,
		Lat:                             latPtr,
		Lon:                             lonPtr,
		PosAgeS:                         posAgePtr,
		DominantFamily:                  dominantFamily,
		SyncEligible:                    eligibility.refinedAbsolutePhaseTrusted,
		CompactSyncEligible:             eligibility.compactEligible,
		RefinedSyncPresent:              eligibility.refinedPresent,
		RefinedSyncUsable:               eligibility.refinedUsable,
		RefinedSyncAbsolutePhaseTrusted: eligibility.refinedAbsolutePhaseTrusted,
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
	eligibility := e.exportSyncEligibility(s, dominantFamily, assoc)
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
		SyncEligible:          eligibility.refinedAbsolutePhaseTrusted,
		CompactSyncEligible:   eligibility.compactEligible,
		RefinedSyncPresent:    eligibility.refinedPresent,
		RefinedSyncUsable:     eligibility.refinedUsable,
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
		SyncEligible:          eligibility.refinedAbsolutePhaseTrusted,
		CompactSyncEligible:   eligibility.compactEligible,
		RefinedSyncPresent:    eligibility.refinedPresent,
		RefinedSyncUsable:     eligibility.refinedUsable,
		AssociationConfidence: assoc,
	})
	e.profiler.Observe("evidence_export", time.Since(tExport))
}

func (e *engine) exportSyncEligibility(s *iid.IIDState, dominantFamily bool, assoc float32) exportSyncEligibility {
	syncQuality, _ := s.SyncSnapshot()
	result := exportSyncEligibility{
		compactEligible: dominantFamily && assoc > 0.0 && syncQuality >= 0.3,
	}
	if s.MultiSync == nil {
		return result
	}
	snap := s.MultiSync.Snapshot()
	result.refinedPresent = snap.Present
	result.refinedUsable = snap.Present && snap.Usable
	result.refinedAbsolutePhaseTrusted = snap.Present && snap.AbsolutePhaseTrusted
	return result
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
	rcconfig.Apply(msg.Key, msg.Value)
	cfg := rcconfig.Get()
	if cfg.HasReceiver {
		e.fmState.SetReceiver(cfg.ReceiverLat, cfg.ReceiverLon)
	}
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
			"status":                 snap.Status,
			"has_period":             snap.HasPeriod,
			"period_s":               nil,
			"has_reference_icao":     snap.HasRefICAO,
			"reference_icao":         nil,
			"sync_state_present":     snap.SyncPresent,
			"sync_quality":           snap.SyncQuality,
			"sync_state_usable":      snap.SyncUsable,
			"sync_period_s":          nil,
			"sync_phase_epoch_us":    nil,
			"sync_phase_offset_deg":  nil,
			"sync_jitter_deg":        nil,
			"sync_residual_ema_deg":  nil,
			"sync_last_residual_deg": nil,
			"sync_holdover":          snap.SyncHoldover,
			"sync_n_frames":          snap.SyncNSyncFrames,
			"sync_n_rejected_frames": snap.SyncNRejectedFrames,
			"sync_last_updated":      nil,
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
		if admission := s.MultiSyncAdmissionSnapshot(); admission.Counts != nil {
			iidPayload["multi_sync_admission"] = map[string]interface{}{
				"last_reason": admission.LastReason,
				"last_icao": func() interface{} {
					if admission.LastICAO == 0 {
						return nil
					}
					return admission.LastICAO
				}(),
				"last_ts": admission.LastTS,
				"counts":  admission.Counts,
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
		MsgType:            protocol.MsgIIDState,
		IID:                iidNum,
		PeriodS:            periodS,
		RPM:                rpmMsg,
		Status:             status,
		RefICAO:            refICAO,
		SyncQuality:        syncQuality,
		SyncStatePresent:   syncPresent,
		SyncStateUsable:    syncUsable,
		SyncPeriodS:        syncPeriodS,
		SyncPhaseEpochUS:   syncPhaseEpochUS,
		SyncPhaseOffsetDeg: syncPhaseOffsetDeg,
		SyncJitterDeg:      syncJitterDeg,
		SyncResidualEMA:    syncResidualEMA,
		SyncLastResidual:   syncLastResidual,
		SyncNFrames:        syncNFrames,
		SyncNRejected:      syncNRejected,
		SyncHoldover:       syncHoldover,
		NBurstRecords:      nBurstRecords,
		LastUpdated:        float64(time.Now().UnixMicro()) / 1e6,
		Revision:           rev,
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
		s.RefreshReference(records, nowUS)

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
