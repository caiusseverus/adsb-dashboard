// Package frame implements the per-IID sweep-frame accumulator.
//
// Stage 4: when the reference aircraft fires, open a sweep frame; collect
// non-reference aircraft observations within the sweep window; emit
// FRAME_READY when the next reference burst closes the window.
//
// All methods are called from the single ingest goroutine — no locking needed
// on the accumulator itself. The position cache and IIDState are thread-safe.
package frame

import (
	"log/slog"
	"math"
	"time"

	rcconfig "github.com/caiusseverus/adsb-dashboard/radar-core/config"
	"github.com/caiusseverus/adsb-dashboard/radar-core/iid"
	"github.com/caiusseverus/adsb-dashboard/radar-core/output"
	"github.com/caiusseverus/adsb-dashboard/radar-core/protocol"
)

// Accumulator constants — match sweep.py.
const (
	coSweepWindowUS               = 70_000.0 // max offset for a co-sweep burst
	phaseFamilyToleranceFraction  = 0.15     // period fraction used when coSweepWindow is narrower
	phaseFamilyHistoryMin         = 2        // minimum historical offsets for phase check
	periodMatchTolerance          = 0.15     // max period deviation for dominant-family fallback
	minFrameSeparationFraction    = 0.5      // minimum frame separation as fraction of period
	centroidHistoryMinPerICAO     = 30       // lower bound on retained centroids per ICAO
	centroidHistoryHardMaxPerICAO = 480      // hard ceiling on retained centroids per ICAO
)

// liveFrame is an in-progress sweep frame.
type liveFrame struct {
	refICAO      uint32
	refLat       float64
	refLon       float64
	refArrivalUS float64
	refPosAgeS   float64
	nAircraft    int // reference counts as 1
	observations []protocol.FrameObservation
	seenICAOs    map[uint32]struct{}
}

// DiagnosticsSnapshot is a point-in-time view of frame accumulator gates/state.
type DiagnosticsSnapshot struct {
	FrameIndex                  uint32
	OpenFrame                   bool
	OpenFrameRefICAO            uint32
	OpenFrameNAircraft          int
	OpenFrameNObs               int
	LastFrameStartUS            float64
	LastGateReason              string
	LastGateICAO                uint32
	GateCounts                  map[string]uint64
	CentroidHistoryICAOCount    int
	CentroidHistoryTotal        int
	CentroidHistoryMaxPerICAO   int
	CentroidHistoryCapPerICAO   int
	CentroidHistoryCapHit       bool
	CentroidHistoryCapHitsTotal uint64
}

// Accumulator manages live frame state for one IID.
type Accumulator struct {
	iidNum           uint8
	writer           *output.Writer
	positions        *iid.PositionCache
	frameIndex       uint32
	frame            *liveFrame
	lastFrameStartUS float64
	// Optional callback invoked after each FRAME_READY emission.
	OnFrameEmitted func(*protocol.FrameReady)
	// Per-ICAO centroid history for phase-family checks.
	centroidHistory        map[uint32][]float64
	centroidHistoryCapHits uint64
	gateCounts             map[string]uint64
	lastGateReason         string
	lastGateICAO           uint32
	ObserveStage           func(stage string, dur time.Duration)
}

// New returns an Accumulator for the given IID.
func New(iidNum uint8, writer *output.Writer, positions *iid.PositionCache) *Accumulator {
	return &Accumulator{
		iidNum:          iidNum,
		writer:          writer,
		positions:       positions,
		centroidHistory: make(map[uint32][]float64),
		gateCounts:      make(map[string]uint64),
	}
}

// Reset clears all frame state (called on RESET_IID).
func (a *Accumulator) Reset() {
	a.frame = nil
	a.lastFrameStartUS = 0
	a.centroidHistory = make(map[uint32][]float64)
	a.centroidHistoryCapHits = 0
	a.gateCounts = make(map[string]uint64)
	a.lastGateReason = ""
	a.lastGateICAO = 0
}

// OnBurst processes one fired burst. state provides the current period,
// reference ICAO, and family membership.
func (a *Accumulator) OnBurst(
	icao uint32,
	centroidUS float64,
	nReplies int,
	sigPtr *float64,
	state *iid.IIDState,
) {
	// Append to centroid history first — used by phase-family check.
	a.appendCentroid(icao, centroidUS)

	status, _, _, _ := state.Snapshot()
	_ = status
	periodSPtr := state.OperationalPeriodSnapshot()
	if periodSPtr == nil {
		a.recordGate("no_df_base_period", icao)
		return // no DF-authoritative base period — can't accumulate frames
	}
	periodS := *periodSPtr
	periodUS := periodS * 1_000_000.0

	_, refICAOPtr := state.SyncSnapshot()
	if refICAOPtr == nil {
		a.recordGate("no_reference", icao)
		return // no reference aircraft selected yet
	}
	refICAO := *refICAOPtr

	family := state.FamilySnapshot()

	// ---- Frame timeout: close if this burst is past the sweep window ----
	if a.frame != nil && centroidUS >= a.frame.refArrivalUS+periodUS {
		a.finalizeFrame(periodS)
		a.frame = nil
	}

	if icao == refICAO {
		// Reference aircraft burst — try to open a new frame.
		if a.frame != nil {
			return // frame already open; don't restart mid-sweep
		}
		tDominant := time.Now()
		matchesDominant := a.matchesDominant(icao, periodS, family)
		if a.ObserveStage != nil {
			a.ObserveStage("dominant_check", time.Since(tDominant))
		}
		if !matchesDominant {
			a.recordGate("ref_not_dominant", icao)
			return
		}
		// Suppress frames that start too close to the previous one.
		if a.shouldSuppressStart(centroidUS, periodUS) {
			a.recordGate("start_suppressed", icao)
			return
		}
		pos := a.positions.Get(icao)
		if pos == nil {
			a.frame = nil
			a.recordGate("missing_ref_position", icao)
			return // can't start frame without ref position
		}
		refPosAgeS := float64(time.Since(pos.TS).Seconds())

		tMutation := time.Now()
		a.frame = &liveFrame{
			refICAO:      icao,
			refLat:       pos.Lat,
			refLon:       pos.Lon,
			refArrivalUS: centroidUS,
			refPosAgeS:   refPosAgeS,
			nAircraft:    1,
			seenICAOs:    map[uint32]struct{}{icao: {}},
		}
		if a.ObserveStage != nil {
			a.ObserveStage("frame_mutation", time.Since(tMutation))
		}
		a.lastFrameStartUS = centroidUS
		return
	}

	// Non-reference burst — try to add as observation.
	if a.frame == nil {
		a.recordGate("no_open_frame", icao)
		return
	}
	if centroidUS < a.frame.refArrivalUS {
		a.recordGate("predates_frame", icao)
		return // burst predates this frame
	}
	if centroidUS >= a.frame.refArrivalUS+periodUS {
		a.recordGate("past_frame_window", icao)
		return // already past sweep window (would have been caught above)
	}
	if _, seen := a.frame.seenICAOs[icao]; seen {
		a.recordGate("duplicate_icao", icao)
		return // one observation per aircraft per frame
	}
	tDominant := time.Now()
	matchesDominant := a.matchesDominant(icao, periodS, family)
	if a.ObserveStage != nil {
		a.ObserveStage("dominant_check", time.Since(tDominant))
	}
	if !matchesDominant {
		a.recordGate("obs_not_dominant", icao)
		return
	}
	tPhase := time.Now()
	matchesPhase := a.matchesPhaseFamily(refICAO, a.frame.refArrivalUS, icao, centroidUS, periodUS)
	if a.ObserveStage != nil {
		a.ObserveStage("phase_check", time.Since(tPhase))
	}
	if !matchesPhase {
		a.recordGate("phase_mismatch", icao)
		return
	}
	pos := a.positions.Get(icao)
	if pos == nil {
		a.recordGate("missing_obs_position", icao)
		return // no position for this aircraft
	}

	posAgeS := float32(time.Since(pos.TS).Seconds())

	nR := nReplies
	if nR > 255 {
		nR = 255
	}
	tMutation := time.Now()
	a.frame.observations = append(a.frame.observations, protocol.FrameObservation{
		ICAO:      icao,
		Lat:       pos.Lat,
		Lon:       pos.Lon,
		ArrivalUS: centroidUS,
		NReplies:  uint8(nR),
		PosAgeS:   posAgeS,
	})
	a.frame.seenICAOs[icao] = struct{}{}
	a.frame.nAircraft++
	if a.ObserveStage != nil {
		a.ObserveStage("frame_mutation", time.Since(tMutation))
	}
}

// finalizeFrame closes the current frame and emits FRAME_READY if sufficient.
func (a *Accumulator) finalizeFrame(periodS float64) {
	tFinalize := time.Now()
	defer func() {
		if a.ObserveStage != nil {
			a.ObserveStage("frame_finalisation", time.Since(tFinalize))
		}
	}()
	f := a.frame
	if f == nil {
		return
	}
	nAircraft := f.nAircraft
	if nAircraft < 3 {
		a.recordGate("insufficient_aircraft", f.refICAO)
		return // insufficient — discard without emitting
	}
	quality := "marginal"
	if nAircraft >= 4 {
		quality = "good"
	}

	a.frameIndex++
	a.recordGate("frame_emitted", f.refICAO)
	slog.Info("radar-core: FRAME_READY emitted",
		"iid", a.iidNum,
		"frame", a.frameIndex,
		"quality", quality,
		"n_aircraft", nAircraft,
		"ref_icao", f.refICAO,
	)
	refPosAgeF32 := float32(f.refPosAgeS)
	msg := &protocol.FrameReady{
		MsgType:      protocol.MsgFrameReady,
		IID:          a.iidNum,
		FrameIndex:   a.frameIndex,
		PeriodS:      periodS,
		RefICAO:      f.refICAO,
		RefLat:       f.refLat,
		RefLon:       f.refLon,
		RefArrivalUS: f.refArrivalUS,
		RefPosAgeS:   &refPosAgeF32,
		Observations: f.observations,
		Quality:      quality,
	}
	a.writer.SendFrameReady(msg)
	if a.OnFrameEmitted != nil {
		a.OnFrameEmitted(msg)
	}
}

// appendCentroid adds a centroid to the per-ICAO history, capping at max size.
func (a *Accumulator) appendCentroid(icao uint32, centroidUS float64) {
	hist := a.centroidHistory[icao]
	hist = append(hist, centroidUS)
	capPerICAO := a.centroidHistoryCapPerICAO()
	if len(hist) > capPerICAO {
		dropped := len(hist) - capPerICAO
		hist = hist[dropped:]
		a.centroidHistoryCapHits += uint64(dropped)
	}
	a.centroidHistory[icao] = hist
}

func (a *Accumulator) centroidHistoryCapPerICAO() int {
	cfgCap := rcconfig.Get().CentroidHistoryMaxPerICAO
	if cfgCap < centroidHistoryMinPerICAO {
		return centroidHistoryMinPerICAO
	}
	if cfgCap > centroidHistoryHardMaxPerICAO {
		return centroidHistoryHardMaxPerICAO
	}
	return cfgCap
}

// matchesDominant checks whether an ICAO belongs to the dominant period family.
// Falls back to burst-history-based check when family snapshot is not yet available.
func (a *Accumulator) matchesDominant(icao uint32, periodS float64, family *iid.ICAOFamily) bool {
	if family != nil {
		if _, ok := family.FoldedICAOs[icao]; ok {
			return true
		}
		if _, ok := family.ResidualICAOs[icao]; ok {
			return false
		}
		// ICAO not in either set — fall through to history check.
	}
	// Fallback: check burst history median period.
	hist := a.centroidHistory[icao]
	if len(hist) < 4 {
		return false // insufficient history — conservative reject
	}
	sorted := sortedCopy(hist)
	var intervalsS []float64
	for i := 0; i < len(sorted)-1; i++ {
		iv := (sorted[i+1] - sorted[i]) / 1_000_000.0
		if iv > 0.5 && iv < 30.0 {
			intervalsS = append(intervalsS, iv)
		}
	}
	if len(intervalsS) < 2 {
		return false
	}
	med := median(intervalsS)
	return math.Abs(med-periodS)/periodS <= periodMatchTolerance
}

// matchesPhaseFamily checks that icao's burst is consistent with being in the
// same sweep as refICAO at refArrivalUS. Port of _burst_matches_reference_phase_family().
func (a *Accumulator) matchesPhaseFamily(refICAO uint32, refArrivalUS float64, icao uint32, burstUS, periodUS float64) bool {
	if icao == refICAO || periodUS <= 0 {
		return true
	}
	refHist := a.centroidHistory[refICAO]
	icaoHist := a.centroidHistory[icao]
	if len(refHist) == 0 || len(icaoHist) == 0 {
		return true // insufficient history — optimistic accept
	}

	toleranceUS := math.Max(coSweepWindowUS, periodUS*phaseFamilyToleranceFraction)

	var priorRefs []float64
	for _, c := range refHist {
		if c < refArrivalUS {
			priorRefs = append(priorRefs, c)
		}
	}
	var priorObs []float64
	for _, c := range icaoHist {
		if c < burstUS {
			priorObs = append(priorObs, c)
		}
	}
	if len(priorRefs) == 0 || len(priorObs) == 0 {
		return true
	}

	var offsets []float64
	for _, obs := range priorObs {
		nearest := nearestInSlice(priorRefs, obs)
		offset := math.Mod(obs-nearest, periodUS)
		if offset < 0 {
			offset += periodUS
		}
		offsets = append(offsets, offset)
	}
	if len(offsets) < phaseFamilyHistoryMin {
		return true
	}

	expectedOffset := median(offsets)
	observedOffset := math.Mod(burstUS-refArrivalUS, periodUS)
	if observedOffset < 0 {
		observedOffset += periodUS
	}
	circularDelta := math.Min(
		math.Abs(observedOffset-expectedOffset),
		periodUS-math.Abs(observedOffset-expectedOffset),
	)
	return circularDelta <= toleranceUS
}

// shouldSuppressStart returns true if a new frame would start too soon
// after the previous one (prevents back-to-back duplicates).
func (a *Accumulator) shouldSuppressStart(startUS, periodUS float64) bool {
	if a.lastFrameStartUS == 0 {
		return false
	}
	return (startUS - a.lastFrameStartUS) < periodUS*minFrameSeparationFraction
}

func (a *Accumulator) recordGate(reason string, icao uint32) {
	a.gateCounts[reason]++
	a.lastGateReason = reason
	a.lastGateICAO = icao
	count := a.gateCounts[reason]
	if count == 1 || count%1000 == 0 {
		slog.Info("radar-core: frame accumulator gate",
			"iid", a.iidNum,
			"reason", reason,
			"count", count,
			"icao", icao,
		)
	}
}

// Diagnostics returns a copy of the current gate counters and frame state.
// Called from the ingest goroutine (same as OnBurst), so no extra locking is needed.
func (a *Accumulator) Diagnostics() DiagnosticsSnapshot {
	diag := DiagnosticsSnapshot{
		FrameIndex:       a.frameIndex,
		OpenFrame:        a.frame != nil,
		LastFrameStartUS: a.lastFrameStartUS,
		LastGateReason:   a.lastGateReason,
		LastGateICAO:     a.lastGateICAO,
		GateCounts:       make(map[string]uint64, len(a.gateCounts)),
	}
	if a.frame != nil {
		diag.OpenFrameRefICAO = a.frame.refICAO
		diag.OpenFrameNAircraft = a.frame.nAircraft
		diag.OpenFrameNObs = len(a.frame.observations)
	}
	diag.CentroidHistoryCapPerICAO = a.centroidHistoryCapPerICAO()
	diag.CentroidHistoryICAOCount = len(a.centroidHistory)
	diag.CentroidHistoryCapHitsTotal = a.centroidHistoryCapHits
	for _, hist := range a.centroidHistory {
		n := len(hist)
		diag.CentroidHistoryTotal += n
		if n > diag.CentroidHistoryMaxPerICAO {
			diag.CentroidHistoryMaxPerICAO = n
		}
	}
	diag.CentroidHistoryCapHit = diag.CentroidHistoryMaxPerICAO >= diag.CentroidHistoryCapPerICAO
	for k, v := range a.gateCounts {
		diag.GateCounts[k] = v
	}
	return diag
}

// --- small helpers ---

func nearestInSlice(vals []float64, target float64) float64 {
	best := vals[0]
	bestDist := math.Abs(target - best)
	for _, v := range vals[1:] {
		d := math.Abs(target - v)
		if d < bestDist {
			bestDist = d
			best = v
		}
	}
	return best
}

func sortedCopy(vals []float64) []float64 {
	cp := make([]float64, len(vals))
	copy(cp, vals)
	// Simple insertion sort — lists are short (bounded centroid history).
	for i := 1; i < len(cp); i++ {
		v := cp[i]
		j := i - 1
		for j >= 0 && cp[j] > v {
			cp[j+1] = cp[j]
			j--
		}
		cp[j+1] = v
	}
	return cp
}

func median(vals []float64) float64 {
	n := len(vals)
	if n == 0 {
		return 0
	}
	cp := sortedCopy(vals)
	if n%2 == 0 {
		return (cp[n/2-1] + cp[n/2]) / 2
	}
	return cp[n/2]
}
