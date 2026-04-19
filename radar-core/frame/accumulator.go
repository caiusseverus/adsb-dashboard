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
	"math"

	"github.com/caiusseverus/adsb-dashboard/radar-core/iid"
	"github.com/caiusseverus/adsb-dashboard/radar-core/output"
	"github.com/caiusseverus/adsb-dashboard/radar-core/protocol"
)

// Accumulator constants — match sweep.py.
const (
	coSweepWindowUS             = 70_000.0  // max offset for a co-sweep burst
	phaseFamilyToleranceFraction = 0.15     // period fraction used when coSweepWindow is narrower
	phaseFamilyHistoryMin        = 2        // minimum historical offsets for phase check
	periodMatchTolerance         = 0.15     // max period deviation for dominant-family fallback
	minFrameSeparationFraction   = 0.5      // minimum frame separation as fraction of period
	centroidHistoryMax           = 30       // max centroid timestamps kept per ICAO
)

// liveFrame is an in-progress sweep frame.
type liveFrame struct {
	refICAO      uint32
	refLat       float64
	refLon       float64
	refArrivalUS float64
	refPosAgeS   float64
	nAircraft    int       // reference counts as 1
	observations []protocol.FrameObservation
	seenICAOs    map[uint32]struct{}
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
	OnFrameEmitted func()
	// Per-ICAO centroid history for phase-family checks.
	centroidHistory  map[uint32][]float64
}

// New returns an Accumulator for the given IID.
func New(iidNum uint8, writer *output.Writer, positions *iid.PositionCache) *Accumulator {
	return &Accumulator{
		iidNum:          iidNum,
		writer:          writer,
		positions:       positions,
		centroidHistory: make(map[uint32][]float64),
	}
}

// Reset clears all frame state (called on RESET_IID).
func (a *Accumulator) Reset() {
	a.frame = nil
	a.lastFrameStartUS = 0
	a.centroidHistory = make(map[uint32][]float64)
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

	status, periodSPtr, _, _ := state.Snapshot()
	_ = status
	if periodSPtr == nil {
		return // no period estimate yet — can't accumulate frames
	}
	periodS := *periodSPtr
	periodUS := periodS * 1_000_000.0

	_, refICAOPtr := state.SyncSnapshot()
	if refICAOPtr == nil {
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
		if !a.matchesDominant(icao, periodS, family) {
			return
		}
		// Suppress frames that start too close to the previous one.
		if a.shouldSuppressStart(centroidUS, periodUS) {
			return
		}
		pos := a.positions.Get(icao)
		if pos == nil {
			a.frame = nil
			return // can't start frame without ref position
		}
		posAgeS := float64(0)
		// a.positions.Get already filters stale; age is always <= 30s
		_ = posAgeS

		a.frame = &liveFrame{
			refICAO:      icao,
			refLat:       pos.Lat,
			refLon:       pos.Lon,
			refArrivalUS: centroidUS,
			nAircraft:    1,
			seenICAOs:    map[uint32]struct{}{icao: {}},
		}
		a.lastFrameStartUS = centroidUS
		return
	}

	// Non-reference burst — try to add as observation.
	if a.frame == nil {
		return
	}
	if centroidUS < a.frame.refArrivalUS {
		return // burst predates this frame
	}
	if centroidUS >= a.frame.refArrivalUS+periodUS {
		return // already past sweep window (would have been caught above)
	}
	if _, seen := a.frame.seenICAOs[icao]; seen {
		return // one observation per aircraft per frame
	}
	if !a.matchesDominant(icao, periodS, family) {
		return
	}
	if !a.matchesPhaseFamily(refICAO, a.frame.refArrivalUS, icao, centroidUS, periodUS) {
		return
	}
	pos := a.positions.Get(icao)
	if pos == nil {
		return // no position for this aircraft
	}

	var posAgeS float32
	// pos is fresh (Get() filters stale entries); use 0.0 as the age upper bound.

	nR := nReplies
	if nR > 255 {
		nR = 255
	}
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
}

// finalizeFrame closes the current frame and emits FRAME_READY if sufficient.
func (a *Accumulator) finalizeFrame(periodS float64) {
	f := a.frame
	if f == nil {
		return
	}
	nAircraft := f.nAircraft
	if nAircraft < 3 {
		return // insufficient — discard without emitting
	}
	quality := "marginal"
	if nAircraft >= 4 {
		quality = "good"
	}

	a.frameIndex++
	if a.OnFrameEmitted != nil {
		a.OnFrameEmitted()
	}
	a.writer.SendFrameReady(&protocol.FrameReady{
		MsgType:      protocol.MsgFrameReady,
		IID:          a.iidNum,
		FrameIndex:   a.frameIndex,
		PeriodS:      periodS,
		RefICAO:      f.refICAO,
		RefLat:       f.refLat,
		RefLon:       f.refLon,
		RefArrivalUS: f.refArrivalUS,
		Observations: f.observations,
		Quality:      quality,
	})
}

// appendCentroid adds a centroid to the per-ICAO history, capping at max size.
func (a *Accumulator) appendCentroid(icao uint32, centroidUS float64) {
	hist := a.centroidHistory[icao]
	hist = append(hist, centroidUS)
	if len(hist) > centroidHistoryMax {
		hist = hist[len(hist)-centroidHistoryMax:]
	}
	a.centroidHistory[icao] = hist
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
	// Simple insertion sort — lists are short (max 30).
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
