package iid

// multisync.go — per-IID multi-aircraft sync refinement solver.
//
// This is a Go port of the Python _update_multi_aircraft_sync_state() path in
// backend/radar/sweep.py.  It runs entirely in Go using burst evidence that is
// already available from the BurstFired hot path, so Python no longer needs to
// maintain the aligned-burst observation buffer or run the solver on every
// throttled interval.
//
// Key design decisions:
//   - Propagation delay correction included (same formula as Python).
//   - Waveform correction NOT included; Python waveform bins remain authoritative
//     for Stage 3 and are applied after reading the Go sync state.
//   - Motion compensation NOT included; deferred for a later slice.
//   - Anchor selection: highest-quality ICAO with good recent observations.
//   - Period refinement: slope EMA + persistence gate (matches Python).
//   - Wrong-period reacquire: state machine matching Python's reacquire logic.

import (
	"math"
	"sort"
	"sync"
	"time"
)

const (
	// Speed of light propagation constant: μs per nautical mile (one-way).
	uSPerNMLight = 1852.0 / 299792458.0 * 1e6

	// Observation retention window.
	multiSyncRetentionS = 360.0
	multiSyncMaxObs     = 6000

	// Minimum number of observations to attempt a fit.
	multiSyncMinObs = 3

	// Fit window: cover this many rotations (minimum 30 s).
	multiSyncWindowRotations = 6.0
	multiSyncWindowMinS      = 30.0

	// Residual thresholds (degrees).
	residualInlierDeg  = 20.0
	residualSoftDeg2   = 50.0 // renamed to avoid clash with sync.go's residualSoftDeg
	residualRejectGate = 35.0 // fit gate (stricter than soft threshold)
	residualWrapDeg    = 150.0

	// Gain for phase correction.
	multiSyncGainBase = 0.12
	multiSyncGainMax  = 0.20

	// Period refinement constants (match Python).
	slopeEMAAlpha         = 0.08
	slopeDeadBand         = 0.08  // deg/s — must exceed to drive period change
	persistMinEntries     = 5
	periodPPMPerUpdate    = 60.0
	periodPPMFromBase     = 2000.0
	periodPPMStrong       = 400.0
	periodPPMBaseStrong   = 15000.0
	periodRefineMinInlier = 6
	periodRefineMinSpanRot = 2.0
	periodGain            = 0.12

	// Trust promotion thresholds — the refined solver must earn a stable run streak
	// before its period is promoted to the trusted base.  Until then the clamp is
	// applied loosely against the bootstrap (compact-sync) seed.
	trustMinStreak      = 8    // consecutive updates required to promote the period
	trustMinFitPool     = 8    // min fit-pool entries per update to count toward streak
	trustMinICAOs       = 3    // min distinct ICAOs per update to count toward streak
	trustMaxResidualDeg = 8.0  // detrended MAD ceiling for a trust-eligible update (deg)
	trustMaxRejFrac     = 0.15 // rejection fraction ceiling for a trust-eligible update

	// Period clamp limits during the untrusted bootstrap phase.
	// Wider than the trusted-base limits so the solver can escape an incorrect seed family.
	periodPPMFromBootstrap       = 10000.0 // ±1% from bootstrap — bootstrap phase wide gate
	periodPPMFromBootstrapStrong = 30000.0 // ±3% under strong fit in bootstrap phase

	// Reacquire / failure detection.
	reacquireMADThreshold = 8.0 // deg — detrended MAD for recovery
	reacquireMinClean     = 2   // consecutive clean updates to exit reacquire

	// ICAO quality memory.
	icaoQualityMADAlpha  = 0.15
	icaoQualityRejectMAD = 20.0 // deg — ICAO gets rejected from fit above this
	icaoQualityWarnMAD   = 12.0

	// Throttle: minimum interval between solver runs (matches Python 250 ms).
	multiSyncMinIntervalS = 0.25
)

// MultiSyncObs is one sync-eligible burst observation buffered by the solver.
type MultiSyncObs struct {
	CentroidUS float64
	ICAO       uint32
	BearingDeg float64
	RangeNM    float32
	PosAgeS    float32
	NReplies   int
	SignalDBFS *float32 // nil when unknown
	WallTS     float64  // Unix wall-clock seconds
}

// ICAOSyncQuality tracks residual quality for one ICAO.
type ICAOSyncQuality struct {
	ResidualMADDeg float64
	NObservations  int
	LastUpdateTS   float64
}

// scoredObs is an internal working struct for one scored observation.
type scoredObs struct {
	o               MultiSyncObs
	residual        float64
	effectiveUS     float64
	phaseInRot      float64
	baseW           float64
	effectiveW      float64
	status          string
	fitEligible     bool
	fitRejectReason string
}

// candidateEvalResult holds the scoring result for one candidate period
// evaluated during wrong-period reacquire search.
type candidateEvalResult struct {
	periodS     float64
	inlierCount int
	icaoCount   int
	madDeg      float64
	rejFrac     float64
	score       float64
}

// MultiSyncSnapshot is a point-in-time view for protocol emission.
type MultiSyncSnapshot struct {
	Present               bool
	Usable                bool
	PeriodS               float64
	PeriodBaseS           float64
	PhaseEpochUS          float64
	PhaseOffsetDeg        float64
	JitterDeg             float64
	ResidualEMADeg        float64
	NSyncUpdates          int
	Holdover              bool
	LastUpdated           float64
	PeriodReacquireActive bool
	PeriodReacquireReason string
	AnchorICAO            *uint32
	AnchorPhaseDeg        float64
	AnchorScore           float64
	// Bootstrap / trust diagnostics.
	BootstrapPeriodS        float64 // compact-sync seed captured at first run
	TrustedBasePeriodS      float64 // promoted from refined period after trustMinStreak updates; 0=not yet trusted
	TrustUpdateStreak       int     // consecutive updates meeting trust criteria
	BaseClamped             bool    // true if the base-period clamp fired on the last run
	BaseClampDiffPPM        float64 // raw PPM deviation that triggered (or would have triggered) the clamp
	WrongPeriodSuspect      bool    // true if wrong-period suspicion was raised on the last run
	ReacquireCandidatePeriod float64 // period chosen by candidate search during reacquire (0 if not active)
	ReacquireCandidateScore  float64 // score of that candidate
}

// MultiSyncSolver holds per-IID multi-aircraft sync refinement state.
type MultiSyncSolver struct {
	mu sync.Mutex

	iid uint8

	// Observation buffer (bounded, time-windowed).
	obs []MultiSyncObs

	// Published sync state.
	Present               bool
	Usable                bool
	PeriodS               float64
	PeriodBaseS           float64
	PhaseEpochUS          float64
	PhaseOffsetDeg        float64
	JitterDeg             float64
	ResidualEMADeg        float64
	NSyncUpdates          int
	Holdover              bool
	LastUpdated           float64

	// Period refinement.
	SmoothSlopeDegPerS    float64
	SlopeHistory          []float64
	PeriodReacquireActive bool
	PeriodReacquireReason string
	PeriodFailureStreak   int
	CleanReacquireStreak  int

	// Anchor.
	AnchorICAO     *uint32
	AnchorPhaseDeg float64
	AnchorScore    float64

	// Per-ICAO quality memory.
	ICAOQuality map[uint32]*ICAOSyncQuality

	// Bootstrap vs trusted base separation.
	// BootstrapPeriodS is the compact-sync period captured on first run and never overwritten.
	// TrustedBasePeriodS is promoted from the refined period after trustMinStreak consistent
	// multi-aircraft updates.  Until trust is established, the clamp is applied loosely
	// against BootstrapPeriodS so the solver can escape an incorrect seed family.
	BootstrapPeriodS  float64
	TrustedBasePeriodS float64
	TrustUpdateStreak int

	// Per-run diagnostics (updated each solver run, readable via Snapshot).
	LastBaseClamped          bool    // true if base-period clamp fired on the last run
	LastBaseClampDiffPPM     float64 // raw PPM deviation that triggered (or would have triggered) the clamp
	LastWrongPeriodSuspect   bool    // true if wrong-period suspicion was raised on the last run
	LastReacquireCandidateP  float64 // period chosen by candidate search during reacquire (0 if not active)
	LastReacquireCandidateSc float64 // score of that candidate period

	// Throttle.
	lastRunTS float64
}

// NewMultiSyncSolver creates an empty solver for the given IID.
func NewMultiSyncSolver(iid uint8) *MultiSyncSolver {
	return &MultiSyncSolver{
		iid:         iid,
		ICAOQuality: make(map[uint32]*ICAOSyncQuality),
	}
}

// AddObs appends a sync-eligible burst observation and prunes stale entries.
func (ms *MultiSyncSolver) AddObs(o MultiSyncObs) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.obs = append(ms.obs, o)
	ms.pruneObs()
}

func (ms *MultiSyncSolver) pruneObs() {
	if len(ms.obs) == 0 {
		return
	}
	cutoff := ms.obs[len(ms.obs)-1].WallTS - multiSyncRetentionS
	i := sort.Search(len(ms.obs), func(j int) bool { return ms.obs[j].WallTS >= cutoff })
	if i > 0 {
		ms.obs = ms.obs[i:]
	}
	if len(ms.obs) > multiSyncMaxObs {
		ms.obs = ms.obs[len(ms.obs)-multiSyncMaxObs:]
	}
}

// TryUpdate attempts to run the solver if the throttle interval has elapsed.
// sync is the current compact frame-sync state for this IID (provides the seed
// period/phase when no multi-sync state exists yet).  Returns true if a run occurred.
func (ms *MultiSyncSolver) TryUpdate(sync *SyncState) bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	now := float64(time.Now().UnixMicro()) / 1e6
	if now-ms.lastRunTS < multiSyncMinIntervalS {
		return false
	}
	ms.lastRunTS = now
	ms.runFit(sync, now)
	return true
}

// Reset clears all state for this IID.
func (ms *MultiSyncSolver) Reset() {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.obs = ms.obs[:0]
	ms.Present = false
	ms.Usable = false
	ms.PeriodS = 0
	ms.PeriodBaseS = 0
	ms.PhaseEpochUS = 0
	ms.PhaseOffsetDeg = 0
	ms.JitterDeg = 0
	ms.ResidualEMADeg = 0
	ms.NSyncUpdates = 0
	ms.Holdover = false
	ms.LastUpdated = 0
	ms.SmoothSlopeDegPerS = 0
	ms.SlopeHistory = nil
	ms.PeriodReacquireActive = false
	ms.PeriodReacquireReason = ""
	ms.PeriodFailureStreak = 0
	ms.CleanReacquireStreak = 0
	ms.AnchorICAO = nil
	ms.AnchorPhaseDeg = 0
	ms.AnchorScore = 0
	for k := range ms.ICAOQuality {
		delete(ms.ICAOQuality, k)
	}
	ms.BootstrapPeriodS = 0
	ms.TrustedBasePeriodS = 0
	ms.TrustUpdateStreak = 0
	ms.LastBaseClamped = false
	ms.LastBaseClampDiffPPM = 0
	ms.LastWrongPeriodSuspect = false
	ms.LastReacquireCandidateP = 0
	ms.LastReacquireCandidateSc = 0
}

// Snapshot returns a copy of the published state for protocol emission.
func (ms *MultiSyncSolver) Snapshot() MultiSyncSnapshot {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	snap := MultiSyncSnapshot{
		Present:               ms.Present,
		Usable:                ms.Usable,
		PeriodS:               ms.PeriodS,
		PeriodBaseS:           ms.PeriodBaseS,
		PhaseEpochUS:          ms.PhaseEpochUS,
		PhaseOffsetDeg:        ms.PhaseOffsetDeg,
		JitterDeg:             ms.JitterDeg,
		ResidualEMADeg:        ms.ResidualEMADeg,
		NSyncUpdates:          ms.NSyncUpdates,
		Holdover:              ms.Holdover,
		LastUpdated:           ms.LastUpdated,
		PeriodReacquireActive: ms.PeriodReacquireActive,
		PeriodReacquireReason: ms.PeriodReacquireReason,
		// Bootstrap / trust diagnostics.
		BootstrapPeriodS:        ms.BootstrapPeriodS,
		TrustedBasePeriodS:      ms.TrustedBasePeriodS,
		TrustUpdateStreak:       ms.TrustUpdateStreak,
		BaseClamped:             ms.LastBaseClamped,
		BaseClampDiffPPM:        ms.LastBaseClampDiffPPM,
		WrongPeriodSuspect:      ms.LastWrongPeriodSuspect,
		ReacquireCandidatePeriod: ms.LastReacquireCandidateP,
		ReacquireCandidateScore:  ms.LastReacquireCandidateSc,
	}
	if ms.AnchorICAO != nil {
		v := *ms.AnchorICAO
		snap.AnchorICAO = &v
		snap.AnchorPhaseDeg = ms.AnchorPhaseDeg
		snap.AnchorScore = ms.AnchorScore
	}
	return snap
}

// ─── internal solver ──────────────────────────────────────────────────────────

func (ms *MultiSyncSolver) runFit(sync *SyncState, nowUnix float64) {
	// Determine seed period/phase from existing multi-sync state or frame sync.
	var seedPeriodS, seedEpochUS, seedOffsetDeg float64
	if ms.Present && ms.PeriodS > 0 {
		seedPeriodS = ms.PeriodS
		seedEpochUS = ms.PhaseEpochUS
		seedOffsetDeg = ms.PhaseOffsetDeg
	} else if sync != nil && sync.PeriodS > 0 {
		seedPeriodS = sync.PeriodS
		seedEpochUS = sync.PhaseEpochUS
		seedOffsetDeg = sync.PhaseOffsetDeg
	} else {
		return // no seed available yet
	}

	// Record the compact-sync bootstrap period on the very first run.  This is never
	// overwritten so the solver always knows what family it started in, even after the
	// refined period has diverged considerably from it.
	if ms.BootstrapPeriodS <= 0 {
		if ms.Present && ms.PeriodS > 0 {
			// First run after a live restart — treat the existing refined period as bootstrap.
			ms.BootstrapPeriodS = ms.PeriodS
		} else if sync != nil && sync.PeriodS > 0 {
			ms.BootstrapPeriodS = sync.PeriodS
		}
	}

	// Determine the effective clamp base and whether it is trusted.
	// TrustedBasePeriodS is promoted from the refined period only after trustMinStreak
	// consistent multi-aircraft updates; until then we clamp loosely against
	// BootstrapPeriodS so the solver can escape an incorrect seed family.
	trusted := ms.TrustedBasePeriodS > 0
	basePeriodS := ms.TrustedBasePeriodS
	if basePeriodS <= 0 {
		basePeriodS = ms.BootstrapPeriodS
		if basePeriodS <= 0 {
			basePeriodS = seedPeriodS
		}
	}
	livePeriodS := seedPeriodS
	periodUS := livePeriodS * 1e6

	// Rolling observation window.
	windowS := math.Max(livePeriodS*multiSyncWindowRotations, multiSyncWindowMinS)
	cutoffTS := nowUnix - windowS
	var recent []MultiSyncObs
	for _, o := range ms.obs {
		if o.WallTS >= cutoffTS {
			recent = append(recent, o)
		}
	}
	if len(recent) < multiSyncMinObs {
		ms.Holdover = true
		return
	}

	// Score each observation.
	scored := make([]scoredObs, 0, len(recent))
	for _, o := range recent {
		effectiveUS := msPropCorrectedUS(float64(o.CentroidUS), float64(o.RangeNM))
		predicted := msPredictBearing(seedEpochUS, seedOffsetDeg, periodUS, effectiveUS)
		phaseInRot := math.Mod((effectiveUS-seedEpochUS)/periodUS*360.0, 360.0)
		if phaseInRot < 0 {
			phaseInRot += 360.0
		}
		residual := circularDiff(o.BearingDeg, predicted)
		absR := math.Abs(residual)
		status := msClassifyResidual(absR)
		baseW := msScoreObs(o)
		qEntry := ms.ICAOQuality[o.ICAO]
		qMult := msICAOQualityMult(qEntry)
		var effectiveW float64
		switch status {
		case "inlier":
			effectiveW = baseW * qMult
		case "soft":
			effectiveW = baseW * 0.2 * qMult
		}
		fitRejectReason := msFitRejectReason(o, status, absR, qEntry)
		scored = append(scored, scoredObs{
			o:               o,
			residual:        residual,
			effectiveUS:     effectiveUS,
			phaseInRot:      phaseInRot,
			baseW:           baseW,
			effectiveW:      effectiveW,
			status:          status,
			fitEligible:     fitRejectReason == "",
			fitRejectReason: fitRejectReason,
		})
	}

	// Update ICAO quality memory from fit-eligible observations.
	ms.updateICAOQuality(scored)

	// Build fit pool.
	type fitEntry struct {
		effectiveUS float64
		residual    float64
		weight      float64
		icao        uint32
	}
	var fitPool []fitEntry
	for _, se := range scored {
		if se.fitEligible && se.effectiveW > 0 {
			fitPool = append(fitPool, fitEntry{se.effectiveUS, se.residual, se.effectiveW, se.o.ICAO})
		}
	}
	// Fallback to anchor-weight pool when no fit-eligible observations pass gates.
	if len(fitPool) == 0 {
		for _, se := range scored {
			if se.baseW > 0 && se.fitRejectReason != "near_wrap_residual" {
				fitPool = append(fitPool, fitEntry{
					se.effectiveUS, se.residual,
					se.baseW * msICAOQualityMult(ms.ICAOQuality[se.o.ICAO]),
					se.o.ICAO,
				})
			}
		}
	}
	if len(fitPool) == 0 {
		ms.Holdover = true
		ms.Usable = false
		return
	}

	nInliers := 0
	nRejected := 0
	for _, se := range scored {
		switch se.status {
		case "inlier":
			nInliers++
		case "rejected":
			nRejected++
		}
	}
	majorityRejected := nRejected >= len(recent)/2+1

	// Weighted linear fit: residual ~ a + b*(t - tRef).
	tRef := fitPool[0].effectiveUS
	for _, fe := range fitPool {
		if fe.effectiveUS < tRef {
			tRef = fe.effectiveUS
		}
	}
	tRef /= 1e6
	xs := make([]float64, len(fitPool))
	ys := make([]float64, len(fitPool))
	ws := make([]float64, len(fitPool))
	for i, fe := range fitPool {
		xs[i] = fe.effectiveUS/1e6 - tRef
		ys[i] = fe.residual
		ws[i] = fe.weight
	}
	aFit, bFit := weightedLinearFit(xs, ys, ws)

	// Conservative phase-correction gain.
	nEff := math.Max(float64(nInliers), 1.0)
	gain := math.Min(multiSyncGainBase+0.01*(nEff-1), multiSyncGainMax)

	// Advance epoch to most recent anchor-eligible observation.
	var newEpochUS float64
	for _, se := range scored {
		if se.baseW > 0 && se.fitRejectReason != "near_wrap_residual" {
			if se.effectiveUS > newEpochUS {
				newEpochUS = se.effectiveUS
			}
		}
	}
	if newEpochUS == 0 {
		newEpochUS = scored[len(scored)-1].effectiveUS
	}

	// Propagate existing model to new epoch (mixed fallback).
	existingAtNew := math.Mod((newEpochUS-seedEpochUS)/periodUS*360.0+seedOffsetDeg, 360.0)
	if existingAtNew < 0 {
		existingAtNew += 360.0
	}
	mixedFallback := math.Mod(existingAtNew+aFit*gain, 360.0)
	if mixedFallback < 0 {
		mixedFallback += 360.0
	}

	// Anchor selection.
	newOffset, anchorICAO, anchorScore, anchorPhaseDeg := ms.selectAnchor(
		scored, newEpochUS, periodUS, mixedFallback,
	)

	// Period refinement.
	spanS := 0.0
	for _, x := range xs {
		if x > spanS {
			spanS = x
		}
	}
	fitICAOs := make(map[uint32]bool)
	for _, fe := range fitPool {
		fitICAOs[fe.icao] = true
	}

	smoothedSlope := (1.0-slopeEMAAlpha)*ms.SmoothSlopeDegPerS + slopeEMAAlpha*bFit
	ms.SmoothSlopeDegPerS = smoothedSlope
	ms.SlopeHistory = append(ms.SlopeHistory, smoothedSlope)
	if len(ms.SlopeHistory) > 12 {
		ms.SlopeHistory = ms.SlopeHistory[len(ms.SlopeHistory)-12:]
	}

	refinedPeriodS, _, _, clampedByBase, clampDiffPPM := ms.refinePeriod(
		livePeriodS, basePeriodS, smoothedSlope,
		len(fitPool), len(fitICAOs), spanS, majorityRejected,
		trusted,
	)

	// Wrong-period detection.
	detrended := msDetrendedMAD(scored, aFit, bFit, tRef)
	wrongPeriodSuspect := ms.assessPeriodFailure(len(recent), nRejected, len(fitPool), len(fitICAOs), majorityRejected, detrended)
	if wrongPeriodSuspect {
		ms.PeriodFailureStreak++
	} else {
		ms.PeriodFailureStreak = 0
	}
	ms.LastWrongPeriodSuspect = wrongPeriodSuspect

	cleanUpdate := len(recent) > 0 &&
		float64(nRejected)/float64(len(recent)) < 0.25 &&
		len(fitPool) >= 6 &&
		len(fitICAOs) >= 2 &&
		detrended <= reacquireMADThreshold

	if ms.PeriodReacquireActive {
		if cleanUpdate {
			ms.CleanReacquireStreak++
			if ms.CleanReacquireStreak >= reacquireMinClean {
				ms.PeriodReacquireActive = false
				ms.PeriodReacquireReason = ""
				ms.PeriodFailureStreak = 0
			}
			// On a clean update, let the solver's refined period pass through rather than
			// holding the bootstrap base — the candidate search already guided us to a
			// plausible family and this update is confirming it.
		} else {
			ms.CleanReacquireStreak = 0
			// Search for the best-scoring period family instead of snapping back to the
			// bootstrap base, which may itself be the wrong family.
			bestP, bestSc := ms.searchBestCandidate(scored, seedEpochUS)
			ms.LastReacquireCandidateP = bestP
			ms.LastReacquireCandidateSc = bestSc
			refinedPeriodS = bestP
		}
	} else {
		ms.CleanReacquireStreak = 0
		if ms.PeriodFailureStreak >= 3 || (ms.PeriodFailureStreak >= 2 && wrongPeriodSuspect) {
			ms.PeriodReacquireActive = true
			ms.PeriodReacquireReason = "failure_streak"
			ms.TrustUpdateStreak = 0
			// On entering reacquire, search candidate periods immediately.  This allows the
			// solver to jump to the correct family rather than holding the incorrect bootstrap.
			bestP, bestSc := ms.searchBestCandidate(scored, seedEpochUS)
			ms.LastReacquireCandidateP = bestP
			ms.LastReacquireCandidateSc = bestSc
			refinedPeriodS = bestP
		} else {
			// Not in reacquire; clear stale candidate diagnostics.
			ms.LastReacquireCandidateP = 0
			ms.LastReacquireCandidateSc = 0
		}
	}

	// Trust promotion: once the solver has accumulated enough consistent multi-aircraft
	// evidence, promote the current refined period to TrustedBasePeriodS.  From that
	// point the clamp tightens against the trusted base rather than the bootstrap seed.
	trustedEnough := !majorityRejected &&
		len(recent) > 0 &&
		float64(nRejected)/float64(len(recent)) < trustMaxRejFrac &&
		len(fitPool) >= trustMinFitPool &&
		len(fitICAOs) >= trustMinICAOs &&
		detrended <= trustMaxResidualDeg

	if ms.PeriodReacquireActive || wrongPeriodSuspect {
		// Problem detected — reset trust streak; do not promote.
		ms.TrustUpdateStreak = 0
	} else if trustedEnough {
		ms.TrustUpdateStreak++
		if ms.TrustUpdateStreak >= trustMinStreak {
			if ms.TrustedBasePeriodS <= 0 {
				// First promotion: adopt the refined period as the trusted base.
				ms.TrustedBasePeriodS = refinedPeriodS
			} else {
				// Slow EMA keeps the trusted base tracking a genuinely stable family
				// without snapping to transient fluctuations.
				ms.TrustedBasePeriodS = 0.98*ms.TrustedBasePeriodS + 0.02*refinedPeriodS
			}
		}
	}

	// Capture per-run clamp diagnostics (after refinePeriod returned them).
	ms.LastBaseClamped = clampedByBase
	ms.LastBaseClampDiffPPM = clampDiffPPM

	// Residual EMA update from fit pool.
	var newResidualEMA float64
	if ms.ResidualEMADeg > 0 {
		newResidualEMA = ms.ResidualEMADeg
		for _, fe := range fitPool {
			absR := math.Abs(fe.residual)
			newResidualEMA = 0.85*newResidualEMA + 0.15*absR
		}
	} else {
		if len(fitPool) > 0 {
			sum := 0.0
			for _, fe := range fitPool {
				sum += math.Abs(fe.residual)
			}
			newResidualEMA = sum / float64(len(fitPool))
		} else {
			newResidualEMA = 5.0
		}
	}

	// Publish.
	ms.Present = true
	ms.Usable = len(fitPool) >= 4 && !majorityRejected
	ms.PeriodS = refinedPeriodS
	ms.PeriodBaseS = basePeriodS
	ms.PhaseEpochUS = newEpochUS
	ms.PhaseOffsetDeg = newOffset
	ms.JitterDeg = clamp(newResidualEMA, 1.5, 20.0)
	ms.ResidualEMADeg = newResidualEMA
	ms.NSyncUpdates++
	ms.Holdover = len(fitPool) < 2
	ms.LastUpdated = nowUnix
	if anchorICAO != nil {
		ms.AnchorICAO = anchorICAO
		ms.AnchorPhaseDeg = anchorPhaseDeg
		ms.AnchorScore = anchorScore
	}
}

func (ms *MultiSyncSolver) updateICAOQuality(scored []scoredObs) {
	groups := make(map[uint32][]float64)
	for _, se := range scored {
		if se.fitEligible {
			groups[se.o.ICAO] = append(groups[se.o.ICAO], math.Abs(se.residual))
		}
	}
	nowUnix := float64(time.Now().UnixMicro()) / 1e6
	for icao, absResiduals := range groups {
		med := medianFloat64(absResiduals)
		q := ms.ICAOQuality[icao]
		if q == nil {
			ms.ICAOQuality[icao] = &ICAOSyncQuality{
				ResidualMADDeg: med, NObservations: 1, LastUpdateTS: nowUnix,
			}
		} else {
			q.ResidualMADDeg = (1.0-icaoQualityMADAlpha)*q.ResidualMADDeg + icaoQualityMADAlpha*med
			q.NObservations++
			q.LastUpdateTS = nowUnix
		}
	}
}

// selectAnchor returns (newOffsetDeg, anchorICAO, anchorScore, anchorPhaseDeg).
func (ms *MultiSyncSolver) selectAnchor(
	scored []scoredObs,
	newEpochUS, periodUS, fallbackOffset float64,
) (newOffset float64, anchorICAO *uint32, anchorScore, anchorPhaseDeg float64) {
	type icaoAnchor struct {
		icao        uint32
		sinSum      float64
		cosSum      float64
		nObs        int
		score       float64
	}
	byICAO := make(map[uint32]*icaoAnchor)
	for _, se := range scored {
		if se.status == "rejected" || se.baseW <= 0 || se.fitRejectReason == "near_wrap_residual" {
			continue
		}
		phaseRel := math.Mod((se.effectiveUS-newEpochUS)/periodUS*360.0, 360.0)
		implied := math.Mod(se.o.BearingDeg-phaseRel, 360.0)
		if implied < 0 {
			implied += 360.0
		}
		impliedRad := implied * math.Pi / 180.0
		icao := se.o.ICAO
		a := byICAO[icao]
		if a == nil {
			q := ms.ICAOQuality[icao]
			sc := msAnchorScore(q, se.baseW)
			byICAO[icao] = &icaoAnchor{
				icao:   icao,
				sinSum: math.Sin(impliedRad),
				cosSum: math.Cos(impliedRad),
				nObs:   1,
				score:  sc,
			}
		} else {
			a.sinSum += math.Sin(impliedRad)
			a.cosSum += math.Cos(impliedRad)
			a.nObs++
		}
	}

	var best *icaoAnchor
	for _, a := range byICAO {
		if best == nil || a.score > best.score {
			best = a
		}
	}
	if best == nil || best.score < 0.1 {
		return fallbackOffset, nil, 0, 0
	}

	// Circular mean of implied phases.
	meanRad := math.Atan2(best.sinSum, best.cosSum)
	meanDeg := math.Mod(meanRad*180.0/math.Pi+360.0, 360.0)

	icao := best.icao
	return meanDeg, &icao, best.score, meanDeg
}

func (ms *MultiSyncSolver) refinePeriod(
	livePeriodS, basePeriodS, smoothedSlope float64,
	nFit, nFitICAOs int, spanS float64, majorityRejected bool,
	trusted bool, // true = tight clamp from promoted trusted base; false = wide clamp from bootstrap seed
) (refined float64, blockReason, reacquireReason string, clamped bool, clampDiffPPM float64) {
	refined = livePeriodS
	absSmoothed := math.Abs(smoothedSlope)
	slopeSign := 0
	if smoothedSlope > 0 {
		slopeSign = 1
	} else if smoothedSlope < 0 {
		slopeSign = -1
	}
	prev := ms.SlopeHistory
	if len(prev) > 8 {
		prev = prev[len(prev)-8:]
	}
	consistent := 0
	for _, s := range prev {
		if math.Abs(s) >= slopeDeadBand {
			if (s > 0 && slopeSign > 0) || (s < 0 && slopeSign < 0) {
				consistent++
			}
		}
	}
	persistent := slopeSign != 0 &&
		len(prev) >= persistMinEntries &&
		consistent >= persistMinEntries &&
		absSmoothed >= slopeDeadBand

	switch {
	case majorityRejected:
		blockReason = "majority_rejected"
	case nFit < periodRefineMinInlier:
		blockReason = "insufficient_fit_observations"
	case nFitICAOs < 2:
		blockReason = "insufficient_fit_icaos"
	case spanS < periodRefineMinSpanRot*livePeriodS:
		blockReason = "insufficient_fit_span"
	case !persistent:
		blockReason = "slope_not_persistent"
	}
	if blockReason != "" || livePeriodS <= 0 {
		return
	}

	rateNominal := 360.0 / livePeriodS
	rateTarget := rateNominal + smoothedSlope*periodGain
	if rateTarget <= 0 {
		blockReason = "non_positive_rate_target"
		return
	}
	candidate := 360.0 / rateTarget

	strongFit := nFit >= 12 && nFitICAOs >= 3 && spanS >= 4.0*livePeriodS
	ppmPerUpdate := periodPPMPerUpdate
	// Choose clamp limits based on whether the base period is trusted.
	// In the bootstrap phase (trusted=false) we allow a much wider excursion so the
	// solver can migrate to the correct period family without being held in the wrong one.
	var ppmFromBase float64
	if trusted {
		ppmFromBase = periodPPMFromBase
		if strongFit && persistent {
			ppmPerUpdate = periodPPMStrong
			ppmFromBase = periodPPMBaseStrong
		}
	} else {
		ppmFromBase = periodPPMFromBootstrap
		if strongFit && persistent {
			ppmPerUpdate = periodPPMStrong
			ppmFromBase = periodPPMFromBootstrapStrong
		}
	}
	deltaPPM := (candidate - livePeriodS) / livePeriodS * 1e6
	if deltaPPM > ppmPerUpdate {
		candidate = livePeriodS * (1.0 + ppmPerUpdate*1e-6)
	} else if deltaPPM < -ppmPerUpdate {
		candidate = livePeriodS * (1.0 - ppmPerUpdate*1e-6)
	}
	if basePeriodS > 0 {
		rawPPM := (candidate - basePeriodS) / basePeriodS * 1e6
		clampDiffPPM = rawPPM // capture for diagnostics even when clamp does not fire
		if rawPPM > ppmFromBase {
			candidate = basePeriodS * (1.0 + ppmFromBase*1e-6)
			clamped = true
		} else if rawPPM < -ppmFromBase {
			candidate = basePeriodS * (1.0 - ppmFromBase*1e-6)
			clamped = true
		}
	}
	refined = candidate
	if math.Abs(deltaPPM) > periodPPMStrong*3 {
		reacquireReason = "large_period_jump"
	}
	return
}

func (ms *MultiSyncSolver) assessPeriodFailure(
	nRecent, nRejected, nFit, nFitICAOs int,
	majorityRejected bool, detrendedMAD float64,
) bool {
	if majorityRejected && nFit < 4 {
		return true
	}
	if detrendedMAD > 30.0 && nRecent > 5 {
		return true
	}
	return false
}

// ─── reacquire candidate search ──────────────────────────────────────────────

// searchBestCandidate evaluates a set of nearby period candidates and returns the
// (period, score) pair with the best evidence from the current fit-eligible window.
// Called during wrong-period reacquire to escape an incorrect period family instead
// of snapping back to the (potentially wrong) bootstrap base period.
func (ms *MultiSyncSolver) searchBestCandidate(scored []scoredObs, epochUS float64) (bestPeriod, bestScore float64) {
	candidates := ms.buildCandidatePeriods()
	bestPeriod = ms.PeriodS // safe fallback: current refined period
	bestScore = -1.0
	for _, cand := range candidates {
		r := ms.evalCandidatePeriod(scored, cand, epochUS)
		if r.score > bestScore {
			bestScore = r.score
			bestPeriod = r.periodS
		}
	}
	return
}

// buildCandidatePeriods returns a deduplicated set of period values to probe
// during reacquire.  Includes the current refined period, the bootstrap seed,
// the trusted base (if promoted), and fractional offsets around each anchor.
func (ms *MultiSyncSolver) buildCandidatePeriods() []float64 {
	// Probe offsets in fractional terms (signed).
	probeOffsets := [8]float64{-0.020, -0.010, -0.005, -0.002, 0.002, 0.005, 0.010, 0.020}
	// Key by period rounded to the nearest microsecond to deduplicate near-identical values.
	seen := make(map[int64]bool)
	var candidates []float64

	add := func(p float64) {
		if p < 0.5 || p > 20.0 {
			return
		}
		key := int64(p * 1e6) // μs resolution
		if seen[key] {
			return
		}
		seen[key] = true
		candidates = append(candidates, p)
	}

	anchors := [3]float64{ms.PeriodS, ms.BootstrapPeriodS, ms.TrustedBasePeriodS}
	for _, anchor := range anchors {
		if anchor <= 0 {
			continue
		}
		add(anchor)
		for _, frac := range probeOffsets {
			add(anchor * (1.0 + frac))
		}
	}
	return candidates
}

// evalCandidatePeriod scores how well a candidate period explains the current
// fit-eligible observation window using a circular-mean phase estimate.
// A higher score means stronger evidence for that period family.
func (ms *MultiSyncSolver) evalCandidatePeriod(scored []scoredObs, candidatePeriodS, epochUS float64) candidateEvalResult {
	if candidatePeriodS <= 0 || len(scored) == 0 {
		return candidateEvalResult{periodS: candidatePeriodS, score: -1}
	}
	periodUS := candidatePeriodS * 1e6

	// Estimate the best-fit phase offset for this candidate via circular mean of implied offsets.
	var sinSum, cosSum float64
	n := 0
	for _, se := range scored {
		if !se.fitEligible {
			continue
		}
		phaseInRot := math.Mod((se.effectiveUS-epochUS)/periodUS*360.0, 360.0)
		if phaseInRot < 0 {
			phaseInRot += 360.0
		}
		impliedOffset := math.Mod(se.o.BearingDeg-phaseInRot+360.0, 360.0)
		rad := impliedOffset * math.Pi / 180.0
		sinSum += math.Sin(rad)
		cosSum += math.Cos(rad)
		n++
	}
	if n == 0 {
		return candidateEvalResult{periodS: candidatePeriodS, score: -1}
	}
	bestOffset := math.Mod(math.Atan2(sinSum, cosSum)*180.0/math.Pi+360.0, 360.0)

	// Compute residuals with the best-fit offset and classify each observation.
	icaos := make(map[uint32]bool)
	inliers, rejected := 0, 0
	var absResiduals []float64
	for _, se := range scored {
		if !se.fitEligible {
			continue
		}
		predicted := msPredictBearing(epochUS, bestOffset, periodUS, se.effectiveUS)
		absR := math.Abs(circularDiff(se.o.BearingDeg, predicted))
		absResiduals = append(absResiduals, absR)
		switch msClassifyResidual(absR) {
		case "inlier":
			inliers++
			icaos[se.o.ICAO] = true
		case "rejected":
			rejected++
		}
	}
	if len(absResiduals) == 0 {
		return candidateEvalResult{periodS: candidatePeriodS, score: -1}
	}
	madDeg := medianFloat64(absResiduals)
	total := len(absResiduals)
	rejFrac := float64(rejected) / float64(total)
	icaoCount := len(icaos)

	// Score rewards inlier count and ICAO diversity; penalises high MAD and rejection fraction.
	score := float64(inliers) * float64(icaoCount) / (1.0 + madDeg/5.0) / (1.0 + rejFrac*3.0)

	return candidateEvalResult{
		periodS:     candidatePeriodS,
		inlierCount: inliers,
		icaoCount:   icaoCount,
		madDeg:      madDeg,
		rejFrac:     rejFrac,
		score:       score,
	}
}

// ─── package-level pure helpers ───────────────────────────────────────────────

// msPropCorrectedUS applies one-way aircraft→receiver propagation delay.
func msPropCorrectedUS(centroidUS, rangeNM float64) float64 {
	if rangeNM <= 0 {
		return centroidUS
	}
	return centroidUS - rangeNM*uSPerNMLight
}

// msPredictBearing returns the predicted bearing (deg) using the compact sync model.
func msPredictBearing(epochUS, offsetDeg, periodUS, effectiveUS float64) float64 {
	if periodUS <= 0 {
		return offsetDeg
	}
	phaseInRot := math.Mod((effectiveUS-epochUS)/periodUS*360.0, 360.0)
	if phaseInRot < 0 {
		phaseInRot += 360.0
	}
	return math.Mod(phaseInRot+offsetDeg, 360.0)
}

// msClassifyResidual returns "inlier", "soft", or "rejected".
func msClassifyResidual(absR float64) string {
	if absR <= residualInlierDeg {
		return "inlier"
	}
	if absR <= residualSoftDeg2 {
		return "soft"
	}
	return "rejected"
}

// msScoreObs returns a quality weight (0–1).
func msScoreObs(o MultiSyncObs) float64 {
	nW := math.Min(float64(o.NReplies)/4.0, 1.0)
	sigW := 0.5
	if o.SignalDBFS != nil {
		sigW = math.Max(0.2, math.Min(1.0, (float64(*o.SignalDBFS)+50.0)/40.0))
	}
	ageW := 0.2
	switch {
	case o.PosAgeS <= 1.0:
		ageW = 1.0
	case o.PosAgeS <= 5.0:
		ageW = 0.8
	case o.PosAgeS <= 10.0:
		ageW = 0.5
	}
	return nW * sigW * ageW
}

func msICAOQualityMult(q *ICAOSyncQuality) float64 {
	if q == nil {
		return 1.0
	}
	mad := math.Max(q.ResidualMADDeg, 0.5)
	return math.Max(0.1, math.Min(1.0, 1.0/(1.0+mad/3.0)))
}

func msFitRejectReason(o MultiSyncObs, status string, absR float64, q *ICAOSyncQuality) string {
	if absR >= residualWrapDeg {
		return "near_wrap_residual"
	}
	if absR > residualRejectGate {
		return "residual_gate"
	}
	if float64(o.PosAgeS) > 8.0 {
		return "stale_position"
	}
	if q != nil && q.ResidualMADDeg >= icaoQualityRejectMAD {
		return "icao_quality_reject"
	}
	if status == "rejected" {
		return "residual_gate"
	}
	return ""
}

func msAnchorScore(q *ICAOSyncQuality, baseW float64) float64 {
	if q == nil {
		return baseW * 0.5
	}
	madFactor := math.Max(0.1, 1.0-q.ResidualMADDeg/icaoQualityWarnMAD)
	return baseW * madFactor
}

func msDetrendedMAD(scored []scoredObs, aFit, bFit, tRef float64) float64 {
	var detrended []float64
	for _, se := range scored {
		if !se.fitEligible {
			continue
		}
		t := se.effectiveUS/1e6 - tRef
		detrended = append(detrended, math.Abs(se.residual-(aFit+bFit*t)))
	}
	if len(detrended) == 0 {
		return 999.0
	}
	return medianFloat64(detrended)
}

// BearingAndRangeNM computes the initial bearing (deg) and range (NM) from the
// receiver at (rxLat, rxLon) to an aircraft at (acLat, acLon).
// Returns (bearing, rangeNM) or (-1, 0) when the receiver position is unknown.
func BearingAndRangeNM(rxLat, rxLon, acLat, acLon float64) (float64, float64) {
	p1 := rxLat * math.Pi / 180.0
	p2 := acLat * math.Pi / 180.0
	dl := (acLon - rxLon) * math.Pi / 180.0
	y := math.Sin(dl) * math.Cos(p2)
	x := math.Cos(p1)*math.Sin(p2) - math.Sin(p1)*math.Cos(p2)*math.Cos(dl)
	bearing := math.Mod(math.Atan2(y, x)*180.0/math.Pi+360.0, 360.0)

	// Haversine distance.
	dp := p2 - p1
	dlon := dl
	a := math.Sin(dp/2)*math.Sin(dp/2) + math.Cos(p1)*math.Cos(p2)*math.Sin(dlon/2)*math.Sin(dlon/2)
	distM := 2 * 6371000.0 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	rangeNM := distM / 1852.0
	return bearing, rangeNM
}

// weightedLinearFit computes y = a + b*x by weighted least squares.
// Returns (a, b); falls back to (weighted mean, 0) when underdetermined.
func weightedLinearFit(xs, ys, ws []float64) (a, b float64) {
	n := len(xs)
	if n == 0 || n != len(ys) || n != len(ws) {
		return 0, 0
	}
	var totalW, sumWX, sumWY float64
	for i := range xs {
		if ws[i] <= 0 {
			continue
		}
		totalW += ws[i]
		sumWX += ws[i] * xs[i]
		sumWY += ws[i] * ys[i]
	}
	if totalW <= 0 {
		return 0, 0
	}
	mx := sumWX / totalW
	my := sumWY / totalW
	var num, den float64
	for i := range xs {
		if ws[i] <= 0 {
			continue
		}
		dx := xs[i] - mx
		num += ws[i] * dx * (ys[i] - my)
		den += ws[i] * dx * dx
	}
	if den <= 0 {
		return my, 0
	}
	b = num / den
	a = my - b*mx
	return
}

func medianFloat64(vs []float64) float64 {
	if len(vs) == 0 {
		return 0
	}
	sorted := make([]float64, len(vs))
	copy(sorted, vs)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2.0
}
