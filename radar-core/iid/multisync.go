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
//   - Motion compensation: Option B geometric guard — period correction prefers
//     ≥3 contributing ICAOs and ≥30° bearing spread when the DF dominant prior is
//     active; limited evidence can still run in degraded mode with reduced gain/step.
//   - Anchor selection: highest-quality ICAO with good recent observations. Candidate
//     anchors remain diagnostic until validator-backed authority promotion applies them.
//   - Period refinement: slope EMA + persistence gate (matches Python).
//   - Wrong-period reacquire: state machine matching Python's reacquire logic.
//   - The refined model is a bounded correction around the DF dominant period family,
//     not an independent period estimator. Reacquire candidates outside the dominant
//     bound are diagnostic only; they do not update the published period.
//   - AbsolutePhaseTrusted is an output of refined_authoritative authority and strong
//     validation, not an input prerequisite for entering refined_authoritative.
//   - Compact/dominant-recovery residuals are operational frames; refined candidate-anchor
//     residuals are a separate diagnostic basis until promotion.
//
// File layout (each stage can be tested independently):
//   multisync.go              — solver struct, public API, runFit orchestration
//   multisync_observation.go  — Stage 1: propagation correction, residual scoring, fit eligibility
//   multisync_period.go       — Stage 2: slope fitting, period refinement, trust promotion
//   multisync_phase.go        — Stage 3: anchor selection, phase branch solving, validation
//   multisync_authority.go    — Stage 4: authority state machine, candidate/authoritative update
//   multisync_long_term.go    — persistent long-term period estimator (bounded EMA around dominant)
//   multisync_branch.go       — persistent bounded branch-track set with validation-aware updates
//   multisync_snapshot.go     — Snapshot construction and protocol emission

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

	// Period refinement constants.
	slopeEMAAlpha                  = 0.08
	retainedSlopeEMAAlpha          = 0.05
	slopeDeadBand                  = 0.01 // deg/s — retained-window slope must exceed this to drive period change
	persistMinEntries              = 5
	periodPPMPerUpdate             = 60.0
	periodPPMFromBase              = 2000.0
	periodPPMStrong                = 400.0
	periodPPMBaseStrong            = 15000.0
	periodRefineMinInlier          = 6
	periodRefineMinSpanRot         = 2.0
	periodGain                     = 0.12
	periodGainDegraded             = 0.04
	periodRefineDegradedMaxStepPPM = 5.0
	periodFitUnwrappedResidualGate = 90.0

	// Trust promotion thresholds — the refined solver must earn a stable run streak
	// before its period is promoted to the trusted base. When dominantPeriodS > 0
	// the trusted base is secondary: the DF dominant prior is the family authority
	// and the base-period clamp is overridden to dominantPeriodS regardless of trust.
	trustMinStreak      = 8    // consecutive updates required to promote the period
	trustMinFitPool     = 8    // min fit-pool entries per update to count toward streak
	trustMinICAOs       = 3    // min distinct ICAOs per update to count toward streak
	trustMaxResidualDeg = 8.0  // detrended MAD ceiling for a trust-eligible update (deg)
	trustMaxRejFrac     = 0.15 // rejection fraction ceiling for a trust-eligible update

	// Period clamp limits when the DF dominant prior is NOT available.
	periodPPMFromBootstrap       = 10000.0
	periodPPMFromBootstrapStrong = 30000.0

	// Period refinement limits relative to the DF dominant prior (used when dominantPeriodS > 0).
	periodRefineMaxPPMFromDominant = 300.0
	periodRefineMaxStepPPM         = 20.0

	// Motion compensation guard (Option B).
	periodRefineMotionGuardMinICAOs = 3
	periodRefineMinBearingSpreadDeg = 30.0

	// Reacquire / failure detection.
	reacquireMADThreshold = 8.0
	reacquireMinClean     = 2

	recoveryResidualEMAThreshold     = 18.0
	recoveryRejectedFrameThreshold   = 4
	recoveryFitEligibleMin           = 4
	recoveryPeriodDeltaPPM           = 2500.0
	recoveryResidualRejectGate       = 55.0
	recoveryPositionAgeMaxS          = 15.0
	recoveryEntryFailureStreak       = 2
	recoveryAnchorStarvedCandidates  = 0
	recoveryAnchorStarvedFitEligible = 2

	// Authority hysteresis.
	authorityPromoteRefinedStreak       = 6
	authorityRefinedFailureStreak       = 5
	authorityCompactReclaimStreak       = 8
	authorityRecoveryEntryStreak        = 2
	authorityRecoveryExitStreak         = 6
	authorityCompactHealthyDeltaPPM     = 1000.0
	authorityMinModeHoldS               = 15.0
	authorityPromotionMaxSlopeDegPerS   = 0.05
	authorityPromotionMaxSlopeWindowDeg = 12.0

	anchorSwitchMinScoreDelta = 0.12
	anchorSwitchMinScoreRatio = 1.25
	anchorHoldMinUpdates      = 3
	anchorPoorSpreadDeg       = 18.0
	anchorPoorFitFraction     = 0.55

	anchorMinScore       = 0.15
	anchorMinFitEligible = 2
	anchorMinFitFraction = 0.18
	anchorMaxSpreadDeg   = 32.0

	// Global branch clustering.
	globalBranchMatchDeg     = 35.0
	globalBranchMinObs       = 3
	globalBranchMaxSpreadDeg = 42.0

	candidateStablePeriodPPM         = 300.0
	candidateStablePhaseDeg          = 10.0
	candidatePromotionMinStreak      = 6
	authoritativeInitPromotionStreak = 4
	authoritativePeriodGain          = 0.02
	authoritativePhaseGain           = 0.08
	authoritativePeriodMaxStepPPM    = 20.0
	authoritativeValidationStrong    = 0.7
	authoritativeValidationWeak      = 0.45
	branchAmbiguityRejectThreshold   = 0.9
	validatorAgreementMinCount       = 2
	validatorAgreementPhaseDeg       = 12.0
	validatorDisagreementPhaseDeg    = 24.0
	validatorMinObsPerICAO           = 2

	// ICAO quality memory.
	icaoQualityMADAlpha  = 0.15
	icaoQualityRejectMAD = 20.0
	icaoQualityWarnMAD   = 12.0

	// Throttle: minimum interval between solver runs (matches Python 250 ms).
	multiSyncMinIntervalS = 0.25
)

const (
	authorityModeCompact  = "compact_authoritative"
	authorityModeRecovery = "dominant_recovery"
	authorityModeRefined  = "refined_authoritative"
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

// PreparedObservation is a scored, propagation-corrected observation ready for period/phase fitting.
// It is the output of the observation-preparation stage and the input to all subsequent stages.
type PreparedObservation struct {
	Obs MultiSyncObs

	EffectiveUS    float64 // propagation-corrected timestamp (µs)
	BearingDeg     float64 // observed bearing (= Obs.BearingDeg, kept for convenience)
	PredictedDeg   float64 // bearing predicted by the seed hypothesis
	ResidualDeg    float64 // circular difference: observed − predicted
	AbsResidualDeg float64

	BaseWeight      float64 // quality weight before ICAO multiplier
	EffectiveWeight float64 // weight used in fit (0 when rejected)

	Status          string // "inlier" | "soft" | "rejected"
	FitEligible     bool   // passes all fit-admission gates
	FitRejectReason string // populated when FitEligible is false

	CorrectionFlags []string // diagnostic labels (e.g. "recovery_relaxed_admission")

	// phaseInRot is an internal field: phase position within one rotation (0–360°).
	phaseInRot float64
}

// candidateEvalResult holds the scoring result for one candidate period
// evaluated during wrong-period reacquire search.
type candidateEvalResult struct {
	periodS                 float64
	inlierCount             int
	icaoCount               int
	madDeg                  float64
	rejFrac                 float64
	score                   float64
	phaseOffsetDeg          float64
	publishedOffsetDeg      float64
	newEpochUS              float64
	anchorICAO              *uint32
	anchorScore             float64
	anchorPhaseDeg          float64
	anchorCandidateCount    int
	anchorNoCandidateReason string
	anchorCandidates        []AnchorCandidateSnapshot
}

// AnchorCandidateSnapshot is a point-in-time diagnostic view of one anchor candidate.
type AnchorCandidateSnapshot struct {
	ICAO                uint32
	Score               float64
	SpreadDeg           float64
	ObsCount            int
	FitEligibleCount    int
	FitEligibleFraction float64
	Status              string
	RejectReasons       []string
}

// publishedAlignment is the internal result of a phase alignment computation
// (epoch, offset, anchor) before it is written to the candidate or authoritative state.
type publishedAlignment struct {
	epochUS                 float64
	offsetDeg               float64
	anchorICAO              *uint32
	anchorScore             float64
	anchorPhaseDeg          float64
	anchorCandidateCount    int
	anchorNoCandidateReason string
	anchorCandidates        []AnchorCandidateSnapshot
}

// syncStateEstimate is an internal struct carrying one complete sync hypothesis
// (period, epoch, offset, anchor) through the pipeline stages.
type syncStateEstimate struct {
	present     bool
	periodS     float64
	epochUS     float64
	offsetDeg   float64
	anchorICAO  *uint32
	anchorScore float64
	anchorPhase float64
}

// syncValidationSummary is the result of evaluatePhaseValidation.
type syncValidationSummary struct {
	score              float64
	branchAmbiguity    float64
	// anchorCompetitionAmbiguity is the raw secondScore/topScore from anchor-candidate
	// competition, before the global observation-coherence modifiers are applied.
	// branchAmbiguity is the combined score used for operational decisions.
	anchorCompetitionAmbiguity float64
	circularDispersion         float64
	validatorAgreement         int
	validatorDisagree          int
	validatorEvaluated         int
	// validatorExcluded counts fit-eligible non-anchor ICAOs that had fewer than
	// validatorMinObsPerICAO observations and were excluded from the agree/disagree tally.
	validatorExcluded int
	status            string
	strong            bool
	weak              bool
	// PerICAOOffsets is the per-ICAO validator table built alongside validation.
	PerICAOOffsets []PerICAOPhaseOffset
}

// MultiSyncSolver holds per-IID multi-aircraft sync refinement state.
type MultiSyncSolver struct {
	mu sync.Mutex

	iid uint8

	// Observation buffer (bounded, time-windowed).
	obs []MultiSyncObs

	// Published sync state.
	Present        bool
	Usable         bool
	PeriodS        float64
	PeriodBaseS    float64
	PhaseEpochUS   float64
	PhaseOffsetDeg float64
	JitterDeg      float64
	ResidualEMADeg float64
	NSyncUpdates   int
	Holdover       bool
	LastUpdated    float64

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
	BootstrapPeriodS   float64
	TrustedBasePeriodS float64
	TrustUpdateStreak  int

	// Per-run diagnostics (updated each solver run, readable via Snapshot).
	LastBaseClamped                      bool
	LastBaseClampDiffPPM                 float64
	LastWrongPeriodSuspect               bool
	LastReacquireCandidateP              float64
	LastReacquireCandidateSc             float64
	LastFitTotalObs                      int
	LastFitEligibleObs                   int
	LastFitRejectedObs                   int
	LastFitContributingICAOs             int
	LastFitWindowS                       float64
	LastDisplayWindowS                   float64
	LastFitSpanS                         float64
	LastResidualSlopeDegPerS             float64
	LastFitRejectReasons                 map[string]uint64
	LastAnchorCandidateCount             int
	LastAnchorNoCandidate                string
	LastAnchorCandidates                 []AnchorCandidateSnapshot
	LastDominantPriorPeriodS             float64
	LastActiveFamilyPriorPeriodS         float64
	LastActiveFamilyPriorSource          string
	LastDominantPriorActive              bool
	LastCompactPeriodS                   float64
	LastPeriodDeltaToDominantS           float64
	LastPeriodDeltaToDominantPPM         float64
	LastCompactDeltaToDominantS          float64
	LastCompactDeltaToDominantPPM        float64
	LastCompactSyncUnreliable            bool
	LastRecoveryModeActive               bool
	LastRecoveryTriggerReasons           []string
	LastCompactGatingBypassed            bool
	LastRecoveryRelaxedAdmissions        int
	ActiveAuthorityMode                  string
	AuthoritySwitchCount                 int
	LastAuthoritySwitchTS                float64
	LastAuthoritySwitchReason            string
	AuthorityEnterStreak                 int
	AuthorityExitStreak                  int
	RefinedHealthyStreak                 int
	RefinedFailureStreak                 int
	CompactHealthyStreak                 int
	RecoveryEntryStreak                  int
	RecoveryExitStreak                   int
	AnchorSwitchCount                    int
	LastAnchorSwitchTS                   float64
	LastAnchorSwitchReason               string
	AnchorHoldUpdates                    int
	CandidatePresent                     bool
	CandidatePeriodS                     float64
	CandidatePhaseEpochUS                float64
	CandidatePhaseOffsetDeg              float64
	CandidateAnchorICAO                  *uint32
	CandidateAnchorScore                 float64
	CandidateAnchorPhaseDeg              float64
	CandidateValidationScore             float64
	CandidateStateSinceTS                float64
	CandidatePromotionStreak             int
	AuthoritativePresent                 bool
	AuthoritativePeriodS                 float64
	AuthoritativePhaseEpochUS            float64
	AuthoritativePhaseOffsetDeg          float64
	AuthoritativeAnchorICAO              *uint32
	AuthoritativeAnchorScore             float64
	AuthoritativeAnchorPhaseDeg          float64
	AuthoritativeValidationScore         float64
	AuthoritativeStateSinceTS            float64
	LastAuthoritativePeriodGain          float64
	LastAuthoritativePhaseGain           float64
	LastPeriodFrozenDueToPhaseValidation bool
	LastBranchAmbiguityScore             float64
	LastAnchorCompetitionAmbiguity       float64
	LastCircularDispersionDeg            float64
	LastValidatorAgreementCount          int
	LastValidatorDisagreementCount       int
	LastValidatorExcludedCount           int
	LastPerICAOPhaseOffsets              []PerICAOPhaseOffset
	LastCandidateValidationStatus        string
	LastCandidateApplicationBlockReason  string
	LastCandidateMode                    string
	LastAuthoritativeMode                string

	// AbsolutePhaseTrusted is true only when period is within periodRefineMaxPPMFromDominant,
	// anchor is valid, and phase validation is strong.
	AbsolutePhaseTrusted bool

	LastDominantPriorInconsistent bool
	LastPeriodRefineBlockReason   string
	LastFitBearingSpreadDeg           float64
	LastPeriodFitAcceptedObservations int
	LastPeriodFitRejectedObservations int
	LastPeriodFitRejectedAfterUnwrap  int
	LastSlopeWindowDeg                float64
	LastSlopePromotionGatePassed      bool
	LastAuthorityPromotionBlockReason string
	LastResidualCorrectionBasis       string
	LastMotionGuardDegraded           bool

	// ─── Long-term estimator separation ───────────────────────────────────────
	// These fields separate the short-term measurement (the 30s fit window) from
	// the long-term applied state. `LastLocal*` mirror the per-window fit
	// outputs; `LongTerm*` and `Branch*` hold the persistent estimator state.
	// `LongTermPeriodEstimateS` drives the published `PeriodS` via the apply
	// path in runFit (sourced from the long-term estimator once seeded).
	// Branch promotion to authoritative authority requires accumulated branch
	// confidence. Existing `LastResidualSlopeDegPerS`, `LastFitSpanS`, etc.
	// continue to mean the short-window measurement.
	LastLocalPeriodMeasurementS float64 // slope-corrected refined period from current fit window
	LastLocalCandidateAnchorICAO *uint32
	LastLocalBranchOffsetDeg     float64 // candidate phase offset chosen by current fit window
	LastLocalValidatorAgreement  int
	LastLocalFitQualityScore     float64 // 0..1; derived from detrended MAD + fit pool counts

	// Long-term period estimator (Layer 2 will populate). Bounded EMA-style
	// estimate that survives across fit windows; the DF dominant period remains
	// the family authority — these fields nudge only within ±300 PPM of it.
	LongTermPeriodEstimateS           float64
	LongTermPeriodEstimatorConfidence float64
	LongTermPeriodEstimatorAgeS       float64
	ConsecutivePeriodConsistentWindows int
	LastPeriodUpdateDeltaS            float64

	// Long-term branch / phase estimator (Layer 3 will populate). A small
	// bounded set of competing branch tracks; promotion to refined_authoritative
	// requires accumulated branch confidence, not just the current window.
	LongTermBranchAnchorICAO   *uint32
	LongTermBranchOffsetDeg    float64
	BranchEstimatorConfidence  float64
	BranchConsistentWindows    int
	BranchContradictionWindows int
	BranchCompetitorCount      int
	BranchPromotionBlockReason string

	// BranchTracks is the bounded set of competing phase-branch estimates.
	// Layer 3 populates and ages these; Layer 4 promotes a track to authority.
	// Capped at branchMaxTracks (4) — the weakest is evicted on overflow.
	BranchTracks []*BranchEstimate

	// Internal: wall-clock TS of the last long-term-estimator seed (used to
	// derive LongTermPeriodEstimatorAgeS). Not exposed in Snapshot.
	lastLongTermPeriodSeedTS float64

	// Throttle.
	lastRunTS float64
}

// NewMultiSyncSolver creates an empty solver for the given IID.
func NewMultiSyncSolver(iid uint8) *MultiSyncSolver {
	return &MultiSyncSolver{
		iid:                 iid,
		ICAOQuality:         make(map[uint32]*ICAOSyncQuality),
		ActiveAuthorityMode: authorityModeCompact,
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
func (ms *MultiSyncSolver) TryUpdate(sync *SyncState, dominantPeriodS float64) bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	now := float64(time.Now().UnixMicro()) / 1e6
	if now-ms.lastRunTS < multiSyncMinIntervalS {
		return false
	}
	ms.lastRunTS = now
	ms.runFit(sync, dominantPeriodS, now)
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
	ms.LastFitTotalObs = 0
	ms.LastFitEligibleObs = 0
	ms.LastFitRejectedObs = 0
	ms.LastFitContributingICAOs = 0
	ms.LastFitWindowS = 0
	ms.LastDisplayWindowS = multiSyncRetentionS
	ms.LastFitSpanS = 0
	ms.LastResidualSlopeDegPerS = 0
	ms.LastFitRejectReasons = nil
	ms.LastAnchorCandidateCount = 0
	ms.LastAnchorNoCandidate = ""
	ms.LastAnchorCandidates = nil
	ms.LastDominantPriorPeriodS = 0
	ms.LastActiveFamilyPriorPeriodS = 0
	ms.LastActiveFamilyPriorSource = ""
	ms.LastDominantPriorActive = false
	ms.LastCompactPeriodS = 0
	ms.LastPeriodDeltaToDominantS = 0
	ms.LastPeriodDeltaToDominantPPM = 0
	ms.LastCompactDeltaToDominantS = 0
	ms.LastCompactDeltaToDominantPPM = 0
	ms.LastCompactSyncUnreliable = false
	ms.LastRecoveryModeActive = false
	ms.LastRecoveryTriggerReasons = nil
	ms.LastCompactGatingBypassed = false
	ms.LastRecoveryRelaxedAdmissions = 0
	ms.ActiveAuthorityMode = authorityModeCompact
	ms.AuthoritySwitchCount = 0
	ms.LastAuthoritySwitchTS = 0
	ms.LastAuthoritySwitchReason = ""
	ms.AuthorityEnterStreak = 0
	ms.AuthorityExitStreak = 0
	ms.RefinedHealthyStreak = 0
	ms.RefinedFailureStreak = 0
	ms.CompactHealthyStreak = 0
	ms.RecoveryEntryStreak = 0
	ms.RecoveryExitStreak = 0
	ms.AnchorSwitchCount = 0
	ms.LastAnchorSwitchTS = 0
	ms.LastAnchorSwitchReason = ""
	ms.AnchorHoldUpdates = 0
	ms.CandidatePresent = false
	ms.CandidatePeriodS = 0
	ms.CandidatePhaseEpochUS = 0
	ms.CandidatePhaseOffsetDeg = 0
	ms.CandidateAnchorICAO = nil
	ms.CandidateAnchorScore = 0
	ms.CandidateAnchorPhaseDeg = 0
	ms.CandidateValidationScore = 0
	ms.CandidateStateSinceTS = 0
	ms.CandidatePromotionStreak = 0
	ms.AuthoritativePresent = false
	ms.AuthoritativePeriodS = 0
	ms.AuthoritativePhaseEpochUS = 0
	ms.AuthoritativePhaseOffsetDeg = 0
	ms.AuthoritativeAnchorICAO = nil
	ms.AuthoritativeAnchorScore = 0
	ms.AuthoritativeAnchorPhaseDeg = 0
	ms.AuthoritativeValidationScore = 0
	ms.AuthoritativeStateSinceTS = 0
	ms.LastAuthoritativePeriodGain = 0
	ms.LastAuthoritativePhaseGain = 0
	ms.LastPeriodFrozenDueToPhaseValidation = false
	ms.LastBranchAmbiguityScore = 0
	ms.LastAnchorCompetitionAmbiguity = 0
	ms.LastCircularDispersionDeg = 0
	ms.LastValidatorAgreementCount = 0
	ms.LastValidatorDisagreementCount = 0
	ms.LastValidatorExcludedCount = 0
	ms.LastPerICAOPhaseOffsets = nil
	ms.LastCandidateValidationStatus = ""
	ms.LastCandidateApplicationBlockReason = ""
	ms.LastCandidateMode = ""
	ms.LastAuthoritativeMode = ""
	ms.AbsolutePhaseTrusted = false
	ms.LastDominantPriorInconsistent = false
	ms.LastPeriodRefineBlockReason = ""
	ms.LastFitBearingSpreadDeg = 0
	ms.LastPeriodFitAcceptedObservations = 0
	ms.LastPeriodFitRejectedObservations = 0
	ms.LastPeriodFitRejectedAfterUnwrap = 0
	ms.LastSlopeWindowDeg = 0
	ms.LastSlopePromotionGatePassed = false
	ms.LastAuthorityPromotionBlockReason = ""
	ms.LastResidualCorrectionBasis = ""
	ms.LastMotionGuardDegraded = false
	ms.LastLocalPeriodMeasurementS = 0
	ms.LastLocalCandidateAnchorICAO = nil
	ms.LastLocalBranchOffsetDeg = 0
	ms.LastLocalValidatorAgreement = 0
	ms.LastLocalFitQualityScore = 0
	ms.LongTermPeriodEstimateS = 0
	ms.LongTermPeriodEstimatorConfidence = 0
	ms.LongTermPeriodEstimatorAgeS = 0
	ms.ConsecutivePeriodConsistentWindows = 0
	ms.LastPeriodUpdateDeltaS = 0
	ms.LongTermBranchAnchorICAO = nil
	ms.LongTermBranchOffsetDeg = 0
	ms.BranchEstimatorConfidence = 0
	ms.BranchConsistentWindows = 0
	ms.BranchContradictionWindows = 0
	ms.BranchCompetitorCount = 0
	ms.BranchPromotionBlockReason = ""
	ms.BranchTracks = nil
	ms.lastLongTermPeriodSeedTS = 0
}

// ─── internal solver — pipeline orchestration ─────────────────────────────────

// runFit is the main solver loop. It runs as a pipeline of four discrete stages:
//
//  1. prepareObservations — propagation correction, residual scoring, fit eligibility
//  2. fitPeriod           — slope fitting, period refinement, trust promotion
//  3. solvePhaseBranch    — anchor selection, phase branch solving, validation
//  4. (authority updates) — updateCandidateState, updateAuthoritativeState, authority mode
//
// Each stage produces an explicit result struct. The stages do not share implicit state
// other than what is passed through these structs and the solver's persistent fields.
// See the individual stage files for detailed invariants and testability notes.
func (ms *MultiSyncSolver) runFit(sync *SyncState, dominantPeriodS, nowUnix float64) {
	ms.ensureAuthorityMode()
	ms.LastDominantPriorInconsistent = false

	// ── Seed phase selection ──────────────────────────────────────────────────
	// Priority: candidate > authoritative > published > compact sync > nothing.
	var seedEpochUS, seedOffsetDeg float64
	switch {
	case ms.CandidatePresent && ms.CandidatePhaseEpochUS > 0:
		seedEpochUS = ms.CandidatePhaseEpochUS
		seedOffsetDeg = ms.CandidatePhaseOffsetDeg
	case ms.AuthoritativePresent && ms.AuthoritativePhaseEpochUS > 0:
		seedEpochUS = ms.AuthoritativePhaseEpochUS
		seedOffsetDeg = ms.AuthoritativePhaseOffsetDeg
	case ms.Present && ms.PhaseEpochUS > 0:
		seedEpochUS = ms.PhaseEpochUS
		seedOffsetDeg = ms.PhaseOffsetDeg
	case sync != nil && sync.PeriodS > 0:
		seedEpochUS = sync.PhaseEpochUS
		seedOffsetDeg = sync.PhaseOffsetDeg
	case ms.Present && ms.PeriodS > 0:
		seedEpochUS = ms.PhaseEpochUS
		seedOffsetDeg = ms.PhaseOffsetDeg
	default:
		return // no phase seed available yet
	}

	compactPeriodS := 0.0
	if sync != nil && sync.PeriodS > 0 {
		compactPeriodS = sync.PeriodS
	}

	// Capture the compact-sync period on the very first run as the bootstrap seed.
	// This is never overwritten so the solver always knows what family it started in.
	if ms.BootstrapPeriodS <= 0 {
		switch {
		case dominantPeriodS > 0:
			ms.BootstrapPeriodS = dominantPeriodS
		case compactPeriodS > 0:
			ms.BootstrapPeriodS = compactPeriodS
		case ms.Present && ms.PeriodS > 0:
			ms.BootstrapPeriodS = ms.PeriodS
		}
	}

	// ── Recovery assessment and entry ────────────────────────────────────────
	recoveryRequested, compactUnreliable, recoveryReasons := ms.assessRecoveryMode(sync, dominantPeriodS)
	if recoveryRequested {
		ms.RecoveryEntryStreak++
	} else {
		ms.RecoveryEntryStreak = 0
	}
	if ms.ActiveAuthorityMode != authorityModeRecovery && ms.RecoveryEntryStreak >= authorityRecoveryEntryStreak {
		ms.setAuthorityMode(authorityModeRecovery, "recovery_requested_streak", nowUnix)
		ms.RecoveryExitStreak = 0
	}
	recoveryActive := ms.ActiveAuthorityMode == authorityModeRecovery

	// ── Active family prior selection ─────────────────────────────────────────
	activeFamilyPriorS, activeFamilyPriorSource := ms.selectActiveFamilyPrior(compactPeriodS, dominantPeriodS, ms.ActiveAuthorityMode)
	if activeFamilyPriorS <= 0 {
		return
	}
	livePeriodS := activeFamilyPriorS

	ms.LastDominantPriorPeriodS = dominantPeriodS
	ms.LastActiveFamilyPriorPeriodS = activeFamilyPriorS
	ms.LastActiveFamilyPriorSource = activeFamilyPriorSource
	ms.LastDominantPriorActive = dominantPeriodS > 0 && activeFamilyPriorSource == "dominant_live_df"
	ms.LastCompactPeriodS = compactPeriodS
	ms.LastCompactSyncUnreliable = compactUnreliable
	ms.LastRecoveryModeActive = recoveryActive
	ms.LastRecoveryTriggerReasons = append([]string(nil), recoveryReasons...)
	ms.LastCompactGatingBypassed = recoveryActive
	ms.LastRecoveryRelaxedAdmissions = 0

	// ── Clamp base and trusted state ─────────────────────────────────────────
	trusted := ms.TrustedBasePeriodS > 0 && !recoveryActive
	basePeriodS := activeFamilyPriorS
	if trusted && ms.TrustedBasePeriodS > 0 {
		basePeriodS = ms.TrustedBasePeriodS
	} else if dominantPeriodS > 0 {
		basePeriodS = dominantPeriodS
	} else if compactPeriodS > 0 {
		basePeriodS = compactPeriodS
	} else if ms.BootstrapPeriodS > 0 {
		basePeriodS = ms.BootstrapPeriodS
	}

	// ── Dominant-prior family enforcement ────────────────────────────────────
	// When the DF alignment period is known and the current live period has drifted
	// outside the ±300 PPM refinement band, snap back immediately.
	// Slope-based refinement cannot self-correct a large period error: if the model
	// is settled at the wrong period, observations score cleanly at that period,
	// slope ≈ 0, and refinePeriod's slope_not_persistent gate fires before the
	// dominant clamp is ever reached.
	if dominantPeriodS > 0 && livePeriodS > 0 {
		livePPM := (livePeriodS - dominantPeriodS) / dominantPeriodS * 1e6
		if math.Abs(livePPM) > periodRefineMaxPPMFromDominant {
			if livePPM > 0 {
				livePeriodS = dominantPeriodS * (1.0 + periodRefineMaxPPMFromDominant*1e-6)
			} else {
				livePeriodS = dominantPeriodS * (1.0 - periodRefineMaxPPMFromDominant*1e-6)
			}
			basePeriodS = dominantPeriodS
			trusted = false
			ms.TrustedBasePeriodS = 0
			ms.TrustUpdateStreak = 0
			if ms.AuthoritativePeriodS > 0 {
				ms.AuthoritativePeriodS = livePeriodS
			}
		}
	}

	// ── Rolling observation window ────────────────────────────────────────────
	windowS := math.Max(livePeriodS*multiSyncWindowRotations, multiSyncWindowMinS)
	cutoffTS := nowUnix - windowS
	var recent []MultiSyncObs
	for _, o := range ms.obs {
		if o.WallTS >= cutoffTS {
			recent = append(recent, o)
		}
	}
	if len(recent) < multiSyncMinObs {
		ms.LastFitWindowS = windowS
		ms.LastDisplayWindowS = multiSyncRetentionS
		ms.Holdover = true
		return
	}

	// ── Stage 1: Observation preparation ─────────────────────────────────────
	prepared, fitRejectReasons, fitEligibleObs, recoveryRelaxedAdmissions :=
		ms.prepareObservations(recent, seedEpochUS, seedOffsetDeg, livePeriodS, recoveryActive)
	ms.updateICAOQuality(prepared)
	ms.LastRecoveryRelaxedAdmissions = recoveryRelaxedAdmissions

	// Count rejects for cleanUpdate and trustedEnough gates.
	nRejected := 0
	for _, p := range prepared {
		if p.Status == "rejected" {
			nRejected++
		}
	}
	majorityRejected := nRejected >= len(recent)/2+1

	ms.LastFitTotalObs = len(recent)
	ms.LastFitEligibleObs = fitEligibleObs
	ms.LastFitRejectedObs = len(recent) - fitEligibleObs
	ms.LastFitWindowS = windowS
	ms.LastDisplayWindowS = multiSyncRetentionS
	ms.LastFitRejectReasons = fitRejectReasons

	// ── Stage 2: Period fitting ───────────────────────────────────────────────
	// fitPeriod builds the fit pool internally and returns pool counts + residuals,
	// eliminating the need for a duplicate pool construction here.
	period := ms.fitPeriod(prepared, seedEpochUS, seedOffsetDeg, livePeriodS, basePeriodS, dominantPeriodS, recoveryActive, nowUnix)

	if period.FitPoolCount == 0 {
		ms.LastFitContributingICAOs = 0
		ms.LastFitSpanS = 0
		ms.LastAnchorCandidateCount = 0
		ms.LastAnchorNoCandidate = "no_fit_pool"
		ms.LastAnchorCandidates = nil
		ms.Holdover = true
		ms.Usable = false
		return
	}

	ms.LastFitContributingICAOs = period.FitICAOCount

	ms.LastFitSpanS = period.FitSpanS
	ms.LastResidualSlopeDegPerS = period.ResidualSlopeDegPerS
	ms.LastPeriodRefineBlockReason = period.BlockReason
	ms.LastBaseClamped = period.BaseClamped
	ms.LastBaseClampDiffPPM = period.ClampDiffPPM
	ms.LastPeriodFitAcceptedObservations = period.AcceptedObs
	ms.LastPeriodFitRejectedObservations = period.RejectedObs
	ms.LastPeriodFitRejectedAfterUnwrap = period.RejectedAfterUnwrap
	ms.LastFitBearingSpreadDeg = period.BearingSpreadDeg
	ms.LastResidualCorrectionBasis = period.CorrectionBasis
	ms.LastMotionGuardDegraded = period.MotionGuardDegraded

	// Layer 1: snapshot the short-window period measurement separately from
	// the applied period. In Layer 2 this feeds the long-term EMA; for now
	// the applied period (PeriodS) is still derived from the same value.
	ms.LastLocalPeriodMeasurementS = period.AppliedPeriodS
	if period.FitPoolCount > 0 {
		// Quality score in [0,1]: 1 when MAD ≤ trustMaxResidualDeg AND we have
		// enough fit-pool entries / ICAOs; degrades smoothly as MAD grows.
		madTerm := 1.0
		if period.DetrendedMADDeg > 0 {
			madTerm = trustMaxResidualDeg / math.Max(period.DetrendedMADDeg, trustMaxResidualDeg)
		}
		poolTerm := math.Min(1.0, float64(period.FitPoolCount)/float64(trustMinFitPool))
		icaoTerm := math.Min(1.0, float64(period.FitICAOCount)/float64(trustMinICAOs))
		ms.LastLocalFitQualityScore = clamp(madTerm*poolTerm*icaoTerm, 0, 1)
	} else {
		ms.LastLocalFitQualityScore = 0
	}

	// Layer 2: feed the local measurement into the persistent long-term
	// estimator. This nudges (or seeds) `LongTermPeriodEstimateS`. The
	// applied period further down (`finalPeriodS`) is then sourced from the
	// long-term estimator rather than the raw short-window measurement.
	ms.updateLongTermPeriodEstimator(
		ms.LastLocalPeriodMeasurementS,
		dominantPeriodS,
		nowUnix,
		ms.LastLocalFitQualityScore,
	)

	// Slope gate for authority promotion.
	slopeWindowDeg := math.Abs(period.ResidualSlopeDegPerS) * period.FitSpanS
	ms.LastSlopeWindowDeg = slopeWindowDeg
	ms.LastSlopePromotionGatePassed = period.SlopeGatePassed
	authorityPromotionBlockReason := ""
	if !period.SlopeGatePassed {
		if math.Abs(period.ResidualSlopeDegPerS) > authorityPromotionMaxSlopeDegPerS {
			authorityPromotionBlockReason = "slope_exceeds_promotion_threshold"
		} else {
			authorityPromotionBlockReason = "slope_window_exceeds_promotion_threshold"
		}
	}
	ms.LastAuthorityPromotionBlockReason = authorityPromotionBlockReason

	// Update failure streak.
	if period.WrongPeriodSuspect {
		ms.PeriodFailureStreak++
	} else {
		ms.PeriodFailureStreak = 0
	}
	ms.LastWrongPeriodSuspect = period.WrongPeriodSuspect

	// ── Reacquire logic ───────────────────────────────────────────────────────
	// Determines whether searchBestCandidate should run and whether its result
	// should replace the refined period (or be reported as diagnostic only).
	//
	// Layer 2: the applied period is sourced from the long-term estimator once
	// it is seeded. Until seeding, fall back to the raw short-window measurement
	// so behaviour at first refined entry is unchanged.
	finalPeriodS := period.AppliedPeriodS
	if ms.LongTermPeriodEstimateS > 0 {
		finalPeriodS = ms.LongTermPeriodEstimateS
	}
	var finalAlignment publishedAlignment
	finalAlignmentReady := false

	// period.DetrendedMADDeg is computed by fitPeriod over the same fit pool,
	// avoiding a duplicate weighted linear fit here.
	cleanUpdate := len(recent) > 0 &&
		float64(nRejected)/float64(len(recent)) < 0.25 &&
		period.FitPoolCount >= 6 &&
		period.FitICAOCount >= 2 &&
		period.DetrendedMADDeg <= reacquireMADThreshold

	// applyReacquireCandidate applies a reacquire search result if valid.
	// When dominantPeriodS > 0, candidates outside the refinement bound are
	// diagnostic only — they do not update the published period.
	applyReacquireCandidate := func(best candidateEvalResult) {
		ms.LastReacquireCandidateP = best.periodS
		ms.LastReacquireCandidateSc = best.score
		if best.periodS <= 0 {
			return
		}
		withinDominantBound := dominantPeriodS <= 0
		if dominantPeriodS > 0 {
			_, candidatePPM := periodDeltaToDominant(best.periodS, dominantPeriodS)
			withinDominantBound = math.Abs(candidatePPM) <= periodRefineMaxPPMFromDominant
		}
		if withinDominantBound {
			finalPeriodS = best.periodS
			// Layer 2: a reacquire candidate is a deliberate jump — reseed the
			// long-term estimator so it tracks the new family rather than
			// drifting toward it from the prior estimate.
			ms.reseedLongTermPeriodEstimator(best.periodS, nowUnix)
			finalAlignment = publishedAlignment{
				epochUS:                 best.newEpochUS,
				offsetDeg:               best.publishedOffsetDeg,
				anchorICAO:              best.anchorICAO,
				anchorScore:             best.anchorScore,
				anchorPhaseDeg:          best.anchorPhaseDeg,
				anchorCandidateCount:    best.anchorCandidateCount,
				anchorNoCandidateReason: best.anchorNoCandidateReason,
				anchorCandidates:        best.anchorCandidates,
			}
			finalAlignmentReady = true
		} else {
			// Candidate is outside dominant-prior bound: diagnostic only.
			ms.LastWrongPeriodSuspect = true
			ms.LastDominantPriorInconsistent = true
		}
	}

	if recoveryActive {
		ms.PeriodReacquireActive = true
		ms.PeriodReacquireReason = joinReasons(recoveryReasons)
		if cleanUpdate {
			ms.CleanReacquireStreak++
			if ms.CleanReacquireStreak >= reacquireMinClean {
				ms.PeriodReacquireActive = false
				ms.PeriodReacquireReason = ""
				ms.PeriodFailureStreak = 0
				ms.LastRecoveryModeActive = false
				ms.LastRecoveryTriggerReasons = nil
			}
		} else {
			ms.CleanReacquireStreak = 0
			best := ms.searchBestCandidate(prepared, seedEpochUS)
			applyReacquireCandidate(best)
		}
	} else {
		ms.CleanReacquireStreak = 0
		if ms.PeriodFailureStreak >= 3 || (ms.PeriodFailureStreak >= 2 && period.WrongPeriodSuspect) {
			ms.PeriodReacquireActive = true
			ms.PeriodReacquireReason = "failure_streak"
			ms.TrustUpdateStreak = 0
			ms.LastRecoveryModeActive = true
			ms.LastRecoveryTriggerReasons = appendUniqueStrings(ms.LastRecoveryTriggerReasons, "failure_streak")
			best := ms.searchBestCandidate(prepared, seedEpochUS)
			applyReacquireCandidate(best)
		} else {
			ms.LastReacquireCandidateP = 0
			ms.LastReacquireCandidateSc = 0
		}
	}

	// Trust promotion.
	trustedEnough := !majorityRejected &&
		len(recent) > 0 &&
		float64(nRejected)/float64(len(recent)) < trustMaxRejFrac &&
		period.FitPoolCount >= trustMinFitPool &&
		period.FitICAOCount >= trustMinICAOs &&
		period.DetrendedMADDeg <= trustMaxResidualDeg
	ms.applyTrustedBaseUpdate(finalPeriodS, trustedEnough, period.WrongPeriodSuspect)

	// Update basePeriodS after trust update.
	if ms.TrustedBasePeriodS > 0 {
		basePeriodS = ms.TrustedBasePeriodS
	} else if dominantPeriodS > 0 {
		basePeriodS = dominantPeriodS
	} else if compactPeriodS > 0 {
		basePeriodS = compactPeriodS
	} else if ms.BootstrapPeriodS > 0 {
		basePeriodS = ms.BootstrapPeriodS
	} else {
		basePeriodS = livePeriodS
	}

	// ── Stage 3: Phase branch solving ────────────────────────────────────────
	if !finalAlignmentReady {
		finalAlignment = ms.buildAlignmentForPeriod(prepared, seedEpochUS, seedOffsetDeg, finalPeriodS, nowUnix)
	}
	ms.LastAnchorCandidateCount = finalAlignment.anchorCandidateCount
	ms.LastAnchorNoCandidate = finalAlignment.anchorNoCandidateReason
	ms.LastAnchorCandidates = finalAlignment.anchorCandidates

	candidateEstimate := syncStateEstimate{
		present:     true,
		periodS:     finalPeriodS,
		epochUS:     finalAlignment.epochUS,
		offsetDeg:   finalAlignment.offsetDeg,
		anchorICAO:  finalAlignment.anchorICAO,
		anchorScore: finalAlignment.anchorScore,
		anchorPhase: finalAlignment.anchorPhaseDeg,
	}
	candidateMode := "tracking"
	if recoveryActive {
		candidateMode = "recovery"
	} else if ms.ActiveAuthorityMode == authorityModeCompact {
		candidateMode = "bootstrap"
	}

	validation := ms.evaluatePhaseValidation(prepared, candidateEstimate, finalAlignment.anchorCandidates)
	ms.LastBranchAmbiguityScore = validation.branchAmbiguity
	ms.LastAnchorCompetitionAmbiguity = validation.anchorCompetitionAmbiguity
	ms.LastCircularDispersionDeg = validation.circularDispersion
	ms.LastValidatorAgreementCount = validation.validatorAgreement
	ms.LastValidatorDisagreementCount = validation.validatorDisagree
	ms.LastValidatorExcludedCount = validation.validatorExcluded
	ms.LastPerICAOPhaseOffsets = validation.PerICAOOffsets
	ms.LastCandidateValidationStatus = validation.status
	ms.LastCandidateMode = candidateMode

	// Layer 1: snapshot the short-window candidate anchor / branch offset and
	// validator agreement count separately from the applied authoritative state.
	if finalAlignment.anchorICAO != nil {
		v := *finalAlignment.anchorICAO
		ms.LastLocalCandidateAnchorICAO = &v
	} else {
		ms.LastLocalCandidateAnchorICAO = nil
	}
	ms.LastLocalBranchOffsetDeg = finalAlignment.offsetDeg
	ms.LastLocalValidatorAgreement = validation.validatorAgreement

	// Layer 3 / fix: ingest the local candidate into the persistent branch
	// tracker, validation-aware. The dominant track populates LongTermBranch*
	// fields. Authority promotion (Layer 4) consumes accumulated branch
	// confidence rather than the per-window candidate.
	branchQuality := ms.LastLocalFitQualityScore
	var branchValidation LocalCandidateValidation
	switch {
	case validation.strong:
		branchValidation = LocalCandidateValidated
		branchQuality = math.Min(1.0, branchQuality+0.1)
	case validation.status == "validator_disagreement" || validation.status == "branch_ambiguous":
		branchValidation = LocalCandidateDisagreed
	case validation.weak:
		branchValidation = LocalCandidateWeak
		branchQuality *= 0.5
	default:
		branchValidation = LocalCandidateWeak
	}
	ms.updateBranchTracks(
		ms.LastLocalCandidateAnchorICAO,
		ms.LastLocalBranchOffsetDeg,
		branchQuality,
		nowUnix,
		branchValidation,
	)

	// ── Stage 4: Candidate and authoritative state update ─────────────────────
	ms.updateCandidateState(candidateEstimate, validation, candidateMode, nowUnix)
	ms.updateAuthoritativeState(candidateEstimate, validation, recoveryActive, nowUnix)

	// ── Publish sync state ────────────────────────────────────────────────────
	// The published period/phase depends on authority mode.
	publishedEstimate := candidateEstimate
	if ms.ActiveAuthorityMode == authorityModeCompact && sync != nil && sync.PeriodS > 0 {
		publishedEstimate = syncStateEstimate{
			present:   true,
			periodS:   sync.PeriodS,
			epochUS:   sync.PhaseEpochUS,
			offsetDeg: sync.PhaseOffsetDeg,
		}
	} else if ms.AuthoritativePresent {
		publishedEstimate = syncStateEstimate{
			present:     true,
			periodS:     ms.AuthoritativePeriodS,
			epochUS:     ms.AuthoritativePhaseEpochUS,
			offsetDeg:   ms.AuthoritativePhaseOffsetDeg,
			anchorICAO:  ms.AuthoritativeAnchorICAO,
			anchorScore: ms.AuthoritativeAnchorScore,
			anchorPhase: ms.AuthoritativeAnchorPhaseDeg,
		}
	}

	// Residual EMA update using the fit residuals returned by fitPeriod.
	var newResidualEMA float64
	if ms.ResidualEMADeg > 0 {
		newResidualEMA = ms.ResidualEMADeg
		for _, r := range period.FitResiduals {
			newResidualEMA = 0.85*newResidualEMA + 0.15*math.Abs(r)
		}
	} else if len(period.FitResiduals) > 0 {
		sum := 0.0
		for _, r := range period.FitResiduals {
			sum += math.Abs(r)
		}
		newResidualEMA = sum / float64(len(period.FitResiduals))
	} else {
		newResidualEMA = 5.0
	}

	ms.Present = true
	ms.Usable = period.FitPoolCount >= 4 && !majorityRejected
	ms.PeriodS = publishedEstimate.periodS
	ms.PeriodBaseS = basePeriodS
	ms.PhaseEpochUS = publishedEstimate.epochUS
	ms.PhaseOffsetDeg = publishedEstimate.offsetDeg
	if ms.AuthoritativePresent && ms.JitterDeg > 0 {
		ms.JitterDeg = clamp(0.92*ms.JitterDeg+0.08*newResidualEMA, 1.5, 20.0)
	} else {
		ms.JitterDeg = clamp(newResidualEMA, 1.5, 20.0)
	}
	ms.ResidualEMADeg = newResidualEMA
	ms.NSyncUpdates++
	ms.Holdover = period.FitPoolCount < 2
	ms.LastUpdated = nowUnix

	if candidateEstimate.anchorICAO != nil {
		ms.AnchorICAO = candidateEstimate.anchorICAO
		ms.AnchorPhaseDeg = candidateEstimate.anchorPhase
		ms.AnchorScore = candidateEstimate.anchorScore
	} else {
		ms.AnchorICAO = nil
		ms.AnchorPhaseDeg = 0
		ms.AnchorScore = 0
	}
	ms.LastAuthoritativeMode = "tentative_candidate"
	if ms.AuthoritativePresent {
		ms.LastAuthoritativeMode = "settled_authoritative"
	}

	ms.LastPeriodDeltaToDominantS, ms.LastPeriodDeltaToDominantPPM = periodDeltaToDominant(ms.PeriodS, dominantPeriodS)
	ms.LastCompactDeltaToDominantS, ms.LastCompactDeltaToDominantPPM = periodDeltaToDominant(compactPeriodS, dominantPeriodS)

	// AbsolutePhaseTrusted: true only when the period is bounded to the DF dominant
	// prior, anchor phase validation is strong, and branch ambiguity is low.
	// This is an OUTPUT of successful refined authority — not an input to promotion.
	ms.AbsolutePhaseTrusted = dominantPeriodS > 0 &&
		ms.ActiveAuthorityMode == authorityModeRefined &&
		ms.AuthoritativePresent &&
		math.Abs(ms.LastPeriodDeltaToDominantPPM) <= periodRefineMaxPPMFromDominant &&
		ms.AnchorICAO != nil &&
		validation.score >= authoritativeValidationStrong &&
		validation.branchAmbiguity < branchAmbiguityRejectThreshold &&
		(period.FitICAOCount < validatorAgreementMinCount || validation.validatorAgreement >= validatorAgreementMinCount)

	// ── Authority mode post-fit update ────────────────────────────────────────
	_, candidateDeltaToDominantPPM := periodDeltaToDominant(candidateEstimate.periodS, dominantPeriodS)
	dominantBoundOK := dominantPeriodS <= 0 || math.Abs(candidateDeltaToDominantPPM) <= periodRefineMaxPPMFromDominant

	// Layer 4: refined-healthy now also requires the long-term estimators to
	// have accumulated enough evidence. The estimators are seeded on the first
	// viable window, so these gates do not block bootstrap; they only delay
	// promotion until consecutive consistent windows have been observed.
	longTermPeriodHealthy := ms.LongTermPeriodEstimateS <= 0 ||
		ms.LongTermPeriodEstimatorConfidence >= periodEstimatorMinConfidence
	branchHealthy := len(ms.BranchTracks) == 0 ||
		(ms.BranchEstimatorConfidence >= branchPromotionMinConfidence &&
			!competitorTooClose(ms.BranchTracks, ms.BranchEstimatorConfidence))

	refinedHealthy := ms.Usable &&
		candidateEstimate.anchorICAO != nil &&
		period.FitPoolCount >= trustMinFitPool &&
		period.FitICAOCount >= validatorAgreementMinCount &&
		validation.strong &&
		dominantBoundOK &&
		!majorityRejected &&
		!period.WrongPeriodSuspect &&
		period.SlopeGatePassed &&
		longTermPeriodHealthy &&
		branchHealthy

	refinedFailure := !ms.Usable ||
		candidateEstimate.anchorICAO == nil ||
		validation.weak ||
		period.WrongPeriodSuspect ||
		majorityRejected ||
		period.DetrendedMADDeg > reacquireMADThreshold

	compactHealthy := false
	if sync != nil && compactPeriodS > 0 {
		_, compactDeltaPPM := periodDeltaToDominant(compactPeriodS, dominantPeriodS)
		compactHealthy = !sync.Holdover &&
			sync.ResidualEMA < reacquireMADThreshold &&
			sync.NRejectedFrames < recoveryRejectedFrameThreshold &&
			(dominantPeriodS <= 0 || math.Abs(compactDeltaPPM) <= authorityCompactHealthyDeltaPPM)
	}

	ms.updateAuthorityModePostFit(refinedHealthy, refinedFailure, compactHealthy, recoveryRequested, cleanUpdate, nowUnix)
	ms.LastRecoveryModeActive = ms.ActiveAuthorityMode == authorityModeRecovery
	ms.LastCompactGatingBypassed = ms.ActiveAuthorityMode == authorityModeRecovery

	// Final block reason (cleared once refined_authoritative with anchor is active).
	if ms.ActiveAuthorityMode == authorityModeRefined && ms.AuthoritativePresent && ms.AuthoritativeAnchorICAO != nil {
		ms.LastCandidateApplicationBlockReason = ""
	} else {
		ms.LastCandidateApplicationBlockReason = ms.candidateApplicationBlockReason(
			candidateEstimate, validation,
			period.SlopeGatePassed, dominantBoundOK,
			period.FitPoolCount, period.FitICAOCount,
			recoveryRequested, cleanUpdate,
			nowUnix,
		)
	}
	if ms.ActiveAuthorityMode != authorityModeRecovery && !recoveryRequested {
		ms.LastRecoveryTriggerReasons = nil
	}
}

// ─── package-level pure helpers ───────────────────────────────────────────────

// periodDeltaToDominant returns the absolute and PPM difference between periodS
// and dominantPeriodS. Returns (0, 0) when either is zero.
func periodDeltaToDominant(periodS, dominantPeriodS float64) (float64, float64) {
	if periodS <= 0 || dominantPeriodS <= 0 {
		return 0, 0
	}
	deltaS := periodS - dominantPeriodS
	return deltaS, deltaS / dominantPeriodS * 1e6
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func appendUniqueStrings(values []string, extras ...string) []string {
	out := append(append([]string(nil), values...), extras...)
	return uniqueStrings(out)
}

func joinReasons(reasons []string) string {
	if len(reasons) == 0 {
		return ""
	}
	return reasons[0]
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
	if den == 0 {
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
