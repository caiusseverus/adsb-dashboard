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
//   - Motion compensation: Option B geometric guard — period correction requires
//     ≥3 contributing ICAOs and ≥30° bearing spread among them when the DF dominant
//     prior is active. Full velocity-vector compensation is deferred to a later slice.
//   - Anchor selection: highest-quality ICAO with good recent observations.
//   - Period refinement: slope EMA + persistence gate (matches Python).
//   - Wrong-period reacquire: state machine matching Python's reacquire logic.
//   - The refined model is a bounded correction around the DF dominant period family,
//     not an independent period estimator. Reacquire candidates outside the dominant
//     bound are diagnostic only; they do not update the published period.

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
	// These legacy bootstrap limits allow the solver to migrate families when no
	// authoritative external period family is provided by the DF alignment model.
	// When dominantPeriodS > 0 these values are overridden by the dominant-prior bounds below.
	periodPPMFromBootstrap       = 10000.0 // ±1% from bootstrap — legacy bootstrap phase gate
	periodPPMFromBootstrapStrong = 30000.0 // ±3% under strong fit — legacy bootstrap phase gate

	// Period refinement limits relative to the DF dominant prior (used when dominantPeriodS > 0).
	// These replace the bootstrap escape window and bound the refined period tightly around
	// the family already established by the DF alignment model. The refined solver is not an
	// independent period estimator; it is a bounded correction within the DF-assigned family.
	periodRefineMaxPPMFromDominant = 300.0 // max PPM deviation from DF dominant prior
	periodRefineMaxStepPPM         = 20.0  // max per-update period correction step when dominant is active

	// Motion compensation guard (Option B) — geometric safeguards for period correction
	// when the DF dominant prior is active. Requires ≥3 contributing ICAOs and sufficient
	// azimuth spread to reduce the risk of common-motion bias driving spurious corrections.
	// When dominant prior is not active, the legacy 2-ICAO minimum applies unchanged.
	periodRefineMotionGuardMinICAOs = 3    // min distinct ICAOs for period update when dominant is active
	periodRefineMinBearingSpreadDeg = 30.0 // min circular arc coverage of contributing ICAO bearings

	// Reacquire / failure detection.
	reacquireMADThreshold = 8.0 // deg — detrended MAD for recovery
	reacquireMinClean     = 2   // consecutive clean updates to exit reacquire

	recoveryResidualEMAThreshold     = 18.0
	recoveryRejectedFrameThreshold   = 4
	recoveryFitEligibleMin           = 4
	recoveryPeriodDeltaPPM           = 2500.0
	recoveryResidualRejectGate       = 55.0
	recoveryPositionAgeMaxS          = 15.0
	recoveryEntryFailureStreak       = 2
	recoveryAnchorStarvedCandidates  = 0
	recoveryAnchorStarvedFitEligible = 2
	// Authority hysteresis: longer streaks + minimum hold time prevent flapping.
	authorityPromoteRefinedStreak       = 6 // was 3 — need sustained health before promoting
	authorityRefinedFailureStreak       = 5 // was 3 — need sustained failure before dropping
	authorityCompactReclaimStreak       = 8 // was 5 — compact needs long healthy window to reclaim
	authorityRecoveryEntryStreak        = 2
	authorityRecoveryExitStreak         = 6 // was 4 — longer clean window to exit recovery
	authorityCompactHealthyDeltaPPM     = 1000.0
	authorityMinModeHoldS               = 15.0 // min seconds in any mode before switching (recovery exempt)
	authorityPromotionMaxSlopeDegPerS   = 0.05
	authorityPromotionMaxSlopeWindowDeg = 12.0

	anchorSwitchMinScoreDelta = 0.12
	anchorSwitchMinScoreRatio = 1.25
	anchorHoldMinUpdates      = 3
	anchorPoorSpreadDeg       = 18.0
	anchorPoorFitFraction     = 0.55

	// Anchor candidate hard-gates — tighter than the legacy score-only check.
	anchorMinScore       = 0.15 // was 0.1 — reject weak anchors earlier
	anchorMinFitEligible = 2    // at least 2 fit-eligible observations required
	anchorMinFitFraction = 0.18 // at least 18% of observations must be fit-eligible
	anchorMaxSpreadDeg   = 32.0 // circular spread ceiling for anchor candidates

	// Global branch clustering: reject anchors that diverge from the population consensus.
	// This breaks wrong-branch self-reinforcement where all ICAOs locally agree on a bad phase.
	globalBranchMatchDeg     = 35.0 // ICAO mean more than this from global center → rejected
	globalBranchMinObs       = 3    // minimum observations to trust global branch estimate
	globalBranchMaxSpreadDeg = 42.0 // global spread above this → multi-cluster / branch-ambiguous

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
	icaoQualityRejectMAD = 20.0 // deg — ICAO gets rejected from fit above this
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

type syncStateEstimate struct {
	present     bool
	periodS     float64
	epochUS     float64
	offsetDeg   float64
	anchorICAO  *uint32
	anchorScore float64
	anchorPhase float64
}

type syncValidationSummary struct {
	score              float64
	branchAmbiguity    float64
	circularDispersion float64
	validatorAgreement int
	validatorDisagree  int
	strong             bool
	weak               bool
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
	BootstrapPeriodS                 float64 // compact-sync seed captured at first run
	TrustedBasePeriodS               float64 // promoted from refined period after trustMinStreak updates; 0=not yet trusted
	TrustUpdateStreak                int     // consecutive updates meeting trust criteria
	BaseClamped                      bool    // true if the base-period clamp fired on the last run
	BaseClampDiffPPM                 float64 // raw PPM deviation that triggered (or would have triggered) the clamp
	WrongPeriodSuspect               bool    // true if wrong-period suspicion was raised on the last run
	ReacquireCandidatePeriod         float64 // period chosen by candidate search during reacquire (0 if not active)
	ReacquireCandidateScore          float64 // score of that candidate
	FitTotalObservations             int
	FitEligibleObservations          int
	FitRejectedObservations          int
	FitContributingICAOs             int
	FitWindowS                       float64
	DisplayWindowS                   float64
	FitSpanS                         float64
	ResidualSlopeDegPerS             float64
	FitRejectReasons                 map[string]uint64
	AnchorCandidateCount             int
	AnchorNoCandidateReason          string
	AnchorCandidates                 []AnchorCandidateSnapshot
	DominantPriorPeriodS             float64
	TrustedRefinedPeriodS            float64
	ActiveFamilyPriorPeriodS         float64
	ActiveFamilyPriorSource          string
	DominantPriorActive              bool
	CompactPeriodS                   float64
	PeriodDeltaToDominantS           float64
	PeriodDeltaToDominantPPM         float64
	CompactDeltaToDominantS          float64
	CompactDeltaToDominantPPM        float64
	CompactSyncUnreliable            bool
	RecoveryModeActive               bool
	RecoveryTriggerReasons           []string
	CompactGatingBypassed            bool
	RecoveryRelaxedAdmissions        int
	ActiveAuthorityMode              string
	AuthoritySwitchCount             int
	LastAuthoritySwitchTS            float64
	LastAuthoritySwitchReason        string
	AuthorityEnterStreak             int
	AuthorityExitStreak              int
	AnchorSwitchCount                int
	LastAnchorSwitchTS               float64
	LastAnchorSwitchReason           string
	AnchorHoldUpdates                int
	CandidatePeriodS                 float64
	AuthoritativePeriodS             float64
	CandidatePhaseOffsetDeg          float64
	AuthoritativePhaseOffsetDeg      float64
	CandidateAnchorICAO              *uint32
	AuthoritativeAnchorICAO          *uint32
	CandidateValidationScore         float64
	AuthoritativeValidationScore     float64
	AuthoritativeStateAgeS           float64
	CandidatePromotionStreak         int
	AuthoritativePeriodUpdateGain    float64
	AuthoritativePhaseUpdateGain     float64
	PeriodFrozenDueToPhaseValidation bool
	BranchAmbiguityScore             float64
	CircularDispersionDeg            float64
	ValidatorAgreementCount          int
	ValidatorDisagreementCount       int
	CandidateMode                    string
	AuthoritativeMode                string
	// AbsolutePhaseTrusted is true only when the period is bounded to the DF dominant prior,
	// an anchor is selected, and phase validation is strong. The localiser must gate
	// geographic bearing prediction on this flag rather than the broader Usable field.
	AbsolutePhaseTrusted bool
	// DominantPriorInconsistent is set when the reacquire candidate search finds evidence
	// for a period outside the allowed refinement bound around dominantPeriodS. This is
	// purely diagnostic — the DF alignment model should react to it, not the refined solver.
	DominantPriorInconsistent bool
	// PeriodRefineBlockReason records why period refinement was blocked on the last run.
	// Empty string means the slope candidate was evaluated (though it may have been clamped).
	// Populated with reasons such as "motion_guard_insufficient_icaos" or
	// "motion_guard_insufficient_geometry" for the motion-compensation guard.
	PeriodRefineBlockReason string
	// FitBearingSpreadDeg is the circular arc coverage of contributing ICAO mean bearings
	// from the last retained-slope fit. Used to diagnose the motion-guard geometry check.
	FitBearingSpreadDeg           float64
	PeriodFitAcceptedObservations int
	PeriodFitRejectedObservations int
	PeriodFitRejectedAfterUnwrap  int
	SlopeWindowDeg                float64
	SlopePromotionGatePassed      bool
	AuthorityPromotionBlockReason string
	ResidualCorrectionBasis       string
	MotionGuardDegraded           bool
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
	// BootstrapPeriodS is the compact-sync period captured on first run and never overwritten.
	// It is a fallback clamp base used only when no DF dominant prior is available.
	// When dominantPeriodS > 0, the dominant prior is the family authority; bootstrap
	// escape behaviour is suppressed and the clamp is anchored to dominantPeriodS.
	// TrustedBasePeriodS is promoted from the refined period after trustMinStreak consistent
	// multi-aircraft updates, but is also overridden by dominantPeriodS when present.
	BootstrapPeriodS   float64
	TrustedBasePeriodS float64
	TrustUpdateStreak  int

	// Per-run diagnostics (updated each solver run, readable via Snapshot).
	LastBaseClamped                      bool    // true if base-period clamp fired on the last run
	LastBaseClampDiffPPM                 float64 // raw PPM deviation that triggered (or would have triggered) the clamp
	LastWrongPeriodSuspect               bool    // true if wrong-period suspicion was raised on the last run
	LastReacquireCandidateP              float64 // period chosen by candidate search during reacquire (0 if not active)
	LastReacquireCandidateSc             float64 // score of that candidate period
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
	LastCircularDispersionDeg            float64
	LastValidatorAgreementCount          int
	LastValidatorDisagreementCount       int
	LastCandidateMode                    string
	LastAuthoritativeMode                string

	// Absolute phase trust: true only when period is within periodRefineMaxPPMFromDominant
	// of the DF dominant prior, anchor is valid, and phase validation is strong.
	AbsolutePhaseTrusted bool
	// LastDominantPriorInconsistent is set when the reacquire candidate lies outside the
	// allowed refinement bound around dominantPeriodS. Diagnostic only — the candidate
	// period is not published in this case.
	LastDominantPriorInconsistent bool
	// LastPeriodRefineBlockReason records the block reason from the most recent
	// refinePeriod call. Empty when refinement ran (slope candidate was evaluated).
	LastPeriodRefineBlockReason string
	// LastFitBearingSpreadDeg is the circular arc coverage of mean bearings among
	// contributing ICAOs from the most recent retained-slope fit.
	LastFitBearingSpreadDeg           float64
	LastPeriodFitAcceptedObservations int
	LastPeriodFitRejectedObservations int
	LastPeriodFitRejectedAfterUnwrap  int
	LastSlopeWindowDeg                float64
	LastSlopePromotionGatePassed      bool
	LastAuthorityPromotionBlockReason string
	LastResidualCorrectionBasis       string
	LastMotionGuardDegraded           bool

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
	ms.LastCircularDispersionDeg = 0
	ms.LastValidatorAgreementCount = 0
	ms.LastValidatorDisagreementCount = 0
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
		BootstrapPeriodS:                 ms.BootstrapPeriodS,
		TrustedBasePeriodS:               ms.TrustedBasePeriodS,
		TrustUpdateStreak:                ms.TrustUpdateStreak,
		BaseClamped:                      ms.LastBaseClamped,
		BaseClampDiffPPM:                 ms.LastBaseClampDiffPPM,
		WrongPeriodSuspect:               ms.LastWrongPeriodSuspect,
		ReacquireCandidatePeriod:         ms.LastReacquireCandidateP,
		ReacquireCandidateScore:          ms.LastReacquireCandidateSc,
		FitTotalObservations:             ms.LastFitTotalObs,
		FitEligibleObservations:          ms.LastFitEligibleObs,
		FitRejectedObservations:          ms.LastFitRejectedObs,
		FitContributingICAOs:             ms.LastFitContributingICAOs,
		FitWindowS:                       ms.LastFitWindowS,
		DisplayWindowS:                   ms.LastDisplayWindowS,
		FitSpanS:                         ms.LastFitSpanS,
		ResidualSlopeDegPerS:             ms.LastResidualSlopeDegPerS,
		FitRejectReasons:                 make(map[string]uint64, len(ms.LastFitRejectReasons)),
		AnchorCandidateCount:             ms.LastAnchorCandidateCount,
		AnchorNoCandidateReason:          ms.LastAnchorNoCandidate,
		AnchorCandidates:                 make([]AnchorCandidateSnapshot, len(ms.LastAnchorCandidates)),
		DominantPriorPeriodS:             ms.LastDominantPriorPeriodS,
		TrustedRefinedPeriodS:            ms.TrustedBasePeriodS,
		ActiveFamilyPriorPeriodS:         ms.LastActiveFamilyPriorPeriodS,
		ActiveFamilyPriorSource:          ms.LastActiveFamilyPriorSource,
		DominantPriorActive:              ms.LastDominantPriorActive,
		CompactPeriodS:                   ms.LastCompactPeriodS,
		PeriodDeltaToDominantS:           ms.LastPeriodDeltaToDominantS,
		PeriodDeltaToDominantPPM:         ms.LastPeriodDeltaToDominantPPM,
		CompactDeltaToDominantS:          ms.LastCompactDeltaToDominantS,
		CompactDeltaToDominantPPM:        ms.LastCompactDeltaToDominantPPM,
		CompactSyncUnreliable:            ms.LastCompactSyncUnreliable,
		RecoveryModeActive:               ms.LastRecoveryModeActive,
		RecoveryTriggerReasons:           append([]string(nil), ms.LastRecoveryTriggerReasons...),
		CompactGatingBypassed:            ms.LastCompactGatingBypassed,
		RecoveryRelaxedAdmissions:        ms.LastRecoveryRelaxedAdmissions,
		ActiveAuthorityMode:              ms.ActiveAuthorityMode,
		AuthoritySwitchCount:             ms.AuthoritySwitchCount,
		LastAuthoritySwitchTS:            ms.LastAuthoritySwitchTS,
		LastAuthoritySwitchReason:        ms.LastAuthoritySwitchReason,
		AuthorityEnterStreak:             ms.AuthorityEnterStreak,
		AuthorityExitStreak:              ms.AuthorityExitStreak,
		AnchorSwitchCount:                ms.AnchorSwitchCount,
		LastAnchorSwitchTS:               ms.LastAnchorSwitchTS,
		LastAnchorSwitchReason:           ms.LastAnchorSwitchReason,
		AnchorHoldUpdates:                ms.AnchorHoldUpdates,
		CandidatePeriodS:                 ms.CandidatePeriodS,
		AuthoritativePeriodS:             ms.AuthoritativePeriodS,
		CandidatePhaseOffsetDeg:          ms.CandidatePhaseOffsetDeg,
		AuthoritativePhaseOffsetDeg:      ms.AuthoritativePhaseOffsetDeg,
		CandidateValidationScore:         ms.CandidateValidationScore,
		AuthoritativeValidationScore:     ms.AuthoritativeValidationScore,
		CandidatePromotionStreak:         ms.CandidatePromotionStreak,
		AuthoritativePeriodUpdateGain:    ms.LastAuthoritativePeriodGain,
		AuthoritativePhaseUpdateGain:     ms.LastAuthoritativePhaseGain,
		PeriodFrozenDueToPhaseValidation: ms.LastPeriodFrozenDueToPhaseValidation,
		BranchAmbiguityScore:             ms.LastBranchAmbiguityScore,
		CircularDispersionDeg:            ms.LastCircularDispersionDeg,
		ValidatorAgreementCount:          ms.LastValidatorAgreementCount,
		ValidatorDisagreementCount:       ms.LastValidatorDisagreementCount,
		CandidateMode:                    ms.LastCandidateMode,
		AuthoritativeMode:                ms.LastAuthoritativeMode,
		AbsolutePhaseTrusted:             ms.AbsolutePhaseTrusted,
		DominantPriorInconsistent:        ms.LastDominantPriorInconsistent,
		PeriodRefineBlockReason:          ms.LastPeriodRefineBlockReason,
		FitBearingSpreadDeg:              ms.LastFitBearingSpreadDeg,
		PeriodFitAcceptedObservations:    ms.LastPeriodFitAcceptedObservations,
		PeriodFitRejectedObservations:    ms.LastPeriodFitRejectedObservations,
		PeriodFitRejectedAfterUnwrap:     ms.LastPeriodFitRejectedAfterUnwrap,
		SlopeWindowDeg:                   ms.LastSlopeWindowDeg,
		SlopePromotionGatePassed:         ms.LastSlopePromotionGatePassed,
		AuthorityPromotionBlockReason:    ms.LastAuthorityPromotionBlockReason,
		ResidualCorrectionBasis:          ms.LastResidualCorrectionBasis,
		MotionGuardDegraded:              ms.LastMotionGuardDegraded,
	}
	if ms.AuthoritativeStateSinceTS > 0 && ms.LastUpdated > 0 {
		snap.AuthoritativeStateAgeS = math.Max(0, ms.LastUpdated-ms.AuthoritativeStateSinceTS)
	}
	for k, v := range ms.LastFitRejectReasons {
		snap.FitRejectReasons[k] = v
	}
	copy(snap.AnchorCandidates, ms.LastAnchorCandidates)
	if ms.AnchorICAO != nil {
		v := *ms.AnchorICAO
		snap.AnchorICAO = &v
		snap.AnchorPhaseDeg = ms.AnchorPhaseDeg
		snap.AnchorScore = ms.AnchorScore
	}
	if ms.CandidateAnchorICAO != nil {
		v := *ms.CandidateAnchorICAO
		snap.CandidateAnchorICAO = &v
	}
	if ms.AuthoritativeAnchorICAO != nil {
		v := *ms.AuthoritativeAnchorICAO
		snap.AuthoritativeAnchorICAO = &v
	}
	return snap
}

// ─── internal solver ──────────────────────────────────────────────────────────

func (ms *MultiSyncSolver) runFit(sync *SyncState, dominantPeriodS, nowUnix float64) {
	ms.ensureAuthorityMode()
	ms.LastDominantPriorInconsistent = false
	// Determine seed phase from existing multi-sync state or frame sync.
	var seedEpochUS, seedOffsetDeg float64
	if ms.CandidatePresent && ms.CandidatePhaseEpochUS > 0 {
		seedEpochUS = ms.CandidatePhaseEpochUS
		seedOffsetDeg = ms.CandidatePhaseOffsetDeg
	} else if ms.AuthoritativePresent && ms.AuthoritativePhaseEpochUS > 0 {
		seedEpochUS = ms.AuthoritativePhaseEpochUS
		seedOffsetDeg = ms.AuthoritativePhaseOffsetDeg
	} else if ms.Present && ms.PhaseEpochUS > 0 {
		seedEpochUS = ms.PhaseEpochUS
		seedOffsetDeg = ms.PhaseOffsetDeg
	} else if sync != nil && sync.PeriodS > 0 {
		seedEpochUS = sync.PhaseEpochUS
		seedOffsetDeg = sync.PhaseOffsetDeg
	} else if ms.Present && ms.PeriodS > 0 {
		seedEpochUS = ms.PhaseEpochUS
		seedOffsetDeg = ms.PhaseOffsetDeg
	} else {
		return // no phase seed available yet
	}

	compactPeriodS := 0.0
	if sync != nil && sync.PeriodS > 0 {
		compactPeriodS = sync.PeriodS
	}

	// Record the compact-sync bootstrap period on the very first run.  This is never
	// overwritten so the solver always knows what family it started in, even after the
	// refined period has diverged considerably from it.
	if ms.BootstrapPeriodS <= 0 {
		if dominantPeriodS > 0 {
			ms.BootstrapPeriodS = dominantPeriodS
		} else if compactPeriodS > 0 {
			ms.BootstrapPeriodS = compactPeriodS
		} else if ms.Present && ms.PeriodS > 0 {
			// First run after a live restart — treat the existing refined period as bootstrap.
			ms.BootstrapPeriodS = ms.PeriodS
		}
	}

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
	activeFamilyPriorS, activeFamilyPriorSource := ms.selectActiveFamilyPrior(compactPeriodS, dominantPeriodS, ms.ActiveAuthorityMode)
	if activeFamilyPriorS <= 0 {
		return
	}
	livePeriodS := activeFamilyPriorS
	periodUS := livePeriodS * 1e6
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

	// Determine the effective clamp base and whether it is trusted.
	// TrustedBasePeriodS is promoted from the refined period only after trustMinStreak
	// consistent multi-aircraft updates; until then we clamp loosely against
	// BootstrapPeriodS so the solver can escape an incorrect seed family.
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

	// Dominant-prior family enforcement.
	// When the DF alignment period is known and the current live period has drifted
	// outside the ±300 PPM refinement band, snap back immediately. Slope-based
	// refinement cannot self-correct a large period error: if the model is settled at
	// the wrong period, observations score cleanly at that period, slope ≈ 0, and
	// refinePeriod's slope_not_persistent gate fires before the dominant clamp is
	// ever reached. We must force the correction here, before observations are scored.
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
		ms.LastFitWindowS = windowS
		ms.LastDisplayWindowS = multiSyncRetentionS
		ms.Holdover = true
		return
	}

	// Score each observation.
	scored := make([]scoredObs, 0, len(recent))
	fitRejectReasons := make(map[string]uint64)
	fitEligibleObs := 0
	recoveryRelaxedAdmissions := 0
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
		normalRejectReason := msFitRejectReason(o, status, absR, qEntry, false)
		fitRejectReason := msFitRejectReason(o, status, absR, qEntry, recoveryActive)
		var effectiveW float64
		switch status {
		case "inlier":
			effectiveW = baseW * qMult
		case "soft":
			effectiveW = baseW * 0.2 * qMult
		case "rejected":
			if recoveryActive && absR <= recoveryResidualRejectGate {
				effectiveW = baseW * 0.1 * qMult
			}
		}
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
		if fitRejectReason == "" {
			fitEligibleObs++
			if recoveryActive && normalRejectReason != "" {
				recoveryRelaxedAdmissions++
			}
		} else {
			fitRejectReasons[fitRejectReason]++
		}
	}
	ms.LastRecoveryRelaxedAdmissions = recoveryRelaxedAdmissions

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
		ms.LastFitTotalObs = len(recent)
		ms.LastFitEligibleObs = fitEligibleObs
		ms.LastFitRejectedObs = len(recent) - fitEligibleObs
		ms.LastFitContributingICAOs = 0
		ms.LastFitWindowS = windowS
		ms.LastDisplayWindowS = multiSyncRetentionS
		ms.LastFitSpanS = 0
		ms.LastFitRejectReasons = fitRejectReasons
		ms.LastAnchorCandidateCount = 0
		ms.LastAnchorNoCandidate = "no_fit_pool"
		ms.LastAnchorCandidates = nil
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
	ms.LastFitTotalObs = len(recent)
	ms.LastFitEligibleObs = fitEligibleObs
	ms.LastFitRejectedObs = len(recent) - fitEligibleObs
	ms.LastFitContributingICAOs = len(fitICAOs)
	ms.LastFitWindowS = windowS
	ms.LastDisplayWindowS = multiSyncRetentionS
	ms.LastFitSpanS = spanS
	ms.LastFitRejectReasons = fitRejectReasons

	prevSlope := ms.SmoothSlopeDegPerS
	smoothedSlope := (1.0-slopeEMAAlpha)*prevSlope + slopeEMAAlpha*bFit

	periodSlope, periodFitCount, periodFitICAOs, periodFitSpanS, periodFitBearingSpreadDeg, periodSlopeOK := ms.retainedResidualSlope(
		seedEpochUS, seedOffsetDeg, livePeriodS, nowUnix, recoveryActive,
	)
	if periodSlopeOK {
		rawRetainedSlope := periodSlope
		ms.SmoothSlopeDegPerS = (1.0-retainedSlopeEMAAlpha)*prevSlope + retainedSlopeEMAAlpha*rawRetainedSlope
		ms.LastResidualSlopeDegPerS = ms.SmoothSlopeDegPerS
		periodSlope = rawRetainedSlope
		ms.LastResidualCorrectionBasis = "wrapped_fit"
		ms.LastPeriodFitAcceptedObservations = periodFitCount
		ms.LastPeriodFitRejectedObservations = len(ms.obs) - periodFitCount
		ms.LastPeriodFitRejectedAfterUnwrap = 0
	} else {
		// Standard retained slope unavailable (often because near-wrap residuals dominate).
		// Try the unwrapped per-ICAO slope — captures slope even when residuals wrap
		// through ±180°, visible as diagonal bands in the residual chart.
		uwSlope, uwAcc, uwRej, uwRejAfterUnwrap, uwICAOs, uwBearingSpread, uwSpanS, uwOK :=
			ms.retainedResidualSlopeUnwrapped(seedEpochUS, seedOffsetDeg, livePeriodS, nowUnix)
		ms.LastPeriodFitAcceptedObservations = uwAcc
		ms.LastPeriodFitRejectedObservations = uwRej
		ms.LastPeriodFitRejectedAfterUnwrap = uwRejAfterUnwrap
		if uwOK {
			ms.SmoothSlopeDegPerS = (1.0-retainedSlopeEMAAlpha)*prevSlope + retainedSlopeEMAAlpha*uwSlope
			ms.LastResidualSlopeDegPerS = ms.SmoothSlopeDegPerS
			periodSlope = uwSlope
			periodFitCount = uwAcc
			periodFitICAOs = uwICAOs
			periodFitSpanS = uwSpanS
			periodFitBearingSpreadDeg = uwBearingSpread
			ms.LastResidualCorrectionBasis = "unwrapped_fit"
		} else {
			periodSlope = smoothedSlope
			ms.SmoothSlopeDegPerS = periodSlope
			ms.LastResidualSlopeDegPerS = periodSlope
			periodFitCount = len(fitPool)
			periodFitICAOs = len(fitICAOs)
			periodFitSpanS = spanS
			periodFitBearingSpreadDeg = msBearingSpreadDeg(ms.obs, fitICAOs, nowUnix-multiSyncRetentionS)
			ms.LastResidualCorrectionBasis = "ema_fallback"
		}
	}
	ms.LastFitBearingSpreadDeg = periodFitBearingSpreadDeg
	ms.SlopeHistory = append(ms.SlopeHistory, periodSlope)
	if len(ms.SlopeHistory) > 12 {
		ms.SlopeHistory = ms.SlopeHistory[len(ms.SlopeHistory)-12:]
	}

	refinedPeriodS, refineBlockReason, _, clampedByBase, clampDiffPPM := ms.refinePeriod(
		livePeriodS, basePeriodS, dominantPeriodS, periodSlope,
		periodFitCount, periodFitICAOs, periodFitSpanS, periodFitBearingSpreadDeg,
		majorityRejected, trusted,
	)
	ms.LastPeriodRefineBlockReason = refineBlockReason

	// Wrong-period detection.
	detrended := msDetrendedMAD(scored, aFit, bFit, tRef)
	wrongPeriodSuspect := ms.assessPeriodFailure(len(recent), nRejected, len(fitPool), len(fitICAOs), majorityRejected, detrended)
	if wrongPeriodSuspect {
		ms.PeriodFailureStreak++
	} else {
		ms.PeriodFailureStreak = 0
	}
	ms.LastWrongPeriodSuspect = wrongPeriodSuspect

	finalPeriodS := refinedPeriodS
	var finalAlignment publishedAlignment
	finalAlignmentReady := false

	cleanUpdate := len(recent) > 0 &&
		float64(nRejected)/float64(len(recent)) < 0.25 &&
		len(fitPool) >= 6 &&
		len(fitICAOs) >= 2 &&
		detrended <= reacquireMADThreshold

	// applyReacquireCandidate tests whether the reacquire search result should be
	// published as the new live period, or whether it is diagnostic only.
	//
	// When dominantPeriodS > 0, the refined model must not escape the DF-aligned family.
	// If the best candidate is outside periodRefineMaxPPMFromDominant, it is reported as
	// "dominant prior inconsistent" — a signal for the DF alignment model to reacquire —
	// but it must not update the published period.
	//
	// When dominantPeriodS <= 0 (no DF alignment available), the old behaviour is
	// preserved so the solver can still migrate to a better family on its own.
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
			// The DF alignment model should be notified to reacquire.
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
			// On a clean update, the refined period passes through rather than
			// jumping to a candidate — the existing period is already converging.
		} else {
			ms.CleanReacquireStreak = 0
			// Search for the best-scoring period. If it is within the dominant-prior
			// bound it replaces the live period; otherwise it is diagnostic only.
			best := ms.searchBestCandidate(scored, seedEpochUS)
			applyReacquireCandidate(best)
		}
	} else {
		ms.CleanReacquireStreak = 0
		if ms.PeriodFailureStreak >= 3 || (ms.PeriodFailureStreak >= 2 && wrongPeriodSuspect) {
			ms.PeriodReacquireActive = true
			ms.PeriodReacquireReason = "failure_streak"
			ms.TrustUpdateStreak = 0
			ms.LastRecoveryModeActive = true
			ms.LastRecoveryTriggerReasons = appendUniqueStrings(ms.LastRecoveryTriggerReasons, "failure_streak")
			// Search candidate periods. If the best is within the dominant-prior
			// bound it replaces the live period; otherwise it is diagnostic only.
			best := ms.searchBestCandidate(scored, seedEpochUS)
			applyReacquireCandidate(best)
		} else {
			// Not in reacquire; clear stale candidate diagnostics.
			ms.LastReacquireCandidateP = 0
			ms.LastReacquireCandidateSc = 0
		}
	}

	// Trust promotion: once the solver has accumulated enough consistent multi-aircraft
	// evidence, promote the current published period to TrustedBasePeriodS.  From that
	// point the clamp tightens against the trusted base rather than the bootstrap seed.
	// This must use finalPeriodS, not the pre-publication refinedPeriodS, so the trusted
	// base never drifts toward a family that was not actually published this run.
	trustedEnough := !majorityRejected &&
		len(recent) > 0 &&
		float64(nRejected)/float64(len(recent)) < trustMaxRejFrac &&
		len(fitPool) >= trustMinFitPool &&
		len(fitICAOs) >= trustMinICAOs &&
		detrended <= trustMaxResidualDeg

	ms.applyTrustedBaseUpdate(finalPeriodS, trustedEnough, wrongPeriodSuspect)

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

	if !finalAlignmentReady {
		finalAlignment = ms.buildAlignmentForPeriod(scored, seedEpochUS, seedOffsetDeg, finalPeriodS, nowUnix)
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
	validation := ms.evaluatePhaseValidation(scored, candidateEstimate, finalAlignment.anchorCandidates)
	ms.LastBranchAmbiguityScore = validation.branchAmbiguity
	ms.LastCircularDispersionDeg = validation.circularDispersion
	ms.LastValidatorAgreementCount = validation.validatorAgreement
	ms.LastValidatorDisagreementCount = validation.validatorDisagree
	ms.LastCandidateMode = candidateMode

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

	ms.updateCandidateState(candidateEstimate, validation, candidateMode, nowUnix)
	ms.updateAuthoritativeState(candidateEstimate, validation, recoveryActive, nowUnix)

	publishedEstimate := candidateEstimate
	if ms.AuthoritativePresent {
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

	// Publish.
	ms.Present = true
	ms.Usable = len(fitPool) >= 4 && !majorityRejected
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
	ms.Holdover = len(fitPool) < 2
	ms.LastUpdated = nowUnix
	if publishedEstimate.anchorICAO != nil {
		ms.AnchorICAO = publishedEstimate.anchorICAO
		ms.AnchorPhaseDeg = publishedEstimate.anchorPhase
		ms.AnchorScore = publishedEstimate.anchorScore
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
	// The localiser must not use refined sync for geographic bearing prediction unless
	// this flag is set — Usable alone is insufficient since it covers relative sync only.
	ms.AbsolutePhaseTrusted = dominantPeriodS > 0 &&
		ms.AuthoritativePresent &&
		math.Abs(ms.LastPeriodDeltaToDominantPPM) <= periodRefineMaxPPMFromDominant &&
		ms.AnchorICAO != nil &&
		validation.score >= authoritativeValidationStrong &&
		validation.branchAmbiguity < branchAmbiguityRejectThreshold &&
		(len(fitICAOs) < validatorAgreementMinCount || validation.validatorAgreement >= validatorAgreementMinCount)

	// Slope gate for authority promotion.
	// Block promotion when the retained-window residual slope indicates period error.
	// This prevents the solver from entering refined_authoritative while the residual
	// chart still shows a clear slope — the most common cause of visible wrong-period lock.
	slopeWindowDeg := math.Abs(ms.LastResidualSlopeDegPerS) * periodFitSpanS
	ms.LastSlopeWindowDeg = slopeWindowDeg
	slopeGatePassed := math.Abs(ms.LastResidualSlopeDegPerS) <= authorityPromotionMaxSlopeDegPerS &&
		slopeWindowDeg <= authorityPromotionMaxSlopeWindowDeg
	ms.LastSlopePromotionGatePassed = slopeGatePassed
	authorityPromotionBlockReason := ""
	if !slopeGatePassed {
		if math.Abs(ms.LastResidualSlopeDegPerS) > authorityPromotionMaxSlopeDegPerS {
			authorityPromotionBlockReason = "slope_exceeds_promotion_threshold"
		} else {
			authorityPromotionBlockReason = "slope_window_exceeds_promotion_threshold"
		}
	}
	ms.LastAuthorityPromotionBlockReason = authorityPromotionBlockReason

	refinedHealthy := ms.Usable &&
		candidateEstimate.anchorICAO != nil &&
		len(fitPool) >= trustMinFitPool &&
		len(fitICAOs) >= 2 &&
		validation.score >= authoritativeValidationStrong &&
		!majorityRejected &&
		!wrongPeriodSuspect &&
		slopeGatePassed // block promotion when residual slope is too large
	refinedFailure := !ms.Usable ||
		candidateEstimate.anchorICAO == nil ||
		validation.weak ||
		wrongPeriodSuspect ||
		majorityRejected ||
		detrended > reacquireMADThreshold
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
	if ms.ActiveAuthorityMode != authorityModeRecovery && !recoveryRequested {
		ms.LastRecoveryTriggerReasons = nil
	}
}

func (ms *MultiSyncSolver) selectActiveFamilyPrior(compactPeriodS, dominantPeriodS float64, authorityMode string) (float64, string) {
	if authorityMode == authorityModeRefined {
		if ms.AuthoritativePresent && ms.AuthoritativePeriodS > 0 {
			return ms.AuthoritativePeriodS, "authoritative_refined"
		}
		if ms.CandidatePresent && ms.CandidatePeriodS > 0 {
			return ms.CandidatePeriodS, "candidate_refined"
		}
		if ms.TrustedBasePeriodS > 0 {
			return ms.TrustedBasePeriodS, "trusted_refined"
		}
		if ms.Present && ms.PeriodS > 0 {
			return ms.PeriodS, "current_refined"
		}
	}
	if authorityMode == authorityModeCompact {
		if dominantPeriodS > 0 {
			return dominantPeriodS, "dominant_live_df_seed"
		}
		if compactPeriodS > 0 {
			return compactPeriodS, "compact_seed"
		}
	}
	if ms.CandidatePresent && ms.CandidatePeriodS > 0 && authorityMode != authorityModeCompact {
		return ms.CandidatePeriodS, "candidate_refined"
	}
	if ms.AuthoritativePresent && ms.AuthoritativePeriodS > 0 && authorityMode != authorityModeCompact {
		return ms.AuthoritativePeriodS, "authoritative_refined"
	}
	if ms.TrustedBasePeriodS > 0 && authorityMode != authorityModeCompact {
		return ms.TrustedBasePeriodS, "trusted_refined"
	}
	if ms.Present && ms.PeriodS > 0 && !ms.Holdover {
		return ms.PeriodS, "current_refined"
	}
	if dominantPeriodS > 0 {
		return dominantPeriodS, "dominant_live_df"
	}
	if compactPeriodS > 0 {
		return compactPeriodS, "compact_seed"
	}
	if ms.BootstrapPeriodS > 0 {
		return ms.BootstrapPeriodS, "bootstrap_seed"
	}
	return 0, ""
}

func (ms *MultiSyncSolver) retainedResidualSlope(
	seedEpochUS, seedOffsetDeg, periodS, nowUnix float64,
	recoveryActive bool,
) (slope float64, nFit, nICAOs int, spanS, bearingSpreadDeg float64, ok bool) {
	// Estimate the common period-error slope across retained observations while
	// allowing each ICAO its own phase intercept. This keeps spurious per-aircraft
	// offsets from steering the hardware-period correction.
	// Also returns the circular arc coverage of contributing ICAO mean bearings for
	// the motion-compensation geometry guard.
	if periodS <= 0 {
		return 0, 0, 0, 0, 0, false
	}
	type entry struct {
		icao uint32
		x    float64
		y    float64
		w    float64
	}
	type accum struct {
		n      int
		sumW   float64
		sumX   float64
		sumY   float64
		sumSin float64 // weighted circular-mean bearing (sin component)
		sumCos float64 // weighted circular-mean bearing (cos component)
	}

	cutoffTS := nowUnix - multiSyncRetentionS
	periodUS := periodS * 1e6
	entries := make([]entry, 0, len(ms.obs))
	byICAO := make(map[uint32]*accum)
	minX := math.Inf(1)
	maxX := math.Inf(-1)
	for _, o := range ms.obs {
		if o.WallTS < cutoffTS {
			continue
		}
		effectiveUS := msPropCorrectedUS(float64(o.CentroidUS), float64(o.RangeNM))
		predicted := msPredictBearing(seedEpochUS, seedOffsetDeg, periodUS, effectiveUS)
		residual := circularDiff(o.BearingDeg, predicted)
		absR := math.Abs(residual)
		status := msClassifyResidual(absR)
		qEntry := ms.ICAOQuality[o.ICAO]
		rejectReason := msFitRejectReason(o, status, absR, qEntry, recoveryActive)
		if rejectReason != "" {
			continue
		}
		weight := msScoreObs(o) * msICAOQualityMult(qEntry)
		if weight <= 0 {
			continue
		}
		x := effectiveUS / 1e6
		entries = append(entries, entry{icao: o.ICAO, x: x, y: residual, w: weight})
		a := byICAO[o.ICAO]
		if a == nil {
			a = &accum{}
			byICAO[o.ICAO] = a
		}
		a.n++
		a.sumW += weight
		a.sumX += weight * x
		a.sumY += weight * residual
		rad := o.BearingDeg * math.Pi / 180.0
		a.sumSin += weight * math.Sin(rad)
		a.sumCos += weight * math.Cos(rad)
		if x < minX {
			minX = x
		}
		if x > maxX {
			maxX = x
		}
	}
	if len(entries) < periodRefineMinInlier || len(byICAO) < 2 {
		return 0, len(entries), len(byICAO), 0, 0, false
	}

	num := 0.0
	den := 0.0
	used := 0
	usedICAOs := make(map[uint32]bool, len(byICAO))
	for _, e := range entries {
		a := byICAO[e.icao]
		if a == nil || a.n < 2 || a.sumW <= 0 {
			continue
		}
		xMean := a.sumX / a.sumW
		yMean := a.sumY / a.sumW
		dx := e.x - xMean
		dy := e.y - yMean
		num += e.w * dx * dy
		den += e.w * dx * dx
		used++
		usedICAOs[e.icao] = true
	}
	if used < periodRefineMinInlier || len(usedICAOs) < 2 || den <= 0 ||
		math.IsNaN(num) || math.IsInf(num, 0) || math.IsNaN(den) || math.IsInf(den, 0) {
		span := 0.0
		if !math.IsInf(minX, 0) && !math.IsInf(maxX, 0) {
			span = maxX - minX
		}
		return 0, used, len(usedICAOs), span, 0, false
	}

	// Bearing diversity of contributing ICAOs (for motion-guard geometry check).
	bearingMeans := make([]float64, 0, len(usedICAOs))
	for icao := range usedICAOs {
		a := byICAO[icao]
		if a != nil && a.sumW > 0 {
			meanDeg := math.Atan2(a.sumSin/a.sumW, a.sumCos/a.sumW) * 180.0 / math.Pi
			bearingMeans = append(bearingMeans, meanDeg)
		}
	}
	return num / den, used, len(usedICAOs), maxX - minX, bearingCircularSpreadDeg(bearingMeans), true
}

// bearingCircularSpreadDeg returns the arc coverage (degrees) spanned by a set of
// bearings on a circle: 360° minus the largest gap between consecutive bearings.
// Returns 0 for fewer than 2 bearings.
func bearingCircularSpreadDeg(bearings []float64) float64 {
	if len(bearings) < 2 {
		return 0.0
	}
	sorted := make([]float64, len(bearings))
	for i, b := range bearings {
		r := math.Mod(b, 360.0)
		if r < 0 {
			r += 360.0
		}
		sorted[i] = r
	}
	sort.Float64s(sorted)
	maxGap := sorted[0] + 360.0 - sorted[len(sorted)-1]
	for i := 1; i < len(sorted); i++ {
		if gap := sorted[i] - sorted[i-1]; gap > maxGap {
			maxGap = gap
		}
	}
	return 360.0 - maxGap
}

// retainedResidualSlopeUnwrapped estimates the period-error slope by first
// unwrapping residuals per ICAO before fitting. It is called as a fallback when
// retainedResidualSlope cannot gather enough evidence because near-wrap residuals
// (abs ≥ 150°) dominate — the pattern visible as diagonal bands in the residual chart.
//
// Unlike retainedResidualSlope, this function does NOT reject near_wrap_residual
// observations. Instead it:
//  1. Applies only basic quality gates (pos age, ICAO quality, signal weight).
//  2. Per-ICAO: sorts by time and unwraps residuals to remove ±360° jumps.
//  3. Detrends each ICAO by subtracting its weighted mean unwrapped residual.
//  4. Rejects detrended observations that exceed periodFitUnwrappedResidualGate.
//  5. Fits a common slope via per-ICAO intercept correction (same as the standard path).
//
// Returns: slope (deg/s), nAccepted, nRejected (pre-unwrap), nRejAfterUnwrap,
// nFitICAOs, bearingSpreadDeg, fitSpanS, ok.
func (ms *MultiSyncSolver) retainedResidualSlopeUnwrapped(
	seedEpochUS, seedOffsetDeg, periodS, nowUnix float64,
) (slope float64, nAccepted, nRejected, nRejAfterUnwrap, nFitICAOs int, bearingSpreadDeg, fitSpanS float64, ok bool) {
	if periodS <= 0 {
		return
	}
	type rawObs struct {
		x          float64 // effectiveUS / 1e6
		y          float64 // raw circular residual ([-180, 180])
		w          float64 // observation weight
		bearingDeg float64
	}
	cutoffTS := nowUnix - multiSyncRetentionS
	periodUS := periodS * 1e6

	// Collect per-ICAO observations with basic quality gates only.
	// Residual magnitude is deliberately NOT used to reject here.
	byICAO := make(map[uint32][]rawObs)
	totalInput := 0
	for _, o := range ms.obs {
		if o.WallTS < cutoffTS {
			continue
		}
		w := msScoreObs(o) * msICAOQualityMult(ms.ICAOQuality[o.ICAO])
		if w <= 0 {
			continue
		}
		if float64(o.PosAgeS) > 8.0 {
			continue
		}
		q := ms.ICAOQuality[o.ICAO]
		if q != nil && q.ResidualMADDeg >= icaoQualityRejectMAD {
			continue
		}
		effectiveUS := msPropCorrectedUS(float64(o.CentroidUS), float64(o.RangeNM))
		predicted := msPredictBearing(seedEpochUS, seedOffsetDeg, periodUS, effectiveUS)
		residual := circularDiff(o.BearingDeg, predicted)
		byICAO[o.ICAO] = append(byICAO[o.ICAO], rawObs{
			x:          effectiveUS / 1e6,
			y:          residual,
			w:          w,
			bearingDeg: o.BearingDeg,
		})
		totalInput++
	}
	if len(byICAO) < 2 {
		return
	}

	type fitEntry struct {
		icao uint32
		x    float64
		y    float64 // detrended unwrapped residual
		w    float64
	}
	type bearingAccum struct{ sumSin, sumCos float64 }
	icaoBearings := make(map[uint32]*bearingAccum, len(byICAO))

	var entries []fitEntry
	minX := math.Inf(1)
	maxX := math.Inf(-1)

	for icao, obs := range byICAO {
		if len(obs) < 2 {
			nRejected += len(obs)
			continue
		}
		// Sort by time.
		sort.Slice(obs, func(i, j int) bool { return obs[i].x < obs[j].x })
		// Unwrap: remove circular jumps > ±180° between consecutive residuals.
		unwrapped := make([]float64, len(obs))
		unwrapped[0] = obs[0].y
		for i := 1; i < len(obs); i++ {
			diff := obs[i].y - obs[i-1].y
			for diff > 180.0 {
				diff -= 360.0
			}
			for diff < -180.0 {
				diff += 360.0
			}
			unwrapped[i] = unwrapped[i-1] + diff
		}
		// Weighted mean of unwrapped values for per-ICAO detrending.
		sumW, sumWY := 0.0, 0.0
		for i, o := range obs {
			sumW += o.w
			sumWY += o.w * unwrapped[i]
		}
		if sumW <= 0 {
			nRejected += len(obs)
			continue
		}
		meanY := sumWY / sumW

		ba := &bearingAccum{}
		icaoBearings[icao] = ba
		for i, o := range obs {
			detrended := unwrapped[i] - meanY
			if math.Abs(detrended) > periodFitUnwrappedResidualGate {
				nRejAfterUnwrap++
				continue
			}
			entries = append(entries, fitEntry{icao: icao, x: o.x, y: detrended, w: o.w})
			if o.x < minX {
				minX = o.x
			}
			if o.x > maxX {
				maxX = o.x
			}
			rad := o.bearingDeg * math.Pi / 180.0
			ba.sumSin += math.Sin(rad)
			ba.sumCos += math.Cos(rad)
		}
	}
	nAccepted = len(entries)
	nRejected = totalInput - nAccepted - nRejAfterUnwrap
	if nRejected < 0 {
		nRejected = 0
	}

	if nAccepted < periodRefineMinInlier {
		return
	}

	// Per-ICAO intercept correction (same approach as retainedResidualSlope).
	type icaoAccum struct {
		n    int
		sumW float64
		sumX float64
		sumY float64
	}
	icaoAccums := make(map[uint32]*icaoAccum, len(byICAO))
	for _, e := range entries {
		a := icaoAccums[e.icao]
		if a == nil {
			a = &icaoAccum{}
			icaoAccums[e.icao] = a
		}
		a.n++
		a.sumW += e.w
		a.sumX += e.w * e.x
		a.sumY += e.w * e.y
	}
	nFitICAOs = 0
	for _, a := range icaoAccums {
		if a.n >= 2 {
			nFitICAOs++
		}
	}
	if nFitICAOs < 2 {
		return
	}

	num, den := 0.0, 0.0
	used := 0
	for _, e := range entries {
		a := icaoAccums[e.icao]
		if a == nil || a.n < 2 || a.sumW <= 0 {
			continue
		}
		xMean := a.sumX / a.sumW
		yMean := a.sumY / a.sumW
		dx := e.x - xMean
		dy := e.y - yMean
		num += e.w * dx * dy
		den += e.w * dx * dx
		used++
	}
	if used < periodRefineMinInlier || den <= 0 || math.IsNaN(num) || math.IsNaN(den) || math.IsInf(num, 0) || math.IsInf(den, 0) {
		return
	}

	// Bearing diversity of contributing ICAOs for motion-guard geometry check.
	var bearingMeans []float64
	for icao := range icaoAccums {
		if icaoAccums[icao] == nil || icaoAccums[icao].n < 2 {
			continue
		}
		ba := icaoBearings[icao]
		if ba == nil {
			continue
		}
		total := math.Hypot(ba.sumSin, ba.sumCos)
		if total <= 0 {
			continue
		}
		meanDeg := math.Atan2(ba.sumSin, ba.sumCos) * 180.0 / math.Pi
		bearingMeans = append(bearingMeans, meanDeg)
	}
	bearingSpreadDeg = bearingCircularSpreadDeg(bearingMeans)
	if !math.IsInf(minX, 1) && !math.IsInf(maxX, -1) {
		fitSpanS = maxX - minX
	}
	slope = num / den
	ok = true
	return
}

// msBearingSpreadDeg computes the circular arc coverage of the mean bearings of a
// set of ICAOs across retained observations. Used by the motion-guard check in the
// EMA-slope fallback path where retainedResidualSlope did not return a fresh slope.
func msBearingSpreadDeg(obs []MultiSyncObs, icaos map[uint32]bool, cutoffTS float64) float64 {
	type bAccum struct{ sumSin, sumCos float64 }
	byICAO := make(map[uint32]*bAccum, len(icaos))
	for _, o := range obs {
		if o.WallTS < cutoffTS || !icaos[o.ICAO] {
			continue
		}
		a := byICAO[o.ICAO]
		if a == nil {
			a = &bAccum{}
			byICAO[o.ICAO] = a
		}
		rad := o.BearingDeg * math.Pi / 180.0
		a.sumSin += math.Sin(rad)
		a.sumCos += math.Cos(rad)
	}
	means := make([]float64, 0, len(byICAO))
	for _, a := range byICAO {
		means = append(means, math.Atan2(a.sumSin, a.sumCos)*180.0/math.Pi)
	}
	return bearingCircularSpreadDeg(means)
}

func (ms *MultiSyncSolver) assessRecoveryMode(sync *SyncState, dominantPeriodS float64) (bool, bool, []string) {
	reasons := make([]string, 0, 8)
	compactUnreliable := false
	compactPeriodS := 0.0
	if sync != nil && sync.PeriodS > 0 {
		compactPeriodS = sync.PeriodS
	}
	if sync == nil || compactPeriodS <= 0 {
		reasons = append(reasons, "compact_missing")
		compactUnreliable = true
	}
	if sync != nil {
		if sync.Holdover {
			reasons = append(reasons, "compact_holdover")
			compactUnreliable = true
		}
		if sync.ResidualEMA >= recoveryResidualEMAThreshold {
			reasons = append(reasons, "compact_residual_ema")
			compactUnreliable = true
		}
		if sync.NRejectedFrames >= recoveryRejectedFrameThreshold {
			reasons = append(reasons, "compact_rejected_frames")
			compactUnreliable = true
		}
	}
	if ms.Holdover {
		reasons = append(reasons, "refined_holdover")
	}
	if ms.ResidualEMADeg >= recoveryResidualEMAThreshold {
		reasons = append(reasons, "refined_residual_ema")
	}
	if ms.Present && ms.LastAnchorCandidateCount <= recoveryAnchorStarvedCandidates {
		reasons = append(reasons, "no_anchor_candidates")
	}
	if ms.Present && ms.LastFitEligibleObs <= recoveryAnchorStarvedFitEligible {
		reasons = append(reasons, "fit_pool_starved")
	}
	if ms.PeriodFailureStreak >= recoveryEntryFailureStreak {
		reasons = append(reasons, "failure_streak")
	}
	if dominantPeriodS > 0 {
		_, compactDeltaPPM := periodDeltaToDominant(compactPeriodS, dominantPeriodS)
		if math.Abs(compactDeltaPPM) >= recoveryPeriodDeltaPPM {
			reasons = append(reasons, "compact_dominant_delta")
			compactUnreliable = true
		}
		if ms.Present && ms.PeriodS > 0 {
			_, refinedDeltaPPM := periodDeltaToDominant(ms.PeriodS, dominantPeriodS)
			if math.Abs(refinedDeltaPPM) >= recoveryPeriodDeltaPPM {
				reasons = append(reasons, "refined_dominant_delta")
			}
		}
	}
	if ms.PeriodReacquireActive {
		reasons = append(reasons, "recovery_in_progress")
	}
	if dominantPeriodS <= 0 {
		return ms.PeriodReacquireActive, compactUnreliable, uniqueStrings(reasons)
	}
	hasFailure := compactUnreliable ||
		ms.Holdover ||
		ms.ResidualEMADeg >= recoveryResidualEMAThreshold ||
		(ms.Present && ms.LastAnchorCandidateCount <= recoveryAnchorStarvedCandidates) ||
		(ms.Present && ms.LastFitEligibleObs <= recoveryAnchorStarvedFitEligible) ||
		ms.PeriodFailureStreak >= recoveryEntryFailureStreak ||
		ms.PeriodReacquireActive
	_, compactDeltaPPM := periodDeltaToDominant(compactPeriodS, dominantPeriodS)
	_, refinedDeltaPPM := periodDeltaToDominant(ms.PeriodS, dominantPeriodS)
	largeDelta := math.Abs(compactDeltaPPM) >= recoveryPeriodDeltaPPM || math.Abs(refinedDeltaPPM) >= recoveryPeriodDeltaPPM
	return hasFailure && largeDelta, compactUnreliable, uniqueStrings(reasons)
}

func (ms *MultiSyncSolver) ensureAuthorityMode() {
	if ms.ActiveAuthorityMode == "" {
		ms.ActiveAuthorityMode = authorityModeCompact
	}
}

func (ms *MultiSyncSolver) setAuthorityMode(mode, reason string, nowUnix float64) {
	ms.ensureAuthorityMode()
	if ms.ActiveAuthorityMode == mode {
		return
	}
	// Enforce minimum hold time to prevent run-to-run flapping.
	// Recovery entry is exempt — it is urgent and must override the lockout.
	if mode != authorityModeRecovery && ms.LastAuthoritySwitchTS > 0 {
		if nowUnix-ms.LastAuthoritySwitchTS < authorityMinModeHoldS {
			return
		}
	}
	ms.ActiveAuthorityMode = mode
	ms.AuthoritySwitchCount++
	ms.LastAuthoritySwitchTS = nowUnix
	ms.LastAuthoritySwitchReason = reason
	ms.AuthorityEnterStreak = 0
	ms.AuthorityExitStreak = 0
}

func (ms *MultiSyncSolver) updateAuthorityModePostFit(
	refinedHealthy, refinedFailure, compactHealthy, recoveryRequested, cleanUpdate bool,
	nowUnix float64,
) {
	if refinedHealthy {
		ms.RefinedHealthyStreak++
	} else {
		ms.RefinedHealthyStreak = 0
	}
	if refinedFailure {
		ms.RefinedFailureStreak++
	} else {
		ms.RefinedFailureStreak = 0
	}
	if compactHealthy {
		ms.CompactHealthyStreak++
	} else {
		ms.CompactHealthyStreak = 0
	}

	switch ms.ActiveAuthorityMode {
	case authorityModeRecovery:
		if cleanUpdate && !recoveryRequested {
			ms.RecoveryExitStreak++
		} else {
			ms.RecoveryExitStreak = 0
		}
		ms.AuthorityExitStreak = ms.RecoveryExitStreak
		if ms.RecoveryExitStreak >= authorityRecoveryExitStreak {
			if ms.RefinedHealthyStreak >= authorityPromoteRefinedStreak {
				ms.setAuthorityMode(authorityModeRefined, "recovery_converged_to_refined", nowUnix)
			} else if ms.CompactHealthyStreak >= authorityCompactReclaimStreak {
				ms.setAuthorityMode(authorityModeCompact, "recovery_released_to_compact", nowUnix)
			}
		}
	case authorityModeCompact:
		ms.AuthorityEnterStreak = ms.RefinedHealthyStreak
		if ms.RefinedHealthyStreak >= authorityPromoteRefinedStreak {
			ms.setAuthorityMode(authorityModeRefined, "refined_usable_streak", nowUnix)
		}
	case authorityModeRefined:
		ms.AuthorityExitStreak = ms.RefinedFailureStreak
		if ms.RefinedFailureStreak >= authorityRefinedFailureStreak {
			if recoveryRequested {
				ms.setAuthorityMode(authorityModeRecovery, "refined_failure_streak", nowUnix)
				ms.RecoveryExitStreak = 0
			} else if ms.CompactHealthyStreak >= authorityCompactReclaimStreak {
				ms.setAuthorityMode(authorityModeCompact, "compact_healthy_reclaim", nowUnix)
			}
		}
	}
}

func (ms *MultiSyncSolver) applyTrustedBaseUpdate(finalPeriodS float64, trustedEnough, wrongPeriodSuspect bool) {
	if ms.PeriodReacquireActive || wrongPeriodSuspect {
		ms.TrustUpdateStreak = 0
		return
	}
	if !trustedEnough {
		// The field is explicitly consecutive: any update that misses the trust gate resets it.
		ms.TrustUpdateStreak = 0
		return
	}

	ms.TrustUpdateStreak++
	if ms.TrustUpdateStreak < trustMinStreak {
		return
	}
	if ms.TrustedBasePeriodS <= 0 {
		// First promotion: adopt the published period as the trusted base.
		ms.TrustedBasePeriodS = finalPeriodS
		return
	}

	// Slow EMA keeps the trusted base tracking a genuinely stable family
	// without snapping to transient fluctuations.
	ms.TrustedBasePeriodS = 0.98*ms.TrustedBasePeriodS + 0.02*finalPeriodS
}

func (ms *MultiSyncSolver) updateCandidateState(
	estimate syncStateEstimate,
	validation syncValidationSummary,
	candidateMode string,
	nowUnix float64,
) {
	stable := false
	if ms.CandidatePresent {
		_, deltaPPM := periodDeltaToDominant(estimate.periodS, ms.CandidatePeriodS)
		phaseDelta := math.Abs(circularDiff(estimate.offsetDeg, ms.CandidatePhaseOffsetDeg))
		sameAnchor := (ms.CandidateAnchorICAO == nil && estimate.anchorICAO == nil) ||
			(ms.CandidateAnchorICAO != nil && estimate.anchorICAO != nil && *ms.CandidateAnchorICAO == *estimate.anchorICAO)
		stable = math.Abs(deltaPPM) <= candidateStablePeriodPPM &&
			phaseDelta <= candidateStablePhaseDeg &&
			sameAnchor
	}
	if validation.strong {
		if stable {
			ms.CandidatePromotionStreak++
		} else {
			ms.CandidatePromotionStreak = 1
			ms.CandidateStateSinceTS = nowUnix
		}
	} else {
		ms.CandidatePromotionStreak = 0
		ms.CandidateStateSinceTS = nowUnix
	}
	if !ms.CandidatePresent || !stable {
		ms.CandidateStateSinceTS = nowUnix
	}
	ms.CandidatePresent = estimate.present
	ms.CandidatePeriodS = estimate.periodS
	ms.CandidatePhaseEpochUS = estimate.epochUS
	ms.CandidatePhaseOffsetDeg = estimate.offsetDeg
	ms.CandidateAnchorICAO = estimate.anchorICAO
	ms.CandidateAnchorScore = estimate.anchorScore
	ms.CandidateAnchorPhaseDeg = estimate.anchorPhase
	ms.CandidateValidationScore = validation.score
	ms.LastCandidateMode = candidateMode
}

func (ms *MultiSyncSolver) updateAuthoritativeState(
	candidate syncStateEstimate,
	validation syncValidationSummary,
	recoveryActive bool,
	nowUnix float64,
) {
	ms.LastAuthoritativePeriodGain = 0
	ms.LastAuthoritativePhaseGain = 0
	ms.LastPeriodFrozenDueToPhaseValidation = false
	if !candidate.present {
		return
	}

	promotionReady := validation.strong && ms.CandidatePromotionStreak >= candidatePromotionMinStreak
	if !ms.AuthoritativePresent {
		if validation.strong && ms.CandidatePromotionStreak >= authoritativeInitPromotionStreak {
			ms.AuthoritativePresent = true
			ms.AuthoritativePeriodS = candidate.periodS
			ms.AuthoritativePhaseEpochUS = candidate.epochUS
			ms.AuthoritativePhaseOffsetDeg = candidate.offsetDeg
			ms.AuthoritativeAnchorICAO = candidate.anchorICAO
			ms.AuthoritativeAnchorScore = candidate.anchorScore
			ms.AuthoritativeAnchorPhaseDeg = candidate.anchorPhase
			ms.AuthoritativeValidationScore = validation.score
			ms.AuthoritativeStateSinceTS = nowUnix
			ms.LastAuthoritativeMode = "authoritative_initialized"
		}
		return
	}

	if !promotionReady || recoveryActive {
		ms.LastPeriodFrozenDueToPhaseValidation = true
		ms.AuthoritativeValidationScore = 0.9*ms.AuthoritativeValidationScore + 0.1*validation.score
		ms.LastAuthoritativeMode = "authoritative_holding"
		return
	}

	periodGain := authoritativePeriodGain
	phaseGain := authoritativePhaseGain
	if validation.branchAmbiguity >= branchAmbiguityRejectThreshold {
		periodGain = 0
		phaseGain = 0
		ms.LastPeriodFrozenDueToPhaseValidation = true
	}
	if validation.score < authoritativeValidationStrong {
		periodGain = 0
		phaseGain *= 0.5
		ms.LastPeriodFrozenDueToPhaseValidation = true
	}
	ms.LastAuthoritativePeriodGain = periodGain
	ms.LastAuthoritativePhaseGain = phaseGain

	if periodGain > 0 && ms.AuthoritativePeriodS > 0 {
		maxStepS := ms.AuthoritativePeriodS * authoritativePeriodMaxStepPPM / 1e6
		targetDelta := candidate.periodS - ms.AuthoritativePeriodS
		clampedDelta := clamp(targetDelta, -maxStepS, maxStepS)
		ms.AuthoritativePeriodS += clampedDelta * periodGain
	}
	if phaseGain > 0 {
		phaseDelta := circularDiff(candidate.offsetDeg, ms.AuthoritativePhaseOffsetDeg)
		ms.AuthoritativePhaseOffsetDeg = math.Mod(ms.AuthoritativePhaseOffsetDeg+phaseDelta*phaseGain+360.0, 360.0)
		ms.AuthoritativePhaseEpochUS = candidate.epochUS
	}
	if candidate.anchorICAO != nil &&
		ms.CandidatePromotionStreak >= candidatePromotionMinStreak &&
		validation.branchAmbiguity < branchAmbiguityRejectThreshold {
		ms.AuthoritativeAnchorICAO = candidate.anchorICAO
		ms.AuthoritativeAnchorScore = candidate.anchorScore
		ms.AuthoritativeAnchorPhaseDeg = candidate.anchorPhase
	}
	ms.AuthoritativeValidationScore = 0.85*ms.AuthoritativeValidationScore + 0.15*validation.score
	ms.LastAuthoritativeMode = "authoritative_tracking"
}

func (ms *MultiSyncSolver) evaluatePhaseValidation(
	scored []scoredObs,
	estimate syncStateEstimate,
	candidates []AnchorCandidateSnapshot,
) syncValidationSummary {
	summary := syncValidationSummary{
		score:              0,
		branchAmbiguity:    1,
		circularDispersion: 999,
		weak:               true,
	}
	if !estimate.present || estimate.periodS <= 0 || estimate.anchorICAO == nil {
		return summary
	}

	topScore := 0.0
	secondScore := 0.0
	selectedSpread := 999.0
	for _, row := range candidates {
		if row.Status == "rejected" {
			continue
		}
		if row.Score > topScore {
			secondScore = topScore
			topScore = row.Score
		} else if row.Score > secondScore {
			secondScore = row.Score
		}
		if row.ICAO == *estimate.anchorICAO {
			selectedSpread = row.SpreadDeg
		}
	}
	if topScore > 0 {
		summary.branchAmbiguity = clamp(secondScore/topScore, 0, 1)
	}
	summary.circularDispersion = selectedSpread

	// Global phase analysis: compute circular mean and spread across ALL
	// fit-eligible observations to detect wrong-branch self-reinforcement.
	// If all ICAOs are on the same wrong branch they will all "validate" the
	// wrong anchor, so we need an independent global check here.
	periodUS := estimate.periodS * 1e6
	var gSinSum, gCosSum float64
	gN := 0
	for _, se := range scored {
		if !se.fitEligible {
			continue
		}
		phaseRel := math.Mod((se.effectiveUS-estimate.epochUS)/periodUS*360.0, 360.0)
		if phaseRel < 0 {
			phaseRel += 360.0
		}
		implied := math.Mod(se.o.BearingDeg-phaseRel+360.0, 360.0)
		rad := implied * math.Pi / 180.0
		gSinSum += math.Sin(rad)
		gCosSum += math.Cos(rad)
		gN++
	}
	globalCohesionBonus := 0.0
	if gN >= globalBranchMinObs {
		globalMeanDeg := math.Mod(math.Atan2(gSinSum, gCosSum)*180.0/math.Pi+360.0, 360.0)
		gR := math.Hypot(gSinSum, gCosSum) / float64(gN)
		globalSpread := 999.0
		if gR >= 1 {
			globalSpread = 0
		} else if gR > 0 {
			globalSpread = math.Sqrt(-2.0*math.Log(gR)) * 180.0 / math.Pi
		}
		// If global spread is large the phase population is multi-modal → raise ambiguity.
		if globalSpread > globalBranchMaxSpreadDeg {
			summary.branchAmbiguity = math.Max(summary.branchAmbiguity, 0.75)
		} else if globalSpread > 25.0 {
			summary.branchAmbiguity = math.Max(summary.branchAmbiguity, 0.55)
		}
		// If the estimate offset is near the global mean, give a cohesion bonus.
		anchorVsGlobal := math.Abs(circularDiff(estimate.offsetDeg, globalMeanDeg))
		if anchorVsGlobal <= 12.0 && globalSpread <= 20.0 {
			globalCohesionBonus = 0.15
		} else if anchorVsGlobal > 30.0 {
			// Estimate is far from global mean — likely wrong branch.
			summary.branchAmbiguity = math.Max(summary.branchAmbiguity, 0.7)
		}
	}

	type validatorAccum struct {
		count int
		sin   float64
		cos   float64
	}
	byICAO := make(map[uint32]*validatorAccum)
	for _, se := range scored {
		if !se.fitEligible || se.o.ICAO == *estimate.anchorICAO {
			continue
		}
		phaseRel := math.Mod((se.effectiveUS-estimate.epochUS)/periodUS*360.0, 360.0)
		if phaseRel < 0 {
			phaseRel += 360.0
		}
		implied := math.Mod(se.o.BearingDeg-phaseRel+360.0, 360.0)
		diff := circularDiff(implied, estimate.offsetDeg)
		acc := byICAO[se.o.ICAO]
		if acc == nil {
			acc = &validatorAccum{}
			byICAO[se.o.ICAO] = acc
		}
		acc.count++
		rad := diff * math.Pi / 180.0
		acc.sin += math.Sin(rad)
		acc.cos += math.Cos(rad)
	}
	for _, acc := range byICAO {
		if acc.count < validatorMinObsPerICAO {
			continue
		}
		mean := math.Atan2(acc.sin, acc.cos) * 180.0 / math.Pi
		absMean := math.Abs(mean)
		if absMean <= validatorAgreementPhaseDeg {
			summary.validatorAgreement++
		} else if absMean >= validatorDisagreementPhaseDeg {
			summary.validatorDisagree++
		}
	}

	agreementScore := clamp(float64(summary.validatorAgreement)/3.0, 0, 1)
	disagreementPenalty := clamp(float64(summary.validatorDisagree)/3.0, 0, 1)
	dispersionScore := 0.0
	if selectedSpread < 999 {
		dispersionScore = clamp(1.0-selectedSpread/30.0, 0, 1)
	}
	ambiguityScore := 1.0 - summary.branchAmbiguity
	summary.score = clamp(0.40*agreementScore+0.25*dispersionScore+0.20*ambiguityScore+globalCohesionBonus-0.35*disagreementPenalty, 0, 1)
	summary.strong = summary.validatorAgreement >= validatorAgreementMinCount &&
		summary.branchAmbiguity < branchAmbiguityRejectThreshold &&
		summary.score >= authoritativeValidationStrong
	summary.weak = summary.score < authoritativeValidationWeak || summary.validatorAgreement == 0
	return summary
}

func (ms *MultiSyncSolver) buildAlignmentForPeriod(
	scored []scoredObs,
	seedEpochUS, seedOffsetDeg, periodS, nowUnix float64,
) publishedAlignment {
	periodUS := periodS * 1e6
	if periodUS <= 0 {
		return publishedAlignment{epochUS: seedEpochUS, offsetDeg: seedOffsetDeg}
	}

	type fitEntry struct {
		effectiveUS float64
		residual    float64
		weight      float64
	}
	var fitPool []fitEntry
	nInliers := 0
	for _, se := range scored {
		predicted := msPredictBearing(seedEpochUS, seedOffsetDeg, periodUS, se.effectiveUS)
		residual := circularDiff(se.o.BearingDeg, predicted)
		absResidual := math.Abs(residual)
		if se.fitEligible && se.effectiveW > 0 {
			fitPool = append(fitPool, fitEntry{
				effectiveUS: se.effectiveUS,
				residual:    residual,
				weight:      se.effectiveW,
			})
		}
		if msClassifyResidual(absResidual) == "inlier" {
			nInliers++
		}
	}
	if len(fitPool) == 0 {
		for _, se := range scored {
			if se.baseW <= 0 || se.fitRejectReason == "near_wrap_residual" {
				continue
			}
			predicted := msPredictBearing(seedEpochUS, seedOffsetDeg, periodUS, se.effectiveUS)
			fitPool = append(fitPool, fitEntry{
				effectiveUS: se.effectiveUS,
				residual:    circularDiff(se.o.BearingDeg, predicted),
				weight:      se.baseW * msICAOQualityMult(ms.ICAOQuality[se.o.ICAO]),
			})
		}
	}
	if len(fitPool) == 0 {
		return publishedAlignment{epochUS: seedEpochUS, offsetDeg: seedOffsetDeg}
	}

	tRef := fitPool[0].effectiveUS
	for _, fe := range fitPool[1:] {
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
	phaseAdjustDeg, _ := weightedLinearFit(xs, ys, ws)
	alignment := ms.buildPublishedAlignment(
		scored,
		seedEpochUS,
		seedOffsetDeg,
		periodS,
		phaseAdjustDeg,
		nInliers,
		nowUnix,
		true,
	)
	return alignment
}

func (ms *MultiSyncSolver) buildPublishedAlignment(
	scored []scoredObs,
	seedEpochUS, seedOffsetDeg, periodS, phaseAdjustDeg float64,
	nInliers int,
	nowUnix float64,
	recordSelection bool,
) publishedAlignment {
	periodUS := periodS * 1e6
	if periodUS <= 0 {
		return publishedAlignment{epochUS: seedEpochUS, offsetDeg: seedOffsetDeg}
	}

	out := publishedAlignment{}
	// Advance epoch to the newest anchor-eligible observation.
	for _, se := range scored {
		if se.baseW > 0 && se.fitRejectReason != "near_wrap_residual" {
			if se.effectiveUS > out.epochUS {
				out.epochUS = se.effectiveUS
			}
		}
	}
	if out.epochUS == 0 && len(scored) > 0 {
		out.epochUS = scored[len(scored)-1].effectiveUS
	}
	if out.epochUS == 0 {
		out.epochUS = seedEpochUS
	}

	// Conservative phase-correction gain for the fallback branch.
	nEff := math.Max(float64(nInliers), 1.0)
	gain := math.Min(multiSyncGainBase+0.01*(nEff-1), multiSyncGainMax)
	existingAtNew := math.Mod((out.epochUS-seedEpochUS)/periodUS*360.0+seedOffsetDeg, 360.0)
	if existingAtNew < 0 {
		existingAtNew += 360.0
	}
	mixedFallback := math.Mod(existingAtNew+phaseAdjustDeg*gain, 360.0)
	if mixedFallback < 0 {
		mixedFallback += 360.0
	}
	out.offsetDeg, out.anchorICAO, out.anchorScore, out.anchorPhaseDeg, out.anchorCandidateCount, out.anchorNoCandidateReason, out.anchorCandidates = ms.selectAnchor(
		scored, out.epochUS, periodUS, mixedFallback, nowUnix, recordSelection,
	)
	return out
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
	newEpochUS, periodUS, fallbackOffset, nowUnix float64, recordSelection bool,
) (newOffset float64, anchorICAO *uint32, anchorScore, anchorPhaseDeg float64, candidateCount int, noCandidateReason string, candidates []AnchorCandidateSnapshot) {
	type icaoAnchor struct {
		icao             uint32
		sinSum           float64
		cosSum           float64
		nObs             int
		totalObs         int
		fitEligibleCount int
		latestImplied    float64
		latestEffective  float64
		hasLatestImplied bool
		maxBaseW         float64
		score            float64
		spreadDeg        float64
		fitFraction      float64
		rejectReasons    map[string]bool
	}
	byICAO := make(map[uint32]*icaoAnchor)
	for _, se := range scored {
		a := byICAO[se.o.ICAO]
		if a == nil {
			a = &icaoAnchor{icao: se.o.ICAO, rejectReasons: make(map[string]bool)}
			byICAO[se.o.ICAO] = a
		}
		a.totalObs++
		if se.fitEligible {
			a.fitEligibleCount++
		} else if se.fitRejectReason != "" {
			a.rejectReasons[se.fitRejectReason] = true
		}
		if se.status == "rejected" || se.baseW <= 0 || se.fitRejectReason == "near_wrap_residual" {
			continue
		}
		phaseRel := math.Mod((se.effectiveUS-newEpochUS)/periodUS*360.0, 360.0)
		implied := math.Mod(se.o.BearingDeg-phaseRel, 360.0)
		if implied < 0 {
			implied += 360.0
		}
		impliedRad := implied * math.Pi / 180.0
		a.sinSum += math.Sin(impliedRad)
		a.cosSum += math.Cos(impliedRad)
		a.nObs++
		if !a.hasLatestImplied || se.effectiveUS >= a.latestEffective {
			a.latestImplied = implied
			a.latestEffective = se.effectiveUS
			a.hasLatestImplied = true
		}
		if se.baseW > a.maxBaseW {
			a.maxBaseW = se.baseW
		}
	}

	// Compute the global phase branch center from all per-ICAO accumulators.
	// ICAOs whose circular mean diverges from this center are on a competing branch
	// and must be rejected to prevent wrong-branch self-reinforcement.
	var gSinSum, gCosSum float64
	gN := 0
	for _, a := range byICAO {
		if a.nObs > 0 {
			gSinSum += a.sinSum
			gCosSum += a.cosSum
			gN += a.nObs
		}
	}
	globalMeanDeg := 0.0
	globalSpreadDeg := 999.0
	globalBranchValid := false
	if gN >= globalBranchMinObs {
		globalMeanDeg = math.Mod(math.Atan2(gSinSum, gCosSum)*180.0/math.Pi+360.0, 360.0)
		gR := math.Hypot(gSinSum, gCosSum) / float64(gN)
		if gR >= 1 {
			globalSpreadDeg = 0
		} else if gR > 0 {
			globalSpreadDeg = math.Sqrt(-2.0*math.Log(gR)) * 180.0 / math.Pi
		}
		globalBranchValid = globalSpreadDeg <= globalBranchMaxSpreadDeg
	}

	var best *icaoAnchor
	for _, a := range byICAO {
		baseScore := msAnchorScore(ms.ICAOQuality[a.icao], a.maxBaseW)
		// Boost score by fit-eligible observation count: well-supported ICAOs are
		// strongly preferred over 2-observation candidates.  Factor = 1 + log₂(n),
		// capped at 4×, so a 10-obs ICAO is ≈ 2× stronger than a 2-obs ICAO.
		obsFactor := math.Min(4.0, 1.0+math.Log2(math.Max(1.0, float64(a.fitEligibleCount))))
		a.score = baseScore * obsFactor
		rejectReasons := make([]string, 0, len(a.rejectReasons)+3)
		for reason := range a.rejectReasons {
			rejectReasons = append(rejectReasons, reason)
		}
		sort.Strings(rejectReasons)
		fitFraction := 0.0
		if a.totalObs > 0 {
			fitFraction = float64(a.fitEligibleCount) / float64(a.totalObs)
		}
		a.fitFraction = fitFraction

		// Compute circular spread before status check so it can gate rejection.
		spreadDeg := 0.0
		if a.nObs > 1 {
			r := math.Hypot(a.sinSum, a.cosSum) / float64(a.nObs)
			if r > 0 && r < 1 {
				spreadDeg = math.Sqrt(-2.0*math.Log(r)) * 180.0 / math.Pi
			}
		}
		a.spreadDeg = spreadDeg

		status := "candidate"
		if a.fitEligibleCount == 0 {
			status = "rejected"
			if len(rejectReasons) == 0 {
				rejectReasons = append(rejectReasons, "no_fit_eligible_observations")
			}
		} else if a.fitEligibleCount < anchorMinFitEligible {
			status = "rejected"
			rejectReasons = append(rejectReasons, "insufficient_fit_eligible")
		} else if a.nObs == 0 {
			status = "rejected"
			rejectReasons = append(rejectReasons, "no_anchor_pool_observations")
		} else if a.score < anchorMinScore {
			status = "rejected"
			rejectReasons = append(rejectReasons, "anchor_score_below_threshold")
		} else if fitFraction < anchorMinFitFraction && a.fitEligibleCount < 4 {
			status = "rejected"
			rejectReasons = append(rejectReasons, "fit_fraction_below_threshold")
		} else if spreadDeg >= anchorMaxSpreadDeg && a.nObs >= 3 {
			status = "rejected"
			rejectReasons = append(rejectReasons, "spread_too_large")
		}
		// Reject ICAOs diverged from the global phase branch.
		// This prevents wrong-branch self-reinforcement where all ICAOs
		// cluster locally around an incorrect phase offset.
		if status == "candidate" && globalBranchValid && a.nObs > 0 {
			icaoMeanDeg := math.Mod(math.Atan2(a.sinSum, a.cosSum)*180.0/math.Pi+360.0, 360.0)
			if math.Abs(circularDiff(icaoMeanDeg, globalMeanDeg)) > globalBranchMatchDeg {
				status = "rejected"
				rejectReasons = append(rejectReasons, "diverges_from_global_branch")
			}
		}
		candidates = append(candidates, AnchorCandidateSnapshot{
			ICAO:                a.icao,
			Score:               a.score,
			SpreadDeg:           spreadDeg,
			ObsCount:            a.totalObs,
			FitEligibleCount:    a.fitEligibleCount,
			FitEligibleFraction: fitFraction,
			Status:              status,
			RejectReasons:       rejectReasons,
		})
		if status != "rejected" {
			candidateCount++
		}
		if status != "rejected" && (best == nil || a.score > best.score) {
			best = a
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Status != candidates[j].Status {
			return candidates[i].Status == "candidate"
		}
		if candidates[i].Score != candidates[j].Score {
			return candidates[i].Score > candidates[j].Score
		}
		if candidates[i].FitEligibleCount != candidates[j].FitEligibleCount {
			return candidates[i].FitEligibleCount > candidates[j].FitEligibleCount
		}
		return candidates[i].ICAO < candidates[j].ICAO
	})
	if best == nil || best.score < anchorMinScore {
		if len(candidates) == 0 {
			noCandidateReason = "no_scored_aircraft"
		} else if candidateCount == 0 {
			noCandidateReason = "no_anchor_candidates"
		}
		if recordSelection {
			ms.AnchorHoldUpdates = 0
		}
		return fallbackOffset, nil, 0, 0, candidateCount, noCandidateReason, candidates
	}

	if recordSelection && ms.AnchorICAO != nil && best.icao != *ms.AnchorICAO {
		var current *icaoAnchor
		for _, a := range byICAO {
			if a.icao == *ms.AnchorICAO {
				current = a
				break
			}
		}
		if current != nil && current.fitEligibleCount > 0 && current.nObs > 0 && current.score >= anchorMinScore {
			materiallyBetter := best.score >= current.score+anchorSwitchMinScoreDelta &&
				best.score >= current.score*anchorSwitchMinScoreRatio
			currentPoor := current.spreadDeg >= anchorPoorSpreadDeg ||
				current.fitFraction < anchorPoorFitFraction
			holdActive := ms.AnchorHoldUpdates < anchorHoldMinUpdates
			if !currentPoor && (holdActive || !materiallyBetter) {
				best = current
			}
		}
	}

	// Circular mean of implied phases.
	meanRad := math.Atan2(best.sinSum, best.cosSum)
	meanDeg := math.Mod(meanRad*180.0/math.Pi+360.0, 360.0)
	anchorPhaseDeg = meanDeg
	if best.hasLatestImplied {
		anchorPhaseDeg = best.latestImplied
	}

	icao := best.icao
	if recordSelection {
		ms.recordAnchorSelection(&icao, nowUnix)
	}
	return meanDeg, &icao, best.score, anchorPhaseDeg, candidateCount, "", candidates
}

func (ms *MultiSyncSolver) recordAnchorSelection(anchorICAO *uint32, nowUnix float64) {
	switch {
	case anchorICAO == nil:
		ms.AnchorHoldUpdates = 0
	case ms.AnchorICAO == nil:
		ms.AnchorHoldUpdates = 1
		ms.LastAnchorSwitchTS = nowUnix
		ms.LastAnchorSwitchReason = "initial_anchor"
	case *ms.AnchorICAO != *anchorICAO:
		ms.AnchorSwitchCount++
		ms.AnchorHoldUpdates = 1
		ms.LastAnchorSwitchTS = nowUnix
		ms.LastAnchorSwitchReason = "anchor_hysteresis_switch"
	default:
		ms.AnchorHoldUpdates++
	}
}

func (ms *MultiSyncSolver) refinePeriod(
	livePeriodS, basePeriodS, dominantPeriodS, smoothedSlope float64,
	nFit, nFitICAOs int, spanS, bearingSpreadDeg float64, majorityRejected bool,
	trusted bool, // true = tight clamp from promoted trusted base; false = wide clamp from bootstrap seed
	// When dominantPeriodS > 0, the dominant prior overrides the bootstrap/trusted-base
	// clamp with a tight bound (periodRefineMaxPPMFromDominant). This prevents the refined
	// period from escaping the DF-aligned family regardless of trusted state.
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

	// Degraded motion guard: when DF dominant prior is active but we have fewer than
	// periodRefineMotionGuardMinICAOs (3) ICAOs or insufficient bearing spread,
	// allow a degraded correction path (smaller gain and step) rather than hard-blocking.
	// This ensures period correction still runs during recovery/acquisition when only
	// 2 ICAOs are visible, albeit with reduced confidence.
	// A hard block still applies for fewer than 2 ICAOs (no multi-aircraft evidence at all).
	motionGuardDegraded := dominantPeriodS > 0 && blockReason == "" && ((nFitICAOs >= 2 && nFitICAOs < periodRefineMotionGuardMinICAOs) ||
		(nFitICAOs >= periodRefineMotionGuardMinICAOs && bearingSpreadDeg < periodRefineMinBearingSpreadDeg))
	ms.LastMotionGuardDegraded = motionGuardDegraded

	switch {
	case majorityRejected:
		blockReason = "majority_rejected"
	case nFit < periodRefineMinInlier:
		blockReason = "insufficient_fit_observations"
	case nFitICAOs < 2:
		blockReason = "insufficient_fit_icaos"
	// Motion-compensation guard (Option B): when the DF dominant prior is active,
	// require ≥3 ICAOs with sufficient azimuth spread for full-confidence correction.
	// When only 2 ICAOs are present or spread is low, the motionGuardDegraded flag
	// is set above and a reduced-gain correction path is used instead of a hard block.
	// This prevents period correction from being completely starved during acquisition.
	case spanS < periodRefineMinSpanRot*livePeriodS:
		blockReason = "insufficient_fit_span"
	case !persistent:
		blockReason = "slope_not_persistent"
	}
	if blockReason != "" || livePeriodS <= 0 {
		return
	}

	gainToUse := periodGain
	if motionGuardDegraded {
		gainToUse = periodGainDegraded
	}
	rateNominal := 360.0 / livePeriodS
	rateTarget := rateNominal + smoothedSlope*gainToUse
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
	// When the DF dominant prior is available, override the step and base clamps with
	// tight bounds anchored to the dominant family. This prevents the refined period from
	// escaping the family established by the DF alignment model regardless of trusted state.
	//
	// Motion compensation guard (Option B): when motionGuardDegraded is true (2 ICAOs
	// present but fewer than periodRefineMotionGuardMinICAOs=3, or bearing spread is low),
	// use a tighter per-update step (periodRefineDegradedMaxStepPPM) to reduce bias risk
	// from a small co-moving aircraft cluster. Full velocity-vector compensation is
	// deferred to a later slice.
	if dominantPeriodS > 0 {
		if motionGuardDegraded {
			ppmPerUpdate = periodRefineDegradedMaxStepPPM
		} else {
			ppmPerUpdate = periodRefineMaxStepPPM
		}
		basePeriodS = dominantPeriodS
		ppmFromBase = periodRefineMaxPPMFromDominant
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
func (ms *MultiSyncSolver) searchBestCandidate(scored []scoredObs, epochUS float64) candidateEvalResult {
	candidates := ms.buildCandidatePeriods()
	best := candidateEvalResult{periodS: ms.PeriodS, score: -1.0}
	for _, cand := range candidates {
		r := ms.evalCandidatePeriod(scored, cand, epochUS)
		if r.score > best.score {
			best = r
		}
	}
	return best
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

	anchors := [4]float64{ms.PeriodS, ms.BootstrapPeriodS, ms.TrustedBasePeriodS, ms.LastDominantPriorPeriodS}
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

	alignment := ms.buildPublishedAlignment(
		scored,
		epochUS,
		bestOffset,
		candidatePeriodS,
		0.0,
		inliers,
		0,
		false,
	)

	return candidateEvalResult{
		periodS:                 candidatePeriodS,
		inlierCount:             inliers,
		icaoCount:               icaoCount,
		madDeg:                  madDeg,
		rejFrac:                 rejFrac,
		score:                   score,
		phaseOffsetDeg:          bestOffset,
		publishedOffsetDeg:      alignment.offsetDeg,
		newEpochUS:              alignment.epochUS,
		anchorICAO:              alignment.anchorICAO,
		anchorScore:             alignment.anchorScore,
		anchorPhaseDeg:          alignment.anchorPhaseDeg,
		anchorCandidateCount:    alignment.anchorCandidateCount,
		anchorNoCandidateReason: alignment.anchorNoCandidateReason,
		anchorCandidates:        alignment.anchorCandidates,
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

func msFitRejectReason(o MultiSyncObs, status string, absR float64, q *ICAOSyncQuality, recoveryMode bool) string {
	if absR >= residualWrapDeg {
		return "near_wrap_residual"
	}
	rejectGate := residualRejectGate
	if recoveryMode {
		rejectGate = recoveryResidualRejectGate
	}
	if absR > rejectGate {
		return "residual_gate"
	}
	maxPosAgeS := 8.0
	if recoveryMode {
		maxPosAgeS = recoveryPositionAgeMaxS
	}
	if float64(o.PosAgeS) > maxPosAgeS {
		return "stale_position"
	}
	if q != nil && q.ResidualMADDeg >= icaoQualityRejectMAD && !recoveryMode {
		return "icao_quality_reject"
	}
	if status == "rejected" && !recoveryMode {
		return "residual_gate"
	}
	return ""
}

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
