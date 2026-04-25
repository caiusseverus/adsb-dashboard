package iid

// multisync_snapshot.go — diagnostic snapshot construction and protocol emission.
//
// This file is responsible only for copying internal solver state into a
// MultiSyncSnapshot for external consumers. It must not make decisions or mutate
// operational state.

import "math"

// MultiSyncSnapshot is a point-in-time view for protocol emission.
// All fields are copied atomically under the solver lock.
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
	// AnchorCompetitionAmbiguity is the raw secondScore/topScore from anchor-candidate
	// competition before global observation-coherence modifiers are applied.
	AnchorCompetitionAmbiguity       float64
	CircularDispersionDeg            float64
	ValidatorAgreementCount          int
	ValidatorDisagreementCount       int
	ValidatorExcludedCount           int
	// PerICAOPhaseOffsets is the per-ICAO validator table from the last run.
	// Each entry shows whether the ICAO was counted as agree/disagree/excluded and why.
	PerICAOPhaseOffsets              []PerICAOPhaseOffset
	CandidateValidationStatus        string
	CandidateApplicationBlockReason  string
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

	// ─── Long-term estimator separation (Layer 1: diagnostics) ────────────────
	// Local* fields are the short-window (current fit window) measurement.
	// LongTerm* / Branch* fields are the persistent estimator state. In Layer 1
	// LongTerm* are zero-valued placeholders; Layer 2/3 wire them to behaviour.
	LocalPeriodMeasurementS  float64
	LocalCandidateAnchorICAO *uint32
	LocalBranchOffsetDeg     float64
	LocalValidatorAgreement  int
	LocalFitQualityScore     float64

	LongTermPeriodEstimateS            float64
	LongTermPeriodEstimatorConfidence  float64
	LongTermPeriodEstimatorAgeS        float64
	ConsecutivePeriodConsistentWindows int
	PeriodUpdateDeltaS                 float64

	LongTermBranchAnchorICAO   *uint32
	LongTermBranchOffsetDeg    float64
	BranchEstimatorConfidence  float64
	BranchConsistentWindows    int
	BranchContradictionWindows int
	BranchCompetitorCount      int
	BranchPromotionBlockReason string
}

// Snapshot returns a copy of the published state for protocol emission.
// It is safe to call concurrently with AddObs and TryUpdate.
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
		AnchorCompetitionAmbiguity:       ms.LastAnchorCompetitionAmbiguity,
		CircularDispersionDeg:            ms.LastCircularDispersionDeg,
		ValidatorAgreementCount:          ms.LastValidatorAgreementCount,
		ValidatorDisagreementCount:       ms.LastValidatorDisagreementCount,
		ValidatorExcludedCount:           ms.LastValidatorExcludedCount,
		CandidateValidationStatus:        ms.LastCandidateValidationStatus,
		CandidateApplicationBlockReason:  ms.LastCandidateApplicationBlockReason,
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

		LocalPeriodMeasurementS: ms.LastLocalPeriodMeasurementS,
		LocalBranchOffsetDeg:    ms.LastLocalBranchOffsetDeg,
		LocalValidatorAgreement: ms.LastLocalValidatorAgreement,
		LocalFitQualityScore:    ms.LastLocalFitQualityScore,

		LongTermPeriodEstimateS:            ms.LongTermPeriodEstimateS,
		LongTermPeriodEstimatorConfidence:  ms.LongTermPeriodEstimatorConfidence,
		LongTermPeriodEstimatorAgeS:        ms.LongTermPeriodEstimatorAgeS,
		ConsecutivePeriodConsistentWindows: ms.ConsecutivePeriodConsistentWindows,
		PeriodUpdateDeltaS:                 ms.LastPeriodUpdateDeltaS,

		LongTermBranchOffsetDeg:    ms.LongTermBranchOffsetDeg,
		BranchEstimatorConfidence:  ms.BranchEstimatorConfidence,
		BranchConsistentWindows:    ms.BranchConsistentWindows,
		BranchContradictionWindows: ms.BranchContradictionWindows,
		BranchCompetitorCount:      ms.BranchCompetitorCount,
		BranchPromotionBlockReason: ms.BranchPromotionBlockReason,
	}
	if ms.LastLocalCandidateAnchorICAO != nil {
		v := *ms.LastLocalCandidateAnchorICAO
		snap.LocalCandidateAnchorICAO = &v
	}
	if ms.LongTermBranchAnchorICAO != nil {
		v := *ms.LongTermBranchAnchorICAO
		snap.LongTermBranchAnchorICAO = &v
	}
	if ms.AuthoritativeStateSinceTS > 0 && ms.LastUpdated > 0 {
		snap.AuthoritativeStateAgeS = math.Max(0, ms.LastUpdated-ms.AuthoritativeStateSinceTS)
	}
	for k, v := range ms.LastFitRejectReasons {
		snap.FitRejectReasons[k] = v
	}
	copy(snap.AnchorCandidates, ms.LastAnchorCandidates)
	if len(ms.LastPerICAOPhaseOffsets) > 0 {
		snap.PerICAOPhaseOffsets = make([]PerICAOPhaseOffset, len(ms.LastPerICAOPhaseOffsets))
		copy(snap.PerICAOPhaseOffsets, ms.LastPerICAOPhaseOffsets)
	}
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
