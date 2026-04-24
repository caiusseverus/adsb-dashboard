package iid

import "math"

// multisync_authority.go — authority state machine.
//
// This file is responsible only for the authority mode state machine and the
// candidate/authoritative state update. It consumes PeriodFitResult and
// PhaseBranchResult from earlier stages and produces an AuthorityDecision that
// applyResults writes into the published solver fields.
//
// Invariants:
//   - Authority decision does not rescore raw observations.
//   - AbsolutePhaseTrusted is an OUTPUT of successful refined authority, not a
//     prerequisite for entering it.
//   - Compact/dominant-recovery residual basis remains separate from refined
//     candidate-anchor diagnostics.

// AuthorityDecision captures the outputs of the authority state machine for one run.
// applyResults reads this struct and writes the corresponding solver fields.
type AuthorityDecision struct {
	ActiveAuthorityMode string

	// Applied sync state (what gets published).
	AppliedPeriodS       float64
	AppliedPhaseEpochUS  float64
	AppliedPhaseOffsetDeg float64
	AppliedAnchorICAO    *uint32

	// Promotion/demotion streaks (for diagnostics).
	CandidatePromotionStreak int
	RefinedHealthyStreak     int
	RefinedFailureStreak     int

	// AbsolutePhaseTrusted is set after refined_authoritative is achieved AND
	// phase validation is strong. It is not an input to promotion.
	AbsolutePhaseTrusted bool
	RefinedUsable        bool

	// Block reason for current run (why the candidate was not promoted, if any).
	PromotionBlockReason string
	LastSwitchReason     string
	Switched             bool

	// Gains applied to authoritative period/phase this run (for diagnostics).
	PeriodGainApplied float64
	PhaseGainApplied  float64
	PeriodFrozen      bool
}

// assessRecoveryMode determines whether the solver should enter dominant-period recovery.
// Returns (recoveryRequested, compactUnreliable, reasons).
//
// compactUnreliable is a separate diagnostic: it indicates that the compact sync cannot
// be trusted as a period seed even when recovery is not triggered (e.g. when dominantPeriodS
// is not available and the compact sync is just noisy).
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

// setAuthorityMode transitions to a new authority mode, respecting the minimum hold time.
// Recovery entry is exempt from the hold time.
func (ms *MultiSyncSolver) setAuthorityMode(mode, reason string, nowUnix float64) {
	ms.ensureAuthorityMode()
	if ms.ActiveAuthorityMode == mode {
		return
	}
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

// updateAuthorityModePostFit advances the authority mode state machine based on the
// health of the current fit. It is called after period and phase solving.
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
		if ms.RefinedHealthyStreak >= authorityPromoteRefinedStreak {
			ms.setAuthorityMode(authorityModeRefined, "recovery_converged_to_refined", nowUnix)
		} else if ms.RecoveryExitStreak >= authorityRecoveryExitStreak {
			if ms.CompactHealthyStreak >= authorityCompactReclaimStreak {
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

// candidateApplicationBlockReason returns a non-empty string explaining why the
// current candidate cannot be promoted to authoritative authority, or "" if
// promotion is possible.
func (ms *MultiSyncSolver) candidateApplicationBlockReason(
	estimate syncStateEstimate,
	validation syncValidationSummary,
	slopeGatePassed, dominantBoundOK bool,
	fitPoolCount, fitICAOCount int,
	recoveryRequested, cleanUpdate bool,
	nowUnix float64,
) string {
	if !estimate.present {
		return "period_not_stable"
	}
	if fitPoolCount < trustMinFitPool {
		return "insufficient_fit_observations"
	}
	if fitICAOCount < validatorAgreementMinCount {
		return "insufficient_fit_icaos"
	}
	if estimate.anchorICAO == nil {
		if ms.LastAnchorNoCandidate != "" {
			return "no_anchor_candidate"
		}
		return "anchor_candidate_invalid"
	}
	if validation.status != "validated" {
		switch validation.status {
		case "":
			return "validation_unavailable"
		case "validation_unavailable":
			return "validation_unavailable"
		case "insufficient_validator_agreement":
			return "validator_agreement_insufficient"
		case "validator_disagreement":
			return "validator_disagreement_too_high"
		case "branch_ambiguous":
			return "branch_ambiguous"
		case "weak_validation":
			return "weak_validation"
		default:
			return validation.status
		}
	}
	if !slopeGatePassed {
		return "slope_gate_failed"
	}
	if !dominantBoundOK {
		return "dominant_prior_bound_failed"
	}
	if ms.CandidatePromotionStreak < candidatePromotionMinStreak {
		return "candidate_streak_not_met"
	}
	if ms.ActiveAuthorityMode == authorityModeCompact {
		if ms.LastAuthoritySwitchTS > 0 && nowUnix-ms.LastAuthoritySwitchTS < authorityMinModeHoldS {
			return "authority_hold_time"
		}
		return "already_compact_authority"
	}
	if ms.ActiveAuthorityMode == authorityModeRecovery {
		if recoveryRequested || !cleanUpdate || ms.RecoveryExitStreak < authorityRecoveryExitStreak {
			return "recovery_streak_not_met"
		}
	}
	if ms.ActiveAuthorityMode == authorityModeRefined {
		return ""
	}
	return "period_not_stable"
}

// updateCandidateState advances the candidate phase state with the latest estimate.
// The promotion streak is incremented only when the estimate is stable AND validation
// is strong.
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

// updateAuthoritativeState applies the candidate estimate to the authoritative state
// via slow EMA gains. Promotion from candidate to authoritative requires a sufficient
// promotion streak and strong validation.
//
// Invariant: AbsolutePhaseTrusted is set by applyResults, not here — this function
// only updates the authoritative period/phase/anchor.
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
