package iid

// multisync_phase.go — absolute phase branch solving stage.
//
// This file is responsible only for selecting the best anchor ICAO, computing implied
// phase offsets per observation, and validating branch coherence. It does not change
// the period and does not enter or leave authority modes.
//
// Invariants:
//   - A selected anchor always has anchor_delta_deg == 0.0 in anchor-relative diagnostics.
//   - Branch ambiguity and validator agreement use the same fit-eligible validator population.
//   - Candidate anchor diagnostics are not operational authority until refined_authoritative.
//   - Active residual basis and candidate-anchor diagnostic basis are separate.

import (
	"math"
	"sort"
)

// PhaseBranchResult captures all outputs of the phase branch solving stage.
// It is passed to decideAuthority and applyResults without modification.
type PhaseBranchResult struct {
	// The candidate state estimate for this run.
	Estimate  syncStateEstimate
	Alignment publishedAlignment

	// Phase validation summary (validators close to anchor agree/disagree).
	Validation syncValidationSummary

	// Per-anchor candidates from selectAnchor.
	AnchorCandidates []AnchorCandidateSnapshot

	// Candidate mode label for diagnostics.
	CandidateMode string

	// Reacquire candidate, if the period stage triggered a search.
	// The phase stage carries this through for applyResults.
	ReacquireCandidateAlignment publishedAlignment
	ReacquireCandidateApplied   bool // true when the reacquire candidate replaced the period
}

// PerICAOPhaseOffset holds the implied phase offset and validator role for one ICAO.
// Exported for diagnostic use by protocol layers.
type PerICAOPhaseOffset struct {
	ICAO           uint32
	LatestOffsetDeg float64
	AnchorDeltaDeg *float64 // nil for non-anchor validators; 0.0 for the selected anchor
	Role           string   // "anchor" | "validator_agree" | "validator_disagree" | "validator_neutral" | "rejected"
	RejectReasons  []string
}

// solvePhaseBranch runs the phase branch solving stage. It selects the best anchor
// ICAO, computes the published alignment for the fitted period, and evaluates validator
// agreement.
//
// This stage must not modify ms.PeriodS, ms.AuthoritativePeriodS, or any authority streak.
func (ms *MultiSyncSolver) solvePhaseBranch(
	prepared []PreparedObservation,
	period PeriodFitResult,
	seedEpochUS, seedOffsetDeg float64,
	recoveryActive bool,
	nowUnix float64,
) PhaseBranchResult {
	r := PhaseBranchResult{}

	r.Alignment = ms.buildAlignmentForPeriod(prepared, seedEpochUS, seedOffsetDeg, period.AppliedPeriodS, nowUnix)
	r.AnchorCandidates = r.Alignment.anchorCandidates

	r.Estimate = syncStateEstimate{
		present:     true,
		periodS:     period.AppliedPeriodS,
		epochUS:     r.Alignment.epochUS,
		offsetDeg:   r.Alignment.offsetDeg,
		anchorICAO:  r.Alignment.anchorICAO,
		anchorScore: r.Alignment.anchorScore,
		anchorPhase: r.Alignment.anchorPhaseDeg,
	}

	r.Validation = ms.evaluatePhaseValidation(prepared, r.Estimate, r.AnchorCandidates)

	if recoveryActive {
		r.CandidateMode = "recovery"
	} else if ms.ActiveAuthorityMode == authorityModeCompact {
		r.CandidateMode = "bootstrap"
	} else {
		r.CandidateMode = "tracking"
	}

	return r
}

// evaluatePhaseValidation scores the candidate phase estimate by measuring how well
// non-anchor ICAOs agree with the proposed phase offset. It also computes global branch
// coherence to detect wrong-branch self-reinforcement.
//
// The same fit-eligible observation population is used for both branch ambiguity and
// validator agreement — they are not computed on different subsets.
func (ms *MultiSyncSolver) evaluatePhaseValidation(
	prepared []PreparedObservation,
	estimate syncStateEstimate,
	candidates []AnchorCandidateSnapshot,
) syncValidationSummary {
	summary := syncValidationSummary{
		score:              0,
		branchAmbiguity:    1,
		circularDispersion: 999,
		status:             "validation_unavailable",
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

	// Global phase analysis: circular mean and spread across ALL fit-eligible
	// observations to detect wrong-branch self-reinforcement.
	// If all ICAOs are on the same wrong branch they will all "validate" the
	// wrong anchor, so we need an independent global check here.
	periodUS := estimate.periodS * 1e6
	var gSinSum, gCosSum float64
	gN := 0
	for _, p := range prepared {
		if !p.FitEligible {
			continue
		}
		phaseRel := math.Mod((p.EffectiveUS-estimate.epochUS)/periodUS*360.0, 360.0)
		if phaseRel < 0 {
			phaseRel += 360.0
		}
		implied := math.Mod(p.Obs.BearingDeg-phaseRel+360.0, 360.0)
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
		if globalSpread > globalBranchMaxSpreadDeg {
			summary.branchAmbiguity = math.Max(summary.branchAmbiguity, 0.75)
		} else if globalSpread > 25.0 {
			summary.branchAmbiguity = math.Max(summary.branchAmbiguity, 0.55)
		}
		anchorVsGlobal := math.Abs(circularDiff(estimate.offsetDeg, globalMeanDeg))
		if anchorVsGlobal <= 12.0 && globalSpread <= 20.0 {
			globalCohesionBonus = 0.15
		} else if anchorVsGlobal > 30.0 {
			summary.branchAmbiguity = math.Max(summary.branchAmbiguity, 0.7)
		}
	}

	// Validator agreement: count non-anchor ICAOs that agree/disagree with the
	// proposed phase offset. Uses the same fit-eligible population as the global check.
	type validatorAccum struct {
		count int
		sin   float64
		cos   float64
	}
	byICAO := make(map[uint32]*validatorAccum)
	for _, p := range prepared {
		if !p.FitEligible || p.Obs.ICAO == *estimate.anchorICAO {
			continue
		}
		phaseRel := math.Mod((p.EffectiveUS-estimate.epochUS)/periodUS*360.0, 360.0)
		if phaseRel < 0 {
			phaseRel += 360.0
		}
		implied := math.Mod(p.Obs.BearingDeg-phaseRel+360.0, 360.0)
		diff := circularDiff(implied, estimate.offsetDeg)
		acc := byICAO[p.Obs.ICAO]
		if acc == nil {
			acc = &validatorAccum{}
			byICAO[p.Obs.ICAO] = acc
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
		summary.validatorEvaluated++
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
		summary.validatorAgreement > summary.validatorDisagree &&
		summary.branchAmbiguity < branchAmbiguityRejectThreshold &&
		summary.score >= authoritativeValidationStrong
	summary.weak = summary.score < authoritativeValidationWeak || summary.validatorAgreement == 0
	switch {
	case summary.validatorEvaluated == 0:
		summary.status = "validation_unavailable"
	case summary.validatorDisagree >= summary.validatorAgreement && summary.validatorDisagree > 0:
		summary.status = "validator_disagreement"
	case summary.validatorAgreement < validatorAgreementMinCount:
		summary.status = "insufficient_validator_agreement"
	case summary.branchAmbiguity >= branchAmbiguityRejectThreshold:
		summary.status = "branch_ambiguous"
	case summary.score < authoritativeValidationStrong:
		summary.status = "weak_validation"
	default:
		summary.status = "validated"
	}
	return summary
}

// buildAlignmentForPeriod computes a published phase alignment for the given period
// by fitting a phase correction over the fit-eligible observation pool and then
// selecting the best anchor ICAO.
func (ms *MultiSyncSolver) buildAlignmentForPeriod(
	prepared []PreparedObservation,
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
	for _, p := range prepared {
		predicted := msPredictBearing(seedEpochUS, seedOffsetDeg, periodUS, p.EffectiveUS)
		residual := circularDiff(p.Obs.BearingDeg, predicted)
		absResidual := math.Abs(residual)
		if p.FitEligible && p.EffectiveWeight > 0 {
			fitPool = append(fitPool, fitEntry{
				effectiveUS: p.EffectiveUS,
				residual:    residual,
				weight:      p.EffectiveWeight,
			})
		}
		if msClassifyResidual(absResidual) == "inlier" {
			nInliers++
		}
	}
	if len(fitPool) == 0 {
		for _, p := range prepared {
			if p.BaseWeight <= 0 || p.FitRejectReason == "near_wrap_residual" {
				continue
			}
			predicted := msPredictBearing(seedEpochUS, seedOffsetDeg, periodUS, p.EffectiveUS)
			fitPool = append(fitPool, fitEntry{
				effectiveUS: p.EffectiveUS,
				residual:    circularDiff(p.Obs.BearingDeg, predicted),
				weight:      p.BaseWeight * msICAOQualityMult(ms.ICAOQuality[p.Obs.ICAO]),
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
	return ms.buildPublishedAlignment(prepared, seedEpochUS, seedOffsetDeg, periodS, phaseAdjustDeg, nInliers, nowUnix, true)
}

// buildPublishedAlignment advances the epoch, applies a phase adjustment, and selects
// the best anchor ICAO.
func (ms *MultiSyncSolver) buildPublishedAlignment(
	prepared []PreparedObservation,
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
	for _, p := range prepared {
		if p.BaseWeight > 0 && p.FitRejectReason != "near_wrap_residual" {
			if p.EffectiveUS > out.epochUS {
				out.epochUS = p.EffectiveUS
			}
		}
	}
	if out.epochUS == 0 && len(prepared) > 0 {
		out.epochUS = prepared[len(prepared)-1].EffectiveUS
	}
	if out.epochUS == 0 {
		out.epochUS = seedEpochUS
	}

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
		prepared, out.epochUS, periodUS, mixedFallback, nowUnix, recordSelection,
	)
	return out
}

// selectAnchor chooses the best-quality ICAO to use as the phase anchor and returns
// the implied phase offset computed from that ICAO's observations.
//
// Invariant: the selected anchor always has anchor_delta_deg == 0.0 in any
// anchor-relative display — because the offset is defined as the anchor's own
// implied phase.
func (ms *MultiSyncSolver) selectAnchor(
	prepared []PreparedObservation,
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
	for _, p := range prepared {
		a := byICAO[p.Obs.ICAO]
		if a == nil {
			a = &icaoAnchor{icao: p.Obs.ICAO, rejectReasons: make(map[string]bool)}
			byICAO[p.Obs.ICAO] = a
		}
		a.totalObs++
		if p.FitEligible {
			a.fitEligibleCount++
		} else if p.FitRejectReason != "" {
			a.rejectReasons[p.FitRejectReason] = true
		}
		if p.Status == "rejected" || p.BaseWeight <= 0 || p.FitRejectReason == "near_wrap_residual" {
			continue
		}
		phaseRel := math.Mod((p.EffectiveUS-newEpochUS)/periodUS*360.0, 360.0)
		implied := math.Mod(p.Obs.BearingDeg-phaseRel, 360.0)
		if implied < 0 {
			implied += 360.0
		}
		impliedRad := implied * math.Pi / 180.0
		a.sinSum += math.Sin(impliedRad)
		a.cosSum += math.Cos(impliedRad)
		a.nObs++
		if !a.hasLatestImplied || p.EffectiveUS >= a.latestEffective {
			a.latestImplied = implied
			a.latestEffective = p.EffectiveUS
			a.hasLatestImplied = true
		}
		if p.BaseWeight > a.maxBaseW {
			a.maxBaseW = p.BaseWeight
		}
	}

	// Global phase branch center: ICAOs whose circular mean diverges from this
	// center are on a competing branch and must be rejected.
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

// recordAnchorSelection updates anchor switch tracking state.
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

// ─── reacquire candidate search ──────────────────────────────────────────────

// searchBestCandidate evaluates a set of nearby period candidates and returns the
// (period, score) pair with the best evidence from the current fit-eligible window.
// Called during wrong-period reacquire to escape an incorrect period family instead
// of snapping back to the (potentially wrong) bootstrap base period.
func (ms *MultiSyncSolver) searchBestCandidate(prepared []PreparedObservation, epochUS float64) candidateEvalResult {
	candidates := ms.buildCandidatePeriods()
	best := candidateEvalResult{periodS: ms.PeriodS, score: -1.0}
	for _, cand := range candidates {
		r := ms.evalCandidatePeriod(prepared, cand, epochUS)
		if r.score > best.score {
			best = r
		}
	}
	return best
}

// buildCandidatePeriods returns a deduplicated set of period values to probe
// during reacquire.
func (ms *MultiSyncSolver) buildCandidatePeriods() []float64 {
	probeOffsets := [8]float64{-0.020, -0.010, -0.005, -0.002, 0.002, 0.005, 0.010, 0.020}
	seen := make(map[int64]bool)
	var candidates []float64

	add := func(p float64) {
		if p < 0.5 || p > 20.0 {
			return
		}
		key := int64(p * 1e6)
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
func (ms *MultiSyncSolver) evalCandidatePeriod(prepared []PreparedObservation, candidatePeriodS, epochUS float64) candidateEvalResult {
	if candidatePeriodS <= 0 || len(prepared) == 0 {
		return candidateEvalResult{periodS: candidatePeriodS, score: -1}
	}
	periodUS := candidatePeriodS * 1e6

	var sinSum, cosSum float64
	n := 0
	for _, p := range prepared {
		if !p.FitEligible {
			continue
		}
		phaseInRot := math.Mod((p.EffectiveUS-epochUS)/periodUS*360.0, 360.0)
		if phaseInRot < 0 {
			phaseInRot += 360.0
		}
		impliedOffset := math.Mod(p.Obs.BearingDeg-phaseInRot+360.0, 360.0)
		rad := impliedOffset * math.Pi / 180.0
		sinSum += math.Sin(rad)
		cosSum += math.Cos(rad)
		n++
	}
	if n == 0 {
		return candidateEvalResult{periodS: candidatePeriodS, score: -1}
	}
	bestOffset := math.Mod(math.Atan2(sinSum, cosSum)*180.0/math.Pi+360.0, 360.0)

	icaos := make(map[uint32]bool)
	inliers, rejected := 0, 0
	var absResiduals []float64
	for _, p := range prepared {
		if !p.FitEligible {
			continue
		}
		predictedB := msPredictBearing(epochUS, bestOffset, periodUS, p.EffectiveUS)
		absR := math.Abs(circularDiff(p.Obs.BearingDeg, predictedB))
		absResiduals = append(absResiduals, absR)
		switch msClassifyResidual(absR) {
		case "inlier":
			inliers++
			icaos[p.Obs.ICAO] = true
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

	score := float64(inliers) * float64(icaoCount) / (1.0 + madDeg/5.0) / (1.0 + rejFrac*3.0)

	alignment := ms.buildPublishedAlignment(prepared, epochUS, bestOffset, candidatePeriodS, 0.0, inliers, 0, false)

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
