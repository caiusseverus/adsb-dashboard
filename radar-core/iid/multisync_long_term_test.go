package iid

// multisync_long_term_test.go — tests for the long-term estimator separation
// (Layers 1–3). Layer 1 establishes the diagnostic surface: short-window
// `Local*` measurements distinct from `LongTerm*` / branch state. Layers 2/3
// add behaviour to those `LongTerm*` fields; tests for that behaviour are
// added in the same file alongside their layer.

import (
	"math"
	"strings"
	"testing"
)

// L1.1: Snapshot exposes the new Local* and LongTerm* fields with sane
// defaults on a fresh solver. Long-term fields are zero placeholders;
// Local* fields are zero until a fit has run.
func TestLayer1_FreshSolverSnapshotHasLongTermFieldsZeroed(t *testing.T) {
	ms := newSolver()
	snap := ms.Snapshot()

	if snap.LocalPeriodMeasurementS != 0 {
		t.Errorf("fresh solver: LocalPeriodMeasurementS = %v, want 0", snap.LocalPeriodMeasurementS)
	}
	if snap.LocalCandidateAnchorICAO != nil {
		t.Errorf("fresh solver: LocalCandidateAnchorICAO = %v, want nil", *snap.LocalCandidateAnchorICAO)
	}
	if snap.LocalFitQualityScore != 0 {
		t.Errorf("fresh solver: LocalFitQualityScore = %v, want 0", snap.LocalFitQualityScore)
	}
	if snap.LongTermPeriodEstimateS != 0 {
		t.Errorf("fresh solver: LongTermPeriodEstimateS = %v, want 0 placeholder", snap.LongTermPeriodEstimateS)
	}
	if snap.LongTermPeriodEstimatorConfidence != 0 {
		t.Errorf("fresh solver: LongTermPeriodEstimatorConfidence = %v, want 0", snap.LongTermPeriodEstimatorConfidence)
	}
	if snap.BranchEstimatorConfidence != 0 {
		t.Errorf("fresh solver: BranchEstimatorConfidence = %v, want 0", snap.BranchEstimatorConfidence)
	}
	if snap.BranchPromotionBlockReason != "" {
		t.Errorf("fresh solver: BranchPromotionBlockReason = %q, want empty", snap.BranchPromotionBlockReason)
	}
}

// L1.2: After a fit window with a clean pool, the Local* mirror fields are
// populated and Snapshot exposes them. In Layer 1 the applied period
// (PeriodS) is still derived from the same short-window measurement, so
// LocalPeriodMeasurementS must equal PeriodS within tight tolerance after a
// successful fit. (Layer 2 will diverge them.)
func TestLayer1_LocalMeasurementMirrorsAppliedAfterFit(t *testing.T) {
	ms := newSolver()
	periodS := 4.8
	icaos := []uint32{0xA01, 0xA02, 0xA03, 0xA04}

	// Seed a published refined state so runFit has a phase seed and is in a
	// position to update the local-measurement diagnostics.
	ms.Present = true
	ms.PeriodS = periodS
	ms.PhaseEpochUS = 0
	ms.PhaseOffsetDeg = 0
	ms.PeriodBaseS = periodS
	ms.BootstrapPeriodS = periodS

	// Push a few rotations of clean observations.
	now := 1_000_000.0
	for rot := 0; rot < 8; rot++ {
		baseUS := float64(rot) * periodS * 1e6
		for i, icao := range icaos {
			us := baseUS + float64(i)*periodS/float64(len(icaos))*1e6
			phaseRel := math.Mod(us/(periodS*1e6)*360.0, 360.0)
			if phaseRel < 0 {
				phaseRel += 360.0
			}
			ms.AddObs(MultiSyncObs{
				CentroidUS: us,
				ICAO:       icao,
				BearingDeg: phaseRel,
				RangeNM:    30.0,
				PosAgeS:    0.5,
				NReplies:   8,
				SignalDBFS: sig(-30.0),
				WallTS:     now + float64(rot)*periodS + float64(i)*0.05,
			})
		}
	}

	// Run the solver directly (bypassing TryUpdate's throttle) using the seed
	// period as both compact and dominant prior.
	sync := &SyncState{PeriodS: periodS, PhaseEpochUS: 0, PhaseOffsetDeg: 0}
	ms.runFit(sync, periodS, now+float64(8)*periodS+1.0)

	if ms.LastLocalPeriodMeasurementS <= 0 {
		t.Fatalf("LastLocalPeriodMeasurementS not populated after fit: %v", ms.LastLocalPeriodMeasurementS)
	}
	if ms.LastLocalFitQualityScore < 0 || ms.LastLocalFitQualityScore > 1 {
		t.Errorf("LastLocalFitQualityScore out of [0,1]: %v", ms.LastLocalFitQualityScore)
	}

	snap := ms.Snapshot()
	if snap.LocalPeriodMeasurementS != ms.LastLocalPeriodMeasurementS {
		t.Errorf("snapshot LocalPeriodMeasurementS = %v, solver = %v",
			snap.LocalPeriodMeasurementS, ms.LastLocalPeriodMeasurementS)
	}
	if snap.LocalFitQualityScore != ms.LastLocalFitQualityScore {
		t.Errorf("snapshot LocalFitQualityScore = %v, solver = %v",
			snap.LocalFitQualityScore, ms.LastLocalFitQualityScore)
	}
	// Layer 2: the long-term estimator is seeded by the first viable fit.
	// On bootstrap it equals the local measurement exactly (no divergence).
	if snap.LongTermPeriodEstimateS <= 0 {
		t.Errorf("Layer 2: LongTermPeriodEstimateS should be seeded after a clean fit, got %v",
			snap.LongTermPeriodEstimateS)
	}
	if math.Abs(snap.LongTermPeriodEstimateS-snap.LocalPeriodMeasurementS) > 1e-6 {
		t.Errorf("Layer 2 bootstrap: LongTermPeriodEstimateS (%v) should equal LocalPeriodMeasurementS (%v) at seed",
			snap.LongTermPeriodEstimateS, snap.LocalPeriodMeasurementS)
	}
	// Layer 3: branch tracker should have at least one track populated when an
	// anchor was selected. Confidence may be small but must be > 0.
	if snap.LocalCandidateAnchorICAO != nil && snap.BranchEstimatorConfidence <= 0 {
		t.Errorf("Layer 3: BranchEstimatorConfidence should be > 0 when local anchor selected, got %v",
			snap.BranchEstimatorConfidence)
	}
}

// L2.A: Noisy zero-mean local-measurement injections converge to a long-term
// estimate with strictly less variance than the input. This is the headline
// requirement of the redesign — the long-term estimator must smooth out
// short-window wobble.
func TestLayer2_LongTermEstimatorReducesNoiseVariance(t *testing.T) {
	ms := newSolver()
	truth := 4.8
	dominantS := truth

	// Seed with a clean window.
	ms.LastLocalPeriodMeasurementS = truth
	ms.LastLocalFitQualityScore = 1.0
	ms.updateLongTermPeriodEstimator(truth, dominantS, 1000.0, 1.0)
	if ms.LongTermPeriodEstimateS != truth {
		t.Fatalf("seed: LongTerm = %v, want %v", ms.LongTermPeriodEstimateS, truth)
	}

	// Inject 200 noisy zero-mean measurements (±30 PPM — realistic 30s-fit noise).
	rng := newDeterministicRNG(0x517B)
	const n = 200
	noisePPM := 30.0
	var sumLocalSq, sumLTSq float64
	for i := 0; i < n; i++ {
		offsetPPM := (rng.float64()*2 - 1) * noisePPM
		local := truth * (1.0 + offsetPPM*1e-6)
		ms.updateLongTermPeriodEstimator(local, dominantS, 1000.0+float64(i+1), 1.0)
		// Local variance about truth.
		dLocal := (local - truth) / truth * 1e6
		sumLocalSq += dLocal * dLocal
		// Long-term variance about truth.
		dLT := (ms.LongTermPeriodEstimateS - truth) / truth * 1e6
		sumLTSq += dLT * dLT
	}
	localVar := sumLocalSq / float64(n)
	ltVar := sumLTSq / float64(n)
	// Long-term variance must be at least 4× lower than the input.
	if ltVar*4 > localVar {
		t.Errorf("Long-term variance not sufficiently reduced: localVar=%.2f LTvar=%.2f (need LT*4 < local)",
			localVar, ltVar)
	}
	// Final estimate should be within ±50 PPM of truth.
	finalPPM := math.Abs(ms.LongTermPeriodEstimateS-truth) / truth * 1e6
	if finalPPM > 50.0 {
		t.Errorf("Long-term final estimate drifted: %v (PPM from truth = %.1f, allowed 50)",
			ms.LongTermPeriodEstimateS, finalPPM)
	}
	// Confidence should have grown high.
	if ms.LongTermPeriodEstimatorConfidence < 0.6 {
		t.Errorf("Confidence too low after %d consistent windows: %v", n, ms.LongTermPeriodEstimatorConfidence)
	}
}

// L2.C: A single bad 30s window (huge contradictory measurement) does not
// reset the long-term estimate. Confidence may decay; the estimate itself
// must remain near truth.
func TestLayer2_SingleBadWindowDoesNotResetLongTerm(t *testing.T) {
	ms := newSolver()
	truth := 4.8
	dominantS := truth

	// Seed and stabilise with 20 clean windows.
	ms.updateLongTermPeriodEstimator(truth, dominantS, 1000.0, 1.0)
	for i := 0; i < 20; i++ {
		ms.updateLongTermPeriodEstimator(truth, dominantS, 1000.0+float64(i+1), 1.0)
	}
	preEst := ms.LongTermPeriodEstimateS
	preConf := ms.LongTermPeriodEstimatorConfidence
	if math.Abs(preEst-truth) > 1e-6 {
		t.Fatalf("pre-attack: LongTerm = %v, want %v", preEst, truth)
	}

	// Inject one bad measurement: +250 PPM (well outside the consistency band).
	bad := truth * (1.0 + 250e-6)
	ms.updateLongTermPeriodEstimator(bad, dominantS, 1100.0, 0.6)

	// The estimate must still be within the dominant bound and broadly close
	// to truth — the EMA gain ensures one window can't yank it.
	postEst := ms.LongTermPeriodEstimateS
	driftPPM := math.Abs(postEst-truth) / truth * 1e6
	if driftPPM > 80.0 {
		t.Errorf("Single bad window pulled long-term too far: drift=%.1f PPM (estimate=%v, was=%v)",
			driftPPM, postEst, preEst)
	}
	// Confidence may have decayed but must be > 0.
	if ms.LongTermPeriodEstimatorConfidence <= 0 {
		t.Errorf("Confidence collapsed on single bad window: %v (was %v)",
			ms.LongTermPeriodEstimatorConfidence, preConf)
	}
	if ms.ConsecutivePeriodConsistentWindows != 0 {
		t.Errorf("ConsecutivePeriodConsistentWindows should reset on contradiction, got %d",
			ms.ConsecutivePeriodConsistentWindows)
	}
}

// L2.dominantBound: Long-term updates respect the ±300 PPM dominant bound.
func TestLayer2_LongTermBoundedByDominantPrior(t *testing.T) {
	ms := newSolver()
	dominantS := 4.8
	// Seed at the upper bound.
	upper := dominantS * (1.0 + 250e-6)
	ms.updateLongTermPeriodEstimator(upper, dominantS, 1000.0, 1.0)

	// Try to push way outside the bound.
	farLocal := dominantS * (1.0 + 5000e-6)
	for i := 0; i < 50; i++ {
		ms.updateLongTermPeriodEstimator(farLocal, dominantS, 1000.0+float64(i+1), 1.0)
	}

	maxAllowed := dominantS * (1.0 + periodRefineMaxPPMFromDominant*1e-6)
	if ms.LongTermPeriodEstimateS > maxAllowed+1e-9 {
		t.Errorf("Long-term escaped dominant bound: %v > %v", ms.LongTermPeriodEstimateS, maxAllowed)
	}
}

// deterministicRNG is a tiny xorshift32 for reproducible test noise.
type deterministicRNG struct{ s uint32 }

func newDeterministicRNG(seed uint32) *deterministicRNG {
	if seed == 0 {
		seed = 1
	}
	return &deterministicRNG{s: seed}
}

func (r *deterministicRNG) next() uint32 {
	r.s ^= r.s << 13
	r.s ^= r.s >> 17
	r.s ^= r.s << 5
	return r.s
}

func (r *deterministicRNG) float64() float64 {
	return float64(r.next()) / float64(^uint32(0))
}

// L3.B: A stable branch repeatedly seen across many fit windows accumulates
// branch-track confidence and becomes the dominant track.
func TestLayer3_StableBranchAccumulatesConfidence(t *testing.T) {
	ms := newSolver()
	anchor := uint32(0xA01)
	branchOffset := 137.0

	// Feed 30 windows of the same anchor + offset (with tiny noise).
	rng := newDeterministicRNG(0x42)
	for i := 0; i < 30; i++ {
		jitter := (rng.float64()*2 - 1) * 2.0 // ±2°
		ms.updateBranchTracks(&anchor, branchOffset+jitter, 1.0, 1000.0+float64(i))
	}

	if len(ms.BranchTracks) != 1 {
		t.Errorf("expected 1 track after 30 consistent windows, got %d", len(ms.BranchTracks))
	}
	if ms.BranchEstimatorConfidence < 0.6 {
		t.Errorf("BranchEstimatorConfidence too low: %v (want >=0.6)", ms.BranchEstimatorConfidence)
	}
	if ms.BranchConsistentWindows < 25 {
		t.Errorf("BranchConsistentWindows = %d (want >=25)", ms.BranchConsistentWindows)
	}
	if ms.LongTermBranchAnchorICAO == nil || *ms.LongTermBranchAnchorICAO != anchor {
		t.Errorf("dominant anchor not exposed: %v", ms.LongTermBranchAnchorICAO)
	}
	driftDeg := math.Abs(circularDiff(ms.LongTermBranchOffsetDeg, branchOffset))
	if driftDeg > 3.0 {
		t.Errorf("dominant offset drifted: %v vs target %v (delta %.2f°)",
			ms.LongTermBranchOffsetDeg, branchOffset, driftDeg)
	}
}

// L3.D: A sustained contradictory branch (different offset for many windows)
// eventually takes over from the original dominant track.
func TestLayer3_SustainedContradictionTakesOver(t *testing.T) {
	ms := newSolver()
	anchorA := uint32(0xA01)
	anchorB := uint32(0xB02)
	offsetA := 50.0
	offsetB := 200.0 // 150° away — outside match radius

	// Establish anchorA / offsetA dominant.
	for i := 0; i < 15; i++ {
		ms.updateBranchTracks(&anchorA, offsetA, 1.0, 1000.0+float64(i))
	}
	if ms.LongTermBranchAnchorICAO == nil || *ms.LongTermBranchAnchorICAO != anchorA {
		t.Fatalf("seed phase: dominant should be A, got %v", ms.LongTermBranchAnchorICAO)
	}
	preConf := ms.BranchEstimatorConfidence

	// Sustained competing branch B for 20 windows.
	for i := 0; i < 20; i++ {
		ms.updateBranchTracks(&anchorB, offsetB, 1.0, 2000.0+float64(i))
	}

	if ms.LongTermBranchAnchorICAO == nil || *ms.LongTermBranchAnchorICAO != anchorB {
		t.Errorf("after sustained contradiction, dominant should be B, got %v", ms.LongTermBranchAnchorICAO)
	}
	driftDeg := math.Abs(circularDiff(ms.LongTermBranchOffsetDeg, offsetB))
	if driftDeg > 5.0 {
		t.Errorf("dominant offset not aligned with B: %v vs %v (delta %.2f°)",
			ms.LongTermBranchOffsetDeg, offsetB, driftDeg)
	}
	if ms.BranchEstimatorConfidence < preConf*0.5 {
		t.Errorf("dominant confidence dropped sharply on takeover: %v (was %v)",
			ms.BranchEstimatorConfidence, preConf)
	}
}

// L3.cap: BranchTracks length never exceeds branchMaxTracks even when many
// distinct branches are seen across many windows. (Eviction order is
// confidence-driven; specific survivor identity is covered by the takeover
// test, not here.)
func TestLayer3_BranchTracksCappedAtMax(t *testing.T) {
	ms := newSolver()
	// Spawn 30 distinct branches at evenly-spaced offsets.
	for i := 0; i < 30; i++ {
		anchor := uint32(0xA00 + i)
		offset := math.Mod(float64(i)*40.0, 360.0)
		ms.updateBranchTracks(&anchor, offset, 1.0, 1000.0+float64(i))
		if len(ms.BranchTracks) > branchMaxTracks {
			t.Fatalf("BranchTracks exceeded cap %d at iteration %d: got %d",
				branchMaxTracks, i, len(ms.BranchTracks))
		}
	}
	if len(ms.BranchTracks) != branchMaxTracks {
		t.Errorf("final track count = %d, want %d", len(ms.BranchTracks), branchMaxTracks)
	}
}

// L4.A: Authority promotion is blocked when long-term period confidence is
// below threshold, with a specific block reason exposing the values.
func TestLayer4_PeriodEstimatorConfidenceGate(t *testing.T) {
	ms := newSolver()
	ms.ActiveAuthorityMode = authorityModeRefined
	ms.CandidatePromotionStreak = candidatePromotionMinStreak + 1
	// Long-term seeded but with low confidence (just below threshold).
	ms.LongTermPeriodEstimateS = 4.8
	ms.LongTermPeriodEstimatorConfidence = periodEstimatorMinConfidence - 0.05

	estimate := syncStateEstimate{
		present: true, periodS: 4.8, epochUS: 0, offsetDeg: 45.0,
		anchorICAO: ptr(uint32(0xA01)),
	}
	v := strongValidation()
	reason := ms.candidateApplicationBlockReason(
		estimate, v, true, true, trustMinFitPool+1, validatorAgreementMinCount+2, false, true, 1000.0,
	)
	if !strings.Contains(reason, "period_estimator_confidence_insufficient") {
		t.Errorf("expected period_estimator_confidence_insufficient block, got %q", reason)
	}
	if !strings.Contains(reason, "conf=") || !strings.Contains(reason, "required>=") {
		t.Errorf("block reason missing diagnostic numbers: %q", reason)
	}

	// Once confidence rises above threshold, the gate clears (other gates
	// must not fire — branch tracks are empty so branch gates pass).
	ms.LongTermPeriodEstimatorConfidence = periodEstimatorMinConfidence + 0.05
	reason = ms.candidateApplicationBlockReason(
		estimate, v, true, true, trustMinFitPool+1, validatorAgreementMinCount+2, false, true, 1000.0,
	)
	if reason != "" {
		t.Errorf("expected no block once period confidence sufficient, got %q", reason)
	}
}

// L4.B: Authority promotion is blocked when branch confidence is below threshold.
func TestLayer4_BranchConfidenceGate(t *testing.T) {
	ms := newSolver()
	ms.ActiveAuthorityMode = authorityModeRefined
	ms.CandidatePromotionStreak = candidatePromotionMinStreak + 1
	ms.LongTermPeriodEstimateS = 4.8
	ms.LongTermPeriodEstimatorConfidence = 0.9
	// Add a branch track but with low confidence.
	anchor := uint32(0xA01)
	ms.BranchTracks = []*BranchEstimate{{
		OffsetDeg: 45.0, AnchorICAO: &anchor,
		Confidence:                  branchPromotionMinConfidence - 0.05,
		ConsecutiveSupportedWindows: 1,
	}}
	ms.BranchEstimatorConfidence = branchPromotionMinConfidence - 0.05
	estimate := syncStateEstimate{
		present: true, periodS: 4.8, epochUS: 0, offsetDeg: 45.0,
		anchorICAO: &anchor,
	}
	reason := ms.candidateApplicationBlockReason(
		estimate, strongValidation(), true, true, trustMinFitPool+1, validatorAgreementMinCount+2, false, true, 1000.0,
	)
	if !strings.Contains(reason, "branch_confidence_insufficient") {
		t.Errorf("expected branch_confidence_insufficient block, got %q", reason)
	}
}

// L4.C: Authority promotion is blocked when a competing branch has comparable
// confidence to the dominant.
func TestLayer4_BranchCompetitorDominantGate(t *testing.T) {
	ms := newSolver()
	ms.ActiveAuthorityMode = authorityModeRefined
	ms.CandidatePromotionStreak = candidatePromotionMinStreak + 1
	ms.LongTermPeriodEstimateS = 4.8
	ms.LongTermPeriodEstimatorConfidence = 0.9
	// Two tracks; runner-up at 0.6 of dominant (above branchCompetitorMaxRelativeFrac=0.5).
	anchorA := uint32(0xA01)
	anchorB := uint32(0xB02)
	ms.BranchTracks = []*BranchEstimate{
		{OffsetDeg: 45.0, AnchorICAO: &anchorA, Confidence: 0.5},
		{OffsetDeg: 200.0, AnchorICAO: &anchorB, Confidence: 0.35},
	}
	ms.BranchEstimatorConfidence = 0.5
	estimate := syncStateEstimate{
		present: true, periodS: 4.8, epochUS: 0, offsetDeg: 45.0,
		anchorICAO: &anchorA,
	}
	reason := ms.candidateApplicationBlockReason(
		estimate, strongValidation(), true, true, trustMinFitPool+1, validatorAgreementMinCount+2, false, true, 1000.0,
	)
	if reason != "branch_competitor_dominant" {
		t.Errorf("expected branch_competitor_dominant block, got %q", reason)
	}

	// Decay competitor below threshold (e.g. 0.20/0.50 = 0.4 < 0.5).
	ms.BranchTracks[1].Confidence = 0.20
	reason = ms.candidateApplicationBlockReason(
		estimate, strongValidation(), true, true, trustMinFitPool+1, validatorAgreementMinCount+2, false, true, 1000.0,
	)
	if reason == "branch_competitor_dominant" {
		t.Errorf("expected gate to clear once competitor decays, got %q", reason)
	}
}

// L1.3: Reset clears all long-term and local diagnostic fields.
func TestLayer1_ResetClearsLongTermFields(t *testing.T) {
	ms := newSolver()
	ms.LastLocalPeriodMeasurementS = 4.8
	icao := uint32(0xA01)
	ms.LastLocalCandidateAnchorICAO = &icao
	ms.LastLocalBranchOffsetDeg = 45.0
	ms.LastLocalValidatorAgreement = 3
	ms.LastLocalFitQualityScore = 0.7
	ms.LongTermPeriodEstimateS = 4.8
	ms.LongTermPeriodEstimatorConfidence = 0.5
	ms.BranchEstimatorConfidence = 0.6
	ms.BranchPromotionBlockReason = "branch_confidence_insufficient"

	ms.Reset()

	if ms.LastLocalPeriodMeasurementS != 0 ||
		ms.LastLocalCandidateAnchorICAO != nil ||
		ms.LastLocalBranchOffsetDeg != 0 ||
		ms.LastLocalValidatorAgreement != 0 ||
		ms.LastLocalFitQualityScore != 0 {
		t.Error("Reset did not clear LastLocal* fields")
	}
	if ms.LongTermPeriodEstimateS != 0 ||
		ms.LongTermPeriodEstimatorConfidence != 0 ||
		ms.BranchEstimatorConfidence != 0 ||
		ms.BranchPromotionBlockReason != "" {
		t.Error("Reset did not clear LongTerm/Branch* fields")
	}
}
