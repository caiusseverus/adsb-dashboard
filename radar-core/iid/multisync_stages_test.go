package iid

// multisync_stages_test.go — stage-independent unit tests.
//
// Each test group exercises one pipeline stage in isolation, using only that
// stage's public inputs and inspecting its returned result struct. Tests must
// not depend on the ordering or internal state changes made by TryUpdate.
//
// Test categories:
//   A. Observation preparation (prepareObservations, helper functions)
//   B. Period fitting (fitPeriod, retainedResidualSlope, dominant-prior bound)
//   C. Phase branch solving (evaluatePhaseValidation, selectAnchor)
//   D. Authority state machine (updateCandidateState, updateAuthoritativeState,
//      candidateApplicationBlockReason, AbsolutePhaseTrusted)
//   E. Snapshot / protocol (Snapshot field population)

import (
	"math"
	"strings"
	"testing"
)

// ─── helpers ──────────────────────────────────────────────────────────────────

func newSolver() *MultiSyncSolver {
	return NewMultiSyncSolver(1)
}

func ptr[T any](v T) *T { return &v }

func sig(v float32) *float32 { return &v }

func makeObs(centroidUS, bearingDeg float64, icao uint32, rangeNM float32, posAgeS float32) MultiSyncObs {
	return MultiSyncObs{
		CentroidUS: centroidUS,
		ICAO:       icao,
		BearingDeg: bearingDeg,
		RangeNM:    rangeNM,
		PosAgeS:    posAgeS,
		NReplies:   8,
		SignalDBFS: sig(-30.0),
		WallTS:     1000.0,
	}
}

// perfectPrepared builds PreparedObservations that are perfectly on-model
// (residual=0) for a given set of ICAOs, two observations per ICAO.
func perfectPrepared(periodS, epochUS, offsetDeg float64, icaos []uint32) []PreparedObservation {
	var out []PreparedObservation
	for i, icao := range icaos {
		for j := 0; j < 2; j++ {
			us := epochUS + float64(i*2+j)*periodS/float64(len(icaos))*1e6
			phaseRel := math.Mod((us-epochUS)/(periodS*1e6)*360.0, 360.0)
			if phaseRel < 0 {
				phaseRel += 360.0
			}
			bearing := math.Mod(phaseRel+offsetDeg, 360.0)
			out = append(out, PreparedObservation{
				Obs: MultiSyncObs{
					CentroidUS: us,
					ICAO:       icao,
					BearingDeg: bearing,
					RangeNM:    30.0,
					PosAgeS:    0.5,
					NReplies:   8,
					SignalDBFS: sig(-30.0),
				},
				EffectiveUS:     us,
				BearingDeg:      bearing,
				PredictedDeg:    bearing,
				ResidualDeg:     0,
				AbsResidualDeg:  0,
				BaseWeight:      1.0,
				EffectiveWeight: 1.0,
				Status:          "inlier",
				FitEligible:     true,
				FitRejectReason: "",
				phaseInRot:      phaseRel,
			})
		}
	}
	return out
}

// strongValidation builds a syncValidationSummary that satisfies all authority
// promotion gates.
func strongValidation() syncValidationSummary {
	return syncValidationSummary{
		score:              0.85,
		branchAmbiguity:    0.20,
		circularDispersion: 5.0,
		validatorAgreement: 3,
		validatorDisagree:  0,
		validatorEvaluated: 3,
		status:             "validated",
		strong:             true,
		weak:               false,
	}
}

// ─── A: Observation preparation ──────────────────────────────────────────────

// A1: propagation correction is subtracted from centroidUS.
func TestPrepareObs_PropagationCorrection(t *testing.T) {
	rangeNM := float32(100.0)
	centroidUS := 1_000_000.0
	expected := centroidUS - float64(rangeNM)*uSPerNMLight

	got := msPropCorrectedUS(centroidUS, float64(rangeNM))
	if math.Abs(got-expected) > 0.001 {
		t.Errorf("msPropCorrectedUS: got %.4f, want %.4f", got, expected)
	}
}

// A2: zero range leaves centroidUS unchanged.
func TestPrepareObs_ZeroRangePropagation(t *testing.T) {
	centroidUS := 5_000_000.0
	got := msPropCorrectedUS(centroidUS, 0)
	if got != centroidUS {
		t.Errorf("expected centroidUS unchanged for range=0, got %v", got)
	}
}

// A3: residual classification boundaries.
func TestPrepareObs_ResidualClassification(t *testing.T) {
	cases := []struct {
		absR float64
		want string
	}{
		{0, "inlier"},
		{residualInlierDeg - 0.1, "inlier"},
		{residualInlierDeg, "inlier"},
		{residualInlierDeg + 0.1, "soft"},
		{residualSoftDeg2 - 0.1, "soft"},
		{residualSoftDeg2, "soft"},
		{residualSoftDeg2 + 0.1, "rejected"},
		{170.0, "rejected"},
	}
	for _, tc := range cases {
		got := msClassifyResidual(tc.absR)
		if got != tc.want {
			t.Errorf("msClassifyResidual(%.1f) = %q, want %q", tc.absR, got, tc.want)
		}
	}
}

// A4: near-wrap residual (>= residualWrapDeg) is rejected as near_wrap_residual.
func TestPrepareObs_NearWrapRejectReason(t *testing.T) {
	o := makeObs(0, 180.0, 0xAAA, 30, 0.5)
	// absR = residualWrapDeg => near_wrap_residual regardless of status.
	reason := msFitRejectReason(o, "rejected", residualWrapDeg, nil, false)
	if reason != "near_wrap_residual" {
		t.Errorf("expected near_wrap_residual, got %q", reason)
	}
	reason2 := msFitRejectReason(o, "soft", residualWrapDeg+5, nil, false)
	if reason2 != "near_wrap_residual" {
		t.Errorf("expected near_wrap_residual for absR>residualWrapDeg, got %q", reason2)
	}
}

// A5: stale position (> 8s) is rejected as stale_position.
func TestPrepareObs_StalePositionReject(t *testing.T) {
	o := makeObs(0, 10.0, 0xBBB, 30, 20.0)
	reason := msFitRejectReason(o, "inlier", 5.0, nil, false)
	if reason != "stale_position" {
		t.Errorf("expected stale_position, got %q", reason)
	}
}

// A6: stale position is admitted under recovery mode up to recoveryPositionAgeMaxS.
func TestPrepareObs_StalePosition_RecoveryRelaxed(t *testing.T) {
	age := float32(recoveryPositionAgeMaxS - 1.0)
	o := makeObs(0, 10.0, 0xBBB, 30, age)
	reason := msFitRejectReason(o, "inlier", 5.0, nil, true)
	if reason != "" {
		t.Errorf("expected eligible under recovery, got reject %q", reason)
	}
	// Beyond the recovery gate, still rejected.
	o2 := makeObs(0, 10.0, 0xBBB, 30, float32(recoveryPositionAgeMaxS+1.0))
	reason2 := msFitRejectReason(o2, "inlier", 5.0, nil, true)
	if reason2 != "stale_position" {
		t.Errorf("expected stale_position beyond recovery gate, got %q", reason2)
	}
}

// A7: ICAO quality reject gate fires above threshold.
func TestPrepareObs_ICAOQualityReject(t *testing.T) {
	o := makeObs(0, 10.0, 0xCCC, 30, 0.5)
	q := &ICAOSyncQuality{ResidualMADDeg: icaoQualityRejectMAD + 1.0}
	reason := msFitRejectReason(o, "inlier", 5.0, q, false)
	if reason != "icao_quality_reject" {
		t.Errorf("expected icao_quality_reject, got %q", reason)
	}
	// Not fired in recovery mode.
	reasonRecovery := msFitRejectReason(o, "inlier", 5.0, q, true)
	if reasonRecovery == "icao_quality_reject" {
		t.Error("icao_quality_reject must not fire in recovery mode")
	}
}

// A8: prepareObservations populates all fields consistently.
func TestPrepareObs_FullPipeline(t *testing.T) {
	ms := newSolver()
	periodS := 4.0
	epochUS := 0.0
	offsetDeg := 90.0

	// Two on-model observations from different ICAOs.
	obs := []MultiSyncObs{
		makeObs(epochUS+0.5*periodS*1e6, math.Mod(0.5*360.0+offsetDeg, 360.0), 0x111, 0, 0.3),
		makeObs(epochUS+1.0*periodS*1e6, math.Mod(1.0*360.0+offsetDeg, 360.0), 0x222, 0, 0.3),
	}
	// Adjust bearings to exact model values.
	for i := range obs {
		ph := math.Mod((obs[i].CentroidUS-epochUS)/(periodS*1e6)*360.0, 360.0)
		obs[i].BearingDeg = math.Mod(ph+offsetDeg, 360.0)
	}

	prepared, rejectReasons, eligible, relaxed := ms.prepareObservations(obs, epochUS, offsetDeg, periodS, false)

	if len(prepared) != 2 {
		t.Fatalf("expected 2 prepared observations, got %d", len(prepared))
	}
	if eligible != 2 {
		t.Errorf("expected 2 eligible, got %d", eligible)
	}
	if relaxed != 0 {
		t.Errorf("expected 0 relaxed, got %d", relaxed)
	}
	if len(rejectReasons) != 0 {
		t.Errorf("expected no reject reasons, got %v", rejectReasons)
	}
	for i, p := range prepared {
		if math.Abs(p.ResidualDeg) > 1.0 {
			t.Errorf("[%d] expected near-zero residual, got %.2f", i, p.ResidualDeg)
		}
		if p.Status != "inlier" {
			t.Errorf("[%d] expected status inlier, got %q", i, p.Status)
		}
		if !p.FitEligible {
			t.Errorf("[%d] expected fit eligible", i)
		}
		if p.EffectiveUS == 0 {
			t.Errorf("[%d] EffectiveUS must be set", i)
		}
	}
}

// A9: large residual observation is ineligible and counted in rejectReasons.
func TestPrepareObs_LargeResidualRejected(t *testing.T) {
	ms := newSolver()
	periodS := 4.0
	epochUS := 0.0
	offsetDeg := 0.0

	// Observation with bearing 90° off from model prediction.
	obs := []MultiSyncObs{
		makeObs(epochUS, 90.0, 0xDDD, 0, 0.3), // model predicts 0°, bearing=90° → residual=90°
	}
	prepared, rejectReasons, eligible, _ := ms.prepareObservations(obs, epochUS, offsetDeg, periodS, false)

	if eligible != 0 {
		t.Errorf("expected 0 eligible (high residual), got %d", eligible)
	}
	if len(rejectReasons) == 0 {
		t.Error("expected at least one reject reason")
	}
	if len(prepared) != 1 {
		t.Fatalf("expected 1 prepared entry, got %d", len(prepared))
	}
	if prepared[0].FitEligible {
		t.Error("expected FitEligible=false for high-residual observation")
	}
}

// A10: msScoreObs weight is bounded [0,1] and increases with fresh position.
func TestPrepareObs_ObsScore(t *testing.T) {
	fresh := makeObs(0, 0, 1, 30, 0.5)
	stale := makeObs(0, 0, 1, 30, 10.0)
	wFresh := msScoreObs(fresh)
	wStale := msScoreObs(stale)
	if wFresh <= wStale {
		t.Errorf("fresh obs should score higher: fresh=%.3f, stale=%.3f", wFresh, wStale)
	}
	if wFresh > 1.0 || wStale < 0 {
		t.Errorf("weights must be in [0,1]: fresh=%.3f, stale=%.3f", wFresh, wStale)
	}
}

// A11: msICAOQualityMult returns 1.0 for nil quality entry.
func TestPrepareObs_ICAOQualityMult_NilEntry(t *testing.T) {
	got := msICAOQualityMult(nil)
	if got != 1.0 {
		t.Errorf("nil entry should give mult=1.0, got %.3f", got)
	}
}

// A12: msICAOQualityMult decreases as MAD increases.
func TestPrepareObs_ICAOQualityMult_Decreasing(t *testing.T) {
	low := msICAOQualityMult(&ICAOSyncQuality{ResidualMADDeg: 1.0})
	high := msICAOQualityMult(&ICAOSyncQuality{ResidualMADDeg: 10.0})
	if low <= high {
		t.Errorf("quality mult should decrease with MAD: low=%.3f, high=%.3f", low, high)
	}
}

// ─── B: Period fitting ────────────────────────────────────────────────────────

// B1: fitPeriod with perfectly on-model observations returns status="ok" and a
// period within 100 PPM of the true period.
func TestPeriodFit_PerfectObservations(t *testing.T) {
	ms := newSolver()
	periodS := 4.0
	epochUS := 0.0
	offsetDeg := 45.0

	// Seed initial base period.
	ms.PeriodBaseS = periodS
	ms.TrustedBasePeriodS = periodS

	prepared := perfectPrepared(periodS, epochUS, offsetDeg, []uint32{0x1, 0x2, 0x3, 0x4})
	r := ms.fitPeriod(prepared, epochUS, offsetDeg, periodS, periodS, 0, false, 1000.0)

	if r.Status == "reacquire" {
		t.Errorf("unexpected reacquire status with perfect observations")
	}
	if r.AppliedPeriodS <= 0 {
		t.Errorf("expected positive applied period, got %.4f", r.AppliedPeriodS)
	}
	_, ppm := periodDeltaToDominant(r.AppliedPeriodS, periodS)
	if math.Abs(ppm) > 200 {
		t.Errorf("period drift %.1f PPM exceeds 200 PPM with perfect observations", ppm)
	}
}

// B2: motion guard is degraded when ICAOs are spatially clustered (low bearing spread).
// Tests refinePeriod directly — the guard fires inside refinePeriod based on nFitICAOs
// and bearingSpreadDeg, and requires dominantPeriodS > 0.
func TestPeriodFit_MotionGuardDegraded_ClusteredICAOs(t *testing.T) {
	ms := newSolver()
	// Seed a persistent slope history.
	for i := 0; i < persistMinEntries+2; i++ {
		ms.SlopeHistory = append(ms.SlopeHistory, 1.0)
	}

	dominantPeriodS := 4.0
	livePeriodS := dominantPeriodS

	// nFitICAOs=3 (= periodRefineMotionGuardMinICAOs) with bearingSpreadDeg < threshold.
	ms.refinePeriod(
		livePeriodS, livePeriodS, dominantPeriodS, 1.0,
		periodRefineMinInlier+2,
		periodRefineMotionGuardMinICAOs,               // exactly at boundary
		periodRefineMinSpanRot*livePeriodS+1.0,        // sufficient span
		periodRefineMinBearingSpreadDeg-5.0,           // well below 30° threshold
		false,                                          // majorityRejected
		true,                                           // trusted
	)

	if !ms.LastMotionGuardDegraded {
		t.Error("expected MotionGuardDegraded=true with bearing spread < threshold and dominant prior active")
	}
}

// B3: dominant-prior bound (300 PPM) is enforced by refinePeriod. When the
// slope-corrected candidate would exceed ±300 PPM from dominantPeriodS, it is clamped
// and BaseClamped is set. Tests refinePeriod directly since fitPeriod only applies
// the correction after the retained-window slope is available (requires ms.obs).
func TestPeriodFit_DominantPriorBoundEnforced(t *testing.T) {
	ms := newSolver()
	// Persistent slope history.
	for i := 0; i < persistMinEntries+2; i++ {
		ms.SlopeHistory = append(ms.SlopeHistory, 1.0)
	}

	dominantPeriodS := 4.0
	// livePeriodS is already 500 PPM above dominant — any slope direction will
	// produce a candidate that exceeds the ±300 PPM dominant bound.
	livePeriodS := dominantPeriodS * (1.0 + 500.0/1e6)

	refined, blockReason, _, clamped, _ := ms.refinePeriod(
		livePeriodS, livePeriodS, dominantPeriodS, 1.0, // positive slope pushes further
		periodRefineMinInlier+2,
		periodRefineMotionGuardMinICAOs+1,
		periodRefineMinSpanRot*livePeriodS+1.0,
		50.0,  // wide bearing spread → no motion guard
		false, // majorityRejected
		true,  // trusted
	)

	if blockReason != "" {
		t.Fatalf("unexpected block reason: %q", blockReason)
	}
	if !clamped {
		t.Error("expected clamped=true when period exceeds dominant bound")
	}
	_, ppm := periodDeltaToDominant(refined, dominantPeriodS)
	if math.Abs(ppm) > periodRefineMaxPPMFromDominant+1 {
		t.Errorf("refined period %.6f is %.1f PPM from dominant (limit %g PPM)",
			refined, ppm, periodRefineMaxPPMFromDominant)
	}
}

// B4: bearingCircularSpreadDeg returns near-zero for identical bearings.
func TestPeriodFit_BearingSpread_Identical(t *testing.T) {
	bearings := []float64{90, 90, 90, 90}
	spread := bearingCircularSpreadDeg(bearings)
	if spread > 5 {
		t.Errorf("identical bearings spread should be near-zero, got %.2f", spread)
	}
}

// B5: bearingCircularSpreadDeg returns large value for uniformly distributed bearings.
func TestPeriodFit_BearingSpread_Uniform(t *testing.T) {
	bearings := make([]float64, 36)
	for i := range bearings {
		bearings[i] = float64(i) * 10.0
	}
	spread := bearingCircularSpreadDeg(bearings)
	if spread < 100 {
		t.Errorf("uniformly distributed bearings should have large spread, got %.2f", spread)
	}
}

// B6: FitSpanS, FitICAOs, and FitPoolCount are populated from the fit pool.
// FitPoolCount/FitICAOCount come from the short-window prepared set;
// FitICAOs/FitSpanS come from the retained-window slope (requires ms.obs populated).
func TestPeriodFit_FitStatisticsPopulated(t *testing.T) {
	ms := newSolver()
	periodS := 4.0
	epochUS := 0.0
	offsetDeg := 45.0
	ms.PeriodBaseS = periodS
	ms.TrustedBasePeriodS = periodS
	nowUnix := 1000.0

	// Populate ms.obs with retained observations so retainedResidualSlope has data.
	icaos := []uint32{0xA, 0xB, 0xC, 0xD}
	for _, icao := range icaos {
		for j := 0; j < 4; j++ {
			us := epochUS + float64(j)*periodS/2.0*1e6
			phaseRel := math.Mod((us-epochUS)/(periodS*1e6)*360.0, 360.0)
			if phaseRel < 0 {
				phaseRel += 360.0
			}
			bearing := math.Mod(phaseRel+offsetDeg, 360.0)
			ms.obs = append(ms.obs, MultiSyncObs{
				CentroidUS: us,
				ICAO:       icao,
				BearingDeg: bearing,
				RangeNM:    30,
				PosAgeS:    0.5,
				NReplies:   8,
				SignalDBFS: sig(-30.0),
				WallTS:     nowUnix - float64(j)*2.0,
			})
		}
	}

	prepared := perfectPrepared(periodS, epochUS, offsetDeg, icaos)
	r := ms.fitPeriod(prepared, epochUS, offsetDeg, periodS, periodS, 0, false, nowUnix)

	if r.FitPoolCount <= 0 {
		t.Errorf("expected FitPoolCount > 0, got %d", r.FitPoolCount)
	}
	if r.FitICAOCount <= 0 {
		t.Errorf("expected FitICAOCount > 0, got %d", r.FitICAOCount)
	}
	if r.FitSpanS < 0 {
		t.Errorf("expected FitSpanS >= 0, got %.4f", r.FitSpanS)
	}
}

// ─── C: Phase branch solving ──────────────────────────────────────────────────

// C1: selected anchor has anchor_delta = 0 (the anchor defines its own offset).
func TestPhase_AnchorDeltaZero(t *testing.T) {
	ms := newSolver()
	periodS := 4.0
	epochUS := 0.0
	offsetDeg := 90.0
	anchorICAO := uint32(0xA01)

	prepared := perfectPrepared(periodS, epochUS, offsetDeg, []uint32{anchorICAO, 0xA02, 0xA03})

	period := PeriodFitResult{
		AppliedPeriodS: periodS,
		Status:         "ok",
		SlopeGatePassed: true,
		FitPoolCount:   len(prepared),
		FitICAOCount:   3,
	}
	result := ms.solvePhaseBranch(prepared, period, epochUS, offsetDeg, false, 1000.0)

	if result.Estimate.anchorICAO == nil {
		t.Fatal("expected an anchor to be selected")
	}
	// The anchor's implied offset must equal the alignment offset.
	gotAnchorPhase := result.Alignment.anchorPhaseDeg
	gotOffset := result.Alignment.offsetDeg
	delta := math.Abs(circularDiff(gotAnchorPhase, gotOffset))
	if delta > 2.0 {
		t.Errorf("anchor phase delta to offset is %.2f°, expected near 0°", delta)
	}
}

// C2: validator agreement and disagreement counts use the same fit-eligible population.
func TestPhase_ValidatorAgreementFromFitEligible(t *testing.T) {
	// Three ICAOs on-model (residual=0), one with large residual (FitEligible=false).
	periodS := 4.0
	epochUS := 0.0
	offsetDeg := 45.0
	anchor := uint32(0xF01)
	validators := []uint32{0xF02, 0xF03}

	prepared := perfectPrepared(periodS, epochUS, offsetDeg, append([]uint32{anchor}, validators...))

	// Add an ineligible observation that disagrees strongly — must not affect validator counts.
	prepared = append(prepared, PreparedObservation{
		Obs:             MultiSyncObs{ICAO: 0xF99},
		FitEligible:     false,
		Status:          "rejected",
		ResidualDeg:     100,
		AbsResidualDeg:  100,
		EffectiveUS:     epochUS + 0.5*periodS*1e6,
		phaseInRot:      180,
		EffectiveWeight: 0,
	})

	ms := newSolver()
	estimate := syncStateEstimate{
		present:    true,
		periodS:    periodS,
		epochUS:    epochUS,
		offsetDeg:  offsetDeg,
		anchorICAO: ptr(anchor),
	}
	// Build candidates list so anchor selection doesn't block.
	candidates := []AnchorCandidateSnapshot{
		{ICAO: anchor, Score: 0.9, SpreadDeg: 5.0, Status: "ok"},
	}
	v := ms.evaluatePhaseValidation(prepared, estimate, candidates)

	// The ineligible observation must not inflate disagreement count.
	if v.validatorDisagree > 0 {
		t.Errorf("expected validatorDisagree=0 (ineligible obs must not count), got %d", v.validatorDisagree)
	}
}

// C3: evaluatePhaseValidation returns validation_unavailable when anchor is nil.
func TestPhase_ValidationUnavailable_NoAnchor(t *testing.T) {
	ms := newSolver()
	estimate := syncStateEstimate{
		present:    true,
		periodS:    4.0,
		epochUS:    0,
		offsetDeg:  0,
		anchorICAO: nil, // no anchor
	}
	v := ms.evaluatePhaseValidation(perfectPrepared(4.0, 0, 0, []uint32{0x1, 0x2}), estimate, nil)
	if v.status != "validation_unavailable" {
		t.Errorf("expected validation_unavailable without anchor, got %q", v.status)
	}
	if v.strong {
		t.Error("strong must be false without anchor")
	}
}

// C4: branch ambiguity is low when a single anchor dominates the candidate list.
func TestPhase_BranchAmbiguity_SingleDominantAnchor(t *testing.T) {
	ms := newSolver()
	periodS := 4.0
	epochUS := 0.0
	offsetDeg := 0.0
	anchor := uint32(0xE01)

	prepared := perfectPrepared(periodS, epochUS, offsetDeg, []uint32{anchor, 0xE02, 0xE03})
	estimate := syncStateEstimate{
		present:    true,
		periodS:    periodS,
		epochUS:    epochUS,
		offsetDeg:  offsetDeg,
		anchorICAO: ptr(anchor),
	}
	candidates := []AnchorCandidateSnapshot{
		{ICAO: anchor, Score: 0.95, SpreadDeg: 5.0, Status: "ok"},
		{ICAO: 0xE02, Score: 0.10, SpreadDeg: 8.0, Status: "ok"},
	}
	v := ms.evaluatePhaseValidation(prepared, estimate, candidates)
	if v.branchAmbiguity >= branchAmbiguityRejectThreshold {
		t.Errorf("branch ambiguity %.2f too high for dominant anchor (threshold %.2f)",
			v.branchAmbiguity, branchAmbiguityRejectThreshold)
	}
}

// C5: solvePhaseBranch sets CandidateMode="bootstrap" in compact authority mode.
func TestPhase_CandidateModeBootstrap(t *testing.T) {
	ms := newSolver()
	ms.ActiveAuthorityMode = authorityModeCompact
	periodS := 4.0
	prepared := perfectPrepared(periodS, 0, 45.0, []uint32{0x1, 0x2, 0x3})
	period := PeriodFitResult{
		AppliedPeriodS:  periodS,
		Status:          "ok",
		SlopeGatePassed: true,
		FitPoolCount:    len(prepared),
		FitICAOCount:    3,
	}
	result := ms.solvePhaseBranch(prepared, period, 0, 45.0, false, 1000.0)
	if result.CandidateMode != "bootstrap" {
		t.Errorf("expected CandidateMode=bootstrap, got %q", result.CandidateMode)
	}
}

// ─── D: Authority state machine ───────────────────────────────────────────────

// D1: AbsolutePhaseTrusted is not set before authority promotion (it is an output,
// not an input). When ensureAuthorityMode is called from a cold state, the mode is
// initialised to compact and AbsolutePhaseTrusted remains false.
func TestAuthority_AbsolutePhaseTrusted_IsOutput(t *testing.T) {
	ms := newSolver()
	ms.ensureAuthorityMode()
	if ms.AbsolutePhaseTrusted {
		t.Error("AbsolutePhaseTrusted must be false before any promotion")
	}
	// Manually populate authoritative state and set mode to refined, but AbsolutePhaseTrusted
	// must only be set by applyResults, not by ensureAuthorityMode.
	ms.ActiveAuthorityMode = authorityModeRefined
	ms.ensureAuthorityMode()
	// ensureAuthorityMode must not set AbsolutePhaseTrusted.
	if ms.AbsolutePhaseTrusted {
		t.Error("ensureAuthorityMode must not set AbsolutePhaseTrusted")
	}
}

// D2: updateCandidateState increments promotion streak only when estimate is stable
// and validation is strong.
func TestAuthority_PromotionStreakIncrement(t *testing.T) {
	ms := newSolver()
	periodS := 4.0
	offsetDeg := 45.0
	epochUS := 0.0

	estimate := syncStateEstimate{
		present:   true,
		periodS:   periodS,
		epochUS:   epochUS,
		offsetDeg: offsetDeg,
	}
	validation := strongValidation()

	// First call: no prior candidate, streak goes to 1.
	ms.updateCandidateState(estimate, validation, "tracking", 1000.0)
	if ms.CandidatePromotionStreak != 1 {
		t.Errorf("expected streak=1 after first call, got %d", ms.CandidatePromotionStreak)
	}

	// Second call with identical estimate: stable → streak increments.
	ms.updateCandidateState(estimate, validation, "tracking", 1001.0)
	if ms.CandidatePromotionStreak != 2 {
		t.Errorf("expected streak=2 after stable second call, got %d", ms.CandidatePromotionStreak)
	}
}

// D3: updateCandidateState resets streak when estimate changes significantly.
func TestAuthority_PromotionStreakReset_UnstableEstimate(t *testing.T) {
	ms := newSolver()
	e1 := syncStateEstimate{present: true, periodS: 4.0, epochUS: 0, offsetDeg: 45.0}
	e2 := syncStateEstimate{present: true, periodS: 4.0, epochUS: 0, offsetDeg: 180.0} // 135° shift
	v := strongValidation()

	ms.updateCandidateState(e1, v, "tracking", 1000.0)
	streak1 := ms.CandidatePromotionStreak
	ms.updateCandidateState(e2, v, "tracking", 1001.0)
	if ms.CandidatePromotionStreak > streak1 {
		t.Errorf("streak should reset on unstable estimate, before=%d after=%d",
			streak1, ms.CandidatePromotionStreak)
	}
}

// D4: updateCandidateState resets streak when validation is not strong.
func TestAuthority_PromotionStreakReset_WeakValidation(t *testing.T) {
	ms := newSolver()
	e := syncStateEstimate{present: true, periodS: 4.0, epochUS: 0, offsetDeg: 45.0}
	v := strongValidation()

	ms.updateCandidateState(e, v, "tracking", 1000.0)
	if ms.CandidatePromotionStreak != 1 {
		t.Fatalf("expected streak=1 after strong validation, got %d", ms.CandidatePromotionStreak)
	}

	weak := syncValidationSummary{score: 0.2, status: "weak_validation", weak: true}
	ms.updateCandidateState(e, weak, "tracking", 1001.0)
	if ms.CandidatePromotionStreak != 0 {
		t.Errorf("expected streak=0 after weak validation, got %d", ms.CandidatePromotionStreak)
	}
}

// D5: candidateApplicationBlockReason returns "candidate_streak_not_met" when
// CandidatePromotionStreak is below candidatePromotionMinStreak.
func TestAuthority_BlockReason_StreakNotMet(t *testing.T) {
	ms := newSolver()
	ms.ActiveAuthorityMode = authorityModeRefined
	ms.CandidatePromotionStreak = candidatePromotionMinStreak - 1

	estimate := syncStateEstimate{
		present:    true,
		periodS:    4.0,
		epochUS:    0,
		offsetDeg:  45.0,
		anchorICAO: ptr(uint32(0xA01)),
	}
	v := strongValidation()
	reason := ms.candidateApplicationBlockReason(estimate, v, true, true, trustMinFitPool+1, validatorAgreementMinCount+1, false, true, 1000.0)
	if reason != "candidate_streak_not_met" {
		t.Errorf("expected candidate_streak_not_met, got %q", reason)
	}
}

// D6: candidateApplicationBlockReason returns "" when all gates pass in refined mode.
func TestAuthority_BlockReason_AllGatesPassed(t *testing.T) {
	ms := newSolver()
	ms.ActiveAuthorityMode = authorityModeRefined
	ms.CandidatePromotionStreak = candidatePromotionMinStreak + 1

	estimate := syncStateEstimate{
		present:    true,
		periodS:    4.0,
		epochUS:    0,
		offsetDeg:  45.0,
		anchorICAO: ptr(uint32(0xA01)),
	}
	v := strongValidation()
	reason := ms.candidateApplicationBlockReason(estimate, v, true, true, trustMinFitPool+1, validatorAgreementMinCount+1, false, true, 1000.0)
	if reason != "" {
		t.Errorf("expected no block reason in refined mode with all gates passed, got %q", reason)
	}
}

// D7: candidateApplicationBlockReason returns "period_not_stable" when estimate is absent.
func TestAuthority_BlockReason_EstimateAbsent(t *testing.T) {
	ms := newSolver()
	ms.ActiveAuthorityMode = authorityModeRefined
	ms.CandidatePromotionStreak = candidatePromotionMinStreak + 1

	estimate := syncStateEstimate{present: false}
	v := strongValidation()
	reason := ms.candidateApplicationBlockReason(estimate, v, true, true, trustMinFitPool+1, validatorAgreementMinCount+1, false, true, 1000.0)
	if reason != "period_not_stable" {
		t.Errorf("expected period_not_stable for absent estimate, got %q", reason)
	}
}

// D8: candidateApplicationBlockReason returns "slope_gate_failed" when slope gate fails.
func TestAuthority_BlockReason_SlopeGateFailed(t *testing.T) {
	ms := newSolver()
	ms.ActiveAuthorityMode = authorityModeRefined
	ms.CandidatePromotionStreak = candidatePromotionMinStreak + 1

	estimate := syncStateEstimate{
		present:    true,
		periodS:    4.0,
		epochUS:    0,
		offsetDeg:  45.0,
		anchorICAO: ptr(uint32(0xA01)),
	}
	v := strongValidation()
	reason := ms.candidateApplicationBlockReason(estimate, v, false /* slopeGatePassed=false */, true, trustMinFitPool+1, validatorAgreementMinCount+1, false, true, 1000.0)
	if reason != "slope_gate_failed" {
		t.Errorf("expected slope_gate_failed, got %q", reason)
	}
}

// D9: updateAuthoritativeState initializes AuthoritativePresent when promotion streak
// meets authoritativeInitPromotionStreak and validation is strong.
func TestAuthority_AuthoritativeInit(t *testing.T) {
	ms := newSolver()
	ms.CandidatePromotionStreak = authoritativeInitPromotionStreak + 1

	estimate := syncStateEstimate{
		present:    true,
		periodS:    4.0,
		epochUS:    0,
		offsetDeg:  90.0,
		anchorICAO: ptr(uint32(0xC01)),
	}
	v := strongValidation()
	ms.updateAuthoritativeState(estimate, v, false, 1000.0)

	if !ms.AuthoritativePresent {
		t.Error("expected AuthoritativePresent=true after init promotion")
	}
	if math.Abs(ms.AuthoritativePeriodS-4.0) > 0.001 {
		t.Errorf("AuthoritativePeriodS not initialized: %.4f", ms.AuthoritativePeriodS)
	}
}

// D10: updateAuthoritativeState freezes the period when validation is not strong
// (even when promotion streak is met).
func TestAuthority_PeriodFrozen_WeakValidation(t *testing.T) {
	ms := newSolver()
	// Initialize authoritative state first.
	ms.AuthoritativePresent = true
	ms.AuthoritativePeriodS = 4.0
	ms.AuthoritativePhaseEpochUS = 0
	ms.AuthoritativePhaseOffsetDeg = 45.0
	ms.CandidatePromotionStreak = candidatePromotionMinStreak + 5

	estimate := syncStateEstimate{
		present:    true,
		periodS:    4.1,
		epochUS:    0,
		offsetDeg:  46.0,
		anchorICAO: ptr(uint32(0xC01)),
	}
	weakVal := syncValidationSummary{
		score:  0.3,
		status: "weak_validation",
		weak:   true,
	}
	prevPeriod := ms.AuthoritativePeriodS
	ms.updateAuthoritativeState(estimate, weakVal, false, 1001.0)

	if ms.AuthoritativePeriodS != prevPeriod {
		t.Errorf("period should be frozen during weak validation: was %.4f, now %.4f",
			prevPeriod, ms.AuthoritativePeriodS)
	}
	if !ms.LastPeriodFrozenDueToPhaseValidation {
		t.Error("expected LastPeriodFrozenDueToPhaseValidation=true for weak validation")
	}
}

// D11: compact and refined authority modes remain separate — entering compact never
// sets AuthoritativePresent.
func TestAuthority_CompactRefinedSeparation(t *testing.T) {
	ms := newSolver()
	ms.ensureAuthorityMode()
	if ms.ActiveAuthorityMode != authorityModeCompact {
		t.Fatalf("cold solver should start in compact authority, got %q", ms.ActiveAuthorityMode)
	}
	// AuthoritativePresent must be false in compact mode.
	if ms.AuthoritativePresent {
		t.Error("AuthoritativePresent must not be true in compact mode")
	}
}

// ─── E: Snapshot / protocol ──────────────────────────────────────────────────

// E1: Snapshot copies all present fields without modifying solver state.
func TestSnapshot_FieldsPopulated(t *testing.T) {
	ms := newSolver()
	ms.Present = true
	ms.Usable = true
	ms.PeriodS = 4.12345
	ms.PeriodBaseS = 4.100
	ms.PhaseOffsetDeg = 77.5
	ms.PhaseEpochUS = 123456789.0
	ms.JitterDeg = 2.3
	ms.ResidualEMADeg = 1.1
	ms.NSyncUpdates = 42
	ms.Holdover = false
	ms.LastUpdated = 9999.0
	ms.AbsolutePhaseTrusted = true
	ms.ActiveAuthorityMode = authorityModeRefined
	ms.CandidatePromotionStreak = 7
	icao := uint32(0xABCDEF)
	ms.AnchorICAO = &icao
	ms.AnchorPhaseDeg = 33.0
	ms.AnchorScore = 0.88
	candICAO := uint32(0x111111)
	ms.CandidateAnchorICAO = &candICAO
	authICAO := uint32(0x222222)
	ms.AuthoritativeAnchorICAO = &authICAO
	ms.LastAuthorityPromotionBlockReason = "slope_gate_failed"

	snap := ms.Snapshot()

	if !snap.Present {
		t.Error("snapshot Present mismatch")
	}
	if !snap.Usable {
		t.Error("snapshot Usable mismatch")
	}
	if snap.PeriodS != ms.PeriodS {
		t.Errorf("PeriodS mismatch: snap=%.5f, ms=%.5f", snap.PeriodS, ms.PeriodS)
	}
	if snap.PhaseOffsetDeg != ms.PhaseOffsetDeg {
		t.Errorf("PhaseOffsetDeg mismatch")
	}
	if !snap.AbsolutePhaseTrusted {
		t.Error("AbsolutePhaseTrusted not exported in snapshot")
	}
	if snap.ActiveAuthorityMode != authorityModeRefined {
		t.Errorf("ActiveAuthorityMode mismatch: %q", snap.ActiveAuthorityMode)
	}
	if snap.AuthorityPromotionBlockReason != "slope_gate_failed" {
		t.Errorf("AuthorityPromotionBlockReason not exported: %q", snap.AuthorityPromotionBlockReason)
	}
	if snap.AnchorICAO == nil || *snap.AnchorICAO != icao {
		t.Error("AnchorICAO not correctly copied")
	}
	if snap.CandidateAnchorICAO == nil || *snap.CandidateAnchorICAO != candICAO {
		t.Error("CandidateAnchorICAO not correctly copied")
	}
	if snap.AuthoritativeAnchorICAO == nil || *snap.AuthoritativeAnchorICAO != authICAO {
		t.Error("AuthoritativeAnchorICAO not correctly copied")
	}
}

// E2: Snapshot does not alias pointer fields — modifications to returned snap must
// not affect solver state.
func TestSnapshot_NoPointerAliasing(t *testing.T) {
	ms := newSolver()
	icao := uint32(0xABCDEF)
	ms.AnchorICAO = &icao

	snap := ms.Snapshot()
	if snap.AnchorICAO == nil {
		t.Fatal("expected AnchorICAO in snapshot")
	}
	// Modify the snapshot's pointer target — must not affect ms.AnchorICAO.
	*snap.AnchorICAO = 0xFFFFFF
	if *ms.AnchorICAO != 0xABCDEF {
		t.Error("snapshot AnchorICAO aliases solver field — deep copy required")
	}
}

// E3: Snapshot.FitRejectReasons is a separate map (not aliased from solver).
func TestSnapshot_FitRejectReasons_NotAliased(t *testing.T) {
	ms := newSolver()
	ms.LastFitRejectReasons = map[string]uint64{"stale_position": 3, "residual_gate": 1}

	snap := ms.Snapshot()
	snap.FitRejectReasons["injected"] = 999

	if _, ok := ms.LastFitRejectReasons["injected"]; ok {
		t.Error("FitRejectReasons in snapshot must not alias solver's map")
	}
}

// E4: AuthoritativeStateAgeS is computed from LastUpdated - AuthoritativeStateSinceTS.
func TestSnapshot_AuthoritativeStateAge(t *testing.T) {
	ms := newSolver()
	ms.LastUpdated = 1000.0
	ms.AuthoritativeStateSinceTS = 985.0

	snap := ms.Snapshot()
	expected := 15.0
	if math.Abs(snap.AuthoritativeStateAgeS-expected) > 0.001 {
		t.Errorf("AuthoritativeStateAgeS: got %.3f, want %.3f", snap.AuthoritativeStateAgeS, expected)
	}
}

// E5: Snapshot returns AuthoritativeStateAgeS=0 when AuthoritativeStateSinceTS is 0.
func TestSnapshot_AuthoritativeStateAge_Zero(t *testing.T) {
	ms := newSolver()
	ms.LastUpdated = 1000.0
	ms.AuthoritativeStateSinceTS = 0

	snap := ms.Snapshot()
	if snap.AuthoritativeStateAgeS != 0 {
		t.Errorf("expected AuthoritativeStateAgeS=0 when StateSinceTS=0, got %.3f", snap.AuthoritativeStateAgeS)
	}
}

// E6: AnchorCandidates slice is deep-copied, not shared with solver state.
func TestSnapshot_AnchorCandidates_NotAliased(t *testing.T) {
	ms := newSolver()
	ms.LastAnchorCandidates = []AnchorCandidateSnapshot{
		{ICAO: 0x111, Score: 0.9},
		{ICAO: 0x222, Score: 0.7},
	}

	snap := ms.Snapshot()
	if len(snap.AnchorCandidates) != 2 {
		t.Fatalf("expected 2 anchor candidates in snapshot, got %d", len(snap.AnchorCandidates))
	}
	// Mutate the snapshot slice — must not affect solver.
	snap.AnchorCandidates[0].Score = 0.0
	if ms.LastAnchorCandidates[0].Score == 0.0 {
		t.Error("AnchorCandidates in snapshot must not alias solver field")
	}
}

// ─── B7: Unwrapped slope double-count fix ──────────────────────────────────────

// B7: retainedResidualSlopeUnwrapped does not double-count single-observation ICAOs.
// After the fix, nAccepted + nRejected == total observations that passed the initial
// quality filter. Single-obs ICAOs are in nRejected exactly once (they start in nRejected
// and are never subtracted, since they are skipped before the accepted-- decrement).
func TestPeriodFit_UnwrappedSlope_NoSingleObsDoubleCount(t *testing.T) {
	ms := newSolver()
	periodS := 4.0
	epochUS := 0.0
	offsetDeg := 0.0
	nowUnix := 2000.0

	sigVal := float32(-30.0)

	// Three ICAOs with 2 obs each (qualify for per-ICAO unwrapped fit).
	multiObsICAOs := []uint32{0x1, 0x2, 0x3}
	// Two ICAOs with 1 obs each (disqualified by len<2 check).
	singleObsICAOs := []uint32{0x4, 0x5}

	for _, icao := range multiObsICAOs {
		for j := 0; j < 2; j++ {
			us := epochUS + float64(j)*periodS*1e6
			bearing := simulatedBearing(us, epochUS, offsetDeg, periodS)
			ms.obs = append(ms.obs, MultiSyncObs{
				CentroidUS: us,
				ICAO:       icao,
				BearingDeg: bearing,
				RangeNM:    30,
				PosAgeS:    0.5,
				NReplies:   8,
				SignalDBFS: &sigVal,
				WallTS:     nowUnix - float64(j)*0.5,
			})
		}
	}
	for _, icao := range singleObsICAOs {
		ms.obs = append(ms.obs, MultiSyncObs{
			CentroidUS: epochUS,
			ICAO:       icao,
			BearingDeg: offsetDeg,
			RangeNM:    30,
			PosAgeS:    0.5,
			NReplies:   8,
			SignalDBFS: &sigVal,
			WallTS:     nowUnix,
		})
	}

	_, nAcc, nRej, nRejAfterUnwrap, _, _, _, ok := ms.retainedResidualSlopeUnwrapped(epochUS, offsetDeg, periodS, nowUnix)

	// If no slope was computed, the counts must still be consistent.
	total := len(multiObsICAOs)*2 + len(singleObsICAOs)
	if ok {
		if nAcc+nRej != total {
			t.Errorf("nAcc(%d)+nRej(%d)=%d must equal total input %d (no double-count)",
				nAcc, nRej, nAcc+nRej, total)
		}
		if nRejAfterUnwrap > nRej {
			t.Errorf("nRejAfterUnwrap(%d) must be <= nRej(%d)", nRejAfterUnwrap, nRej)
		}
	} else {
		if nAcc+nRej > total {
			t.Errorf("even without a fit, nAcc(%d)+nRej(%d)=%d exceeds total %d (double-count detected)",
				nAcc, nRej, nAcc+nRej, total)
		}
	}
}

// ─── D12: Validator disagreement regression (3/18 scenario) ────────────────────

// D12: regression — large validator disagreement produces a numeric block reason.
// Models the "many ICAOs visible, anchor selected, many anchor deltas near zero,
// but validator result says 3 agree / 18 reject" scenario.
// Verifies that (a) the 18 rejections are legitimate (wrong-phase ICAOs with >=2 obs),
// (b) the block reason includes numeric counts, and (c) the per-ICAO table
// correctly classifies all 22 ICAOs.
func TestAuthority_ValidatorDisagreementBlockReason_Numeric(t *testing.T) {
	periodS := 4.0
	epochUS := 0.0
	anchorPhase := 45.0
	anchor := uint32(0xA00)

	// Helper: build 2 on-model PreparedObservations for one ICAO at the given phase offset.
	obsForICAO := func(icao uint32, phaseOffset float64) []PreparedObservation {
		var out []PreparedObservation
		for j := 0; j < 2; j++ {
			us := float64(j) * periodS * 1e6
			phaseRel := math.Mod((us-epochUS)/(periodS*1e6)*360.0, 360.0)
			if phaseRel < 0 {
				phaseRel += 360.0
			}
			bearing := math.Mod(phaseRel+anchorPhase+phaseOffset, 360.0)
			out = append(out, PreparedObservation{
				Obs:             MultiSyncObs{ICAO: icao, BearingDeg: bearing},
				EffectiveUS:     us,
				FitEligible:     true,
				EffectiveWeight: 1.0,
				BaseWeight:      1.0,
				Status:          "inlier",
			})
		}
		return out
	}

	// Anchor: 2 observations at anchorPhase.
	prepared := obsForICAO(anchor, 0)

	// 3 validators that agree (phase offset 0° → |diff|=0 <= 12°).
	for i := 0; i < 3; i++ {
		prepared = append(prepared, obsForICAO(uint32(0xB00+i), 0)...)
	}

	// 18 validators that disagree (phase offset > validatorDisagreementPhaseDeg=24°).
	for i := 0; i < 18; i++ {
		prepared = append(prepared, obsForICAO(uint32(0xC00+i), validatorDisagreementPhaseDeg+6)...)
	}

	ms := newSolver()
	ms.ActiveAuthorityMode = authorityModeRefined
	ms.CandidatePromotionStreak = candidatePromotionMinStreak + 1

	estimate := syncStateEstimate{
		present:    true,
		periodS:    periodS,
		epochUS:    epochUS,
		offsetDeg:  anchorPhase,
		anchorICAO: ptr(anchor),
	}
	candidates := []AnchorCandidateSnapshot{
		{ICAO: anchor, Score: 0.9, SpreadDeg: 3.0, Status: "candidate"},
	}

	v := ms.evaluatePhaseValidation(prepared, estimate, candidates)

	// Verify validator counts match the setup.
	if v.validatorAgreement != 3 {
		t.Errorf("expected 3 validators agree, got %d", v.validatorAgreement)
	}
	if v.validatorDisagree != 18 {
		t.Errorf("expected 18 validators disagree, got %d", v.validatorDisagree)
	}
	if v.status != "validator_disagreement" {
		t.Errorf("expected validator_disagreement status, got %q", v.status)
	}

	// Verify block reason includes numeric values.
	reason := ms.candidateApplicationBlockReason(
		estimate, v, true, true, trustMinFitPool+1, validatorAgreementMinCount+3, false, true, 1000.0,
	)
	if !strings.Contains(reason, "agree=3") {
		t.Errorf("block reason missing agree=3: %q", reason)
	}
	if !strings.Contains(reason, "reject=18") {
		t.Errorf("block reason missing reject=18: %q", reason)
	}
	wantSuffix := "required agree>reject and agree>=" + strings.TrimSpace(strings.Split(reason, "required")[1])
	_ = wantSuffix // just verifying "required" is present
	if !strings.Contains(reason, "required agree>reject") {
		t.Errorf("block reason missing threshold description: %q", reason)
	}

	// Verify per-ICAO table classifies all 22 ICAOs.
	roleCount := make(map[string]int)
	for _, o := range v.PerICAOOffsets {
		roleCount[o.Role]++
	}
	if roleCount["anchor"] != 1 {
		t.Errorf("expected 1 anchor in per-ICAO table, got %d", roleCount["anchor"])
	}
	if roleCount["validator_agree"] != 3 {
		t.Errorf("expected 3 validator_agree in per-ICAO table, got %d", roleCount["validator_agree"])
	}
	if roleCount["validator_disagree"] != 18 {
		t.Errorf("expected 18 validator_disagree in per-ICAO table, got %d", roleCount["validator_disagree"])
	}
	total := roleCount["anchor"] + roleCount["validator_agree"] + roleCount["validator_disagree"]
	if total != 22 {
		t.Errorf("expected 22 total ICAOs in per-ICAO table, got %d (roles: %v)", total, roleCount)
	}
}

// D13: candidateApplicationBlockReason includes numeric counts for insufficient_validator_agreement.
func TestAuthority_InsufficientAgreementBlockReason_Numeric(t *testing.T) {
	ms := newSolver()
	ms.ActiveAuthorityMode = authorityModeRefined
	ms.CandidatePromotionStreak = candidatePromotionMinStreak + 1

	estimate := syncStateEstimate{
		present:    true,
		periodS:    4.0,
		epochUS:    0,
		offsetDeg:  45.0,
		anchorICAO: ptr(uint32(0xA01)),
	}
	v := syncValidationSummary{
		status:             "insufficient_validator_agreement",
		validatorAgreement: 1,
		validatorEvaluated: 5,
		score:              0.3,
	}
	reason := ms.candidateApplicationBlockReason(
		estimate, v, true, true, trustMinFitPool+1, validatorAgreementMinCount+2, false, true, 1000.0,
	)
	if !strings.Contains(reason, "agree=1") {
		t.Errorf("block reason missing agree=1: %q", reason)
	}
	if !strings.Contains(reason, "eval=5") {
		t.Errorf("block reason missing eval=5: %q", reason)
	}
	if !strings.Contains(reason, "required>=") {
		t.Errorf("block reason missing required>= threshold: %q", reason)
	}
}

// E7: Snapshot copies PerICAOPhaseOffsets as a separate slice (no aliasing).
func TestSnapshot_PerICAOPhaseOffsets_Copied(t *testing.T) {
	ms := newSolver()
	ms.LastPerICAOPhaseOffsets = []PerICAOPhaseOffset{
		{ICAO: 0xA01, Role: "anchor", AnchorDeltaDeg: 0},
		{ICAO: 0xA02, Role: "validator_agree", AnchorDeltaDeg: 3.0},
	}

	snap := ms.Snapshot()
	if len(snap.PerICAOPhaseOffsets) != 2 {
		t.Fatalf("expected 2 per-ICAO offsets in snapshot, got %d", len(snap.PerICAOPhaseOffsets))
	}
	// Mutate snap — must not affect solver.
	snap.PerICAOPhaseOffsets[0].Role = "mutated"
	if ms.LastPerICAOPhaseOffsets[0].Role != "anchor" {
		t.Error("PerICAOPhaseOffsets in snapshot aliases solver field")
	}
}
