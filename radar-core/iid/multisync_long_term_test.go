package iid

// multisync_long_term_test.go — tests for the long-term estimator separation
// (Layers 1–3). Layer 1 establishes the diagnostic surface: short-window
// `Local*` measurements distinct from `LongTerm*` / branch state. Layers 2/3
// add behaviour to those `LongTerm*` fields; tests for that behaviour are
// added in the same file alongside their layer.

import (
	"math"
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
	// Layer 1: applied period and local measurement still come from the same
	// place. They should match within numerical noise.
	if math.Abs(ms.LastLocalPeriodMeasurementS-ms.PeriodS) > 1e-6 {
		t.Errorf("Layer 1: LocalPeriodMeasurementS (%v) should match PeriodS (%v) — Layer 2 introduces divergence",
			ms.LastLocalPeriodMeasurementS, ms.PeriodS)
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
	// Layer 1: long-term placeholder fields remain zero — no behaviour wired yet.
	if snap.LongTermPeriodEstimateS != 0 {
		t.Errorf("Layer 1 placeholder: LongTermPeriodEstimateS should be 0, got %v", snap.LongTermPeriodEstimateS)
	}
	if snap.BranchEstimatorConfidence != 0 {
		t.Errorf("Layer 1 placeholder: BranchEstimatorConfidence should be 0, got %v", snap.BranchEstimatorConfidence)
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
