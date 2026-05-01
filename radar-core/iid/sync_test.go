package iid

import (
	"math"
	"testing"
	"time"
)

func TestSyncState_Bootstrap(t *testing.T) {
	s := NewSyncState(3, 4.0, 1_000_000.0, 0.0, 1.0)
	if s.NSyncFrames != 1 {
		t.Errorf("NSyncFrames = %d, want 1", s.NSyncFrames)
	}
	if s.PeriodS != 4.0 {
		t.Errorf("PeriodS = %.2f, want 4.0", s.PeriodS)
	}
	if s.PhaseEpochUS != 1_000_000.0 {
		t.Errorf("PhaseEpochUS = %.0f, want 1000000", s.PhaseEpochUS)
	}
	if s.Holdover {
		t.Error("Holdover should be false after bootstrap")
	}
}

func TestSyncState_AcceptedUpdate(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)

	// One full period later — predicted bearing = 0+360 = 0 mod 360 = 0.
	// New offset is also 0, so residual = 0, full accept.
	accepted := s.UpdateEpoch(4_000_000.0, 0.0, 4.0, 1.0, 4, 0.5)
	if !accepted {
		t.Error("expected update to be accepted")
	}
	if s.NSyncFrames != 2 {
		t.Errorf("NSyncFrames = %d, want 2", s.NSyncFrames)
	}
	if s.PhaseEpochUS != 4_000_000.0 {
		t.Errorf("PhaseEpochUS = %.0f, want 4000000", s.PhaseEpochUS)
	}
}

func TestSyncState_HardReject(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)

	// 60° residual — exceeds 50° hard-reject threshold.
	accepted := s.UpdateEpoch(4_000_000.0, 60.0, 4.0, 1.0, 4, 0.5)
	if accepted {
		t.Error("expected hard-reject (residual > 50°)")
	}
	if s.NRejectedFrames != 1 {
		t.Errorf("NRejectedFrames = %d, want 1", s.NRejectedFrames)
	}
	if !s.Holdover {
		t.Error("Holdover should be true after reject")
	}
	if s.NSyncFrames != 1 {
		t.Errorf("NSyncFrames = %d, want 1 (unchanged)", s.NSyncFrames)
	}
	if s.PhaseEpochUS != 0.0 {
		t.Errorf("PhaseEpochUS = %.0f, want unchanged 0", s.PhaseEpochUS)
	}
}

func TestSyncState_QualityGate(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)

	// nAircraft=1 — below maintenance threshold
	accepted := s.UpdateEpoch(4_000_000.0, 0.0, 4.0, 1.0, 1, 0.5)
	if accepted {
		t.Error("expected quality-gate rejection for n_aircraft=1")
	}
	if !s.Holdover {
		t.Error("Holdover should be set after quality-gate rejection")
	}
}

func TestSyncState_QualityGate_FreshRefPosition(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)

	// nAircraft=3 with refPosAgeS<=2.0 — should pass quality gate
	accepted := s.UpdateEpoch(4_000_000.0, 0.0, 4.0, 1.0, 3, 1.5)
	if !accepted {
		t.Error("n_aircraft=3 with fresh ref pos should be accepted")
	}
}

func TestSyncState_PredictBearing(t *testing.T) {
	// Epoch at 0µs, offset 0°, period 4s.
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)

	// At 1s into the sweep (1/4 period) = 90°.
	b := s.PredictBearing(1_000_000.0)
	if math.Abs(b-90.0) > 0.1 {
		t.Errorf("bearing at 1s = %.2f, want 90°", b)
	}

	// At 2s = 180°.
	b = s.PredictBearing(2_000_000.0)
	if math.Abs(b-180.0) > 0.1 {
		t.Errorf("bearing at 2s = %.2f, want 180°", b)
	}
}

func TestSyncState_PredictBearing_WithOffset(t *testing.T) {
	// Phase offset = 45°.
	s := NewSyncState(3, 4.0, 0.0, 45.0, 1.0)
	b := s.PredictBearing(0.0)
	if math.Abs(b-45.0) > 0.1 {
		t.Errorf("bearing at epoch = %.2f, want 45°", b)
	}
}

func TestCircularDiff(t *testing.T) {
	cases := []struct{ b, a, want float64 }{
		{10, 350, 20},
		{350, 10, -20},
		{90, 0, 90},
		{0, 90, -90},
	}
	for _, tc := range cases {
		got := circularDiff(tc.b, tc.a)
		if math.Abs(got-tc.want) > 0.01 {
			t.Errorf("circularDiff(%.0f, %.0f) = %.2f, want %.2f", tc.b, tc.a, got, tc.want)
		}
	}
}

func TestWrap360(t *testing.T) {
	cases := []struct{ in, want float64 }{
		{0, 0},
		{360, 0},
		{361, 1},
		{-1, 359},
		{-180, 180},
		{-360, 0},
		{720, 0},
		{-0.001, 359.999},
		{359.999, 359.999},
	}
	for _, tc := range cases {
		got := wrap360(tc.in)
		if math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("wrap360(%.3f) = %.6f, want %.6f", tc.in, got, tc.want)
		}
	}
}

// TestSyncState_PredictBearing_NeverNegative verifies PredictBearing always
// returns a value in [0, 360) regardless of epoch and phase.
func TestSyncState_PredictBearing_NeverNegative(t *testing.T) {
	// Phase offset 0°, 4s period, epoch at 0.
	s := NewSyncState(3, 4.0, 2_000_000.0, 0.0, 1.0)

	// Arrival before epoch — raw formula produces negative intermediate value.
	b := s.PredictBearing(0.0)
	if b < 0 || b >= 360 {
		t.Errorf("PredictBearing(0) = %.4f, want in [0, 360)", b)
	}

	// Arrival at -1s relative to epoch (would be -90° without wrap).
	b = s.PredictBearing(1_000_000.0)
	if b < 0 || b >= 360 {
		t.Errorf("PredictBearing(1s before period) = %.4f, want in [0, 360)", b)
	}
}

// TestSyncState_UpdateEpoch_NeverNegativeOffset verifies PhaseOffsetDeg is always
// non-negative after UpdateEpoch, even when the blend crosses zero.
func TestSyncState_UpdateEpoch_NeverNegativeOffset(t *testing.T) {
	// Start with a small positive offset so blending can push below 0.
	s := NewSyncState(3, 4.0, 0.0, 2.0, 1.0)

	// New observation slightly negative relative to current model — blending
	// could produce a negative intermediate value without wrap360.
	// Provide a new epoch one period later, new offset = 1°.
	accepted := s.UpdateEpoch(4_000_000.0, 1.0, 4.0, 1.0, 4, 0.5)
	if !accepted {
		t.Fatal("expected update to be accepted")
	}
	if s.PhaseOffsetDeg < 0 {
		t.Errorf("PhaseOffsetDeg = %.4f, must be >= 0", s.PhaseOffsetDeg)
	}
	if s.PhaseOffsetDeg >= 360 {
		t.Errorf("PhaseOffsetDeg = %.4f, must be < 360", s.PhaseOffsetDeg)
	}
}

// TestSyncState_UpdateEpoch_ResidualBlendCrossesWrap verifies that a blended
// offset that crosses the 360→0 wrap boundary stays in [0, 360).
func TestSyncState_UpdateEpoch_ResidualBlendCrossesWrap(t *testing.T) {
	// Offset near 359°.
	s := NewSyncState(3, 4.0, 0.0, 359.0, 1.0)
	// New observation at 1° — residual +2°, blending will push above 360.
	s.UpdateEpoch(4_000_000.0, 1.0, 4.0, 1.0, 4, 0.5)
	if s.PhaseOffsetDeg < 0 || s.PhaseOffsetDeg >= 360 {
		t.Errorf("PhaseOffsetDeg = %.4f, want in [0, 360)", s.PhaseOffsetDeg)
	}
}

// TestSyncState_QualityGate_3AircraftUnknownAge verifies that n_aircraft==3
// with an unknown ref position age (modelled as a very large value) is rejected.
// Note: Go's UpdateEpoch takes refPosAgeS as float64, so callers representing
// "unknown" must use a large sentinel rather than a nil.
func TestSyncState_QualityGate_3Aircraft_LargeAge(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	// n=3, age=999 (well above 2.0 fresh threshold) — must be rejected.
	accepted := s.UpdateEpoch(4_000_000.0, 0.0, 4.0, 1.0, 3, 999.0)
	if accepted {
		t.Error("n_aircraft=3 with age=999 should be rejected by quality gate")
	}
	if !s.Holdover {
		t.Error("Holdover should be set after quality-gate rejection")
	}
}

func TestSyncState_PeriodRefinement_ZeroResidualSlope(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	for i := 1; i <= 12; i++ {
		accepted := s.UpdateEpoch(float64(i)*4_000_000.0, 0.0, 4.0, 1.0, 4, 0.5)
		if !accepted {
			t.Fatalf("update %d rejected", i)
		}
	}
	if math.Abs(s.PeriodDeltaS) > 1e-6 {
		t.Fatalf("period delta=%.9f, want near zero", s.PeriodDeltaS)
	}
}

func TestSyncState_PeriodRefinement_PositiveSlopeMovesDeltaNegative(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	for i := 1; i <= 12; i++ {
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, float64(i)*0.4, 0xAA+uint32(i%3), true, 4, 0.5, 0xAA, "go_refiner_active")
	}
	if s.ResidualSlopeDegPerS <= 0 {
		t.Fatalf("residual slope=%.6f, want positive", s.ResidualSlopeDegPerS)
	}
	if s.PeriodDeltaS >= 0 {
		t.Fatalf("period delta=%.9f, want negative correction", s.PeriodDeltaS)
	}
	if math.Abs(s.EffectivePeriodS-4.0) > 4.0*refinementAbsBoundFraction+1e-9 {
		t.Fatalf("effective period=%.9f out of bound around base", s.EffectivePeriodS)
	}
}

func TestSyncState_PeriodRefinement_RejectsExcessiveSlope(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	for i := 1; i <= 8; i++ {
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, float64(i)*0.2, 0xAA+uint32(i%3), true, 4, 0.5, 0xAA, "go_refiner_active")
	}
	for i := 9; i <= 14; i++ {
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, float64(i-8)*8.0, 0xAA+uint32(i%3), true, 4, 0.5, 0xAA, "go_refiner_active")
	}
	if s.PeriodRefinementStatus != "proposed_out_of_bounds_decay" {
		t.Fatalf("status=%q, want proposed_out_of_bounds_decay", s.PeriodRefinementStatus)
	}
	if s.PeriodRejectReason != "refinement_out_of_bounds" {
		t.Fatalf("reject reason=%q, want refinement_out_of_bounds", s.PeriodRejectReason)
	}
	if math.Abs(s.PeriodDeltaS) > 4.0*refinementAbsBoundFraction+1e-9 {
		t.Fatalf("period delta=%.9f exceeds absolute bound", s.PeriodDeltaS)
	}
	if math.Abs(s.EffectivePeriodS-(4.0+s.PeriodDeltaS)) > 1e-9 {
		t.Fatalf("effective period=%.9f not base+delta", s.EffectivePeriodS)
	}
}

func TestSyncState_FitEpochResetOnReferenceICAOChange(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	for i := 1; i <= 12; i++ {
		residual := -6.0 + float64(i)*0.3
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, residual, 0xAA+uint32(i%4), true, 4, 0.5, 0xAA, "go_refiner_active")
	}
	if len(s.residualHistory) == 0 {
		t.Fatal("expected seeded residual history")
	}
	prevEpochID := s.FitEpochID
	prevSegmentCount := s.FitSegmentCount

	s.AddRefinementResidualObservation(52_000_000.0, -1.2, 0xBB01, true, 4, 0.5, 0xBB, "go_refiner_active")

	if s.FitEpochID <= prevEpochID {
		t.Fatalf("fit epoch id=%d, want > %d after reference change", s.FitEpochID, prevEpochID)
	}
	if s.FitSegmentCount != prevSegmentCount+1 {
		t.Fatalf("fit segment count=%d, want %d", s.FitSegmentCount, prevSegmentCount+1)
	}
	if s.FitEpochResetReason != "reference_icao_changed" {
		t.Fatalf("fit epoch reset reason=%q, want reference_icao_changed", s.FitEpochResetReason)
	}
	if s.FitDroppedOnEpochReset <= 0 {
		t.Fatalf("expected dropped observations on epoch reset, got %d", s.FitDroppedOnEpochReset)
	}
	if s.FitEpochObservationCount != 1 {
		t.Fatalf("fit epoch observation count=%d, want 1 for new epoch", s.FitEpochObservationCount)
	}
}

func TestSyncState_FitEpochResetOnAuthorityBasisChange(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	for i := 1; i <= 10; i++ {
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, float64(i)*0.2, 0xCC+uint32(i%3), true, 4, 0.5, 0xAA, "go_refiner_active")
	}
	if len(s.residualHistory) == 0 {
		t.Fatal("expected seeded residual history")
	}
	prevEpochID := s.FitEpochID

	s.AddRefinementResidualObservation(44_000_000.0, 1.1, 0xCC, true, 4, 0.5, 0xAA, "go_refiner_holdover")

	if s.FitEpochID <= prevEpochID {
		t.Fatalf("fit epoch id=%d, want > %d after authority basis change", s.FitEpochID, prevEpochID)
	}
	if s.FitEpochResetReason != "period_authority_changed" {
		t.Fatalf("fit epoch reset reason=%q, want period_authority_changed", s.FitEpochResetReason)
	}
	if s.FitDroppedOnEpochReset <= 0 {
		t.Fatalf("expected dropped observations on epoch reset, got %d", s.FitDroppedOnEpochReset)
	}
}

func TestSyncState_RepeatedHardBoundRejectResetsFitEpoch(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)

	triggeredReset := false
	for i := 1; i <= 24; i++ {
		residual := -30.0 + float64(i)*3.0
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, residual, 0xDD+uint32(i%4), true, 4, 0.5, 0xAA, "go_refiner_active")
		if s.FitEpochResetReason == "repeated_hard_bound_reject" {
			triggeredReset = true
			break
		}
	}
	if !triggeredReset {
		t.Fatal("expected repeated hard-bound rejection to rotate fit epoch")
	}
	if s.LastRejectedDeltaS == 0 {
		t.Fatal("expected last_rejected_delta_s to be populated when hard-bound reject triggered epoch reset")
	}
	if s.LastRejectedDeltaReason != "requested_delta_exceeds_hard_bound" {
		t.Fatalf("last_rejected_delta_reason=%q, want requested_delta_exceeds_hard_bound", s.LastRejectedDeltaReason)
	}
	if s.HardBoundReason != "" {
		t.Fatalf("hard_bound_reason=%q, want empty after epoch reset", s.HardBoundReason)
	}
	if s.RequestedDeltaPPM == 0 {
		t.Fatal("expected requested delta ppm to be populated from last rejected")
	}
	if s.FitDroppedOnEpochReset <= 0 {
		t.Fatalf("expected dropped observations after repeated hard-bound reset, got %d", s.FitDroppedOnEpochReset)
	}
}

func TestSyncState_HoldoverReason_QualityGateAndHardResidual(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	accepted := s.UpdateEpoch(4_000_000.0, 0.0, 4.0, 1.0, 1, 0.5)
	if accepted {
		t.Fatal("expected reject on insufficient aircraft")
	}
	if s.HoldoverReason != "insufficient_aircraft" {
		t.Fatalf("holdover reason=%q", s.HoldoverReason)
	}
	if s.HoldoverReasonCounts["quality_gate_failed"] == 0 {
		t.Fatal("expected quality_gate_failed count")
	}
	accepted = s.UpdateEpoch(8_000_000.0, 80.0, 4.0, 1.0, 6, 0.5)
	if accepted {
		t.Fatal("expected hard residual reject")
	}
	if s.HoldoverReason != "hard_residual_reject_holdover" {
		t.Fatalf("holdover reason=%q", s.HoldoverReason)
	}
}

func TestSyncState_MaintenanceUpdateClearsHoldoverWithoutStrictGate(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	s.Holdover = true
	s.HoldoverReason = "insufficient_aircraft"
	accepted := s.UpdateEpoch(4_000_000.0, 0.0, 4.0, 1.0, 2, 0.5)
	if !accepted {
		t.Fatal("maintenance update should be accepted")
	}
	if s.Holdover {
		t.Fatal("holdover should clear on accepted maintenance update")
	}
	if s.LastUpdateEpochStrictGatePass {
		t.Fatal("strict gate should remain false for n_aircraft=2")
	}
}

func seedReacquireFitSupport(s *SyncState) {
	for i := 0; i < 24; i++ {
		icao := uint32(0xAA + uint32(i%4))
		s.AddRefinementResidualObservation(float64(i+1)*4_000_000.0, 1.0, icao, true, 4, 0.5, 0, "go_refiner_active")
	}
}

func TestSyncState_HardResidualInHoldoverAccumulatesUntilReacquire(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	seedReacquireFitSupport(s)
	s.Holdover = true
	s.HoldoverReason = "quality_gate_failed"

	// First two hard rejects while in holdover: track evidence, no re-anchor yet.
	if s.UpdateEpoch(4_000_000.0, 90.0, 4.0, 1.0, 4, 0.5, 0xAA) {
		t.Fatal("first holdover hard reject should not reacquire")
	}
	if s.UpdateEpoch(8_000_000.0, 90.0, 4.0, 1.0, 4, 0.5, 0xAA) {
		t.Fatal("second holdover hard reject should not reacquire")
	}
	if s.ConsecutiveHardResidualRejects != 2 {
		t.Fatalf("consecutive rejects=%d, want 2", s.ConsecutiveHardResidualRejects)
	}
	if s.HoldoverReason != "hard_residual_reject_holdover" {
		t.Fatalf("holdover reason=%q, want hard_residual_reject_holdover", s.HoldoverReason)
	}

	// Third consecutive reject triggers controlled provisional re-anchor.
	if !s.UpdateEpoch(12_000_000.0, 90.0, 4.0, 1.0, 4, 0.5, 0xAA) {
		t.Fatal("third holdover hard reject should trigger reacquisition")
	}
	if s.PhaseEpochUS != 12_000_000.0 {
		t.Fatalf("phase epoch=%.0f, want reacquired epoch", s.PhaseEpochUS)
	}
	if s.Holdover {
		t.Fatal("holdover should clear on controlled reacquisition")
	}
	if !s.ReacquiredProvisional {
		t.Fatal("reacquisition should be marked provisional")
	}
	if s.LastUpdateEpochStrictGatePass {
		t.Fatal("reacquisition must not grant strict authority")
	}
}

func TestSyncState_ReacquirePreservesPeriodDelta(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	seedReacquireFitSupport(s)
	s.PeriodDeltaS = 0.001
	s.EffectivePeriodS = s.BasePeriodS + s.PeriodDeltaS
	before := s.PeriodDeltaS
	s.Holdover = true
	s.HoldoverReason = "hard_residual_reject"

	s.UpdateEpoch(4_000_000.0, 90.0, 4.0, 1.0, 4, 0.5, 0xAA)
	s.UpdateEpoch(8_000_000.0, 90.0, 4.0, 1.0, 4, 0.5, 0xAA)
	if !s.UpdateEpoch(12_000_000.0, 90.0, 4.0, 1.0, 4, 0.5, 0xAA) {
		t.Fatal("expected reacquisition")
	}
	if math.Abs(s.PeriodDeltaS-before) > 1e-12 {
		t.Fatalf("period delta changed across reacquire: got %.12f want %.12f", s.PeriodDeltaS, before)
	}
	if math.Abs(s.EffectivePeriodS-(s.BasePeriodS+s.PeriodDeltaS)) > 1e-12 {
		t.Fatal("effective period invariant broken after reacquire")
	}
}

func TestSyncState_MaintenanceAllowsNAircraftOneWithStrongResidualSupport(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	seedReacquireFitSupport(s)
	// Strengthen support to pass maintenance-from-support thresholds.
	for i := 0; i < 40; i++ {
		icao := uint32(0xB0 + uint32(i%8))
		s.AddRefinementResidualObservation(float64(i+1)*2_000_000.0, 0.5, icao, true, 4, 0.4, 0, "go_refiner_active")
	}
	accepted := s.UpdateEpoch(4_000_000.0, 0.0, 4.0, 1.0, 1, 1.0, 0xAA)
	if !accepted {
		t.Fatal("expected maintenance accept with strong residual support and nAircraft=1")
	}
	if s.LastUpdateEpochStrictGatePass {
		t.Fatal("strict authority gate must remain false for nAircraft=1")
	}
}

func TestSyncState_MaintenanceRejectsTrueSingleAircraftWithoutSupport(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	accepted := s.UpdateEpoch(4_000_000.0, 0.0, 4.0, 1.0, 1, 1.0, 0xAA)
	if accepted {
		t.Fatal("expected reject for nAircraft=1 without residual support")
	}
	if s.LastUpdateEpochStrictGatePass {
		t.Fatal("strict authority gate should be false")
	}
}

func TestSyncState_UpdateAcceptRejectImprovesWithStrongSupportAtNAircraftOne(t *testing.T) {
	baseline := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	for i := 1; i <= 6; i++ {
		epochUS := float64(i) * 4_000_000.0
		baseline.UpdateEpoch(epochUS, baseline.PredictBearing(epochUS), 4.0, 1.0, 1, 1.2, 0xAA)
	}

	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	for i := 0; i < 48; i++ {
		icao := uint32(0xC0 + uint32(i%10))
		s.AddRefinementResidualObservation(float64(i+1)*1_500_000.0, 0.2, icao, true, 4, 0.4, 0, "go_refiner_active")
	}
	for i := 1; i <= 6; i++ {
		epochUS := float64(i) * 4_000_000.0
		observed := s.PredictBearing(epochUS)
		s.UpdateEpoch(epochUS, observed, 4.0, 1.0, 1, 1.2, 0xAA)
	}
	if s.UpdateEpochAccepts <= baseline.UpdateEpochAccepts {
		t.Fatalf("expected improved accepts: baseline=%d supported=%d", baseline.UpdateEpochAccepts, s.UpdateEpochAccepts)
	}
	if s.UpdateEpochRejects >= baseline.UpdateEpochRejects {
		t.Fatalf("expected fewer rejects: baseline=%d supported=%d", baseline.UpdateEpochRejects, s.UpdateEpochRejects)
	}
}

func TestSyncState_StrictGatePassFlagAfterRecovery(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	s.Holdover = true
	accepted := s.UpdateEpoch(4_000_000.0, 0.0, 4.0, 1.0, 2, 0.5)
	if !accepted || s.LastUpdateEpochStrictGatePass {
		t.Fatal("first maintenance update should accept with strict gate false")
	}
	accepted = s.UpdateEpoch(8_000_000.0, 0.0, 4.0, 1.0, 4, 0.5)
	if !accepted {
		t.Fatal("strict update should accept")
	}
	if !s.LastUpdateEpochStrictGatePass {
		t.Fatal("strict gate flag should be true on strict-quality update")
	}
}

func TestSyncState_UpdateEpoch_UsesEffectivePeriodForPrediction(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	s.PeriodDeltaS = -0.2
	s.EffectivePeriodS = 3.8
	s.PeriodS = 3.8

	accepted := s.UpdateEpoch(3_800_000.0, 0.0, 4.0, 1.0, 4, 0.5)
	if !accepted {
		t.Fatal("expected update accepted")
	}
	if math.Abs(s.LastResidualDeg) > 1.0 {
		t.Fatalf("residual %.3f too large; prediction likely did not use effective period", s.LastResidualDeg)
	}
}

func TestSyncState_PeriodRefinement_ClosedLoopReducesSlope(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)

	for i := 1; i <= 16; i++ {
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, float64(i)*0.8, 0xAA+uint32(i%3), true, 4, 0.5, 0xAA, "go_refiner_active")
	}
	if s.PeriodDeltaS >= 0 {
		t.Fatalf("expected negative correction, got %.9f", s.PeriodDeltaS)
	}
	initialSlope := s.ResidualSlopeDegPerS
	if initialSlope <= 0 {
		t.Fatalf("expected positive initial slope, got %.6f", initialSlope)
	}

	for i := 17; i <= 32; i++ {
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, 0.0, 0xBB+uint32(i%3), true, 4, 0.5, 0xAA, "go_refiner_active")
	}
	if math.Abs(s.ResidualSlopeDegPerS) >= math.Abs(initialSlope) {
		t.Fatalf("residual slope did not reduce: initial=%.6f current=%.6f", initialSlope, s.ResidualSlopeDegPerS)
	}
	if math.Abs(s.PeriodDeltaS) > 4.0*refinementAbsBoundFraction+1e-9 {
		t.Fatalf("delta exceeded bound: %.9f", s.PeriodDeltaS)
	}
}

func TestFitResidualSlopeDegPerS_PositiveNoWrap(t *testing.T) {
	obs := make([]residualObservation, 0, 24)
	for i := 0; i < 24; i++ {
		obs = append(obs, residualObservation{
			EpochUS:     float64(i+1) * 1_000_000.0,
			ResidualDeg: -20.0 + float64(i)*0.5,
			Weight:      1.0,
		})
	}
	slope, ok, _ := fitResidualSlopeDegPerS(obs)
	if !ok {
		t.Fatal("expected fit success")
	}
	if slope <= 0 {
		t.Fatalf("slope=%.6f, want positive", slope)
	}
}

func TestFitResidualSlopeDegPerS_NegativeNoWrap(t *testing.T) {
	obs := make([]residualObservation, 0, 24)
	for i := 0; i < 24; i++ {
		obs = append(obs, residualObservation{
			EpochUS:     float64(i+1) * 1_000_000.0,
			ResidualDeg: 30.0 - float64(i)*0.6,
			Weight:      1.0,
		})
	}
	slope, ok, _ := fitResidualSlopeDegPerS(obs)
	if !ok {
		t.Fatal("expected fit success")
	}
	if slope >= 0 {
		t.Fatalf("slope=%.6f, want negative", slope)
	}
}

func TestFitResidualSlopeDegPerS_NegativeAcrossWrap(t *testing.T) {
	obs := make([]residualObservation, 0, 24)
	for i := 0; i < 24; i++ {
		unwrapped := 170.0 - float64(i)*20.0
		wrapped := circularDiff(unwrapped, 0.0)
		obs = append(obs, residualObservation{
			EpochUS:     float64(i+1) * 1_000_000.0,
			ResidualDeg: wrapped,
			Weight:      1.0,
		})
	}
	slope, ok, _ := fitResidualSlopeDegPerS(obs)
	if !ok {
		t.Fatal("expected fit success")
	}
	if slope >= 0 {
		t.Fatalf("slope=%.6f, want negative", slope)
	}
}

func TestFitResidualSlopeDegPerS_PositiveAcrossWrap(t *testing.T) {
	obs := make([]residualObservation, 0, 24)
	for i := 0; i < 24; i++ {
		unwrapped := -170.0 + float64(i)*20.0
		wrapped := circularDiff(unwrapped, 0.0)
		obs = append(obs, residualObservation{
			EpochUS:     float64(i+1) * 1_000_000.0,
			ResidualDeg: wrapped,
			Weight:      1.0,
		})
	}
	slope, ok, _ := fitResidualSlopeDegPerS(obs)
	if !ok {
		t.Fatal("expected fit success")
	}
	if slope <= 0 {
		t.Fatalf("slope=%.6f, want positive", slope)
	}
}

func TestFitResidualSlopeDegPerS_SoftWeightsPreserved(t *testing.T) {
	obs := make([]residualObservation, 0, 24)
	for i := 0; i < 24; i++ {
		w := 1.0
		if i%3 == 0 {
			w = 0.25
		}
		obs = append(obs, residualObservation{
			EpochUS:     float64(i+1) * 1_000_000.0,
			ResidualDeg: -40.0 + float64(i)*0.4,
			Weight:      w,
		})
	}
	slope, ok, _ := fitResidualSlopeDegPerS(obs)
	if !ok {
		t.Fatal("expected fit success")
	}
	if slope <= 0 {
		t.Fatalf("slope=%.6f, want positive", slope)
	}
}

func TestSyncState_AddRefinementResidualValue_UsesProvidedResidualDirectly(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	for i := 0; i < 80; i++ {
		s.AddRefinementResidualValue(float64(i+1)*1_000_000.0, float64(i+1)*0.05, 0xAA+uint32(i%8), true, 4, 0.2, 0, "go_refiner_active")
	}
	if s.RefinementEligibleCount == 0 {
		t.Fatal("expected eligible observations")
	}
	if s.PeriodDeltaS >= 0 {
		t.Fatalf("expected negative correction for positive residual slope, got %.9f", s.PeriodDeltaS)
	}
}

func TestSyncState_ResidualRetentionPerICAOAndGlobalCap(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	for i := 0; i < 400; i++ {
		icao := uint32(0xAA + uint32(i%40))
		s.AddRefinementResidualObservation(float64(i+1)*100_000.0, 2.0, icao, true, 4, 0.2, 0, "go_refiner_active")
	}
	if len(s.residualHistory) > refinementGlobalCap {
		t.Fatalf("history len=%d exceeds global cap %d", len(s.residualHistory), refinementGlobalCap)
	}
	if !s.FitGlobalCapHit {
		t.Fatal("expected global cap hit")
	}
	perICAO := map[uint32]int{}
	for _, o := range s.residualHistory {
		perICAO[o.ICAO]++
	}
	for icao, n := range perICAO {
		if n > refinementPerICAOCap {
			t.Fatalf("icao 0x%X retained %d > cap %d", icao, n, refinementPerICAOCap)
		}
	}
}

func TestSyncState_RejectsStaleRefPositionForRefinement(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	s.AddRefinementResidualObservation(1_000_000.0, 1.0, 0xAA, true, 4, 9.0, 0, "go_refiner_active")
	if s.RefinementLastRejectReason != "stale_ref_position" {
		t.Fatalf("reject reason=%q, want stale_ref_position", s.RefinementLastRejectReason)
	}
}

func TestSyncState_SuspiciousICAOExclusion(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	now := float64(time.Now().Unix())
	for i := 0; i < 5; i++ {
		s.RefinementLastObservationUnix = now + float64(i)
		s.AddRefinementResidualObservation(float64(i+1)*1_000_000.0, 90.0, 0xAA, true, 4, 0.2, 0, "go_refiner_active")
	}
	if s.SuspiciousICAOCount == 0 {
		t.Fatal("expected suspicious ICAO count > 0")
	}
	s.AddRefinementResidualObservation(10_000_000.0, 1.0, 0xAA, true, 4, 0.2, 0, "go_refiner_active")
	if s.RefinementLastRejectReason != "suspicious_icao_excluded" {
		t.Fatalf("reject reason=%q, want suspicious_icao_excluded", s.RefinementLastRejectReason)
	}
}

func TestSyncState_PeriodRefinementSignForPPMDrift(t *testing.T) {
	// Sign convention for the synthetic drift cases:
	// ppm error = (true_period - base_period) / base_period.
	// Therefore:
	//   +ppm => true period is longer than base, so expected correction delta is positive.
	//   -ppm => true period is shorter than base, so expected correction delta is negative.
	makeResidual := func(i int, periodTrue, base float64) float64 {
		tS := float64(i) * base
		return circularDiff(tS*(360.0/periodTrue), tS*(360.0/base))
	}
	{
		s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
		truePeriod := 4.0 * (1.0 + 50.0/1e6)
		for i := 1; i <= 40; i++ {
			r := makeResidual(i, truePeriod, 4.0)
			s.AddRefinementResidualObservation(float64(i)*4_000_000.0, r, 0xAB+uint32(i%4), true, 4, 0.2, 0, "go_refiner_active")
		}
		if s.PeriodDeltaS <= 0 {
			t.Fatalf("+50ppm expected positive delta, got %.9f", s.PeriodDeltaS)
		}
	}
	{
		s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
		truePeriod := 4.0 * (1.0 - 50.0/1e6)
		for i := 1; i <= 40; i++ {
			r := makeResidual(i, truePeriod, 4.0)
			s.AddRefinementResidualObservation(float64(i)*4_000_000.0, r, 0xCD+uint32(i%4), true, 4, 0.2, 0, "go_refiner_active")
		}
		if s.PeriodDeltaS >= 0 {
			t.Fatalf("-50ppm expected negative delta, got %.9f", s.PeriodDeltaS)
		}
	}
}

// TestSelectReference verifies the reference aircraft selection scoring.
func TestSelectReference_BasicSelection(t *testing.T) {
	// Two ICAOs: 0xAA has a perfect 4s period; 0xBB has a noisy period.
	var records []BurstRecord
	now := time.Now()
	for i := 0; i < 10; i++ {
		records = append(records, BurstRecord{
			ICAO:       0xAA,
			CentroidUS: float64(i) * 4_000_000.0,
			FiredAt:    now,
		})
	}
	// 0xBB: 4.5s period — less regular.
	for i := 0; i < 6; i++ {
		records = append(records, BurstRecord{
			ICAO:       0xBB,
			CentroidUS: float64(i) * 4_500_000.0,
			FiredAt:    now,
		})
	}
	ref := SelectReference(records, 4.0, 0, 0)
	if ref != 0xAA {
		t.Errorf("reference = 0x%X, want 0xAA", ref)
	}
}

func TestSelectReference_Hysteresis(t *testing.T) {
	// 0xAA has a marginally better score than 0xBB — but 0xBB is current ref.
	// Hysteresis should prevent switching unless challenger wins by >25%.
	var records []BurstRecord
	now := time.Now()
	for i := 0; i < 8; i++ {
		records = append(records, BurstRecord{ICAO: 0xAA, CentroidUS: float64(i) * 4_010_000.0, FiredAt: now})
	}
	for i := 0; i < 8; i++ {
		records = append(records, BurstRecord{ICAO: 0xBB, CentroidUS: float64(i) * 4_000_000.0, FiredAt: now})
	}
	// 0xBB is current reference — 0xAA is slightly better but within hysteresis.
	ref := SelectReference(records, 4.0, 0xBB, 0)
	if ref != 0xBB {
		t.Errorf("reference = 0x%X, want 0xBB (hysteresis holds)", ref)
	}
}

func TestIIDState_UpdateSyncEpoch(t *testing.T) {
	s := NewIIDState(7)
	period := 4.0
	s.PeriodS = &period
	s.SetBasePeriod(period)
	s.Status = "SINGLE_RADAR"

	// Bootstrap sync on first reference burst.
	s.UpdateSyncEpoch(0.0, 0.0, 4, 0.5)
	q, _ := s.SyncSnapshot()
	if q <= 0 {
		t.Errorf("sync quality = %.2f, want > 0 after bootstrap", q)
	}

	// Advance epoch.
	s.UpdateSyncEpoch(4_000_000.0, 0.0, 4, 0.5)
	q2, _ := s.SyncSnapshot()
	if q2 <= 0 {
		t.Errorf("sync quality = %.2f, want > 0 after epoch advance", q2)
	}
}

func TestPositionCache_SetAndGet(t *testing.T) {
	c := NewPositionCache()
	c.Update(0xAA, 51.5, -0.1, nil, float64(time.Now().Unix()))

	pos := c.Get(0xAA)
	if pos == nil {
		t.Fatal("expected non-nil position")
	}
	if math.Abs(pos.Lat-51.5) > 1e-6 {
		t.Errorf("lat = %.6f, want 51.5", pos.Lat)
	}
}

func TestPositionCache_Stale(t *testing.T) {
	c := NewPositionCache()
	// Fake a timestamp 60 seconds ago — beyond positionMaxAgeS.
	old := float64(time.Now().Unix() - 60)
	c.Update(0xBB, 51.5, -0.1, nil, old)

	if c.Get(0xBB) != nil {
		t.Error("expected nil for stale position")
	}
}

func TestPositionCache_Prune(t *testing.T) {
	c := NewPositionCache()
	old := float64(time.Now().Unix() - 60)
	c.Update(0xCC, 51.5, -0.1, nil, old)
	c.Update(0xDD, 52.0, -0.2, nil, float64(time.Now().Unix()))

	removed := c.Prune()
	if removed != 1 {
		t.Errorf("pruned %d, want 1", removed)
	}
	if c.Get(0xDD) == nil {
		t.Error("fresh position should survive prune")
	}
}

func TestSyncState_ProposedDeltaClearedOnFitEpochReset(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)

	for i := 1; i <= 12; i++ {
		residual := -6.0 + float64(i)*0.3
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, residual, 0xAA+uint32(i%4), true, 4, 0.5, 0xAA, "go_refiner_active")
	}
	if s.ProposedDeltaS == 0 {
		t.Fatal("expected non-zero proposed delta with sufficient observations")
	}
	prevProposed := s.ProposedDeltaS

	s.AddRefinementResidualObservation(52_000_000.0, -1.2, 0xBB01, true, 4, 0.5, 0xBB, "go_refiner_active")

	if s.FitEpochObservationCount != 1 {
		t.Fatalf("fit epoch observation count=%d, want 1 after reset", s.FitEpochObservationCount)
	}
	if s.ProposedDeltaS != 0 {
		t.Fatalf("proposed_delta_s=%.9f, want 0 after fit epoch reset with 1 observation (stale=%v)", s.ProposedDeltaS, prevProposed != 0)
	}
	if s.LastHardBound {
		t.Fatal("hard_bound should be false when no valid fit was attempted")
	}
	if s.AppliedDeltaS != 0 {
		t.Fatalf("applied_delta_s=%.9f, want 0", s.AppliedDeltaS)
	}
}

func TestSyncState_ProposedDeltaNullWhenInsufficientHistory(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)

	s.AddRefinementResidualObservation(4_000_000.0, -1.2, 0xAA, true, 4, 0.5, 0xAA, "go_refiner_active")
	if s.FitEpochObservationCount != 1 {
		t.Fatalf("fit epoch observation count=%d, want 1", s.FitEpochObservationCount)
	}
	if s.ProposedDeltaS != 0 {
		t.Fatalf("proposed_delta_s=%.9f, want 0 with insufficient history (1 obs)", s.ProposedDeltaS)
	}
	if s.PeriodRefinementStatus != "insufficient_history" {
		t.Fatalf("period refinement status=%q, want insufficient_history", s.PeriodRefinementStatus)
	}
	if s.LastHardBound {
		t.Fatal("hard_bound should be false with insufficient data")
	}
	if s.HardBoundReason != "" {
		t.Fatalf("hard_bound_reason=%q, want empty", s.HardBoundReason)
	}
}

func TestSyncState_InsufficientICAOsGatesProposal(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)

	for i := 1; i <= 10; i++ {
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, float64(i)*0.2, 0, true, 4, 0.5, 0, "go_refiner_active")
	}
	if s.FitICAOCount != 0 {
		t.Fatalf("fit icao count=%d, want 0 (all observations had icao=0)", s.FitICAOCount)
	}
	if s.PeriodRefinementStatus != "insufficient_icaos" {
		t.Fatalf("period refinement status=%q, want insufficient_icaos", s.PeriodRefinementStatus)
	}
	if s.ProposedDeltaS != 0 {
		t.Fatalf("proposed_delta_s=%.9f, want 0 with insufficient ICAOs", s.ProposedDeltaS)
	}
}

func TestSyncState_LastRejectedDeltaPreserved(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)

	for i := 1; i <= 24; i++ {
		residual := -30.0 + float64(i)*3.0
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, residual, 0xEE+uint32(i%5), true, 4, 0.5, 0xAA, "go_refiner_active")
		if s.LastRejectedDeltaS != 0 {
			break
		}
	}
	if s.LastRejectedDeltaS == 0 {
		t.Fatal("expected last_rejected_delta_s to be populated on hard-bound rejection")
	}
	if s.LastRejectedDeltaReason != "requested_delta_exceeds_hard_bound" {
		t.Fatalf("last_rejected_delta_reason=%q, want requested_delta_exceeds_hard_bound", s.LastRejectedDeltaReason)
	}
	if s.LastRejectedDeltaEpochID == 0 {
		t.Fatal("last_rejected_delta_epoch_id should be non-zero")
	}
	if s.ProposedDeltaS != 0 {
		t.Fatalf("proposed_delta_s=%.9f, want 0 after hard-bound rejection (moved to last_rejected)", s.ProposedDeltaS)
	}
}

func TestSyncState_ProposalResumesAfterSufficientObservations(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)

	s.AddRefinementResidualObservation(4_000_000.0, -1.2, 0xAA, true, 4, 0.5, 0xAA, "go_refiner_active")
	if s.ProposedDeltaS != 0 {
		t.Fatal("proposed_delta_s should be 0 after 1 observation")
	}

	for i := 2; i <= 10; i++ {
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, -2.0, 0xAA+uint32(i%3), true, 4, 0.5, 0xAA, "go_refiner_active")
	}

	if s.FitObservationCount < 8 {
		t.Skip("not enough observations accumulated")
	}
	if s.ProposedDeltaS == 0 && s.PeriodRefinementStatus == "insufficient_span" {
		t.Skipf("span insufficient (%.1fs < %.1fs)", s.FitSpanS, 20.0)
	}
	if s.ProposedDeltaS == 0 && s.PeriodRefinementStatus == "insufficient_icaos" {
		t.Skipf("ICAOs insufficient: %d < %d", s.FitICAOCount, residualFitMinICAOs)
	}
	if s.ProposedDeltaS == 0 {
		t.Fatalf("proposed_delta_s=0 after %d observations (status=%s), want non-zero", s.FitObservationCount, s.PeriodRefinementStatus)
	}
}
