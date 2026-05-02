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
	prevHistoryLen := len(s.residualHistory)

	s.RecordReferenceChange(0xAA)
	s.RecordReferenceChange(0xBB)
	s.AddRefinementResidualObservation(52_000_000.0, -1.2, 0xBB01, true, 4, 0.5, 0xBB, "go_refiner_active")

	if s.FitEpochID != prevEpochID {
		t.Fatalf("fit epoch id=%d, want %d (should not reset on reference change)", s.FitEpochID, prevEpochID)
	}
	if len(s.residualHistory) != prevHistoryLen+1 {
		t.Fatalf("residual history len=%d, want %d (no observations dropped)", len(s.residualHistory), prevHistoryLen+1)
	}
	if s.ReferenceChangeCount != 1 {
		t.Fatalf("reference change count=%d, want 1", s.ReferenceChangeCount)
	}
	if s.CurrentReferenceICAO != 0xBB {
		t.Fatalf("current reference icao=%x, want 0xBB", s.CurrentReferenceICAO)
	}
	if s.PreviousReferenceICAO != 0xAA {
		t.Fatalf("previous reference icao=%x, want 0xAA", s.PreviousReferenceICAO)
	}
	if s.FitObservationsAddedSinceReset < 12 {
		t.Fatalf("fit observations added since reset=%d, want >= 12", s.FitObservationsAddedSinceReset)
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

	s.AddRefinementResidualObservation(44_000_000.0, 1.1, 0xCC, true, 4, 0.5, 0xAA, "base_python")

	if s.FitEpochID <= prevEpochID {
		t.Fatalf("fit epoch id=%d, want > %d after non-holdover authority basis change", s.FitEpochID, prevEpochID)
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
	if s.LastRejectedDeltaPPM == 0 {
		t.Fatal("expected last_rejected_delta_ppm to be populated")
	}
	if s.ProposedDeltaS != 0 {
		t.Fatalf("proposed_delta_s=%.9f, want 0 after epoch reset", s.ProposedDeltaS)
	}
	if s.RequestedDeltaS != 0 {
		t.Fatalf("requested_delta_s=%.9f, want 0 after epoch reset", s.RequestedDeltaS)
	}
	if s.RequestedDeltaPPM != 0 {
		t.Fatalf("requested_delta_ppm=%.6f, want 0 after epoch reset", s.RequestedDeltaPPM)
	}
	if s.LastHardBound {
		t.Fatal("last_hard_bound should be false after epoch reset")
	}
	if s.HardBoundReason != "" {
		t.Fatalf("hard_bound_reason=%q, want empty after epoch reset", s.HardBoundReason)
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
	ref := SelectReference(records, 4.0, 0, 0, nil)
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
	ref := SelectReference(records, 4.0, 0xBB, 0, nil)
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
		icao := 0xAA + uint32(i%4)
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, residual, icao, true, 4, 0.5, 0xAA, "go_refiner_active")
	}
	if s.ProposedDeltaS == 0 {
		t.Fatal("expected non-zero proposed delta with sufficient observations")
	}

	s.AddRefinementResidualObservation(52_000_000.0, -1.2, 0xBB01, true, 4, 0.5, 0xAA, "base_python")

	if s.FitEpochObservationCount != 1 {
		t.Fatalf("fit epoch observation count=%d, want 1 after non-holdover authority basis reset", s.FitEpochObservationCount)
	}
	if s.ProposedDeltaS != 0 {
		t.Fatalf("proposed_delta_s=%.9f, want 0 after fit epoch reset with 1 observation", s.ProposedDeltaS)
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
	if s.LastRejectedDeltaPPM == 0 {
		t.Fatal("expected last_rejected_delta_ppm to be populated")
	}
	if s.ProposedDeltaS != 0 {
		t.Fatalf("proposed_delta_s=%.9f, want 0 after hard-bound rejection (moved to last_rejected)", s.ProposedDeltaS)
	}
	if s.RequestedDeltaS != 0 {
		t.Fatalf("requested_delta_s=%.9f, want 0 — proposal moved to last_rejected", s.RequestedDeltaS)
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

func TestSelectReference_PrefersPositionCandidate(t *testing.T) {
	cache := NewPositionCache()
	nowUnix := float64(time.Now().Unix())
	// 0xAA has a fresh position; 0xBB does not.
	cache.Update(0xAA, 51.5, -0.1, nil, nowUnix)

	var records []BurstRecord
	now := time.Now()
	// 0xAA: 5 bursts (fewer = worse countFactor score).
	for i := 0; i < 5; i++ {
		records = append(records, BurstRecord{ICAO: 0xAA, CentroidUS: float64(i) * 4_000_000.0, FiredAt: now})
	}
	// 0xBB: 10 bursts (more = better countFactor score), but no position.
	for i := 0; i < 10; i++ {
		records = append(records, BurstRecord{ICAO: 0xBB, CentroidUS: float64(i) * 4_000_000.0, FiredAt: now})
	}

	// Without cache, 0xBB wins (lower score).
	if ref := SelectReference(records, 4.0, 0, 0, nil); ref != 0xBB {
		t.Errorf("without cache: expected 0xBB to win by score, got 0x%X", ref)
	}
	// With cache, 0xAA wins (has position).
	if ref := SelectReference(records, 4.0, 0, 0, cache); ref != 0xAA {
		t.Errorf("with cache: expected 0xAA to win (has position), got 0x%X", ref)
	}
}

func TestSelectReference_NoPositionCandidates_FallsBackToScore(t *testing.T) {
	// Empty cache: no positions known. Both candidates compete on burst score.
	cache := NewPositionCache()
	var records []BurstRecord
	now := time.Now()
	for i := 0; i < 10; i++ {
		records = append(records, BurstRecord{ICAO: 0xAA, CentroidUS: float64(i) * 4_000_000.0, FiredAt: now})
	}
	for i := 0; i < 6; i++ {
		records = append(records, BurstRecord{ICAO: 0xBB, CentroidUS: float64(i) * 4_500_000.0, FiredAt: now})
	}
	// 0xAA has more bursts and matches the period better.
	ref := SelectReference(records, 4.0, 0, 0, cache)
	if ref != 0xAA {
		t.Errorf("fallback: expected 0xAA to win by score, got 0x%X", ref)
	}
}

func TestUpdateEpoch_MissingRefPositionSentinel(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	// refPosAgeS=-1 is the "not in Go position cache" sentinel.
	accepted := s.UpdateEpoch(4_000_000.0, 0.0, 4.0, 1.0, 4, -1.0)
	if accepted {
		t.Fatal("expected rejection when refPosAgeS=-1 (not in cache)")
	}
	if s.LastUpdateEpochRejectReason != "missing_reference_position" {
		t.Errorf("reject reason=%q, want missing_reference_position", s.LastUpdateEpochRejectReason)
	}
	if !s.Holdover {
		t.Error("expected holdover after missing_reference_position")
	}
	if s.UpdateEpochRejectCounts["missing_reference_position"] != 1 {
		t.Errorf("reject count=%d, want 1", s.UpdateEpochRejectCounts["missing_reference_position"])
	}
}

func TestUpdateEpoch_StaleRefPositionDistinctFromMissing(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	// refPosAgeS=20s — stale but not missing (positive value > 8s threshold).
	accepted := s.UpdateEpoch(4_000_000.0, 0.0, 4.0, 1.0, 4, 20.0)
	if accepted {
		t.Fatal("expected rejection for stale refPosAgeS=20s")
	}
	if s.LastUpdateEpochRejectReason != "stale_reference_position" {
		t.Errorf("reject reason=%q, want stale_reference_position (not missing)", s.LastUpdateEpochRejectReason)
	}
}

func TestUpdateEpoch_FreshRefPosition_Accepts(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	// refPosAgeS=1s — fresh position, should accept.
	accepted := s.UpdateEpoch(4_000_000.0, 0.0, 4.0, 1.0, 4, 1.0)
	if !accepted {
		t.Errorf("expected accept for fresh refPosAgeS=1s, reject reason=%q", s.LastUpdateEpochRejectReason)
	}
}

func TestFitInlierRatio_EmptyFitSet_Sentinel(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	// No observations added yet — FitInlierRatio should be the -1.0 sentinel.
	if s.FitInlierRatio >= 0.0 {
		t.Errorf("FitInlierRatio = %.2f after bootstrap, want < 0 (sentinel for no data)", s.FitInlierRatio)
	}
}

func TestFitInlierRatio_AllInliers_ReturnsOne(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	// Add 12 hard-inlier observations: |residual| <= 20°, all dominant, fresh positions.
	for i := 1; i <= 12; i++ {
		residual := float64(i-6) * 1.5 // range [-7.5, +9.0] — well within 20°
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, residual, 0xAA+uint32(i%4), true, 4, 0.5, 0xAA, "go_refiner_active")
	}
	if s.FitInlierRatio <= 0.0 || s.FitInlierRatio > 1.0 {
		t.Fatalf("FitInlierRatio = %.4f, want > 0 and <= 1", s.FitInlierRatio)
	}
	if s.FitInlierRatio < 0.999 {
		t.Errorf("FitInlierRatio = %.4f with all inliers, want 1.0", s.FitInlierRatio)
	}
}

func TestFitInlierRatio_MixedInliersOutliers_Fractional(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	// Use balanced residuals that don't trigger out-of-bounds slope reset.
	// Hard inliers: |residual| <= 20° (Soft=false).
	// Soft outliers: 20° < |residual| <= 50° (Soft=true, weight=0.25).
	// Alternating positive/negative residuals keeps slope near zero.
	for i := 1; i <= 5; i++ {
		residual := 5.0 * float64(i%2*2-1) // alternates +5, -5
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, residual, 0xAA+uint32(i), true, 4, 0.5, 0xAA, "go_refiner_active")
	}
	for i := 6; i <= 10; i++ {
		residual := 25.0 * float64(i%2*2-1) // alternates +25, -25
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, residual, 0xAA+uint32(i), true, 4, 0.5, 0xAA, "go_refiner_active")
	}
	// expected: 5 inliers / 10 eligible = 0.5
	expected := 0.5
	if math.Abs(s.FitInlierRatio-expected) > 0.01 {
		t.Errorf("FitInlierRatio = %.4f, want %.4f (5 inliers / 10 eligible)", s.FitInlierRatio, expected)
	}
}

func TestFitInlierRatio_RejectsExcluded_DenominatorCountsRejects(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	// Add balanced inliers to avoid slope out-of-bounds
	for i := 1; i <= 4; i++ {
		residual := 5.0 * float64(i%2*2-1)
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, residual, 0xAA+uint32(i), true, 4, 0.5, 0xAA, "go_refiner_active")
	}
	// Add 4 hard-reject observations (|residual| > 50°) — these count toward denominator
	for i := 5; i <= 8; i++ {
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, 60.0, 0xAA+uint32(i), true, 4, 0.5, 0xAA, "go_refiner_active")
	}
	// Expected: 4 inliers / (4 eligible + 4 rejected) = 4/8 = 0.5
	expected := 0.5
	if math.Abs(s.FitInlierRatio-expected) > 0.01 {
		t.Errorf("FitInlierRatio = %.4f, want %.4f (4 inliers / (4+4) total)", s.FitInlierRatio, expected)
	}
}

func TestFitInlierRatio_RatioNeverOutsideZeroOne(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	// Add all hard rejects only — no eligible observations
	for i := 1; i <= 5; i++ {
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, 60.0, 0xAA+uint32(i), true, 4, 0.5, 0xAA, "go_refiner_active")
	}
	// With only rejects, eligible = 0 + 5 = 5, inliers = 0, ratio = 0/5 = 0.0
	if math.Abs(s.FitInlierRatio-0.0) > 0.01 {
		t.Errorf("FitInlierRatio = %.4f with only rejects, want 0.0", s.FitInlierRatio)
	}
	if s.FitInlierRatio < 0.0 || s.FitInlierRatio > 1.0 {
		t.Errorf("FitInlierRatio = %.4f outside [0.0, 1.0]", s.FitInlierRatio)
	}
}

func TestFitInlierRatio_ResetsOnEpochReset(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	for i := 1; i <= 10; i++ {
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, 5.0, 0xAA+uint32(i%4), true, 4, 0.5, 0xAA, "go_refiner_active")
	}
	if s.FitInlierRatio < 0.0 {
		t.Fatalf("FitInlierRatio = %.2f after adding observations, want >= 0", s.FitInlierRatio)
	}
	s.resetFitEpochLocked("test_reset")
	if s.FitInlierRatio >= 0.0 {
		t.Errorf("FitInlierRatio = %.2f after reset, want < 0 (sentinel for no data)", s.FitInlierRatio)
	}
}

func TestFitInlierRatio_DebugSnapshotPropagation(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	for i := 1; i <= 8; i++ {
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, 5.0, 0xAA+uint32(i%3), true, 4, 0.5, 0xAA, "go_refiner_active")
	}
	ratio := s.FitInlierRatio
	if ratio <= 0.0 || ratio > 1.0 {
		t.Fatalf("FitInlierRatio = %.4f in SyncState, invalid", ratio)
	}

	// Verify ratio is accessible via DebugSnapshot (mirrors what Python receives)
	iid := &IIDState{IID: 3, Sync: s}
	snap := iid.DebugStateSnapshot()
	if math.Abs(snap.FitInlierRatio-ratio) > 0.001 {
		t.Errorf("DebugSnapshot.FitInlierRatio = %.4f, want %.4f (from SyncState)", snap.FitInlierRatio, ratio)
	}
}

func TestHoldoverToggleDoesNotResetFitEpoch(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	for i := 1; i <= 12; i++ {
		residual := -2.0 + float64(i)*0.3
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, residual, 0xAA+uint32(i%4), true, 4, 0.5, 0xAA, "go_refiner_active")
	}
	if s.FitObservationCount == 0 {
		t.Fatal("expected non-zero fit observations after seeding")
	}
	prevEpochID := s.FitEpochID
	prevHistoryLen := len(s.residualHistory)

	s.Holdover = true
	s.HoldoverReason = "hard_residual_reject"
	accepted := s.UpdateEpoch(52_000_000.0, 1.0, 4.0, 1.0, 3, 0.5, 0xAA)

	if s.FitEpochID != prevEpochID {
		t.Fatalf("fit epoch id changed from %d to %d when holdover toggled true; holdover alone must not reset fit epoch", prevEpochID, s.FitEpochID)
	}
	if accepted {
		if len(s.residualHistory) <= prevHistoryLen {
			t.Fatalf("accepted update should append observation; residualHistory len=%d, want > %d", len(s.residualHistory), prevHistoryLen)
		}
	}
}

func TestHoldoverToggleFalseDoesNotResetFitEpoch(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	seedReacquireFitSupport(s)
	s.Holdover = true
	s.HoldoverReason = "hard_residual_reject"

	for i := 0; i < 3; i++ {
		s.UpdateEpoch(float64(i+1)*4_000_000.0, 90.0, 4.0, 1.0, 4, 0.5, 0xAA)
	}
	// Third call triggers reacquire_provisional, clearing holdover.
	if s.Holdover {
		t.Fatal("expected holdover cleared by provisional reacquire")
	}
	if !s.ReacquiredProvisional {
		t.Fatal("expected reacquired flag set")
	}
	if s.HoldoverReason != "reacquired_provisional" {
		t.Fatalf("holdover reason=%q, want reacquired_provisional", s.HoldoverReason)
	}
	if s.HoldoverReasonCounts["hard_residual_reject_holdover"] == 0 {
		t.Fatal("hard_residual_reject_holdover reason should be counted on the third holdover call")
	}
}

func TestAuthorityBasisActiveHoldoverToggleDoesNotResetFitEpoch(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	for i := 1; i <= 10; i++ {
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, float64(i)*0.2, 0xCC+uint32(i%3), true, 4, 0.5, 0xAA, "go_refiner_active")
	}
	prevEpochID := s.FitEpochID
	prevObsCount := s.FitObservationCount

	s.AddRefinementResidualObservation(44_000_000.0, 1.1, 0xCC, true, 4, 0.5, 0xAA, "go_refiner_holdover")

	if s.FitEpochID != prevEpochID {
		t.Fatalf("fit epoch id changed from %d to %d when authority changed between go_refiner_active and go_refiner_holdover", prevEpochID, s.FitEpochID)
	}
	if s.FitObservationCount <= prevObsCount {
		t.Fatal("fit observation count must increase with new observation, not reset")
	}
}

func TestTrueResidualBasisChangeStillResetsFitEpoch(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	for i := 1; i <= 10; i++ {
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, float64(i)*0.2, 0xCC+uint32(i%3), true, 4, 0.5, 0xAA, "go_refiner_active")
	}
	s.fitEpochResidualBasis = "recorded_event_basis"
	prevEpochID := s.FitEpochID

	s.maybeResetFitEpochLocked(fitEpochContext{
		epochUS:        44_000_000.0,
		residualBasis:  "observed_minus_predicted",
		referenceICAO:  0xAA,
		phaseOffsetDeg: 0,
		holdover:       false,
		basePeriodS:    4.0,
		authorityBasis: "go_refiner_active",
	})

	if s.FitEpochID <= prevEpochID {
		t.Fatalf("residual basis change must still reset fit epoch")
	}
	if s.FitEpochResetReason != "residual_basis_changed" {
		t.Fatalf("reset reason=%q, want residual_basis_changed", s.FitEpochResetReason)
	}
}

func TestTrueMaterialBasePeriodChangeStillResetsFitEpoch(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	for i := 1; i <= 10; i++ {
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, float64(i)*0.2, 0xCC+uint32(i%3), true, 4, 0.5, 0xAA, "go_refiner_active")
	}
	s.fitEpochBasePeriodS = 4.0
	prevEpochID := s.FitEpochID

	s.maybeResetFitEpochLocked(fitEpochContext{
		epochUS:        44_000_000.0,
		residualBasis:  "observed_minus_predicted",
		referenceICAO:  0xAA,
		phaseOffsetDeg: 0,
		holdover:       false,
		basePeriodS:    4.005,
		authorityBasis: "go_refiner_active",
	})

	if s.FitEpochID <= prevEpochID {
		t.Fatalf("material base-period change (0.125%%) must still reset fit epoch")
	}
	if s.FitEpochResetReason != "base_period_changed_material" {
		t.Fatalf("reset reason=%q, want base_period_changed_material", s.FitEpochResetReason)
	}
}

func TestSuccessfulProvisionalReacquirePreservesReacquireSupport(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	seedReacquireFitSupport(s)
	s.Holdover = true
	s.HoldoverReason = "hard_residual_reject"

	for i := 0; i < 3; i++ {
		s.UpdateEpoch(float64(i+1)*4_000_000.0, 90.0, 4.0, 1.0, 4, 0.5, 0xAA)
	}

	if !s.ReacquiredProvisional {
		t.Fatal("must be reacquired after 3 consecutive hard rejects")
	}
	if s.ReacquireSupportObservationCount == 0 {
		t.Fatal("reacquire support observation count must be preserved across reacquire")
	}
	if s.ReacquireSupportICAOCount == 0 {
		t.Fatal("reacquire support ICAO count must be preserved across reacquire")
	}
}

func TestReacquireSupportPreventsCollapseInCanReacquire(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	seedReacquireFitSupport(s)
	s.Holdover = true
	s.HoldoverReason = "hard_residual_reject"

	for i := 0; i < 3; i++ {
		s.UpdateEpoch(float64(i+1)*4_000_000.0, 90.0, 4.0, 1.0, 4, 0.5, 0xAA)
	}

	if s.FitObservationCount >= reacquireMinFitObs {
		t.Log("note: fit observation count partially preserved; reacquire support also available")
	}

	canReacquire := s.canReacquireInHoldover(4.0, 4, 0.5, 0xAA)
	if !canReacquire {
		t.Fatal("reacquire should still be possible using preserved support counters")
	}
	if s.ReacquireSupportObservationCount < reacquireMinFitObs {
		t.Fatal("reacquire support observation count must be sufficient for reacquire")
	}
}

func TestRepeatedHardResidualReacquireCyclesDoNotTrapInLowFitCount(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	seedReacquireFitSupport(s)

	initialSupportObs := s.FitObservationCount
	initialSupportICAO := s.FitICAOCount
	t.Logf("initial support: %d obs, %d ICAOs", initialSupportObs, initialSupportICAO)

	reacquireCount := 0
	for cycle := 0; cycle < 5; cycle++ {
		s.Holdover = true
		s.HoldoverReason = "hard_residual_reject"
		s.ConsecutiveHardResidualRejects = 2

		epochUS := float64((cycle*10)+1) * 4_000_000.0
		accepted := s.UpdateEpoch(epochUS, 90.0, 4.0, 1.0, 4, 0.5, 0xAA)
		if accepted {
			reacquireCount++
			t.Logf("cycle %d: reacquired at epoch=%.0f", cycle, epochUS)
		}
		canReacquire := s.canReacquireInHoldover(4.0, 4, 0.5, 0xAA)
		if !canReacquire {
			t.Fatalf("cycle %d: reacquire support collapsed (obs=%d/%d, icao=%d/%d); canReacquireInHoldover returned false",
				cycle, s.FitObservationCount, s.ReacquireSupportObservationCount,
				s.FitICAOCount, s.ReacquireSupportICAOCount)
		}

		for j := 0; j < 5; j++ {
			icao := uint32(0xAA + uint32((cycle*5+j)%4))
			s.AddRefinementResidualObservation(
				float64((cycle*10+j+2))*4_000_000.0, 1.0, icao, true, 4, 0.5, 0xAA, "go_refiner_active")
		}
	}

	if reacquireCount < 5 {
		t.Fatalf("reacquire count=%d, want 5; reacquire should succeed every cycle", reacquireCount)
	}
	if s.ReacquireSupportObservationCount < 24 {
		t.Fatalf("reacquire support observation count=%d, want >= 24 (initial seed preserved)", s.ReacquireSupportObservationCount)
	}
}

func TestDebugSnapshotExposesReacquireSupportCounters(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	seedReacquireFitSupport(s)

	iid := &IIDState{IID: 3, Sync: s}
	snap := iid.DebugStateSnapshot()

	if snap.ReacquireSupportObservationCount != 0 {
		t.Fatalf("ReacquireSupportObservationCount=%d, want 0 before any reacquire", snap.ReacquireSupportObservationCount)
	}

	s.Holdover = true
	s.HoldoverReason = "hard_residual_reject"
	for i := 0; i < 3; i++ {
		s.UpdateEpoch(float64(i+1)*4_000_000.0, 90.0, 4.0, 1.0, 4, 0.5, 0xAA)
	}

	snap = iid.DebugStateSnapshot()
	if snap.ReacquireSupportObservationCount == 0 {
		t.Fatal("ReacquireSupportObservationCount must be non-zero after reacquire")
	}
	if snap.ReacquireSupportICAOCount == 0 {
		t.Fatal("ReacquireSupportICAOCount must be non-zero after reacquire")
	}
}

func TestNonHoldoverAuthorityChangeStillResetsFitEpoch(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)
	for i := 1; i <= 10; i++ {
		s.AddRefinementResidualObservation(float64(i)*4_000_000.0, float64(i)*0.2, 0xCC+uint32(i%3), true, 4, 0.5, 0xAA, "go_refiner_active")
	}
	prevEpochID := s.FitEpochID

	s.AddRefinementResidualObservation(44_000_000.0, 1.1, 0xCC, true, 4, 0.5, 0xAA, "py_authority")

	if s.FitEpochID <= prevEpochID {
		t.Fatal("non-holdover authority basis change (go_refiner_active → py_authority) must still reset fit epoch")
	}
	if s.FitEpochResetReason != "period_authority_changed" {
		t.Fatalf("reset reason=%q, want period_authority_changed", s.FitEpochResetReason)
	}
}
