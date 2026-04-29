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
}

func TestSyncState_QualityGate(t *testing.T) {
	s := NewSyncState(3, 4.0, 0.0, 0.0, 1.0)

	// nAircraft=2 — below threshold (need >=4, or >=3 with fresh pos)
	accepted := s.UpdateEpoch(4_000_000.0, 0.0, 4.0, 1.0, 2, 0.5)
	if accepted {
		t.Error("expected quality-gate rejection for n_aircraft=2")
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
		// Increasing residual drift over time.
		newOffset := float64(i) * 1.0
		accepted := s.UpdateEpoch(float64(i)*4_000_000.0, newOffset, 4.0, 1.0, 4, 0.5)
		if !accepted {
			t.Fatalf("update %d rejected", i)
		}
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
		accepted := s.UpdateEpoch(float64(i)*4_000_000.0, float64(i)*0.2, 4.0, 1.0, 4, 0.5)
		if !accepted {
			t.Fatalf("warm-up update %d rejected", i)
		}
	}
	for i := 9; i <= 14; i++ {
		// Large drift slope (while staying under hard residual reject gate)
		// => out-of-bounds proposed correction.
		accepted := s.UpdateEpoch(float64(i)*4_000_000.0, float64(i-8)*8.0, 4.0, 1.0, 4, 0.5)
		if !accepted {
			t.Fatalf("high-slope update %d rejected", i)
		}
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

	// Initial consistent drift pushes correction.
	for i := 1; i <= 16; i++ {
		accepted := s.UpdateEpoch(float64(i)*4_000_000.0, float64(i)*0.8, 4.0, 1.0, 4, 0.5)
		if !accepted {
			t.Fatalf("initial update %d rejected", i)
		}
	}
	if s.PeriodDeltaS >= 0 {
		t.Fatalf("expected negative correction, got %.9f", s.PeriodDeltaS)
	}
	initialSlope := s.ResidualSlopeDegPerS
	if initialSlope <= 0 {
		t.Fatalf("expected positive initial slope, got %.6f", initialSlope)
	}

	// Continue with observations that follow corrected prediction; loop should settle.
	for i := 17; i <= 32; i++ {
		newEpochUS := float64(i) * 4_000_000.0
		predicted := wrap360(s.predictBearingAt(newEpochUS, s.EffectivePeriodS*1e6))
		accepted := s.UpdateEpoch(newEpochUS, predicted, 4.0, 1.0, 4, 0.5)
		if !accepted {
			t.Fatalf("settling update %d rejected", i)
		}
	}
	if math.Abs(s.ResidualSlopeDegPerS) >= math.Abs(initialSlope) {
		t.Fatalf("residual slope did not reduce: initial=%.6f current=%.6f", initialSlope, s.ResidualSlopeDegPerS)
	}
	if math.Abs(s.PeriodDeltaS) > 4.0*refinementAbsBoundFraction+1e-9 {
		t.Fatalf("delta exceeded bound: %.9f", s.PeriodDeltaS)
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
