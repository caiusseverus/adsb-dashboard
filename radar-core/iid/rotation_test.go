package iid

import (
	"math"
	"testing"
	"time"
)

// makeBursts builds a BurstRecord slice for one ICAO with evenly-spaced centroids.
func makeBursts(icao uint32, startUS float64, periodUS float64, count int) []BurstRecord {
	records := make([]BurstRecord, count)
	for i := range records {
		records[i] = BurstRecord{
			ICAO:       icao,
			CentroidUS: startUS + float64(i)*periodUS,
			NReplies:   2,
			FiredAt:    time.Now(),
		}
	}
	return records
}

// TestAnalyseICAO_BasicPeriod verifies that a uniform 4s period is recovered.
func TestAnalyseICAO_BasicPeriod(t *testing.T) {
	bursts := makeBursts(0xAA, 0, 4_000_000, 8)
	result := analyseICAO(bursts)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if math.Abs(result.medianPeriodS-4.0) > 0.01 {
		t.Errorf("median period = %.3f, want 4.0", result.medianPeriodS)
	}
}

// TestAnalyseICAO_InsufficientBursts returns nil for fewer than 4 bursts.
func TestAnalyseICAO_InsufficientBursts(t *testing.T) {
	bursts := makeBursts(0xAA, 0, 4_000_000, 3)
	if analyseICAO(bursts) != nil {
		t.Error("expected nil for <4 bursts")
	}
}

// TestAnalyseICAO_InvalidIntervals returns nil when intervals are all out of range.
func TestAnalyseICAO_InvalidIntervals(t *testing.T) {
	// 0.5s intervals — below the 1s filter floor.
	bursts := makeBursts(0xAA, 0, 500_000, 8)
	if analyseICAO(bursts) != nil {
		t.Error("expected nil for sub-1s intervals")
	}
}

// TestSnapIntervals_AllSnap verifies full snap for integer multiples.
func TestSnapIntervals_AllSnap(t *testing.T) {
	intervals := []float64{4.0, 4.0, 8.0, 4.0} // all multiples of 4s
	snap := snapIntervals(intervals, 4.0, 0.08)
	if snap.snapRate < 0.99 {
		t.Errorf("snap rate = %.2f, want ~1.0", snap.snapRate)
	}
}

// TestSnapIntervals_NoSnap verifies zero snap for mismatched intervals.
func TestSnapIntervals_NoSnap(t *testing.T) {
	intervals := []float64{3.0, 3.0, 3.0}
	snap := snapIntervals(intervals, 4.0, 0.08)
	if snap.snapRate > 0.01 {
		t.Errorf("snap rate = %.2f, want ~0.0", snap.snapRate)
	}
}

// TestAnalyseBurstRecords_SingleICAO verifies SINGLE_RADAR verdict for one clean ICAO.
func TestAnalyseBurstRecords_SingleICAO(t *testing.T) {
	records := makeBursts(0xBB, 0, 4_000_000, 12)
	model := AnalyseBurstRecords(records)

	if model.Status != "SINGLE_RADAR" {
		t.Errorf("status = %s, want SINGLE_RADAR", model.Status)
	}
	if model.DominantPeriodS == nil {
		t.Fatal("dominant period is nil")
	}
	if math.Abs(*model.DominantPeriodS-4.0) > 0.05 {
		t.Errorf("dominant period = %.3f, want ~4.0", *model.DominantPeriodS)
	}
	if model.NQualifying < 1 {
		t.Error("n_qualifying should be >= 1")
	}
}

// TestAnalyseBurstRecords_TwoICAOsSamePeriod verifies SINGLE_RADAR for two ICAOs
// with the same underlying period.
func TestAnalyseBurstRecords_TwoICAOsSamePeriod(t *testing.T) {
	var records []BurstRecord
	records = append(records, makeBursts(0xAA, 0, 4_000_000, 10)...)
	// 0xBB starts at 1s offset — same period but different phase.
	records = append(records, makeBursts(0xBB, 1_000_000, 4_000_000, 10)...)

	model := AnalyseBurstRecords(records)
	if model.DominantPeriodS == nil {
		t.Fatal("dominant period is nil")
	}
	if math.Abs(*model.DominantPeriodS-4.0) > 0.05 {
		t.Errorf("dominant period = %.3f, want ~4.0", *model.DominantPeriodS)
	}
	if model.NResidual != 0 {
		t.Errorf("n_residual = %d, want 0", model.NResidual)
	}
}

// TestAnalyseBurstRecords_InsufficientData returns INSUFFICIENT_DATA for empty input.
func TestAnalyseBurstRecords_InsufficientData(t *testing.T) {
	model := AnalyseBurstRecords(nil)
	if model.Status != "INSUFFICIENT_DATA" {
		t.Errorf("status = %s, want INSUFFICIENT_DATA", model.Status)
	}
}

// TestAnalyseBurstRecords_HarmonicFolding: 0xAA has 8s period (missed every other sweep),
// 0xBB has 4s period. After harmonic folding both should resolve to 4s.
func TestAnalyseBurstRecords_HarmonicFolding(t *testing.T) {
	var records []BurstRecord
	// 0xAA: seen every 8s (misses alternate sweeps of a 4s radar).
	records = append(records, makeBursts(0xAA, 0, 8_000_000, 8)...)
	// 0xBB: seen every 4s.
	records = append(records, makeBursts(0xBB, 0, 4_000_000, 10)...)

	model := AnalyseBurstRecords(records)
	if model.DominantPeriodS == nil {
		t.Fatal("dominant period is nil")
	}
	if math.Abs(*model.DominantPeriodS-4.0) > 0.1 {
		t.Errorf("dominant period after harmonic folding = %.3f, want ~4.0", *model.DominantPeriodS)
	}
}

// TestIIDState_AddAndTake verifies dirty-flag lifecycle.
func TestIIDState_AddAndTake(t *testing.T) {
	s := NewIIDState(3)

	// Before any adds, TakeIfDirty should return nil.
	if s.TakeIfDirty() != nil {
		t.Error("expected nil before any bursts added")
	}

	s.AddBurst(0xAA, 0, 2)
	s.AddBurst(0xAA, 4_000_000, 2)

	snap := s.TakeIfDirty()
	if snap == nil {
		t.Fatal("expected non-nil snapshot after adds")
	}
	if len(snap) != 2 {
		t.Errorf("snapshot len = %d, want 2", len(snap))
	}

	// Second TakeIfDirty should return nil (dirty cleared).
	if s.TakeIfDirty() != nil {
		t.Error("expected nil after take clears dirty flag")
	}
}

// TestIIDState_Reset verifies that Reset clears records and status.
func TestIIDState_Reset(t *testing.T) {
	s := NewIIDState(5)
	s.AddBurst(0xAA, 0, 2)
	s.Reset()

	if s.TakeIfDirty() != nil {
		t.Error("expected nil records after Reset")
	}
	if s.Status != "UNKNOWN" {
		t.Errorf("status = %s, want UNKNOWN after Reset", s.Status)
	}
}

// TestReinforce_PeriodConverges verifies that repeated Apply calls converge status.
func TestReinforce_PeriodConverges(t *testing.T) {
	s := NewIIDState(7)

	// Apply a clean model repeatedly to build support.
	period := 4.0
	for i := 0; i < 20; i++ {
		model := &RotationModel{
			DominantPeriodS:    &period,
			Status:             "SINGLE_RADAR",
			NQualifying:        6,
			PrimaryDirectCount: 6,
		}
		s.ApplyRotation(model)
	}

	st, p, _, support := s.Snapshot()
	if st != "SINGLE_RADAR" {
		t.Errorf("status = %s, want SINGLE_RADAR", st)
	}
	if p == nil || math.Abs(*p-4.0) > 0.01 {
		t.Errorf("period = %v, want ~4.0", p)
	}
	if support < primaryConfidenceTarget {
		t.Errorf("support = %d, want >= %d", support, primaryConfidenceTarget)
	}
}
