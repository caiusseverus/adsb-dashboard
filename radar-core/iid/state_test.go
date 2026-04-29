package iid

import "testing"

func TestRefreshReference_PrefersDominantFamily(t *testing.T) {
	s := NewIIDState(7)
	period := 4.0
	s.PeriodS = &period
	s.SetBasePeriod(period)
	s.LastRotationModel = &RotationModel{
		Family: &ICAOFamily{
			FoldedICAOs:   map[uint32]struct{}{0xAAAAAA: {}},
			ResidualICAOs: map[uint32]struct{}{0xBBBBBB: {}},
		},
	}
	current := uint32(0xBBBBBB)
	s.RefICAO = &current

	records := []BurstRecord{
		{ICAO: 0xAAAAAA, CentroidUS: 0},
		{ICAO: 0xAAAAAA, CentroidUS: 4_000_000},
		{ICAO: 0xAAAAAA, CentroidUS: 8_000_000},
		{ICAO: 0xAAAAAA, CentroidUS: 12_000_000},
		{ICAO: 0xBBBBBB, CentroidUS: 0},
		{ICAO: 0xBBBBBB, CentroidUS: 4_100_000},
		{ICAO: 0xBBBBBB, CentroidUS: 8_200_000},
		{ICAO: 0xBBBBBB, CentroidUS: 12_300_000},
	}
	s.RefreshReference(records, 12_000_000)

	if s.RefICAO == nil {
		t.Fatal("reference ICAO was cleared, expected dominant-family candidate")
	}
	if *s.RefICAO != 0xAAAAAA {
		t.Fatalf("reference ICAO=0x%X, want 0xAAAAAA", *s.RefICAO)
	}
}

func TestRefreshReference_ClearsWhenNoDominantCandidates(t *testing.T) {
	s := NewIIDState(9)
	period := 4.0
	s.PeriodS = &period
	s.SetBasePeriod(period)
	s.LastRotationModel = &RotationModel{
		Family: &ICAOFamily{
			FoldedICAOs:   map[uint32]struct{}{0xCCCCCC: {}}, // not present in records
			ResidualICAOs: map[uint32]struct{}{0xBBBBBB: {}},
		},
	}
	current := uint32(0xBBBBBB)
	s.RefICAO = &current

	records := []BurstRecord{
		{ICAO: 0xBBBBBB, CentroidUS: 0},
		{ICAO: 0xBBBBBB, CentroidUS: 4_000_000},
		{ICAO: 0xBBBBBB, CentroidUS: 8_000_000},
		{ICAO: 0xBBBBBB, CentroidUS: 12_000_000},
	}
	s.RefreshReference(records, 12_000_000)

	if s.RefICAO != nil {
		t.Fatalf("reference ICAO not cleared, got 0x%X", *s.RefICAO)
	}
}

func TestDFBasePeriodOverridesDisagreeingCompactPeriod(t *testing.T) {
	s := NewIIDState(12)
	compactPeriod := 2.01
	s.PeriodS = &compactPeriod
	s.Status = "SINGLE_RADAR"

	dfBasePeriod := 4.79
	s.SetBasePeriod(dfBasePeriod)

	if got := s.OperationalPeriodSnapshot(); got == nil || *got != dfBasePeriod {
		t.Fatalf("operational period=%v, want DF base %.2f", got, dfBasePeriod)
	}
	snap := s.DebugStateSnapshot()
	if snap.PeriodAgreesWithDF {
		t.Fatal("expected compact/DF period disagreement")
	}
	if snap.PeriodRejectReason != "compact_period_disagrees_with_df" {
		t.Fatalf("period reject reason=%q", snap.PeriodRejectReason)
	}
	if snap.EffectivePeriodS != dfBasePeriod {
		t.Fatalf("effective period=%.2f, want DF base %.2f", snap.EffectivePeriodS, dfBasePeriod)
	}
	if snap.PeriodDeltaS != 0 {
		t.Fatalf("period delta=%.6f, want 0", snap.PeriodDeltaS)
	}
}

func TestAddBurst_UsesDensityAwareCap(t *testing.T) {
	s := NewIIDState(11)

	// active=100 => cap ~= 100 * 16 * 1.4 = 2240 (bounded)
	for i := 0; i < 2500; i++ {
		s.AddBurst(0xAAAAAA, float64(i)*1_000_000.0, 2, 100)
	}
	snap := s.DebugStateSnapshot()
	if snap.BurstRecordsDynamicCap != 2240 {
		t.Fatalf("dynamic cap=%d, want 2240", snap.BurstRecordsDynamicCap)
	}
	if snap.BurstRecordsTotal != 2240 {
		t.Fatalf("retained=%d, want 2240", snap.BurstRecordsTotal)
	}
	if !snap.BurstRecordsCapHit {
		t.Fatal("expected burst_records_cap_hit=true")
	}
	if snap.BurstRecordsCapHitsTotal == 0 {
		t.Fatal("expected burst_records_cap_hits_total > 0")
	}
}
