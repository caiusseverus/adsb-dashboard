package iid

import "testing"

func TestRefreshReference_PrefersDominantFamily(t *testing.T) {
	s := NewIIDState(7)
	period := 4.0
	s.PeriodS = &period
	s.SetBasePeriod(period)
	s.LastRotationModel = &RotationModel{
		DominantPeriodS: &period,
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
		DominantPeriodS: &period,
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

func TestSyncProtocolSnapshotRejectsDFPeriodDisagreement(t *testing.T) {
	s := NewIIDState(13)
	compactPeriod := 2.01
	dfBasePeriod := 4.79
	s.PeriodS = &compactPeriod
	s.SetBasePeriod(dfBasePeriod)
	s.Status = "SINGLE_RADAR"
	s.Sync = NewSyncState(s.IID, dfBasePeriod, 0, 0, 1.0)
	s.Sync.PeriodAgreesWithDF = false
	s.Sync.PeriodRejectReason = "compact_period_disagrees_with_df"

	_, usable, periodS, _, _, _, _, _, _, _, _ := s.SyncProtocolSnapshot()
	if usable {
		t.Fatal("sync usable despite DF period disagreement")
	}
	if periodS == nil || *periodS != dfBasePeriod {
		t.Fatalf("sync period=%v, want DF base %.2f", periodS, dfBasePeriod)
	}
}

func TestRefreshReferenceIgnoresDisagreeingGoFamilyGate(t *testing.T) {
	s := NewIIDState(14)
	dfBasePeriod := 4.79
	compactPeriod := 2.01
	s.SetBasePeriod(dfBasePeriod)
	s.LastRotationModel = &RotationModel{
		DominantPeriodS: &compactPeriod,
		Family: &ICAOFamily{
			FoldedICAOs:   map[uint32]struct{}{0xAAAAAA: {}},
			ResidualICAOs: map[uint32]struct{}{0xBBBBBB: {}},
		},
	}

	records := []BurstRecord{
		{ICAO: 0xAAAAAA, CentroidUS: 0},
		{ICAO: 0xAAAAAA, CentroidUS: 2_010_000},
		{ICAO: 0xAAAAAA, CentroidUS: 4_020_000},
		{ICAO: 0xAAAAAA, CentroidUS: 6_030_000},
		{ICAO: 0xBBBBBB, CentroidUS: 0},
		{ICAO: 0xBBBBBB, CentroidUS: 4_790_000},
		{ICAO: 0xBBBBBB, CentroidUS: 9_580_000},
		{ICAO: 0xBBBBBB, CentroidUS: 14_370_000},
	}
	s.RefreshReference(records, 14_370_000)

	if s.RefICAO == nil {
		t.Fatal("reference ICAO was not selected")
	}
	if *s.RefICAO != 0xBBBBBB {
		t.Fatalf("reference ICAO=0x%X, want DF-period candidate 0xBBBBBB", *s.RefICAO)
	}
}

func TestFamilySnapshotSuppressesDisagreeingGoFamilyGate(t *testing.T) {
	s := NewIIDState(16)
	dfBasePeriod := 4.79
	compactPeriod := 2.01
	s.SetBasePeriod(dfBasePeriod)
	s.LastRotationModel = &RotationModel{
		DominantPeriodS: &compactPeriod,
		Family: &ICAOFamily{
			FoldedICAOs:   map[uint32]struct{}{0xAAAAAA: {}},
			ResidualICAOs: map[uint32]struct{}{0xBBBBBB: {}},
		},
	}

	if family := s.FamilySnapshot(); family != nil {
		t.Fatalf("family snapshot exposed disagreeing Go family: %+v", family)
	}
}

func TestReinforceReportsDFPeriodDisagreementStatus(t *testing.T) {
	s := NewIIDState(15)
	dfBasePeriod := 4.79
	compactPeriod := 2.01
	s.SetBasePeriod(dfBasePeriod)
	reinforce(s, &RotationModel{
		DominantPeriodS:    &compactPeriod,
		Status:             "SINGLE_RADAR",
		PrimaryDirectCount: 6,
	})

	if s.Status != "DF_PERIOD_DISAGREE" {
		t.Fatalf("status=%q, want DF_PERIOD_DISAGREE", s.Status)
	}
	if s.PeriodAgreesWithDF {
		t.Fatal("expected period disagreement")
	}
	if s.PeriodRejectReason != "compact_period_disagrees_with_df" {
		t.Fatalf("period reject reason=%q", s.PeriodRejectReason)
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
