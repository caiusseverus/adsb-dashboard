package iid

import (
	"math"
	"testing"
)

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

func TestDFBasePeriodAliasImmunityDuringSyncRefinement(t *testing.T) {
	s := NewIIDState(18)
	compactAlias := 2.0133
	base := 4.7906
	s.PeriodS = &compactAlias
	s.SetBasePeriod(base)
	s.Status = "SINGLE_RADAR"

	s.UpdateSyncEpoch(0.0, 0.0, 4, 0.5)
	for i := 1; i <= 14; i++ {
		acceptedOffset := float64(i) * 2.0
		s.UpdateSyncEpoch(float64(i)*base*1e6, acceptedOffset, 4, 0.5)
	}
	snap := s.DebugStateSnapshot()
	if math.Abs(snap.EffectivePeriodS-base) > base*0.005+1e-9 {
		t.Fatalf("effective period=%.6f not bounded near base %.6f", snap.EffectivePeriodS, base)
	}
	if math.Abs(snap.EffectivePeriodS-compactAlias) < 0.5 {
		t.Fatalf("effective period moved toward alias %.4f: got %.6f", compactAlias, snap.EffectivePeriodS)
	}
}

func TestSetBasePeriod_MaterialChangeResetsRefinementDeltaAndHistory(t *testing.T) {
	s := NewIIDState(19)
	base := 4.0
	s.SetBasePeriod(base)
	s.Status = "SINGLE_RADAR"
	s.UpdateSyncEpoch(0.0, 0.0, 4, 0.5)
	if s.Sync == nil {
		t.Fatal("expected sync state")
	}

	// Build a non-zero refinement delta and residual history.
	for i := 1; i <= 12; i++ {
		s.UpdateSyncEpoch(float64(i)*4_000_000.0, float64(i)*0.8, 4, 0.5)
	}
	if s.Sync.PeriodDeltaS == 0 {
		t.Fatal("expected non-zero refinement delta before base change")
	}
	if len(s.Sync.residualHistory) == 0 {
		t.Fatal("expected residual history before base change")
	}

	newBase := 4.2 // >0.1% change
	s.SetBasePeriod(newBase)
	if s.Sync.PeriodDeltaS != 0 {
		t.Fatalf("delta=%.9f, want 0 after material base change", s.Sync.PeriodDeltaS)
	}
	if len(s.Sync.residualHistory) != 0 {
		t.Fatalf("residual history len=%d, want 0 after reset", len(s.Sync.residualHistory))
	}
	if s.Sync.EffectivePeriodS != newBase {
		t.Fatalf("effective=%.6f, want new base %.6f", s.Sync.EffectivePeriodS, newBase)
	}
	if s.Sync.PeriodSource != "df_alignment" {
		t.Fatalf("period source=%q, want df_alignment", s.Sync.PeriodSource)
	}
	if s.Sync.PeriodRefinementStatus != "base_period_changed_reset" {
		t.Fatalf("refinement status=%q, want base_period_changed_reset", s.Sync.PeriodRefinementStatus)
	}
}

func TestSetBasePeriod_SmallChangePreservesRefinementDeltaAndHistory(t *testing.T) {
	s := NewIIDState(20)
	base := 4.0
	s.SetBasePeriod(base)
	s.Status = "SINGLE_RADAR"
	s.UpdateSyncEpoch(0.0, 0.0, 4, 0.5)
	if s.Sync == nil {
		t.Fatal("expected sync state")
	}

	for i := 1; i <= 12; i++ {
		s.UpdateSyncEpoch(float64(i)*4_000_000.0, float64(i)*0.8, 4, 0.5)
	}
	if s.Sync.PeriodDeltaS == 0 {
		t.Fatal("expected non-zero refinement delta before base change")
	}
	prevDelta := s.Sync.PeriodDeltaS
	prevHistLen := len(s.Sync.residualHistory)

	// 0.2% change (<0.5% reset threshold): preserve refinement.
	newBase := 4.008
	s.SetBasePeriod(newBase)
	if s.Sync.PeriodDeltaS == 0 {
		t.Fatal("delta was reset on small base change")
	}
	if math.Abs(s.Sync.PeriodDeltaS-prevDelta) > 1e-9 {
		t.Fatalf("delta changed unexpectedly: got %.9f want %.9f", s.Sync.PeriodDeltaS, prevDelta)
	}
	if len(s.Sync.residualHistory) != prevHistLen {
		t.Fatalf("residual history len changed: got %d want %d", len(s.Sync.residualHistory), prevHistLen)
	}
}

func TestPeriodRefinement_MultiAircraftResidualStreamDrivesDelta(t *testing.T) {
	s := NewIIDState(21)
	base := 4.0
	s.SetBasePeriod(base)
	s.Sync = NewSyncState(s.IID, base, 0.0, 0.0, 1.0)
	ref := uint32(0xAAAAAA)
	s.RefICAO = &ref

	for i := 1; i <= 30; i++ {
		epochUS := float64(i) * 1_000_000.0
		predicted := s.Sync.PredictBearing(epochUS)
		observedBearingDeg := wrap360(predicted + float64(i)*0.2)
		icao := uint32(0x100000 + i%5)
		s.RecordBurstResidualObservation(epochUS, icao, observedBearingDeg, 6, 0.5, true)
	}
	if s.PeriodDeltaS == 0 {
		t.Fatal("expected non-zero period delta from multi-aircraft residual slope")
	}
	if s.PeriodSource != "df_alignment_plus_residual_slope" {
		t.Fatalf("period source=%q, want df_alignment_plus_residual_slope", s.PeriodSource)
	}
}

func TestPeriodRefinement_NonReferenceResidualsDriveDelta(t *testing.T) {
	s := NewIIDState(22)
	base := 4.0
	s.SetBasePeriod(base)
	s.Sync = NewSyncState(s.IID, base, 0.0, 0.0, 1.0)
	ref := uint32(0xAAAAAA)
	s.RefICAO = &ref

	for i := 1; i <= 30; i++ {
		epochUS := float64(i) * 1_000_000.0
		predicted := s.Sync.PredictBearing(epochUS)
		observedBearingDeg := wrap360(predicted + float64(i)*0.2)
		icao := uint32(0xBBBB00 + i%7) // all non-reference
		s.RecordBurstResidualObservation(epochUS, icao, observedBearingDeg, 5, 0.5, true)
	}
	if s.PeriodDeltaS == 0 {
		t.Fatal("expected non-zero period delta from non-reference residuals")
	}
}

func TestPeriodRefinement_RejectedOutliersDoNotDriveDelta(t *testing.T) {
	s := NewIIDState(23)
	base := 4.0
	s.SetBasePeriod(base)
	s.Sync = NewSyncState(s.IID, base, 0.0, 0.0, 1.0)
	ref := uint32(0xAAAAAA)
	s.RefICAO = &ref

	for i := 1; i <= 20; i++ {
		epochUS := float64(i) * 1_000_000.0
		predicted := s.Sync.PredictBearing(epochUS)
		observedBearingDeg := wrap360(predicted + 100.0) // guaranteed hard outlier residual
		icao := uint32(0xCC0000 + i%3)
		s.RecordBurstResidualObservation(epochUS, icao, observedBearingDeg, 5, 0.5, true)
	}
	if s.PeriodDeltaS != 0 {
		t.Fatalf("expected zero period delta from rejected-only stream, got %.9f", s.PeriodDeltaS)
	}
	if s.Sync.RefinementEligibleCount != 0 {
		t.Fatalf("eligible count=%d, want 0", s.Sync.RefinementEligibleCount)
	}
	if s.Sync.RefinementRejectedCount == 0 {
		t.Fatal("expected rejected refinement observations")
	}
}

func TestPeriodRefinement_ReferenceUpdatesNotOnlyPath(t *testing.T) {
	s := NewIIDState(24)
	base := 4.0
	s.SetBasePeriod(base)
	s.Sync = NewSyncState(s.IID, base, 0.0, 0.0, 1.0)
	ref := uint32(0xAAAAAA)
	s.RefICAO = &ref

	// Reference updates alone with zero residual should not create refinement.
	for i := 1; i <= 10; i++ {
		s.UpdateSyncEpoch(float64(i)*4_000_000.0, 0.0, 6, 0.5)
	}
	if s.PeriodDeltaS != 0 {
		t.Fatalf("reference-only path unexpectedly refined period: %.9f", s.PeriodDeltaS)
	}

	// Add non-reference residual slope; now refinement should engage.
	for i := 11; i <= 30; i++ {
		epochUS := float64(i) * 1_000_000.0
		predicted := s.Sync.PredictBearing(epochUS)
		observedBearingDeg := wrap360(predicted + float64(i-10)*0.2)
		icao := uint32(0xDD0000 + i%5)
		s.RecordBurstResidualObservation(epochUS, icao, observedBearingDeg, 6, 0.5, true)
	}
	if s.PeriodDeltaS == 0 {
		t.Fatal("expected refinement after non-reference eligible residual stream")
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
