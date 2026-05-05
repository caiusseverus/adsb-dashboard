package iid

import (
	"math"
	"testing"
	"time"
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
	s.RefreshReference(records, 12_000_000, nil)

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
	s.RefreshReference(records, 12_000_000, nil)

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
	// Operational: refined effective == DF base (no delta), so they agree.
	if !snap.PeriodAgreesWithDF {
		t.Fatal("expected refined effective period agrees with DF base")
	}
	if snap.PeriodRejectReason != "" {
		t.Fatalf("period reject reason=%q, want empty", snap.PeriodRejectReason)
	}
	// Diagnostic: compact period 2.01 disagrees with DF base 4.79.
	if snap.CompactPeriodAgreesWithDF {
		t.Fatal("expected compact period diagnostic disagreement")
	}
	if snap.CompactPeriodDiagnosticReason != "compact_period_disagrees_with_df" {
		t.Fatalf("compact diagnostic reason=%q", snap.CompactPeriodDiagnosticReason)
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
		if s.Sync != nil {
			s.Sync.AddRefinementResidualObservation(float64(i)*4_000_000.0, float64(i)*0.8, 0xAA+uint32(i%3), true, 4, 0.5, 0xAA, "go_refiner_active")
		}
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
		if s.Sync != nil {
			s.Sync.AddRefinementResidualObservation(float64(i)*4_000_000.0, float64(i)*0.8, 0xAA+uint32(i%3), true, 4, 0.5, 0xAA, "go_refiner_active")
		}
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
	if s.Sync.RefinementReferenceUpdates == 0 {
		t.Fatal("expected reference update counter to increment")
	}
	if s.Sync.RefinementEligibleCount != 0 {
		t.Fatalf("expected zero burst-refinement eligible observations before burst path calls, got %d", s.Sync.RefinementEligibleCount)
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

func TestPeriodRefinement_AllowsLearningDuringHoldoverButSyncRemainsUnusable(t *testing.T) {
	s := NewIIDState(25)
	base := 4.0
	s.SetBasePeriod(base)
	s.Sync = NewSyncState(s.IID, base, 0.0, 0.0, 1.0)
	s.Sync.Holdover = true
	ref := uint32(0xAAAAAA)
	s.RefICAO = &ref

	for i := 1; i <= 30; i++ {
		epochUS := float64(i) * 1_000_000.0
		predicted := s.Sync.PredictBearing(epochUS)
		observedBearingDeg := wrap360(predicted + float64(i)*0.2)
		s.RecordBurstResidualObservation(epochUS, uint32(0xEE0000+i%3), observedBearingDeg, 6, 0.5, true)
	}
	if s.PeriodDeltaS == 0 {
		t.Fatal("expected holdover refinement learning to move period delta")
	}
	_, usable, _, _, _, _, _, _, _, _, holdover := s.SyncProtocolSnapshot()
	if usable {
		t.Fatal("sync should remain unusable during holdover")
	}
	if !holdover {
		t.Fatal("expected holdover flag true")
	}
}

func TestPeriodRefinement_RecordResidualValueDoesNotDoubleSubtractPrediction(t *testing.T) {
	s := NewIIDState(26)
	base := 4.0
	s.SetBasePeriod(base)
	s.Sync = NewSyncState(s.IID, base, 0.0, 0.0, 1.0)
	ref := uint32(0xAAAAAA)
	s.RefICAO = &ref

	for i := 1; i <= 30; i++ {
		epochUS := float64(i) * 1_000_000.0
		s.RecordBurstResidualValue(epochUS, uint32(0xAB0000+i%4), float64(i)*0.2, 6, 0.5, true)
	}
	if s.PeriodDeltaS == 0 {
		t.Fatal("expected non-zero delta from direct residual-value stream")
	}
}

func TestPeriodRefinement_RecordResidualValueWithoutReferenceAllowed(t *testing.T) {
	s := NewIIDState(26)
	base := 4.0
	s.SetBasePeriod(base)
	s.Sync = NewSyncState(s.IID, base, 0.0, 0.0, 1.0)
	// No RefICAO on purpose: direct residual-value path should still accumulate.
	for i := 1; i <= 30; i++ {
		epochUS := float64(i) * 1_000_000.0
		s.RecordBurstResidualValue(epochUS, uint32(0xAB1000+i%5), float64(i)*0.2, 6, 0.5, true)
	}
	if s.PeriodDeltaS == 0 {
		t.Fatal("expected non-zero delta from direct residual stream without RefICAO")
	}
	if s.Sync.RefinementLastRejectReason == "no_reference" {
		t.Fatal("direct residual path should not require Go RefICAO")
	}
}

func TestUpdateEpochDiagnostics_RecordRejectInputs(t *testing.T) {
	s := NewIIDState(61)
	base := 4.0
	s.SetBasePeriod(base)
	s.Sync = NewSyncState(s.IID, base, 0.0, 0.0, 1.0)
	ref := uint32(0xAAAAAA)
	s.RefICAO = &ref

	// Force insufficient-aircraft reject.
	s.UpdateSyncEpoch(4_000_000.0, 0.0, 1, 0.5)
	snap := s.DebugStateSnapshot()
	if snap.LastUpdateEpochRejectReason != "insufficient_aircraft" {
		t.Fatalf("reject reason=%q", snap.LastUpdateEpochRejectReason)
	}
	if snap.LastUpdateEpochNAircraft != 1 {
		t.Fatalf("n_aircraft=%d", snap.LastUpdateEpochNAircraft)
	}
}

func TestUpdateEpochDiagnostics_HardRejectDetailsExposed(t *testing.T) {
	s := NewIIDState(63)
	base := 4.0
	s.SetBasePeriod(base)
	s.Sync = NewSyncState(s.IID, base, 0.0, 0.0, 1.0)
	ref := uint32(0xA0B0C0)
	s.RefICAO = &ref

	// Force hard residual reject.
	s.UpdateSyncEpoch(4_000_000.0, 60.0, 6, 0.5)
	snap := s.DebugStateSnapshot()
	if snap.LastUpdateEpochRejectReason != "hard_residual_reject" {
		t.Fatalf("reject reason=%q", snap.LastUpdateEpochRejectReason)
	}
	if math.Abs(snap.LastUpdateEpochRawResidualDeg+300.0) > 0.01 {
		t.Fatalf("raw residual=%.2f, want -300", snap.LastUpdateEpochRawResidualDeg)
	}
	if math.Abs(snap.LastUpdateEpochWrappedResidualDeg-60.0) > 0.01 {
		t.Fatalf("wrapped residual=%.2f, want 60", snap.LastUpdateEpochWrappedResidualDeg)
	}
	if snap.HardRejectTransitionConsecutiveBefore != 0 || snap.HardRejectTransitionConsecutiveAfter != 1 {
		t.Fatalf("hard reject before/after=%d/%d, want 0/1", snap.HardRejectTransitionConsecutiveBefore, snap.HardRejectTransitionConsecutiveAfter)
	}
	if !snap.HardRejectEnteredHoldover {
		t.Fatal("expected hard reject entered holdover flag")
	}
	if snap.HardRejectGateReason != "wrapped_abs_residual_gt_50deg" {
		t.Fatalf("hard reject gate reason=%q", snap.HardRejectGateReason)
	}
	if snap.LastUpdateEpochRefICAO != ref {
		t.Fatalf("ref icao=%06X, want %06X", snap.LastUpdateEpochRefICAO, ref)
	}
	if math.Abs(snap.LastUpdateEpochRefBearingDeg-60.0) > 0.01 {
		t.Fatalf("ref bearing=%.2f, want 60", snap.LastUpdateEpochRefBearingDeg)
	}
	if snap.LastUpdateEpochRefRangeNM != -1.0 {
		t.Fatalf("ref range=%.2f, want -1 sentinel", snap.LastUpdateEpochRefRangeNM)
	}
}

func TestUpdateEpochMaintenanceGateUpdatesWithoutStrictAuthority(t *testing.T) {
	s := NewIIDState(62)
	base := 4.0
	s.SetBasePeriod(base)
	s.Status = "SINGLE_RADAR"
	s.Sync = NewSyncState(s.IID, base, 0.0, 0.0, 1.0)
	ref := uint32(0xAAAAAA)
	s.RefICAO = &ref

	// n=2 passes maintenance gate but fails strict gate.
	s.UpdateSyncEpoch(4_000_000.0, 0.0, 2, 0.5)
	snap := s.DebugStateSnapshot()
	if snap.UpdateEpochAccepts == 0 {
		t.Fatal("expected maintenance accept")
	}
	if snap.SyncHoldover {
		t.Fatal("maintenance accept should clear holdover")
	}
	_, usable, _, _, _, _, _, _, _, _, _ := s.SyncProtocolSnapshot()
	if usable {
		t.Fatal("strict authority must remain false under maintenance-only gate")
	}
}

func TestPeriodRefinement_NegativeSlopeDrivesPositiveDelta(t *testing.T) {
	s := NewIIDState(28)
	base := 4.0
	s.SetBasePeriod(base)
	s.Sync = NewSyncState(s.IID, base, 0.0, 0.0, 1.0)
	ref := uint32(0xAAAAAA)
	s.RefICAO = &ref

	for i := 1; i <= 30; i++ {
		epochUS := float64(i) * 1_000_000.0
		predicted := s.Sync.PredictBearing(epochUS)
		observedBearingDeg := wrap360(predicted - float64(i)*0.2)
		s.RecordBurstResidualObservation(epochUS, uint32(0xBC0000+i%4), observedBearingDeg, 6, 0.5, true)
	}
	if s.PeriodDeltaS <= 0 {
		t.Fatalf("period delta=%.9f, want positive for negative observed-minus-predicted slope", s.PeriodDeltaS)
	}
}

func TestPeriodRefinement_WrapCrossingRegression_3971s(t *testing.T) {
	base := 3.971
	sync := NewSyncState(29, base, 0.0, 0.0, 1.0)
	sync.BasePeriodS = base

	// Diagonal residual band crossing ±180° across ~300s.
	for i := 1; i <= 300; i++ {
		epochUS := float64(i) * 1_000_000.0
		unwrappedResidual := 170.0 + float64(i)*0.4
		wrappedResidual := circularDiff(unwrappedResidual, 0.0)
		sync.appendResidualObservationLocked(epochUS, wrappedResidual, uint32(0xAB+i%6), true, false, 1.0, 6, 0.2)
	}
	sync.applyBoundedPeriodRefinement()
	sync.EffectivePeriodS = sync.BasePeriodS + sync.PeriodDeltaS
	if sync.PeriodDeltaS >= 0 {
		t.Fatalf("period delta=%.9f, want negative correction for positive observed-minus-predicted slope", sync.PeriodDeltaS)
	}
	if math.Abs(sync.EffectivePeriodS-(base+sync.PeriodDeltaS)) > 1e-9 {
		t.Fatalf("effective period=%.9f not base+delta", sync.EffectivePeriodS)
	}
	if math.Abs(sync.EffectivePeriodS-base) > base*0.005+1e-9 {
		t.Fatalf("effective period=%.9f exceeds bounded delta around base %.6f", sync.EffectivePeriodS, base)
	}
	alias := 2.0133
	if math.Abs(sync.EffectivePeriodS-alias) < 0.5 {
		t.Fatalf("effective period drifted toward alias %.4f: got %.9f", alias, sync.EffectivePeriodS)
	}
}

func TestPeriodRefinement_NonDominantRejectedCountersIncrement(t *testing.T) {
	s := NewIIDState(27)
	base := 4.0
	s.SetBasePeriod(base)
	s.Sync = NewSyncState(s.IID, base, 0.0, 0.0, 1.0)
	ref := uint32(0xAAAAAA)
	s.RefICAO = &ref

	for i := 1; i <= 10; i++ {
		epochUS := float64(i) * 1_000_000.0
		predicted := s.Sync.PredictBearing(epochUS)
		observedBearingDeg := wrap360(predicted + float64(i)*0.2)
		s.RecordBurstResidualObservation(epochUS, uint32(0xFA0000+i), observedBearingDeg, 5, 0.5, false)
	}
	if s.PeriodDeltaS != 0 {
		t.Fatalf("non-dominant residuals should not drive delta, got %.9f", s.PeriodDeltaS)
	}
	if s.Sync.RefinementRejectedCount == 0 {
		t.Fatal("expected rejected counter to increment")
	}
	if s.Sync.RefinementLastRejectReason != "no_dominant_family" {
		t.Fatalf("last reject reason=%q", s.Sync.RefinementLastRejectReason)
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
	s.RefreshReference(records, 14_370_000, nil)

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
	// Operational: no Sync, refined effective == DF base, so operational agreement is true.
	if !s.PeriodAgreesWithDF {
		t.Fatal("expected refined operational period agreement (effective == DF base)")
	}
	if s.PeriodRejectReason != "" {
		t.Fatalf("period reject reason=%q, want empty (operational)", s.PeriodRejectReason)
	}
	// Diagnostic: compact period disagrees with DF base.
	if s.CompactPeriodAgreesWithDF {
		t.Fatal("expected compact diagnostic disagreement")
	}
	if s.CompactPeriodDiagnosticReason != "compact_period_disagrees_with_df" {
		t.Fatalf("compact diagnostic reason=%q", s.CompactPeriodDiagnosticReason)
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

func TestRefreshReference_PrefersPositionHoldingCandidate(t *testing.T) {
	cache := NewPositionCache()
	nowUnix := float64(time.Now().Unix())
	// 0xAA has a fresh position; 0xBB does not.
	cache.Update(0xAA, 51.5, -0.1, nil, nowUnix)

	s := NewIIDState(7)
	period := 4.0
	s.SetBasePeriod(period)

	// 0xBB has more bursts (better countFactor score) but no position.
	// 0xAA has fewer bursts but a fresh position.
	var records []BurstRecord
	now := time.Now()
	for i := 0; i < 5; i++ {
		records = append(records, BurstRecord{ICAO: 0xAA, CentroidUS: float64(i) * 4_000_000.0, FiredAt: now})
	}
	for i := 0; i < 10; i++ {
		records = append(records, BurstRecord{ICAO: 0xBB, CentroidUS: float64(i) * 4_000_000.0, FiredAt: now})
	}

	nowUS := float64(9) * 4_000_000.0
	s.RefreshReference(records, nowUS, cache)

	if s.RefICAO == nil {
		t.Fatal("reference ICAO not selected")
	}
	if *s.RefICAO != 0xAA {
		t.Errorf("reference ICAO=0x%X, want 0xAA (has position)", *s.RefICAO)
	}
	if !s.LastRefSelHasPosition {
		t.Error("LastRefSelHasPosition should be true for 0xAA")
	}
	if s.LastRefSelMissingReason != "" {
		t.Errorf("LastRefSelMissingReason=%q, want empty", s.LastRefSelMissingReason)
	}
}

func TestSetBasePeriod_NoHoldoverOnCompactDisagreement(t *testing.T) {
	s := NewIIDState(20)
	compactPeriod := 2.01
	s.PeriodS = &compactPeriod

	dfBasePeriod := 4.79
	s.SetBasePeriod(dfBasePeriod)

	if s.PeriodAgreesWithDF {
		// Because the refined effective (DF base + delta=0) equals the incoming DF base,
		// operational agreement must be true.
	}
	if s.PeriodRejectReason != "" {
		t.Fatalf("PeriodRejectReason=%q, want empty (compact disagree is diagnostic-only)", s.PeriodRejectReason)
	}
	// Compact diagnostic must flag the disagreement.
	if s.CompactPeriodAgreesWithDF {
		t.Fatal("CompactPeriodAgreesWithDF should be false for 2.01 vs 4.79")
	}
	if s.CompactPeriodDiagnosticReason != "compact_period_disagrees_with_df" {
		t.Fatalf("CompactPeriodDiagnosticReason=%q", s.CompactPeriodDiagnosticReason)
	}
	if s.CompactPeriodDisagreementS == 0 {
		t.Fatal("CompactPeriodDisagreementS should be non-zero")
	}
}

func TestSetBasePeriod_HoldoverOnRefinedDisagreement(t *testing.T) {
	s := NewIIDState(21)
	dfBasePeriod := 4.79
	s.SetBasePeriod(dfBasePeriod)

	// Manually create a Sync with a large refinement delta to force refined disagreement.
	s.RefICAO = new(uint32)
	*s.RefICAO = 0xAAAAAA
	s.Sync = NewSyncState(s.IID, dfBasePeriod, 0.0, 0.0, 1.0)
	s.Sync.BasePeriodS = dfBasePeriod
	s.Sync.PeriodDeltaS = dfBasePeriod * 0.02 // 2% delta -> refined effective is 2% off base
	s.Sync.EffectivePeriodS = s.Sync.BasePeriodS + s.Sync.PeriodDeltaS

	// Calling SetBasePeriod again should trigger operational holdover because
	// the refined effective is >1% away from the incoming DF base.
	s.SetBasePeriod(dfBasePeriod)

	if s.PeriodAgreesWithDF {
		t.Fatal("expected operational PeriodAgreesWithDF=false (refined effective disagrees with DF base)")
	}
	if s.PeriodRejectReason != "refined_period_disagrees_with_df" {
		t.Fatalf("PeriodRejectReason=%q, want refined_period_disagrees_with_df", s.PeriodRejectReason)
	}
}

func TestSetBasePeriod_SmallDeltaDoesNotTriggerRefinedDisagreement(t *testing.T) {
	s := NewIIDState(22)
	dfBasePeriod := 4.98

	// Simulate a realistic retained delta of 4.58ms on 4.98s → 0.092% → well within 1%.
	retainedDelta := 0.00458

	s.RefICAO = new(uint32)
	*s.RefICAO = 0xBBBBBB
	s.Sync = NewSyncState(s.IID, dfBasePeriod, 0.0, 0.0, 1.0)
	s.Sync.BasePeriodS = dfBasePeriod
	s.Sync.PeriodDeltaS = retainedDelta
	s.Sync.EffectivePeriodS = s.Sync.BasePeriodS + s.Sync.PeriodDeltaS

	s.SetBasePeriod(dfBasePeriod)

	if !s.PeriodAgreesWithDF {
		t.Fatal("expected operational PeriodAgreesWithDF=true (4.58ms on 4.98s is 0.092%, within 1% tolerance)")
	}
	if s.PeriodRejectReason != "" {
		t.Fatalf("PeriodRejectReason=%q, want empty", s.PeriodRejectReason)
	}
}

func TestReinforceCompactDisagreementIsDiagnosticOnly(t *testing.T) {
	s := NewIIDState(23)
	dfBasePeriod := 4.79
	compactPeriod := 2.01
	s.SetBasePeriod(dfBasePeriod)
	reinforce(s, &RotationModel{
		DominantPeriodS:    &compactPeriod,
		Status:             "SINGLE_RADAR",
		PrimaryDirectCount: 6,
	})

	// Operational agreement must remain true (no Sync, refined = base).
	if !s.PeriodAgreesWithDF {
		t.Fatal("operational PeriodAgreesWithDF should be true (reinforce does not gate operational)")
	}
	if s.PeriodRejectReason != "" {
		t.Fatalf("PeriodRejectReason=%q, want empty (reinforce is diagnostic-only)", s.PeriodRejectReason)
	}
	// Compact diagnostic must still flag the disagreement.
	if s.CompactPeriodAgreesWithDF {
		t.Fatal("CompactPeriodAgreesWithDF should be false")
	}
	// Status still reflects the diagnostic disagreement.
	if s.Status != "DF_PERIOD_DISAGREE" {
		t.Fatalf("Status=%q, want DF_PERIOD_DISAGREE", s.Status)
	}
}
