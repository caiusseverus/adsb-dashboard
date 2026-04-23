package iid

import (
	"math"
	"testing"
	"time"
)

// buildObs creates a synthetic MultiSyncObs for a given burst timestamp and bearing.
func buildObs(centroidUS float64, bearingDeg float64, icao uint32, wallTS float64) MultiSyncObs {
	posAge := float32(0.5)
	sig := float32(-30.0)
	return MultiSyncObs{
		CentroidUS: centroidUS,
		ICAO:       icao,
		BearingDeg: bearingDeg,
		RangeNM:    50.0,
		PosAgeS:    posAge,
		NReplies:   8,
		SignalDBFS:  &sig,
		WallTS:     wallTS,
	}
}

// simulatedBearing computes the expected bearing for a burst at centroidUS using
// a simple sync model (no waveform, no propagation).
func simulatedBearing(centroidUS, epochUS, offsetDeg, periodS float64) float64 {
	periodUS := periodS * 1e6
	phase := math.Mod((centroidUS-epochUS)/periodUS*360.0, 360.0)
	if phase < 0 {
		phase += 360.0
	}
	return math.Mod(phase+offsetDeg, 360.0)
}

func TestMultiSyncSolver_BasicFit(t *testing.T) {
	ms := NewMultiSyncSolver(1)

	// Synthetic radar: period 4.0s, phase epoch 0, offset 45°.
	periodS := 4.0
	epochUS := 0.0
	offsetDeg := 45.0

	// Seed a compact sync state for the solver to use as a starting point.
	sync := NewSyncState(1, periodS, epochUS, offsetDeg, 0.8)

	// Generate 30 observations spanning 3 rotations from 3 aircraft.
	now := float64(time.Now().UnixMicro()) / 1e6
	startUS := epochUS + 1e6 // 1 second in
	for i := 0; i < 30; i++ {
		us := startUS + float64(i)*periodS/10.0*1e6
		bearing := simulatedBearing(us, epochUS, offsetDeg, periodS)
		// Rotate through 3 ICAOs.
		icao := uint32(0xABC001 + i%3)
		wallTS := now - 60.0 + float64(i)*2.0
		ms.obs = append(ms.obs, buildObs(us, bearing, icao, wallTS))
	}

	// Force a solver run by setting lastRunTS to 0.
	ms.lastRunTS = 0.0
	ran := ms.TryUpdate(sync)
	if !ran {
		t.Fatal("expected TryUpdate to run the solver")
	}
	if !ms.Present {
		t.Error("expected Present=true after successful fit")
	}
	if !ms.Usable {
		t.Error("expected Usable=true with clean observations")
	}
	if math.Abs(ms.PeriodS-periodS) > 0.1 {
		t.Errorf("period mismatch: got %.4f, want ~%.4f", ms.PeriodS, periodS)
	}
	// Verify predictor correctness: predictions at the observation timestamps
	// should match the observed bearings (within sync jitter tolerance).
	testUS := startUS + 1.5*periodS*1e6 // mid-way through observations
	predictedBearing := msPredictBearing(ms.PhaseEpochUS, ms.PhaseOffsetDeg, ms.PeriodS*1e6, testUS)
	expectedBearing := simulatedBearing(testUS, epochUS, offsetDeg, periodS)
	bearingErr := math.Abs(math.Mod(predictedBearing-expectedBearing+540.0, 360.0) - 180.0)
	if bearingErr > 10.0 {
		t.Errorf("predictor error %.2f° > 10° at test timestamp (predicted=%.2f expected=%.2f)",
			bearingErr, predictedBearing, expectedBearing)
	}
}

func TestMultiSyncSolver_Throttle(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	sync := NewSyncState(1, 4.0, 0.0, 0.0, 0.8)

	// Add one observation.
	ms.obs = append(ms.obs, buildObs(1e6, 90.0, 0xABC001, float64(time.Now().Unix())))

	// First call: throttle time is 0 so it should run.
	ms.lastRunTS = 0.0
	ran := ms.TryUpdate(sync)
	if !ran {
		t.Error("expected first TryUpdate to run")
	}

	// Second call immediately: should be throttled.
	ran = ms.TryUpdate(sync)
	if ran {
		t.Error("expected second immediate TryUpdate to be throttled")
	}
}

func TestMultiSyncSolver_InsufficientObservations(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	sync := NewSyncState(1, 4.0, 0.0, 0.0, 0.8)

	// Only 2 observations — below multiSyncMinObs (3).
	now := float64(time.Now().Unix())
	ms.obs = append(ms.obs, buildObs(1e6, 90.0, 0xABC001, now))
	ms.obs = append(ms.obs, buildObs(2e6, 180.0, 0xABC002, now))

	ms.lastRunTS = 0.0
	ms.TryUpdate(sync)

	if ms.Present {
		t.Error("expected Present=false with insufficient observations")
	}
	if !ms.Holdover {
		t.Error("expected Holdover=true when obs count is too low")
	}
}

func TestMultiSyncSolver_Reset(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	sync := NewSyncState(1, 4.0, 0.0, 45.0, 0.8)
	now := float64(time.Now().Unix())
	for i := 0; i < 20; i++ {
		ms.obs = append(ms.obs, buildObs(float64(i)*4e5, float64(i*18), 0xABC001, now))
	}
	ms.lastRunTS = 0.0
	ms.TryUpdate(sync)
	if !ms.Present {
		t.Skip("solver did not produce a state — skipping reset assertions")
	}

	ms.Reset()
	if ms.Present {
		t.Error("expected Present=false after reset")
	}
	if len(ms.obs) != 0 {
		t.Errorf("expected empty obs buffer after reset, got %d", len(ms.obs))
	}
	if ms.PeriodS != 0 {
		t.Errorf("expected PeriodS=0 after reset, got %.4f", ms.PeriodS)
	}
	if len(ms.ICAOQuality) != 0 {
		t.Error("expected ICAOQuality cleared after reset")
	}
}

func TestMultiSyncSolver_ObsPrune(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	now := float64(time.Now().Unix())
	// Add old and new observations.
	for i := 0; i < 10; i++ {
		ms.obs = append(ms.obs, buildObs(float64(i)*1e6, float64(i*36), 0xABC001, now-400.0)) // stale
	}
	for i := 0; i < 5; i++ {
		ms.obs = append(ms.obs, buildObs(float64(i)*1e6+1e8, float64(i*36), 0xABC002, now-10.0)) // fresh
	}
	before := len(ms.obs)
	ms.pruneObs()
	after := len(ms.obs)
	if after >= before {
		t.Errorf("expected pruning to remove stale obs (before=%d, after=%d)", before, after)
	}
	if after != 5 {
		t.Errorf("expected 5 fresh obs after prune, got %d", after)
	}
}

func TestMultiSyncSolver_Snapshot(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	sync := NewSyncState(1, 4.0, 0.0, 45.0, 0.8)
	now := float64(time.Now().UnixMicro()) / 1e6
	for i := 0; i < 30; i++ {
		us := float64(i) * 4e5
		bearing := simulatedBearing(us, 0, 45.0, 4.0)
		ms.obs = append(ms.obs, buildObs(us, bearing, uint32(0xABC001+i%3), now-60.0+float64(i)*2.0))
	}
	ms.lastRunTS = 0.0
	ms.TryUpdate(sync)

	snap := ms.Snapshot()
	if snap.Present != ms.Present {
		t.Error("snapshot Present mismatch")
	}
	if snap.PeriodS != ms.PeriodS {
		t.Error("snapshot PeriodS mismatch")
	}
}

func TestWeightedLinearFit_BasicSlope(t *testing.T) {
	xs := []float64{0, 1, 2, 3}
	ys := []float64{1, 3, 5, 7} // y = 1 + 2*x
	ws := []float64{1, 1, 1, 1}
	a, b := weightedLinearFit(xs, ys, ws)
	if math.Abs(a-1.0) > 0.01 {
		t.Errorf("intercept: got %.4f, want 1.0", a)
	}
	if math.Abs(b-2.0) > 0.01 {
		t.Errorf("slope: got %.4f, want 2.0", b)
	}
}

func TestWeightedLinearFit_Empty(t *testing.T) {
	a, b := weightedLinearFit(nil, nil, nil)
	if a != 0 || b != 0 {
		t.Errorf("expected (0,0) for empty input, got (%.4f, %.4f)", a, b)
	}
}

func TestBearingAndRangeNM(t *testing.T) {
	// Receiver at London, aircraft at Paris — rough sanity check.
	bearing, rangeNM := BearingAndRangeNM(51.5, -0.1, 48.8, 2.3)
	if bearing < 100 || bearing > 160 {
		t.Errorf("bearing %.1f not in expected range 100-160 for London→Paris", bearing)
	}
	if rangeNM < 180 || rangeNM > 260 {
		t.Errorf("range %.1f NM not in expected range 180-260 for London→Paris", rangeNM)
	}
}

// ─── bootstrap / trust / reacquire tests ─────────────────────────────────────

// TestMultiSyncSolver_BootstrapRecorded verifies that BootstrapPeriodS is set from the
// compact-sync seed on the first run and is never overwritten on subsequent runs.
func TestMultiSyncSolver_BootstrapRecorded(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	sync := NewSyncState(1, 4.0, 0.0, 45.0, 0.8)

	now := float64(time.Now().UnixMicro()) / 1e6
	for i := 0; i < 30; i++ {
		us := float64(i) * 4e5
		bearing := simulatedBearing(us, 0, 45.0, 4.0)
		ms.obs = append(ms.obs, buildObs(us, bearing, uint32(0xABC001+i%3), now-60.0+float64(i)*2.0))
	}
	ms.lastRunTS = 0.0
	ms.TryUpdate(sync)

	if ms.BootstrapPeriodS != 4.0 {
		t.Errorf("expected BootstrapPeriodS=4.0 after first run, got %.4f", ms.BootstrapPeriodS)
	}

	// A second run with a different compact-sync period must not overwrite BootstrapPeriodS.
	sync2 := NewSyncState(1, 5.0, 0.0, 45.0, 0.8)
	ms.lastRunTS = 0.0
	ms.TryUpdate(sync2)
	if ms.BootstrapPeriodS != 4.0 {
		t.Errorf("BootstrapPeriodS must not change on subsequent runs: got %.4f", ms.BootstrapPeriodS)
	}
}

// TestMultiSyncSolver_TrustPromotion verifies that after trustMinStreak consistent
// high-quality updates, TrustedBasePeriodS is promoted from the refined period.
func TestMultiSyncSolver_TrustPromotion(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	periodS := 4.0
	sync := NewSyncState(1, periodS, 0.0, 45.0, 0.8)

	now := float64(time.Now().UnixMicro()) / 1e6

	// Feed many clean observations spread across several rotations and multiple ICAOs.
	// Re-run until TrustedBasePeriodS is promoted.
	promoted := false
	for run := 0; run < trustMinStreak+4; run++ {
		for i := 0; i < 20; i++ {
			us := float64(run*100+i) * periodS / 20.0 * 1e6
			bearing := simulatedBearing(us, 0, 45.0, periodS)
			ms.obs = append(ms.obs, buildObs(us, bearing, uint32(0xABC001+i%4), now+float64(run)*periodS))
		}
		ms.lastRunTS = 0.0
		ms.TryUpdate(sync)
		if ms.TrustedBasePeriodS > 0 {
			promoted = true
			break
		}
	}

	if !promoted {
		t.Errorf("expected TrustedBasePeriodS to be promoted after %d streak updates, got 0", trustMinStreak)
	}
	if math.Abs(ms.TrustedBasePeriodS-periodS) > 0.1 {
		t.Errorf("TrustedBasePeriodS %.4f too far from true period %.4f", ms.TrustedBasePeriodS, periodS)
	}
}

// TestMultiSyncSolver_EscapeWrongSeed verifies that when the solver is in wrong-period
// reacquire with an incorrect bootstrap seed, the candidate search selects a period
// closer to the true period than the bootstrap — not the bootstrap itself.
//
// This tests the reacquire candidate-search mechanism directly without relying on
// organic reacquire triggering (which requires many solver runs with accumulated obs).
func TestMultiSyncSolver_EscapeWrongSeed(t *testing.T) {
	truePeriodS := 4.0
	wrongBootstrapS := 4.1 // 2.5% off — within probe range (±2% probes reach 4.018)
	epochUS := 0.0

	ms := NewMultiSyncSolver(1)
	ms.BootstrapPeriodS = wrongBootstrapS
	ms.PeriodS = wrongBootstrapS
	ms.PhaseEpochUS = epochUS
	ms.PhaseOffsetDeg = 45.0
	ms.Present = true
	// Force into reacquire state as if failure streak fired.
	ms.PeriodReacquireActive = true
	ms.PeriodReacquireReason = "failure_streak"
	ms.PeriodFailureStreak = 3

	// Build scored observations consistent with truePeriodS from multiple ICAOs.
	var scored []scoredObs
	for i := 0; i < 30; i++ {
		us := float64(i) * truePeriodS / 10.0 * 1e6
		bearing := simulatedBearing(us, epochUS, 45.0, truePeriodS)
		sig := float32(-30.0)
		o := MultiSyncObs{
			CentroidUS: us,
			ICAO:       uint32(0xABC001 + i%3),
			BearingDeg: bearing,
			RangeNM:    0,
			PosAgeS:    0.5,
			NReplies:   8,
			SignalDBFS:  &sig,
		}
		se := scoredObs{
			o:           o,
			effectiveUS: us,
			fitEligible: true,
		}
		scored = append(scored, se)
	}

	// Directly invoke the candidate search (the part called during reacquire).
	bestP, bestSc := ms.searchBestCandidate(scored, epochUS)

	// The winning candidate must score better than the wrong bootstrap for true-period obs.
	wrongBootstrapScore := ms.evalCandidatePeriod(scored, wrongBootstrapS, epochUS)
	if bestSc <= wrongBootstrapScore.score {
		t.Errorf("candidate search did not find a better period than bootstrap: bestP=%.4f score=%.2f, bootstrap score=%.2f",
			bestP, bestSc, wrongBootstrapScore.score)
	}
	// The winning period must be meaningfully closer to the true period than the bootstrap.
	if math.Abs(bestP-truePeriodS) >= math.Abs(wrongBootstrapS-truePeriodS) {
		t.Errorf("reacquire candidate (%.4f) is not closer to true period (%.4f) than bootstrap (%.4f)",
			bestP, truePeriodS, wrongBootstrapS)
	}
}

// TestMultiSyncSolver_NoDriftOnWeakEvidence verifies that the solver does not drift
// wildly when observations are sparse or noisy.
func TestMultiSyncSolver_NoDriftOnWeakEvidence(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	periodS := 4.0
	sync := NewSyncState(1, periodS, 0.0, 45.0, 0.8)

	now := float64(time.Now().UnixMicro()) / 1e6
	// Only 4 observations — below the trust thresholds.
	for i := 0; i < 4; i++ {
		us := float64(i) * periodS * 1e6
		ms.obs = append(ms.obs, buildObs(us, simulatedBearing(us, 0, 45.0, periodS),
			uint32(0xABC001+i%2), now))
	}
	ms.lastRunTS = 0.0
	ms.TryUpdate(sync)

	if ms.TrustUpdateStreak > 0 {
		t.Errorf("trust streak should not advance on weak evidence: got %d", ms.TrustUpdateStreak)
	}
	if ms.TrustedBasePeriodS > 0 {
		t.Errorf("TrustedBasePeriodS must not be promoted on weak evidence: got %.4f", ms.TrustedBasePeriodS)
	}
}

// TestMultiSyncSolver_ReacquireCandidateSearch verifies that when wrong-period
// reacquire fires, the candidate search produces a non-zero score and period.
func TestMultiSyncSolver_ReacquireCandidateSearch(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	periodS := 4.0
	wrongSeedS := 6.0
	sync := NewSyncState(1, wrongSeedS, 0.0, 45.0, 0.8)

	now := float64(time.Now().UnixMicro()) / 1e6
	// Build observations consistent with periodS.  Many rejected against wrongSeedS
	// to trigger wrong-period detection.
	for i := 0; i < 40; i++ {
		us := float64(i) * periodS / 10.0 * 1e6
		bearing := simulatedBearing(us, 0, 45.0, periodS)
		ms.obs = append(ms.obs, buildObs(us, bearing, uint32(0xABC001+i%3), now+float64(i)*0.4))
	}
	// Force the solver into reacquire by presetting failure streak.
	ms.PeriodFailureStreak = 3
	ms.PeriodReacquireActive = true
	ms.PeriodReacquireReason = "failure_streak"
	// Also preset bootstrap so the candidate set includes the wrong seed.
	ms.BootstrapPeriodS = wrongSeedS

	ms.lastRunTS = 0.0
	ms.TryUpdate(sync)

	// Candidate search must have run and produced a non-zero result.
	if ms.LastReacquireCandidateP <= 0 {
		t.Error("expected reacquire candidate period > 0 after search")
	}
	if ms.LastReacquireCandidateSc < 0 {
		t.Error("expected non-negative reacquire candidate score")
	}
}

// TestMultiSyncSolver_TrustResetOnReacquire verifies that TrustUpdateStreak is reset
// when wrong-period suspicion is raised, preventing premature trust promotion.
//
// Note: 90° wrong bearings are used so residuals are classified as "rejected" (absR>35°)
// but not "near_wrap" (absR<150°); 180° wrong bearings would trigger the near_wrap_residual
// guard which causes an early return before the trust-reset logic is reached.
func TestMultiSyncSolver_TrustResetOnReacquire(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	ms.TrustUpdateStreak = trustMinStreak - 1 // almost promoted
	ms.BootstrapPeriodS = 4.0
	ms.PeriodS = 4.0
	ms.Present = true
	ms.PhaseEpochUS = 0.0
	ms.PhaseOffsetDeg = 45.0

	sync := NewSyncState(1, 4.0, 0.0, 45.0, 0.8)
	now := float64(time.Now().UnixMicro()) / 1e6

	// Feed mostly-rejected observations.
	// 90° off gives absR≈90° → "rejected" by residual gate, NOT near_wrap.
	// This lets the solver reach the reacquire block without an early return.
	for i := 0; i < 20; i++ {
		us := float64(i) * 1e6
		bearing := math.Mod(simulatedBearing(us, 0, 45.0, 4.0)+90.0, 360.0)
		ms.obs = append(ms.obs, buildObs(us, bearing, uint32(0xABC001+i%2), now+float64(i)*0.5))
	}
	// Pre-set failure streak so reacquire entry is triggered (PeriodFailureStreak++ makes it ≥3).
	ms.PeriodFailureStreak = 2
	ms.lastRunTS = 0.0
	ms.TryUpdate(sync)

	// After reacquire fires, TrustUpdateStreak must have been reset.
	if ms.TrustUpdateStreak != 0 {
		t.Errorf("trust streak should be 0 after reacquire entry, got %d", ms.TrustUpdateStreak)
	}
}

// TestMultiSyncSolver_GoodSeedConverges verifies no regression: a correct bootstrap
// seed produces a usable state and promotes to trusted base after enough clean updates.
func TestMultiSyncSolver_GoodSeedConverges(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	periodS := 4.0
	sync := NewSyncState(1, periodS, 0.0, 45.0, 0.8)

	now := float64(time.Now().UnixMicro()) / 1e6
	// Feed sufficient clean observations for trust promotion.
	for run := 0; run <= trustMinStreak+2; run++ {
		for i := 0; i < 12; i++ {
			us := float64(run*60+i) * periodS / 12.0 * 1e6
			bearing := simulatedBearing(us, 0, 45.0, periodS)
			ms.obs = append(ms.obs, buildObs(us, bearing, uint32(0xABC001+i%3), now+float64(run)*periodS))
		}
		ms.lastRunTS = 0.0
		ms.TryUpdate(sync)
	}

	if !ms.Present || !ms.Usable {
		t.Errorf("expected Present=true Usable=true with good seed, got Present=%v Usable=%v", ms.Present, ms.Usable)
	}
	if ms.BootstrapPeriodS != periodS {
		t.Errorf("expected BootstrapPeriodS=%.2f, got %.4f", periodS, ms.BootstrapPeriodS)
	}
	// Period must have stayed near the true value.
	if math.Abs(ms.PeriodS-periodS) > 0.2 {
		t.Errorf("period drifted from good seed: got %.4f, want ~%.4f", ms.PeriodS, periodS)
	}
}

// TestMultiSyncSolver_DiagnosticsExposedInSnapshot verifies that the new diagnostic
// fields round-trip correctly through Snapshot().
func TestMultiSyncSolver_DiagnosticsExposedInSnapshot(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	ms.BootstrapPeriodS = 4.0
	ms.TrustedBasePeriodS = 4.001
	ms.TrustUpdateStreak = 5
	ms.LastBaseClamped = true
	ms.LastBaseClampDiffPPM = 1234.5
	ms.LastWrongPeriodSuspect = true
	ms.LastReacquireCandidateP = 3.99
	ms.LastReacquireCandidateSc = 7.5

	snap := ms.Snapshot()
	if snap.BootstrapPeriodS != 4.0 {
		t.Errorf("BootstrapPeriodS: got %.4f want 4.0", snap.BootstrapPeriodS)
	}
	if snap.TrustedBasePeriodS != 4.001 {
		t.Errorf("TrustedBasePeriodS: got %.4f want 4.001", snap.TrustedBasePeriodS)
	}
	if snap.TrustUpdateStreak != 5 {
		t.Errorf("TrustUpdateStreak: got %d want 5", snap.TrustUpdateStreak)
	}
	if !snap.BaseClamped {
		t.Error("BaseClamped: expected true")
	}
	if snap.BaseClampDiffPPM != 1234.5 {
		t.Errorf("BaseClampDiffPPM: got %.1f want 1234.5", snap.BaseClampDiffPPM)
	}
	if !snap.WrongPeriodSuspect {
		t.Error("WrongPeriodSuspect: expected true")
	}
	if snap.ReacquireCandidatePeriod != 3.99 {
		t.Errorf("ReacquireCandidatePeriod: got %.4f want 3.99", snap.ReacquireCandidatePeriod)
	}
	if snap.ReacquireCandidateScore != 7.5 {
		t.Errorf("ReacquireCandidateScore: got %.1f want 7.5", snap.ReacquireCandidateScore)
	}
}

// TestEvalCandidatePeriod_CorrectPeriodScoresHigher verifies that the candidate
// evaluation function scores the true period higher than a wrong period.
func TestEvalCandidatePeriod_CorrectPeriodScoresHigher(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	truePeriodS := 4.0
	epochUS := 0.0

	// Build scored observations consistent with the true period.
	var scored []scoredObs
	for i := 0; i < 30; i++ {
		us := float64(i) * truePeriodS / 10.0 * 1e6
		bearing := simulatedBearing(us, epochUS, 45.0, truePeriodS)
		sig := float32(-30.0)
		o := MultiSyncObs{
			CentroidUS: us,
			ICAO:       uint32(0xABC001 + i%3),
			BearingDeg: bearing,
			RangeNM:    0, // no propagation correction in this test
			PosAgeS:    0.5,
			NReplies:   8,
			SignalDBFS:  &sig,
		}
		se := scoredObs{
			o:           o,
			effectiveUS: us,
			fitEligible: true,
		}
		scored = append(scored, se)
	}

	correctResult := ms.evalCandidatePeriod(scored, truePeriodS, epochUS)
	wrongResult := ms.evalCandidatePeriod(scored, 6.0, epochUS)

	if correctResult.score <= wrongResult.score {
		t.Errorf("correct period (%.1f) scored %.2f <= wrong period (6.0) scored %.2f",
			truePeriodS, correctResult.score, wrongResult.score)
	}
	if correctResult.inlierCount < 20 {
		t.Errorf("expected most obs to be inliers for correct period, got %d", correctResult.inlierCount)
	}
}

func TestMultiSyncSolver_ICAOQualityDownweights(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	// Mark one ICAO as noisy.
	ms.ICAOQuality[0xBEEF01] = &ICAOSyncQuality{
		ResidualMADDeg: 25.0, // above icaoQualityWarnMAD but below reject
	}
	q := ms.ICAOQuality[0xBEEF01]
	mult := msICAOQualityMult(q)
	if mult >= 1.0 {
		t.Errorf("expected downweighted multiplier for noisy ICAO, got %.4f", mult)
	}
	// A clean ICAO should get multiplier = 1.0.
	mult2 := msICAOQualityMult(nil)
	if mult2 != 1.0 {
		t.Errorf("expected multiplier=1.0 for nil quality, got %.4f", mult2)
	}
}
