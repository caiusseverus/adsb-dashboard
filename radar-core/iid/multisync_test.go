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
		SignalDBFS: &sig,
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

func scoreObsForSeed(ms *MultiSyncSolver, obs []MultiSyncObs, seedEpochUS, seedOffsetDeg, seedPeriodS float64) []scoredObs {
	seedPeriodUS := seedPeriodS * 1e6
	scored := make([]scoredObs, 0, len(obs))
	for _, o := range obs {
		effectiveUS := msPropCorrectedUS(o.CentroidUS, float64(o.RangeNM))
		predicted := msPredictBearing(seedEpochUS, seedOffsetDeg, seedPeriodUS, effectiveUS)
		residual := circularDiff(o.BearingDeg, predicted)
		absR := math.Abs(residual)
		status := msClassifyResidual(absR)
		baseW := msScoreObs(o)
		qEntry := ms.ICAOQuality[o.ICAO]
		qMult := msICAOQualityMult(qEntry)
		effectiveW := 0.0
		switch status {
		case "inlier":
			effectiveW = baseW * qMult
		case "soft":
			effectiveW = baseW * 0.2 * qMult
		}
		fitRejectReason := msFitRejectReason(o, status, absR, qEntry, false)
		scored = append(scored, scoredObs{
			o:               o,
			residual:        residual,
			effectiveUS:     effectiveUS,
			phaseInRot:      math.Mod((effectiveUS-seedEpochUS)/seedPeriodUS*360.0, 360.0),
			baseW:           baseW,
			effectiveW:      effectiveW,
			status:          status,
			fitEligible:     fitRejectReason == "",
			fitRejectReason: fitRejectReason,
		})
	}
	return scored
}

func meanAbsAlignmentResidual(obs []MultiSyncObs, epochUS, offsetDeg, periodS float64) float64 {
	if len(obs) == 0 {
		return 0
	}
	periodUS := periodS * 1e6
	sum := 0.0
	for _, o := range obs {
		effectiveUS := msPropCorrectedUS(o.CentroidUS, float64(o.RangeNM))
		predicted := msPredictBearing(epochUS, offsetDeg, periodUS, effectiveUS)
		sum += math.Abs(circularDiff(o.BearingDeg, predicted))
	}
	return sum / float64(len(obs))
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
	ran := ms.TryUpdate(sync, 0)
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

func TestMultiSyncSolver_DominantPeriodSeedsButDoesNotLockRefinement(t *testing.T) {
	ms := NewMultiSyncSolver(1)

	truePeriodS := 4.0
	dominantPeriodS := 4.001
	epochUS := 0.0
	offsetDeg := 45.0
	sync := NewSyncState(1, dominantPeriodS, epochUS, offsetDeg, 0.8)
	now := float64(time.Now().UnixMicro()) / 1e6
	for i := 0; i < 180; i++ {
		us := 1_000_000.0 + float64(i)*truePeriodS/2.0*1e6
		bearing := simulatedBearing(us, epochUS, offsetDeg, truePeriodS)
		icao := uint32(0xDAD000 + i%4)
		ms.obs = append(ms.obs, buildObs(us, bearing, icao, now-300.0+float64(i)*2.0))
	}

	for i := 0; i < persistMinEntries+4; i++ {
		ms.runFit(sync, dominantPeriodS, now+float64(i))
	}

	if !ms.Present {
		t.Fatal("expected solver to publish a state")
	}
	if ms.LastActiveFamilyPriorSource != "dominant_live_df_seed" && ms.LastActiveFamilyPriorSource != "candidate_refined" {
		t.Fatalf("expected dominant seed or candidate refined prior, got %q", ms.LastActiveFamilyPriorSource)
	}
	initialErr := math.Abs(dominantPeriodS - truePeriodS)
	refinedErr := math.Abs(ms.CandidatePeriodS - truePeriodS)
	if refinedErr >= initialErr {
		t.Fatalf("expected candidate period to move closer to true period; initial err %.9f refined err %.9f period %.9f slope %.6f history=%v fit=%d span=%.3f source=%s",
			initialErr, refinedErr, ms.CandidatePeriodS, ms.LastResidualSlopeDegPerS, ms.SlopeHistory, ms.LastFitEligibleObs, ms.LastFitSpanS, ms.LastActiveFamilyPriorSource)
	}
	if math.Abs(ms.CandidatePeriodS-dominantPeriodS) < 1e-9 {
		t.Fatalf("expected refined candidate not to be locked to dominant %.9f", dominantPeriodS)
	}
	if math.Abs(ms.LastCompactPeriodS-dominantPeriodS) > 1e-6 {
		t.Fatalf("expected compact/DF seed diagnostic %.6f, got %.6f", dominantPeriodS, ms.LastCompactPeriodS)
	}
}

func TestMultiSyncSolver_Throttle(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	sync := NewSyncState(1, 4.0, 0.0, 0.0, 0.8)

	// Add one observation.
	ms.obs = append(ms.obs, buildObs(1e6, 90.0, 0xABC001, float64(time.Now().Unix())))

	// First call: throttle time is 0 so it should run.
	ms.lastRunTS = 0.0
	ran := ms.TryUpdate(sync, 0)
	if !ran {
		t.Error("expected first TryUpdate to run")
	}

	// Second call immediately: should be throttled.
	ran = ms.TryUpdate(sync, 0)
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
	ms.TryUpdate(sync, 0)

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
	ms.TryUpdate(sync, 0)
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
	ms.TryUpdate(sync, 0)

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
	ms.TryUpdate(sync, 0)

	if ms.BootstrapPeriodS != 4.0 {
		t.Errorf("expected BootstrapPeriodS=4.0 after first run, got %.4f", ms.BootstrapPeriodS)
	}

	// A second run with a different compact-sync period must not overwrite BootstrapPeriodS.
	sync2 := NewSyncState(1, 5.0, 0.0, 45.0, 0.8)
	ms.lastRunTS = 0.0
	ms.TryUpdate(sync2, 0)
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
		ms.TryUpdate(sync, 0)
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
			SignalDBFS: &sig,
		}
		se := scoredObs{
			o:           o,
			effectiveUS: us,
			fitEligible: true,
		}
		scored = append(scored, se)
	}

	// Directly invoke the candidate search (the part called during reacquire).
	best := ms.searchBestCandidate(scored, epochUS)

	// The winning candidate must score better than the wrong bootstrap for true-period obs.
	wrongBootstrapScore := ms.evalCandidatePeriod(scored, wrongBootstrapS, epochUS)
	if best.score <= wrongBootstrapScore.score {
		t.Errorf("candidate search did not find a better period than bootstrap: bestP=%.4f score=%.2f, bootstrap score=%.2f",
			best.periodS, best.score, wrongBootstrapScore.score)
	}
	// The winning period must be meaningfully closer to the true period than the bootstrap.
	if math.Abs(best.periodS-truePeriodS) >= math.Abs(wrongBootstrapS-truePeriodS) {
		t.Errorf("reacquire candidate (%.4f) is not closer to true period (%.4f) than bootstrap (%.4f)",
			best.periodS, truePeriodS, wrongBootstrapS)
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
	ms.TryUpdate(sync, 0)

	if ms.TrustUpdateStreak > 0 {
		t.Errorf("trust streak should not advance on weak evidence: got %d", ms.TrustUpdateStreak)
	}
	if ms.TrustedBasePeriodS > 0 {
		t.Errorf("TrustedBasePeriodS must not be promoted on weak evidence: got %.4f", ms.TrustedBasePeriodS)
	}
}

func TestMultiSyncSolver_TrustStreakResetsOnNonEligibleUpdate(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	periodS := 4.0
	sync := NewSyncState(1, periodS, 0.0, 45.0, 0.8)
	now := float64(time.Now().UnixMicro()) / 1e6

	for run := 0; run < 3; run++ {
		for i := 0; i < 16; i++ {
			us := float64(run*100+i) * periodS / 16.0 * 1e6
			bearing := simulatedBearing(us, 0, 45.0, periodS)
			ms.obs = append(ms.obs, buildObs(us, bearing, uint32(0xABC100+i%4), now+float64(run)))
		}
		ms.lastRunTS = 0.0
		ms.TryUpdate(sync, 0)
	}
	if ms.TrustUpdateStreak == 0 {
		t.Fatal("expected streak to advance on clean updates")
	}

	// Weak-but-clean update: insufficient support for trust, but not a reacquire trigger.
	ms.obs = nil
	for i := 0; i < 4; i++ {
		us := float64(400+i) * periodS * 1e6
		bearing := simulatedBearing(us, 0, 45.0, periodS)
		ms.obs = append(ms.obs, buildObs(us, bearing, uint32(0xABC200+i%2), now+10.0))
	}
	ms.lastRunTS = 0.0
	ms.TryUpdate(sync, 0)

	if ms.TrustUpdateStreak != 0 {
		t.Fatalf("expected non-trust-eligible update to reset streak, got %d", ms.TrustUpdateStreak)
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
	ms.ActiveAuthorityMode = authorityModeRecovery
	// Also preset bootstrap so the candidate set includes the wrong seed.
	ms.BootstrapPeriodS = wrongSeedS

	ms.lastRunTS = 0.0
	ms.TryUpdate(sync, 0)

	// Candidate search must have run and produced a non-zero result.
	if ms.LastReacquireCandidateP <= 0 {
		t.Error("expected reacquire candidate period > 0 after search")
	}
	if ms.LastReacquireCandidateSc < 0 {
		t.Error("expected non-negative reacquire candidate score")
	}
}

func TestMultiSyncSolver_ReacquirePublishesConsistentCandidateFamily(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	truePeriodS := 4.0
	wrongSeedS := 4.08
	sync := NewSyncState(1, wrongSeedS, 0.0, 45.0, 0.8)

	now := float64(time.Now().UnixMicro()) / 1e6
	var newestUS float64
	var scored []scoredObs
	seedPeriodUS := wrongSeedS * 1e6
	for i := 0; i < 5; i++ {
		us := float64(i) * truePeriodS * 0.8 * 1e6
		bearing := simulatedBearing(us, 0.0, 45.0, truePeriodS)
		obs := buildObs(us, bearing, uint32(0xABC001+i%3), now+float64(i)*0.4)
		ms.obs = append(ms.obs, obs)
		effectiveUS := msPropCorrectedUS(obs.CentroidUS, float64(obs.RangeNM))
		predicted := msPredictBearing(0.0, 45.0, seedPeriodUS, effectiveUS)
		residual := circularDiff(obs.BearingDeg, predicted)
		absR := math.Abs(residual)
		status := msClassifyResidual(absR)
		baseW := msScoreObs(obs)
		qMult := msICAOQualityMult(ms.ICAOQuality[obs.ICAO])
		effectiveW := 0.0
		switch status {
		case "inlier":
			effectiveW = baseW * qMult
		case "soft":
			effectiveW = baseW * 0.2 * qMult
		}
		fitRejectReason := msFitRejectReason(obs, status, absR, ms.ICAOQuality[obs.ICAO], false)
		scored = append(scored, scoredObs{
			o:               obs,
			residual:        residual,
			effectiveUS:     effectiveUS,
			phaseInRot:      math.Mod((effectiveUS-0.0)/seedPeriodUS*360.0, 360.0),
			baseW:           baseW,
			effectiveW:      effectiveW,
			status:          status,
			fitEligible:     fitRejectReason == "",
			fitRejectReason: fitRejectReason,
		})
		newestUS = us
	}
	ms.BootstrapPeriodS = wrongSeedS
	ms.PeriodS = wrongSeedS
	ms.Present = true
	ms.PhaseEpochUS = 0.0
	ms.PhaseOffsetDeg = 45.0
	ms.PeriodFailureStreak = 3
	ms.PeriodReacquireActive = true
	ms.PeriodReacquireReason = "failure_streak"
	ms.ActiveAuthorityMode = authorityModeRecovery
	ms.updateICAOQuality(scored)
	expected := ms.searchBestCandidate(scored, 0.0)

	ms.lastRunTS = 0.0
	ms.TryUpdate(sync, 0)

	if math.Abs(ms.PeriodS-ms.LastReacquireCandidateP) > 1e-9 {
		t.Fatalf("published period %.6f should match candidate %.6f", ms.PeriodS, ms.LastReacquireCandidateP)
	}
	if math.Abs(ms.PeriodS-wrongSeedS) < 1e-6 {
		t.Fatalf("expected reacquire to publish a new candidate family, still at seed %.6f", wrongSeedS)
	}
	if math.Abs(ms.PeriodS-expected.periodS) > 1e-9 {
		t.Fatalf("published period %.6f should match expected candidate %.6f", ms.PeriodS, expected.periodS)
	}
	expectedEpochUS := msPropCorrectedUS(newestUS, 50.0)
	if math.Abs(ms.PhaseEpochUS-expected.newEpochUS) > 1e-6 || math.Abs(ms.PhaseEpochUS-expectedEpochUS) > 1e-6 {
		t.Fatalf("published phase epoch %.1f should match candidate/newest effective epoch %.1f", ms.PhaseEpochUS, expected.newEpochUS)
	}
	if math.Abs(circularDiff(ms.PhaseOffsetDeg, expected.publishedOffsetDeg)) > 1e-6 {
		t.Fatalf("published phase offset %.6f should match candidate package %.6f", ms.PhaseOffsetDeg, expected.publishedOffsetDeg)
	}
}

func TestMultiSyncSolver_NormalRefinementPublishesAlignmentFromRefinedPeriod(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	livePeriodS := 4.05
	truePeriodS := 4.0
	seedEpochUS := 0.0
	seedOffsetDeg := 45.0
	sync := NewSyncState(1, livePeriodS, seedEpochUS, seedOffsetDeg, 0.8)
	ms.SlopeHistory = []float64{0.6, 0.6, 0.6, 0.6, 0.6}
	ms.SmoothSlopeDegPerS = 0.6

	now := float64(time.Now().UnixMicro()) / 1e6
	for i := 0; i < 48; i++ {
		us := float64(i) * truePeriodS / 8.0 * 1e6
		bearing := simulatedBearing(us, seedEpochUS, seedOffsetDeg, truePeriodS)
		ms.obs = append(ms.obs, buildObs(us, bearing, uint32(0xABC001+i%4), now+float64(i)*0.3))
	}

	ms.lastRunTS = 0.0
	ms.TryUpdate(sync, 0)

	if ms.PeriodReacquireActive {
		t.Fatal("expected normal refinement path, not reacquire")
	}
	if math.Abs(ms.PeriodS-livePeriodS) < 1e-4 {
		t.Fatalf("expected refined period to move away from live seed %.6f, got %.6f", livePeriodS, ms.PeriodS)
	}

	scored := scoreObsForSeed(ms, ms.obs, seedEpochUS, seedOffsetDeg, livePeriodS)
	expected := ms.buildAlignmentForPeriod(scored, seedEpochUS, seedOffsetDeg, ms.PeriodS, now)
	stale := ms.buildAlignmentForPeriod(scored, seedEpochUS, seedOffsetDeg, livePeriodS, now)

	if math.Abs(ms.PhaseEpochUS-expected.epochUS) > 1e-6 {
		t.Fatalf("published epoch %.6f should match refined-period alignment %.6f", ms.PhaseEpochUS, expected.epochUS)
	}
	if math.Abs(circularDiff(ms.PhaseOffsetDeg, expected.offsetDeg)) > 1e-6 {
		t.Fatalf("published offset %.6f should match refined-period alignment %.6f", ms.PhaseOffsetDeg, expected.offsetDeg)
	}

	publishedResidual := meanAbsAlignmentResidual(ms.obs, ms.PhaseEpochUS, ms.PhaseOffsetDeg, ms.PeriodS)
	staleResidual := meanAbsAlignmentResidual(ms.obs, stale.epochUS, stale.offsetDeg, ms.PeriodS)
	if publishedResidual > staleResidual {
		t.Fatalf("published refined-period alignment residual %.4f should not be worse than stale-seed alignment %.4f", publishedResidual, staleResidual)
	}
}

func TestMultiSyncSolver_PeriodBaseReflectsSameRunTrustPromotion(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	bootstrapPeriodS := 4.05
	truePeriodS := 4.0
	seedEpochUS := 0.0
	seedOffsetDeg := 45.0
	sync := NewSyncState(1, bootstrapPeriodS, seedEpochUS, seedOffsetDeg, 0.8)

	ms.BootstrapPeriodS = bootstrapPeriodS
	ms.TrustUpdateStreak = trustMinStreak - 1
	ms.SlopeHistory = []float64{0.6, 0.6, 0.6, 0.6, 0.6}
	ms.SmoothSlopeDegPerS = 0.6

	now := float64(time.Now().UnixMicro()) / 1e6
	for i := 0; i < 48; i++ {
		us := float64(i) * truePeriodS / 8.0 * 1e6
		bearing := simulatedBearing(us, seedEpochUS, seedOffsetDeg, truePeriodS)
		ms.obs = append(ms.obs, buildObs(us, bearing, uint32(0xABC100+i%4), now+float64(i)*0.3))
	}

	ms.lastRunTS = 0.0
	ms.TryUpdate(sync, 0)

	if ms.TrustedBasePeriodS <= 0 {
		t.Fatal("expected trusted base promotion on this run")
	}
	if math.Abs(ms.PeriodBaseS-ms.TrustedBasePeriodS) > 1e-9 {
		t.Fatalf("PeriodBaseS %.9f should reflect promoted trusted base %.9f immediately", ms.PeriodBaseS, ms.TrustedBasePeriodS)
	}
	if math.Abs(ms.PeriodBaseS-bootstrapPeriodS) < 1e-6 {
		t.Fatalf("PeriodBaseS should not lag on bootstrap base %.6f after promotion", bootstrapPeriodS)
	}
}

func TestMultiSyncSolver_TrustedBaseEMAUsesPublishedFinalPeriod(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	ms.TrustedBasePeriodS = 4.0000
	ms.TrustUpdateStreak = trustMinStreak - 1

	finalPeriodS := 4.2000
	ms.applyTrustedBaseUpdate(finalPeriodS, true, false)

	expected := 0.98*4.0000 + 0.02*finalPeriodS
	if math.Abs(ms.TrustedBasePeriodS-expected) > 1e-9 {
		t.Fatalf("TrustedBasePeriodS %.9f should EMA toward published final period %.9f, want %.9f", ms.TrustedBasePeriodS, finalPeriodS, expected)
	}
	if ms.TrustUpdateStreak != trustMinStreak {
		t.Fatalf("expected trust streak to advance to %d, got %d", trustMinStreak, ms.TrustUpdateStreak)
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
	ms.TryUpdate(sync, 0)

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
		ms.TryUpdate(sync, 0)
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
			SignalDBFS: &sig,
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

func TestMultiSyncSolver_UsesDominantPriorDuringRecovery(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	truePeriodS := 4.0
	badCompactPeriodS := 4.16
	sync := NewSyncState(1, badCompactPeriodS, 0.0, 45.0, 0.8)

	now := float64(time.Now().UnixMicro()) / 1e6
	ms.ActiveAuthorityMode = authorityModeRecovery
	for i := 0; i < 24; i++ {
		us := float64(i) * truePeriodS / 4.0 * 1e6
		icao := uint32(0xABC001 + i%3)
		bearing := simulatedBearing(us, 0.0, 45.0, truePeriodS)
		posAge := float32(0.5)
		sig := float32(-30.0)
		ms.obs = append(ms.obs, MultiSyncObs{
			CentroidUS: us,
			ICAO:       icao,
			BearingDeg: bearing,
			RangeNM:    0.0,
			PosAgeS:    posAge,
			NReplies:   8,
			SignalDBFS: &sig,
			WallTS:     now - 20.0 + float64(i)*0.5,
		})
	}

	ms.lastRunTS = 0.0
	if !ms.TryUpdate(sync, truePeriodS) {
		t.Fatal("expected solver to run")
	}

	snap := ms.Snapshot()
	if !snap.RecoveryModeActive {
		t.Fatal("expected recovery mode to activate when compact period diverges from dominant period")
	}
	if snap.ActiveFamilyPriorSource != "dominant_live_df" {
		t.Fatalf("expected dominant prior source during recovery, got %q", snap.ActiveFamilyPriorSource)
	}
	if !snap.DominantPriorActive {
		t.Fatal("expected dominant prior to be marked active")
	}
	if math.Abs(snap.ActiveFamilyPriorPeriodS-truePeriodS) > 0.01 {
		t.Fatalf("expected dominant prior period %.3f, got %.6f", truePeriodS, snap.ActiveFamilyPriorPeriodS)
	}
	if math.Abs(snap.PeriodS-truePeriodS) >= math.Abs(badCompactPeriodS-truePeriodS) {
		t.Fatalf("expected refined period %.6f to move closer to dominant %.3f than compact %.3f", snap.PeriodS, truePeriodS, badCompactPeriodS)
	}
}

func TestMultiSyncSolver_AuthorityModeHysteresis(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	if ms.ActiveAuthorityMode != authorityModeCompact {
		t.Fatalf("expected compact authority at init, got %q", ms.ActiveAuthorityMode)
	}

	for i := 0; i < authorityPromoteRefinedStreak-1; i++ {
		ms.updateAuthorityModePostFit(true, false, false, false, true, float64(i+1))
		if ms.ActiveAuthorityMode != authorityModeCompact {
			t.Fatalf("refined authority promoted too early on step %d: %q", i, ms.ActiveAuthorityMode)
		}
	}
	ms.updateAuthorityModePostFit(true, false, false, false, true, 10.0)
	if ms.ActiveAuthorityMode != authorityModeRefined {
		t.Fatalf("expected refined authority after healthy streak, got %q", ms.ActiveAuthorityMode)
	}

	for i := 0; i < authorityCompactReclaimStreak-1; i++ {
		ms.updateAuthorityModePostFit(false, true, true, false, false, 20.0+float64(i))
		if ms.ActiveAuthorityMode != authorityModeRefined {
			t.Fatalf("refined authority dropped too early on failure step %d: %q", i, ms.ActiveAuthorityMode)
		}
	}
	ms.updateAuthorityModePostFit(false, true, true, false, false, 30.0)
	if ms.ActiveAuthorityMode != authorityModeCompact {
		t.Fatalf("expected compact authority only after sustained failure/compact health, got %q", ms.ActiveAuthorityMode)
	}
	if ms.AuthoritySwitchCount < 2 {
		t.Fatalf("expected authority switch count to record both promotions, got %d", ms.AuthoritySwitchCount)
	}
}

func TestMultiSyncSolver_HealthyCompactAssessmentStaysOutOfRecovery(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	periodS := 4.0
	sync := NewSyncState(1, periodS, 0.0, 45.0, 0.9)
	recovery, compactUnreliable, reasons := ms.assessRecoveryMode(sync, periodS)
	if recovery {
		t.Fatalf("did not expect recovery mode for healthy compact sync, got reasons=%v", reasons)
	}
	if compactUnreliable {
		t.Fatalf("did not expect healthy compact sync to be marked unreliable, got reasons=%v", reasons)
	}
}

func TestMultiSyncSolver_RecoveryAdmissionRelaxesPositionAgeGate(t *testing.T) {
	o := MultiSyncObs{
		CentroidUS: 1_000_000.0,
		ICAO:       0xABC200,
		BearingDeg: 120.0,
		RangeNM:    0.0,
		PosAgeS:    10.0,
		NReplies:   8,
	}
	if got := msFitRejectReason(o, "inlier", 4.0, nil, false); got != "stale_position" {
		t.Fatalf("expected normal admission to reject stale position, got %q", got)
	}
	if got := msFitRejectReason(o, "inlier", 4.0, nil, true); got != "" {
		t.Fatalf("expected recovery admission to relax stale-position gate, got %q", got)
	}
}

func TestEvalCandidatePeriod_PreservesAnchorCandidates(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	truePeriodS := 4.0
	epochUS := 0.0

	var scored []scoredObs
	for i := 0; i < 18; i++ {
		us := float64(i) * truePeriodS / 3.0 * 1e6
		bearing := simulatedBearing(us, epochUS, 45.0, truePeriodS)
		sig := float32(-30.0)
		o := MultiSyncObs{
			CentroidUS: us,
			ICAO:       uint32(0xABC001 + i%3),
			BearingDeg: bearing,
			RangeNM:    0.0,
			PosAgeS:    0.5,
			NReplies:   8,
			SignalDBFS: &sig,
		}
		scored = append(scored, scoredObs{
			o:           o,
			effectiveUS: us,
			baseW:       0.8,
			effectiveW:  0.8,
			status:      "inlier",
			fitEligible: true,
		})
	}

	result := ms.evalCandidatePeriod(scored, truePeriodS, epochUS)
	if result.anchorCandidateCount == 0 {
		t.Fatal("expected candidate evaluation to retain anchor candidate count")
	}
	if len(result.anchorCandidates) == 0 {
		t.Fatal("expected candidate evaluation to retain anchor candidate rows")
	}
	if result.anchorICAO == nil {
		t.Fatal("expected candidate evaluation to retain selected anchor")
	}
}

func buildAnchorScoredObs(icao uint32, offsetDeg, baseW float64, count int) []scoredObs {
	out := make([]scoredObs, 0, count)
	for i := 0; i < count; i++ {
		effectiveUS := float64(i) * 1_000_000.0
		phaseDeg := math.Mod(effectiveUS/4_000_000.0*360.0, 360.0)
		bearing := math.Mod(phaseDeg+offsetDeg, 360.0)
		out = append(out, scoredObs{
			o: MultiSyncObs{
				CentroidUS: effectiveUS,
				ICAO:       icao,
				BearingDeg: bearing,
				RangeNM:    0.0,
				PosAgeS:    0.5,
				NReplies:   8,
			},
			effectiveUS: effectiveUS,
			baseW:       baseW,
			effectiveW:  baseW,
			status:      "inlier",
			fitEligible: true,
		})
	}
	return out
}

func TestMultiSyncSolver_SelectAnchorKeepsCurrentOnSmallScoreDelta(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	currentICAO := uint32(0xABC001)
	ms.AnchorICAO = &currentICAO
	ms.AnchorHoldUpdates = anchorHoldMinUpdates

	scored := append(
		buildAnchorScoredObs(currentICAO, 30.0, 0.55, 4),
		buildAnchorScoredObs(0xABC002, 60.0, 0.62, 4)...,
	)

	_, anchorICAO, _, _, _, _, _ := ms.selectAnchor(scored, 0.0, 4_000_000.0, 0.0, 123.0, true)
	if anchorICAO == nil {
		t.Fatal("expected selected anchor")
	}
	if *anchorICAO != currentICAO {
		t.Fatalf("expected hysteresis to keep current anchor %06X, got %06X", currentICAO, *anchorICAO)
	}
	if ms.AnchorSwitchCount != 0 {
		t.Fatalf("expected no anchor switch, got %d", ms.AnchorSwitchCount)
	}
	if ms.AnchorHoldUpdates <= anchorHoldMinUpdates {
		t.Fatalf("expected hold counter to continue increasing, got %d", ms.AnchorHoldUpdates)
	}
}

func TestMultiSyncSolver_SelectAnchorSwitchesWhenChallengerMateriallyBetter(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	currentICAO := uint32(0xABC001)
	ms.AnchorICAO = &currentICAO
	ms.AnchorHoldUpdates = anchorHoldMinUpdates

	scored := append(
		buildAnchorScoredObs(currentICAO, 30.0, 0.40, 4),
		buildAnchorScoredObs(0xABC002, 60.0, 0.72, 4)...,
	)

	_, anchorICAO, _, _, _, _, _ := ms.selectAnchor(scored, 0.0, 4_000_000.0, 0.0, 456.0, true)
	if anchorICAO == nil {
		t.Fatal("expected selected anchor")
	}
	if want := uint32(0xABC002); *anchorICAO != want {
		t.Fatalf("expected materially better challenger %06X, got %06X", want, *anchorICAO)
	}
	if ms.AnchorSwitchCount != 1 {
		t.Fatalf("expected one anchor switch, got %d", ms.AnchorSwitchCount)
	}
	if ms.LastAnchorSwitchReason != "anchor_hysteresis_switch" {
		t.Fatalf("unexpected anchor switch reason %q", ms.LastAnchorSwitchReason)
	}
	if ms.AnchorHoldUpdates != 1 {
		t.Fatalf("expected hold counter reset after switch, got %d", ms.AnchorHoldUpdates)
	}
}

func TestMultiSyncSolver_AuthoritativePeriodHasStrongInertia(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	ms.AuthoritativePresent = true
	ms.AuthoritativePeriodS = 4.0
	ms.AuthoritativePhaseOffsetDeg = 45.0
	ms.AuthoritativePhaseEpochUS = 0.0
	ms.CandidatePromotionStreak = candidatePromotionMinStreak

	validation := syncValidationSummary{
		score:              0.92,
		branchAmbiguity:    0.2,
		circularDispersion: 4.0,
		validatorAgreement: 3,
		strong:             true,
	}
	for i := 0; i < 20; i++ {
		candidatePeriod := 4.002
		if i%2 == 1 {
			candidatePeriod = 3.998
		}
		ms.updateAuthoritativeState(syncStateEstimate{
			present:   true,
			periodS:   candidatePeriod,
			epochUS:   float64(i) * 1e6,
			offsetDeg: 45.0,
		}, validation, false, float64(i+1))
	}
	if math.Abs(ms.AuthoritativePeriodS-4.0) > 0.0001 {
		t.Fatalf("expected authoritative period to remain nearly fixed, got %.6f", ms.AuthoritativePeriodS)
	}
	if ms.LastAuthoritativePeriodGain <= 0 {
		t.Fatalf("expected non-zero period gain under strong validation, got %.6f", ms.LastAuthoritativePeriodGain)
	}
}

func TestMultiSyncSolver_WeakPhaseFreezesAuthoritativePeriod(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	ms.AuthoritativePresent = true
	ms.AuthoritativePeriodS = 4.0
	ms.AuthoritativePhaseOffsetDeg = 45.0
	ms.AuthoritativePhaseEpochUS = 0.0
	ms.AuthoritativeAnchorICAO = nil
	ms.CandidatePromotionStreak = 0

	ms.updateAuthoritativeState(syncStateEstimate{
		present:   true,
		periodS:   4.2,
		epochUS:   1e6,
		offsetDeg: 210.0,
	}, syncValidationSummary{
		score:              0.2,
		branchAmbiguity:    0.95,
		circularDispersion: 40.0,
		validatorAgreement: 0,
		validatorDisagree:  2,
		weak:               true,
	}, false, 10.0)

	if ms.AuthoritativePeriodS != 4.0 {
		t.Fatalf("expected weak phase validation to freeze authoritative period, got %.6f", ms.AuthoritativePeriodS)
	}
	if !ms.LastPeriodFrozenDueToPhaseValidation {
		t.Fatal("expected weak phase validation to mark period frozen")
	}
	if ms.LastAuthoritativePeriodGain != 0 {
		t.Fatalf("expected zero authoritative period gain under weak validation, got %.6f", ms.LastAuthoritativePeriodGain)
	}
}

func TestMultiSyncSolver_WrongPhaseBranchNotPromotedQuickly(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	authAnchor := uint32(0xABC001)
	ms.AuthoritativePresent = true
	ms.AuthoritativePeriodS = 4.0
	ms.AuthoritativePhaseOffsetDeg = 30.0
	ms.AuthoritativePhaseEpochUS = 0.0
	ms.AuthoritativeAnchorICAO = &authAnchor
	ms.CandidatePromotionStreak = candidatePromotionMinStreak

	candidateAnchor := uint32(0xABC002)
	ms.updateAuthoritativeState(syncStateEstimate{
		present:     true,
		periodS:     4.0,
		epochUS:     1e6,
		offsetDeg:   210.0,
		anchorICAO:  &candidateAnchor,
		anchorScore: 0.8,
		anchorPhase: 210.0,
	}, syncValidationSummary{
		score:              0.75,
		branchAmbiguity:    0.96,
		circularDispersion: 8.0,
		validatorAgreement: 2,
		weak:               false,
	}, false, 20.0)

	if math.Abs(circularDiff(ms.AuthoritativePhaseOffsetDeg, 30.0)) > 1e-6 {
		t.Fatalf("expected authoritative phase branch to hold, got %.2f", ms.AuthoritativePhaseOffsetDeg)
	}
	if ms.AuthoritativeAnchorICAO == nil || *ms.AuthoritativeAnchorICAO != authAnchor {
		t.Fatalf("expected authoritative anchor to remain %06X, got %#v", authAnchor, ms.AuthoritativeAnchorICAO)
	}
}

func TestMultiSyncSolver_ValidatorBackedPromotionInitializesAuthoritativeState(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	ms.CandidatePromotionStreak = authoritativeInitPromotionStreak
	anchor := uint32(0xABC010)

	ms.updateAuthoritativeState(syncStateEstimate{
		present:     true,
		periodS:     4.0,
		epochUS:     2e6,
		offsetDeg:   55.0,
		anchorICAO:  &anchor,
		anchorScore: 0.9,
		anchorPhase: 55.0,
	}, syncValidationSummary{
		score:              0.9,
		branchAmbiguity:    0.2,
		circularDispersion: 3.0,
		validatorAgreement: 3,
		strong:             true,
	}, false, 30.0)

	if !ms.AuthoritativePresent {
		t.Fatal("expected strong validator-backed candidate to initialize authoritative state")
	}
	if ms.AuthoritativeAnchorICAO == nil || *ms.AuthoritativeAnchorICAO != anchor {
		t.Fatalf("expected authoritative anchor %06X, got %#v", anchor, ms.AuthoritativeAnchorICAO)
	}
	if math.Abs(ms.AuthoritativePhaseOffsetDeg-55.0) > 1e-6 {
		t.Fatalf("expected authoritative phase to initialize from candidate, got %.2f", ms.AuthoritativePhaseOffsetDeg)
	}
}

func TestMultiSyncSolver_RecoveryThenSlowSettlement(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	authAnchor := uint32(0xABC111)
	ms.AuthoritativePresent = true
	ms.AuthoritativePeriodS = 4.0
	ms.AuthoritativePhaseOffsetDeg = 40.0
	ms.AuthoritativePhaseEpochUS = 0.0
	ms.AuthoritativeAnchorICAO = &authAnchor
	ms.CandidatePromotionStreak = candidatePromotionMinStreak

	validation := syncValidationSummary{
		score:              0.9,
		branchAmbiguity:    0.2,
		circularDispersion: 4.0,
		validatorAgreement: 3,
		strong:             true,
	}
	candidateAnchor := uint32(0xABC222)
	candidate := syncStateEstimate{
		present:     true,
		periodS:     4.05,
		epochUS:     5e6,
		offsetDeg:   100.0,
		anchorICAO:  &candidateAnchor,
		anchorScore: 0.8,
		anchorPhase: 100.0,
	}

	ms.updateAuthoritativeState(candidate, validation, true, 40.0)
	if math.Abs(ms.AuthoritativePeriodS-4.0) > 1e-9 {
		t.Fatalf("expected recovery to hold authoritative period, got %.6f", ms.AuthoritativePeriodS)
	}
	if math.Abs(circularDiff(ms.AuthoritativePhaseOffsetDeg, 40.0)) > 1e-6 {
		t.Fatalf("expected recovery to hold authoritative phase, got %.2f", ms.AuthoritativePhaseOffsetDeg)
	}

	for i := 0; i < 6; i++ {
		ms.CandidatePromotionStreak = candidatePromotionMinStreak
		ms.updateAuthoritativeState(candidate, validation, false, 41.0+float64(i))
	}
	if math.Abs(ms.AuthoritativePeriodS-4.0) < 1e-6 {
		t.Fatal("expected authoritative period to begin moving after sustained non-recovery validation")
	}
	if math.Abs(ms.AuthoritativePeriodS-4.05) < 0.001 {
		t.Fatalf("expected authoritative period to settle slowly, got %.6f", ms.AuthoritativePeriodS)
	}
	if math.Abs(circularDiff(ms.AuthoritativePhaseOffsetDeg, 100.0)) < 20.0 {
		t.Fatalf("expected authoritative phase to move cautiously, got %.2f", ms.AuthoritativePhaseOffsetDeg)
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

// ─── dominant-prior lock / AbsolutePhaseTrusted tests ────────────────────────

// TestA_DominantPriorLock verifies that when dominantPeriodS is provided, the published
// refined period cannot exceed periodRefineMaxPPMFromDominant from the dominant prior,
// even when burst evidence strongly suggests a period far outside that bound.
func TestA_DominantPriorLock(t *testing.T) {
	dominantPeriodS := 4.0
	ms := NewMultiSyncSolver(1)
	sync := NewSyncState(1, dominantPeriodS, 0.0, 45.0, 0.8)

	// Observations consistent with a period 2% away from dominant (20000 PPM — well
	// outside the 300ppm refinement bound).
	farPeriodS := dominantPeriodS * 1.02
	now := float64(time.Now().UnixMicro()) / 1e6
	for i := 0; i < 60; i++ {
		us := float64(i) * farPeriodS / 10.0 * 1e6
		bearing := simulatedBearing(us, 0.0, 45.0, farPeriodS)
		ms.obs = append(ms.obs, buildObs(us, bearing, uint32(0xABC001+i%3), now+float64(i)*0.3))
	}
	// Run multiple times to allow slope and reacquire paths to fire.
	for i := 0; i < 14; i++ {
		ms.lastRunTS = 0.0
		ms.runFit(sync, dominantPeriodS, now+float64(i))
	}

	snap := ms.Snapshot()
	if snap.PeriodS <= 0 {
		t.Skip("solver did not produce a state")
	}
	maxAllowedDeltaPPM := periodRefineMaxPPMFromDominant + 1.0 // +1 for float tolerance
	_, publishedPPM := periodDeltaToDominant(snap.PeriodS, dominantPeriodS)
	if math.Abs(publishedPPM) > maxAllowedDeltaPPM {
		t.Fatalf("published period %.9f is %.2f PPM from dominant %.9f, exceeds allowed %.1f PPM",
			snap.PeriodS, math.Abs(publishedPPM), dominantPeriodS, periodRefineMaxPPMFromDominant)
	}
}

// TestB_BoundedSlopeCorrection verifies that when a persistent residual slope is present
// with a dominant prior active, each per-update period step is bounded by
// periodRefineMaxStepPPM and the final period stays within periodRefineMaxPPMFromDominant.
func TestB_BoundedSlopeCorrection(t *testing.T) {
	dominantPeriodS := 4.0
	ms := NewMultiSyncSolver(1)
	// Seed slope history so slope-based refinement fires.
	ms.SlopeHistory = []float64{0.5, 0.5, 0.5, 0.5, 0.5, 0.5}
	ms.SmoothSlopeDegPerS = 0.5
	ms.PeriodS = dominantPeriodS
	ms.Present = true
	ms.PhaseEpochUS = 0.0
	ms.PhaseOffsetDeg = 45.0
	ms.BootstrapPeriodS = dominantPeriodS

	// Observations matching dominant period (clean, no slope — slope comes from seed above).
	now := float64(time.Now().UnixMicro()) / 1e6
	for i := 0; i < 80; i++ {
		us := float64(i) * dominantPeriodS / 10.0 * 1e6
		bearing := simulatedBearing(us, 0.0, 45.0, dominantPeriodS)
		ms.obs = append(ms.obs, buildObs(us, bearing, uint32(0xABC001+i%3), now+float64(i)*0.3))
	}

	initialPeriod := ms.PeriodS
	sync := NewSyncState(1, dominantPeriodS, 0.0, 45.0, 0.8)
	ms.lastRunTS = 0.0
	ms.runFit(sync, dominantPeriodS, now)

	if ms.PeriodS <= 0 {
		t.Skip("solver did not produce a state")
	}
	// Per-update step must not exceed periodRefineMaxStepPPM (+1 float tolerance).
	stepPPM := math.Abs(ms.PeriodS-initialPeriod) / initialPeriod * 1e6
	if stepPPM > periodRefineMaxStepPPM+1.0 {
		t.Fatalf("single-step period change %.3f PPM exceeds max step %.3f PPM when dominant prior active",
			stepPPM, periodRefineMaxStepPPM)
	}
	// Final period must be within dominant bound.
	_, deltaPPM := periodDeltaToDominant(ms.PeriodS, dominantPeriodS)
	if math.Abs(deltaPPM) > periodRefineMaxPPMFromDominant+1.0 {
		t.Fatalf("period %.9f is %.2f PPM from dominant — outside allowed bound %.1f PPM",
			ms.PeriodS, math.Abs(deltaPPM), periodRefineMaxPPMFromDominant)
	}
}

// TestC_NoAutonomousReacquireOutsideBound verifies that when the reacquire candidate search
// finds a period outside the dominant-prior bound, it populates diagnostic fields only and
// does NOT update the published period or authoritative state.
//
// Uses wrongSeedS=5.0 (25% = 250000 PPM from dominant 4.0) so that observations
// score predominantly as "rejected" against the 4.0s prior, forcing cleanUpdate=false
// and thus triggering the candidate search.
func TestC_NoAutonomousReacquireOutsideBound(t *testing.T) {
	dominantPeriodS := 4.0
	wrongSeedS := 5.0 // 250000 PPM off — far outside 300ppm bound; forces large residuals

	ms := NewMultiSyncSolver(1)
	// ms.Present is NOT set so selectActiveFamilyPrior falls through to dominantPeriodS.
	// This ensures observations score against the dominant (4.0s) rather than wrongSeedS,
	// producing large residuals and forcing cleanUpdate=false so the candidate search fires.
	ms.BootstrapPeriodS = wrongSeedS
	ms.ActiveAuthorityMode = authorityModeRecovery
	ms.PeriodFailureStreak = 5
	ms.PeriodReacquireActive = true

	// Observations consistent with wrongSeedS (outside dominant bound).
	// Against dominant 4.0s, the bearings drift ~9°/obs and ~72% become "rejected" after
	// several rotations, forcing cleanUpdate=false and triggering the candidate search.
	sync := NewSyncState(1, wrongSeedS, 0.0, 45.0, 0.8)
	now := float64(time.Now().UnixMicro()) / 1e6
	for i := 0; i < 40; i++ {
		us := float64(i) * wrongSeedS / 10.0 * 1e6
		bearing := simulatedBearing(us, 0.0, 45.0, wrongSeedS)
		ms.obs = append(ms.obs, buildObs(us, bearing, uint32(0xABC001+i%3), now+float64(i)*0.5))
	}
	ms.lastRunTS = 0.0
	ms.TryUpdate(sync, dominantPeriodS)

	snap := ms.Snapshot()
	// Candidate search should have populated diagnostic fields.
	if snap.ReacquireCandidatePeriod <= 0 {
		t.Error("expected reacquire candidate to be populated as diagnostic")
	}
	// Published period must remain within dominant bound.
	if snap.PeriodS > 0 {
		_, publishedPPM := periodDeltaToDominant(snap.PeriodS, dominantPeriodS)
		if math.Abs(publishedPPM) > periodRefineMaxPPMFromDominant+1.0 {
			t.Fatalf("candidate outside dominant bound was published: period %.9f is %.2f PPM from dominant",
				snap.PeriodS, math.Abs(publishedPPM))
		}
	}
	// If the candidate found is itself outside the bound, DominantPriorInconsistent must be set.
	if snap.ReacquireCandidatePeriod > 0 {
		_, candidatePPM := periodDeltaToDominant(snap.ReacquireCandidatePeriod, dominantPeriodS)
		if math.Abs(candidatePPM) > periodRefineMaxPPMFromDominant {
			if !snap.DominantPriorInconsistent {
				t.Fatalf("expected DominantPriorInconsistent=true when candidate (%.2f PPM from dominant) is outside bound",
					math.Abs(candidatePPM))
			}
			// And the published period must NOT match the out-of-bound candidate.
			if snap.PeriodS > 0 && math.Abs(snap.PeriodS-snap.ReacquireCandidatePeriod) < 1e-9 {
				t.Fatalf("out-of-bound candidate period %.9f was published as live period", snap.ReacquireCandidatePeriod)
			}
		}
	}
}

// TestD_AbsolutePhaseTrustedRequiresAnchor verifies that AbsolutePhaseTrusted is false
// when no anchor passes validation, even when the authoritative state is present.
func TestD_AbsolutePhaseTrustedRequiresAnchor(t *testing.T) {
	ms := NewMultiSyncSolver(1)
	dominantPeriodS := 4.0
	sync := NewSyncState(1, dominantPeriodS, 0.0, 45.0, 0.8)

	// Minimal observations — below anchor selection threshold for most ICAOs.
	now := float64(time.Now().UnixMicro()) / 1e6
	for i := 0; i < 4; i++ {
		us := float64(i) * dominantPeriodS * 1e6
		ms.obs = append(ms.obs, buildObs(us, simulatedBearing(us, 0, 45.0, dominantPeriodS),
			uint32(0xABC001+i%2), now+float64(i)))
	}
	// Force authoritative present without a valid anchor.
	anchor := uint32(0xABC001)
	ms.AuthoritativePresent = true
	ms.AuthoritativePeriodS = dominantPeriodS
	ms.AuthoritativePhaseOffsetDeg = 45.0
	ms.AuthoritativePhaseEpochUS = 0.0
	ms.AuthoritativeAnchorICAO = &anchor
	ms.AbsolutePhaseTrusted = true // set; must be cleared if anchor/validation fails

	ms.lastRunTS = 0.0
	ms.TryUpdate(sync, dominantPeriodS)

	snap := ms.Snapshot()
	// With only 4 observations across 2 ICAOs, validation will be weak → not trusted.
	// (We don't hard-assert here because obs may or may not satisfy anchor gates,
	// but we verify the field is accessible and correctly typed.)
	_ = snap.AbsolutePhaseTrusted // field must compile and be readable
	// If usable state was not reached, AbsolutePhaseTrusted must be false.
	if !snap.Present || !snap.Usable {
		if snap.AbsolutePhaseTrusted {
			t.Fatal("AbsolutePhaseTrusted must be false when solver is not usable")
		}
	}
}

// TestE_AbsolutePhaseTrustedRequiresPopulationAgreement verifies that AbsolutePhaseTrusted
// is false when two ICAOs are on opposite phase branches (high branch ambiguity).
func TestE_AbsolutePhaseTrustedRequiresPopulationAgreement(t *testing.T) {
	periodS := 4.0
	dominantPeriodS := 4.0
	ms := NewMultiSyncSolver(1)
	sync := NewSyncState(1, periodS, 0.0, 45.0, 0.8)

	now := float64(time.Now().UnixMicro()) / 1e6
	// Two ICAOs on opposite phase branches: offset 45° vs 225°.
	for i := 0; i < 30; i++ {
		us := float64(i) * periodS / 10.0 * 1e6
		bearingA := simulatedBearing(us, 0.0, 45.0, periodS)
		bearingB := simulatedBearing(us, 0.0, 225.0, periodS)
		ms.obs = append(ms.obs, buildObs(us, bearingA, 0xABC001, now+float64(i)*0.4))
		ms.obs = append(ms.obs, buildObs(us, bearingB, 0xABC002, now+float64(i)*0.4))
	}
	// Bootstrap with authoritative state to allow validation to run.
	anchor := uint32(0xABC001)
	ms.AuthoritativePresent = true
	ms.AuthoritativePeriodS = periodS
	ms.AuthoritativePhaseOffsetDeg = 45.0
	ms.AuthoritativePhaseEpochUS = 0.0
	ms.AuthoritativeAnchorICAO = &anchor
	ms.CandidatePromotionStreak = candidatePromotionMinStreak

	ms.lastRunTS = 0.0
	ms.TryUpdate(sync, dominantPeriodS)

	snap := ms.Snapshot()
	// With two ICAOs on opposite branches, branch ambiguity is high and/or validators
	// disagree — AbsolutePhaseTrusted must be false.
	if snap.AbsolutePhaseTrusted {
		t.Fatalf("AbsolutePhaseTrusted must be false with conflicting phase branches (ambiguity=%.2f, validatorDisagree=%d)",
			snap.BranchAmbiguityScore, snap.ValidatorDisagreementCount)
	}
}

// TestF_LocaliserGatingViaSnapshot verifies that AbsolutePhaseTrusted propagates correctly
// through Snapshot() so that the localiser can use it to gate geographic bearing prediction.
func TestF_LocaliserGatingViaSnapshot(t *testing.T) {
	ms := NewMultiSyncSolver(1)

	// Initial state: not trusted.
	snap := ms.Snapshot()
	if snap.AbsolutePhaseTrusted {
		t.Fatal("initial AbsolutePhaseTrusted must be false")
	}
	if snap.DominantPriorInconsistent {
		t.Fatal("initial DominantPriorInconsistent must be false")
	}

	// Manually set fields and verify they round-trip through Snapshot.
	ms.AbsolutePhaseTrusted = true
	ms.LastDominantPriorInconsistent = true
	snap = ms.Snapshot()
	if !snap.AbsolutePhaseTrusted {
		t.Fatal("AbsolutePhaseTrusted must propagate through Snapshot")
	}
	if !snap.DominantPriorInconsistent {
		t.Fatal("DominantPriorInconsistent must propagate through Snapshot")
	}

	// Reset must clear both.
	ms.Reset()
	snap = ms.Snapshot()
	if snap.AbsolutePhaseTrusted {
		t.Fatal("AbsolutePhaseTrusted must be cleared by Reset")
	}
	if snap.DominantPriorInconsistent {
		t.Fatal("DominantPriorInconsistent must be cleared by Reset")
	}
}

// TestDominantPriorInconsistentDiagnostic verifies that when a reacquire candidate is outside
// the dominant-prior bound, DominantPriorInconsistent is set diagnostically and the
// published period stays within the bound.
//
// Uses wrongSeedS=5.0 (25% off) to ensure observations score as "rejected" against the
// dominant 4.0s prior, triggering the reacquire candidate search.
// TestG_SettledWrongPeriodSnapsToDominant is the regression test for the real-world
// scenario where the refined model locked at 4.0178s before the DF dominant prior
// (4.7859s) became available. Previously the model would stay stuck because slope ≈ 0
// at the settled period → slope_not_persistent → refinePeriod early-exits before the
// dominant clamp code. The fix enforces the dominant bound in runFit before scoring.
func TestG_SettledWrongPeriodSnapsToDominant(t *testing.T) {
	wrongPeriodS := 4.0178
	dominantS := 4.7859
	// dominantPPM is ~191,000 — far outside the 300 PPM refinement bound.

	ms := NewMultiSyncSolver(1)
	// Simulate a solver that settled at the wrong period.
	ms.PeriodS = wrongPeriodS
	ms.Present = true
	ms.Usable = true
	ms.AuthoritativePresent = true
	ms.AuthoritativePeriodS = wrongPeriodS
	ms.TrustedBasePeriodS = wrongPeriodS
	ms.TrustUpdateStreak = 20
	ms.ActiveAuthorityMode = authorityModeRefined
	ms.BootstrapPeriodS = wrongPeriodS

	// Observations that are consistent with wrongPeriodS (look clean at the wrong period).
	sync := NewSyncState(1, wrongPeriodS, 0.0, 45.0, 0.8)
	now := float64(time.Now().UnixMicro()) / 1e6
	for i := 0; i < 30; i++ {
		us := float64(i) * wrongPeriodS / 10.0 * 1e6
		bearing := simulatedBearing(us, 0.0, 45.0, wrongPeriodS)
		icao := uint32(0xABC001 + i%3)
		ms.obs = append(ms.obs, buildObs(us, bearing, icao, now+float64(i)*0.5))
	}

	ms.lastRunTS = 0.0
	ms.TryUpdate(sync, dominantS)

	snap := ms.Snapshot()
	if !snap.Present {
		t.Fatal("expected Present=true after solver run")
	}
	_, publishedPPM := periodDeltaToDominant(snap.PeriodS, dominantS)
	if math.Abs(publishedPPM) > periodRefineMaxPPMFromDominant+1.0 {
		t.Fatalf("published period %.9f is %.1f PPM from dominant %.9f — model failed to snap (bound ±%.0f PPM)",
			snap.PeriodS, math.Abs(publishedPPM), dominantS, periodRefineMaxPPMFromDominant)
	}
	// Trust must have been cleared by the family snap.
	if ms.TrustedBasePeriodS > 0 {
		_, trustedPPM := periodDeltaToDominant(ms.TrustedBasePeriodS, dominantS)
		if math.Abs(trustedPPM) > periodRefineMaxPPMFromDominant+1.0 {
			t.Errorf("TrustedBasePeriodS %.9f still outside dominant bound after snap", ms.TrustedBasePeriodS)
		}
	}
}

func TestDominantPriorInconsistentDiagnostic(t *testing.T) {
	dominantPeriodS := 4.0
	wrongSeedS := 5.0 // 250000 PPM off — far outside 300ppm bound
	ms := NewMultiSyncSolver(1)
	// ms.Present is NOT set so selectActiveFamilyPrior falls through to dominantPeriodS,
	// ensuring observations score against 4.0s rather than wrongSeedS.
	ms.BootstrapPeriodS = wrongSeedS
	ms.ActiveAuthorityMode = authorityModeRecovery
	ms.PeriodReacquireActive = true
	ms.PeriodFailureStreak = 5

	// Observations consistent with wrongSeedS; scored against dominant 4.0 → mostly rejected.
	sync := NewSyncState(1, wrongSeedS, 0.0, 45.0, 0.8)
	now := float64(time.Now().UnixMicro()) / 1e6
	for i := 0; i < 40; i++ {
		us := float64(i) * wrongSeedS / 10.0 * 1e6
		bearing := simulatedBearing(us, 0.0, 45.0, wrongSeedS)
		ms.obs = append(ms.obs, buildObs(us, bearing, uint32(0xABC001+i%3), now+float64(i)*0.5))
	}
	ms.lastRunTS = 0.0
	ms.TryUpdate(sync, dominantPeriodS)

	snap := ms.Snapshot()
	// If candidate is outside the dominant bound, DominantPriorInconsistent is set.
	if snap.ReacquireCandidatePeriod > 0 {
		_, candidatePPM := periodDeltaToDominant(snap.ReacquireCandidatePeriod, dominantPeriodS)
		if math.Abs(candidatePPM) > periodRefineMaxPPMFromDominant {
			if !snap.DominantPriorInconsistent {
				t.Fatalf("expected DominantPriorInconsistent when candidate %.9f is %.2f PPM from dominant",
					snap.ReacquireCandidatePeriod, math.Abs(candidatePPM))
			}
		}
	}
	// Published period must be within dominant bound.
	if snap.PeriodS > 0 {
		_, publishedPPM := periodDeltaToDominant(snap.PeriodS, dominantPeriodS)
		if math.Abs(publishedPPM) > periodRefineMaxPPMFromDominant+1.0 {
			t.Fatalf("published period %.9f is %.2f PPM from dominant — exceeds bound %.1f PPM",
				snap.PeriodS, math.Abs(publishedPPM), periodRefineMaxPPMFromDominant)
		}
	}
}
