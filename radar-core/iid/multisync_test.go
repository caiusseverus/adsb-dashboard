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
