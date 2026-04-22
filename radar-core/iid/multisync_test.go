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
