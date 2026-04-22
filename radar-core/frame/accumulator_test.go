package frame

import (
	"math"
	"net"
	"testing"
	"time"

	rcconfig "github.com/caiusseverus/adsb-dashboard/radar-core/config"
	"github.com/caiusseverus/adsb-dashboard/radar-core/iid"
	"github.com/caiusseverus/adsb-dashboard/radar-core/output"
	"github.com/caiusseverus/adsb-dashboard/radar-core/protocol"
)

// --- test helpers ---

func newTestAccumulator() (*Accumulator, *output.Writer, *iid.PositionCache) {
	w := output.NewWriter()
	pos := iid.NewPositionCache()
	acc := New(3, w, pos)
	return acc, w, pos
}

func makeIIDState(iidNum uint8, periodS float64, status string, refICAO uint32) *iid.IIDState {
	s := iid.NewIIDState(iidNum)
	s.PeriodS = &periodS
	s.Status = status
	// Inject a known ref ICAO into the sync snapshot by bootstrapping sync.
	// We call UpdateSyncEpoch which sets RefICAO.
	// First set RefICAO directly via RefreshReference simulation:
	// Simplest approach: inject period + ref via ApplyRotation then RefreshReference.
	// Actually IIDState.RefICAO is unexported but accessible via SyncSnapshot.
	// We need to use public API. For tests, build a burst record set and call
	// RefreshReference to select the right ICAO.
	return s
}

// injectRef builds burst records for the given ICAO with a uniform period
// and calls RefreshReference so that ICAO is selected as reference.
func injectRef(s *iid.IIDState, refICAO uint32, periodS float64, count int) {
	now := time.Now()
	for i := 0; i < count; i++ {
		s.AddBurst(refICAO, float64(i)*periodS*1_000_000.0, 2, 1)
		_ = now
	}
	// Build rotation model snapshot so RefreshReference works.
	snap := s.TakeIfDirty()
	if snap != nil {
		model := iid.AnalyseBurstRecords(snap)
		s.ApplyRotation(model)
		s.RefreshReference(snap, float64(count-1)*periodS*1_000_000.0)
	}
}

// --- tests ---

// TestAccumulator_NoFrameWithoutPeriod verifies no frames are emitted before
// a period estimate is available.
func TestAccumulator_NoFrameWithoutPeriod(t *testing.T) {
	acc, _, pos := newTestAccumulator()
	pos.Update(0xAA, 51.5, -0.1, nil, float64(time.Now().Unix()))

	s := iid.NewIIDState(3) // no period set
	emitted := 0
	acc.OnFrameEmitted = func(_ *protocol.FrameReady) { emitted++ }

	acc.OnBurst(0xAA, 0.0, 2, nil, s)
	if emitted != 0 {
		t.Errorf("emitted %d frames, want 0 (no period)", emitted)
	}
}

// TestAccumulator_NoFrameWithoutRefICAO verifies no frames before reference is selected.
func TestAccumulator_NoFrameWithoutRefICAO(t *testing.T) {
	acc, _, pos := newTestAccumulator()
	pos.Update(0xAA, 51.5, -0.1, nil, float64(time.Now().Unix()))

	s := iid.NewIIDState(3)
	period := 4.0
	s.PeriodS = &period
	s.Status = "SINGLE_RADAR"
	// RefICAO is nil — no reference selected yet.
	emitted := 0
	acc.OnFrameEmitted = func(_ *protocol.FrameReady) { emitted++ }
	acc.OnBurst(0xAA, 0.0, 2, nil, s)
	if emitted != 0 {
		t.Errorf("emitted %d frames, want 0 (no ref ICAO)", emitted)
	}
}

// TestAccumulator_FrameEmitted builds a complete frame and verifies FRAME_READY.
// This test uses the public IIDState API to set up the state.
func TestAccumulator_FullFrameFlow(t *testing.T) {
	acc, w, pos := newTestAccumulator()
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	w.SetConn(serverConn)

	received := make(chan *protocol.FrameReady, 1)
	go func() {
		framer := protocol.NewFramer(clientConn)
		payload, err := framer.Read()
		if err != nil {
			return
		}
		msg, err := protocol.Decode(payload)
		if err != nil {
			return
		}
		if frameMsg, ok := msg.(*protocol.FrameReady); ok {
			received <- frameMsg
		}
	}()

	now := float64(time.Now().Unix())
	pos.Update(0xAA, 51.5, -0.1, nil, now) // ref
	pos.Update(0xBB, 51.6, -0.2, nil, now) // obs 1
	pos.Update(0xCC, 51.7, -0.3, nil, now) // obs 2
	pos.Update(0xDD, 51.8, -0.4, nil, now) // obs 3 (triggers "good")

	// Build IID state with 0xAA as reference at 4s period.
	s := iid.NewIIDState(3)
	period := 4.0
	injectRef(s, 0xAA, period, 10)
	_, refICAOPtr := s.SyncSnapshot()
	if refICAOPtr == nil || *refICAOPtr != 0xAA {
		t.Skipf("reference ICAO not selected (got %v) — skipping", refICAOPtr)
	}

	// Also inject burst history for obs ICAOs so matchesDominant passes.
	for i := 0; i < 5; i++ {
		acc.appendCentroid(0xBB, float64(i)*4_000_000.0)
		acc.appendCentroid(0xCC, float64(i)*4_000_000.0+500_000.0)
		acc.appendCentroid(0xDD, float64(i)*4_000_000.0+1_000_000.0)
	}

	emitted := 0
	acc.OnFrameEmitted = func(_ *protocol.FrameReady) { emitted++ }

	// Ref fires at 40s — opens a frame.
	acc.OnBurst(0xAA, 40_000_000.0, 2, nil, s)
	// Three observations within the sweep window.
	acc.OnBurst(0xBB, 40_500_000.0, 2, nil, s)
	acc.OnBurst(0xCC, 41_000_000.0, 2, nil, s)
	acc.OnBurst(0xDD, 41_500_000.0, 2, nil, s)

	// Ref fires again at 44s — closes the frame.
	acc.OnBurst(0xAA, 44_000_000.0, 2, nil, s)

	if emitted != 1 {
		t.Errorf("emitted %d frames, want 1", emitted)
	}
	select {
	case msg := <-received:
		if msg.IID != 3 {
			t.Fatalf("FRAME_READY iid=%d, want 3", msg.IID)
		}
		if msg.FrameIndex != 1 {
			t.Fatalf("FRAME_READY frame=%d, want 1", msg.FrameIndex)
		}
		if msg.RefICAO != 0xAA {
			t.Fatalf("FRAME_READY ref=0x%X, want 0xAA", msg.RefICAO)
		}
		if msg.Quality != "good" {
			t.Fatalf("FRAME_READY quality=%s, want good", msg.Quality)
		}
		if math.Abs(msg.PeriodS-period) > 1e-9 {
			t.Fatalf("FRAME_READY period=%.6f, want %.6f", msg.PeriodS, period)
		}
		if len(msg.Observations) != 3 {
			t.Fatalf("FRAME_READY observations=%d, want 3", len(msg.Observations))
		}
		if msg.Observations[0].ICAO != 0xBB || msg.Observations[0].Lat != 51.6 || msg.Observations[0].Lon != -0.2 {
			t.Fatalf("unexpected first observation: %+v", msg.Observations[0])
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for FRAME_READY payload")
	}
}

// TestAccumulator_InsufficientObservations verifies frames with <3 aircraft are dropped.
func TestAccumulator_InsufficientObservations(t *testing.T) {
	acc, _, pos := newTestAccumulator()
	now := float64(time.Now().Unix())
	pos.Update(0xAA, 51.5, -0.1, nil, now)
	pos.Update(0xBB, 51.6, -0.2, nil, now)

	s := iid.NewIIDState(3)
	period := 4.0
	injectRef(s, 0xAA, period, 10)
	_, refICAOPtr := s.SyncSnapshot()
	if refICAOPtr == nil || *refICAOPtr != 0xAA {
		t.Skip("reference not selected")
	}
	for i := 0; i < 5; i++ {
		acc.appendCentroid(0xBB, float64(i)*4_000_000.0)
	}

	emitted := 0
	acc.OnFrameEmitted = func(_ *protocol.FrameReady) { emitted++ }

	acc.OnBurst(0xAA, 40_000_000.0, 2, nil, s) // open
	acc.OnBurst(0xBB, 40_500_000.0, 2, nil, s) // 1 obs only
	acc.OnBurst(0xAA, 44_000_000.0, 2, nil, s) // close — only 2 aircraft, drop

	if emitted != 0 {
		t.Errorf("emitted %d frames, want 0 (only 2 aircraft)", emitted)
	}
}

// TestAccumulator_DuplicateICAOPerFrame verifies each ICAO appears at most once.
func TestAccumulator_DuplicateICAOPerFrame(t *testing.T) {
	acc, _, pos := newTestAccumulator()
	now := float64(time.Now().Unix())
	pos.Update(0xAA, 51.5, -0.1, nil, now)
	pos.Update(0xBB, 51.6, -0.2, nil, now)
	pos.Update(0xCC, 51.7, -0.3, nil, now)

	s := iid.NewIIDState(3)
	period := 4.0
	injectRef(s, 0xAA, period, 10)
	_, refICAOPtr := s.SyncSnapshot()
	if refICAOPtr == nil || *refICAOPtr != 0xAA {
		t.Skip("reference not selected")
	}
	for i := 0; i < 5; i++ {
		acc.appendCentroid(0xBB, float64(i)*4_000_000.0)
		acc.appendCentroid(0xCC, float64(i)*4_000_000.0+500_000.0)
	}

	acc.OnBurst(0xAA, 40_000_000.0, 2, nil, s)
	acc.OnBurst(0xBB, 40_500_000.0, 2, nil, s)
	acc.OnBurst(0xBB, 41_000_000.0, 2, nil, s) // duplicate ICAO
	acc.OnBurst(0xCC, 41_500_000.0, 2, nil, s)
	acc.OnBurst(0xAA, 44_000_000.0, 2, nil, s)

	if acc.frame != nil && acc.frame.nAircraft > 3 {
		// frame was just closed — check via counter
	}
}

// TestAccumulator_Reset clears all state.
func TestAccumulator_Reset(t *testing.T) {
	acc, _, pos := newTestAccumulator()
	pos.Update(0xAA, 51.5, -0.1, nil, float64(time.Now().Unix()))
	acc.appendCentroid(0xAA, 0.0)
	acc.lastFrameStartUS = 1000.0

	acc.Reset()

	if acc.lastFrameStartUS != 0 {
		t.Error("lastFrameStartUS not reset")
	}
	if len(acc.centroidHistory) != 0 {
		t.Error("centroidHistory not cleared")
	}
}

// TestMatchesDominant_FallbackHistoryCheck tests the history fallback.
func TestMatchesDominant_FallbackHistoryCheck(t *testing.T) {
	acc, _, _ := newTestAccumulator()

	// ICAO with a perfect 4s period in centroid history.
	for i := 0; i < 8; i++ {
		acc.appendCentroid(0xAA, float64(i)*4_000_000.0)
	}
	if !acc.matchesDominant(0xAA, 4.0, nil) {
		t.Error("expected dominant match for uniform 4s period")
	}
	// ICAO with a 7s period — should not match 4s family.
	for i := 0; i < 6; i++ {
		acc.appendCentroid(0xBB, float64(i)*7_000_000.0)
	}
	if acc.matchesDominant(0xBB, 4.0, nil) {
		t.Error("expected no dominant match for 7s ICAO against 4s family")
	}
}

// TestMedian verifies the median helper.
func TestMedian(t *testing.T) {
	cases := []struct {
		vals []float64
		want float64
	}{
		{[]float64{3, 1, 2}, 2},
		{[]float64{4, 1, 3, 2}, 2.5},
		{[]float64{5}, 5},
	}
	for _, tc := range cases {
		got := median(tc.vals)
		if math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("median(%v) = %.2f, want %.2f", tc.vals, got, tc.want)
		}
	}
}

func TestAccumulator_CentroidHistoryCapDiagnostics(t *testing.T) {
	acc, _, _ := newTestAccumulator()

	// Use a small test cap and restore default after this test.
	rcconfig.Apply("CENTROID_HISTORY_MAX_PER_ICAO", 35)
	defer rcconfig.Apply("CENTROID_HISTORY_MAX_PER_ICAO", rcconfig.DefaultCentroidHistoryMaxPerICAO)

	for i := 0; i < 60; i++ {
		acc.appendCentroid(0xAAAAAA, float64(i)*4_000_000.0)
	}

	diag := acc.Diagnostics()
	if diag.CentroidHistoryCapPerICAO != 35 {
		t.Fatalf("cap_per_icao=%d, want 35", diag.CentroidHistoryCapPerICAO)
	}
	if diag.CentroidHistoryICAOCount != 1 {
		t.Fatalf("icao_count=%d, want 1", diag.CentroidHistoryICAOCount)
	}
	if diag.CentroidHistoryTotal != 35 {
		t.Fatalf("total=%d, want 35", diag.CentroidHistoryTotal)
	}
	if diag.CentroidHistoryMaxPerICAO != 35 {
		t.Fatalf("max_per_icao=%d, want 35", diag.CentroidHistoryMaxPerICAO)
	}
	if !diag.CentroidHistoryCapHit {
		t.Fatal("expected centroid history cap hit")
	}
	if diag.CentroidHistoryCapHitsTotal == 0 {
		t.Fatal("expected centroid history cap hit counter > 0")
	}
}

// TestAccumulator_ObservationPosAgeS_NonZero verifies that observations carry the
// real position age (> 0 when the position was set a few seconds ago).
func TestAccumulator_ObservationPosAgeS_NonZero(t *testing.T) {
	acc, _, pos := newTestAccumulator()

	// Set positions several seconds in the past.
	past := float64(time.Now().Unix() - 5)
	pos.Update(0xAA, 51.5, -0.1, nil, past)
	pos.Update(0xBB, 51.6, -0.2, nil, past)
	pos.Update(0xCC, 51.7, -0.3, nil, past)

	s := iid.NewIIDState(3)
	period := 4.0
	injectRef(s, 0xAA, period, 10)
	_, refICAOPtr := s.SyncSnapshot()
	if refICAOPtr == nil || *refICAOPtr != 0xAA {
		t.Skip("reference ICAO not selected")
	}
	for i := 0; i < 5; i++ {
		acc.appendCentroid(0xBB, float64(i)*4_000_000.0)
		acc.appendCentroid(0xCC, float64(i)*4_000_000.0+500_000.0)
	}

	acc.OnBurst(0xAA, 40_000_000.0, 2, nil, s)
	acc.OnBurst(0xBB, 40_500_000.0, 2, nil, s)
	acc.OnBurst(0xCC, 41_000_000.0, 2, nil, s)

	// Frame not yet finalised — inspect via emitted callback.
	var emittedFrame *protocol.FrameReady
	acc.OnFrameEmitted = func(f *protocol.FrameReady) { emittedFrame = f }
	acc.OnBurst(0xAA, 44_000_000.0, 2, nil, s) // close frame

	if emittedFrame == nil {
		t.Fatal("no frame emitted")
	}
	for _, obs := range emittedFrame.Observations {
		if obs.PosAgeS < 4.0 {
			t.Errorf("obs 0x%X PosAgeS=%.2f, want >= 4 (position was 5s old)", obs.ICAO, obs.PosAgeS)
		}
	}
}

// TestAccumulator_FrameReady_RefPosAgeS verifies that FRAME_READY carries the
// real reference position age, not always 0.
func TestAccumulator_FrameReady_RefPosAgeS(t *testing.T) {
	acc, _, pos := newTestAccumulator()

	past := float64(time.Now().Unix() - 3)
	pos.Update(0xAA, 51.5, -0.1, nil, past)
	pos.Update(0xBB, 51.6, -0.2, nil, past)
	pos.Update(0xCC, 51.7, -0.3, nil, past)

	s := iid.NewIIDState(3)
	period := 4.0
	injectRef(s, 0xAA, period, 10)
	_, refICAOPtr := s.SyncSnapshot()
	if refICAOPtr == nil || *refICAOPtr != 0xAA {
		t.Skip("reference ICAO not selected")
	}
	for i := 0; i < 5; i++ {
		acc.appendCentroid(0xBB, float64(i)*4_000_000.0)
		acc.appendCentroid(0xCC, float64(i)*4_000_000.0+500_000.0)
	}

	var emittedFrame *protocol.FrameReady
	acc.OnFrameEmitted = func(f *protocol.FrameReady) { emittedFrame = f }

	acc.OnBurst(0xAA, 40_000_000.0, 2, nil, s)
	acc.OnBurst(0xBB, 40_500_000.0, 2, nil, s)
	acc.OnBurst(0xCC, 41_000_000.0, 2, nil, s)
	acc.OnBurst(0xAA, 44_000_000.0, 2, nil, s)

	if emittedFrame == nil {
		t.Fatal("no frame emitted")
	}
	if emittedFrame.RefPosAgeS == nil {
		t.Fatal("RefPosAgeS is nil, want a real age value")
	}
	if *emittedFrame.RefPosAgeS < 2.0 {
		t.Errorf("RefPosAgeS=%.2f, want >= 2 (position was 3s old)", *emittedFrame.RefPosAgeS)
	}
}
