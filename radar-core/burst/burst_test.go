package burst_test

import (
	"math"
	"testing"

	"github.com/caiusseverus/adsb-dashboard/radar-core/burst"
)

const burstGapUS = 200_000.0

func pf64(v float64) *float64 { return &v }

// --- Centroid tests ---

func TestCentroidSingleReplyNoSignal(t *testing.T) {
	replies := []burst.Reply{{ArrivalUS: 1000.0, HasSignal: false}}
	c, sig, n := burst.Centroid(replies)
	if c != 1000.0 {
		t.Errorf("centroid: got %v want 1000.0", c)
	}
	if sig != nil {
		t.Errorf("expected nil signal, got %v", *sig)
	}
	if n != 1 {
		t.Errorf("nReplies: got %v want 1", n)
	}
}

func TestCentroidEqualWeights(t *testing.T) {
	// No signals → equal weights → simple mean.
	replies := []burst.Reply{
		{ArrivalUS: 1000.0, HasSignal: false},
		{ArrivalUS: 2000.0, HasSignal: false},
		{ArrivalUS: 3000.0, HasSignal: false},
	}
	c, _, _ := burst.Centroid(replies)
	if math.Abs(c-2000.0) > 1e-9 {
		t.Errorf("centroid: got %v want 2000.0", c)
	}
}

func TestCentroidSignalWeighted(t *testing.T) {
	// w = 10^(dbfs/20). Two replies at -20dBFS and -40dBFS.
	// w1 = 10^(-20/20) = 0.1,  w2 = 10^(-40/20) = 0.01
	// centroid = (1000*0.1 + 2000*0.01) / (0.1+0.01) = (100+20)/0.11 = 1090.909...
	replies := []burst.Reply{
		{ArrivalUS: 1000.0, SignalDBFS: -20.0, HasSignal: true},
		{ArrivalUS: 2000.0, SignalDBFS: -40.0, HasSignal: true},
	}
	c, sig, _ := burst.Centroid(replies)
	want := (1000.0*math.Pow(10, -20.0/20) + 2000.0*math.Pow(10, -40.0/20)) /
		(math.Pow(10, -20.0/20) + math.Pow(10, -40.0/20))
	if math.Abs(c-want) > 1e-6 {
		t.Errorf("centroid: got %v want %v", c, want)
	}
	if sig == nil || math.Abs(*sig-(-20.0)) > 1e-9 {
		t.Errorf("signal: got %v want -20.0", sig)
	}
}

func TestCentroidMixedSignal(t *testing.T) {
	// One reply has signal, one does not. No-signal reply gets w=1.0.
	replies := []burst.Reply{
		{ArrivalUS: 0.0, HasSignal: false},           // w=1.0
		{ArrivalUS: 1000.0, SignalDBFS: 0.0, HasSignal: true}, // w=10^0=1.0
	}
	c, sig, _ := burst.Centroid(replies)
	if math.Abs(c-500.0) > 1e-9 {
		t.Errorf("centroid: got %v want 500.0", c)
	}
	if sig == nil || *sig != 0.0 {
		t.Errorf("signal: got %v want 0.0", sig)
	}
}

func TestCentroidEmpty(t *testing.T) {
	c, sig, n := burst.Centroid(nil)
	if c != 0 || sig != nil || n != 0 {
		t.Errorf("empty centroid: got c=%v sig=%v n=%v", c, sig, n)
	}
}

// --- Builder tests ---

func TestBuilderNoFire(t *testing.T) {
	b := burst.NewBuilder(1)
	// Three replies within the gap — no burst fires.
	fired := b.OnEvent(0.0, 0xAA, nil, burstGapUS)
	fired = append(fired, b.OnEvent(100.0, 0xAA, nil, burstGapUS)...)
	fired = append(fired, b.OnEvent(200.0, 0xAA, nil, burstGapUS)...)
	if len(fired) != 0 {
		t.Errorf("expected 0 fired, got %d", len(fired))
	}
}

func TestBuilderFiresOnGap(t *testing.T) {
	b := burst.NewBuilder(2)
	b.OnEvent(0.0, 0xBB, nil, burstGapUS)
	b.OnEvent(100.0, 0xBB, nil, burstGapUS)

	// Arrival 300,000µs later — exceeds BURST_GAP_US.
	fired := b.OnEvent(300_000.0, 0xBB, nil, burstGapUS)
	if len(fired) != 1 {
		t.Fatalf("expected 1 fired burst, got %d", len(fired))
	}
	if fired[0].ICAO != 0xBB {
		t.Errorf("ICAO: got %v want 0xBB", fired[0].ICAO)
	}
	if fired[0].NReplies != 2 {
		t.Errorf("NReplies: got %d want 2", fired[0].NReplies)
	}
	// Centroid of two equal-weight replies at 0 and 100 = 50.
	if math.Abs(fired[0].CentroidUS-50.0) > 1e-9 {
		t.Errorf("CentroidUS: got %v want 50.0", fired[0].CentroidUS)
	}
}

func TestBuilderMultiICAO(t *testing.T) {
	b := burst.NewBuilder(3)
	// Two ICAOs, replies within gap.
	b.OnEvent(0.0, 0xAA, nil, burstGapUS)
	b.OnEvent(50.0, 0xBB, nil, burstGapUS)
	b.OnEvent(100.0, 0xAA, nil, burstGapUS)

	// Large gap: both ICAOs should fire.
	fired := b.OnEvent(400_000.0, 0xCC, nil, burstGapUS)
	if len(fired) != 2 {
		t.Fatalf("expected 2 fired bursts, got %d", len(fired))
	}
	// Centroids sorted ascending.
	if fired[0].CentroidUS > fired[1].CentroidUS {
		t.Errorf("bursts not sorted by centroid")
	}
}

func TestBuilderFiredCentroidWithSignal(t *testing.T) {
	b := burst.NewBuilder(4)
	sig1 := -30.0
	sig2 := -20.0
	b.OnEvent(0.0, 0xDD, &sig1, burstGapUS)
	b.OnEvent(100.0, 0xDD, &sig2, burstGapUS)

	fired := b.OnEvent(400_000.0, 0xEE, nil, burstGapUS)
	if len(fired) != 1 {
		t.Fatalf("expected 1 fired burst, got %d", len(fired))
	}
	fb := fired[0]
	// Strongest signal should be -20 dBFS.
	if fb.SignalDBFS == nil || math.Abs(*fb.SignalDBFS-(-20.0)) > 1e-9 {
		t.Errorf("SignalDBFS: got %v want -20.0", fb.SignalDBFS)
	}
	// Centroid: w1=10^(-30/20)=0.03162, w2=10^(-20/20)=0.1
	w1 := math.Pow(10, -30.0/20)
	w2 := math.Pow(10, -20.0/20)
	want := (0.0*w1 + 100.0*w2) / (w1 + w2)
	if math.Abs(fb.CentroidUS-want) > 1e-6 {
		t.Errorf("CentroidUS: got %v want %v", fb.CentroidUS, want)
	}
}

func TestBuilderReset(t *testing.T) {
	b := burst.NewBuilder(5)
	b.OnEvent(0.0, 0xFF, nil, burstGapUS)
	b.Reset()
	// After reset, no burst fires on gap because state is cleared.
	fired := b.OnEvent(400_000.0, 0xFF, nil, burstGapUS)
	if len(fired) != 0 {
		t.Errorf("expected 0 after reset, got %d", len(fired))
	}
}

func TestBuilderActiveICAOs(t *testing.T) {
	b := burst.NewBuilder(6)
	if b.ActiveICAOs() != 0 {
		t.Errorf("expected 0 active ICAOs, got %d", b.ActiveICAOs())
	}
	b.OnEvent(0.0, 0x11, nil, burstGapUS)
	b.OnEvent(0.0, 0x22, nil, burstGapUS)
	if b.ActiveICAOs() != 2 {
		t.Errorf("expected 2 active ICAOs, got %d", b.ActiveICAOs())
	}
}
