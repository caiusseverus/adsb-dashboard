package fm

import (
	"math"
	"testing"

	"github.com/caiusseverus/adsb-dashboard/radar-core/protocol"
)

func TestClassifyFrameForAccumulationMatchesPythonReducedArcTier(t *testing.T) {
	r := &SolveResult{CentroidUncertaintyKM: 6.0, NInlierPairCircles: 5, BestClusterSupportScore: 1.2, SupportDominanceRatio: 1.4, PairwiseWeightedRMSDeg: 20.0}
	rej, tier := classifyFrameForAccumulation(r)
	if rej != "" || tier != "accepted_reduced_arc_high_quality" {
		t.Fatalf("got rejection=%q tier=%q", rej, tier)
	}
}

func TestSameLobeMetricsMatchPythonFixture(t *testing.T) {
	closeA := cluster{MeanXKM: 0, MeanYKM: 0, RMSKM: 3, TotalWeight: 10, MemberCount: 24, Contributing: set(1, 2, 3, 4, 5, 6)}
	closeB := cluster{MeanXKM: 2.4, MeanYKM: 1.2, RMSKM: 2.8, TotalWeight: 9.7, MemberCount: 22, Contributing: set(2, 3, 4, 6, 7, 8)}
	farC := cluster{MeanXKM: 52, MeanYKM: 50, RMSKM: 3.2, TotalWeight: 9.5, MemberCount: 20, Contributing: set(30, 31, 32, 33, 34)}
	if !sameLobeMetrics(closeA, closeB, 20).sameLobe || !sameLobeMetrics(closeA, closeB, 20).mergeLike {
		t.Fatalf("close clusters should be same-lobe/merge-like")
	}
	if sameLobeMetrics(closeA, farC, 20).sameLobe || sameLobeMetrics(closeA, farC, 20).mergeLike {
		t.Fatalf("far clusters should be distinct")
	}
}

func TestProcessSyntheticFramePythonParityFixture(t *testing.T) {
	state := NewState(51.0, -1.0, true)
	frame := &protocol.FrameReady{MsgType: protocol.MsgFrameReady, IID: 3, FrameIndex: 1, PeriodS: 10, RefICAO: 0xA00001, RefLat: 51.8, RefLon: -1.6, RefArrivalUS: 1_000_000, Quality: "good"}
	radarLat, radarLon := 51.5, -1.2
	refBearing := bearingDeg(radarLat, radarLon, frame.RefLat, frame.RefLon)
	for _, ac := range []struct {
		icao     uint32
		lat, lon float64
	}{{0xA00002, 51.2, -0.4}, {0xA00003, 50.7, -1.5}, {0xA00004, 51.9, -0.8}, {0xA00005, 50.9, -0.2}} {
		phase := math.Mod(bearingDeg(radarLat, radarLon, ac.lat, ac.lon)-refBearing+360, 360)
		frame.Observations = append(frame.Observations, protocol.FrameObservation{ICAO: ac.icao, Lat: ac.lat, Lon: ac.lon, ArrivalUS: frame.RefArrivalUS + phase/360*frame.PeriodS*1_000_000, NReplies: 4})
	}
	result, fmState := state.ProcessFrame(frame)
	if result == nil || !result.Success || !result.CandidatePosition {
		t.Fatalf("expected candidate solve, got result=%+v", result)
	}
	if result.Lat == nil || math.Abs(*result.Lat-50.8474) > 0.05 {
		t.Fatalf("lat drifted from Python fixture: got %v", result.Lat)
	}
	if result.Lon == nil || math.Abs(*result.Lon-(-0.1180)) > 0.05 {
		t.Fatalf("lon drifted from Python fixture: got %v", result.Lon)
	}
	if fmState == nil || fmState.FramesReachingSolver != 1 || fmState.CandidatePositions != 1 {
		t.Fatalf("bad fm state: %+v", fmState)
	}
}

func set(vals ...int) map[int]struct{} {
	m := map[int]struct{}{}
	for _, v := range vals {
		m[v] = struct{}{}
	}
	return m
}
