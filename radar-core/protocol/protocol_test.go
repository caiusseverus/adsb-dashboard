package protocol_test

import (
	"bytes"
	"math"
	"reflect"
	"testing"

	"github.com/caiusseverus/adsb-dashboard/radar-core/protocol"
)

// roundTrip encodes msg then decodes and returns the result.
func roundTrip(t *testing.T, msg interface{}) interface{} {
	t.Helper()
	payload, err := protocol.Encode(msg)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	got, err := protocol.Decode(payload)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	return got
}

func pf32(v float32) *float32 { return &v }
func pf64(v float64) *float64 { return &v }
func pu32(v uint32) *uint32   { return &v }
func pi32(v int32) *int32     { return &v }

func TestRadarEventRoundTrip(t *testing.T) {
	orig := &protocol.RadarEvent{
		MsgType:    protocol.MsgRadarEvent,
		ArrivalUS:  123456789.5,
		IID:        7,
		ICAO:       0xABCDEF,
		SignalDBFS: pf32(-45.5),
	}
	got := roundTrip(t, orig).(*protocol.RadarEvent)
	if got.ArrivalUS != orig.ArrivalUS {
		t.Errorf("ArrivalUS: got %v want %v", got.ArrivalUS, orig.ArrivalUS)
	}
	if got.IID != orig.IID {
		t.Errorf("IID: got %v want %v", got.IID, orig.IID)
	}
	if got.ICAO != orig.ICAO {
		t.Errorf("ICAO: got %v want %v", got.ICAO, orig.ICAO)
	}
	if got.SignalDBFS == nil || *got.SignalDBFS != *orig.SignalDBFS {
		t.Errorf("SignalDBFS: got %v want %v", got.SignalDBFS, orig.SignalDBFS)
	}
}

func TestRadarEventNilSignal(t *testing.T) {
	orig := &protocol.RadarEvent{
		MsgType:   protocol.MsgRadarEvent,
		ArrivalUS: 1.0,
		IID:       0,
		ICAO:      0x123456,
	}
	got := roundTrip(t, orig).(*protocol.RadarEvent)
	if got.SignalDBFS != nil {
		t.Errorf("expected nil SignalDBFS, got %v", *got.SignalDBFS)
	}
}

func TestPositionUpdateRoundTrip(t *testing.T) {
	orig := &protocol.PositionUpdate{
		MsgType: protocol.MsgPositionUpdate,
		ICAO:    0x400F3C,
		Lat:     51.4775,
		Lon:     -0.4614,
		AltFt:   pi32(35000),
		TS:      1714000000.123,
	}
	got := roundTrip(t, orig).(*protocol.PositionUpdate)
	if math.Abs(got.Lat-orig.Lat) > 1e-9 {
		t.Errorf("Lat: got %v want %v", got.Lat, orig.Lat)
	}
	if math.Abs(got.Lon-orig.Lon) > 1e-9 {
		t.Errorf("Lon: got %v want %v", got.Lon, orig.Lon)
	}
	if got.AltFt == nil || *got.AltFt != *orig.AltFt {
		t.Errorf("AltFt: got %v want %v", got.AltFt, orig.AltFt)
	}
}

func TestConfigUpdateRoundTrip(t *testing.T) {
	orig := &protocol.ConfigUpdate{
		MsgType: protocol.MsgConfigUpdate,
		Key:     "BURST_RECORD_MAX_AGE_S",
		Value:   float64(120),
	}
	got := roundTrip(t, orig).(*protocol.ConfigUpdate)
	if got.Key != orig.Key {
		t.Errorf("Key: got %v want %v", got.Key, orig.Key)
	}
}

func TestSnapshotReqRoundTrip(t *testing.T) {
	orig := &protocol.SnapshotReq{
		MsgType: protocol.MsgSnapshotReq,
		ReqID:   42,
		Scope:   "iid:3",
	}
	got := roundTrip(t, orig).(*protocol.SnapshotReq)
	if got.ReqID != orig.ReqID || got.Scope != orig.Scope {
		t.Errorf("got %+v want %+v", got, orig)
	}
}

func TestResetIIDRoundTrip(t *testing.T) {
	orig := &protocol.ResetIID{MsgType: protocol.MsgResetIID, IID: 3}
	got := roundTrip(t, orig).(*protocol.ResetIID)
	if got.IID != orig.IID {
		t.Errorf("IID: got %v want %v", got.IID, orig.IID)
	}
}

func TestBurstFiredRoundTrip(t *testing.T) {
	orig := &protocol.BurstFired{
		MsgType:        protocol.MsgBurstFired,
		IID:            2,
		ICAO:           0x3C4B4A,
		CentroidUS:     9876543210.75,
		NReplies:       5,
		SignalDBFS:     pf32(-33.0),
		Lat:            pf64(51.5),
		Lon:            pf64(-0.1),
		BearingDeg:     pf32(275.3),
		RangeNM:        pf32(42.1),
		PosAgeS:        pf32(1.2),
		DominantFamily: true,
	}
	got := roundTrip(t, orig).(*protocol.BurstFired)
	if got.CentroidUS != orig.CentroidUS {
		t.Errorf("CentroidUS: got %v want %v", got.CentroidUS, orig.CentroidUS)
	}
	if !got.DominantFamily {
		t.Errorf("bool field lost: DominantFamily=%v", got.DominantFamily)
	}
	if got.Lat == nil || math.Abs(*got.Lat-*orig.Lat) > 1e-9 {
		t.Errorf("Lat: got %v want %v", got.Lat, orig.Lat)
	}
}

func TestFMFrameResultRoundTrip(t *testing.T) {
	orig := &protocol.FMFrameResult{
		MsgType:                  protocol.MsgFMFrameResult,
		IID:                      7,
		FrameIndex:               12,
		SweepStartUS:             1234567.5,
		Success:                  true,
		SolveStatus:              "success",
		SolveReason:              "accepted",
		CandidatePosition:        true,
		AccumAccepted:            true,
		RejectionReason:          "accepted",
		AdmissionTier:            "accepted_high_confidence",
		Lat:                      pf64(51.5),
		Lon:                      pf64(-0.2),
		CEPM:                     pf64(950.0),
		NContributingArcs:        8,
		AzimuthSpreadDeg:         87.5,
		Weight:                   2.75,
		PairwiseRMSDeg:           14.2,
		ClusterMemberCount:       7,
		SecondClusterMemberCount: 3,
		MemberDominanceRatio:     2.1,
		WeightDominanceRatio:     1.7,
		SupportDominanceRatio:    1.6,
		BestClusterSupportScore:  1.4,
		AmbiguitySameLobeBypass:  false,
		ProcessedAt:              1714000000.25,
	}
	got := roundTrip(t, orig).(*protocol.FMFrameResult)
	if got.SweepStartUS != orig.SweepStartUS {
		t.Errorf("SweepStartUS: got %v want %v", got.SweepStartUS, orig.SweepStartUS)
	}
	if got.AzimuthSpreadDeg != orig.AzimuthSpreadDeg {
		t.Errorf("AzimuthSpreadDeg: got %v want %v", got.AzimuthSpreadDeg, orig.AzimuthSpreadDeg)
	}
	if got.Weight != orig.Weight {
		t.Errorf("Weight: got %v want %v", got.Weight, orig.Weight)
	}
}

func TestBurstFiredNoPosition(t *testing.T) {
	orig := &protocol.BurstFired{
		MsgType:    protocol.MsgBurstFired,
		IID:        1,
		ICAO:       0x111111,
		CentroidUS: 1000.0,
		NReplies:   2,
	}
	got := roundTrip(t, orig).(*protocol.BurstFired)
	if got.Lat != nil || got.Lon != nil || got.BearingDeg != nil {
		t.Errorf("expected nil position fields, got lat=%v lon=%v brg=%v", got.Lat, got.Lon, got.BearingDeg)
	}
}

func TestFrameReadyRoundTrip(t *testing.T) {
	orig := &protocol.FrameReady{
		MsgType:      protocol.MsgFrameReady,
		IID:          4,
		FrameIndex:   100,
		PeriodS:      4.008,
		RefICAO:      0xAA1122,
		RefLat:       52.1,
		RefLon:       0.3,
		RefArrivalUS: 9999999.0,
		Quality:      "good",
		Observations: []protocol.FrameObservation{
			{ICAO: 0xBB2233, Lat: 51.9, Lon: 0.1, ArrivalUS: 10000020.5, NReplies: 3, PosAgeS: 0.5},
			{ICAO: 0xCC3344, Lat: 52.3, Lon: 0.5, ArrivalUS: 10000120.0, NReplies: 4, PosAgeS: 1.1},
		},
	}
	got := roundTrip(t, orig).(*protocol.FrameReady)
	if got.FrameIndex != orig.FrameIndex {
		t.Errorf("FrameIndex: got %v want %v", got.FrameIndex, orig.FrameIndex)
	}
	if got.Quality != orig.Quality {
		t.Errorf("Quality: got %v want %v", got.Quality, orig.Quality)
	}
	if len(got.Observations) != len(orig.Observations) {
		t.Fatalf("Observations len: got %d want %d", len(got.Observations), len(orig.Observations))
	}
	if got.Observations[0].ICAO != orig.Observations[0].ICAO {
		t.Errorf("obs[0].ICAO: got %v want %v", got.Observations[0].ICAO, orig.Observations[0].ICAO)
	}
}

func TestIIDStateRoundTrip(t *testing.T) {
	period := 4.008
	rpm := float32(14.97)
	refICAO := uint32(0xDEAD01)
	orig := &protocol.IIDState{
		MsgType:             protocol.MsgIIDState,
		IID:                 3,
		PeriodS:             &period,
		RPM:                 &rpm,
		Status:              "SINGLE_RADAR",
		RefICAO:             &refICAO,
		SyncQuality:         0.95,
		FitObservationCount: 32,
		SlopeSignConvention: "observed_minus_predicted",
		NBurstRecords:       212,
		LastUpdated:         1714000100.0,
		Revision:            7,
	}
	got := roundTrip(t, orig).(*protocol.IIDState)
	if got.Status != orig.Status {
		t.Errorf("Status: got %v want %v", got.Status, orig.Status)
	}
	if got.Revision != orig.Revision {
		t.Errorf("Revision: got %v want %v", got.Revision, orig.Revision)
	}
	if got.PeriodS == nil || math.Abs(*got.PeriodS-*orig.PeriodS) > 1e-9 {
		t.Errorf("PeriodS: got %v want %v", got.PeriodS, orig.PeriodS)
	}
	if got.FitObservationCount != orig.FitObservationCount {
		t.Errorf("FitObservationCount: got %v want %v", got.FitObservationCount, orig.FitObservationCount)
	}
	if got.SlopeSignConvention != orig.SlopeSignConvention {
		t.Errorf("SlopeSignConvention: got %q want %q", got.SlopeSignConvention, orig.SlopeSignConvention)
	}
}

func TestIIDStateNilFields(t *testing.T) {
	orig := &protocol.IIDState{
		MsgType: protocol.MsgIIDState,
		IID:     5,
		Status:  "INSUFFICIENT_DATA",
	}
	got := roundTrip(t, orig).(*protocol.IIDState)
	if got.PeriodS != nil || got.RPM != nil || got.RefICAO != nil {
		t.Errorf("expected nil optional fields, got period=%v rpm=%v ref=%v", got.PeriodS, got.RPM, got.RefICAO)
	}
}

func TestHealthRoundTrip(t *testing.T) {
	orig := &protocol.Health{
		MsgType:       protocol.MsgHealth,
		UptimeS:       3600.5,
		EventsIn:      1_234_567,
		BurstsFired:   89_000,
		FramesEmitted: 4_200,
		QueueDepth:    12,
		DropCount:     0,
		ActiveIIDs:    3,
	}
	got := roundTrip(t, orig).(*protocol.Health)
	if !reflect.DeepEqual(got, orig) {
		t.Errorf("Health mismatch:\ngot  %+v\nwant %+v", got, orig)
	}
}

// TestFraming verifies that WriteFrame / ReadFrame round-trip arbitrary payloads.
func TestFraming(t *testing.T) {
	cases := [][]byte{
		{},
		{0x01},
		bytes.Repeat([]byte{0xAB}, 1024),
		bytes.Repeat([]byte{0xFF}, 65537),
	}
	for _, payload := range cases {
		var buf bytes.Buffer
		if err := protocol.WriteFrame(&buf, payload); err != nil {
			t.Fatalf("WriteFrame failed: %v", err)
		}
		got, err := protocol.ReadFrame(&buf)
		if err != nil {
			t.Fatalf("ReadFrame failed: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Errorf("framing round-trip mismatch (len %d)", len(payload))
		}
	}
}

func TestFramingTooLarge(t *testing.T) {
	// Manually craft a frame header claiming 2 MiB.
	var buf bytes.Buffer
	tooBig := make([]byte, 4)
	tooBig[0] = 0x00
	tooBig[1] = 0x20 // 2 MiB = 0x00200000
	tooBig[2] = 0x00
	tooBig[3] = 0x00
	buf.Write(tooBig)
	_, err := protocol.ReadFrame(&buf)
	if err != protocol.ErrFrameTooLarge {
		t.Errorf("expected ErrFrameTooLarge, got %v", err)
	}
}
