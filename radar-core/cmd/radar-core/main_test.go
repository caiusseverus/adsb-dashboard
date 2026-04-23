package main

import (
	"testing"
	"time"

	"github.com/caiusseverus/adsb-dashboard/radar-core/burst"
	"github.com/caiusseverus/adsb-dashboard/radar-core/iid"
)

func TestExportSyncEligibilitySeparatesCompactAndRefinedSemantics(t *testing.T) {
	e := newEngine()
	s := iid.NewIIDState(7)
	s.Sync = iid.NewSyncState(7, 4.0, 0.0, 45.0, 0.9)
	s.LastRotationModel = &iid.RotationModel{
		Family: &iid.ICAOFamily{
			FoldedICAOs: map[uint32]struct{}{0xABC001: {}},
		},
	}
	s.MultiSync.Present = true
	s.MultiSync.Usable = false

	now := time.Now().Unix()
	e.positions.Update(0xABC001, 51.5, -0.1, nil, float64(now))
	f := &burst.FiredBurst{
		IID:        7,
		ICAO:       0xABC001,
		CentroidUS: 1234567.0,
		NReplies:   6,
	}

	e.recordObservationExports(s, f)
	tracks, evidence, _ := e.exports.Snapshot(nil)
	if len(tracks) != 1 || len(evidence) != 1 {
		t.Fatalf("expected one exported track and evidence event, got tracks=%d evidence=%d", len(tracks), len(evidence))
	}

	track := tracks[0]
	if !track.SyncEligible || !track.CompactSyncEligible {
		t.Fatalf("expected compact/bootstrap sync eligibility to be exported on track, got sync_eligible=%v compact=%v", track.SyncEligible, track.CompactSyncEligible)
	}
	if !track.RefinedSyncPresent {
		t.Fatal("expected refined sync presence to be exported on track")
	}
	if track.RefinedSyncUsable {
		t.Fatal("expected refined sync usable to remain false when multi-sync is present but not usable")
	}

	ev := evidence[0]
	if !ev.SyncEligible || !ev.CompactSyncEligible {
		t.Fatalf("expected compact/bootstrap sync eligibility to be exported on evidence, got sync_eligible=%v compact=%v", ev.SyncEligible, ev.CompactSyncEligible)
	}
	if !ev.RefinedSyncPresent {
		t.Fatal("expected refined sync presence to be exported on evidence")
	}
	if ev.RefinedSyncUsable {
		t.Fatal("expected refined sync usable to remain false on evidence")
	}
}
