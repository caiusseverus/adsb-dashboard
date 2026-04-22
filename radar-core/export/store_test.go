package export

import (
	"testing"
	"time"
)

func TestStoreSnapshotFiltersByIIDAndRetention(t *testing.T) {
	store := NewStore(8, 8, 8, 60.0)
	now := float64(time.Now().UnixNano()) / float64(time.Second)
	store.RecordTrackObservation(TrackObservation{IID: 1, ICAO: 0xABCDEF, WallTS: now})
	store.RecordTrackObservation(TrackObservation{IID: 2, ICAO: 0x123456, WallTS: now})
	store.RecordEvidenceEvent(EvidenceEvent{IID: 1, ICAO: 0xABCDEF, WallTS: now})
	store.RecordSweepFrame(SweepFrame{IID: 2, FrameIndex: 7, ExportedAt: now})

	iid := uint8(1)
	track, evidence, frames := store.Snapshot(&iid)
	if len(track) != 1 || track[0].IID != 1 {
		t.Fatalf("unexpected track snapshot: %+v", track)
	}
	if len(evidence) != 1 || evidence[0].IID != 1 {
		t.Fatalf("unexpected evidence snapshot: %+v", evidence)
	}
	if len(frames) != 0 {
		t.Fatalf("unexpected filtered frames: %+v", frames)
	}
}
