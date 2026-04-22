package export

import (
	"sync"
	"time"
)

// Store retains bounded exported DTOs for snapshot and reconnect reconciliation.
type Store struct {
	mu                sync.Mutex
	trackObservations []TrackObservation
	evidenceEvents    []EvidenceEvent
	sweepFrames       []SweepFrame
	maxTrack          int
	maxEvidence       int
	maxFrames         int
	retentionS        float64
}

func NewStore(maxTrack, maxEvidence, maxFrames int, retentionS float64) *Store {
	return &Store{
		maxTrack:    maxTrack,
		maxEvidence: maxEvidence,
		maxFrames:   maxFrames,
		retentionS:  retentionS,
	}
}

func (s *Store) RecordTrackObservation(obs TrackObservation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(time.Now())
	s.trackObservations = append(s.trackObservations, obs)
	s.trackObservations = trimTrackObservations(s.trackObservations, s.maxTrack)
}

func (s *Store) RecordEvidenceEvent(evt EvidenceEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(time.Now())
	s.evidenceEvents = append(s.evidenceEvents, evt)
	s.evidenceEvents = trimEvidenceEvents(s.evidenceEvents, s.maxEvidence)
}

func (s *Store) RecordSweepFrame(frame SweepFrame) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(time.Now())
	s.sweepFrames = append(s.sweepFrames, frame)
	s.sweepFrames = trimSweepFrames(s.sweepFrames, s.maxFrames)
}

func (s *Store) ResetIID(iid uint8) {
	s.mu.Lock()
	defer s.mu.Unlock()
	filteredTrack := s.trackObservations[:0]
	for _, entry := range s.trackObservations {
		if entry.IID != iid {
			filteredTrack = append(filteredTrack, entry)
		}
	}
	s.trackObservations = filteredTrack
	filteredEvidence := s.evidenceEvents[:0]
	for _, entry := range s.evidenceEvents {
		if entry.IID != iid {
			filteredEvidence = append(filteredEvidence, entry)
		}
	}
	s.evidenceEvents = filteredEvidence
	filteredFrames := s.sweepFrames[:0]
	for _, entry := range s.sweepFrames {
		if entry.IID != iid {
			filteredFrames = append(filteredFrames, entry)
		}
	}
	s.sweepFrames = filteredFrames
}

func (s *Store) Snapshot(targetIID *uint8) ([]TrackObservation, []EvidenceEvent, []SweepFrame) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(time.Now())
	tracks := filterTrackObservations(s.trackObservations, targetIID)
	evidence := filterEvidenceEvents(s.evidenceEvents, targetIID)
	frames := filterSweepFrames(s.sweepFrames, targetIID)
	return tracks, evidence, frames
}

func (s *Store) pruneLocked(now time.Time) {
	if s.retentionS <= 0 {
		return
	}
	cutoff := now.Add(-time.Duration(s.retentionS * float64(time.Second))).UnixNano()
	s.trackObservations = pruneTrackObservations(s.trackObservations, cutoff)
	s.evidenceEvents = pruneEvidenceEvents(s.evidenceEvents, cutoff)
	s.sweepFrames = pruneSweepFrames(s.sweepFrames, cutoff)
}

func trimTrackObservations(entries []TrackObservation, max int) []TrackObservation {
	if max <= 0 {
		return entries
	}
	if len(entries) <= max {
		return entries
	}
	return append(entries[:0], entries[len(entries)-max:]...)
}

func trimEvidenceEvents(entries []EvidenceEvent, max int) []EvidenceEvent {
	if max <= 0 {
		return entries
	}
	if len(entries) <= max {
		return entries
	}
	return append(entries[:0], entries[len(entries)-max:]...)
}

func trimSweepFrames(entries []SweepFrame, max int) []SweepFrame {
	if max <= 0 {
		return entries
	}
	if len(entries) <= max {
		return entries
	}
	return append(entries[:0], entries[len(entries)-max:]...)
}

func pruneTrackObservations(entries []TrackObservation, cutoffNS int64) []TrackObservation {
	idx := 0
	for idx < len(entries) && int64(entries[idx].WallTS*float64(time.Second)) < cutoffNS {
		idx++
	}
	if idx <= 0 {
		return entries
	}
	return append(entries[:0], entries[idx:]...)
}

func pruneEvidenceEvents(entries []EvidenceEvent, cutoffNS int64) []EvidenceEvent {
	idx := 0
	for idx < len(entries) && int64(entries[idx].WallTS*float64(time.Second)) < cutoffNS {
		idx++
	}
	if idx <= 0 {
		return entries
	}
	return append(entries[:0], entries[idx:]...)
}

func pruneSweepFrames(entries []SweepFrame, cutoffNS int64) []SweepFrame {
	idx := 0
	for idx < len(entries) && int64(entries[idx].ExportedAt*float64(time.Second)) < cutoffNS {
		idx++
	}
	if idx <= 0 {
		return entries
	}
	return append(entries[:0], entries[idx:]...)
}

func filterTrackObservations(entries []TrackObservation, targetIID *uint8) []TrackObservation {
	out := make([]TrackObservation, 0, len(entries))
	for _, entry := range entries {
		if targetIID != nil && entry.IID != *targetIID {
			continue
		}
		out = append(out, entry)
	}
	return out
}

func filterEvidenceEvents(entries []EvidenceEvent, targetIID *uint8) []EvidenceEvent {
	out := make([]EvidenceEvent, 0, len(entries))
	for _, entry := range entries {
		if targetIID != nil && entry.IID != *targetIID {
			continue
		}
		out = append(out, entry)
	}
	return out
}

func filterSweepFrames(entries []SweepFrame, targetIID *uint8) []SweepFrame {
	out := make([]SweepFrame, 0, len(entries))
	for _, entry := range entries {
		if targetIID != nil && entry.IID != *targetIID {
			continue
		}
		out = append(out, entry)
	}
	return out
}
