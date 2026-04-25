package iid

// multisync_branch.go — persistent long-term branch / phase estimator (Layer 3).
//
// The phase-branch step (multisync_phase.go) returns one local candidate
// anchor + offset per fit window. Those local candidates are noisy and
// occasionally pick a wrong branch. This file maintains a small bounded set
// of competing branch tracks. Each window's local candidate is matched to
// existing tracks by phase proximity; matches reinforce a track, contradictions
// decay it without immediate removal. Promotion to authoritative state
// (Layer 4) consumes accumulated confidence rather than the current window.
//
// Design notes:
//   - At most `branchMaxTracks` tracks are retained. Eviction prefers the
//     lowest-confidence track that hasn't been matched recently.
//   - Match by circular phase proximity (≤ branchTrackMatchDeg). Anchor ICAO
//     is recorded but not part of the match key — different anchors that
//     produce the same phase offset belong to the same physical branch.
//   - Contradictory windows (no track matches) reduce all tracks' confidence
//     slightly to age them. The new offset spawns a competing track.
//   - The dominant track is the highest-confidence one; it is exposed via
//     LongTermBranchAnchorICAO / LongTermBranchOffsetDeg / BranchEstimatorConfidence.

import "math"

const (
	branchMaxTracks            = 4
	branchTrackMatchDeg        = 35.0   // ≤ this circular phase delta = matches an existing track
	branchTrackConfidenceStep  = 0.08   // confidence step on supported window
	branchTrackContradictDecay = 0.5    // confidence multiplier when local contradicts a track
	branchTrackAgingPenalty    = 0.02   // per-window decay applied to non-matching tracks
	branchTrackMaxConfidence   = 1.0
	branchTrackMinConfidence   = 0.0
)

// BranchEstimate holds persistent state for one candidate phase branch.
// It survives across fit windows; promotion to refined authority requires
// accumulated confidence rather than the current window's validation.
type BranchEstimate struct {
	OffsetDeg                   float64
	AnchorICAO                  *uint32 // representative anchor — last one to support this branch
	SupportingICAOs             map[uint32]struct{}
	Confidence                  float64
	LastSeenTS                  float64
	ConsecutiveSupportedWindows int
	ContradictedWindows         int
}

// updateBranchTracks ingests this window's local candidate (anchor + phase offset)
// and updates the persistent branch-track set. It reports the matched track index
// (-1 if a new track was spawned), the post-update dominant track index, and
// total competitor count.
//
// Inputs:
//   - localAnchor: anchor ICAO chosen this window (may be nil → no candidate)
//   - localOffsetDeg: candidate phase offset for the window
//   - localQuality: 0..1 fit quality (scales confidence updates)
//   - nowUnix: current wall-clock time
//
// On `localAnchor == nil` no track is updated; existing tracks decay slightly
// to age them. This preserves long-term state through gaps in the local fit.
func (ms *MultiSyncSolver) updateBranchTracks(
	localAnchor *uint32, localOffsetDeg, localQuality, nowUnix float64,
) {
	// No local candidate — age existing tracks but do not modify the dominant
	// selection in a destructive way.
	if localAnchor == nil {
		for _, tr := range ms.BranchTracks {
			tr.Confidence = math.Max(branchTrackMinConfidence,
				tr.Confidence-branchTrackAgingPenalty)
		}
		ms.recomputeDominantBranch()
		return
	}

	// Match: find the closest existing track whose phase offset is within the
	// match radius.
	matchIdx := -1
	bestDelta := math.MaxFloat64
	for i, tr := range ms.BranchTracks {
		delta := math.Abs(circularDiff(localOffsetDeg, tr.OffsetDeg))
		if delta <= branchTrackMatchDeg && delta < bestDelta {
			bestDelta = delta
			matchIdx = i
		}
	}

	if matchIdx >= 0 {
		tr := ms.BranchTracks[matchIdx]
		// EMA the offset toward the local measurement; gain scales with local
		// quality so noisy windows do not dominate the consensus.
		gain := 0.15 * clamp(localQuality, 0, 1)
		tr.OffsetDeg = circularEMAUpdate(tr.OffsetDeg, localOffsetDeg, gain)
		tr.Confidence = math.Min(branchTrackMaxConfidence,
			tr.Confidence+branchTrackConfidenceStep*clamp(localQuality, 0, 1))
		tr.ConsecutiveSupportedWindows++
		tr.LastSeenTS = nowUnix
		v := *localAnchor
		tr.AnchorICAO = &v
		if tr.SupportingICAOs == nil {
			tr.SupportingICAOs = make(map[uint32]struct{}, 4)
		}
		tr.SupportingICAOs[*localAnchor] = struct{}{}

		// Decay competitors slightly so reinforced tracks stand out.
		for i, other := range ms.BranchTracks {
			if i == matchIdx {
				continue
			}
			other.Confidence = math.Max(branchTrackMinConfidence,
				other.Confidence-branchTrackAgingPenalty)
			other.ContradictedWindows++
		}
	} else {
		// Contradiction: every existing track loses a slice of confidence;
		// spawn a competing track for the new offset.
		for _, tr := range ms.BranchTracks {
			tr.Confidence *= branchTrackContradictDecay
			tr.ContradictedWindows++
			tr.ConsecutiveSupportedWindows = 0
		}
		ms.spawnBranchTrack(localAnchor, localOffsetDeg, localQuality, nowUnix)
	}

	ms.evictWeakestBranchTracks()
	ms.recomputeDominantBranch()
}

// spawnBranchTrack appends a new track for an unrecognised local offset.
// If the cap is already at branchMaxTracks the weakest track is evicted first.
func (ms *MultiSyncSolver) spawnBranchTrack(
	anchor *uint32, offsetDeg, localQuality, nowUnix float64,
) {
	v := *anchor
	tr := &BranchEstimate{
		OffsetDeg:                   offsetDeg,
		AnchorICAO:                  &v,
		SupportingICAOs:             map[uint32]struct{}{*anchor: {}},
		Confidence:                  branchTrackConfidenceStep * clamp(localQuality, 0, 1),
		LastSeenTS:                  nowUnix,
		ConsecutiveSupportedWindows: 1,
		ContradictedWindows:         0,
	}
	ms.BranchTracks = append(ms.BranchTracks, tr)
}

// evictWeakestBranchTracks trims the track set to branchMaxTracks by removing
// the lowest-confidence (and, on ties, oldest-LastSeenTS) track first.
func (ms *MultiSyncSolver) evictWeakestBranchTracks() {
	for len(ms.BranchTracks) > branchMaxTracks {
		worstIdx := 0
		for i, tr := range ms.BranchTracks {
			cur := ms.BranchTracks[worstIdx]
			if tr.Confidence < cur.Confidence ||
				(tr.Confidence == cur.Confidence && tr.LastSeenTS < cur.LastSeenTS) {
				worstIdx = i
			}
		}
		ms.BranchTracks = append(ms.BranchTracks[:worstIdx], ms.BranchTracks[worstIdx+1:]...)
	}
}

// recomputeDominantBranch updates LongTermBranch* fields from the highest-
// confidence track in BranchTracks.
func (ms *MultiSyncSolver) recomputeDominantBranch() {
	if len(ms.BranchTracks) == 0 {
		ms.LongTermBranchAnchorICAO = nil
		ms.LongTermBranchOffsetDeg = 0
		ms.BranchEstimatorConfidence = 0
		ms.BranchConsistentWindows = 0
		ms.BranchContradictionWindows = 0
		ms.BranchCompetitorCount = 0
		return
	}
	domIdx := 0
	for i, tr := range ms.BranchTracks {
		cur := ms.BranchTracks[domIdx]
		if tr.Confidence > cur.Confidence ||
			(tr.Confidence == cur.Confidence && tr.LastSeenTS > cur.LastSeenTS) {
			domIdx = i
		}
	}
	dom := ms.BranchTracks[domIdx]
	if dom.AnchorICAO != nil {
		v := *dom.AnchorICAO
		ms.LongTermBranchAnchorICAO = &v
	} else {
		ms.LongTermBranchAnchorICAO = nil
	}
	ms.LongTermBranchOffsetDeg = dom.OffsetDeg
	ms.BranchEstimatorConfidence = dom.Confidence
	ms.BranchConsistentWindows = dom.ConsecutiveSupportedWindows
	ms.BranchContradictionWindows = dom.ContradictedWindows
	ms.BranchCompetitorCount = len(ms.BranchTracks) - 1
}

// circularEMAUpdate blends `current` toward `target` along the shorter arc on
// a 0–360° circle, with the given gain. The result is normalised to [0, 360).
func circularEMAUpdate(current, target, gain float64) float64 {
	delta := circularDiff(target, current)
	next := math.Mod(current+gain*delta, 360.0)
	if next < 0 {
		next += 360.0
	}
	return next
}
