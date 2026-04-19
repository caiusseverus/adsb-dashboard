package burst

import (
	"sort"
	"time"
)

// FiredBurst is emitted when a burst window closes.
type FiredBurst struct {
	IID        uint8
	ICAO       uint32
	CentroidUS float64
	NReplies   int
	SignalDBFS *float64 // nil if no signal in any reply
	FiredAt    time.Time
}

// pendingBurst tracks in-flight replies for one (IID, ICAO) pair.
type pendingBurst struct {
	icao          uint32
	replies       []Reply
	lastArrivalUS float64
}

// Builder accumulates replies for one IID and fires bursts on gap detection.
// Not safe for concurrent use; the caller serialises access per IID.
type Builder struct {
	iid     uint8
	pending map[uint32]*pendingBurst // keyed by ICAO
}

// NewBuilder creates a Builder for the given IID.
func NewBuilder(iid uint8) *Builder {
	return &Builder{
		iid:     iid,
		pending: make(map[uint32]*pendingBurst),
	}
}

// OnEvent processes one reply. It returns any bursts that fired as a result of
// the gap check triggered by this arrival.
//
// Algorithm mirrors radar_burst_processor_process() in radar_burst.c:
//  1. For every pending burst whose lastArrivalUS is more than burstGapUS behind
//     the new arrival, fire it (compute centroid, clear replies).
//  2. Append the new reply to the pending burst for this ICAO.
func (b *Builder) OnEvent(arrivalUS float64, icao uint32, signalDBFS *float64, burstGapUS float64) []FiredBurst {
	var fired []FiredBurst

	// Step 1: check all pending bursts for gap expiry.
	for _, pb := range b.pending {
		if len(pb.replies) > 0 && (arrivalUS-pb.lastArrivalUS) > burstGapUS {
			centroidUS, sig, n := Centroid(pb.replies)
			fired = append(fired, FiredBurst{
				IID:        b.iid,
				ICAO:       pb.icao,
				CentroidUS: centroidUS,
				NReplies:   n,
				SignalDBFS: sig,
				FiredAt:    time.Now(),
			})
			pb.replies = pb.replies[:0]
			pb.lastArrivalUS = arrivalUS
		}
	}

	// Sort fired bursts by centroid ascending (matches C qsort behaviour).
	if len(fired) > 1 {
		sort.Slice(fired, func(i, j int) bool {
			return fired[i].CentroidUS < fired[j].CentroidUS
		})
	}

	// Step 2: append reply to pending burst for this ICAO.
	pb, ok := b.pending[icao]
	if !ok {
		pb = &pendingBurst{icao: icao}
		b.pending[icao] = pb
	}
	r := Reply{ArrivalUS: arrivalUS, HasSignal: signalDBFS != nil}
	if signalDBFS != nil {
		r.SignalDBFS = *signalDBFS
	}
	pb.replies = append(pb.replies, r)
	pb.lastArrivalUS = arrivalUS

	return fired
}

// Reset clears all pending state for this IID.
func (b *Builder) Reset() {
	b.pending = make(map[uint32]*pendingBurst)
}

// ActiveICAOs returns the number of ICAOs with pending (unfired) replies.
func (b *Builder) ActiveICAOs() int {
	count := 0
	for _, pb := range b.pending {
		if len(pb.replies) > 0 {
			count++
		}
	}
	return count
}
