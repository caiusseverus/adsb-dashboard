// Package burst implements burst assembly and centroid computation.
// The centroid algorithm is a direct port of finalize_pending_burst() in
// backend/native/radar_burst.c so that Go and Python produce identical results.
package burst

import "math"

// Reply is one Mode-S reply within a pending burst window.
type Reply struct {
	ArrivalUS  float64
	SignalDBFS float64
	HasSignal  bool
}

// Diagnostics captures alternate timestamp definitions for one burst.
type Diagnostics struct {
	SimpleCentroidUS   float64
	WeightedCentroidUS *float64
	CentroidDeltaUS    float64
	FirstReplyUS       float64
	StrongestReplyUS   *float64
	MidStrongWindowUS  *float64
	LastReplyUS        float64
	SpanUS             float64
	PeakAmplitude      *float64
}

// Centroid computes the signal-weighted burst centroid from a slice of replies.
//
// Weight per reply: w = 10^(signal_dbfs/20) if signal present, else 1.0.
// Centroid = sum(arrival_us * w) / sum(w).
// Fallback: if total weight is zero (no replies), returns 0.
// Signal: max (least negative) signal_dbfs among all replies; nil if none have signal.
func Centroid(replies []Reply) (centroidUS float64, signalDBFS *float64, nReplies int) {
	nReplies = len(replies)
	if nReplies == 0 {
		return 0, nil, 0
	}

	var totalW, weightedSum float64
	var bestSignal float64
	hasSignal := false

	for _, r := range replies {
		var w float64
		if r.HasSignal {
			w = math.Pow(10.0, r.SignalDBFS/20.0)
			if !hasSignal || r.SignalDBFS > bestSignal {
				bestSignal = r.SignalDBFS
				hasSignal = true
			}
		} else {
			w = 1.0
		}
		weightedSum += r.ArrivalUS * w
		totalW += w
	}

	if totalW > 0 {
		centroidUS = weightedSum / totalW
	} else {
		centroidUS = replies[len(replies)-1].ArrivalUS
	}

	if hasSignal {
		v := bestSignal
		signalDBFS = &v
	}
	return centroidUS, signalDBFS, nReplies
}

// ComputeDiagnostics mirrors Python's refine_burst_center() and
// _compute_burst_timestamp_candidates() helpers for one burst.
func ComputeDiagnostics(replies []Reply) Diagnostics {
	if len(replies) == 0 {
		return Diagnostics{}
	}

	arrivalsUS := make([]float64, 0, len(replies))
	for _, r := range replies {
		arrivalsUS = append(arrivalsUS, r.ArrivalUS)
	}

	simpleCentroid := 0.0
	for _, arrival := range arrivalsUS {
		simpleCentroid += arrival
	}
	simpleCentroid /= float64(len(arrivalsUS))

	var weightedSum, weightSum float64
	var weightedCentroid *float64
	var strongestReplyUS *float64
	var strongestSignal *float64
	strongArrivals := make([]float64, 0, len(replies))
	for _, r := range replies {
		if !r.HasSignal {
			continue
		}
		signal := r.SignalDBFS
		if strongestSignal == nil || signal > *strongestSignal {
			vSignal := signal
			vArrival := r.ArrivalUS
			strongestSignal = &vSignal
			strongestReplyUS = &vArrival
		}
		w := math.Pow(10.0, signal/20.0)
		if w > 0 {
			weightedSum += r.ArrivalUS * w
			weightSum += w
		}
	}
	if len(replies) >= 2 && weightSum > 0 {
		v := weightedSum / weightSum
		weightedCentroid = &v
	}
	if strongestSignal != nil {
		threshold := *strongestSignal - 6.0
		for _, r := range replies {
			if r.HasSignal && r.SignalDBFS >= threshold {
				strongArrivals = append(strongArrivals, r.ArrivalUS)
			}
		}
	}
	var midStrongWindow *float64
	if len(strongArrivals) > 0 {
		minArrival := strongArrivals[0]
		maxArrival := strongArrivals[0]
		for _, arrival := range strongArrivals[1:] {
			if arrival < minArrival {
				minArrival = arrival
			}
			if arrival > maxArrival {
				maxArrival = arrival
			}
		}
		v := (minArrival + maxArrival) / 2.0
		midStrongWindow = &v
	}

	centroidDelta := 0.0
	if weightedCentroid != nil {
		centroidDelta = *weightedCentroid - simpleCentroid
	}

	firstReply := arrivalsUS[0]
	lastReply := arrivalsUS[len(arrivalsUS)-1]
	return Diagnostics{
		SimpleCentroidUS:   simpleCentroid,
		WeightedCentroidUS: weightedCentroid,
		CentroidDeltaUS:    centroidDelta,
		FirstReplyUS:       firstReply,
		StrongestReplyUS:   strongestReplyUS,
		MidStrongWindowUS:  midStrongWindow,
		LastReplyUS:        lastReply,
		SpanUS:             lastReply - firstReply,
		PeakAmplitude:      strongestSignal,
	}
}
