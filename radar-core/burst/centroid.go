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
