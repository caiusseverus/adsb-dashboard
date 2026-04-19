package iid

import (
	"math"
	"sort"
)

// Reference aircraft selection constants (match sweep.py _select_reference_from_live_bursts).
const (
	minBurstsForRef = 4
	recencyPeriods  = 5    // candidate must have burst within this many periods
	hysteresis      = 0.25 // challenger must beat current by this fraction
)

// RefCandidate holds scoring data for one potential reference ICAO.
type RefCandidate struct {
	ICAO  uint32
	Score float64
}

// SelectReference picks the reference aircraft for an IID from its burst records.
//
// It groups records by ICAO, computes inter-burst interval regularity, and
// returns the ICAO with the best (lowest) score. Returns 0 if no candidate
// has sufficient data.
//
// currentRef is the current reference ICAO (0 if none); hysteresis prevents
// unnecessary switches when two candidates are close.
func SelectReference(records []BurstRecord, periodS float64, currentRef uint32, nowUS float64) uint32 {
	if periodS <= 0 || len(records) == 0 {
		return 0
	}

	// Group centroids by ICAO.
	byICAO := map[uint32][]float64{}
	for _, r := range records {
		if r.ICAO == 0 {
			continue
		}
		byICAO[r.ICAO] = append(byICAO[r.ICAO], r.CentroidUS)
	}

	recencyThresholdUS := periodS * float64(recencyPeriods) * 1_000_000.0

	candidates := map[uint32]float64{}
	for icao, centroids := range byICAO {
		if len(centroids) < minBurstsForRef {
			continue
		}
		// Recency check.
		lastC := maxSlice(centroids)
		if nowUS > 0 && (nowUS-lastC) > recencyThresholdUS {
			continue
		}

		sort.Float64s(centroids)
		var intervalsS []float64
		for i := 0; i < len(centroids)-1; i++ {
			iv := (centroids[i+1] - centroids[i]) / 1_000_000.0
			if iv > 0.5 && iv < 30.0 {
				intervalsS = append(intervalsS, iv)
			}
		}
		if len(intervalsS) < 2 {
			continue
		}

		medPeriod := median(intervalsS)
		stdDev := stdev(intervalsS)

		periodDev := math.Abs(medPeriod-periodS) / periodS
		periodTightness := stdDev / periodS
		countFactor := 1.0 / math.Max(float64(len(centroids)), 1)

		candidates[icao] = periodDev + periodTightness + countFactor
	}

	if len(candidates) == 0 {
		return 0
	}

	// Find best (lowest score).
	bestICAO := uint32(0)
	bestScore := math.Inf(1)
	for icao, score := range candidates {
		if score < bestScore {
			bestScore = score
			bestICAO = icao
		}
	}

	// Hysteresis: only displace current ref if challenger is meaningfully better.
	if currentRef != 0 {
		if currentScore, ok := candidates[currentRef]; ok {
			if bestScore > currentScore*(1.0-hysteresis) {
				bestICAO = currentRef
			}
		}
	}

	return bestICAO
}

func maxSlice(vals []float64) float64 {
	m := vals[0]
	for _, v := range vals[1:] {
		if v > m {
			m = v
		}
	}
	return m
}
