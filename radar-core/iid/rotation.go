package iid

import (
	"math"
	"sort"
)

// --- constants (match sweep.py) ---

const (
	minBursts              = 4
	seriesClusterTolerance = 0.12
)

// --- types ---

type icaoResult struct {
	nBursts          int
	nIntervals       int
	centroidsUS      []float64
	medianPeriodS    float64
	meanPeriodS      float64
	stdS             float64
	allIntervalsS    []float64
	seriesCandidates []seriesCandidate
}

type seriesCandidate struct {
	PeriodS     float64
	NIntervals  int
	StdS        float64
}

type snapResult struct {
	snapRate      float64
	multCounts    map[int]int
	impliedDetect float64
}

type foldedEntry struct {
	multiplier    int
	foldedPeriodS float64
	detectionRate float64
}

type candidateEval struct {
	dominantPeriodS float64
	folded          map[uint32]*foldedEntry
	residual        map[uint32]float64
	supportWeight   float64
	directCount     int
	foldedCount     int
}

// --- AnalyseBurstRecords — top-level entry point ---

// ICAOFamily classifies an ICAO's family membership relative to the dominant period.
type ICAOFamily struct {
	FoldedICAOs  map[uint32]struct{} // ICAOs that fold into the dominant period
	ResidualICAOs map[uint32]struct{} // ICAOs that don't fit any family
}

// AnalyseBurstRecords runs the full rotation model derivation on a snapshot
// of burst records for one IID. Port of _analyse_burst_records() from sweep.py.
func AnalyseBurstRecords(records []BurstRecord) *RotationModel {
	// Group by ICAO, sort each group by centroid.
	icaoBursts := map[uint32][]BurstRecord{}
	for _, r := range records {
		if r.ICAO == 0 {
			continue
		}
		icaoBursts[r.ICAO] = append(icaoBursts[r.ICAO], r)
	}
	for icao := range icaoBursts {
		sort.Slice(icaoBursts[icao], func(i, j int) bool {
			return icaoBursts[icao][i].CentroidUS < icaoBursts[icao][j].CentroidUS
		})
	}

	icaoResults := map[uint32]*icaoResult{}
	for icao, bursts := range icaoBursts {
		if r := analyseICAO(bursts); r != nil {
			icaoResults[icao] = r
		}
	}

	if len(icaoResults) == 0 {
		return &RotationModel{Status: "INSUFFICIENT_DATA"}
	}

	harm := foldHarmonics(icaoResults)

	// Collect folded periods to compute spread.
	var foldedPeriods []float64
	for _, fe := range harm.folded {
		foldedPeriods = append(foldedPeriods, fe.foldedPeriodS)
	}
	if len(foldedPeriods) == 0 {
		for _, r := range icaoResults {
			foldedPeriods = append(foldedPeriods, r.medianPeriodS)
		}
	}

	spread := maxFloat(foldedPeriods) - minFloat(foldedPeriods)
	stdS := 0.0
	if len(foldedPeriods) > 1 {
		stdS = stdev(foldedPeriods)
	}

	nResidual := len(harm.residual)
	verdict := "CHECK_MULTI"
	switch {
	case nResidual == 0 && spread < 0.1:
		verdict = "SINGLE_RADAR"
	case nResidual == 0 && spread < 0.5:
		verdict = "LIKELY_SINGLE"
	}

	nHarmonic := 0
	for _, fe := range harm.folded {
		if fe.multiplier > 1 {
			nHarmonic++
		}
	}

	family := &ICAOFamily{
		FoldedICAOs:   make(map[uint32]struct{}, len(harm.folded)),
		ResidualICAOs: make(map[uint32]struct{}, len(harm.residual)),
	}
	for icao := range harm.folded {
		family.FoldedICAOs[icao] = struct{}{}
	}
	for icao := range harm.residual {
		family.ResidualICAOs[icao] = struct{}{}
	}

	model := &RotationModel{
		Status:             verdict,
		NQualifying:        len(icaoResults),
		NHarmonic:          nHarmonic,
		NResidual:          nResidual,
		PeriodStdS:         round6(stdS),
		PrimaryDirectCount: harm.directCount,
		Family:             family,
	}
	if harm.dominantPeriodS > 0 {
		p := harm.dominantPeriodS
		model.DominantPeriodS = &p
		rpm := 60.0 / p
		model.RPM = &rpm
	}
	return model
}

// --- analyseICAO — port of analyse_icao() ---

func analyseICAO(bursts []BurstRecord) *icaoResult {
	if len(bursts) < minBursts {
		return nil
	}

	centroids := make([]float64, len(bursts))
	for i, b := range bursts {
		centroids[i] = b.CentroidUS
	}

	allIntervalsS := make([]float64, len(centroids)-1)
	for i := 0; i < len(centroids)-1; i++ {
		allIntervalsS[i] = (centroids[i+1] - centroids[i]) / 1_000_000.0
	}

	var valid []float64
	for _, iv := range allIntervalsS {
		if iv > 1.0 && iv < 30.0 {
			valid = append(valid, iv)
		}
	}
	if len(valid) < 2 {
		return nil
	}

	series := clusterIntervals(valid)
	if len(series) == 0 {
		return nil
	}
	strongest := series[0]
	dominantPeriod := strongest.PeriodS

	var filtered []float64
	for _, iv := range valid {
		if math.Abs(iv-dominantPeriod)/dominantPeriod <= 0.5 {
			filtered = append(filtered, iv)
		}
	}
	if len(filtered) < 2 {
		filtered = make([]float64, strongest.NIntervals)
		for i := range filtered {
			filtered[i] = dominantPeriod
		}
	}

	nRepliesSum := 0
	for _, b := range bursts {
		nRepliesSum += b.NReplies
	}
	_ = nRepliesSum // avgReplies not stored in Go model

	return &icaoResult{
		nBursts:          len(bursts),
		nIntervals:       strongest.NIntervals,
		centroidsUS:      centroids,
		medianPeriodS:    dominantPeriod,
		meanPeriodS:      mean(filtered),
		stdS:             strongest.StdS,
		allIntervalsS:    allIntervalsS,
		seriesCandidates: series,
	}
}

// clusterIntervals — port of _cluster_repeated_intervals().
func clusterIntervals(intervals []float64) []seriesCandidate {
	sorted := make([]float64, len(intervals))
	copy(sorted, intervals)
	sort.Float64s(sorted)

	var clusters [][]float64
	for _, iv := range sorted {
		placed := false
		for ci := range clusters {
			med := median(clusters[ci])
			if med > 0 && math.Abs(iv-med)/med <= seriesClusterTolerance {
				clusters[ci] = append(clusters[ci], iv)
				placed = true
				break
			}
		}
		if !placed {
			clusters = append(clusters, []float64{iv})
		}
	}

	var candidates []seriesCandidate
	for _, cluster := range clusters {
		if len(cluster) < 2 {
			continue
		}
		med := median(cluster)
		std := 0.0
		if len(cluster) > 1 {
			std = stdev(cluster)
		}
		candidates = append(candidates, seriesCandidate{
			PeriodS:    med,
			NIntervals: len(cluster),
			StdS:       std,
		})
	}

	// Sort: most intervals first; ties break on -std then -period.
	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if a.NIntervals != b.NIntervals {
			return a.NIntervals > b.NIntervals
		}
		if a.StdS != b.StdS {
			return a.StdS < b.StdS
		}
		return a.PeriodS > b.PeriodS
	})
	return candidates
}

// --- foldHarmonics — port of _fold_harmonics() ---

func foldHarmonics(icaoResults map[uint32]*icaoResult) *candidateEval {
	if len(icaoResults) == 0 {
		return &candidateEval{}
	}

	// Collect unique candidate values from all series + median periods.
	seen := map[float64]struct{}{}
	var allValues []float64
	for _, r := range icaoResults {
		for _, sc := range r.seriesCandidates {
			if sc.PeriodS > 0 {
				if _, ok := seen[sc.PeriodS]; !ok {
					seen[sc.PeriodS] = struct{}{}
					allValues = append(allValues, sc.PeriodS)
				}
			}
		}
		if r.medianPeriodS > 0 {
			if _, ok := seen[r.medianPeriodS]; !ok {
				seen[r.medianPeriodS] = struct{}{}
				allValues = append(allValues, r.medianPeriodS)
			}
		}
	}
	if len(allValues) == 0 {
		return &candidateEval{}
	}
	sort.Float64s(allValues)

	// De-duplicate floating-point near-equal values.
	candidates := []float64{allValues[0]}
	for _, v := range allValues[1:] {
		if math.Abs(v-candidates[len(candidates)-1]) > 1e-9 {
			candidates = append(candidates, v)
		}
	}

	var evals []*candidateEval
	for _, c := range candidates {
		evals = append(evals, evaluateBaseCandidate(c, icaoResults, 0.05))
	}

	// Sort by (directCount desc, supportWeight desc, foldedCount desc, residual asc, period desc).
	sort.Slice(evals, func(i, j int) bool {
		a, b := evals[i], evals[j]
		if a.directCount != b.directCount {
			return a.directCount > b.directCount
		}
		if a.supportWeight != b.supportWeight {
			return a.supportWeight > b.supportWeight
		}
		if a.foldedCount != b.foldedCount {
			return a.foldedCount > b.foldedCount
		}
		if len(a.residual) != len(b.residual) {
			return len(a.residual) < len(b.residual)
		}
		return a.dominantPeriodS > b.dominantPeriodS
	})
	return evals[0]
}

// evaluateBaseCandidate — port of _evaluate_base_candidate().
func evaluateBaseCandidate(candidate float64, icaoResults map[uint32]*icaoResult, tolerance float64) *candidateEval {
	eval := &candidateEval{
		dominantPeriodS: candidate,
		folded:          map[uint32]*foldedEntry{},
		residual:        map[uint32]float64{},
	}

	type pass1Result struct {
		icao    uint32
		matched bool
		entry   *foldedEntry
		support float64
		direct  bool
		period  float64
	}

	// Pass 1: series-level matching.
	var residualICAOs []uint32
	for icao, result := range icaoResults {
		best := findBestSeriesMatch(result, candidate, tolerance)
		if best != nil {
			nint := best.nearestInt
			detectionRate := 1.0 / float64(nint)
			eval.folded[icao] = &foldedEntry{
				multiplier:    nint,
				foldedPeriodS: result.medianPeriodS / float64(nint),
				detectionRate: detectionRate,
			}
			eval.foldedCount++
			eval.supportWeight += float64(best.nIntervals) * detectionRate
			if nint == 1 {
				eval.directCount++
			}
		} else {
			eval.residual[icao] = result.medianPeriodS
			residualICAOs = append(residualICAOs, icao)
		}
	}

	// Pass 2: interval-level snap check for residuals.
	const snapThreshold = 0.80
	for _, icao := range residualICAOs {
		result := icaoResults[icao]
		var validIntervals []float64
		for _, iv := range result.allIntervalsS {
			if iv > 1.0 && iv < 30.0 {
				validIntervals = append(validIntervals, iv)
			}
		}

		if len(validIntervals) >= 3 {
			snap := snapIntervals(validIntervals, candidate, 0.08)
			if snap != nil && snap.snapRate >= snapThreshold {
				domMult := dominantMult(snap.multCounts)
				eval.folded[icao] = &foldedEntry{
					multiplier:    domMult,
					foldedPeriodS: candidate,
					detectionRate: snap.impliedDetect,
				}
				eval.foldedCount++
				nIntervals := result.nIntervals
				eval.supportWeight += float64(nIntervals) * snap.impliedDetect
				if domMult == 1 {
					eval.directCount++
				}
				delete(eval.residual, icao)
				continue
			}
		}

		// Pass 2b: centroid sequence matching.
		seq, coverage := pickCentroidSequence(result.centroidsUS, candidate)
		if len(seq) >= 3 && coverage >= 0.6 {
			var observedSpanS float64
			if len(seq) > 1 {
				observedSpanS = (seq[len(seq)-1] - seq[0]) / 1_000_000.0
			}
			impliedSweeps := math.Round(observedSpanS / candidate)
			if impliedSweeps < 1 {
				impliedSweeps = 1
			}
			detectionRate := float64(len(seq)-1) / impliedSweeps
			eval.folded[icao] = &foldedEntry{
				multiplier:    1,
				foldedPeriodS: candidate,
				detectionRate: detectionRate,
			}
			eval.foldedCount++
			sw := math.Max(float64(len(seq)-1), 1) * math.Max(detectionRate, coverage)
			eval.supportWeight += sw
			eval.directCount++
			delete(eval.residual, icao)
		}
	}

	return eval
}

// findBestSeriesMatch — finds the best matching series for a candidate period.
type seriesMatchResult struct {
	nearestInt int
	nIntervals int
	relErr     float64
}

func findBestSeriesMatch(result *icaoResult, candidate, tolerance float64) *seriesMatchResult {
	series := result.seriesCandidates
	if len(series) == 0 {
		series = []seriesCandidate{{
			PeriodS:    result.medianPeriodS,
			NIntervals: result.nIntervals,
			StdS:       result.stdS,
		}}
	}

	var best *seriesMatchResult
	for _, sc := range series {
		ratio := sc.PeriodS / candidate
		nint := int(math.Round(ratio))
		if nint < 1 {
			nint = 1
		}
		relErr := math.Abs(ratio-float64(nint)) / float64(nint)
		if relErr >= tolerance {
			continue
		}
		candidate_ := &seriesMatchResult{
			nearestInt: nint,
			nIntervals: sc.NIntervals,
			relErr:     relErr,
		}
		if best == nil || isBetterMatch(candidate_, best) {
			best = candidate_
		}
	}
	return best
}

func isBetterMatch(a, b *seriesMatchResult) bool {
	// Prefer direct (nint==1), then more intervals, then lower error.
	aDirect := a.nearestInt == 1
	bDirect := b.nearestInt == 1
	if aDirect != bDirect {
		return aDirect
	}
	if a.nIntervals != b.nIntervals {
		return a.nIntervals > b.nIntervals
	}
	return a.relErr < b.relErr
}

// snapIntervals — port of _snap_intervals().
func snapIntervals(intervals []float64, basePeriod, tolerance float64) *snapResult {
	multCounts := map[int]int{}
	for _, iv := range intervals {
		if iv <= 0 {
			continue
		}
		ratio := iv / basePeriod
		nearest := int(math.Round(ratio))
		if nearest < 1 {
			nearest = 1
		}
		if math.Abs(ratio-float64(nearest))/float64(nearest) < tolerance {
			multCounts[nearest]++
		}
	}

	total := len(intervals)
	nSnapped := 0
	for _, c := range multCounts {
		nSnapped += c
	}
	snapRate := 0.0
	if total > 0 {
		snapRate = float64(nSnapped) / float64(total)
	}

	totalSweeps := 0
	for mult, count := range multCounts {
		totalSweeps += mult * count
	}
	impliedDetect := 0.0
	if totalSweeps > 0 {
		impliedDetect = float64(nSnapped) / float64(totalSweeps)
	}

	return &snapResult{
		snapRate:      snapRate,
		multCounts:    multCounts,
		impliedDetect: impliedDetect,
	}
}

// dominantMult returns the key with the highest count, defaulting to 1.
func dominantMult(multCounts map[int]int) int {
	best := 1
	bestCount := -1
	for m, c := range multCounts {
		if c > bestCount || (c == bestCount && m < best) {
			best = m
			bestCount = c
		}
	}
	return best
}

// pickCentroidSequence — port of _pick_centroid_sequence() inner function.
func pickCentroidSequence(centroidsUS []float64, candidate float64) ([]float64, float64) {
	if candidate <= 0 || len(centroidsUS) < 3 {
		return nil, 0
	}

	var bestSeq []float64
	bestError := math.Inf(1)

	for anchorIdx, anchorUS := range centroidsUS {
		matched := []float64{anchorUS}
		errSum := 0.0
		for _, cUS := range centroidsUS[anchorIdx+1:] {
			deltaS := (cUS - anchorUS) / 1_000_000.0
			nearest := math.Round(deltaS / candidate)
			if nearest < 1 {
				continue
			}
			fracErr := math.Abs(deltaS-nearest*candidate) / candidate
			if fracErr <= 0.12 {
				matched = append(matched, cUS)
				errSum += fracErr
			}
		}
		if len(matched) > len(bestSeq) || (len(matched) == len(bestSeq) && errSum < bestError) {
			bestSeq = matched
			bestError = errSum
		}
	}

	coverage := 0.0
	if len(centroidsUS) > 0 {
		coverage = float64(len(bestSeq)) / float64(len(centroidsUS))
	}
	return bestSeq, coverage
}

// --- math helpers ---

func median(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	s := make([]float64, len(vals))
	copy(s, vals)
	sort.Float64s(s)
	n := len(s)
	if n%2 == 0 {
		return (s[n/2-1] + s[n/2]) / 2
	}
	return s[n/2]
}

func mean(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

func stdev(vals []float64) float64 {
	if len(vals) < 2 {
		return 0
	}
	m := mean(vals)
	sumSq := 0.0
	for _, v := range vals {
		d := v - m
		sumSq += d * d
	}
	return math.Sqrt(sumSq / float64(len(vals)-1))
}

func maxFloat(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	m := vals[0]
	for _, v := range vals[1:] {
		if v > m {
			m = v
		}
	}
	return m
}

func minFloat(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	m := vals[0]
	for _, v := range vals[1:] {
		if v < m {
			m = v
		}
	}
	return m
}

func round6(v float64) float64 {
	return math.Round(v*1e6) / 1e6
}
