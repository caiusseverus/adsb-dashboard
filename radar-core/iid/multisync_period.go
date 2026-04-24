package iid

// multisync_period.go — period estimation and refinement stage.
//
// This file is responsible only for estimating and refining the radar rotation period.
// It uses the prepared observation window to fit a common residual slope across retained
// observations, applies gain- and clamp-limited corrections, and returns a PeriodFitResult.
//
// Invariants:
//   - Period fitting does not depend on anchor selection.
//   - Period fitting does not make authority decisions.
//   - The dominant-prior bound is enforced here, not in the authority layer.

import (
	"math"
	"sort"
)

// PeriodFitResult captures all outputs of the period-fitting stage.
// It is passed directly to solvePhaseBranch and decideAuthority — neither stage
// needs to re-run the slope fit or touch the period refinement logic.
type PeriodFitResult struct {
	// Fitted period after slope correction and clamping.
	BasePeriodS      float64 // clamp base used this run
	CandidatePeriodS float64 // slope-corrected candidate before clamping
	AppliedPeriodS   float64 // final period after all clamping (= what scores are computed against)

	// Slope used for correction.
	ResidualSlopeDegPerS float64
	SlopeWindowDeg       float64
	SlopeGatePassed      bool // true when slope is small enough not to block promotion

	// Fit pool statistics.
	FitSpanS    float64
	FitICAOs    int
	AcceptedObs int
	RejectedObs int
	RejectedAfterUnwrap int

	// Diagnostic labels.
	CorrectionBasis     string // "wrapped_fit" | "unwrapped_fit" | "ema_fallback"
	MotionGuardDegraded bool
	BlockReason         string // non-empty if slope gate blocked refinement
	Status              string // "ok" | "blocked:<reason>" | "reacquire"

	// Detrended MAD: used by authority to detect wrong-period lock.
	DetrendedMADDeg float64
	MajorityRejected bool

	// For reacquire: the candidate found by the search, if any.
	ReacquireCandidatePeriodS float64
	ReacquireCandidateScore   float64

	// Base-clamp diagnostics.
	BaseClamped   bool
	ClampDiffPPM  float64

	// Whether a reacquire candidate was found outside the dominant-prior bound.
	DominantPriorInconsistent bool

	// Fit pool counts (for authority promotion gates).
	FitPoolCount int
	FitICAOCount int

	// Bearing spread of contributing ICAOs (for motion-guard geometry check).
	BearingSpreadDeg float64

	// WrongPeriodSuspect is set when the detrended MAD and rejection fraction
	// indicate the current period is wrong.
	WrongPeriodSuspect bool
}

// fitPeriod runs the period-fitting stage against the prepared observation window.
// It returns a PeriodFitResult that summarises the slope fit, any corrections applied,
// and any reacquire candidate found. It also updates ms.SmoothSlopeDegPerS,
// ms.SlopeHistory, and trust-related state (applyTrustedBaseUpdate).
//
// Inputs mirror the current solver state needed for period refinement:
//   - livePeriodS: the period to score observations against
//   - basePeriodS: clamp base (trusted base, dominant, or bootstrap)
//   - dominantPeriodS: DF dominant-prior period (0 if unavailable)
//   - recoveryActive: passed to the slope fit (affects which obs are admitted)
//   - nowUnix: current wall-clock time
func (ms *MultiSyncSolver) fitPeriod(
	prepared []PreparedObservation,
	seedEpochUS, seedOffsetDeg float64,
	livePeriodS, basePeriodS, dominantPeriodS float64,
	recoveryActive bool,
	nowUnix float64,
) PeriodFitResult {
	r := PeriodFitResult{
		BasePeriodS:    basePeriodS,
		AppliedPeriodS: livePeriodS,
		Status:         "ok",
	}

	// Build a fit pool from fit-eligible observations (same pool used by runFit).
	type fitEntry struct {
		effectiveUS float64
		residual    float64
		weight      float64
		icao        uint32
	}
	var fitPool []fitEntry
	for _, p := range prepared {
		if p.FitEligible && p.EffectiveWeight > 0 {
			fitPool = append(fitPool, fitEntry{p.EffectiveUS, p.ResidualDeg, p.EffectiveWeight, p.Obs.ICAO})
		}
	}
	if len(fitPool) == 0 {
		for _, p := range prepared {
			if p.BaseWeight > 0 && p.FitRejectReason != "near_wrap_residual" {
				fitPool = append(fitPool, fitEntry{
					p.EffectiveUS, p.ResidualDeg,
					p.BaseWeight * msICAOQualityMult(ms.ICAOQuality[p.Obs.ICAO]),
					p.Obs.ICAO,
				})
			}
		}
	}

	fitICAOs := make(map[uint32]bool)
	for _, fe := range fitPool {
		fitICAOs[fe.icao] = true
	}
	r.FitPoolCount = len(fitPool)
	r.FitICAOCount = len(fitICAOs)

	// Count inliers and rejects.
	nInliers, nRejected := 0, 0
	for _, p := range prepared {
		switch p.Status {
		case "inlier":
			nInliers++
		case "rejected":
			nRejected++
		}
	}
	r.MajorityRejected = nRejected >= len(prepared)/2+1

	// Weighted linear fit over the fit pool (residual ~ a + b*(t-tRef)).
	tRef := 0.0
	if len(fitPool) > 0 {
		tRef = fitPool[0].effectiveUS
		for _, fe := range fitPool {
			if fe.effectiveUS < tRef {
				tRef = fe.effectiveUS
			}
		}
		tRef /= 1e6
	}
	spanS := 0.0
	xs := make([]float64, len(fitPool))
	ys := make([]float64, len(fitPool))
	ws := make([]float64, len(fitPool))
	for i, fe := range fitPool {
		xs[i] = fe.effectiveUS/1e6 - tRef
		ys[i] = fe.residual
		ws[i] = fe.weight
		if xs[i] > spanS {
			spanS = xs[i]
		}
	}
	aFit, bFit := weightedLinearFit(xs, ys, ws)
	r.FitSpanS = spanS

	// Detrended MAD (used by authority to detect wrong-period lock).
	r.DetrendedMADDeg = msDetrendedMAD(prepared, aFit, bFit, tRef)

	// Wrong-period suspicion.
	r.WrongPeriodSuspect = ms.assessPeriodFailure(
		len(prepared), nRejected, len(fitPool), len(fitICAOs), r.MajorityRejected, r.DetrendedMADDeg,
	)

	prevSlope := ms.SmoothSlopeDegPerS
	smoothedSlope := (1.0-slopeEMAAlpha)*prevSlope + slopeEMAAlpha*bFit

	// Retained-window slope fit (primary path).
	periodSlope, periodFitCount, periodFitICAOs, periodFitSpanS, bearingSpreadDeg, periodSlopeOK :=
		ms.retainedResidualSlope(seedEpochUS, seedOffsetDeg, livePeriodS, nowUnix, recoveryActive)

	if periodSlopeOK {
		ms.SmoothSlopeDegPerS = (1.0-retainedSlopeEMAAlpha)*prevSlope + retainedSlopeEMAAlpha*periodSlope
		r.ResidualSlopeDegPerS = ms.SmoothSlopeDegPerS
		periodSlope = periodSlope // use raw for this run
		r.CorrectionBasis = "wrapped_fit"
		r.AcceptedObs = periodFitCount
		r.RejectedObs = len(ms.obs) - periodFitCount
		r.RejectedAfterUnwrap = 0
		r.FitICAOs = periodFitICAOs
		r.FitSpanS = periodFitSpanS
		r.BearingSpreadDeg = bearingSpreadDeg
	} else {
		// Fallback: unwrapped per-ICAO slope (handles near-wrap / diagonal-band residuals).
		uwSlope, uwAcc, uwRej, uwRejAfterUnwrap, uwICAOs, uwBearingSpread, uwSpanS, uwOK :=
			ms.retainedResidualSlopeUnwrapped(seedEpochUS, seedOffsetDeg, livePeriodS, nowUnix)
		r.AcceptedObs = uwAcc
		r.RejectedObs = uwRej
		r.RejectedAfterUnwrap = uwRejAfterUnwrap
		if uwOK {
			ms.SmoothSlopeDegPerS = (1.0-retainedSlopeEMAAlpha)*prevSlope + retainedSlopeEMAAlpha*uwSlope
			r.ResidualSlopeDegPerS = ms.SmoothSlopeDegPerS
			periodSlope = uwSlope
			r.FitICAOs = uwICAOs
			r.FitSpanS = uwSpanS
			r.BearingSpreadDeg = uwBearingSpread
			r.CorrectionBasis = "unwrapped_fit"
		} else {
			// No retained-window slope: fall back to EMA from the short-window fit.
			periodSlope = smoothedSlope
			ms.SmoothSlopeDegPerS = periodSlope
			r.ResidualSlopeDegPerS = periodSlope
			r.FitICAOs = periodFitICAOs
			r.FitSpanS = spanS
			r.BearingSpreadDeg = msBearingSpreadDeg(ms.obs, fitICAOs, nowUnix-multiSyncRetentionS)
			r.CorrectionBasis = "ema_fallback"
		}
	}

	ms.SlopeHistory = append(ms.SlopeHistory, periodSlope)
	if len(ms.SlopeHistory) > 12 {
		ms.SlopeHistory = ms.SlopeHistory[len(ms.SlopeHistory)-12:]
	}

	refinedPeriodS, blockReason, _, clampedByBase, clampDiffPPM := ms.refinePeriod(
		livePeriodS, basePeriodS, dominantPeriodS, periodSlope,
		r.AcceptedObs, r.FitICAOs, r.FitSpanS, r.BearingSpreadDeg,
		r.MajorityRejected, ms.TrustedBasePeriodS > 0,
	)
	r.BlockReason = blockReason
	r.CandidatePeriodS = refinedPeriodS
	r.AppliedPeriodS = refinedPeriodS
	r.BaseClamped = clampedByBase
	r.ClampDiffPPM = clampDiffPPM

	// Slope gate for authority promotion.
	slopeWindowDeg := math.Abs(r.ResidualSlopeDegPerS) * r.FitSpanS
	r.SlopeWindowDeg = slopeWindowDeg
	r.SlopeGatePassed = math.Abs(r.ResidualSlopeDegPerS) <= authorityPromotionMaxSlopeDegPerS &&
		slopeWindowDeg <= authorityPromotionMaxSlopeWindowDeg

	if blockReason != "" {
		r.Status = "blocked:" + blockReason
	}

	return r
}

// ─── retained-window slope functions ─────────────────────────────────────────

// retainedResidualSlope estimates the common period-error slope across retained observations
// while allowing each ICAO its own phase intercept. This keeps spurious per-aircraft
// offsets from steering the hardware-period correction.
func (ms *MultiSyncSolver) retainedResidualSlope(
	seedEpochUS, seedOffsetDeg, periodS, nowUnix float64,
	recoveryActive bool,
) (slope float64, nFit, nICAOs int, spanS, bearingSpreadDeg float64, ok bool) {
	if periodS <= 0 {
		return 0, 0, 0, 0, 0, false
	}
	type entry struct {
		icao uint32
		x    float64
		y    float64
		w    float64
	}
	type accum struct {
		n      int
		sumW   float64
		sumX   float64
		sumY   float64
		sumSin float64
		sumCos float64
	}

	cutoffTS := nowUnix - multiSyncRetentionS
	periodUS := periodS * 1e6
	entries := make([]entry, 0, len(ms.obs))
	byICAO := make(map[uint32]*accum)
	minX := math.Inf(1)
	maxX := math.Inf(-1)
	for _, o := range ms.obs {
		if o.WallTS < cutoffTS {
			continue
		}
		effectiveUS := msPropCorrectedUS(float64(o.CentroidUS), float64(o.RangeNM))
		predicted := msPredictBearing(seedEpochUS, seedOffsetDeg, periodUS, effectiveUS)
		residual := circularDiff(o.BearingDeg, predicted)
		absR := math.Abs(residual)
		status := msClassifyResidual(absR)
		qEntry := ms.ICAOQuality[o.ICAO]
		rejectReason := msFitRejectReason(o, status, absR, qEntry, recoveryActive)
		if rejectReason != "" {
			continue
		}
		weight := msScoreObs(o) * msICAOQualityMult(qEntry)
		if weight <= 0 {
			continue
		}
		x := effectiveUS / 1e6
		entries = append(entries, entry{icao: o.ICAO, x: x, y: residual, w: weight})
		a := byICAO[o.ICAO]
		if a == nil {
			a = &accum{}
			byICAO[o.ICAO] = a
		}
		a.n++
		a.sumW += weight
		a.sumX += weight * x
		a.sumY += weight * residual
		rad := o.BearingDeg * math.Pi / 180.0
		a.sumSin += weight * math.Sin(rad)
		a.sumCos += weight * math.Cos(rad)
		if x < minX {
			minX = x
		}
		if x > maxX {
			maxX = x
		}
	}
	if len(entries) < periodRefineMinInlier || len(byICAO) < 2 {
		return 0, len(entries), len(byICAO), 0, 0, false
	}

	num := 0.0
	den := 0.0
	used := 0
	usedICAOs := make(map[uint32]bool, len(byICAO))
	for _, e := range entries {
		a := byICAO[e.icao]
		if a == nil || a.n < 2 || a.sumW <= 0 {
			continue
		}
		xMean := a.sumX / a.sumW
		yMean := a.sumY / a.sumW
		dx := e.x - xMean
		dy := e.y - yMean
		num += e.w * dx * dy
		den += e.w * dx * dx
		used++
		usedICAOs[e.icao] = true
	}
	if used < periodRefineMinInlier || len(usedICAOs) < 2 || den <= 0 ||
		math.IsNaN(num) || math.IsInf(num, 0) || math.IsNaN(den) || math.IsInf(den, 0) {
		sp := 0.0
		if !math.IsInf(minX, 0) && !math.IsInf(maxX, 0) {
			sp = maxX - minX
		}
		return 0, used, len(usedICAOs), sp, 0, false
	}

	bearingMeans := make([]float64, 0, len(usedICAOs))
	for icao := range usedICAOs {
		a := byICAO[icao]
		if a != nil && a.sumW > 0 {
			meanDeg := math.Atan2(a.sumSin/a.sumW, a.sumCos/a.sumW) * 180.0 / math.Pi
			bearingMeans = append(bearingMeans, meanDeg)
		}
	}
	return num / den, used, len(usedICAOs), maxX - minX, bearingCircularSpreadDeg(bearingMeans), true
}

// bearingCircularSpreadDeg returns the arc coverage (degrees) spanned by a set of
// bearings on a circle: 360° minus the largest gap between consecutive bearings.
func bearingCircularSpreadDeg(bearings []float64) float64 {
	if len(bearings) < 2 {
		return 0.0
	}
	sorted := make([]float64, len(bearings))
	for i, b := range bearings {
		r := math.Mod(b, 360.0)
		if r < 0 {
			r += 360.0
		}
		sorted[i] = r
	}
	sort.Float64s(sorted)
	maxGap := sorted[0] + 360.0 - sorted[len(sorted)-1]
	for i := 1; i < len(sorted); i++ {
		if gap := sorted[i] - sorted[i-1]; gap > maxGap {
			maxGap = gap
		}
	}
	return 360.0 - maxGap
}

// retainedResidualSlopeUnwrapped estimates the period-error slope by first
// unwrapping residuals per ICAO before fitting. It is called as a fallback when
// retainedResidualSlope cannot gather enough evidence because near-wrap residuals
// (abs ≥ 150°) dominate — the pattern visible as diagonal bands in the residual chart.
//
// Unlike retainedResidualSlope, this function does NOT reject near_wrap_residual
// observations. Instead it:
//  1. Applies only basic quality gates (pos age, ICAO quality, signal weight).
//  2. Per-ICAO: sorts by time and unwraps residuals to remove ±360° jumps.
//  3. Detrends each ICAO by subtracting its weighted mean unwrapped residual.
//  4. Rejects detrended observations that exceed periodFitUnwrappedResidualGate.
//  5. Fits a common slope via per-ICAO intercept correction (same as the standard path).
//
// Returns: slope (deg/s), nAccepted, nRejected (pre-unwrap), nRejAfterUnwrap,
// nFitICAOs, bearingSpreadDeg, fitSpanS, ok.
func (ms *MultiSyncSolver) retainedResidualSlopeUnwrapped(
	seedEpochUS, seedOffsetDeg, periodS, nowUnix float64,
) (slope float64, nAccepted, nRejected, nRejAfterUnwrap, nFitICAOs int, bearingSpreadDeg, fitSpanS float64, ok bool) {
	if periodS <= 0 {
		return
	}
	type rawObs struct {
		x          float64
		y          float64
		w          float64
		bearingDeg float64
	}
	cutoffTS := nowUnix - multiSyncRetentionS
	periodUS := periodS * 1e6

	byICAO := make(map[uint32][]rawObs)
	totalInput := 0
	for _, o := range ms.obs {
		if o.WallTS < cutoffTS {
			continue
		}
		w := msScoreObs(o) * msICAOQualityMult(ms.ICAOQuality[o.ICAO])
		if w <= 0 {
			continue
		}
		if float64(o.PosAgeS) > 8.0 {
			continue
		}
		q := ms.ICAOQuality[o.ICAO]
		if q != nil && q.ResidualMADDeg >= icaoQualityRejectMAD {
			continue
		}
		effectiveUS := msPropCorrectedUS(float64(o.CentroidUS), float64(o.RangeNM))
		predicted := msPredictBearing(seedEpochUS, seedOffsetDeg, periodUS, effectiveUS)
		residual := circularDiff(o.BearingDeg, predicted)
		byICAO[o.ICAO] = append(byICAO[o.ICAO], rawObs{
			x:          effectiveUS / 1e6,
			y:          residual,
			w:          w,
			bearingDeg: o.BearingDeg,
		})
		totalInput++
	}
	nRejected = totalInput
	if len(byICAO) < 2 {
		return
	}

	type fitEntry struct {
		icao uint32
		x    float64
		y    float64
		w    float64
	}
	type bearingAccum struct{ sumSin, sumCos float64 }
	icaoBearings := make(map[uint32]*bearingAccum, len(byICAO))

	var entries []fitEntry
	minX := math.Inf(1)
	maxX := math.Inf(-1)

	for icao, obs := range byICAO {
		if len(obs) < 2 {
			nRejected += len(obs)
			continue
		}
		sort.Slice(obs, func(i, j int) bool { return obs[i].x < obs[j].x })
		unwrapped := make([]float64, len(obs))
		unwrapped[0] = obs[0].y
		for i := 1; i < len(obs); i++ {
			diff := obs[i].y - obs[i-1].y
			for diff > 180.0 {
				diff -= 360.0
			}
			for diff < -180.0 {
				diff += 360.0
			}
			unwrapped[i] = unwrapped[i-1] + diff
		}
		var wSumW, wSumY float64
		for i, o := range obs {
			wSumW += o.w
			wSumY += o.w * unwrapped[i]
		}
		if wSumW <= 0 {
			continue
		}
		meanUnwrapped := wSumY / wSumW
		ba := icaoBearings[icao]
		if ba == nil {
			ba = &bearingAccum{}
			icaoBearings[icao] = ba
		}
		accepted := 0
		for i, o := range obs {
			detrended := unwrapped[i] - meanUnwrapped
			if math.Abs(detrended) > periodFitUnwrappedResidualGate {
				nRejAfterUnwrap++
				continue
			}
			entries = append(entries, fitEntry{icao: icao, x: o.x, y: detrended, w: o.w})
			rad := o.bearingDeg * math.Pi / 180.0
			ba.sumSin += o.w * math.Sin(rad)
			ba.sumCos += o.w * math.Cos(rad)
			if o.x < minX {
				minX = o.x
			}
			if o.x > maxX {
				maxX = o.x
			}
			accepted++
			nRejected--
		}
		nAccepted += accepted
	}

	if nAccepted < periodRefineMinInlier || len(icaoBearings) < 2 {
		return
	}

	// Compute per-ICAO intercept-corrected slope.
	byICAOEntries := make(map[uint32][]fitEntry, len(icaoBearings))
	for _, e := range entries {
		byICAOEntries[e.icao] = append(byICAOEntries[e.icao], e)
	}
	num := 0.0
	den := 0.0
	used := 0
	usedICAOs := make(map[uint32]bool)
	for icao, ics := range byICAOEntries {
		if len(ics) < 2 {
			continue
		}
		var wSum, wX, wY float64
		for _, e := range ics {
			wSum += e.w
			wX += e.w * e.x
			wY += e.w * e.y
		}
		if wSum <= 0 {
			continue
		}
		xMean := wX / wSum
		yMean := wY / wSum
		for _, e := range ics {
			dx := e.x - xMean
			dy := e.y - yMean
			num += e.w * dx * dy
			den += e.w * dx * dx
			used++
		}
		usedICAOs[icao] = true
	}
	if used < periodRefineMinInlier || len(usedICAOs) < 2 || den <= 0 {
		return
	}

	bearingMeans := make([]float64, 0, len(icaoBearings))
	for icao := range usedICAOs {
		ba := icaoBearings[icao]
		if ba != nil {
			bearingMeans = append(bearingMeans, math.Atan2(ba.sumSin, ba.sumCos)*180.0/math.Pi)
		}
	}
	bearingSpreadDeg = bearingCircularSpreadDeg(bearingMeans)
	nFitICAOs = len(usedICAOs)
	if !math.IsInf(minX, 1) && !math.IsInf(maxX, -1) {
		fitSpanS = maxX - minX
	}
	slope = num / den
	ok = true
	return
}

// msBearingSpreadDeg computes the circular arc coverage of the mean bearings of a
// set of ICAOs across retained observations.
func msBearingSpreadDeg(obs []MultiSyncObs, icaos map[uint32]bool, cutoffTS float64) float64 {
	type bAccum struct{ sumSin, sumCos float64 }
	byICAO := make(map[uint32]*bAccum, len(icaos))
	for _, o := range obs {
		if o.WallTS < cutoffTS || !icaos[o.ICAO] {
			continue
		}
		a := byICAO[o.ICAO]
		if a == nil {
			a = &bAccum{}
			byICAO[o.ICAO] = a
		}
		rad := o.BearingDeg * math.Pi / 180.0
		a.sumSin += math.Sin(rad)
		a.sumCos += math.Cos(rad)
	}
	means := make([]float64, 0, len(byICAO))
	for _, a := range byICAO {
		means = append(means, math.Atan2(a.sumSin, a.sumCos)*180.0/math.Pi)
	}
	return bearingCircularSpreadDeg(means)
}

// selectActiveFamilyPrior chooses the period seed that should be used as the
// live period for scoring and slope fitting in the current run.
// The authority mode determines which source takes precedence.
func (ms *MultiSyncSolver) selectActiveFamilyPrior(compactPeriodS, dominantPeriodS float64, authorityMode string) (float64, string) {
	if authorityMode == authorityModeRefined {
		if ms.AuthoritativePresent && ms.AuthoritativePeriodS > 0 {
			return ms.AuthoritativePeriodS, "authoritative_refined"
		}
		if ms.CandidatePresent && ms.CandidatePeriodS > 0 {
			return ms.CandidatePeriodS, "candidate_refined"
		}
		if ms.TrustedBasePeriodS > 0 {
			return ms.TrustedBasePeriodS, "trusted_refined"
		}
		if ms.Present && ms.PeriodS > 0 {
			return ms.PeriodS, "current_refined"
		}
	}
	if authorityMode == authorityModeCompact {
		if dominantPeriodS > 0 {
			return dominantPeriodS, "dominant_live_df_seed"
		}
		if compactPeriodS > 0 {
			return compactPeriodS, "compact_seed"
		}
	}
	if ms.CandidatePresent && ms.CandidatePeriodS > 0 && authorityMode != authorityModeCompact {
		return ms.CandidatePeriodS, "candidate_refined"
	}
	if ms.AuthoritativePresent && ms.AuthoritativePeriodS > 0 && authorityMode != authorityModeCompact {
		return ms.AuthoritativePeriodS, "authoritative_refined"
	}
	if ms.TrustedBasePeriodS > 0 && authorityMode != authorityModeCompact {
		return ms.TrustedBasePeriodS, "trusted_refined"
	}
	if ms.Present && ms.PeriodS > 0 && !ms.Holdover {
		return ms.PeriodS, "current_refined"
	}
	if dominantPeriodS > 0 {
		return dominantPeriodS, "dominant_live_df"
	}
	if compactPeriodS > 0 {
		return compactPeriodS, "compact_seed"
	}
	if ms.BootstrapPeriodS > 0 {
		return ms.BootstrapPeriodS, "bootstrap_seed"
	}
	return 0, ""
}

// refinePeriod applies a slope-based correction to livePeriodS, clamped within the
// allowed PPM band around basePeriodS (or dominantPeriodS when the dominant prior is active).
//
// Returns the refined period, the block reason (empty if refinement ran), a legacy
// reacquireReason string, and clamp diagnostics.
func (ms *MultiSyncSolver) refinePeriod(
	livePeriodS, basePeriodS, dominantPeriodS, smoothedSlope float64,
	nFit, nFitICAOs int, spanS, bearingSpreadDeg float64, majorityRejected bool,
	trusted bool,
) (refined float64, blockReason, reacquireReason string, clamped bool, clampDiffPPM float64) {
	refined = livePeriodS
	absSmoothed := math.Abs(smoothedSlope)
	slopeSign := 0
	if smoothedSlope > 0 {
		slopeSign = 1
	} else if smoothedSlope < 0 {
		slopeSign = -1
	}
	prev := ms.SlopeHistory
	if len(prev) > 8 {
		prev = prev[len(prev)-8:]
	}
	consistent := 0
	for _, s := range prev {
		if math.Abs(s) >= slopeDeadBand {
			if (s > 0 && slopeSign > 0) || (s < 0 && slopeSign < 0) {
				consistent++
			}
		}
	}
	persistent := slopeSign != 0 &&
		len(prev) >= persistMinEntries &&
		consistent >= persistMinEntries &&
		absSmoothed >= slopeDeadBand

	// Degraded motion guard: when DF dominant prior is active but we have fewer than
	// periodRefineMotionGuardMinICAOs (3) ICAOs or insufficient bearing spread,
	// allow a degraded correction path (smaller gain and step) rather than hard-blocking.
	motionGuardDegraded := dominantPeriodS > 0 && blockReason == "" && ((nFitICAOs >= 2 && nFitICAOs < periodRefineMotionGuardMinICAOs) ||
		(nFitICAOs >= periodRefineMotionGuardMinICAOs && bearingSpreadDeg < periodRefineMinBearingSpreadDeg))
	ms.LastMotionGuardDegraded = motionGuardDegraded

	switch {
	case majorityRejected:
		blockReason = "majority_rejected"
	case nFit < periodRefineMinInlier:
		blockReason = "insufficient_fit_observations"
	case nFitICAOs < 2:
		blockReason = "insufficient_fit_icaos"
	case spanS < periodRefineMinSpanRot*livePeriodS:
		blockReason = "insufficient_fit_span"
	case !persistent:
		blockReason = "slope_not_persistent"
	}
	if blockReason != "" || livePeriodS <= 0 {
		return
	}

	gainToUse := periodGain
	if motionGuardDegraded {
		gainToUse = periodGainDegraded
	}
	rateNominal := 360.0 / livePeriodS
	rateTarget := rateNominal + smoothedSlope*gainToUse
	if rateTarget <= 0 {
		blockReason = "non_positive_rate_target"
		return
	}
	candidate := 360.0 / rateTarget

	strongFit := nFit >= 12 && nFitICAOs >= 3 && spanS >= 4.0*livePeriodS
	ppmPerUpdate := periodPPMPerUpdate
	var ppmFromBase float64
	if trusted {
		ppmFromBase = periodPPMFromBase
		if strongFit && persistent {
			ppmPerUpdate = periodPPMStrong
			ppmFromBase = periodPPMBaseStrong
		}
	} else {
		ppmFromBase = periodPPMFromBootstrap
		if strongFit && persistent {
			ppmPerUpdate = periodPPMStrong
			ppmFromBase = periodPPMFromBootstrapStrong
		}
	}
	// When the DF dominant prior is available, override step and base clamps with
	// tight bounds anchored to the dominant family.
	if dominantPeriodS > 0 {
		if motionGuardDegraded {
			ppmPerUpdate = periodRefineDegradedMaxStepPPM
		} else {
			ppmPerUpdate = periodRefineMaxStepPPM
		}
		basePeriodS = dominantPeriodS
		ppmFromBase = periodRefineMaxPPMFromDominant
	}

	deltaPPM := (candidate - livePeriodS) / livePeriodS * 1e6
	if deltaPPM > ppmPerUpdate {
		candidate = livePeriodS * (1.0 + ppmPerUpdate*1e-6)
	} else if deltaPPM < -ppmPerUpdate {
		candidate = livePeriodS * (1.0 - ppmPerUpdate*1e-6)
	}
	if basePeriodS > 0 {
		rawPPM := (candidate - basePeriodS) / basePeriodS * 1e6
		clampDiffPPM = rawPPM
		if rawPPM > ppmFromBase {
			candidate = basePeriodS * (1.0 + ppmFromBase*1e-6)
			clamped = true
		} else if rawPPM < -ppmFromBase {
			candidate = basePeriodS * (1.0 - ppmFromBase*1e-6)
			clamped = true
		}
	}
	refined = candidate
	if math.Abs(deltaPPM) > periodPPMStrong*3 {
		reacquireReason = "large_period_jump"
	}
	return
}

// assessPeriodFailure returns true when the current fit indicates the published period
// is likely wrong. Called once per run to drive the PeriodFailureStreak counter.
func (ms *MultiSyncSolver) assessPeriodFailure(
	nRecent, nRejected, nFit, nFitICAOs int,
	majorityRejected bool, detrendedMAD float64,
) bool {
	if majorityRejected && nFit < 4 {
		return true
	}
	if detrendedMAD > 30.0 && nRecent > 5 {
		return true
	}
	return false
}

// applyTrustedBaseUpdate advances or resets the trust streak and conditionally
// promotes the refined period to TrustedBasePeriodS.
func (ms *MultiSyncSolver) applyTrustedBaseUpdate(finalPeriodS float64, trustedEnough, wrongPeriodSuspect bool) {
	if ms.PeriodReacquireActive || wrongPeriodSuspect {
		ms.TrustUpdateStreak = 0
		return
	}
	if !trustedEnough {
		ms.TrustUpdateStreak = 0
		return
	}
	ms.TrustUpdateStreak++
	if ms.TrustUpdateStreak < trustMinStreak {
		return
	}
	if ms.TrustedBasePeriodS <= 0 {
		ms.TrustedBasePeriodS = finalPeriodS
		return
	}
	ms.TrustedBasePeriodS = 0.98*ms.TrustedBasePeriodS + 0.02*finalPeriodS
}
