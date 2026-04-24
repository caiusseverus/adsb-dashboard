package iid

// multisync_observation.go — observation preparation and residual scoring stage.
//
// This file is responsible only for converting raw MultiSyncObs into PreparedObservation
// values: applying propagation correction, computing residuals against a supplied sync
// hypothesis, classifying each observation, and computing quality weights.
//
// This layer must not know about authority modes, promotion streaks, or anchor promotion.
// It reads ms.ICAOQuality for quality multipliers and writes to it via updateICAOQuality,
// but it does not modify any other solver state.

import (
	"math"
	"time"
)

// prepareObservations scores and filters a recent observation window against the current
// seed hypothesis. It is the first stage of the runFit pipeline and its output is passed
// to all subsequent stages.
//
// Inputs:
//   - recent: observations within the rolling fit window
//   - seedEpochUS/seedOffsetDeg: the current phase hypothesis
//   - livePeriodS: period used for residual prediction
//   - recoveryActive: when true, the residual gate and pos-age gate are relaxed
//
// Outputs:
//   - prepared: one PreparedObservation per input observation
//   - fitRejectReasons: why each observation was rejected from fit eligibility
//   - fitEligibleCount: how many observations passed all admission gates
//   - recoveryRelaxedAdmissions: how many were admitted only because recoveryActive=true
func (ms *MultiSyncSolver) prepareObservations(
	recent []MultiSyncObs,
	seedEpochUS, seedOffsetDeg, livePeriodS float64,
	recoveryActive bool,
) (prepared []PreparedObservation, fitRejectReasons map[string]uint64, fitEligibleCount, recoveryRelaxedAdmissions int) {
	periodUS := livePeriodS * 1e6
	fitRejectReasons = make(map[string]uint64)
	prepared = make([]PreparedObservation, 0, len(recent))

	for _, o := range recent {
		effectiveUS := msPropCorrectedUS(float64(o.CentroidUS), float64(o.RangeNM))
		predicted := msPredictBearing(seedEpochUS, seedOffsetDeg, periodUS, effectiveUS)
		phaseInRot := math.Mod((effectiveUS-seedEpochUS)/periodUS*360.0, 360.0)
		if phaseInRot < 0 {
			phaseInRot += 360.0
		}
		residual := circularDiff(o.BearingDeg, predicted)
		absR := math.Abs(residual)
		status := msClassifyResidual(absR)
		baseW := msScoreObs(o)
		qEntry := ms.ICAOQuality[o.ICAO]
		qMult := msICAOQualityMult(qEntry)
		normalRejectReason := msFitRejectReason(o, status, absR, qEntry, false)
		fitRejectReason := msFitRejectReason(o, status, absR, qEntry, recoveryActive)

		var effectiveW float64
		switch status {
		case "inlier":
			effectiveW = baseW * qMult
		case "soft":
			effectiveW = baseW * 0.2 * qMult
		case "rejected":
			if recoveryActive && absR <= recoveryResidualRejectGate {
				effectiveW = baseW * 0.1 * qMult
			}
		}

		eligible := fitRejectReason == ""
		if eligible {
			fitEligibleCount++
			if recoveryActive && normalRejectReason != "" {
				recoveryRelaxedAdmissions++
			}
		} else {
			fitRejectReasons[fitRejectReason]++
		}

		var correctionFlags []string
		if recoveryActive && normalRejectReason != "" && eligible {
			correctionFlags = []string{"recovery_relaxed_admission"}
		}

		prepared = append(prepared, PreparedObservation{
			Obs:             o,
			EffectiveUS:     effectiveUS,
			BearingDeg:      o.BearingDeg,
			PredictedDeg:    predicted,
			ResidualDeg:     residual,
			AbsResidualDeg:  absR,
			BaseWeight:      baseW,
			EffectiveWeight: effectiveW,
			Status:          status,
			FitEligible:     eligible,
			FitRejectReason: fitRejectReason,
			CorrectionFlags: correctionFlags,
			phaseInRot:      phaseInRot,
		})
	}
	return
}

// updateICAOQuality updates the per-ICAO residual-MAD memory from fit-eligible observations.
// It is called once per solver run immediately after prepareObservations.
func (ms *MultiSyncSolver) updateICAOQuality(prepared []PreparedObservation) {
	groups := make(map[uint32][]float64)
	for _, p := range prepared {
		if p.FitEligible {
			groups[p.Obs.ICAO] = append(groups[p.Obs.ICAO], p.AbsResidualDeg)
		}
	}
	nowUnix := float64(time.Now().UnixMicro()) / 1e6
	for icao, absResiduals := range groups {
		med := medianFloat64(absResiduals)
		q := ms.ICAOQuality[icao]
		if q == nil {
			ms.ICAOQuality[icao] = &ICAOSyncQuality{
				ResidualMADDeg: med, NObservations: 1, LastUpdateTS: nowUnix,
			}
		} else {
			q.ResidualMADDeg = (1.0-icaoQualityMADAlpha)*q.ResidualMADDeg + icaoQualityMADAlpha*med
			q.NObservations++
			q.LastUpdateTS = nowUnix
		}
	}
}

// ─── pure observation helpers ─────────────────────────────────────────────────

// msPropCorrectedUS applies one-way aircraft→receiver propagation delay.
func msPropCorrectedUS(centroidUS, rangeNM float64) float64 {
	if rangeNM <= 0 {
		return centroidUS
	}
	return centroidUS - rangeNM*uSPerNMLight
}

// msPredictBearing returns the predicted bearing (deg) using the sync model.
func msPredictBearing(epochUS, offsetDeg, periodUS, effectiveUS float64) float64 {
	if periodUS <= 0 {
		return offsetDeg
	}
	phaseInRot := math.Mod((effectiveUS-epochUS)/periodUS*360.0, 360.0)
	if phaseInRot < 0 {
		phaseInRot += 360.0
	}
	return math.Mod(phaseInRot+offsetDeg, 360.0)
}

// msClassifyResidual returns "inlier", "soft", or "rejected".
func msClassifyResidual(absR float64) string {
	if absR <= residualInlierDeg {
		return "inlier"
	}
	if absR <= residualSoftDeg2 {
		return "soft"
	}
	return "rejected"
}

// msScoreObs returns a quality weight (0–1) based on reply count, signal, and position age.
func msScoreObs(o MultiSyncObs) float64 {
	nW := math.Min(float64(o.NReplies)/4.0, 1.0)
	sigW := 0.5
	if o.SignalDBFS != nil {
		sigW = math.Max(0.2, math.Min(1.0, (float64(*o.SignalDBFS)+50.0)/40.0))
	}
	ageW := 0.2
	switch {
	case o.PosAgeS <= 1.0:
		ageW = 1.0
	case o.PosAgeS <= 5.0:
		ageW = 0.8
	case o.PosAgeS <= 10.0:
		ageW = 0.5
	}
	return nW * sigW * ageW
}

// msICAOQualityMult returns the fit-weight multiplier for an ICAO based on its residual history.
func msICAOQualityMult(q *ICAOSyncQuality) float64 {
	if q == nil {
		return 1.0
	}
	mad := math.Max(q.ResidualMADDeg, 0.5)
	return math.Max(0.1, math.Min(1.0, 1.0/(1.0+mad/3.0)))
}

// msFitRejectReason returns the reason an observation is ineligible for fitting, or "" if eligible.
// When recoveryMode is true the residual gate and pos-age limit are relaxed.
func msFitRejectReason(o MultiSyncObs, status string, absR float64, q *ICAOSyncQuality, recoveryMode bool) string {
	if absR >= residualWrapDeg {
		return "near_wrap_residual"
	}
	rejectGate := residualRejectGate
	if recoveryMode {
		rejectGate = recoveryResidualRejectGate
	}
	if absR > rejectGate {
		return "residual_gate"
	}
	maxPosAgeS := 8.0
	if recoveryMode {
		maxPosAgeS = recoveryPositionAgeMaxS
	}
	if float64(o.PosAgeS) > maxPosAgeS {
		return "stale_position"
	}
	if q != nil && q.ResidualMADDeg >= icaoQualityRejectMAD && !recoveryMode {
		return "icao_quality_reject"
	}
	if status == "rejected" && !recoveryMode {
		return "residual_gate"
	}
	return ""
}

// msDetrendedMAD returns the median absolute detrended residual from fit-eligible observations.
// aFit and bFit are the intercept and slope of the linear trend, tRef is the time reference.
func msDetrendedMAD(prepared []PreparedObservation, aFit, bFit, tRef float64) float64 {
	var detrended []float64
	for _, p := range prepared {
		if !p.FitEligible {
			continue
		}
		t := p.EffectiveUS/1e6 - tRef
		detrended = append(detrended, math.Abs(p.ResidualDeg-(aFit+bFit*t)))
	}
	if len(detrended) == 0 {
		return 999.0
	}
	return medianFloat64(detrended)
}

// msAnchorScore returns the candidate anchor quality score for one ICAO.
func msAnchorScore(q *ICAOSyncQuality, baseW float64) float64 {
	if q == nil {
		return baseW * 0.5
	}
	madFactor := math.Max(0.1, 1.0-q.ResidualMADDeg/icaoQualityWarnMAD)
	return baseW * madFactor
}
