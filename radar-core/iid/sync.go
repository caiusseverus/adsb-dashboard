package iid

import (
	"math"
	"time"
)

// SyncState tracks the live sweep-phase synchronisation for one IID.
// Port of LiveSyncState (core fields only) from sweep.py.
//
// The bearing conversion formula is:
//
//	bearing_deg = ((arrival_us - PhaseEpochUS) / (PeriodS*1e6) * 360 + PhaseOffsetDeg) mod 360
type SyncState struct {
	IID                    uint8
	PeriodS                float64 // deprecated alias for EffectivePeriodS; kept for tests/compatibility only.
	BasePeriodS            float64
	PeriodDeltaS           float64
	EffectivePeriodS       float64
	PhaseEpochUS           float64 // Beast-monotonic centroid of last accepted reference burst
	PhaseOffsetDeg         float64 // Geometric bearing from radar to ref aircraft at epoch (0 until radar pos known)
	SyncQuality            float64 // 0.0–1.0 from rotation model status
	SyncJitterDeg          float64 // 1-sigma jitter estimate from residual EMA
	ResidualEMA            float64 // EMA of |circular residual| degrees
	NSyncFrames            int     // accepted frame count
	NRejectedFrames        int     // rejected frame count
	LastResidualDeg        float64
	Holdover               bool // true when last update was rejected / too weak
	LastUpdated            time.Time
	PeriodSource           string
	PeriodAgreesWithDF     bool
	PeriodRejectReason     string
	ResidualSlopeDegPerS   float64
	PeriodRefinementStatus string
	residualHistory        []residualObservation
}

const (
	residualRejectDeg             = 50.0 // hard-reject threshold (degrees)
	residualSoftDeg               = 20.0 // soft-accept threshold
	residualEMAAlpha              = 0.2
	residualHistoryMax            = 32
	residualFitMinObs             = 8
	residualFitMinSpanS           = 20.0
	refinementFitWindowS          = 120.0
	refinementAbsBoundFraction    = 0.005
	refinementSlewFractionPerStep = 0.00025
	refinementDecayOnReject       = 0.98
)

type residualObservation struct {
	EpochUS     float64
	ResidualDeg float64
	NAircraft   int
	RefPosAgeS  float64
}

// syncQuality derives a 0-1 quality score from a rotation status string.
// Mirrors _sync_quality_from_model() in sweep.py.
func syncQuality(status string, hasPeriod bool) float64 {
	if !hasPeriod {
		return 0.0
	}
	switch status {
	case "SINGLE_RADAR":
		return 1.0
	case "LIKELY_SINGLE":
		return 0.8
	case "CHECK_MULTI":
		return 0.5
	case "MULTI_RADAR":
		return 0.3
	}
	return 0.0
}

// NewSyncState bootstraps a SyncState from the first accepted sweep frame.
// phaseOffsetDeg is the geometric bearing from radar to reference aircraft
// (0.0 when the radar position is unknown — Stage 3 default).
func NewSyncState(iid uint8, periodS, epochUS, phaseOffsetDeg, quality float64) *SyncState {
	return &SyncState{
		IID:                    iid,
		PeriodS:                periodS,
		BasePeriodS:            periodS,
		PeriodDeltaS:           0,
		EffectivePeriodS:       periodS,
		PhaseEpochUS:           epochUS,
		PhaseOffsetDeg:         phaseOffsetDeg,
		SyncQuality:            quality,
		SyncJitterDeg:          5.0,
		ResidualEMA:            5.0,
		NSyncFrames:            1,
		Holdover:               false,
		LastUpdated:            time.Now(),
		PeriodSource:           "df_alignment",
		PeriodAgreesWithDF:     true,
		PeriodRefinementStatus: "base_only",
	}
}

// UpdateEpoch advances the sync epoch when a new reference aircraft burst arrives.
// nAircraft is the number of aircraft seen in the frame window; refPosAgeS is
// the age of the reference aircraft's position at burst time.
// phaseOffsetDeg is 0.0 until radar position is known.
//
// Returns true if the update was accepted.
func (s *SyncState) UpdateEpoch(newEpochUS, newOffsetDeg, periodS, quality float64, nAircraft int, refPosAgeS float64) bool {
	if periodS <= 0 || math.IsNaN(periodS) || math.IsInf(periodS, 0) {
		s.Holdover = true
		s.PeriodRejectReason = "missing_df_base_period"
		s.PeriodRefinementStatus = "missing_df_base_period"
		s.PeriodDeltaS = 0
		s.EffectivePeriodS = 0
		s.PeriodS = 0
		s.ResidualSlopeDegPerS = 0
		return false
	}

	// Quality gate: frame must have >= 4 aircraft, or >= 3 with fresh ref position.
	eligible := nAircraft >= 4 || (nAircraft >= 3 && refPosAgeS <= 2.0)
	if !eligible {
		s.Holdover = true
		return false
	}

	predictionPeriodS := s.EffectivePeriodS
	if predictionPeriodS <= 0 || math.IsNaN(predictionPeriodS) || math.IsInf(predictionPeriodS, 0) {
		predictionPeriodS = periodS
	}
	periodUS := predictionPeriodS * 1e6

	// Circular residual between the new observation and our current prediction.
	predicted := s.predictBearingAt(newEpochUS, periodUS)
	residual := circularDiff(newOffsetDeg, predicted)
	absResidual := math.Abs(residual)

	newResidualEMA := (1.0-residualEMAAlpha)*s.ResidualEMA + residualEMAAlpha*absResidual

	// Hard reject.
	if absResidual > residualRejectDeg {
		s.NRejectedFrames++
		s.LastResidualDeg = residual
		s.ResidualEMA = newResidualEMA
		s.SyncJitterDeg = clamp(newResidualEMA, 2.0, 20.0)
		s.Holdover = true
		return false
	}

	// Blending alpha: soft-accept for large residuals, normal EMA otherwise.
	inertia := float64(min(s.NSyncFrames+1, 30))
	alpha := 1.0 / math.Max(inertia, 2.0)
	if absResidual > residualSoftDeg {
		alpha = 0.1
	}

	// Re-express existing sync at the new epoch, then blend in the new observation.
	existingAtNewEpoch := wrap360((newEpochUS-s.PhaseEpochUS)/periodUS*360.0 + s.PhaseOffsetDeg)
	blendedOffset := wrap360(existingAtNewEpoch + alpha*residual)

	s.BasePeriodS = periodS
	s.appendResidualObservation(newEpochUS, residual, nAircraft, refPosAgeS)
	s.applyBoundedPeriodRefinement()
	s.EffectivePeriodS = s.BasePeriodS + s.PeriodDeltaS
	if s.EffectivePeriodS <= 0 {
		s.EffectivePeriodS = s.BasePeriodS
		s.PeriodDeltaS = 0
		s.PeriodRefinementStatus = "reset_invalid_effective_period"
		s.PeriodRejectReason = "invalid_effective_period"
	}
	s.PeriodS = s.EffectivePeriodS
	if math.Abs(s.PeriodDeltaS) > 1e-12 {
		s.PeriodSource = "df_alignment_plus_residual_slope"
	} else {
		s.PeriodSource = "df_alignment"
	}
	s.PeriodAgreesWithDF = true
	s.PhaseEpochUS = newEpochUS
	s.PhaseOffsetDeg = blendedOffset
	s.SyncQuality = quality
	s.SyncJitterDeg = clamp(newResidualEMA, 1.5, 15.0)
	s.ResidualEMA = newResidualEMA
	s.NSyncFrames++
	s.LastResidualDeg = residual
	s.Holdover = false
	s.LastUpdated = time.Now()
	return true
}

func (s *SyncState) resetPeriodRefinement(reason string) {
	s.PeriodDeltaS = 0
	s.ResidualSlopeDegPerS = 0
	s.PeriodRefinementStatus = reason
	s.PeriodRejectReason = ""
	s.residualHistory = nil
	s.EffectivePeriodS = s.BasePeriodS
	s.PeriodS = s.EffectivePeriodS
	s.PeriodSource = "df_alignment"
}

func (s *SyncState) appendResidualObservation(epochUS, residualDeg float64, nAircraft int, refPosAgeS float64) {
	s.residualHistory = append(s.residualHistory, residualObservation{
		EpochUS: epochUS, ResidualDeg: residualDeg, NAircraft: nAircraft, RefPosAgeS: refPosAgeS,
	})
	if len(s.residualHistory) > residualHistoryMax {
		s.residualHistory = s.residualHistory[len(s.residualHistory)-residualHistoryMax:]
	}
	cutoffUS := epochUS - refinementFitWindowS*1e6
	idx := 0
	for idx < len(s.residualHistory) && s.residualHistory[idx].EpochUS < cutoffUS {
		idx++
	}
	if idx > 0 {
		s.residualHistory = s.residualHistory[idx:]
	}
}

func (s *SyncState) applyBoundedPeriodRefinement() {
	if s.BasePeriodS <= 0 {
		s.PeriodDeltaS = 0
		s.ResidualSlopeDegPerS = 0
		s.PeriodRefinementStatus = "missing_df_base_period"
		s.PeriodRejectReason = "missing_df_base_period"
		return
	}
	if len(s.residualHistory) < residualFitMinObs {
		s.ResidualSlopeDegPerS = 0
		s.PeriodRefinementStatus = "insufficient_history"
		s.PeriodRejectReason = ""
		return
	}

	slope, ok, reason := fitResidualSlopeDegPerS(s.residualHistory)
	if !ok {
		s.PeriodDeltaS *= refinementDecayOnReject
		s.ResidualSlopeDegPerS = 0
		s.PeriodRefinementStatus = "fit_invalid_decay"
		s.PeriodRejectReason = reason
		return
	}
	s.ResidualSlopeDegPerS = slope

	// residual = observed - predicted. Positive residual slope means prediction lags;
	// negative period correction increases predicted phase growth and reduces that drift.
	proposedDelta := -slope * s.BasePeriodS * s.BasePeriodS / 360.0
	absBound := s.BasePeriodS * refinementAbsBoundFraction
	if math.Abs(proposedDelta) > absBound {
		s.PeriodDeltaS *= refinementDecayOnReject
		s.PeriodRefinementStatus = "proposed_out_of_bounds_decay"
		s.PeriodRejectReason = "refinement_out_of_bounds"
		return
	}

	slew := s.BasePeriodS * refinementSlewFractionPerStep
	target := clamp(proposedDelta, -absBound, absBound)
	step := target - s.PeriodDeltaS
	step = clamp(step, -slew, slew)
	s.PeriodDeltaS = clamp(s.PeriodDeltaS+step, -absBound, absBound)
	if math.Abs(s.PeriodDeltaS) < 1e-12 {
		s.PeriodDeltaS = 0
	}
	s.PeriodRejectReason = ""
	if s.PeriodDeltaS == 0 {
		s.PeriodRefinementStatus = "base_only"
		return
	}
	if math.Abs(step) > 0 {
		s.PeriodRefinementStatus = "applied_slew_limited"
		return
	}
	s.PeriodRefinementStatus = "held"
}

func fitResidualSlopeDegPerS(obs []residualObservation) (float64, bool, string) {
	if len(obs) < residualFitMinObs {
		return 0, false, "insufficient_history"
	}
	t0US := obs[0].EpochUS
	spanS := (obs[len(obs)-1].EpochUS - t0US) / 1e6
	if spanS < residualFitMinSpanS {
		return 0, false, "insufficient_fit_span"
	}
	var sumT, sumY float64
	for _, o := range obs {
		t := (o.EpochUS - t0US) / 1e6
		sumT += t
		sumY += o.ResidualDeg
	}
	n := float64(len(obs))
	meanT := sumT / n
	meanY := sumY / n
	var num, den float64
	for _, o := range obs {
		t := (o.EpochUS - t0US) / 1e6
		dt := t - meanT
		num += dt * (o.ResidualDeg - meanY)
		den += dt * dt
	}
	if den <= 0 || math.IsNaN(den) || math.IsInf(den, 0) {
		return 0, false, "fit_degenerate"
	}
	slope := num / den
	if math.IsNaN(slope) || math.IsInf(slope, 0) {
		return 0, false, "fit_invalid"
	}
	return slope, true, ""
}

// PredictBearing returns the predicted bearing in [0, 360) for an arrival at
// arrivalUS. Returns -1 if no sync state is available.
//
// NOTE: The Go sync state is anchored on a relative phase offset (0.0 until
// the radar position is known).  This is intentional — Go sync provides a
// phase-tracking signal, not an absolute bearing engine.  Do not assume the
// returned value is a geographic bearing unless PhaseOffsetDeg has been
// properly initialised with a geometric reference.
func (s *SyncState) PredictBearing(arrivalUS float64) float64 {
	if s.EffectivePeriodS <= 0 {
		return -1
	}
	periodUS := s.EffectivePeriodS * 1e6
	return wrap360(s.predictBearingAt(arrivalUS, periodUS))
}

func (s *SyncState) predictBearingAt(arrivalUS, periodUS float64) float64 {
	if periodUS <= 0 {
		return s.PhaseOffsetDeg
	}
	return (arrivalUS-s.PhaseEpochUS)/periodUS*360.0 + s.PhaseOffsetDeg
}

// wrap360 wraps an angle to [0, 360). Unlike math.Mod, this is always
// non-negative even for negative inputs.
func wrap360(deg float64) float64 {
	r := math.Mod(deg, 360.0)
	if r < 0 {
		r += 360.0
	}
	return r
}

// circularDiff returns the signed difference b-a, normalised to (-180, 180].
func circularDiff(b, a float64) float64 {
	d := math.Mod(b-a+540.0, 360.0) - 180.0
	return d
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
