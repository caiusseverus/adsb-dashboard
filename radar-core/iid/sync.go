package iid

import (
	"math"
	"sort"
	"time"
)

// SyncState tracks the live sweep-phase synchronisation for one IID.
// Port of LiveSyncState (core fields only) from sweep.py.
//
// The bearing conversion formula is:
//
//	bearing_deg = ((arrival_us - PhaseEpochUS) / (PeriodS*1e6) * 360 + PhaseOffsetDeg) mod 360
type SyncState struct {
	IID                            uint8
	PeriodS                        float64 // deprecated alias for EffectivePeriodS; kept for tests/compatibility only.
	BasePeriodS                    float64
	PeriodDeltaS                   float64
	EffectivePeriodS               float64
	PhaseEpochUS                   float64 // Beast-monotonic centroid of last accepted reference burst
	PhaseOffsetDeg                 float64 // Geometric bearing from radar to ref aircraft at epoch (0 until radar pos known)
	SyncQuality                    float64 // 0.0–1.0 from rotation model status
	SyncJitterDeg                  float64 // 1-sigma jitter estimate from residual EMA
	ResidualEMA                    float64 // EMA of |circular residual| degrees
	NSyncFrames                    int     // accepted frame count
	NRejectedFrames                int     // rejected frame count
	LastResidualDeg                float64
	Holdover                       bool // true when last update was rejected / too weak
	LastUpdated                    time.Time
	PeriodSource                   string
	PeriodAgreesWithDF             bool
	PeriodRejectReason             string
	ResidualSlopeDegPerS           float64
	PeriodRefinementStatus         string
	RefinementPlottedCount         uint64
	RefinementEligibleCount        uint64
	RefinementRejectedCount        uint64
	RefinementReferenceUpdates     uint64
	RefinementLastRejectReason     string
	RefinementLastObservationUnix  float64
	FitObservationCount            int
	FitSpanS                       float64
	FitICAOCount                   int
	FitPerICAOMin                  int
	FitPerICAOMedian               float64
	FitPerICAOMax                  int
	FitRetentionWindowS            float64
	FitGlobalCapHit                bool
	FitLastEvictionReason          string
	FitEligibleObservations        int
	FitRejectedObservations        int
	SuspiciousICAOCount            int
	SuspiciousICAOLastReason       string
	ResidualSlopeEMADegPerS        float64
	ResidualSlopeStdDegPerS        float64
	ProposedDeltaS                 float64
	AppliedDeltaS                  float64
	LastSlewLimited                bool
	LastHardBound                  bool
	SlopeSignConvention            string
	HoldoverReason                 string
	HoldoverReasonCounts           map[string]uint64
	UpdateEpochAttempts            uint64
	UpdateEpochAccepts             uint64
	UpdateEpochRejects             uint64
	LastUpdateEpochRejectReason    string
	LastUpdateEpochNAircraft       int
	LastUpdateEpochRefPosAgeS      float64
	LastUpdateEpochRefICAO         uint32
	UpdateEpochRejectCounts        map[string]uint64
	LastUpdateEpochStrictGatePass  bool
	LastUpdateEpochResidualDeg     float64
	LastUpdateEpochPredictedDeg    float64
	LastUpdateEpochObservedDeg     float64
	ConsecutiveHardResidualRejects uint64
	LastAcceptedEpochUS            float64
	LastAcceptedEpochAtUnix        float64
	SyncEpochAgeS                  float64
	LastAcceptedEpochAgeS          float64
	CurrentPhaseEpochUS            float64
	CandidateEpochUS               float64
	ReacquiredProvisional          bool
	residualHistory                []residualObservation
	icaoRejectHistory              map[uint32][]float64
	icaoSuspiciousUntil            map[uint32]float64
	slopeHistory                   []float64
}

const (
	// Residual convention used by all refiner inputs:
	// residual_deg = observed_bearing_deg - predicted_bearing_deg, wrapped to [-180, +180].
	residualRejectDeg                  = 50.0 // hard-reject threshold (degrees)
	residualSoftDeg                    = 20.0 // soft-accept threshold
	residualSoftWeight                 = 0.25
	residualEMAAlpha                   = 0.2
	residualFitMinObs                  = 8
	residualFitMinSpanS                = 20.0
	residualFitMinICAOs                = 1
	refinementFitWindowS               = 120.0
	refinementPerICAOCap               = 8
	refinementGlobalCap                = 256
	refinementAbsBoundFraction         = 0.005
	refinementSlewFractionPerStep      = 0.00025
	refinementDecayOnReject            = 0.98
	refinementStalePositionMaxS        = 8.0
	suspiciousRejectThreshold          = 3
	suspiciousRejectWindowS            = 30.0
	suspiciousExcludeWindowS           = 60.0
	slopeEMAAlpha                      = 0.2
	slopeStdWindow                     = 12
	reacquireMinConsecutiveHardRejects = 3
	reacquireFallbackHardRejects       = 8
	reacquireFallbackNoAcceptAgeS      = 30.0
	reacquireMinFitObs                 = 12
	reacquireMinFitICAOs               = 3
)

type residualObservation struct {
	EpochUS     float64
	ResidualDeg float64
	NAircraft   int
	RefPosAgeS  float64
	ICAO        uint32
	Dominant    bool
	Soft        bool
	Weight      float64
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
		IID:                     iid,
		PeriodS:                 periodS,
		BasePeriodS:             periodS,
		PeriodDeltaS:            0,
		EffectivePeriodS:        periodS,
		PhaseEpochUS:            epochUS,
		PhaseOffsetDeg:          phaseOffsetDeg,
		SyncQuality:             quality,
		SyncJitterDeg:           5.0,
		ResidualEMA:             5.0,
		NSyncFrames:             1,
		Holdover:                false,
		LastUpdated:             time.Now(),
		PeriodSource:            "df_alignment",
		PeriodAgreesWithDF:      true,
		PeriodRefinementStatus:  "base_only",
		FitRetentionWindowS:     refinementFitWindowS,
		SlopeSignConvention:     "observed_minus_predicted",
		HoldoverReasonCounts:    make(map[string]uint64),
		UpdateEpochRejectCounts: make(map[string]uint64),
		icaoRejectHistory:       make(map[uint32][]float64),
		icaoSuspiciousUntil:     make(map[uint32]float64),
		LastAcceptedEpochUS:     epochUS,
		LastAcceptedEpochAtUnix: float64(time.Now().UnixNano()) / 1e9,
	}
}

func (s *SyncState) enterHoldover(reason string) {
	s.Holdover = true
	s.HoldoverReason = reason
	if reason != "" {
		s.HoldoverReasonCounts[reason] = s.HoldoverReasonCounts[reason] + 1
	}
}

// UpdateEpoch advances the sync epoch when a new reference aircraft burst arrives.
// nAircraft is the number of aircraft seen in the frame window; refPosAgeS is
// the age of the reference aircraft's position at burst time.
// phaseOffsetDeg is 0.0 until radar position is known.
//
// Returns true if the update was accepted.
func (s *SyncState) rejectUpdateEpoch(reason string) bool {
	s.UpdateEpochRejects++
	s.LastUpdateEpochRejectReason = reason
	if reason != "" {
		s.UpdateEpochRejectCounts[reason] = s.UpdateEpochRejectCounts[reason] + 1
	}
	return false
}

func (s *SyncState) setUpdateEpochDiagnostics(candidateEpochUS, observedDeg, predictedDeg, residualDeg float64) {
	s.CandidateEpochUS = candidateEpochUS
	s.CurrentPhaseEpochUS = s.PhaseEpochUS
	s.LastUpdateEpochObservedDeg = observedDeg
	s.LastUpdateEpochPredictedDeg = predictedDeg
	s.LastUpdateEpochResidualDeg = residualDeg
	nowUnix := float64(time.Now().UnixNano()) / 1e9
	if s.LastAcceptedEpochAtUnix > 0 {
		age := nowUnix - s.LastAcceptedEpochAtUnix
		if age < 0 {
			age = 0
		}
		s.LastAcceptedEpochAgeS = age
	} else {
		s.LastAcceptedEpochAgeS = 0
	}
	if candidateEpochUS > 0 && s.PhaseEpochUS > 0 {
		epochAge := (candidateEpochUS - s.PhaseEpochUS) / 1e6
		if epochAge < 0 {
			epochAge = -epochAge
		}
		s.SyncEpochAgeS = epochAge
	} else {
		s.SyncEpochAgeS = 0
	}
}

func (s *SyncState) canReacquireInHoldover(periodS float64, nAircraft int, refPosAgeS float64, refICAO uint32) bool {
	if periodS <= 0 || math.IsNaN(periodS) || math.IsInf(periodS, 0) {
		return false
	}
	if refICAO == 0 {
		return false
	}
	if refPosAgeS < 0 || math.IsNaN(refPosAgeS) || math.IsInf(refPosAgeS, 0) || refPosAgeS > refinementStalePositionMaxS {
		return false
	}
	if nAircraft < 2 {
		return false
	}
	if s.FitObservationCount < reacquireMinFitObs || s.FitICAOCount < reacquireMinFitICAOs {
		return false
	}
	return true
}

func (s *SyncState) UpdateEpoch(newEpochUS, newOffsetDeg, periodS, quality float64, nAircraft int, refPosAgeS float64, refICAOOpt ...uint32) bool {
	refICAO := uint32(0)
	if len(refICAOOpt) > 0 {
		refICAO = refICAOOpt[0]
	}
	s.UpdateEpochAttempts++
	s.LastUpdateEpochNAircraft = nAircraft
	s.LastUpdateEpochRefPosAgeS = refPosAgeS
	s.LastUpdateEpochRefICAO = refICAO
	s.LastUpdateEpochStrictGatePass = false
	if periodS <= 0 || math.IsNaN(periodS) || math.IsInf(periodS, 0) {
		s.enterHoldover("missing_df_base_period")
		s.PeriodRejectReason = "missing_df_base_period"
		s.PeriodRefinementStatus = "missing_df_base_period"
		s.PeriodDeltaS = 0
		s.EffectivePeriodS = 0
		s.PeriodS = 0
		s.ResidualSlopeDegPerS = 0
		return s.rejectUpdateEpoch("missing_df_base_period")
	}

	// Strict authority gate.
	strictEligible := nAircraft >= 4 || (nAircraft >= 3 && refPosAgeS <= 2.0)
	// Maintenance gate: allow epoch tracking to continue with thinner traffic.
	maintenanceEligible := nAircraft >= 2 && refPosAgeS <= refinementStalePositionMaxS
	if !maintenanceEligible {
		s.enterHoldover("quality_gate_failed")
		if nAircraft < 3 {
			s.enterHoldover("insufficient_aircraft")
		} else {
			s.enterHoldover("stale_reference_position")
		}
		if nAircraft < 2 {
			return s.rejectUpdateEpoch("insufficient_aircraft")
		}
		return s.rejectUpdateEpoch("stale_reference_position")
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
	s.setUpdateEpochDiagnostics(newEpochUS, newOffsetDeg, predicted, residual)

	newResidualEMA := (1.0-residualEMAAlpha)*s.ResidualEMA + residualEMAAlpha*absResidual

	// Hard reject.
	if absResidual > residualRejectDeg {
		s.NRejectedFrames++
		s.LastResidualDeg = residual
		s.ResidualEMA = newResidualEMA
		s.SyncJitterDeg = clamp(newResidualEMA, 2.0, 20.0)
		s.ConsecutiveHardResidualRejects++
		if s.Holdover {
			s.enterHoldover("hard_residual_reject_holdover")
			if s.canReacquireInHoldover(periodS, nAircraft, refPosAgeS, refICAO) &&
				(s.ConsecutiveHardResidualRejects >= reacquireMinConsecutiveHardRejects ||
					(s.ConsecutiveHardResidualRejects >= reacquireFallbackHardRejects && s.LastAcceptedEpochAgeS >= reacquireFallbackNoAcceptAgeS)) {
				s.PhaseEpochUS = newEpochUS
				s.PhaseOffsetDeg = wrap360(newOffsetDeg)
				s.SyncQuality = quality
				s.Holdover = false
				s.HoldoverReason = "reacquired_provisional"
				s.LastUpdateEpochStrictGatePass = false
				s.ReacquiredProvisional = true
				s.UpdateEpochAccepts++
				s.LastUpdateEpochRejectReason = ""
				s.LastUpdated = time.Now()
				s.LastAcceptedEpochUS = newEpochUS
				s.LastAcceptedEpochAtUnix = float64(s.LastUpdated.UnixNano()) / 1e9
				s.ConsecutiveHardResidualRejects = 0
				s.LastAcceptedEpochAgeS = 0
				s.SyncEpochAgeS = 0
				return true
			}
		} else {
			s.enterHoldover("hard_residual_reject")
		}
		return s.rejectUpdateEpoch("hard_residual_reject")
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
	s.appendResidualObservationLocked(newEpochUS, residual, 0, true, false, 1.0, nAircraft, refPosAgeS)
	s.RefinementReferenceUpdates++
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
	s.HoldoverReason = ""
	s.UpdateEpochAccepts++
	s.ReacquiredProvisional = false
	s.LastUpdateEpochRejectReason = ""
	s.LastUpdateEpochStrictGatePass = strictEligible
	s.LastAcceptedEpochUS = newEpochUS
	s.LastAcceptedEpochAtUnix = float64(time.Now().UnixNano()) / 1e9
	s.ConsecutiveHardResidualRejects = 0
	s.LastAcceptedEpochAgeS = 0
	s.SyncEpochAgeS = 0
	s.LastUpdated = time.Now()
	return true
}

func (s *SyncState) resetPeriodRefinement(reason string) {
	s.PeriodDeltaS = 0
	s.ResidualSlopeDegPerS = 0
	s.PeriodRefinementStatus = reason
	s.PeriodRejectReason = ""
	s.residualHistory = nil
	s.slopeHistory = nil
	s.EffectivePeriodS = s.BasePeriodS
	s.PeriodS = s.EffectivePeriodS
	s.PeriodSource = "df_alignment"
	s.FitObservationCount = 0
	s.FitSpanS = 0
	s.FitICAOCount = 0
	s.FitPerICAOMin = 0
	s.FitPerICAOMedian = 0
	s.FitPerICAOMax = 0
	s.FitEligibleObservations = 0
	s.FitRejectedObservations = 0
	s.FitGlobalCapHit = false
	s.FitLastEvictionReason = ""
	s.ProposedDeltaS = 0
	s.AppliedDeltaS = 0
	s.LastSlewLimited = false
	s.LastHardBound = false
	s.ResidualSlopeEMADegPerS = 0
	s.ResidualSlopeStdDegPerS = 0
}

func (s *SyncState) appendResidualObservationLocked(
	epochUS, residualDeg float64,
	icao uint32,
	dominant bool,
	soft bool,
	weight float64,
	nAircraft int,
	refPosAgeS float64,
) {
	if weight <= 0 || math.IsNaN(weight) || math.IsInf(weight, 0) {
		weight = 1.0
	}
	obs := residualObservation{
		EpochUS: epochUS, ResidualDeg: residualDeg, NAircraft: nAircraft, RefPosAgeS: refPosAgeS,
		ICAO: icao, Dominant: dominant, Soft: soft, Weight: weight,
	}
	if icao != 0 {
		count := 0
		for i := len(s.residualHistory) - 1; i >= 0; i-- {
			if s.residualHistory[i].ICAO == icao {
				count++
				if count >= refinementPerICAOCap {
					s.residualHistory = append(s.residualHistory[:i], s.residualHistory[i+1:]...)
					s.FitLastEvictionReason = "per_icao_cap"
					break
				}
			}
		}
	}
	s.residualHistory = append(s.residualHistory, obs)
	cutoffUS := epochUS - refinementFitWindowS*1e6
	idx := 0
	for idx < len(s.residualHistory) && s.residualHistory[idx].EpochUS < cutoffUS {
		idx++
	}
	if idx > 0 {
		s.residualHistory = s.residualHistory[idx:]
		s.FitLastEvictionReason = "window_prune"
	}
	for len(s.residualHistory) > refinementGlobalCap {
		s.residualHistory = s.residualHistory[1:]
		s.FitGlobalCapHit = true
		s.FitLastEvictionReason = "global_cap"
	}
	s.FitObservationCount = len(s.residualHistory)
	s.computeFitDistributionLocked()
}

func (s *SyncState) computeFitDistributionLocked() {
	if len(s.residualHistory) == 0 {
		s.FitSpanS = 0
		s.FitICAOCount = 0
		s.FitPerICAOMin = 0
		s.FitPerICAOMedian = 0
		s.FitPerICAOMax = 0
		return
	}
	first := s.residualHistory[0].EpochUS
	last := s.residualHistory[len(s.residualHistory)-1].EpochUS
	s.FitSpanS = (last - first) / 1e6
	byICAO := map[uint32]int{}
	for _, o := range s.residualHistory {
		if o.ICAO != 0 {
			byICAO[o.ICAO]++
		}
	}
	s.FitICAOCount = len(byICAO)
	if len(byICAO) == 0 {
		s.FitPerICAOMin = 0
		s.FitPerICAOMedian = 0
		s.FitPerICAOMax = 0
		return
	}
	per := make([]int, 0, len(byICAO))
	for _, c := range byICAO {
		per = append(per, c)
	}
	sort.Ints(per)
	s.FitPerICAOMin = per[0]
	s.FitPerICAOMax = per[len(per)-1]
	if len(per)%2 == 0 {
		i := len(per) / 2
		s.FitPerICAOMedian = float64(per[i-1]+per[i]) / 2.0
	} else {
		s.FitPerICAOMedian = float64(per[len(per)/2])
	}
}

func (s *SyncState) markICAORejectLocked(icao uint32, nowS float64, reason string) {
	if icao == 0 {
		return
	}
	h := append(s.icaoRejectHistory[icao], nowS)
	cutoff := nowS - suspiciousRejectWindowS
	idx := 0
	for idx < len(h) && h[idx] < cutoff {
		idx++
	}
	if idx > 0 {
		h = h[idx:]
	}
	s.icaoRejectHistory[icao] = h
	if len(h) > suspiciousRejectThreshold {
		s.icaoSuspiciousUntil[icao] = nowS + suspiciousExcludeWindowS
		s.SuspiciousICAOLastReason = reason
	}
	s.updateSuspiciousCountLocked(nowS)
}

func (s *SyncState) updateSuspiciousCountLocked(nowS float64) {
	count := 0
	for icao, until := range s.icaoSuspiciousUntil {
		if until <= nowS {
			delete(s.icaoSuspiciousUntil, icao)
			continue
		}
		count++
	}
	s.SuspiciousICAOCount = count
}

func (s *SyncState) AddRefinementResidualObservation(
	epochUS float64,
	residualDeg float64,
	icao uint32,
	dominant bool,
	nAircraft int,
	refPosAgeS float64,
) {
	s.RefinementPlottedCount++
	s.RefinementLastObservationUnix = float64(time.Now().UnixNano()) / 1e9
	nowS := s.RefinementLastObservationUnix
	s.updateSuspiciousCountLocked(nowS)
	if s.BasePeriodS <= 0 {
		s.PeriodRefinementStatus = "missing_df_base_period"
		s.RefinementLastRejectReason = "missing_df_base_period"
		s.FitRejectedObservations++
		return
	}
	if !dominant {
		s.RefinementRejectedCount++
		s.FitRejectedObservations++
		s.PeriodRefinementStatus = "no_dominant_family"
		s.RefinementLastRejectReason = "no_dominant_family"
		s.HoldoverReasonCounts["no_dominant_family"] = s.HoldoverReasonCounts["no_dominant_family"] + 1
		s.markICAORejectLocked(icao, nowS, "no_dominant_family")
		return
	}
	if epochUS <= 0 || math.IsNaN(epochUS) || math.IsInf(epochUS, 0) || math.IsNaN(residualDeg) || math.IsInf(residualDeg, 0) {
		s.RefinementRejectedCount++
		s.FitRejectedObservations++
		s.PeriodRefinementStatus = "all_rejected"
		s.RefinementLastRejectReason = "invalid_observation"
		s.markICAORejectLocked(icao, nowS, "invalid_observation")
		return
	}
	if refPosAgeS < 0 || math.IsNaN(refPosAgeS) || math.IsInf(refPosAgeS, 0) {
		s.RefinementRejectedCount++
		s.FitRejectedObservations++
		s.PeriodRefinementStatus = "all_rejected"
		s.RefinementLastRejectReason = "invalid_ref_position_age"
		s.markICAORejectLocked(icao, nowS, "invalid_ref_position_age")
		return
	}
	if refPosAgeS > refinementStalePositionMaxS {
		s.RefinementRejectedCount++
		s.FitRejectedObservations++
		s.PeriodRefinementStatus = "all_rejected"
		s.RefinementLastRejectReason = "stale_ref_position"
		s.markICAORejectLocked(icao, nowS, "stale_ref_position")
		return
	}
	absResidual := math.Abs(residualDeg)
	if absResidual > residualRejectDeg {
		s.RefinementRejectedCount++
		s.FitRejectedObservations++
		s.PeriodRefinementStatus = "all_rejected"
		s.RefinementLastRejectReason = "hard_outlier"
		s.markICAORejectLocked(icao, nowS, "hard_outlier")
		return
	}
	if until, ok := s.icaoSuspiciousUntil[icao]; ok && until > nowS {
		s.RefinementRejectedCount++
		s.FitRejectedObservations++
		s.PeriodRefinementStatus = "all_rejected"
		s.RefinementLastRejectReason = "suspicious_icao_excluded"
		return
	}
	soft := absResidual > residualSoftDeg
	weight := 1.0
	if soft {
		weight = residualSoftWeight
	}
	s.RefinementEligibleCount++
	s.FitEligibleObservations++
	s.appendResidualObservationLocked(epochUS, residualDeg, icao, dominant, soft, weight, nAircraft, refPosAgeS)
	s.applyBoundedPeriodRefinement()
	s.EffectivePeriodS = s.BasePeriodS + s.PeriodDeltaS
	if s.EffectivePeriodS <= 0 {
		s.EffectivePeriodS = s.BasePeriodS
		s.PeriodDeltaS = 0
	}
	s.PeriodS = s.EffectivePeriodS
	if math.Abs(s.PeriodDeltaS) > 1e-12 {
		s.PeriodSource = "df_alignment_plus_residual_slope"
	} else {
		s.PeriodSource = "df_alignment"
	}
	s.RefinementLastRejectReason = ""
}

func (s *SyncState) AddRefinementResidualValue(
	epochUS float64,
	residualDeg float64,
	icao uint32,
	dominant bool,
	nAircraft int,
	refPosAgeS float64,
) {
	s.AddRefinementResidualObservation(epochUS, residualDeg, icao, dominant, nAircraft, refPosAgeS)
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
	if s.FitSpanS < residualFitMinSpanS {
		s.ResidualSlopeDegPerS = 0
		s.PeriodRefinementStatus = "insufficient_span"
		s.PeriodRejectReason = ""
		return
	}
	if s.FitICAOCount > 0 && s.FitICAOCount < residualFitMinICAOs {
		s.ResidualSlopeDegPerS = 0
		s.PeriodRefinementStatus = "insufficient_icaos"
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
	s.updateSlopeDiagnosticsLocked(slope)

	// residual = observed - predicted. Positive residual slope means prediction lags;
	// negative period correction increases predicted phase growth and reduces that drift.
	proposedDelta := -slope * s.BasePeriodS * s.BasePeriodS / 360.0
	s.ProposedDeltaS = proposedDelta
	s.LastSlewLimited = false
	s.LastHardBound = false
	absBound := s.BasePeriodS * refinementAbsBoundFraction
	if math.Abs(proposedDelta) > absBound {
		s.PeriodDeltaS *= refinementDecayOnReject
		s.PeriodRefinementStatus = "proposed_out_of_bounds_decay"
		s.PeriodRejectReason = "refinement_out_of_bounds"
		s.LastHardBound = true
		s.AppliedDeltaS = 0
		return
	}

	slew := s.BasePeriodS * refinementSlewFractionPerStep
	target := clamp(proposedDelta, -absBound, absBound)
	step := target - s.PeriodDeltaS
	rawStep := step
	step = clamp(step, -slew, slew)
	if math.Abs(rawStep-step) > 1e-12 {
		s.LastSlewLimited = true
	}
	s.PeriodDeltaS = clamp(s.PeriodDeltaS+step, -absBound, absBound)
	s.AppliedDeltaS = step
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

func (s *SyncState) updateSlopeDiagnosticsLocked(slope float64) {
	s.ResidualSlopeEMADegPerS = (1.0-slopeEMAAlpha)*s.ResidualSlopeEMADegPerS + slopeEMAAlpha*slope
	s.slopeHistory = append(s.slopeHistory, slope)
	if len(s.slopeHistory) > slopeStdWindow {
		s.slopeHistory = s.slopeHistory[len(s.slopeHistory)-slopeStdWindow:]
	}
	if len(s.slopeHistory) == 0 {
		s.ResidualSlopeStdDegPerS = 0
		return
	}
	var mean float64
	for _, v := range s.slopeHistory {
		mean += v
	}
	mean /= float64(len(s.slopeHistory))
	var ss float64
	for _, v := range s.slopeHistory {
		d := v - mean
		ss += d * d
	}
	s.ResidualSlopeStdDegPerS = math.Sqrt(ss / float64(len(s.slopeHistory)))
}

func fitResidualSlopeDegPerS(obs []residualObservation) (float64, bool, string) {
	if len(obs) < residualFitMinObs {
		return 0, false, "insufficient_history"
	}

	valid := make([]residualObservation, 0, len(obs))
	for _, o := range obs {
		if o.EpochUS <= 0 || math.IsNaN(o.EpochUS) || math.IsInf(o.EpochUS, 0) {
			continue
		}
		if math.IsNaN(o.ResidualDeg) || math.IsInf(o.ResidualDeg, 0) {
			continue
		}
		w := o.Weight
		if w <= 0 || math.IsNaN(w) || math.IsInf(w, 0) {
			continue
		}
		valid = append(valid, o)
	}
	if len(valid) < residualFitMinObs {
		return 0, false, "insufficient_history"
	}
	sort.Slice(valid, func(i, j int) bool { return valid[i].EpochUS < valid[j].EpochUS })

	t0US := valid[0].EpochUS
	spanS := (valid[len(valid)-1].EpochUS - t0US) / 1e6
	if spanS < residualFitMinSpanS {
		return 0, false, "insufficient_fit_span"
	}

	unwrapped := make([]float64, len(valid))
	unwrapped[0] = valid[0].ResidualDeg
	prev := unwrapped[0]
	for i := 1; i < len(valid); i++ {
		candidate := valid[i].ResidualDeg
		for candidate-prev > 180.0 {
			candidate -= 360.0
		}
		for candidate-prev < -180.0 {
			candidate += 360.0
		}
		unwrapped[i] = candidate
		prev = candidate
	}

	var sumW, sumT, sumY float64
	for i, o := range valid {
		w := o.Weight
		t := (o.EpochUS - t0US) / 1e6
		sumW += w
		sumT += w * t
		sumY += w * unwrapped[i]
	}
	if sumW <= 0 {
		return 0, false, "fit_degenerate"
	}
	meanT := sumT / sumW
	meanY := sumY / sumW
	var num, den float64
	for i, o := range valid {
		w := o.Weight
		t := (o.EpochUS - t0US) / 1e6
		dt := t - meanT
		num += w * dt * (unwrapped[i] - meanY)
		den += w * dt * dt
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
