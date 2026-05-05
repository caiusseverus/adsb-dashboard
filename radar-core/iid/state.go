// Package iid manages per-IID rotation model state for radar-core Stage 2.
package iid

import (
	"math"
	"sort"
	"sync"
	"time"

	rcconfig "github.com/caiusseverus/adsb-dashboard/radar-core/config"
)

// BurstRecord is a compact record emitted when a burst fires.
type BurstRecord struct {
	ICAO       uint32
	CentroidUS float64
	NReplies   int
	FiredAt    time.Time
}

// RotationModel holds the derived rotation model for one IID.
type RotationModel struct {
	DominantPeriodS    *float64
	Status             string // SINGLE_RADAR, LIKELY_SINGLE, CHECK_MULTI, INSUFFICIENT_DATA
	NQualifying        int
	NHarmonic          int
	NResidual          int
	RPM                *float64
	PeriodStdS         float64
	PrimaryDirectCount int
	// ICAO family membership — populated by AnalyseBurstRecords.
	Family *ICAOFamily
}

// IIDState holds all runtime state for one interrogator identifier.
type IIDState struct {
	IID uint8

	// Burst records — bounded ring, protected by mu.
	mu      sync.Mutex
	records []BurstRecord
	dirty   bool // pending rotation analysis
	// Dynamic burst-retention diagnostics.
	burstDynamicCap    int
	burstCapHitsTotal  uint64
	lastActiveAircraft int

	// Reinforced period state (updated by reinforce).
	PeriodS            *float64
	PrimarySupport     int
	PeriodStdS         float64
	RPM                *float64
	Status             string
	MultiRadarFlag     bool
	LastRotationModel  *RotationModel
	LastUpdated        time.Time
	BasePeriodS        *float64
	PeriodDeltaS       float64
	EffectivePeriodS   *float64
	PeriodSource       string
	PeriodAgreesWithDF bool
	PeriodRejectReason string

	// Compact rotation period vs DF base (diagnostic-only; does not gate holdover).
	CompactPeriodAgreesWithDF   bool
	CompactPeriodDiagnosticReason string
	CompactPeriodDisagreementS    float64
	CompactPeriodDisagreementPPM  float64

	// Stage 3: reference aircraft and compact sync state.
	RefICAO *uint32 // selected reference aircraft (nil until stable)
	Sync    *SyncState

	LastRefSelHasPosition  bool    // chosen reference has a fresh position in Go cache
	LastRefSelPositionAgeS float64 // age in seconds, -1 if not in cache
	LastRefSelMissingReason string  // "not_in_cache", "stale", or ""
}

// DebugSnapshot is a point-in-time operational view of one IID.
type DebugSnapshot struct {
	Status                                   string
	HasPeriod                                bool
	PeriodS                                  float64
	HasRefICAO                               bool
	RefICAO                                  uint32
	SyncPresent                              bool
	SyncQuality                              float64
	SyncUsable                               bool
	SyncPeriodS                              float64
	SyncPhaseEpochUS                         float64
	SyncPhaseOffsetDeg                       float64
	SyncJitterDeg                            float64
	SyncResidualEMA                          float64
	SyncLastResidualDeg                      float64
	SyncHoldover                             bool
	SyncNSyncFrames                          int
	SyncNRejectedFrames                      int
	SyncLastUpdatedUnix                      float64
	BasePeriodS                              float64
	PeriodDeltaS                             float64
	EffectivePeriodS                         float64
	PeriodSource                             string
	PeriodAgreesWithDF                       bool
	PeriodRejectReason                       string
	CompactPeriodAgreesWithDF                bool
	CompactPeriodDiagnosticReason            string
	CompactPeriodDisagreementS               float64
	CompactPeriodDisagreementPPM             float64
	ResidualSlopeDegPerS                     float64
	PeriodRefinementStatus                   string
	RefinementPlottedCount                   uint64
	RefinementEligibleCount                  uint64
	RefinementRejectedCount                  uint64
	RefinementReferenceUpdates               uint64
	RefinementLastRejectReason               string
	RefinementLastObservationAgeS            float64
	RefinementHistoryLen                     int
	FitObservationCount                      int
	FitSpanS                                 float64
	FitICAOCount                             int
	FitObservationsPerICAOMin                int
	FitObservationsPerICAOMedian             float64
	FitObservationsPerICAOMax                int
	FitRetentionWindowS                      float64
	FitGlobalCapHit                          bool
	FitLastEvictionReason                    string
	FitInlierRatio                           float64
	SuspiciousICAOCount                      int
	SuspiciousICAOLastReason                 string
	ResidualSlopeEMADegPerS                  float64
	ResidualSlopeStdDegPerS                  float64
	ProposedDeltaS                           float64
	AppliedDeltaS                            float64
	LastSlewLimited                          bool
	LastHardBound                            bool
	HardBoundReason                          string
	HardBoundLimitS                          float64
	HardBoundLimitPPM                        float64
	LastRejectedDeltaS                       float64
	LastRejectedDeltaReason                  string
	LastRejectedDeltaEpochID                 uint64
	LastRejectedDeltaPPM                     float64
	RequestedDeltaS                          float64
	RequestedDeltaPPM                        float64
	CurrentDeltaS                            float64
	CurrentDeltaPPM                          float64
	DeltaToBaseS                             float64
	DeltaToBasePPM                           float64
	DFBasePeriodS                            float64
	PeriodDisagreementS                      float64
	PeriodDisagreementPPM                    float64
	FitEpochID                               uint64
	FitEpochStartedUnix                      float64
	FitEpochResetReason                      string
	FitEpochObservationCount                 int
	FitEpochSpanS                            float64
	FitDroppedOnEpochReset                   int
	FitSegmentCount                          uint64
	SlopeSignConvention                      string
	HoldoverReason                           string
	HoldoverQualityGateFailed                uint64
	HoldoverMissingDFBasePeriod              uint64
	HoldoverHardResidualReject               uint64
	HoldoverNoReference                      uint64
	HoldoverStaleReferencePosition           uint64
	HoldoverPeriodDisagreement               uint64
	HoldoverInsufficientAircraft             uint64
	HoldoverNoDominantFamily                 uint64
	HoldoverSyncStateMissing                 uint64
	UpdateEpochAttempts                      uint64
	UpdateEpochAccepts                       uint64
	UpdateEpochRejects                       uint64
	LastUpdateEpochRejectReason              string
	LastUpdateEpochNAircraft                 int
	LastUpdateEpochRefPosAgeS                float64
	LastUpdateEpochRefICAO                   uint32
	UpdateEpochRejectQualityGate             uint64
	UpdateEpochRejectMissingBase             uint64
	UpdateEpochRejectHardResidual            uint64
	UpdateEpochRejectNoReference             uint64
	UpdateEpochRejectStaleRefPos             uint64
	UpdateEpochRejectInsufficientAC          uint64
	UpdateEpochLastStrictGatePass            bool
	LastUpdateEpochResidualDeg               float64
	LastUpdateEpochRawResidualDeg            float64
	LastUpdateEpochWrappedResidualDeg        float64
	LastUpdateEpochPredictedDeg              float64
	LastUpdateEpochPredictedWrappedDeg       float64
	LastUpdateEpochObservedDeg               float64
	LastUpdateEpochRefBearingDeg             float64
	LastUpdateEpochRefRangeNM                float64
	HardRejectTransitionConsecutiveBefore    uint64
	HardRejectTransitionConsecutiveAfter     uint64
	HardRejectEnteredHoldover                bool
	HardRejectGateReason                     string
	ConsecutiveHardResidualRejects           uint64
	ConsecutiveHardBoundRejects              uint64
	LastAcceptedEpochAgeS                    float64
	SyncEpochAgeS                            float64
	CurrentPhaseEpochUS                      float64
	CandidateEpochUS                         float64
	ReacquiredProvisional                    bool
	LastUpdateEpochRawCandidateAircraftCount int
	LastUpdateEpochPositionedAircraftCount   int
	LastUpdateEpochDominantAircraftCount     int
	LastUpdateEpochFrameObservationCount     int
	LastUpdateEpochInputReferenceICAO        uint32
	LastUpdateEpochInputReferenceBurstUS     float64
	LastUpdateEpochInputReferencePosAgeS     float64
	LastUpdateEpochExcludedMissingPosition   int
	LastUpdateEpochExcludedStalePosition     int
	LastUpdateEpochExcludedNotDominant       int
	LastUpdateEpochExcludedOutsideWindow     int
	ActiveAircraftEstimate                   int
	BurstRecordsTotal                        int
	BurstRecordsDynamicCap                   int
	BurstRecordsCapHit                       bool
	BurstRecordsCapHitsTotal                 uint64
	BurstRecordsRetainedSpanS                float64
	BurstRecordsICAOs                        int
	BurstRecordsPerICAOMin                   int
	BurstRecordsPerICAOMedian                float64
	BurstRecordsPerICAOMax                   int
	ReferenceEligibleAircraft                int
	DominantFamilyAircraft                   int
	ReferenceSelectionSparseHistory          bool
	FitEpochResetCountByReason               map[string]uint64
	LastFitEpochResetAgeS                    float64
	FitObservationsAddedSinceReset           int
	FitObservationsRejectedSinceReset        int
	LastFitObservationRejectReason           string
	CurrentReferenceICAO                     uint32
	PreviousReferenceICAO                    uint32
	ReferenceChangeCount                     uint64
	ReferenceChurnRate                       float64
	ObservationDropCountsByReason            map[string]uint64
	HoldoverMissingReferencePosition         uint64
	UpdateEpochRejectMissingRefPos           uint64
	LastRefSelHasPosition                    bool
	LastRefSelPositionAgeS                   float64
	LastRefSelMissingReason                  string
	ReacquireSupportObservationCount         int
	ReacquireSupportICAOCount                int
}

const (
	burstRecordsTargetSweepsPerAircraft = 16.0
	burstRecordsHeadroom                = 1.4
	burstRecordsMinPerIID               = 800
	burstRecordsMaxPerIID               = 8000
	dfAgreementToleranceFraction        = 0.01
	// Reset refinement only when DF base changes materially. 0.5% is large
	// enough to ignore normal DF-estimate jitter while still handling true base
	// period shifts.
	basePeriodChangeResetThreshold = 0.005
)

// NewIIDState creates an IIDState for the given IID.
func NewIIDState(iid uint8) *IIDState {
	return &IIDState{
		IID:    iid,
		Status: "UNKNOWN",
	}
}

// AddBurst appends a burst record for this IID, enforcing age and
// density-aware count caps.
func (s *IIDState) AddBurst(icao uint32, centroidUS float64, nReplies int, activeAircraft int) {
	cfg := rcconfig.Get()
	now := time.Now()
	cutoff := now.Add(-time.Duration(cfg.BurstRecordMaxAgeS) * time.Second)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Evict stale entries from the front.
	i := 0
	for i < len(s.records) && s.records[i].FiredAt.Before(cutoff) {
		i++
	}
	if i > 0 {
		s.records = s.records[i:]
	}

	cap := dynamicBurstRecordCap(activeAircraft)
	if cfg.BurstRecordsMaxPerIID > 0 && cfg.BurstRecordsMaxPerIID < cap {
		cap = cfg.BurstRecordsMaxPerIID
	}
	if cap < burstRecordsMinPerIID {
		cap = burstRecordsMinPerIID
	}
	s.burstDynamicCap = cap
	s.lastActiveAircraft = maxInt(activeAircraft, 1)

	// Cap per-IID count.
	for len(s.records) >= cap {
		s.records = s.records[1:]
		s.burstCapHitsTotal++
	}

	s.records = append(s.records, BurstRecord{
		ICAO:       icao,
		CentroidUS: centroidUS,
		NReplies:   nReplies,
		FiredAt:    now,
	})
	s.dirty = true
}

func dynamicBurstRecordCap(activeAircraft int) int {
	active := maxInt(activeAircraft, 1)
	scaled := int(math.Round(float64(active) * burstRecordsTargetSweepsPerAircraft * burstRecordsHeadroom))
	if scaled < burstRecordsMinPerIID {
		return burstRecordsMinPerIID
	}
	if scaled > burstRecordsMaxPerIID {
		return burstRecordsMaxPerIID
	}
	return scaled
}

// TakeIfDirty returns a snapshot of current records if the state is dirty,
// then clears the dirty flag. Returns nil if not dirty or insufficient records.
func (s *IIDState) TakeIfDirty() []BurstRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.dirty {
		return nil
	}
	if len(s.records) == 0 {
		return nil
	}
	snap := make([]BurstRecord, len(s.records))
	copy(snap, s.records)
	s.dirty = false
	return snap
}

// ApplyRotation stores the result of a rotation analysis pass.
func (s *IIDState) ApplyRotation(model *RotationModel) {
	s.mu.Lock()
	defer s.mu.Unlock()
	reinforce(s, model)
	s.LastRotationModel = model
	s.LastUpdated = time.Now()
}

// Reset clears all burst records and reinforced state for this IID.
func (s *IIDState) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = s.records[:0]
	s.dirty = false
	s.PeriodS = nil
	s.PrimarySupport = 0
	s.RPM = nil
	s.Status = "UNKNOWN"
	s.MultiRadarFlag = false
	s.LastRotationModel = nil
	s.RefICAO = nil
	s.Sync = nil
	s.BasePeriodS = nil
	s.PeriodDeltaS = 0
	s.EffectivePeriodS = nil
	s.PeriodSource = ""
	s.PeriodAgreesWithDF = false
	s.PeriodRejectReason = ""
	s.CompactPeriodAgreesWithDF = false
	s.CompactPeriodDiagnosticReason = ""
	s.CompactPeriodDisagreementS = 0
	s.CompactPeriodDisagreementPPM = 0
}

// SetBasePeriod stores the Python DF-alignment-derived period used by
// operational sync and frame generation. Go rotation analysis is diagnostic
// only for this authority path.
func (s *IIDState) SetBasePeriod(periodS float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if periodS <= 0 || math.IsNaN(periodS) || math.IsInf(periodS, 0) {
		s.BasePeriodS = nil
		s.EffectivePeriodS = nil
		s.PeriodDeltaS = 0
		s.PeriodSource = ""
		s.PeriodAgreesWithDF = false
		s.PeriodRejectReason = "missing_df_base_period"
		s.CompactPeriodAgreesWithDF = false
		s.CompactPeriodDiagnosticReason = "missing_df_base_period"
		if s.Sync != nil {
			refICAO := uint32(0)
			if s.RefICAO != nil {
				refICAO = *s.RefICAO
			}
			s.Sync.Holdover = true
			s.Sync.PeriodS = 0
			s.Sync.PeriodRejectReason = "missing_df_base_period"
			s.Sync.PeriodAgreesWithDF = false
			s.Sync.ResetFitEpochOnModelChange("base_period_unavailable", refICAO, "go_refiner_holdover")
		}
		return
	}
	var prevBase float64
	hadPrevBase := s.BasePeriodS != nil && *s.BasePeriodS > 0
	if hadPrevBase {
		prevBase = *s.BasePeriodS
	}
	s.BasePeriodS = &periodS
	if s.Sync != nil {
		if !hadPrevBase {
			s.Sync.resetPeriodRefinement("base_period_initialized")
		} else {
			relChange := math.Abs(periodS-prevBase) / math.Max(prevBase, periodS)
			if relChange > basePeriodChangeResetThreshold {
				s.Sync.resetPeriodRefinement("base_period_changed_reset")
			}
		}
		s.Sync.BasePeriodS = periodS
		s.Sync.EffectivePeriodS = s.Sync.BasePeriodS + s.Sync.PeriodDeltaS
		if s.Sync.EffectivePeriodS <= 0 {
			s.Sync.resetPeriodRefinement("reset_invalid_effective_period")
		}
		s.Sync.PeriodS = s.Sync.EffectivePeriodS
		if math.Abs(s.Sync.PeriodDeltaS) > 1e-12 {
			s.Sync.PeriodSource = "df_alignment_plus_residual_slope"
		} else {
			s.Sync.PeriodSource = "df_alignment"
		}
		s.PeriodDeltaS = s.Sync.PeriodDeltaS
		effective := s.Sync.EffectivePeriodS
		s.EffectivePeriodS = &effective
		s.PeriodSource = s.Sync.PeriodSource
	} else {
		s.PeriodDeltaS = 0
		s.EffectivePeriodS = &periodS
		s.PeriodSource = "df_alignment"
	}

	// ── Operational check: refined effective period vs DF base ──────────
	s.PeriodAgreesWithDF = true
	s.PeriodRejectReason = ""
	if s.Sync != nil {
		refinedEffective := s.Sync.BasePeriodS + s.Sync.PeriodDeltaS
		s.PeriodAgreesWithDF = periodsAgree(refinedEffective, periodS)
		if !s.PeriodAgreesWithDF {
			s.PeriodRejectReason = "refined_period_disagrees_with_df"
		}
	}
	if s.Sync != nil {
		s.Sync.PeriodAgreesWithDF = s.PeriodAgreesWithDF
		s.Sync.PeriodRejectReason = s.PeriodRejectReason
		if s.PeriodRejectReason == "refined_period_disagrees_with_df" {
			s.Sync.enterHoldover("period_disagreement")
		}
	}

	// ── Diagnostic: compact rotation period vs DF base ─────────────────
	s.CompactPeriodAgreesWithDF = false
	s.CompactPeriodDiagnosticReason = ""
	s.CompactPeriodDisagreementS = 0
	s.CompactPeriodDisagreementPPM = 0
	if s.PeriodS != nil {
		s.CompactPeriodAgreesWithDF = periodsAgree(*s.PeriodS, periodS)
		if !s.CompactPeriodAgreesWithDF {
			s.CompactPeriodDisagreementS = *s.PeriodS - periodS
			if periodS > 0 {
				s.CompactPeriodDisagreementPPM = s.CompactPeriodDisagreementS / periodS * 1e6
			}
			s.CompactPeriodDiagnosticReason = "compact_period_disagrees_with_df"
		}
	}
}

// OperationalPeriodSnapshot returns the DF-authoritative effective period.
func (s *IIDState) OperationalPeriodSnapshot() *float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.EffectivePeriodS == nil || *s.EffectivePeriodS <= 0 {
		return nil
	}
	v := *s.EffectivePeriodS
	return &v
}

// RefreshReference re-evaluates reference aircraft selection from current records.
// Called from the rotation analysis ticker after ApplyRotation.
// Records must be the same snapshot used for rotation analysis.
func (s *IIDState) RefreshReference(records []BurstRecord, nowUS float64, positions *PositionCache) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.EffectivePeriodS == nil {
		return
	}
	cur := uint32(0)
	if s.RefICAO != nil {
		cur = *s.RefICAO
	}

	// Keep reference selection aligned with Stage-4 frame gating:
	// frames only open when the reference is in the dominant family.
	filtered := records
	if s.goFamilyAgreesWithDFLocked() && s.LastRotationModel != nil && s.LastRotationModel.Family != nil && len(s.LastRotationModel.Family.FoldedICAOs) > 0 {
		folded := s.LastRotationModel.Family.FoldedICAOs
		filtered = make([]BurstRecord, 0, len(records))
		for _, r := range records {
			if _, ok := folded[r.ICAO]; ok {
				filtered = append(filtered, r)
			}
		}
		if _, ok := folded[cur]; !ok {
			cur = 0
		}
	}

	chosen := SelectReference(filtered, *s.EffectivePeriodS, cur, nowUS, positions)
	if chosen != 0 {
		if s.Sync != nil && cur != 0 && chosen != cur {
			s.Sync.RecordReferenceChange(chosen)
		}
		s.RefICAO = &chosen
		// Record position diagnostics for the chosen reference.
		if positions != nil {
			posRaw, exists := positions.GetRaw(chosen)
			if !exists {
				s.LastRefSelHasPosition = false
				s.LastRefSelPositionAgeS = -1.0
				s.LastRefSelMissingReason = "not_in_cache"
			} else {
				ageS := time.Since(posRaw.TS).Seconds()
				s.LastRefSelPositionAgeS = ageS
				if ageS <= refinementStalePositionMaxS {
					s.LastRefSelHasPosition = true
					s.LastRefSelMissingReason = ""
				} else {
					s.LastRefSelHasPosition = false
					s.LastRefSelMissingReason = "stale"
				}
			}
		} else {
			s.LastRefSelHasPosition = false
			s.LastRefSelPositionAgeS = -1.0
			s.LastRefSelMissingReason = "not_in_cache"
		}
		return
	}
	// If no dominant-family candidate is available, clear the reference so
	// downstream gates expose "no_reference" rather than a permanently
	// non-dominant reference that can never open frames.
	if len(filtered) == 0 {
		if s.Sync != nil && s.RefICAO != nil {
			s.Sync.RecordReferenceChange(0)
		}
		s.RefICAO = nil
	}
}

// UpdateSyncEpoch advances the sync epoch when the reference aircraft fires.
// epochUS is its burst centroid; nAircraft is the count of aircraft in the
// frame window. phaseOffsetDeg is 0 until radar position is known (Stage 4+).
func (s *IIDState) UpdateSyncEpoch(epochUS, phaseOffsetDeg float64, nAircraft int, refPosAgeS float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.BasePeriodS == nil || *s.BasePeriodS <= 0 {
		if s.Sync != nil {
			s.Sync.enterHoldover("missing_df_base_period")
			s.Sync.PeriodAgreesWithDF = false
			s.Sync.PeriodRejectReason = "missing_df_base_period"
			refICAO := uint32(0)
			if s.RefICAO != nil {
				refICAO = *s.RefICAO
			}
			s.Sync.ResetFitEpochOnModelChange("missing_df_base_period", refICAO, "go_refiner_holdover")
		}
		return
	}
	quality := syncQuality(s.Status, s.BasePeriodS != nil)
	if s.Sync == nil {
		// Bootstrap on first reference burst.
		eligible := nAircraft >= 2 && refPosAgeS <= refinementStalePositionMaxS
		if !eligible {
			// No live sync state to update yet, but preserve a diagnostic reason bucket.
			// This is mirrored in DebugStateSnapshot as sync_state_missing-related holdover pressure.
			return
		}
		s.Sync = NewSyncState(s.IID, *s.EffectivePeriodS, epochUS, phaseOffsetDeg, quality)
		s.Sync.PeriodAgreesWithDF = s.PeriodAgreesWithDF
		s.Sync.PeriodRejectReason = s.PeriodRejectReason
		return
	}
	refICAO := uint32(0)
	if s.RefICAO != nil {
		refICAO = *s.RefICAO
	}
	s.Sync.UpdateEpoch(epochUS, phaseOffsetDeg, *s.BasePeriodS, quality, nAircraft, refPosAgeS, refICAO)
	s.PeriodDeltaS = s.Sync.PeriodDeltaS
	effective := s.Sync.EffectivePeriodS
	s.EffectivePeriodS = &effective
	s.PeriodSource = s.Sync.PeriodSource
	s.Sync.PeriodAgreesWithDF = s.PeriodAgreesWithDF
	s.Sync.PeriodRejectReason = s.PeriodRejectReason
}

// BurstRecordsWindow returns a copy of burst records within +/-halfWindowUS around centerEpochUS.
func (s *IIDState) BurstRecordsWindow(centerEpochUS, halfWindowUS float64) []BurstRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	if halfWindowUS <= 0 {
		return nil
	}
	start := centerEpochUS - halfWindowUS
	end := centerEpochUS + halfWindowUS
	out := make([]BurstRecord, 0, len(s.records))
	for _, rec := range s.records {
		if rec.CentroidUS < start || rec.CentroidUS > end {
			continue
		}
		out = append(out, rec)
	}
	return out
}

func (s *IIDState) RecordBurstResidualObservation(
	epochUS float64,
	icao uint32,
	observedBearingDeg float64,
	nAircraft int,
	refPosAgeS float64,
	dominantFamily bool,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Sync == nil {
		return
	}
	if s.BasePeriodS == nil || *s.BasePeriodS <= 0 {
		s.Sync.PeriodRefinementStatus = "missing_df_base_period"
		s.Sync.enterHoldover("missing_df_base_period")
		return
	}
	if s.RefICAO == nil {
		s.Sync.PeriodRefinementStatus = "no_reference"
		s.Sync.RefinementLastRejectReason = "no_reference"
		s.Sync.enterHoldover("no_reference")
		return
	}
	predicted := s.Sync.PredictBearing(epochUS)
	if predicted < 0 || math.IsNaN(predicted) || math.IsInf(predicted, 0) {
		return
	}
	residual := circularDiff(observedBearingDeg, predicted)
	refICAO := uint32(0)
	if s.RefICAO != nil {
		refICAO = *s.RefICAO
	}
	authorityBasis := "go_refiner_active"
	if s.Sync.Holdover {
		authorityBasis = "go_refiner_holdover"
	}
	s.Sync.AddRefinementResidualValue(epochUS, residual, icao, dominantFamily, nAircraft, refPosAgeS, refICAO, authorityBasis)
	s.PeriodDeltaS = s.Sync.PeriodDeltaS
	effective := s.Sync.EffectivePeriodS
	s.EffectivePeriodS = &effective
	s.PeriodSource = s.Sync.PeriodSource
}

func (s *IIDState) RecordBurstResidualValue(
	epochUS float64,
	icao uint32,
	residualDeg float64,
	nAircraft int,
	refPosAgeS float64,
	dominantFamily bool,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Sync == nil {
		return
	}
	if s.BasePeriodS == nil || *s.BasePeriodS <= 0 {
		s.Sync.PeriodRefinementStatus = "missing_df_base_period"
		s.Sync.RefinementLastRejectReason = "missing_df_base_period"
		s.Sync.enterHoldover("missing_df_base_period")
		return
	}
	// Direct residual-value ingestion path: residualDeg is already computed as
	// observed-minus-predicted in the producer path, so this path does not require
	// a selected Go reference aircraft.
	refICAO := uint32(0)
	if s.RefICAO != nil {
		refICAO = *s.RefICAO
	}
	authorityBasis := "go_refiner_active"
	if s.Sync.Holdover {
		authorityBasis = "go_refiner_holdover"
	}
	s.Sync.AddRefinementResidualValue(epochUS, residualDeg, icao, dominantFamily, nAircraft, refPosAgeS, refICAO, authorityBasis)
	s.PeriodDeltaS = s.Sync.PeriodDeltaS
	effective := s.Sync.EffectivePeriodS
	s.EffectivePeriodS = &effective
	s.PeriodSource = s.Sync.PeriodSource
}

// FamilySnapshot returns the current ICAO family membership (may be nil).
// The returned pointer is read-only — callers must not mutate it.
func (s *IIDState) FamilySnapshot() *ICAOFamily {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.goFamilyAgreesWithDFLocked() {
		return nil
	}
	if s.LastRotationModel == nil {
		return nil
	}
	return s.LastRotationModel.Family
}

// SyncSnapshot returns sync state fields for IID_STATE emission.
func (s *IIDState) SyncSnapshot() (quality float32, refICAO *uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Sync != nil {
		quality = float32(s.Sync.SyncQuality)
	}
	return quality, s.RefICAO
}

// SyncProtocolSnapshot returns compact sync fields for protocol emission.
func (s *IIDState) SyncProtocolSnapshot() (
	present bool,
	usable bool,
	periodS *float64,
	phaseEpochUS *float64,
	phaseOffsetDeg *float64,
	jitterDeg *float32,
	residualEMA *float32,
	lastResidual *float32,
	nFrames uint16,
	nRejected uint16,
	holdover bool,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Sync == nil {
		return false, false, nil, nil, nil, nil, nil, nil, 0, 0, false
	}
	present = true
	usable = s.Sync.SyncQuality >= 0.3 && !s.Sync.Holdover && s.Sync.PeriodAgreesWithDF && s.Sync.PeriodRejectReason == "" && s.Sync.LastUpdateEpochStrictGatePass
	holdover = s.Sync.Holdover
	periodValue := s.Sync.EffectivePeriodS
	phaseEpochValue := s.Sync.PhaseEpochUS
	phaseOffsetValue := s.Sync.PhaseOffsetDeg
	jitterValue := float32(s.Sync.SyncJitterDeg)
	residualEMAValue := float32(s.Sync.ResidualEMA)
	lastResidualValue := float32(s.Sync.LastResidualDeg)
	periodS = &periodValue
	phaseEpochUS = &phaseEpochValue
	phaseOffsetDeg = &phaseOffsetValue
	jitterDeg = &jitterValue
	residualEMA = &residualEMAValue
	lastResidual = &lastResidualValue
	if s.Sync.NSyncFrames > 0 {
		if s.Sync.NSyncFrames > math.MaxUint16 {
			nFrames = math.MaxUint16
		} else {
			nFrames = uint16(s.Sync.NSyncFrames)
		}
	}
	if s.Sync.NRejectedFrames > 0 {
		if s.Sync.NRejectedFrames > math.MaxUint16 {
			nRejected = math.MaxUint16
		} else {
			nRejected = uint16(s.Sync.NRejectedFrames)
		}
	}
	return
}

// Snapshot returns a safe copy of the current reinforced state.
func (s *IIDState) Snapshot() (status string, periodS *float64, rpm *float64, support int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Status, s.PeriodS, s.RPM, s.PrimarySupport
}

// DebugStateSnapshot returns current Stage-2/3 state for observability payloads.
func (s *IIDState) DebugStateSnapshot() DebugSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := DebugSnapshot{
		Status: s.Status,
	}
	if s.PeriodS != nil {
		out.HasPeriod = true
		out.PeriodS = *s.PeriodS
	}
	if s.BasePeriodS != nil {
		out.BasePeriodS = *s.BasePeriodS
	}
	if s.EffectivePeriodS != nil {
		out.EffectivePeriodS = *s.EffectivePeriodS
	}
	out.PeriodDeltaS = s.PeriodDeltaS
	out.PeriodSource = s.PeriodSource
	out.PeriodAgreesWithDF = s.PeriodAgreesWithDF
	out.PeriodRejectReason = s.PeriodRejectReason
	out.CompactPeriodAgreesWithDF = s.CompactPeriodAgreesWithDF
	out.CompactPeriodDiagnosticReason = s.CompactPeriodDiagnosticReason
	out.CompactPeriodDisagreementS = s.CompactPeriodDisagreementS
	out.CompactPeriodDisagreementPPM = s.CompactPeriodDisagreementPPM
	if s.Sync != nil {
		out.ResidualSlopeDegPerS = s.Sync.ResidualSlopeDegPerS
		out.ResidualSlopeEMADegPerS = s.Sync.ResidualSlopeEMADegPerS
		out.ResidualSlopeStdDegPerS = s.Sync.ResidualSlopeStdDegPerS
		out.ProposedDeltaS = s.Sync.ProposedDeltaS
		out.AppliedDeltaS = s.Sync.AppliedDeltaS
		out.LastSlewLimited = s.Sync.LastSlewLimited
		out.LastHardBound = s.Sync.LastHardBound
		out.HardBoundReason = s.Sync.HardBoundReason
		out.HardBoundLimitS = s.Sync.HardBoundLimitS
		out.HardBoundLimitPPM = s.Sync.HardBoundLimitPPM
		out.LastRejectedDeltaS = s.Sync.LastRejectedDeltaS
		out.LastRejectedDeltaReason = s.Sync.LastRejectedDeltaReason
		out.LastRejectedDeltaEpochID = s.Sync.LastRejectedDeltaEpochID
		out.LastRejectedDeltaPPM = s.Sync.LastRejectedDeltaPPM
		out.RequestedDeltaS = s.Sync.RequestedDeltaS
		out.RequestedDeltaPPM = s.Sync.RequestedDeltaPPM
		out.CurrentDeltaS = s.Sync.CurrentDeltaS
		out.CurrentDeltaPPM = s.Sync.CurrentDeltaPPM
		out.DeltaToBaseS = s.Sync.DeltaToBaseS
		out.DeltaToBasePPM = s.Sync.DeltaToBasePPM
		out.DFBasePeriodS = s.Sync.DFBasePeriodS
		out.PeriodDisagreementS = s.Sync.PeriodDisagreementS
		out.PeriodDisagreementPPM = s.Sync.PeriodDisagreementPPM
		out.FitEpochID = s.Sync.FitEpochID
		out.FitEpochStartedUnix = s.Sync.FitEpochStartedUnix
		out.FitEpochResetReason = s.Sync.FitEpochResetReason
		out.FitEpochObservationCount = s.Sync.FitEpochObservationCount
		out.FitEpochSpanS = s.Sync.FitEpochSpanS
		out.FitDroppedOnEpochReset = s.Sync.FitDroppedOnEpochReset
		out.FitSegmentCount = s.Sync.FitSegmentCount
		out.SlopeSignConvention = s.Sync.SlopeSignConvention
		out.HoldoverReason = s.Sync.HoldoverReason
		out.HoldoverQualityGateFailed = s.Sync.HoldoverReasonCounts["quality_gate_failed"]
		out.HoldoverMissingDFBasePeriod = s.Sync.HoldoverReasonCounts["missing_df_base_period"]
		out.HoldoverHardResidualReject = s.Sync.HoldoverReasonCounts["hard_residual_reject"]
		out.HoldoverNoReference = s.Sync.HoldoverReasonCounts["no_reference"]
		out.HoldoverStaleReferencePosition = s.Sync.HoldoverReasonCounts["stale_reference_position"]
		out.HoldoverPeriodDisagreement = s.Sync.HoldoverReasonCounts["period_disagreement"]
		out.HoldoverInsufficientAircraft = s.Sync.HoldoverReasonCounts["insufficient_aircraft"]
		out.HoldoverNoDominantFamily = s.Sync.HoldoverReasonCounts["no_dominant_family"]
		out.HoldoverSyncStateMissing = s.Sync.HoldoverReasonCounts["sync_state_missing"]
		out.HoldoverMissingReferencePosition = s.Sync.HoldoverReasonCounts["missing_reference_position"]
		out.UpdateEpochRejectMissingRefPos = s.Sync.UpdateEpochRejectCounts["missing_reference_position"]
		out.UpdateEpochAttempts = s.Sync.UpdateEpochAttempts
		out.UpdateEpochAccepts = s.Sync.UpdateEpochAccepts
		out.UpdateEpochRejects = s.Sync.UpdateEpochRejects
		out.LastUpdateEpochRejectReason = s.Sync.LastUpdateEpochRejectReason
		out.LastUpdateEpochNAircraft = s.Sync.LastUpdateEpochNAircraft
		out.LastUpdateEpochRefPosAgeS = s.Sync.LastUpdateEpochRefPosAgeS
		out.LastUpdateEpochRefICAO = s.Sync.LastUpdateEpochRefICAO
		out.UpdateEpochRejectQualityGate = s.Sync.UpdateEpochRejectCounts["quality_gate_failed"]
		out.UpdateEpochRejectMissingBase = s.Sync.UpdateEpochRejectCounts["missing_df_base_period"]
		out.UpdateEpochRejectHardResidual = s.Sync.UpdateEpochRejectCounts["hard_residual_reject"]
		out.UpdateEpochRejectNoReference = s.Sync.UpdateEpochRejectCounts["no_reference"]
		out.UpdateEpochRejectStaleRefPos = s.Sync.UpdateEpochRejectCounts["stale_reference_position"]
		out.UpdateEpochRejectInsufficientAC = s.Sync.UpdateEpochRejectCounts["insufficient_aircraft"]
		out.UpdateEpochLastStrictGatePass = s.Sync.LastUpdateEpochStrictGatePass
		out.LastUpdateEpochResidualDeg = s.Sync.LastUpdateEpochResidualDeg
		out.LastUpdateEpochRawResidualDeg = s.Sync.LastUpdateEpochRawResidualDeg
		out.LastUpdateEpochWrappedResidualDeg = s.Sync.LastUpdateEpochWrappedResidualDeg
		out.LastUpdateEpochPredictedDeg = s.Sync.LastUpdateEpochPredictedDeg
		out.LastUpdateEpochPredictedWrappedDeg = s.Sync.LastUpdateEpochPredictedWrappedDeg
		out.LastUpdateEpochObservedDeg = s.Sync.LastUpdateEpochObservedDeg
		out.LastUpdateEpochRefBearingDeg = s.Sync.LastUpdateEpochRefBearingDeg
		out.LastUpdateEpochRefRangeNM = s.Sync.LastUpdateEpochRefRangeNM
		out.HardRejectTransitionConsecutiveBefore = s.Sync.HardRejectTransitionConsecutiveBefore
		out.HardRejectTransitionConsecutiveAfter = s.Sync.HardRejectTransitionConsecutiveAfter
		out.HardRejectEnteredHoldover = s.Sync.HardRejectEnteredHoldover
		out.HardRejectGateReason = s.Sync.HardRejectGateReason
		out.ConsecutiveHardResidualRejects = s.Sync.ConsecutiveHardResidualRejects
		out.ConsecutiveHardBoundRejects = s.Sync.ConsecutiveHardBoundRejects
		out.LastAcceptedEpochAgeS = s.Sync.LastAcceptedEpochAgeS
		out.SyncEpochAgeS = s.Sync.SyncEpochAgeS
		out.CurrentPhaseEpochUS = s.Sync.CurrentPhaseEpochUS
		out.CandidateEpochUS = s.Sync.CandidateEpochUS
		out.ReacquiredProvisional = s.Sync.ReacquiredProvisional
		out.LastUpdateEpochRawCandidateAircraftCount = s.Sync.LastUpdateEpochRawCandidateAircraftCount
		out.LastUpdateEpochPositionedAircraftCount = s.Sync.LastUpdateEpochPositionedAircraftCount
		out.LastUpdateEpochDominantAircraftCount = s.Sync.LastUpdateEpochDominantAircraftCount
		out.LastUpdateEpochFrameObservationCount = s.Sync.LastUpdateEpochFrameObservationCount
		out.LastUpdateEpochInputReferenceICAO = s.Sync.LastUpdateEpochInputReferenceICAO
		out.LastUpdateEpochInputReferenceBurstUS = s.Sync.LastUpdateEpochInputReferenceBurstUS
		out.LastUpdateEpochInputReferencePosAgeS = s.Sync.LastUpdateEpochInputReferencePosAgeS
		out.LastUpdateEpochExcludedMissingPosition = s.Sync.LastUpdateEpochExcludedMissingPosition
		out.LastUpdateEpochExcludedStalePosition = s.Sync.LastUpdateEpochExcludedStalePosition
		out.LastUpdateEpochExcludedNotDominant = s.Sync.LastUpdateEpochExcludedNotDominant
		out.LastUpdateEpochExcludedOutsideWindow = s.Sync.LastUpdateEpochExcludedOutsideWindow
		out.PeriodRefinementStatus = s.Sync.PeriodRefinementStatus
		out.RefinementPlottedCount = s.Sync.RefinementPlottedCount
		out.RefinementEligibleCount = s.Sync.RefinementEligibleCount
		out.RefinementRejectedCount = s.Sync.RefinementRejectedCount
		out.RefinementReferenceUpdates = s.Sync.RefinementReferenceUpdates
		out.RefinementLastRejectReason = s.Sync.RefinementLastRejectReason
		out.RefinementHistoryLen = len(s.Sync.residualHistory)
		out.FitObservationCount = s.Sync.FitObservationCount
		out.FitSpanS = s.Sync.FitSpanS
		out.FitICAOCount = s.Sync.FitICAOCount
		out.FitObservationsPerICAOMin = s.Sync.FitPerICAOMin
		out.FitObservationsPerICAOMedian = s.Sync.FitPerICAOMedian
		out.FitObservationsPerICAOMax = s.Sync.FitPerICAOMax
		out.FitRetentionWindowS = s.Sync.FitRetentionWindowS
		out.FitGlobalCapHit = s.Sync.FitGlobalCapHit
		out.FitLastEvictionReason = s.Sync.FitLastEvictionReason
		out.FitInlierRatio = s.Sync.FitInlierRatio
		out.SuspiciousICAOCount = s.Sync.SuspiciousICAOCount
		out.SuspiciousICAOLastReason = s.Sync.SuspiciousICAOLastReason
		if s.Sync.RefinementLastObservationUnix > 0 {
			ageS := float64(time.Now().UnixNano())/1e9 - s.Sync.RefinementLastObservationUnix
			if ageS < 0 {
				ageS = 0
			}
			out.RefinementLastObservationAgeS = ageS
		}
	}
	if s.RefICAO != nil {
		out.HasRefICAO = true
		out.RefICAO = *s.RefICAO
	}
	if s.Sync != nil {
		out.SyncPresent = true
		out.SyncQuality = s.Sync.SyncQuality
		out.SyncPeriodS = s.Sync.EffectivePeriodS
		out.SyncPhaseEpochUS = s.Sync.PhaseEpochUS
		out.SyncPhaseOffsetDeg = s.Sync.PhaseOffsetDeg
		out.SyncJitterDeg = s.Sync.SyncJitterDeg
		out.SyncResidualEMA = s.Sync.ResidualEMA
		out.SyncLastResidualDeg = s.Sync.LastResidualDeg
		out.SyncHoldover = s.Sync.Holdover
		out.SyncNSyncFrames = s.Sync.NSyncFrames
		out.SyncNRejectedFrames = s.Sync.NRejectedFrames
		out.SyncLastUpdatedUnix = float64(s.Sync.LastUpdated.UnixNano()) / 1e9
		out.SyncUsable = s.Sync.SyncQuality >= 0.3 && !s.Sync.Holdover && s.Sync.PeriodAgreesWithDF && s.Sync.PeriodRejectReason == "" && s.Sync.LastUpdateEpochStrictGatePass
		out.FitEpochResetCountByReason = s.Sync.FitEpochResetCountByReason
		if s.Sync.LastFitEpochResetUnix > 0 {
			ageS := float64(time.Now().UnixNano())/1e9 - s.Sync.LastFitEpochResetUnix
			if ageS < 0 {
				ageS = 0
			}
			out.LastFitEpochResetAgeS = ageS
		}
		out.FitObservationsAddedSinceReset = s.Sync.FitObservationsAddedSinceReset
		out.FitObservationsRejectedSinceReset = s.Sync.FitObservationsRejectedSinceReset
		out.LastFitObservationRejectReason = s.Sync.LastFitObservationRejectReason
		out.CurrentReferenceICAO = s.Sync.CurrentReferenceICAO
		out.PreviousReferenceICAO = s.Sync.PreviousReferenceICAO
		out.ReferenceChangeCount = s.Sync.ReferenceChangeCount
		if s.Sync.ReferenceChangeCount > 0 && s.Sync.FitEpochStartedUnix > 0 {
			nowS := float64(time.Now().UnixNano()) / 1e9
			elapsed := nowS - s.Sync.FitEpochStartedUnix
			if elapsed > 0 {
				out.ReferenceChurnRate = float64(s.Sync.ReferenceChangeCount) / elapsed * 60.0
			}
		}
		out.ObservationDropCountsByReason = s.Sync.ObservationDropCountsByReason
		out.ReacquireSupportObservationCount = s.Sync.ReacquireSupportObservationCount
		out.ReacquireSupportICAOCount = s.Sync.ReacquireSupportICAOCount
	}
	out.ActiveAircraftEstimate = s.lastActiveAircraft
	out.BurstRecordsTotal = len(s.records)
	out.BurstRecordsDynamicCap = s.burstDynamicCap
	out.BurstRecordsCapHitsTotal = s.burstCapHitsTotal
	if out.BurstRecordsDynamicCap > 0 && out.BurstRecordsTotal >= out.BurstRecordsDynamicCap {
		out.BurstRecordsCapHit = true
	}
	if len(s.records) >= 2 {
		out.BurstRecordsRetainedSpanS = (s.records[len(s.records)-1].CentroidUS - s.records[0].CentroidUS) / 1_000_000.0
	}
	if s.LastRotationModel != nil && s.LastRotationModel.Family != nil {
		out.DominantFamilyAircraft = len(s.LastRotationModel.Family.FoldedICAOs)
	}

	byICAO := make(map[uint32]int, len(s.records))
	for _, rec := range s.records {
		byICAO[rec.ICAO]++
	}
	out.BurstRecordsICAOs = len(byICAO)
	if len(byICAO) > 0 {
		perICAO := make([]int, 0, len(byICAO))
		for _, count := range byICAO {
			perICAO = append(perICAO, count)
			if count >= 4 {
				out.ReferenceEligibleAircraft++
			}
		}
		sort.Ints(perICAO)
		out.BurstRecordsPerICAOMin = perICAO[0]
		out.BurstRecordsPerICAOMax = perICAO[len(perICAO)-1]
		if len(perICAO)%2 == 0 {
			i := len(perICAO) / 2
			out.BurstRecordsPerICAOMedian = float64(perICAO[i-1]+perICAO[i]) / 2.0
		} else {
			out.BurstRecordsPerICAOMedian = float64(perICAO[len(perICAO)/2])
		}
	}
	out.ReferenceSelectionSparseHistory = out.HasPeriod && !out.HasRefICAO && out.ReferenceEligibleAircraft == 0
	out.LastRefSelHasPosition = s.LastRefSelHasPosition
	out.LastRefSelPositionAgeS = s.LastRefSelPositionAgeS
	out.LastRefSelMissingReason = s.LastRefSelMissingReason
	return out
}

// --- reinforcement logic (ported from _reinforce_radar_characteristics) ---

const (
	supportCap              = 12
	primaryConfidenceTarget = 12
	periodMatchTolerance    = 0.15
	minQualifyingICAOs      = 4
)

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func periodsMatch(a, b float64) bool {
	if a <= 0 || b <= 0 {
		return false
	}
	return math.Abs(a-b)/math.Max(a, b) <= periodMatchTolerance
}

func periodsAgree(a, b float64) bool {
	if a <= 0 || b <= 0 {
		return false
	}
	return math.Abs(a-b)/math.Max(a, b) <= dfAgreementToleranceFraction
}

func (s *IIDState) goFamilyAgreesWithDFLocked() bool {
	if s.BasePeriodS == nil {
		return true
	}
	if s.LastRotationModel == nil || s.LastRotationModel.DominantPeriodS == nil {
		return false
	}
	return periodsAgree(*s.LastRotationModel.DominantPeriodS, *s.BasePeriodS)
}

func blendPeriod(existing float64, support int, observed float64) (float64, int) {
	w := support
	if w < 1 {
		w = 1
	}
	if w > supportCap {
		w = supportCap
	}
	blended := (existing*float64(w) + observed) / float64(w+1)
	newSupport := w + 1
	if newSupport > supportCap {
		newSupport = supportCap
	}
	return blended, newSupport
}

func confidenceFromSupport(support int) float64 {
	if primaryConfidenceTarget <= 0 {
		return 0
	}
	c := float64(support) / float64(primaryConfidenceTarget)
	if c > 1.0 {
		c = 1.0
	}
	return c
}

func reinforce(s *IIDState, model *RotationModel) {
	if model.DominantPeriodS == nil {
		return
	}
	observedPeriod := *model.DominantPeriodS
	supportGain := 0

	if model.DominantPeriodS != nil && periodsMatch(*model.DominantPeriodS, observedPeriod) {
		if model.PrimaryDirectCount > supportGain {
			supportGain = model.PrimaryDirectCount
		}
	}

	primaryPeriod := s.PeriodS
	primarySupport := s.PrimarySupport

	if primaryPeriod != nil && periodsMatch(*primaryPeriod, observedPeriod) {
		if supportGain > 0 {
			blended, blendedSupport := blendPeriod(*primaryPeriod, primarySupport, observedPeriod)
			newSupport := blendedSupport + supportGain - 1
			if newSupport > supportCap {
				newSupport = supportCap
			}
			s.PeriodS = &blended
			primarySupport = newSupport
		}
	} else if primaryPeriod == nil {
		if supportGain > 0 {
			s.PeriodS = &observedPeriod
			primarySupport = supportGain
		}
	} else {
		if supportGain > 0 {
			if primarySupport > 0 {
				primarySupport--
			}
			replacementThreshold := minQualifyingICAOs
			if primarySupport+1 > replacementThreshold {
				replacementThreshold = primarySupport + 1
			}
			if supportGain >= replacementThreshold {
				s.PeriodS = &observedPeriod
				primarySupport = supportGain
				if primarySupport > supportCap {
					primarySupport = supportCap
				}
			}
		}
	}

	s.PrimarySupport = primarySupport
	s.PeriodStdS = model.PeriodStdS

	if s.PeriodS != nil {
		rpm := 60.0 / *s.PeriodS
		s.RPM = &rpm
	} else {
		s.RPM = nil
	}

	if s.BasePeriodS != nil {
		if s.Sync != nil {
			s.PeriodDeltaS = s.Sync.PeriodDeltaS
			effective := s.BasePeriodS
			if effective != nil {
				v := *effective + s.PeriodDeltaS
				s.EffectivePeriodS = &v
			}
			if math.Abs(s.PeriodDeltaS) > 1e-12 {
				s.PeriodSource = "df_alignment_plus_residual_slope"
			} else {
				s.PeriodSource = "df_alignment"
			}
		} else {
			s.EffectivePeriodS = s.BasePeriodS
			s.PeriodDeltaS = 0
			s.PeriodSource = "df_alignment"
		}

		// ── Diagnostic: compact rotation period vs DF base ──────────
		s.CompactPeriodAgreesWithDF = model.DominantPeriodS != nil && periodsAgree(*model.DominantPeriodS, *s.BasePeriodS)
		s.CompactPeriodDisagreementS = 0
		s.CompactPeriodDisagreementPPM = 0
		if !s.CompactPeriodAgreesWithDF && model.DominantPeriodS != nil && s.BasePeriodS != nil && *s.BasePeriodS > 0 {
			s.CompactPeriodDisagreementS = *model.DominantPeriodS - *s.BasePeriodS
			s.CompactPeriodDisagreementPPM = s.CompactPeriodDisagreementS / *s.BasePeriodS * 1e6
		}
		s.CompactPeriodDiagnosticReason = ""
		if model.DominantPeriodS != nil && !s.CompactPeriodAgreesWithDF {
			s.CompactPeriodDiagnosticReason = "compact_period_disagrees_with_df"
		}
	}

	confidence := confidenceFromSupport(primarySupport)
	if s.BasePeriodS != nil && !s.CompactPeriodAgreesWithDF {
		s.Status = "DF_PERIOD_DISAGREE"
		s.MultiRadarFlag = true
		return
	}
	switch {
	case s.PeriodS != nil && confidence >= 0.75:
		s.Status = "SINGLE_RADAR"
		s.MultiRadarFlag = false
	case s.PeriodS != nil && confidence >= 0.35:
		s.Status = "LIKELY_SINGLE"
		s.MultiRadarFlag = false
	default:
		s.Status = model.Status
		s.MultiRadarFlag = model.Status == "CHECK_MULTI"
	}
}
