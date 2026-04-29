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

	// Stage 3: reference aircraft and compact sync state.
	RefICAO *uint32 // selected reference aircraft (nil until stable)
	Sync    *SyncState
}

// DebugSnapshot is a point-in-time operational view of one IID.
type DebugSnapshot struct {
	Status                          string
	HasPeriod                       bool
	PeriodS                         float64
	HasRefICAO                      bool
	RefICAO                         uint32
	SyncPresent                     bool
	SyncQuality                     float64
	SyncUsable                      bool
	SyncPeriodS                     float64
	SyncPhaseEpochUS                float64
	SyncPhaseOffsetDeg              float64
	SyncJitterDeg                   float64
	SyncResidualEMA                 float64
	SyncLastResidualDeg             float64
	SyncHoldover                    bool
	SyncNSyncFrames                 int
	SyncNRejectedFrames             int
	SyncLastUpdatedUnix             float64
	BasePeriodS                     float64
	PeriodDeltaS                    float64
	EffectivePeriodS                float64
	PeriodSource                    string
	PeriodAgreesWithDF              bool
	PeriodRejectReason              string
	ResidualSlopeDegPerS            float64
	PeriodRefinementStatus          string
	ActiveAircraftEstimate          int
	BurstRecordsTotal               int
	BurstRecordsDynamicCap          int
	BurstRecordsCapHit              bool
	BurstRecordsCapHitsTotal        uint64
	BurstRecordsRetainedSpanS       float64
	BurstRecordsICAOs               int
	BurstRecordsPerICAOMin          int
	BurstRecordsPerICAOMedian       float64
	BurstRecordsPerICAOMax          int
	ReferenceEligibleAircraft       int
	DominantFamilyAircraft          int
	ReferenceSelectionSparseHistory bool
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
		if s.Sync != nil {
			s.Sync.Holdover = true
			s.Sync.PeriodS = 0
			s.Sync.PeriodRejectReason = "missing_df_base_period"
			s.Sync.PeriodAgreesWithDF = false
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
	s.PeriodAgreesWithDF = true
	s.PeriodRejectReason = ""
	if s.PeriodS != nil && !periodsAgree(*s.PeriodS, periodS) {
		s.PeriodAgreesWithDF = false
		s.PeriodRejectReason = "compact_period_disagrees_with_df"
	}
	if s.Sync != nil {
		s.Sync.PeriodAgreesWithDF = s.PeriodAgreesWithDF
		s.Sync.PeriodRejectReason = s.PeriodRejectReason
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
func (s *IIDState) RefreshReference(records []BurstRecord, nowUS float64) {
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

	chosen := SelectReference(filtered, *s.EffectivePeriodS, cur, nowUS)
	if chosen != 0 {
		s.RefICAO = &chosen
		return
	}
	// If no dominant-family candidate is available, clear the reference so
	// downstream gates expose "no_reference" rather than a permanently
	// non-dominant reference that can never open frames.
	if len(filtered) == 0 {
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
			s.Sync.Holdover = true
			s.Sync.PeriodAgreesWithDF = false
			s.Sync.PeriodRejectReason = "missing_df_base_period"
		}
		return
	}
	quality := syncQuality(s.Status, s.BasePeriodS != nil)
	if s.Sync == nil {
		// Bootstrap on first reference burst.
		eligible := nAircraft >= 4 || (nAircraft >= 3 && refPosAgeS <= 2.0)
		if !eligible {
			return
		}
		s.Sync = NewSyncState(s.IID, *s.EffectivePeriodS, epochUS, phaseOffsetDeg, quality)
		s.Sync.PeriodAgreesWithDF = s.PeriodAgreesWithDF
		s.Sync.PeriodRejectReason = s.PeriodRejectReason
		return
	}
	s.Sync.UpdateEpoch(epochUS, phaseOffsetDeg, *s.BasePeriodS, quality, nAircraft, refPosAgeS)
	s.PeriodDeltaS = s.Sync.PeriodDeltaS
	effective := s.Sync.EffectivePeriodS
	s.EffectivePeriodS = &effective
	s.PeriodSource = s.Sync.PeriodSource
	s.Sync.PeriodAgreesWithDF = s.PeriodAgreesWithDF
	s.Sync.PeriodRejectReason = s.PeriodRejectReason
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
	usable = s.Sync.SyncQuality >= 0.3 && !s.Sync.Holdover && s.Sync.PeriodAgreesWithDF && s.Sync.PeriodRejectReason == ""
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
	if s.Sync != nil {
		out.ResidualSlopeDegPerS = s.Sync.ResidualSlopeDegPerS
		out.PeriodRefinementStatus = s.Sync.PeriodRefinementStatus
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
		out.SyncUsable = s.Sync.SyncQuality >= 0.3 && !s.Sync.Holdover && s.Sync.PeriodAgreesWithDF && s.Sync.PeriodRejectReason == ""
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
		s.PeriodAgreesWithDF = model.DominantPeriodS != nil && periodsAgree(*model.DominantPeriodS, *s.BasePeriodS)
		if s.PeriodAgreesWithDF {
			s.PeriodRejectReason = ""
		} else {
			s.PeriodRejectReason = "compact_period_disagrees_with_df"
		}
	}

	confidence := confidenceFromSupport(primarySupport)
	if s.BasePeriodS != nil && !s.PeriodAgreesWithDF {
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
