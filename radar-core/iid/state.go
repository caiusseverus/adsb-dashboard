// Package iid manages per-IID rotation model state for radar-core Stage 2.
package iid

import (
	"math"
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
	Status             string  // SINGLE_RADAR, LIKELY_SINGLE, CHECK_MULTI, INSUFFICIENT_DATA
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

	// Reinforced period state (updated by reinforce).
	PeriodS           *float64
	PrimarySupport    int
	PeriodStdS        float64
	RPM               *float64
	Status            string
	MultiRadarFlag    bool
	LastRotationModel *RotationModel
	LastUpdated       time.Time

	// Stage 3: reference aircraft and live sync state.
	RefICAO *uint32   // selected reference aircraft (nil until stable)
	Sync    *SyncState
}

// NewIIDState creates an IIDState for the given IID.
func NewIIDState(iid uint8) *IIDState {
	return &IIDState{
		IID:    iid,
		Status: "UNKNOWN",
	}
}

// AddBurst appends a burst record for this IID, enforcing age and count caps.
func (s *IIDState) AddBurst(icao uint32, centroidUS float64, nReplies int) {
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

	// Cap per-IID count.
	for len(s.records) >= cfg.BurstRecordsMaxPerIID {
		s.records = s.records[1:]
	}

	s.records = append(s.records, BurstRecord{
		ICAO:       icao,
		CentroidUS: centroidUS,
		NReplies:   nReplies,
		FiredAt:    now,
	})
	s.dirty = true
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
}

// RefreshReference re-evaluates reference aircraft selection from current records.
// Called from the rotation analysis ticker after ApplyRotation.
// Records must be the same snapshot used for rotation analysis.
func (s *IIDState) RefreshReference(records []BurstRecord, nowUS float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.PeriodS == nil {
		return
	}
	cur := uint32(0)
	if s.RefICAO != nil {
		cur = *s.RefICAO
	}
	chosen := SelectReference(records, *s.PeriodS, cur, nowUS)
	if chosen != 0 {
		s.RefICAO = &chosen
	}
}

// UpdateSyncEpoch advances the sync epoch when the reference aircraft fires.
// epochUS is its burst centroid; nAircraft is the count of aircraft in the
// frame window. phaseOffsetDeg is 0 until radar position is known (Stage 4+).
func (s *IIDState) UpdateSyncEpoch(epochUS, phaseOffsetDeg float64, nAircraft int, refPosAgeS float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.PeriodS == nil {
		return
	}
	quality := syncQuality(s.Status, s.PeriodS != nil)
	if s.Sync == nil {
		// Bootstrap on first reference burst.
		eligible := nAircraft >= 4 || (nAircraft >= 3 && refPosAgeS <= 2.0)
		if !eligible {
			return
		}
		s.Sync = NewSyncState(s.IID, *s.PeriodS, epochUS, phaseOffsetDeg, quality)
		return
	}
	s.Sync.UpdateEpoch(epochUS, phaseOffsetDeg, *s.PeriodS, quality, nAircraft, refPosAgeS)
}

// FamilySnapshot returns the current ICAO family membership (may be nil).
// The returned pointer is read-only — callers must not mutate it.
func (s *IIDState) FamilySnapshot() *ICAOFamily {
	s.mu.Lock()
	defer s.mu.Unlock()
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

// Snapshot returns a safe copy of the current reinforced state.
func (s *IIDState) Snapshot() (status string, periodS *float64, rpm *float64, support int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Status, s.PeriodS, s.RPM, s.PrimarySupport
}

// --- reinforcement logic (ported from _reinforce_radar_characteristics) ---

const (
	supportCap              = 12
	primaryConfidenceTarget = 12
	periodMatchTolerance    = 0.15
	minQualifyingICAOs      = 4
)

func periodsMatch(a, b float64) bool {
	if a <= 0 || b <= 0 {
		return false
	}
	return math.Abs(a-b)/math.Max(a, b) <= periodMatchTolerance
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

	confidence := confidenceFromSupport(primarySupport)
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
