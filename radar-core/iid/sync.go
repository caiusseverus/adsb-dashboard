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
	IID             uint8
	PeriodS         float64
	PhaseEpochUS    float64 // Beast-monotonic centroid of last accepted reference burst
	PhaseOffsetDeg  float64 // Geometric bearing from radar to ref aircraft at epoch (0 until radar pos known)
	SyncQuality     float64 // 0.0–1.0 from rotation model status
	SyncJitterDeg   float64 // 1-sigma jitter estimate from residual EMA
	ResidualEMA     float64 // EMA of |circular residual| degrees
	NSyncFrames     int     // accepted frame count
	NRejectedFrames int     // rejected frame count
	LastResidualDeg float64
	Holdover        bool      // true when last update was rejected / too weak
	LastUpdated     time.Time
}

const (
	residualRejectDeg = 50.0 // hard-reject threshold (degrees)
	residualSoftDeg   = 20.0 // soft-accept threshold
	residualEMAAlpha  = 0.2
)

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
		IID:            iid,
		PeriodS:        periodS,
		PhaseEpochUS:   epochUS,
		PhaseOffsetDeg: phaseOffsetDeg,
		SyncQuality:    quality,
		SyncJitterDeg:  5.0,
		ResidualEMA:    5.0,
		NSyncFrames:    1,
		Holdover:       false,
		LastUpdated:    time.Now(),
	}
}

// UpdateEpoch advances the sync epoch when a new reference aircraft burst arrives.
// nAircraft is the number of aircraft seen in the frame window; refPosAgeS is
// the age of the reference aircraft's position at burst time.
// phaseOffsetDeg is 0.0 until radar position is known.
//
// Returns true if the update was accepted.
func (s *SyncState) UpdateEpoch(newEpochUS, newOffsetDeg, periodS, quality float64, nAircraft int, refPosAgeS float64) bool {
	// Quality gate: frame must have >= 4 aircraft, or >= 3 with fresh ref position.
	eligible := nAircraft >= 4 || (nAircraft >= 3 && refPosAgeS <= 2.0)
	if !eligible {
		s.Holdover = true
		return false
	}

	periodUS := periodS * 1e6

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

	s.PeriodS = periodS
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

// PredictBearing returns the predicted bearing in [0, 360) for an arrival at
// arrivalUS. Returns -1 if no sync state is available.
//
// NOTE: The Go sync state is anchored on a relative phase offset (0.0 until
// the radar position is known).  This is intentional — Go sync provides a
// phase-tracking signal, not an absolute bearing engine.  Do not assume the
// returned value is a geographic bearing unless PhaseOffsetDeg has been
// properly initialised with a geometric reference.
func (s *SyncState) PredictBearing(arrivalUS float64) float64 {
	if s.PeriodS <= 0 {
		return -1
	}
	periodUS := s.PeriodS * 1e6
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
