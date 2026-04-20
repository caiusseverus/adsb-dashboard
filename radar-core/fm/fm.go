// Package fm ports the operational forward-model per-frame solve and
// accumulation path from Python into radar-core.
package fm

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/caiusseverus/adsb-dashboard/radar-core/protocol"
)

const (
	rEarthM = 6371000.0
	nmToM   = 1852.0

	minRepliesPerBurst       = 2
	maxPositionAgeS          = 10.0
	maxPairsPerAircraftFrame = 8
	maxCirclesPerFrame       = 80
	sigmaBandFloorM          = 500.0
	sigmaBandReferenceM      = 3000.0
	burstCentroidUncS        = 0.030
	positionAgeTauS          = 4.0

	perFrameMaxObs             = 50
	perFrameMinContributing    = 4
	perFrameMaxCEPKM           = 100.0
	intersectionClusterRadius  = 40.0
	endpointRejectKM           = 1.0
	qualityAmbiguityThreshold  = 1.35
	intersectionSecondaryFrac  = 0.75
	qualityNormResidualInlier  = 2.5
	clusterMergeDistanceFrac   = 0.45
	clusterMergeRMSMult        = 1.6
	clusterMergeMaxKM          = 18.0
	clusterMergeMinJaccard     = 0.40
	clusterMergeMinOverlap     = 0.60
	sameLobeRMSMult            = 2.0
	sameLobeMaxKM              = 24.0
	sameLobeMinJaccard         = 0.25
	sameLobeMinOverlap         = 0.50
	qualitySupportScale        = 1.0
	qualityCompactnessScaleKM  = 15.0
	qualityCompactnessExponent = 1.5
	qualityConditioningScale   = 0.5
	qualityConditioningFloor   = 0.01
	qualityDiversityScale      = 0.3
	qualityMaxRefinedSubset    = 30
	qualityGreedyMinEigenGain  = 0.005
	qualityDuplicateBearingTol = 3.0
	qualityPerAircraftCap      = 6

	accumMaxFrameCEPKM               = 20.0
	accumMinInlierPairCircles        = 6
	accumMinInlierPairCirclesRelaxed = 4
	accumRelaxedMinSupportScore      = 1.1
	accumRelaxedMaxPairwiseRMSDeg    = 25.0
	accumMinSupportScore             = 0.8
	accumMinSupportDominanceRatio    = 1.25
	accumMaxPairwiseRMSDeg           = 35.0
	accumGeomDomMinMemberCount       = 6
	accumGeomDomMinMemberRatio       = 2.0
	accumGeomDomMinWeightRatio       = 1.5
	accumGeomDomWeightScale          = 0.5

	bufferMaxFramePositions = 5000

	filterMinArcs                 = 3
	filterMinSpreadDeg            = 30.0
	filterMinDominance            = 1.25
	filterMaxInterp               = 0.60
	filterMinCEPKM                = 0.05
	filterMaxAccumulationCEPKM    = 20.0
	filterMinSigmaM               = 500.0
	filterStage1SigmaFloorM       = 1000.0
	filterStage1AbsCapKM          = 30.0
	filterStage1ClusterSigmaMult  = 6.0
	filterChi2Threshold99         = 9.2103
	filterMaxIter                 = 3
	filterStage2MinN              = 20
	filterHuberC                  = 1.5
	filterMaxConditionNumberGuard = 1e6
)

type FrameEstimate struct {
	FrameIndex                   uint32
	SweepStartUS                 float64
	Lat                          float64
	Lon                          float64
	CEPKM                        float64
	NContributingArcs            int
	AzimuthSpreadDeg             float64
	Weight                       float64
	ClusterDominanceRatio        float64
	InterpolatedPositionFraction float64
	BestClusterSupportScore      float64
	SupportDominanceRatio        float64
	PairwiseWeightedRMSDeg       float64
	ClusterMemberCount           int
	SecondClusterMemberCount     int
	MemberDominanceRatio         float64
	WeightDominanceRatio         float64
	AdmissionTier                string
}

type Stats struct {
	FramesReachingSolver          uint64
	SolverSuccess                 uint64
	CandidatePositions            uint64
	SolverNoCandidate             uint64
	AccumulationRejected          uint64
	AccumulationRejectionReasons  map[string]uint64
	AccumulationAccepted          uint64
	AccumulationAcceptanceTiers   map[string]uint64
	AccumulationWritten           uint64
	LastRejectionReason           string
	LastAdmissionTier             string
	AccumulatedFramePositions     int
	CentroidAvailable             bool
	CentroidLat                   *float64
	CentroidLon                   *float64
	CentroidCEPM                  *float64
	CentroidInliers               int
	CentroidTotal                 int
	CentroidStage0Survivors       int
	CentroidRejectionCounts       map[string]int
	LastFrameSuccess              bool
	LastFrameReason               string
	LastFrameIndex                uint32
	LastFrameHadCandidatePosition bool
	LastSolve                     *SolveResult
}

type State struct {
	mu       sync.Mutex
	recvLat  float64
	recvLon  float64
	hasRecv  bool
	perIID   map[uint8]*iidState
	lastEmit map[uint8]Stats
}

type iidState struct {
	positions []FrameEstimate
	stats     Stats
}

func NewState(receiverLat, receiverLon float64, hasReceiver bool) *State {
	return &State{recvLat: receiverLat, recvLon: receiverLon, hasRecv: hasReceiver, perIID: make(map[uint8]*iidState), lastEmit: make(map[uint8]Stats)}
}

func (s *State) SetReceiver(lat, lon float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recvLat = lat
	s.recvLon = lon
	s.hasRecv = true
}

func (s *State) Reset(iid uint8) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.perIID, iid)
	delete(s.lastEmit, iid)
}

func (s *State) ProcessFrame(frame *protocol.FrameReady) (*protocol.FMFrameResult, *protocol.FMState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.perIID[frame.IID]
	if st == nil {
		st = &iidState{stats: newStats()}
		s.perIID[frame.IID] = st
	}
	st.stats.FramesReachingSolver++
	st.stats.LastFrameIndex = frame.FrameIndex
	st.stats.LastFrameSuccess = false
	st.stats.LastFrameHadCandidatePosition = false
	if !s.hasRecv {
		st.stats.SolverNoCandidate++
		st.stats.LastFrameReason = "receiver_coordinates_not_configured"
		return frameResult(frame, false, false, "receiver_coordinates_not_configured", "", nil), s.protocolState(frame.IID, st)
	}

	est, solved, solve := solveSingleFrame(frame, s.recvLat, s.recvLon)
	if solve != nil && solve.Success {
		st.stats.SolverSuccess++
	}
	st.stats.LastSolve = solve
	if !solved || est == nil {
		st.stats.SolverNoCandidate++
		reason := "no_candidate"
		if solve != nil && solve.Reason != "" {
			reason = solve.Reason
		}
		st.stats.LastFrameReason = reason
		return frameResult(frame, false, false, reason, "", solve), s.protocolState(frame.IID, st)
	}
	st.stats.CandidatePositions++
	st.stats.LastFrameHadCandidatePosition = true

	rejection, tier := classifyFrameForAccumulation(solve)
	if rejection != "" {
		st.stats.AccumulationRejected++
		st.stats.AccumulationRejectionReasons[rejection]++
		st.stats.LastRejectionReason = rejection
		st.stats.LastFrameReason = rejection
		return frameResult(frame, true, false, rejection, "rejected", solve), s.protocolState(frame.IID, st)
	}
	est.AdmissionTier = tier
	if tier == "accepted_geometry_dominant" {
		est.Weight *= accumGeomDomWeightScale
	}
	st.stats.AccumulationAccepted++
	st.stats.AccumulationAcceptanceTiers[tier]++
	st.stats.LastAdmissionTier = tier
	st.stats.LastFrameSuccess = true
	st.stats.LastFrameReason = "accepted"
	st.positions = append(st.positions, *est)
	if len(st.positions) > bufferMaxFramePositions {
		copy(st.positions, st.positions[len(st.positions)-bufferMaxFramePositions:])
		st.positions = st.positions[:bufferMaxFramePositions]
	}
	st.stats.AccumulationWritten++

	return frameResult(frame, true, true, "accepted", tier, solve), s.protocolState(frame.IID, st)
}

func newStats() Stats {
	return Stats{
		AccumulationRejectionReasons: make(map[string]uint64),
		AccumulationAcceptanceTiers:  make(map[string]uint64),
		CentroidRejectionCounts:      make(map[string]int),
	}
}

func (s *State) Snapshot(iid uint8) *protocol.FMState {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.perIID[iid]
	if st == nil {
		return nil
	}
	return s.protocolState(iid, st)
}

func (s *State) protocolState(iid uint8, st *iidState) *protocol.FMState {
	stats := st.stats
	stats.AccumulatedFramePositions = len(st.positions)
	centroid := computeCentroid(st.positions, s.recvLat, s.recvLon)
	stats.CentroidAvailable = centroid.Available
	stats.CentroidLat = nil
	stats.CentroidLon = nil
	stats.CentroidCEPM = nil
	stats.CentroidInliers = centroid.NInliers
	stats.CentroidTotal = centroid.NTotal
	stats.CentroidStage0Survivors = centroid.NStage0Survivors
	stats.CentroidRejectionCounts = centroid.RejectionCounts
	if centroid.Available {
		lat, lon, cep := centroid.Lat, centroid.Lon, centroid.SigmaCombinedM
		stats.CentroidLat = &lat
		stats.CentroidLon = &lon
		stats.CentroidCEPM = &cep
	}
	return statsToProtocol(iid, stats)
}

func frameResult(frame *protocol.FrameReady, candidate, accepted bool, reason, tier string, solve *SolveResult) *protocol.FMFrameResult {
	msg := &protocol.FMFrameResult{
		MsgType:           protocol.MsgFMFrameResult,
		IID:               frame.IID,
		FrameIndex:        frame.FrameIndex,
		Success:           solve != nil && solve.Success,
		CandidatePosition: candidate,
		AccumAccepted:     accepted,
		RejectionReason:   reason,
		AdmissionTier:     tier,
		ProcessedAt:       float64(time.Now().UnixMicro()) / 1e6,
	}
	if solve != nil {
		msg.SolveStatus = solve.Status
		msg.SolveReason = solve.Reason
		msg.NContributingArcs = uint16(max(0, solve.NContributingArcs))
		msg.PairwiseRMSDeg = float32(finiteOrZero(solve.PairwiseWeightedRMSDeg))
		msg.ClusterMemberCount = uint16(max(0, solve.ClusterMemberCount))
		msg.SecondClusterMemberCount = uint16(max(0, solve.SecondClusterMemberCount))
		msg.MemberDominanceRatio = float32(finiteOrZero(solve.MemberDominanceRatio))
		msg.WeightDominanceRatio = float32(finiteOrZero(solve.WeightDominanceRatio))
		msg.SupportDominanceRatio = float32(finiteOrZero(solve.SupportDominanceRatio))
		msg.BestClusterSupportScore = float32(finiteOrZero(solve.BestClusterSupportScore))
		msg.AmbiguitySameLobeBypass = solve.AmbiguitySameLobeBypass
		if isFinite(solve.Lat) && isFinite(solve.Lon) {
			lat, lon := solve.Lat, solve.Lon
			msg.Lat, msg.Lon = &lat, &lon
		}
		if isFinite(solve.CentroidUncertaintyKM) {
			cep := solve.CentroidUncertaintyKM * 1000.0
			msg.CEPM = &cep
		}
	}
	return msg
}

func statsToProtocol(iid uint8, st Stats) *protocol.FMState {
	msg := &protocol.FMState{
		MsgType:                       protocol.MsgFMState,
		IID:                           iid,
		FramesReachingSolver:          st.FramesReachingSolver,
		SolverSuccess:                 st.SolverSuccess,
		CandidatePositions:            st.CandidatePositions,
		SolverNoCandidate:             st.SolverNoCandidate,
		AccumulationRejected:          st.AccumulationRejected,
		AccumulationRejectionReasons:  copyStringUint64(st.AccumulationRejectionReasons),
		AccumulationAccepted:          st.AccumulationAccepted,
		AccumulationAcceptanceTiers:   copyStringUint64(st.AccumulationAcceptanceTiers),
		AccumulationWritten:           st.AccumulationWritten,
		LastRejectionReason:           st.LastRejectionReason,
		LastAdmissionTier:             st.LastAdmissionTier,
		AccumulatedFramePositions:     uint32(max(0, st.AccumulatedFramePositions)),
		CentroidAvailable:             st.CentroidAvailable,
		CentroidLat:                   st.CentroidLat,
		CentroidLon:                   st.CentroidLon,
		CentroidCEPM:                  st.CentroidCEPM,
		CentroidInliers:               uint32(max(0, st.CentroidInliers)),
		CentroidTotal:                 uint32(max(0, st.CentroidTotal)),
		CentroidStage0Survivors:       uint32(max(0, st.CentroidStage0Survivors)),
		CentroidRejectionCounts:       copyStringInt(st.CentroidRejectionCounts),
		LastFrameSuccess:              st.LastFrameSuccess,
		LastFrameReason:               st.LastFrameReason,
		LastFrameIndex:                st.LastFrameIndex,
		LastFrameHadCandidatePosition: st.LastFrameHadCandidatePosition,
		UpdatedAt:                     float64(time.Now().UnixMicro()) / 1e6,
	}
	if st.LastSolve != nil {
		msg.LastSolveStatus = st.LastSolve.Status
		msg.LastSolveReason = st.LastSolve.Reason
		msg.LastAmbiguitySameLobeBypass = st.LastSolve.AmbiguitySameLobeBypass
		msg.LastClusterMemberCount = uint16(max(0, st.LastSolve.ClusterMemberCount))
		msg.LastSecondClusterMemberCount = uint16(max(0, st.LastSolve.SecondClusterMemberCount))
		msg.LastSupportDominanceRatio = float32(finiteOrZero(st.LastSolve.SupportDominanceRatio))
		msg.LastPairwiseRMSDeg = float32(finiteOrZero(st.LastSolve.PairwiseWeightedRMSDeg))
		if isFinite(st.LastSolve.Lat) && isFinite(st.LastSolve.Lon) {
			lat, lon := st.LastSolve.Lat, st.LastSolve.Lon
			msg.LastFrameLat, msg.LastFrameLon = &lat, &lon
		}
		if isFinite(st.LastSolve.CentroidUncertaintyKM) {
			cep := st.LastSolve.CentroidUncertaintyKM * 1000.0
			msg.LastFrameCEPM = &cep
		}
	}
	return msg
}

func copyStringUint64(in map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
func copyStringInt(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
func finiteOrZero(v float64) float64 {
	if isFinite(v) {
		return v
	}
	return 0
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ---- Geometry / solver types ----

type aircraftObs struct {
	ICAO                uint32
	Lat, Lon, ArrivalUS float64
	NReplies            int
	PosAgeS             float64
	Interpolated        bool
}
type rawCircle struct {
	Index                                      int
	IcaoA, IcaoB                               uint32
	FrameIndex                                 uint32
	CxKM, CyKM, RKM                            float64
	PairBaselineM                              float64
	DeltaPhi                                   float64
	RxBearing                                  float64
	LatA, LonA, LatB, LonB, ArrivalA, ArrivalB float64
	RepliesA, RepliesB                         int
	AgeA, AgeB                                 float64
	Endpoints                                  [4]float64
}
type scoredCircle struct {
	rawCircle
	PhiWeight, ResidualScore, AgeWeight, UncertaintyWeight, ReplyWeight, PriorWeight, IntrinsicWeight, CircleScore, SigmaBandM float64
	Selected                                                                                                                   bool
	ExclusionReason                                                                                                            string
}
type intersectionCandidate struct {
	XKM, YKM, Weight float64
	ArcI, ArcJ       int
}
type cluster struct {
	MeanXKM, MeanYKM, RMSKM, TotalWeight float64
	MemberCount                          int
	Contributing                         map[int]struct{}
	Sources                              []int
}
type pairwiseFit struct {
	Score, MeanResidualDeg, ResidualSigmaDeg, WeightedRMSDeg float64
	NResiduals                                               int
}
type quality struct {
	Score, SupportScore, CompactnessKM, LambdaMin, LambdaMax, ConditionNumber, InvConditionNumber, DiversityScore float64
	RawInliers, RefinedCount, EffectiveAircraft, EffectiveFrames                                                  int
	Refined                                                                                                       []int
}

type SolveResult struct {
	Success                   bool
	Status                    string
	Reason                    string
	Lat                       float64
	Lon                       float64
	RMSKM                     float64
	CentroidUncertaintyKM     float64
	NContributingArcs         int
	NInlierPairCircles        int
	NPairs                    int
	AzimuthSpreadDeg          float64
	InterpolatedFraction      float64
	HighQualityFrames         int
	ClusterMemberCount        int
	SecondClusterMemberCount  int
	MemberDominanceRatio      float64
	WeightDominanceRatio      float64
	SupportDominanceRatio     float64
	DominanceRatio            float64
	PairwiseWeightedRMSDeg    float64
	BestClusterSupportScore   float64
	BestClusterQualityScore   float64
	AmbiguitySameLobeBypass   bool
	IsGeometryDominantCluster bool
}

func solveSingleFrame(frame *protocol.FrameReady, recvLat, recvLon float64) (*FrameEstimate, bool, *SolveResult) {
	if frame.Quality == "insufficient" {
		return nil, false, nil
	}
	var best *SolveResult
	for _, dir := range []int{1, -1} {
		res := solveByIntersection(frame, recvLat, recvLon, dir)
		if res == nil || !res.Success {
			continue
		}
		if best == nil || solveRank(res, best) > 0 {
			best = res
		}
	}
	if best == nil {
		return nil, false, &SolveResult{Success: false, Status: "rejected_no_candidate", Reason: "no candidate"}
	}
	if best.NContributingArcs < perFrameMinContributing || best.CentroidUncertaintyKM >= perFrameMaxCEPKM {
		return nil, false, best
	}
	weight := float64(best.NContributingArcs) / math.Pow(best.CentroidUncertaintyKM+0.5, 2)
	return &FrameEstimate{FrameIndex: frame.FrameIndex, SweepStartUS: frame.RefArrivalUS, Lat: best.Lat, Lon: best.Lon, CEPKM: best.CentroidUncertaintyKM, NContributingArcs: best.NContributingArcs, AzimuthSpreadDeg: best.AzimuthSpreadDeg, Weight: weight, ClusterDominanceRatio: best.SupportDominanceRatio, InterpolatedPositionFraction: best.InterpolatedFraction, BestClusterSupportScore: best.BestClusterSupportScore, SupportDominanceRatio: best.SupportDominanceRatio, PairwiseWeightedRMSDeg: best.PairwiseWeightedRMSDeg, ClusterMemberCount: best.ClusterMemberCount, SecondClusterMemberCount: best.SecondClusterMemberCount, MemberDominanceRatio: best.MemberDominanceRatio, WeightDominanceRatio: best.WeightDominanceRatio}, true, best
}

func solveRank(a, b *SolveResult) int {
	av := []float64{a.BestClusterQualityScore, float64(a.NInlierPairCircles), -a.PairwiseWeightedRMSDeg, -a.RMSKM}
	bv := []float64{b.BestClusterQualityScore, float64(b.NInlierPairCircles), -b.PairwiseWeightedRMSDeg, -b.RMSKM}
	for i := range av {
		if av[i] > bv[i] {
			return 1
		}
		if av[i] < bv[i] {
			return -1
		}
	}
	return 0
}

func solveByIntersection(frame *protocol.FrameReady, originLat, originLon float64, direction int) *SolveResult {
	if frame.PeriodS <= 0 || len(frame.Observations) == 0 {
		return &SolveResult{Status: "rejected_no_observations", Reason: "no observations"}
	}
	cosOrig := math.Cos(rad(originLat))
	if math.Abs(cosOrig) < 1e-12 {
		cosOrig = 1e-12
	}
	toXY := func(lat, lon float64) (float64, float64) {
		return (lon - originLon) * rad(1) * rEarthM / 1000 * cosOrig, (lat - originLat) * rad(1) * rEarthM / 1000
	}
	fromXY := func(x, y float64) (float64, float64) {
		return originLat + deg(y/(rEarthM/1000)), originLon + deg(x/((rEarthM/1000)*cosOrig))
	}
	aircraft := []aircraftObs{{ICAO: frame.RefICAO, Lat: frame.RefLat, Lon: frame.RefLon, ArrivalUS: frame.RefArrivalUS, NReplies: 2}}
	maxReplies := 2
	for _, o := range frame.Observations {
		if int(o.NReplies) > maxReplies {
			maxReplies = int(o.NReplies)
		}
	}
	aircraft[0].NReplies = maxReplies
	for _, o := range frame.Observations {
		aircraft = append(aircraft, aircraftObs{ICAO: o.ICAO, Lat: o.Lat, Lon: o.Lon, ArrivalUS: o.ArrivalUS, NReplies: int(o.NReplies), PosAgeS: float64(o.PosAgeS)})
	}
	if len(aircraft) > perFrameMaxObs {
		aircraft = aircraft[:perFrameMaxObs]
	}
	var raw []rawCircle
	for i := 0; i < len(aircraft); i++ {
		for j := i + 1; j < len(aircraft); j++ {
			a, b := aircraft[i], aircraft[j]
			ax, ay := toXY(a.Lat, a.Lon)
			bx, by := toXY(b.Lat, b.Lon)
			dx, dy := bx-ax, by-ay
			d := math.Hypot(dx, dy)
			if d < 0.1 {
				continue
			}
			dtS := (b.ArrivalUS - a.ArrivalUS) / 1_000_000.0
			phaseDeg := math.Mod((dtS/frame.PeriodS)*360.0*float64(direction), 360.0)
			if phaseDeg < 0 {
				phaseDeg += 360
			}
			deltaPhiDeg := math.Min(phaseDeg, 360-phaseDeg)
			deltaPhi := rad(deltaPhiDeg)
			signed := phaseDeg
			if signed > 180 {
				signed -= 360
			}
			signedPhi := rad(signed)
			sinPhi := math.Sin(signedPhi)
			if math.Abs(sinPhi) <= 1e-12 {
				continue
			}
			R := d / (2 * math.Abs(sinPhi))
			px, py := -dy/d, dx/d
			h := -(d / 2) * (math.Cos(signedPhi) / sinPhi)
			cx, cy := (ax+bx)/2+h*px, (ay+by)/2+h*py
			midLat, midLon := fromXY((ax+bx)/2, (ay+by)/2)
			raw = append(raw, rawCircle{Index: len(raw), IcaoA: a.ICAO, IcaoB: b.ICAO, FrameIndex: frame.FrameIndex, CxKM: cx, CyKM: cy, RKM: R, PairBaselineM: d * 1000, DeltaPhi: deltaPhi, RxBearing: bearingDeg(originLat, originLon, midLat, midLon), LatA: a.Lat, LonA: a.Lon, LatB: b.Lat, LonB: b.Lon, ArrivalA: a.ArrivalUS, ArrivalB: b.ArrivalUS, RepliesA: a.NReplies, RepliesB: b.NReplies, AgeA: a.PosAgeS, AgeB: b.PosAgeS, Endpoints: [4]float64{ax, ay, bx, by}})
		}
	}
	scored := scoreCircles(raw, frame.PeriodS)
	selected := selectCircles(scored)
	if len(selected) < 2 {
		return &SolveResult{Status: "rejected_too_few_admitted", Reason: "too few circles after scoring and selection", NPairs: len(raw)}
	}
	cands := intersections(selected)
	if len(cands) == 0 {
		return &SolveResult{Status: "rejected_no_intersections", Reason: "no circle intersections", NPairs: len(selected)}
	}
	plausible := filterCandidates(cands, selected)
	if len(plausible) == 0 {
		return &SolveResult{Status: "rejected_all_candidates_implausible", Reason: "all intersection candidates were implausibly distant", NPairs: len(selected)}
	}
	clusters := buildClusters(plausible, intersectionClusterRadius)
	if len(clusters) == 0 {
		return &SolveResult{Status: "rejected_ambiguous", Reason: "candidate cloud was ambiguous", NPairs: len(selected)}
	}
	type qitem struct {
		q   quality
		fit pairwiseFit
		c   cluster
	}
	var ranked []qitem
	for _, c := range clusters[:minInt(8, len(clusters))] {
		inliers, residuals := clusterInlierCircles(c, selected)
		if len(inliers) == 0 {
			continue
		}
		lat, lon := fromXY(c.MeanXKM, c.MeanYKM)
		fit := scoreCandidatePairwise(lat, lon, selected, frame.PeriodS, direction, nil)
		q := scoreClusterQuality(c.MeanXKM, c.MeanYKM, inliers, residuals, selected, plausible, c)
		ranked = append(ranked, qitem{q: q, fit: fit, c: c})
	}
	if len(ranked) == 0 {
		return &SolveResult{Status: "rejected_no_viable_clusters", Reason: "no clusters passed quality gates", NPairs: len(selected)}
	}
	sort.Slice(ranked, func(i, j int) bool {
		a, b := ranked[i], ranked[j]
		if a.c.MemberCount != b.c.MemberCount {
			return a.c.MemberCount > b.c.MemberCount
		}
		if a.c.TotalWeight != b.c.TotalWeight {
			return a.c.TotalWeight > b.c.TotalWeight
		}
		if a.c.RMSKM != b.c.RMSKM {
			return a.c.RMSKM < b.c.RMSKM
		}
		if a.q.CompactnessKM != b.q.CompactnessKM {
			return a.q.CompactnessKM < b.q.CompactnessKM
		}
		ca, cb := a.q.ConditionNumber, b.q.ConditionNumber
		if !isFinite(ca) {
			ca = 1e9
		}
		if !isFinite(cb) {
			cb = 1e9
		}
		if ca != cb {
			return ca < cb
		}
		return a.q.Score > b.q.Score
	})
	best := ranked[0]
	var second *qitem
	if len(ranked) > 1 {
		second = &ranked[1]
	}
	if !isFinite(best.fit.Score) {
		return &SolveResult{Status: "rejected_ambiguous", Reason: "best cluster has non-finite pairwise fit", NPairs: len(selected)}
	}
	secCluster := (*cluster)(nil)
	secQForAmb := 0.0
	secSupportForAmb := 0.0
	if second != nil {
		secCluster = &second.c
		secQForAmb = second.q.Score
		secSupportForAmb = second.q.SupportScore
	}
	amb := evaluateAmbiguity(best.c, secCluster, best.q.Score, secQForAmb, best.q.SupportScore, secSupportForAmb, best.q.RawInliers)
	if amb.ambiguous {
		return &SolveResult{Status: "rejected_ambiguous", Reason: amb.reason, NPairs: len(selected), AmbiguitySameLobeBypass: amb.sameLobeBypass}
	}
	lat, lon := fromXY(best.c.MeanXKM, best.c.MeanYKM)
	_, normResiduals := circleSupportAt(best.c.MeanXKM, best.c.MeanYKM, selected)
	inlierSet := map[int]struct{}{}
	for i, r := range normResiduals {
		if r <= qualityNormResidualInlier {
			inlierSet[i] = struct{}{}
		}
	}
	var inlierC []scoredCircle
	var inlierCand []intersectionCandidate
	for idx := range inlierSet {
		inlierC = append(inlierC, selected[idx])
	}
	for _, cand := range plausible {
		_, ok1 := inlierSet[cand.ArcI]
		_, ok2 := inlierSet[cand.ArcJ]
		if ok1 && ok2 && math.Hypot(cand.XKM-best.c.MeanXKM, cand.YKM-best.c.MeanYKM) <= intersectionClusterRadius {
			inlierCand = append(inlierCand, cand)
		}
	}
	inlierRMS := best.c.RMSKM
	if len(inlierCand) > 0 {
		tw, ws := 0.0, 0.0
		for _, c := range inlierCand {
			w := math.Max(c.Weight, 0)
			tw += w
			ws += w * (sq(c.XKM-best.c.MeanXKM) + sq(c.YKM-best.c.MeanYKM))
		}
		if tw > 0 {
			inlierRMS = math.Sqrt(ws / tw)
		}
	}
	nArcs := len(inlierC)
	cepKM := inlierRMS / math.Sqrt(float64(max(1, nArcs)))
	bearings := []float64{}
	interp := 0
	for idx := range inlierSet {
		bearings = append(bearings, selected[idx].RxBearing)
		if selected[idx].rawCircle.AgeA > 0 || selected[idx].rawCircle.AgeB > 0 { /* no-op */
		}
		if false {
			interp++
		}
	}
	interpFrac := 0.0
	if nArcs > 0 {
		interpFrac = float64(interp) / float64(nArcs)
	}
	refinedFit := best.fit
	if len(inlierC) > 0 {
		refinedFit = scoreCandidatePairwise(lat, lon, inlierC, frame.PeriodS, direction, nil)
	}
	secMembers, secWeight, secQ, secSupport := 0, 0.0, 0.0, 0.0
	if second != nil {
		secMembers = second.c.MemberCount
		secWeight = second.c.TotalWeight
		secQ = second.q.Score
		secSupport = second.q.SupportScore
	}
	memberRatio := math.Inf(1)
	if secMembers > 0 {
		memberRatio = float64(best.c.MemberCount) / float64(secMembers)
	}
	weightRatio := math.Inf(1)
	if secWeight > 0 {
		weightRatio = best.c.TotalWeight / secWeight
	}
	supportRatio := math.Inf(1)
	if secSupport > 0 {
		supportRatio = best.q.SupportScore / secSupport
	}
	qualityRatio := math.Inf(1)
	if secQ > 0 {
		qualityRatio = best.q.Score / secQ
	}
	return &SolveResult{Success: true, Status: "success", Reason: "success", Lat: lat, Lon: lon, RMSKM: best.c.RMSKM, CentroidUncertaintyKM: cepKM, NContributingArcs: nArcs, NInlierPairCircles: nArcs, NPairs: len(selected), AzimuthSpreadDeg: circularSpreadDeg(bearings), InterpolatedFraction: interpFrac, HighQualityFrames: boolToInt(frame.Quality == "good"), ClusterMemberCount: best.c.MemberCount, SecondClusterMemberCount: secMembers, MemberDominanceRatio: memberRatio, WeightDominanceRatio: weightRatio, SupportDominanceRatio: supportRatio, DominanceRatio: qualityRatio, PairwiseWeightedRMSDeg: refinedFit.WeightedRMSDeg, BestClusterSupportScore: best.q.SupportScore, BestClusterQualityScore: best.q.Score, AmbiguitySameLobeBypass: amb.sameLobeBypass, IsGeometryDominantCluster: amb.geometryDominant}
}
