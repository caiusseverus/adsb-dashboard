package fm

import (
	"math"
	"sort"
)

func scoreCircles(raw []rawCircle, periodS float64) []scoredCircle {
	omega := 2 * math.Pi / periodS
	out := make([]scoredCircle, 0, len(raw))
	for _, r := range raw {
		sc := scoredCircle{rawCircle: r, PriorWeight: 1, ResidualScore: 1}
		if r.DeltaPhi < rad(5) || r.DeltaPhi > rad(175) || !isFinite(r.RKM) || r.RKM <= 0 || r.PairBaselineM <= 0 {
			sc.ExclusionReason = "degenerate_delta_phi"
			sc.SigmaBandM = sigmaBandFloorM
			out = append(out, sc)
			continue
		}
		sinPhi := math.Max(math.Sin(r.DeltaPhi), 1e-12)
		sc.PhiWeight = math.Pow(sinPhi, 1.5)
		minReplies := max(1, minInt(r.RepliesA, r.RepliesB))
		dphiUnc := (burstCentroidUncS / math.Sqrt(float64(minReplies))) * omega
		sc.SigmaBandM = math.Max(sigmaBandFloorM, r.PairBaselineM*math.Abs(dphiUnc)/(2*math.Max(sinPhi*sinPhi, 1e-12)))
		sc.UncertaintyWeight = 1 / (1 + sq(sc.SigmaBandM/sigmaBandReferenceM))
		sc.AgeWeight = math.Exp(-math.Max(r.AgeA, r.AgeB) / positionAgeTauS)
		sc.ReplyWeight = math.Min(1, math.Sqrt(float64(minReplies)/4.0))
		sc.IntrinsicWeight = clamp01(sc.PhiWeight * sc.AgeWeight * sc.ReplyWeight * sc.UncertaintyWeight)
		sc.CircleScore = clamp01(sc.IntrinsicWeight * sc.PriorWeight)
		if minInt(r.RepliesA, r.RepliesB) < minRepliesPerBurst {
			sc.ExclusionReason = "insufficient_replies"
		}
		if math.Max(r.AgeA, r.AgeB) > maxPositionAgeS {
			sc.ExclusionReason = "stale_position"
		}
		out = append(out, sc)
	}
	return out
}

func selectCircles(scored []scoredCircle) []scoredCircle {
	gated := []scoredCircle{}
	for _, sc := range scored {
		if sc.ExclusionReason == "" {
			gated = append(gated, sc)
		}
	}
	sort.Slice(gated, func(i, j int) bool { return gated[i].CircleScore > gated[j].CircleScore })
	selected := []scoredCircle{}
	frameCounts := map[uint32]int{}
	aircraftCounts := map[[2]uint32]int{}
	for _, c := range gated {
		if frameCounts[c.FrameIndex] >= maxCirclesPerFrame {
			continue
		}
		ka, kb := [2]uint32{c.FrameIndex, c.IcaoA}, [2]uint32{c.FrameIndex, c.IcaoB}
		if aircraftCounts[ka] >= maxPairsPerAircraftFrame || aircraftCounts[kb] >= maxPairsPerAircraftFrame {
			continue
		}
		c.Selected = true
		selected = append(selected, c)
		frameCounts[c.FrameIndex]++
		aircraftCounts[ka]++
		aircraftCounts[kb]++
	}
	return selected
}

func intersections(selected []scoredCircle) []intersectionCandidate {
	out := []intersectionCandidate{}
	for i := 0; i < len(selected); i++ {
		for j := i + 1; j < len(selected); j++ {
			a, b := selected[i], selected[j]
			dx, dy := b.CxKM-a.CxKM, b.CyKM-a.CyKM
			d := math.Hypot(dx, dy)
			if d < 1e-6 || d > a.RKM+b.RKM+1e-6 || d < math.Abs(a.RKM-b.RKM)-1e-6 {
				continue
			}
			x := (a.RKM*a.RKM - b.RKM*b.RKM + d*d) / (2 * d)
			h2 := a.RKM*a.RKM - x*x
			if h2 < 0 {
				h2 = 0
			}
			h := math.Sqrt(h2)
			mx, my := a.CxKM+x*dx/d, a.CyKM+x*dy/d
			dAz := angularSep(a.RxBearing, b.RxBearing)
			if dAz > 90 {
				dAz = 180 - dAz
			}
			w := a.CircleScore * b.CircleScore * math.Sin(rad(dAz))
			out = append(out, intersectionCandidate{XKM: mx + h*dy/d, YKM: my - h*dx/d, Weight: w, ArcI: i, ArcJ: j})
			if h > 1e-6 {
				out = append(out, intersectionCandidate{XKM: mx - h*dy/d, YKM: my + h*dx/d, Weight: w, ArcI: i, ArcJ: j})
			}
		}
	}
	return out
}

func filterCandidates(cands []intersectionCandidate, selected []scoredCircle) []intersectionCandidate {
	out := []intersectionCandidate{}
	maxKM := 700 * nmToM / 1000
	for _, c := range cands {
		if math.Hypot(c.XKM, c.YKM) > maxKM {
			continue
		}
		a, b := selected[c.ArcI], selected[c.ArcJ]
		end := []float64{a.Endpoints[0], a.Endpoints[1], a.Endpoints[2], a.Endpoints[3], b.Endpoints[0], b.Endpoints[1], b.Endpoints[2], b.Endpoints[3]}
		reject := false
		for k := 0; k < len(end); k += 2 {
			if math.Hypot(c.XKM-end[k], c.YKM-end[k+1]) <= endpointRejectKM {
				reject = true
				break
			}
		}
		if !reject {
			out = append(out, c)
		}
	}
	return out
}

func buildClusters(cands []intersectionCandidate, radius float64) []cluster {
	neighborhoods := []cluster{}
	for idx, center := range cands {
		members := []intersectionCandidate{}
		for _, c := range cands {
			if math.Hypot(c.XKM-center.XKM, c.YKM-center.YKM) <= radius {
				members = append(members, c)
			}
		}
		tw := 0.0
		for _, m := range members {
			tw += m.Weight
		}
		if tw <= 0 {
			continue
		}
		mx, my := 0.0, 0.0
		contrib := map[int]struct{}{}
		for _, m := range members {
			mx += m.XKM * m.Weight
			my += m.YKM * m.Weight
			contrib[m.ArcI] = struct{}{}
			contrib[m.ArcJ] = struct{}{}
		}
		mx /= tw
		my /= tw
		ss := 0.0
		for _, m := range members {
			ss += m.Weight * (sq(m.XKM-mx) + sq(m.YKM-my))
		}
		neighborhoods = append(neighborhoods, cluster{MeanXKM: mx, MeanYKM: my, RMSKM: math.Sqrt(ss / tw), TotalWeight: tw, MemberCount: len(members), Contributing: contrib, Sources: []int{idx}})
	}
	sort.Slice(neighborhoods, func(i, j int) bool {
		if neighborhoods[i].TotalWeight != neighborhoods[j].TotalWeight {
			return neighborhoods[i].TotalWeight > neighborhoods[j].TotalWeight
		}
		if neighborhoods[i].MemberCount != neighborhoods[j].MemberCount {
			return neighborhoods[i].MemberCount > neighborhoods[j].MemberCount
		}
		return neighborhoods[i].RMSKM < neighborhoods[j].RMSKM
	})
	merged := []cluster{}
	for _, c := range neighborhoods {
		into := -1
		for i, kept := range merged {
			if sameLobeMetrics(c, kept, radius).mergeLike {
				into = i
				break
			}
		}
		if into < 0 {
			merged = append(merged, c)
			continue
		}
		kept := merged[into]
		dom, other := kept, c
		if c.TotalWeight > kept.TotalWeight || (math.Abs(c.TotalWeight-kept.TotalWeight) < 1e-9 && c.MemberCount > kept.MemberCount) {
			dom, other = c, kept
		}
		for k := range other.Contributing {
			dom.Contributing[k] = struct{}{}
		}
		dom.Sources = append(dom.Sources, other.Sources...)
		merged[into] = dom
	}
	return merged
}

type lobe struct{ mergeLike, sameLobe bool }

func sameLobeMetrics(a, b cluster, radius float64) lobe {
	sep := math.Hypot(a.MeanXKM-b.MeanXKM, a.MeanYKM-b.MeanYKM)
	jac, ov := arcOverlap(a.Contributing, b.Contributing)
	rmsScale := math.Max(math.Max(a.RMSKM, b.RMSKM), 1e-3)
	mergeLimit := math.Min(clusterMergeMaxKM, math.Max(clusterMergeDistanceFrac*radius, clusterMergeRMSMult*rmsScale))
	sameLimit := math.Min(sameLobeMaxKM, math.Max(0.6*radius, sameLobeRMSMult*rmsScale))
	veryClose := sep <= math.Max(1.0, math.Min(a.RMSKM, b.RMSKM))
	return lobe{mergeLike: sep <= mergeLimit && (jac >= clusterMergeMinJaccard || ov >= clusterMergeMinOverlap), sameLobe: sep <= sameLimit && (jac >= sameLobeMinJaccard || ov >= sameLobeMinOverlap || veryClose)}
}
func arcOverlap(a, b map[int]struct{}) (float64, float64) {
	inter := 0
	for k := range a {
		if _, ok := b[k]; ok {
			inter++
		}
	}
	union := len(a) + len(b) - inter
	minS := minInt(len(a), len(b))
	if union <= 0 || minS <= 0 {
		return 0, 0
	}
	return float64(inter) / float64(union), float64(inter) / float64(minS)
}

func clusterInlierCircles(c cluster, selected []scoredCircle) ([]int, []float64) {
	in := []int{}
	res := make([]float64, len(selected))
	for i, sc := range selected {
		sigma := math.Max(sc.SigmaBandM/1000, 1e-6)
		n := math.Abs(math.Hypot(c.MeanXKM-sc.CxKM, c.MeanYKM-sc.CyKM)-sc.RKM) / sigma
		res[i] = n
		if n <= qualityNormResidualInlier {
			in = append(in, i)
		}
	}
	return in, res
}
func circleSupportAt(x, y float64, selected []scoredCircle) (float64, []float64) {
	total := 0.0
	res := make([]float64, len(selected))
	for i, sc := range selected {
		sigma := math.Max(sc.SigmaBandM/1000, 1e-6)
		n := math.Abs(math.Hypot(x-sc.CxKM, y-sc.CyKM)-sc.RKM) / sigma
		res[i] = n
		total += sc.CircleScore * math.Exp(-0.5*sq(math.Min(n, 3)))
	}
	return total, res
}

func scoreClusterQuality(x, y float64, inliers []int, residuals []float64, selected []scoredCircle, cands []intersectionCandidate, c cluster) quality {
	support := 0.0
	for _, idx := range inliers {
		support += selected[idx].CircleScore * math.Exp(-0.5*sq(math.Min(residuals[idx], 3)))
	}
	compact := c.RMSKM
	inSet := map[int]struct{}{}
	for _, i := range inliers {
		inSet[i] = struct{}{}
	}
	tw, ss := 0.0, 0.0
	for _, ic := range cands {
		_, a := inSet[ic.ArcI]
		_, b := inSet[ic.ArcJ]
		if a && b && math.Hypot(ic.XKM-c.MeanXKM, ic.YKM-c.MeanYKM) <= intersectionClusterRadius {
			w := math.Max(ic.Weight, 1e-9)
			tw += w
			ss += w * (sq(ic.XKM-c.MeanXKM) + sq(ic.YKM-c.MeanYKM))
		}
	}
	if tw > 0 {
		compact = math.Sqrt(ss / tw)
	}
	refined := selectRefinedSubset(x, y, inliers, selected, residuals)
	lmin, lmax := localInfoEigen(x, y, refined, selected, residuals)
	cond, inv := math.Inf(1), 0.0
	if lmin > qualityConditioningFloor {
		cond = lmax / lmin
	}
	if lmax > 0 {
		inv = lmin / lmax
	}
	bonus := qualityConditioningScale * math.Sqrt(math.Max(0, lmin)) * (1 - 1/(1+inv))
	air := map[uint32]struct{}{}
	bearings := []float64{}
	for _, idx := range refined {
		air[selected[idx].IcaoA] = struct{}{}
		air[selected[idx].IcaoB] = struct{}{}
		bearings = append(bearings, selected[idx].RxBearing)
	}
	div := 0.5*(circularSpreadDeg(bearings)/360) + 0.5*math.Min(1, float64(len(air))/10)
	qscore := qualitySupportScale*(1-math.Exp(-support/3)) + math.Exp(-math.Pow(compact/qualityCompactnessScaleKM, qualityCompactnessExponent)) + bonus + qualityDiversityScale*div
	return quality{Score: qscore, SupportScore: support, CompactnessKM: compact, LambdaMin: lmin, LambdaMax: lmax, ConditionNumber: cond, InvConditionNumber: inv, DiversityScore: div, RawInliers: len(inliers), RefinedCount: len(refined), EffectiveAircraft: len(air), EffectiveFrames: 1, Refined: refined}
}

func selectRefinedSubset(x, y float64, inliers []int, selected []scoredCircle, residuals []float64) []int {
	ordered := append([]int{}, inliers...)
	sort.Slice(ordered, func(i, j int) bool { return selected[ordered[i]].CircleScore > selected[ordered[j]].CircleScore })
	chosen := []int{}
	aircraft := map[uint32]int{}
	bearings := []float64{}
	lambda := 0.0
	for _, idx := range ordered {
		if len(chosen) >= qualityMaxRefinedSubset {
			break
		}
		sc := selected[idx]
		if residuals[idx] > qualityNormResidualInlier {
			continue
		}
		if aircraft[sc.IcaoA] >= qualityPerAircraftCap || aircraft[sc.IcaoB] >= qualityPerAircraftCap {
			continue
		}
		dup := false
		for _, b := range bearings {
			if angularSep(sc.RxBearing, b) < qualityDuplicateBearingTol {
				dup = true
				break
			}
		}
		if dup {
			continue
		}
		test := append(append([]int{}, chosen...), idx)
		lmin, _ := localInfoEigen(x, y, test, selected, residuals)
		if len(chosen) >= 2 && lmin-lambda < qualityGreedyMinEigenGain {
			continue
		}
		chosen = test
		lambda = lmin
		aircraft[sc.IcaoA]++
		aircraft[sc.IcaoB]++
		bearings = append(bearings, sc.RxBearing)
	}
	return chosen
}
func localInfoEigen(x, y float64, idxs []int, selected []scoredCircle, residuals []float64) (float64, float64) {
	a, b, d := 0.0, 0.0, 0.0
	for _, idx := range idxs {
		sc := selected[idx]
		dx, dy := x-sc.CxKM, y-sc.CyKM
		norm := math.Hypot(dx, dy)
		if norm < 1e-9 {
			continue
		}
		nx, ny := dx/norm, dy/norm
		w := sc.CircleScore * math.Exp(-0.5*sq(math.Min(residuals[idx], 3)))
		a += w * nx * nx
		b += w * nx * ny
		d += w * ny * ny
	}
	tr := a + d
	disc := math.Sqrt(math.Max(0, sq(a-d)+4*b*b))
	return (tr - disc) / 2, (tr + disc) / 2
}

type ambiguity struct {
	ambiguous, sameLobeBypass, geometryDominant bool
	reason                                      string
}

func evaluateAmbiguity(best cluster, second *cluster, bestQ, secondQ, bestSupport, secondSupport float64, rawInliers int) ambiguity {
	qratio := math.Inf(1)
	if secondQ > 0 {
		qratio = bestQ / secondQ
	}
	threshold := 1.15
	if rawInliers >= 8 {
		threshold = qualityAmbiguityThreshold
	} else if rawInliers >= 4 {
		threshold = 1.15 + float64(rawInliers-4)/4*(qualityAmbiguityThreshold-1.15)
	}
	if bestQ >= 2 && qratio >= 1.1 {
		threshold = 1.05
	}
	secMembers, secWeight := 0, 0.0
	if second != nil {
		secMembers = second.MemberCount
		secWeight = second.TotalWeight
	}
	mr := math.Inf(1)
	if secMembers > 0 {
		mr = float64(best.MemberCount) / float64(secMembers)
	}
	wr := math.Inf(1)
	if secWeight > 0 {
		wr = best.TotalWeight / secWeight
	}
	geom := best.MemberCount >= accumGeomDomMinMemberCount && (!isFinite(mr) || mr >= accumGeomDomMinMemberRatio) && (!isFinite(wr) || wr >= accumGeomDomMinWeightRatio)
	same := false
	if second != nil {
		same = sameLobeMetrics(best, *second, intersectionClusterRadius).sameLobe
	}
	amb := !geom && qratio < threshold && second != nil && second.TotalWeight >= best.TotalWeight*intersectionSecondaryFrac && !same
	reason := ""
	if amb {
		reason = "quality ratio below threshold"
	}
	return ambiguity{ambiguous: amb, sameLobeBypass: same, geometryDominant: geom, reason: reason}
}

func scoreCandidatePairwise(lat, lon float64, selected []scoredCircle, periodS float64, direction int, only map[int]struct{}) pairwiseFit {
	res := []float64{}
	weights := []float64{}
	ws, wsum := 0.0, 0.0
	for idx, sc := range selected {
		if only != nil {
			if _, ok := only[idx]; !ok {
				continue
			}
		}
		if sc.IntrinsicWeight <= 0 {
			continue
		}
		ba := bearingDeg(lat, lon, sc.LatA, sc.LonA)
		bb := bearingDeg(lat, lon, sc.LatB, sc.LonB)
		pred := math.Mod(bb-ba, 360)
		if pred < 0 {
			pred += 360
		}
		dt := (sc.ArrivalB - sc.ArrivalA) / 1_000_000
		obs := math.Mod((dt/periodS)*360*float64(direction), 360)
		if obs < 0 {
			obs += 360
		}
		r := math.Mod(obs-pred+540, 360) - 180
		res = append(res, r)
		weights = append(weights, sc.IntrinsicWeight)
		ws += r * r * sc.IntrinsicWeight
		wsum += sc.IntrinsicWeight
	}
	if len(res) == 0 || wsum <= 0 {
		return pairwiseFit{Score: math.Inf(1), ResidualSigmaDeg: math.Inf(1), WeightedRMSDeg: math.Inf(1)}
	}
	mean := 0.0
	for i, r := range res {
		mean += r * weights[i]
	}
	mean /= wsum
	sig := 0.0
	for _, r := range res {
		sig += r * r
	}
	sig = math.Sqrt(sig / float64(len(res)))
	return pairwiseFit{Score: ws, MeanResidualDeg: mean, ResidualSigmaDeg: sig, WeightedRMSDeg: math.Sqrt(ws / wsum), NResiduals: len(res)}
}

func classifyFrameForAccumulation(r *SolveResult) (string, string) {
	if r == nil {
		return "no_solve", "rejected"
	}
	cep, n, support, supportDom, rms := r.CentroidUncertaintyKM, r.NInlierPairCircles, r.BestClusterSupportScore, r.SupportDominanceRatio, r.PairwiseWeightedRMSDeg
	if n < accumMinInlierPairCirclesRelaxed {
		return "too_few_inlier_pair_circles", "rejected"
	}
	if !isFinite(cep) || cep >= accumMaxFrameCEPKM {
		return "excessive_frame_cep", "rejected"
	}
	if support < accumMinSupportScore {
		return "poor_support_score", "rejected"
	}
	supportOK := supportDom >= accumMinSupportDominanceRatio
	rmsOK := isFinite(rms) && rms <= accumMaxPairwiseRMSDeg
	if n >= accumMinInlierPairCircles && supportOK && rmsOK {
		return "", "accepted_high_confidence"
	}
	if n < accumMinInlierPairCircles && support >= accumRelaxedMinSupportScore && supportOK && isFinite(rms) && rms <= accumRelaxedMaxPairwiseRMSDeg {
		return "", "accepted_reduced_arc_high_quality"
	}
	noSecond := r.SecondClusterMemberCount == 0
	memberOK := r.ClusterMemberCount >= accumGeomDomMinMemberCount && (noSecond || r.MemberDominanceRatio >= accumGeomDomMinMemberRatio)
	weightOK := noSecond || !isFinite(r.WeightDominanceRatio) || r.WeightDominanceRatio >= accumGeomDomMinWeightRatio
	if memberOK && weightOK {
		return "", "accepted_geometry_dominant"
	}
	if !supportOK {
		return "poor_support_dominance", "rejected"
	}
	return "poor_pairwise_residual_rms", "rejected"
}

type centroidResult struct {
	Available                          bool
	Lat, Lon, SigmaCombinedM           float64
	NTotal, NStage0Survivors, NInliers int
	RejectionCounts                    map[string]int
}
type centroidFrame struct {
	e            FrameEstimate
	x, y, sigma2 float64
}

func computeCentroid(est []FrameEstimate, recvLat, recvLon float64) centroidResult {
	rej := map[string]int{"stage0": 0, "stage0_excessive_cep": 0, "stage1": 0, "stage1_excessive_absolute_distance": 0, "stage2": 0}
	n := len(est)
	if n == 0 {
		return centroidResult{RejectionCounts: rej}
	}
	frames := []centroidFrame{}
	for _, e := range est {
		x, y := latlonToENU(e.Lat, e.Lon, recvLat, recvLon)
		sigma := math.Max(e.CEPKM*1000, filterMinSigmaM)
		frames = append(frames, centroidFrame{e: e, x: x, y: y, sigma2: sigma * sigma})
	}
	surv := []centroidFrame{}
	for _, f := range frames {
		e := f.e
		if e.CEPKM < filterMinCEPKM || e.CEPKM > filterMaxAccumulationCEPKM || e.NContributingArcs < filterMinArcs || e.AzimuthSpreadDeg < filterMinSpreadDeg || e.ClusterDominanceRatio < filterMinDominance || e.InterpolatedPositionFraction > filterMaxInterp || f.sigma2 <= 0 {
			rej["stage0"]++
			if e.CEPKM > filterMaxAccumulationCEPKM {
				rej["stage0_excessive_cep"]++
			}
			continue
		}
		surv = append(surv, f)
	}
	in := surv
	useMedian := true
	for iter := 0; iter < filterMaxIter; iter++ {
		prev := len(in)
		if len(in) == 0 {
			break
		}
		px, py := weightedCentroid(in)
		if useMedian && len(in) >= 2 {
			px, py = medianXY(in)
			useMedian = false
		}
		dists := []float64{}
		for _, f := range in {
			dists = append(dists, math.Hypot(f.x-px, f.y-py))
		}
		med := medianFloat(dists)
		dev := []float64{}
		for _, d := range dists {
			dev = append(dev, math.Abs(d-med))
		}
		sigCluster := math.Max(medianFloat(dev)*1.4826, filterStage1SigmaFloorM)
		absCap := math.Max(filterStage1ClusterSigmaMult*sigCluster, filterStage1AbsCapKM*1000)
		next := []centroidFrame{}
		for _, f := range in {
			dist := math.Hypot(f.x-px, f.y-py)
			if dist > absCap {
				rej["stage1"]++
				rej["stage1_excessive_absolute_distance"]++
				continue
			}
			denom := f.sigma2 + sigCluster*sigCluster + filterStage1SigmaFloorM*filterStage1SigmaFloorM
			if dist*dist/denom <= filterChi2Threshold99 {
				next = append(next, f)
			} else {
				rej["stage1"]++
			}
		}
		if len(next) >= filterStage2MinN {
			stage2 := stage2Filter(next, px, py)
			rej["stage2"] += len(next) - len(stage2)
			in = stage2
		} else {
			in = next
		}
		if len(in) == prev {
			break
		}
	}
	if len(in) < 2 {
		return centroidResult{NTotal: n, NStage0Survivors: len(surv), NInliers: len(in), RejectionCounts: rej}
	}
	px, py, sig := finalWeightedMean(in)
	lat, lon := enuToLatlon(px, py, recvLat, recvLon)
	return centroidResult{Available: true, Lat: lat, Lon: lon, SigmaCombinedM: sig, NTotal: n, NStage0Survivors: len(surv), NInliers: len(in), RejectionCounts: rej}
}
func weightedCentroid(in []centroidFrame) (float64, float64) {
	tw, sx, sy := 0.0, 0.0, 0.0
	for _, f := range in {
		if f.sigma2 > 0 {
			w := 1 / f.sigma2
			tw += w
			sx += f.x * w
			sy += f.y * w
		}
	}
	if tw <= 0 {
		return 0, 0
	}
	return sx / tw, sy / tw
}
func medianXY(in []centroidFrame) (float64, float64) {
	xs, ys := []float64{}, []float64{}
	for _, f := range in {
		xs = append(xs, f.x)
		ys = append(ys, f.y)
	}
	return medianFloat(xs), medianFloat(ys)
}
func finalWeightedMean(in []centroidFrame) (float64, float64, float64) {
	tw, sx, sy := 0.0, 0.0, 0.0
	for _, f := range in {
		if f.sigma2 > 0 {
			w := 1 / f.sigma2
			tw += w
			sx += f.x * w
			sy += f.y * w
		}
	}
	if tw <= 0 {
		return 0, 0, math.Inf(1)
	}
	return sx / tw, sy / tw, 1 / math.Sqrt(tw)
}
func stage2Filter(in []centroidFrame, px, py float64) []centroidFrame {
	dists := []float64{}
	for _, f := range in {
		dists = append(dists, math.Hypot(f.x-px, f.y-py))
	}
	med := medianFloat(dists)
	dev := []float64{}
	for _, d := range dists {
		dev = append(dev, math.Abs(d-med))
	}
	scale := filterHuberC * medianFloat(dev)
	if scale <= 0 {
		scale = 1e-6
	}
	tw, c00, c01, c11 := 0.0, 0.0, 0.0, 0.0
	for i, f := range in {
		w := math.Min(1, scale/math.Max(dists[i], 1e-12))
		dx, dy := f.x-px, f.y-py
		tw += w
		c00 += w * dx * dx
		c01 += w * dx * dy
		c11 += w * dy * dy
	}
	if tw <= 0 {
		return in
	}
	c00 /= tw
	c01 /= tw
	c11 /= tw
	det := c00*c11 - c01*c01
	if math.Abs(det) < 1e-12 {
		return in
	}
	i00, i01, i11 := c11/det, -c01/det, c00/det
	out := []centroidFrame{}
	for _, f := range in {
		dx, dy := f.x-px, f.y-py
		d2 := dx*(i00*dx+i01*dy) + dy*(i01*dx+i11*dy)
		if d2 <= filterChi2Threshold99 {
			out = append(out, f)
		}
	}
	return out
}
func medianFloat(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	cp := append([]float64{}, vals...)
	sort.Float64s(cp)
	n := len(cp)
	if n%2 == 0 {
		return (cp[n/2-1] + cp[n/2]) / 2
	}
	return cp[n/2]
}

func latlonToENU(lat, lon, originLat, originLon float64) (float64, float64) {
	return (lon - originLon) * rad(1) * rEarthM * math.Cos(rad(originLat)), (lat - originLat) * rad(1) * rEarthM
}
func enuToLatlon(east, north, originLat, originLon float64) (float64, float64) {
	return originLat + deg(north/rEarthM), originLon + deg(east/(rEarthM*math.Cos(rad(originLat))))
}
func bearingDeg(lat1, lon1, lat2, lon2 float64) float64 {
	p1, p2 := rad(lat1), rad(lat2)
	dl := rad(lon2 - lon1)
	y := math.Sin(dl) * math.Cos(p2)
	x := math.Cos(p1)*math.Sin(p2) - math.Sin(p1)*math.Cos(p2)*math.Cos(dl)
	return math.Mod(deg(math.Atan2(y, x))+360, 360)
}
func circularSpreadDeg(vals []float64) float64 {
	if len(vals) < 2 {
		return 0
	}
	cp := append([]float64{}, vals...)
	for i := range cp {
		cp[i] = math.Mod(cp[i], 360)
		if cp[i] < 0 {
			cp[i] += 360
		}
	}
	sort.Float64s(cp)
	uniq := []float64{}
	for _, v := range cp {
		if len(uniq) == 0 || math.Abs(v-uniq[len(uniq)-1]) > 1e-9 {
			uniq = append(uniq, v)
		}
	}
	if len(uniq) < 2 {
		return 0
	}
	maxGap := 0.0
	for i := 0; i < len(uniq)-1; i++ {
		if g := uniq[i+1] - uniq[i]; g > maxGap {
			maxGap = g
		}
	}
	if g := 360 - uniq[len(uniq)-1] + uniq[0]; g > maxGap {
		maxGap = g
	}
	return 360 - maxGap
}
func angularSep(a, b float64) float64 {
	d := math.Abs(math.Mod(a-b+180, 360) - 180)
	if d < 0 {
		return -d
	}
	return d
}
func rad(v float64) float64 { return v * math.Pi / 180 }
func deg(v float64) float64 { return v * 180 / math.Pi }
func sq(v float64) float64  { return v * v }
func clamp01(v float64) float64 {
	if !isFinite(v) {
		return 0
	}
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
func isFinite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
