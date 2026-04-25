package iid

// multisync_long_term.go — persistent long-term period estimator (Layer 2).
//
// The 30s fit window only ever measures local residual slope and a refined
// candidate period. Those measurements are noisy. This file maintains a
// bounded EMA-style estimate of the radar period that survives across fit
// windows, anchored to the DF dominant period family.
//
// Design notes:
//   - The DF dominant period remains the family authority. Long-term updates
//     are bounded to ±periodRefineMaxPPMFromDominant of dominantPeriodS.
//   - Bootstrap exception: when no long-term estimate exists, the first
//     sufficiently-clean local measurement seeds the estimator directly.
//   - After bootstrap, each window nudges the estimate by gain * (local - LT).
//     The gain scales with both the local fit quality and accumulated
//     confidence so a long-running, well-supported estimator is harder to
//     perturb than a fresh one.
//   - Confidence rises on consistent windows (small |delta| vs current LT) and
//     decays — but does not reset — on contradictory windows.
//
// Layer 2 behaviour change: runFit reroutes the applied refined period
// (`finalPeriodS`) through `LongTermPeriodEstimateS` once the estimator is
// seeded. Reacquire candidates and recovery-mode searches continue to
// override the apply path directly; both reseed the long-term estimator
// rather than drift toward the post-jump candidate.

import "math"

const (
	// Quality required to seed the long-term estimator from a fresh local
	// measurement (bootstrap exception).
	longTermPeriodSeedQualityMin = 0.30

	// Base nudge gain. The effective gain is base + (max-base) * confidence,
	// then scaled by the local fit quality.
	longTermPeriodGainBase = 0.05
	longTermPeriodGainMax  = 0.15

	// |local - LT| within this many PPM of LT counts as a consistent window.
	longTermPeriodConsistencyPPM = 50.0

	// Confidence step on consistent vs contradictory windows.
	longTermPeriodConfidenceStep            = 0.08
	longTermPeriodConfidenceContradictDecay = 0.50
	longTermPeriodConfidenceMax             = 1.0
)

// updateLongTermPeriodEstimator seeds (when not yet present) or nudges the
// persistent period estimate using the latest local short-window measurement.
// Updates are bounded to ±periodRefineMaxPPMFromDominant around dominantPeriodS.
//
// `localPeriodS` is the short-window slope-corrected period (= ms.LastLocalPeriodMeasurementS).
// `localQuality` is in [0,1] and is used to scale both seed admission and nudge gain.
func (ms *MultiSyncSolver) updateLongTermPeriodEstimator(
	localPeriodS, dominantPeriodS, nowUnix, localQuality float64,
) {
	if localPeriodS <= 0 {
		return
	}

	// Bootstrap exception: first viable window seeds the estimator directly.
	// Subsequent windows can only nudge.
	if ms.LongTermPeriodEstimateS <= 0 {
		if localQuality < longTermPeriodSeedQualityMin {
			return
		}
		ms.LongTermPeriodEstimateS = localPeriodS
		ms.LongTermPeriodEstimatorConfidence = clamp(localQuality, 0, 1)
		ms.ConsecutivePeriodConsistentWindows = 1
		ms.LastPeriodUpdateDeltaS = 0
		ms.lastLongTermPeriodSeedTS = nowUnix
		ms.LongTermPeriodEstimatorAgeS = 0
		return
	}

	diff := localPeriodS - ms.LongTermPeriodEstimateS
	gain := longTermPeriodGainBase +
		(longTermPeriodGainMax-longTermPeriodGainBase)*clamp(ms.LongTermPeriodEstimatorConfidence, 0, 1)
	gain *= clamp(localQuality, 0, 1)
	next := ms.LongTermPeriodEstimateS + gain*diff

	// Bound to ±periodRefineMaxPPMFromDominant of dominantPeriodS when available.
	if dominantPeriodS > 0 {
		maxDiff := dominantPeriodS * periodRefineMaxPPMFromDominant * 1e-6
		if next > dominantPeriodS+maxDiff {
			next = dominantPeriodS + maxDiff
		} else if next < dominantPeriodS-maxDiff {
			next = dominantPeriodS - maxDiff
		}
	}

	ms.LastPeriodUpdateDeltaS = next - ms.LongTermPeriodEstimateS
	ms.LongTermPeriodEstimateS = next

	// Consistency check: is this measurement broadly consistent with the
	// long-term track? Compared in PPM relative to the long-term estimate.
	consistencyPPM := 0.0
	if ms.LongTermPeriodEstimateS > 0 {
		consistencyPPM = math.Abs(diff) / ms.LongTermPeriodEstimateS * 1e6
	}
	if consistencyPPM <= longTermPeriodConsistencyPPM {
		ms.ConsecutivePeriodConsistentWindows++
		ms.LongTermPeriodEstimatorConfidence = math.Min(
			longTermPeriodConfidenceMax,
			ms.LongTermPeriodEstimatorConfidence+longTermPeriodConfidenceStep*clamp(localQuality, 0, 1),
		)
	} else {
		// Contradictory window: reset the consecutive counter and decay
		// confidence — but keep the estimate. A single bad 30s fit must not
		// destroy long-term state.
		ms.ConsecutivePeriodConsistentWindows = 0
		ms.LongTermPeriodEstimatorConfidence *= longTermPeriodConfidenceContradictDecay
	}

	if ms.lastLongTermPeriodSeedTS > 0 {
		ms.LongTermPeriodEstimatorAgeS = math.Max(0, nowUnix-ms.lastLongTermPeriodSeedTS)
	}
}

// reseedLongTermPeriodEstimator forces the long-term estimator back to a known
// period — used when a reacquire candidate replaces the applied period or on
// authority-mode transitions where prior long-term state is no longer relevant.
// Confidence is reset; consecutive-window counter goes to 1 (this seed counts).
func (ms *MultiSyncSolver) reseedLongTermPeriodEstimator(periodS, nowUnix float64) {
	if periodS <= 0 {
		return
	}
	ms.LongTermPeriodEstimateS = periodS
	ms.LongTermPeriodEstimatorConfidence = 0
	ms.ConsecutivePeriodConsistentWindows = 1
	ms.LastPeriodUpdateDeltaS = 0
	ms.lastLongTermPeriodSeedTS = nowUnix
	ms.LongTermPeriodEstimatorAgeS = 0
}
