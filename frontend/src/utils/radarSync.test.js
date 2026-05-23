import { describe, it } from 'node:test'
import assert from 'node:assert'
import {
  getOperationalPeriodTriple,
  isFiniteValue,
  fmtNumber,
  selectCurrentSyncState,
  isWindowedPopulationAnchorMismatch,
  formatHardResidualRejectCounters,
  formatOperationalBlocker,
  classifyResidualDiagnosticRow,
  summarizeResidualDiagnostics,
} from './radarSync.js'

describe('radarSync', () => {
  describe('getOperationalPeriodTriple', () => {
    it('returns the triple for operational authorities', () => {
      const state = {
        period_authority: 'py_refined',
        base_period_s: 3.9765,
        period_delta_s: 0.01381,
        effective_period_s: 3.99031,
      }
      const triple = getOperationalPeriodTriple(state)
      assert.deepStrictEqual(triple, {
        base: 3.9765,
        delta: 0.01381,
        effective: 3.99031,
      })
    })

    it('returns null for holdover even when retained values are present', () => {
      const state = {
        period_authority: 'holdover',
        base_period_s: 3.9765,
        period_delta_s: 0.01381,
        effective_period_s: 3.99031,
      }
      const triple = getOperationalPeriodTriple(state)
      assert.strictEqual(triple, null)
    })

    it('returns null for unavailable even when retained values are present', () => {
      const state = {
        period_authority: 'unavailable',
        base_period_s: 3.9765,
        period_delta_s: 0.01381,
        effective_period_s: 3.99031,
      }
      const triple = getOperationalPeriodTriple(state)
      assert.strictEqual(triple, null)
    })

    it('returns null when values are missing even for operational authority', () => {
      const state = {
        period_authority: 'go_refined',
        base_period_s: 3.9765,
        period_delta_s: null,
        effective_period_s: 3.99031,
      }
      const triple = getOperationalPeriodTriple(state)
      assert.strictEqual(triple, null)
    })
  })

  describe('operational vs diagnostic separation', () => {
    it('with holdover + retained Go delta, operational triple is null but Go delta is visible', () => {
      const syncState = {
        period_authority: 'holdover',
        base_period_s: 3.9765,
        period_delta_s: 0.01381,
        effective_period_s: 3.99031,
        go_diagnostic_period_delta_s: 0.01381,
      }

      // Current operational state logic
      const operationalPeriod = getOperationalPeriodTriple(syncState)
      assert.strictEqual(operationalPeriod, null)
      assert.strictEqual(fmtNumber(operationalPeriod?.base, 4, 's'), '-')
      assert.strictEqual(
        fmtNumber(operationalPeriod?.delta != null ? operationalPeriod.delta * 1000 : null, 2, 'ms'),
        '-'
      )
      assert.strictEqual(fmtNumber(operationalPeriod?.effective, 4, 's'), '-')

      // Go diagnostic refiner card logic (mirrors RadarPage.jsx)
      const goDeltaVisible = Number.isFinite(Number(syncState.go_diagnostic_period_delta_s))
      assert.strictEqual(goDeltaVisible, true)
      const retainedDeltaMs =
        Number(syncState.go_diagnostic_retained_delta_s ?? syncState.go_diagnostic_period_delta_s) * 1000
      assert.strictEqual(fmtNumber(retainedDeltaMs, 2, 'ms'), '13.81ms')
    })
  })

  describe('snapshot consistency policy', () => {
    it('prefers compact sync snapshot for current sync state', () => {
      const syncSnapshot = { sync_state: { phase_anchor_icao: '4D2270', sequence: 120 } }
      const burstTimeline = { sync_state: { phase_anchor_icao: 'AAD213', sequence: 119 } }
      const selected = selectCurrentSyncState(syncSnapshot, burstTimeline)
      assert.deepStrictEqual(selected, syncSnapshot.sync_state)
    })

    it('flags mismatch between current and windowed anchors', () => {
      assert.strictEqual(isWindowedPopulationAnchorMismatch('4D2270', 'AAD213'), true)
      assert.strictEqual(isWindowedPopulationAnchorMismatch('4D2270', '4D2270'), false)
      assert.strictEqual(isWindowedPopulationAnchorMismatch('4D2270', null), false)
    })
  })

  describe('diagnostic formatting fallbacks', () => {
    it('formats missing discontinuity fields safely', () => {
      const syncState = {
        go_diagnostic_fit_epoch_reset_reason: 'phase_offset_discontinuity',
        go_diagnostic_phase_offset_discontinuity_old_deg: null,
        go_diagnostic_phase_offset_discontinuity_new_deg: undefined,
      }
      assert.strictEqual(fmtNumber(syncState.go_diagnostic_phase_offset_discontinuity_old_deg, 2), '-')
      assert.strictEqual(fmtNumber(syncState.go_diagnostic_phase_offset_discontinuity_new_deg, 2), '-')
    })

    it('formats hard residual reject counters with consistent fields', () => {
      const counters = formatHardResidualRejectCounters({
        go_diagnostic_consecutive_hard_residual_rejects: 2,
        update_epoch_reject_hard_residual: 9,
        holdover_hard_residual_reject: 4,
      })
      assert.strictEqual(counters.primaryLabel, '2 consecutive / 9 epoch-reject total')
      assert.strictEqual(counters.holdoverLabel, '4')
    })
  })

  describe('formatOperationalBlocker', () => {
    it('formats active state', () => {
      const result = formatOperationalBlocker({ go_operational_active: true, handoff_state: 'GO_REFINED' })
      assert.strictEqual(result.category, 'operational active')
      assert.match(result.explanation, /Operational:/)
    })

    it('formats holdover/reacquire blocked', () => {
      const result = formatOperationalBlocker({
        holdover: true,
        holdover_reason: 'holdover_sync_invalid',
        holdover_exit_first_failed_gate: 'reacquire_fit_support',
        go_diagnostic_reacquire_support_obs_count: 4,
        go_diagnostic_reacquire_support_icao_count: 1,
      })
      assert.strictEqual(result.category, 'holdover/reacquire blocked')
      assert.match(result.explanation, /Holdover:/)
    })

    it('does not prioritize stale holdover diagnostics over current hysteresis blocker', () => {
      const result = formatOperationalBlocker({
        holdover: false,
        go_operational_blocking_gate: 'go_readiness_hysteresis',
        handoff_reason: 'go_ready_hysteresis_not_started',
        holdover_reason: 'holdover_sync_invalid',
        holdover_exit_first_failed_gate: 'reacquire_valid_ref_icao',
      })
      assert.strictEqual(result.category, 'hysteresis pending / not started')
      assert.match(result.explanation, /Hysteresis not started:/)
    })

    it('keeps holdover blocker when current gate is go_not_holdover', () => {
      const result = formatOperationalBlocker({
        holdover: false,
        go_operational_blocking_gate: 'go_not_holdover',
        holdover_reason: 'holdover_sync_invalid',
        holdover_exit_first_failed_gate: 'reacquire_valid_ref_icao',
      })
      assert.strictEqual(result.category, 'holdover/reacquire blocked')
      assert.match(result.explanation, /Holdover:/)
    })

    it('formats slope blocked', () => {
      const result = formatOperationalBlocker({
        handoff_reason: 'slope_not_converged',
        slope_regression_fail_reason: 'regression_trend_not_decreasing',
      })
      assert.strictEqual(result.category, 'slope not converged')
      assert.match(result.explanation, /Slope not converged:/)
    })

    it('formats sync unusable', () => {
      const result = formatOperationalBlocker({
        go_sync_unusable_reason: 'strict_gate_fail',
        strict_gate_pass: false,
      })
      assert.strictEqual(result.category, 'sync unusable')
      assert.match(result.explanation, /Sync unusable:/)
    })

    it('formats hysteresis pending', () => {
      const result = formatOperationalBlocker({
        handoff_reason: 'go_ready_pending_hysteresis',
        go_operational_ready_streak: 2,
        go_operational_promotion_threshold: 4,
      })
      assert.strictEqual(result.category, 'hysteresis pending / not started')
      assert.match(result.explanation, /Hysteresis pending:/)
    })

    it('formats bootstrap/icao blocked', () => {
      const result = formatOperationalBlocker({
        go_operational_blocking_gate: 'go_readiness.contributing_icaos',
        go_diagnostic_fit_icao_count: 1,
      })
      assert.strictEqual(result.category, 'insufficient contributing ICAOs / Python base')
      assert.match(result.explanation, /Bootstrap:/)
    })

    it('prefers typed readiness hysteresis fields when present', () => {
      const result = formatOperationalBlocker({
        readiness_state: 'hysteresis_not_started',
        readiness_reason: 'go_ready_hysteresis_not_started',
        handoff_reason: 'go_holdover',
        go_operational_blocking_gate: 'go_readiness.go_not_holdover',
      })
      assert.strictEqual(result.category, 'hysteresis pending / not started')
      assert.match(result.explanation, /Hysteresis not started:/)
    })

    it('prefers typed readiness holdover when current readiness says holdover', () => {
      const result = formatOperationalBlocker({
        readiness_state: 'holdover',
        readiness_reason: 'reacquire_ref_pos_fresh',
        holdover_exit_first_failed_gate: 'reacquire_ref_pos_fresh',
        holdover_reason: 'insufficient_aircraft',
        go_operational_blocking_gate: 'go_readiness_hysteresis',
      })
      assert.strictEqual(result.category, 'holdover/reacquire blocked')
      assert.match(result.explanation, /Holdover:/)
    })

    it('formats non-geographic phase', () => {
      const result = formatOperationalBlocker({
        localisation_safe_phase: false,
        phase_basis: 'anchor_relative',
        phase_is_absolute: false,
      })
      assert.strictEqual(result.category, 'geographic phase not localisation-safe')
      assert.match(result.explanation, /Phase not localisation-safe:/)
    })

    it('falls back to unknown', () => {
      const result = formatOperationalBlocker({
        handoff_state: 'GO_REFINING',
        handoff_reason: 'mystery_blocker',
      })
      assert.strictEqual(result.category, 'unknown')
      assert.match(result.explanation, /Unknown blocker:/)
    })
  })

  describe('classifyResidualDiagnosticRow', () => {
    it('labels classifier input rows as classifier_input', () => {
      const row = classifyResidualDiagnosticRow({
        event_kind: 'burst',
        fit_eligible: true,
        classifier_input: true,
        family_id: 'primary',
        family_role: 'dominant',
      })
      assert.strictEqual(row.classifierInput, true)
      assert.strictEqual(row.group, 'classifier_input')
      assert.strictEqual(row.familyRole, 'dominant')
    })

    it('labels DF11 rows as diagnostic-only when not fit eligible', () => {
      const row = classifyResidualDiagnosticRow({
        event_kind: 'df11',
        display_residual_class: 'df11_late',
        fit_eligible: false,
      })
      assert.strictEqual(row.classifierInput, false)
      assert.strictEqual(row.chartOnlyDiagnostic, true)
      assert.strictEqual(row.group, 'df11_diagnostic')
    })

    it('fails safely on missing fields', () => {
      const row = classifyResidualDiagnosticRow({})
      assert.strictEqual(row.group, 'chart_only_diagnostic')
      assert.strictEqual(row.familyId, 'unknown')
      assert.strictEqual(row.familyRole, 'unknown')
      assert.strictEqual(row.contaminationState, 'unknown')
    })
  })

  describe('summarizeResidualDiagnostics', () => {
    it('summarizes family and df11 diagnostics counts', () => {
      const summary = summarizeResidualDiagnostics(
        [
          { event_kind: 'burst', fit_eligible: true, classifier_input: true, family_role: 'dominant', contamination_state: 'insufficient_data', contamination_reason: 'x' },
          { event_kind: 'burst', fit_eligible: false, chart_only_diagnostic: true, family_role: 'outlier' },
          { event_kind: 'burst', fit_eligible: true, family_role: 'secondary' },
        ],
        [
          { timing_class: 'early' },
          { timing_class: 'late' },
          { timing_class: 'on_time' },
        ],
      )
      assert.strictEqual(summary.classifierInputs, 2)
      assert.strictEqual(summary.chartOnlyDiagnostics, 1)
      assert.strictEqual(summary.df11Early, 1)
      assert.strictEqual(summary.df11Late, 1)
      assert.strictEqual(summary.dominantFamilyCount, 1)
      assert.strictEqual(summary.secondaryFamilyCount, 1)
      assert.strictEqual(summary.outlierFamilyCount, 1)
      assert.strictEqual(summary.contaminationState, 'insufficient_data')
    })
  })
})
