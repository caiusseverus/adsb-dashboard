import { describe, it } from 'node:test'
import assert from 'node:assert'
import {
  getOperationalPeriodTriple,
  isFiniteValue,
  fmtNumber,
  selectCurrentSyncState,
  isWindowedPopulationAnchorMismatch,
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
})
