export function humanizeSyncReason(value) {
  if (!value) return '—'
  return String(value).replaceAll('_', ' ')
}

export function formatAuthorityLabel(value) {
  if (!value) return 'unavailable'
  return String(value).replaceAll('_', ' ')
}

export function collectBlockingHandoffGates(syncState) {
  const failures = syncState?.handoff_gate_failures
  if (!failures || typeof failures !== 'object') return []
  const blocking = []
  Object.entries(failures).forEach(([groupName, groupValue]) => {
    if (!groupValue || typeof groupValue !== 'object') return
    Object.entries(groupValue).forEach(([gateName, gateState]) => {
      if (!gateState || typeof gateState !== 'object') return
      if (gateState.passed === false) {
        blocking.push(`${groupName}.${gateName}`)
      }
    })
  })
  return blocking
}

export function authorityModeLabel(syncState) {
  if (!syncState) return 'unavailable'
  const periodAuthority = String(syncState.period_authority ?? '').toLowerCase()
  if (periodAuthority === 'holdover') return 'holdover'
  if (periodAuthority === 'py_base') return 'Python base/bootstrap'
  if (periodAuthority === 'py_refined') return 'Python refined'
  if (periodAuthority === 'go_refined') return 'Go refined/runtime'
  return 'unavailable'
}

const OPERATIONAL_PERIOD_AUTHORITIES = new Set(['py_base', 'py_refined', 'go_refined'])

export function getOperationalPeriodTriple(syncState) {
  if (!syncState) return null
  const authority = String(syncState.period_authority ?? '').toLowerCase()
  if (!OPERATIONAL_PERIOD_AUTHORITIES.has(authority)) return null
  const baseRaw = syncState.base_period_s
  const deltaRaw = syncState.period_delta_s
  const effectiveRaw = syncState.effective_period_s
  if (![baseRaw, deltaRaw, effectiveRaw].every(isFiniteValue)) return null
  const base = Number(baseRaw)
  const delta = Number(deltaRaw)
  const effective = Number(effectiveRaw)
  if ([base, delta, effective].every(Number.isFinite)) return { base, delta, effective }
  return null
}

export function isFiniteValue(value) {
  return value !== null && value !== undefined && Number.isFinite(Number(value))
}

export function fmtNumber(value, digits = 2, suffix = '') {
  if (value === null || value === undefined) return '-'
  const n = Number(value)
  return Number.isFinite(n) ? `${n.toFixed(digits)}${suffix}` : '-'
}

export function selectCurrentSyncState(syncSnapshot, burstTimeline) {
  return syncSnapshot?.sync_state ?? burstTimeline?.sync_state ?? null
}

export function isWindowedPopulationAnchorMismatch(currentAnchorIcao, windowedAnchorIcao) {
  if (!currentAnchorIcao || !windowedAnchorIcao) return false
  return String(currentAnchorIcao) !== String(windowedAnchorIcao)
}
