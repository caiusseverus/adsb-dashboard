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

export function formatHardResidualRejectCounters(syncState) {
  const consecutive = Number(syncState?.go_diagnostic_consecutive_hard_residual_rejects ?? 0)
  const epochRejectTotal = Number(syncState?.update_epoch_reject_hard_residual ?? 0)
  const holdoverTotal = Number(syncState?.holdover_hard_residual_reject ?? 0)
  return {
    primaryLabel: `${consecutive} consecutive / ${epochRejectTotal} epoch-reject total`,
    holdoverLabel: `${holdoverTotal}`,
  }
}

function finiteOrNull(value) {
  if (value === null || value === undefined) return null
  const n = Number(value)
  return Number.isFinite(n) ? n : null
}

function boolLabel(value) {
  return value ? 'yes' : 'no'
}

function pickFirstDefined(...values) {
  for (const value of values) {
    if (value !== null && value !== undefined && value !== '') return value
  }
  return null
}

export function formatOperationalBlocker(syncState) {
  const raw = syncState ?? {}
  const handoffState = String(raw.handoff_state ?? '')
  const handoffReason = String(raw.handoff_reason ?? '')
  const blockingGate = String(raw.go_operational_blocking_gate ?? '')
  const syncUnusableReason = String(raw.go_sync_unusable_reason ?? raw.go_diagnostic_go_sync_unusable_reason ?? '')
  const holdover = raw.holdover === true || handoffState === 'HOLDOVER'
  const holdoverReason = String(raw.holdover_reason ?? raw.last_sync_reject_reason ?? '')
  const holdoverExitFailedGate = String(raw.holdover_exit_first_failed_gate ?? '')
  const holdoverCurrentBlocker = holdover || blockingGate.includes('go_not_holdover')
  const fitIcaoCount = finiteOrNull(pickFirstDefined(raw.go_diagnostic_fit_icao_count, raw.fit_icao_count))
  const fitObsCount = finiteOrNull(pickFirstDefined(raw.go_diagnostic_fit_observation_count, raw.fit_observation_count))
  const fitSpanS = finiteOrNull(pickFirstDefined(raw.go_diagnostic_fit_span_s, raw.fit_span_s))
  const residualAbs = finiteOrNull(raw.last_update_epoch_abs_residual_deg)
  const readyStreak = finiteOrNull(raw.go_operational_ready_streak)
  const readyThreshold = finiteOrNull(raw.go_operational_promotion_threshold)
  const strictGatePass = raw.strict_gate_pass
  const syncQuality = raw.sync_quality
  const phaseBasis = raw.phase_basis
  const phaseAuthority = raw.phase_authority
  const localisationSafePhase = raw.localisation_safe_phase
  const phaseAbsolute = raw.phase_is_absolute
  const geoPhaseStatus = String(raw.geographic_phase_status ?? '')
  const geoInvalidReason = String(raw.geographic_phase_invalid_reason ?? '')
  const refinementStatus = String(raw.operational_period_refinement_status ?? raw.refinement_status ?? '')
  const slopeSubreason = String(raw.slope_not_converged_subreason ?? '')
  const slopeNearZeroFail = String(raw.slope_near_zero_fail_reason ?? '')
  const slopeRegressionFail = String(raw.slope_regression_fail_reason ?? '')
  const reacquireIcaoCount = finiteOrNull(raw.go_diagnostic_reacquire_support_icao_count)
  const reacquireObsCount = finiteOrNull(raw.go_diagnostic_reacquire_support_obs_count)
  const shadowClass = pickFirstDefined(raw.shadow_delta_freeze_safety_class, raw.shadow_freeze_classification)

  const facts = []
  if (shadowClass) {
    facts.push({ label: 'Shadow', value: String(shadowClass) })
  }
  if (raw.go_operational_active === true) {
    return {
      category: 'operational active',
      title: 'Operational',
      explanation: 'Operational: Go refined period active.',
      facts: [
        { label: 'Handoff', value: formatAuthorityLabel(handoffState) },
        { label: 'Authority', value: formatAuthorityLabel(raw.period_authority) },
        ...facts,
      ],
      raw,
    }
  }

  if (holdoverCurrentBlocker) {
    facts.push({ label: 'Holdover reason', value: holdoverReason || 'unspecified' })
    if (holdoverExitFailedGate) facts.push({ label: 'Failed gate', value: holdoverExitFailedGate })
    if (reacquireObsCount != null || reacquireIcaoCount != null) {
      facts.push({ label: 'Reacquire obs/ICAOs', value: `${reacquireObsCount ?? 0}/${reacquireIcaoCount ?? 0}` })
    }
    if (residualAbs != null) facts.push({ label: 'Residual |deg|', value: residualAbs.toFixed(2) })
    return {
      category: 'holdover/reacquire blocked',
      title: 'Holdover / Reacquire blocked',
      explanation: `Holdover: waiting for reacquire support; failed gate: ${holdoverExitFailedGate || 'unknown'}.`,
      facts,
      raw,
    }
  }

  if (syncUnusableReason) {
    facts.push({ label: 'Unusable reason', value: humanizeSyncReason(syncUnusableReason) })
    if (syncQuality != null) facts.push({ label: 'Sync quality', value: String(syncQuality) })
    if (strictGatePass !== undefined) facts.push({ label: 'Strict gate pass', value: boolLabel(strictGatePass) })
    if (raw.period_authority != null) facts.push({ label: 'Period authority', value: formatAuthorityLabel(raw.period_authority) })
    return {
      category: 'sync unusable',
      title: 'Sync unusable',
      explanation: `Sync unusable: ${humanizeSyncReason(syncUnusableReason)}.`,
      facts,
      raw,
    }
  }

  if (
    blockingGate.includes('contributing_icaos') ||
    handoffReason.includes('contributing_icaos') ||
    handoffReason.includes('insufficient_support') ||
    (fitIcaoCount != null && fitIcaoCount < 2)
  ) {
    facts.push({ label: 'Fit ICAOs', value: fitIcaoCount != null ? String(fitIcaoCount) : '—' })
    if (fitObsCount != null) facts.push({ label: 'Fit observations', value: String(fitObsCount) })
    if (fitSpanS != null) facts.push({ label: 'Fit span', value: `${fitSpanS.toFixed(1)}s` })
    return {
      category: 'insufficient contributing ICAOs / Python base',
      title: 'Bootstrap support insufficient',
      explanation: 'Bootstrap: not enough contributing ICAOs.',
      facts,
      raw,
    }
  }

  if (
    blockingGate.includes('slope') ||
    handoffReason === 'slope_not_converged' ||
    slopeSubreason ||
    slopeNearZeroFail ||
    slopeRegressionFail
  ) {
    const slopeReason = slopeRegressionFail || slopeNearZeroFail || slopeSubreason || 'not converged'
    facts.push({ label: 'Subreason', value: humanizeSyncReason(slopeSubreason || 'n/a') })
    if (raw.go_diagnostic_residual_r2 != null) facts.push({ label: 'R²', value: Number(raw.go_diagnostic_residual_r2).toFixed(3) })
    if (raw.go_diagnostic_residual_slope_deg_per_s != null) {
      facts.push({ label: 'Trend deg/s', value: Number(raw.go_diagnostic_residual_slope_deg_per_s).toFixed(4) })
    }
    if (raw.go_diagnostic_slope_near_zero_window_s != null) {
      facts.push({ label: 'Near-zero window', value: `${Number(raw.go_diagnostic_slope_near_zero_window_s).toFixed(1)}s` })
    }
    return {
      category: 'slope not converged',
      title: 'Slope not converged',
      explanation: `Slope not converged: ${humanizeSyncReason(slopeReason)}.`,
      facts,
      raw,
    }
  }

  if (
    refinementStatus === 'insufficient_history' ||
    refinementStatus === 'unavailable' ||
    handoffReason.includes('insufficient_history')
  ) {
    facts.push({ label: 'Refinement status', value: formatAuthorityLabel(refinementStatus || 'unavailable') })
    if (fitObsCount != null || fitIcaoCount != null) {
      facts.push({ label: 'Fit obs/ICAOs', value: `${fitObsCount ?? 0}/${fitIcaoCount ?? 0}` })
    }
    return {
      category: 'refinement history insufficient',
      title: 'Refinement history insufficient',
      explanation: 'Refinement history insufficient: waiting for enough fit history.',
      facts,
      raw,
    }
  }

  if (
    handoffReason === 'go_ready_pending_hysteresis' ||
    handoffReason === 'go_ready_hysteresis_not_started' ||
    blockingGate.includes('hysteresis')
  ) {
    facts.push({ label: 'Ready streak', value: readyStreak != null ? String(readyStreak) : '—' })
    facts.push({ label: 'Threshold', value: readyThreshold != null ? String(readyThreshold) : '—' })
    return {
      category: 'hysteresis pending / not started',
      title: 'Hysteresis pending',
      explanation:
        handoffReason === 'go_ready_hysteresis_not_started'
          ? 'Hysteresis not started: waiting for first ready streak sample.'
          : 'Hysteresis pending: ready streak below promotion threshold.',
      facts,
      raw,
    }
  }

  if (
    localisationSafePhase === false ||
    geoPhaseStatus === 'invalid' ||
    geoInvalidReason ||
    (phaseAbsolute === false && String(phaseBasis ?? '').includes('anchor'))
  ) {
    facts.push({ label: 'Phase basis', value: formatAuthorityLabel(phaseBasis) })
    facts.push({ label: 'Phase authority', value: formatAuthorityLabel(phaseAuthority) })
    facts.push({ label: 'Absolute phase', value: boolLabel(phaseAbsolute === true) })
    if (geoPhaseStatus) facts.push({ label: 'Geo phase status', value: formatAuthorityLabel(geoPhaseStatus) })
    if (geoInvalidReason) facts.push({ label: 'Geo invalid reason', value: humanizeSyncReason(geoInvalidReason) })
    return {
      category: 'geographic phase not localisation-safe',
      title: 'Phase not localisation-safe',
      explanation: 'Phase not localisation-safe: anchor-relative only; no geographic calibration.',
      facts,
      raw,
    }
  }

  return {
    category: 'unknown',
    title: 'Unknown blocker',
    explanation: 'Unknown blocker: diagnostics do not currently map to a known operational gate.',
    facts: [
      { label: 'Handoff state', value: formatAuthorityLabel(handoffState) },
      { label: 'Handoff reason', value: formatAuthorityLabel(handoffReason) },
      { label: 'Blocking gate', value: formatAuthorityLabel(blockingGate) },
      ...facts,
    ],
    raw,
  }
}

function asBool(value) {
  return value === true
}

export function classifyResidualDiagnosticRow(row) {
  const displayClass = String(row?.display_residual_class ?? '')
  const residualClass = String(row?.residual_class ?? row?.classification ?? '')
  const familyId = String(row?.family_id ?? 'unknown')
  const familyRole = String(row?.family_role ?? 'unknown')
  const classifierInput = asBool(row?.classifier_input) || (asBool(row?.fit_eligible) && String(row?.event_kind ?? '') === 'burst')
  const chartOnlyDiagnostic = asBool(row?.chart_only_diagnostic) || !classifierInput
  const fitEligible = asBool(row?.fit_eligible)
  const syncUpdateEligible = asBool(row?.sync_update_eligible)
  const df11Diagnostic = displayClass.startsWith('df11_') || String(row?.event_kind ?? '') === 'df11'

  let group = 'unknown'
  if (classifierInput) group = 'classifier_input'
  else if (df11Diagnostic) group = 'df11_diagnostic'
  else if (displayClass.startsWith('burst_') || String(row?.event_kind ?? '') === 'burst') group = 'burst_diagnostic'
  else if (chartOnlyDiagnostic) group = 'chart_only_diagnostic'

  return {
    classifierInput,
    chartOnlyDiagnostic,
    fitEligible,
    syncUpdateEligible,
    df11Diagnostic,
    group,
    displayClass: displayClass || 'unknown',
    residualClass: residualClass || 'unknown',
    familyId,
    familyRole,
    familyAssignmentReason: row?.family_assignment_reason ?? 'unknown',
    contaminationState: row?.contamination_state ?? 'unknown',
    contaminationReason: row?.contamination_reason ?? 'unknown',
    rejectReason: row?.reject_reason ?? row?.exclusion_reason ?? null,
    exclusionReason: row?.exclusion_reason ?? row?.reject_reason ?? null,
  }
}

export function summarizeResidualDiagnostics(observations, df11Dots) {
  const obs = Array.isArray(observations) ? observations : []
  const df11 = Array.isArray(df11Dots) ? df11Dots : []
  let classifierInputs = 0
  let chartOnly = 0
  let dominant = 0
  let secondary = 0
  let outlier = 0
  let df11Early = 0
  let df11Late = 0
  let df11OnTime = 0
  let contaminationState = 'unknown'
  let contaminationReason = 'unknown'

  for (const row of obs) {
    const meta = classifyResidualDiagnosticRow(row)
    if (meta.classifierInput) classifierInputs += 1
    if (meta.chartOnlyDiagnostic) chartOnly += 1
    if (meta.familyRole === 'dominant') dominant += 1
    else if (meta.familyRole === 'secondary') secondary += 1
    else if (meta.familyRole === 'outlier') outlier += 1
    if (contaminationState === 'unknown' && meta.contaminationState !== 'unknown') {
      contaminationState = meta.contaminationState
    }
    if (contaminationReason === 'unknown' && meta.contaminationReason !== 'unknown') {
      contaminationReason = meta.contaminationReason
    }
  }

  for (const dot of df11) {
    const timing = String(dot?.timing_class ?? '')
    if (timing === 'early') df11Early += 1
    else if (timing === 'late') df11Late += 1
    else if (timing === 'on_time') df11OnTime += 1
  }

  return {
    classifierInputs,
    chartOnlyDiagnostics: chartOnly,
    df11Early,
    df11Late,
    df11OnTime,
    dominantFamilyCount: dominant,
    secondaryFamilyCount: secondary,
    outlierFamilyCount: outlier,
    contaminationState,
    contaminationReason,
  }
}
