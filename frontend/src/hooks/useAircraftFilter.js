import { useState } from 'react'
import { EMERGENCY_SQUAWKS } from '../utils/squawks'

export function aircraftPassesFilter(ac, filter) {
  if (filter === 'military')    return !!ac.military
  if (filter === 'mlat')        return !!ac.mlat
  if (filter === 'interesting') return !!ac.interesting
  if (filter === 'emergency')   return !!(ac.squawk && EMERGENCY_SQUAWKS[ac.squawk])
  if (filter === 'acas')        return !!ac.acas_ra_active
  return true
}

export function useAircraftFilter(initialFilter = 'all') {
  const [filter, setFilter] = useState(initialFilter)

  function filterAircraft(aircraft) {
    if (filter === 'all') return aircraft
    return aircraft.filter(ac => aircraftPassesFilter(ac, filter))
  }

  return { filter, setFilter, filterAircraft, passesFilter: ac => aircraftPassesFilter(ac, filter) }
}
