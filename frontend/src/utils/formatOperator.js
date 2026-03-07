/**
 * Normalise ALL-CAPS operator names (FAA/ADSBExchange style) to title case,
 * while preserving known acronyms and abbreviations.
 *
 * Examples:
 *   "BRITISH AIRWAYS PLC"  → "British Airways Plc"
 *   "ROYAL AIR FORCE"      → "Royal Air Force"
 *   "RAF"                  → "RAF"
 *   "US AIR FORCE"         → "US Air Force"
 */

const ACRONYMS = new Set([
  // Military
  'RAF', 'USAF', 'USMC', 'USN', 'RAAF', 'RNZAF', 'RCAF', 'IAF', 'FAA',
  'NATO', 'AAC', 'RN', 'RM',
  // Legal suffixes
  'PLC', 'LLC', 'LTD', 'INC', 'GMBH', 'AG', 'SA', 'SAS', 'NV', 'BV', 'AB',
  // Country/region codes commonly seen in operator names
  'UK', 'US', 'USA', 'GB', 'EU',
  // Airlines/orgs
  'KLM', 'LOT', 'SAS', 'TAM', 'TAP', 'LAM',
])

export function formatOperator(name) {
  if (!name) return name
  // If the string is not all-caps (mixed case already), return as-is
  if (name !== name.toUpperCase()) return name
  return name
    .split(/\s+/)
    .map(word => ACRONYMS.has(word) ? word : word.charAt(0) + word.slice(1).toLowerCase())
    .join(' ')
}
