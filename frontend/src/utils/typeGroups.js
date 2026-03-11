/**
 * Canonical aircraft type group definitions — shared across map, heatmaps, and any future card.
 * Each group has a value (key), label, color, and either a types[] array of ICAO type codes
 * or a category prefix (e.g. 'H' for helicopters).
 */
export const TYPE_GROUPS = [
  {
    value: 'widebody', label: 'Widebody', color: '#4d9de0',
    types: ['B744','B748','B763','B764','B772','B773','B77W','B77L','B788','B789','B78X',
            'A332','A333','A342','A343','A359','A35K','A388'],
  },
  {
    value: 'narrowbody', label: 'Narrowbody', color: '#3be8b0',
    types: ['A318','A319','A320','A321','A20N','A21N','B735','B736','B737','B738','B739',
            'B38M','B39M','B752','B753','B757','E195','E290'],
  },
  {
    value: 'regional', label: 'Regional', color: '#7ecbff',
    types: ['CRJ2','CRJ7','CRJ9','CRJX','E170','E175','E190','AT72','AT75','AT76',
            'DH8A','DH8B','DH8C','DH8D','SF34','J328','E120'],
  },
  {
    value: 'bizjet', label: 'Biz Jet', color: '#d4a017',
    types: ['C25A','C25B','C25C','C510','C525','C550','C560','C56X','C650','C680','C68A',
            'C700','C750','GL5T','GLEX','GLF4','GLF5','GLF6','E55P','PC24','F2TH','F900',
            'FA7X','F7X','LJ35','LJ40','LJ45','LJ55','LJ60'],
  },
  {
    // Expanded from "Piston GA" — includes turboprops (TBM, PC-12, etc.)
    value: 'ga', label: 'GA', color: '#a3e635',
    types: ['C172','C152','C182','C206','C208','PA28','PA32','PA34','PA44','DA40','DA42',
            'SR20','SR22','C150','BE36','BE58','M20P','M20T','M20J','C210','C421','C441',
            'C340','PA31','PA46','BE99','BE20','P180','TBM7','TBM8','TBM9','PC12'],
  },
  {
    value: 'rotary', label: 'Rotary', color: '#f97316',
    category: 'H',  // matches any type_category starting with 'H'
  },
  {
    // Renamed from "Mil jets" — combat jets and fast trainers
    value: 'fastjet', label: 'Fast Jet', color: '#f85149',
    types: ['F16','FA18','F18','EF18','F15','F35','EUFI','RFAL','GRIF','HAWK','MB339','L39',
            'PC21','PC9','T38','F86','TFAL','SU27','SU30','SU35','MIG2','MIG3','JAS3',
            'A10','AV8B','HAR2','HUNT','TPHR'],
  },
  {
    // New — strategic/tactical transports and dedicated freighters
    value: 'cargo', label: 'Cargo', color: '#bc8cff',
    types: ['C17','C5M','C130','C30J','C27J','CN35','C295','A400','AN12','AN22','AN26',
            'AN72','AN32','IL76','IL78','L382','CL44','Y20','A124','A225',
            'K35R','K35E','KC10','KC46','KDC1'],
  },
]

export const TYPE_GROUP_OTHER_COLOR = '#484f58'

/**
 * Return the TYPE_GROUPS entry matching a type_code or type_category.
 * Returns null if no group matches (= "Other").
 */
export function getTypeGroup(type_code, type_category) {
  if (!type_code && !type_category) return null
  for (const g of TYPE_GROUPS) {
    if (g.category && type_category?.toUpperCase().startsWith(g.category)) return g
    if (g.types && type_code && g.types.includes(type_code.toUpperCase())) return g
  }
  return null
}

/** Return the colour for a type_code/type_category, or OTHER colour. */
export function typeGroupColor(type_code, type_category) {
  return getTypeGroup(type_code, type_category)?.color ?? TYPE_GROUP_OTHER_COLOR
}

/** Palette for operator/country color-by-name modes (10 distinct colours). */
export const NAMED_PALETTE = [
  '#e63946','#4d9de0','#3be8b0','#f97316','#d4a017',
  '#7ecbff','#a3e635','#ff6b9d','#00d2ff','#9b59b6',
]

/**
 * Build a { map: {name: color}, top: [{name, color}] } from an array of aircraft.
 * Top 10 values of `field` by count get palette colours; rest are TYPE_GROUP_OTHER_COLOR.
 */
export function buildNameColorMap(aircraft, field) {
  const counts = {}
  for (const ac of aircraft) {
    const v = ac[field]
    if (v) counts[v] = (counts[v] || 0) + 1
  }
  const sorted = Object.entries(counts).sort((a, b) => b[1] - a[1])
  const map = {}
  const top = []
  sorted.slice(0, 10).forEach(([name], i) => {
    map[name] = NAMED_PALETTE[i]
    top.push({ name, color: NAMED_PALETTE[i] })
  })
  return { map, top }
}
