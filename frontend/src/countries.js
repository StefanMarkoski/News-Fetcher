// The countries with their own page. Label positions are in map coordinates (see WorldMap).
export const COUNTRIES = [
  { code: 'US', name: 'United States', short: 'US', labelX: 190, labelY: 72 },
  { code: 'GB', name: 'United Kingdom', short: 'UK', labelX: 392, labelY: 52 },
  { code: 'DE', name: 'Germany', short: 'DE', labelX: 470, labelY: 28 },
  { code: 'UA', name: 'Ukraine', short: 'UA', labelX: 566, labelY: 122 },
  { code: 'RU', name: 'Russia', short: 'RU', labelX: 690, labelY: 22 },
  { code: 'IL', name: 'Israel', short: 'IL', labelX: 500, labelY: 196 },
  { code: 'IN', name: 'India', short: 'IN', labelX: 669, labelY: 262 },
  { code: 'CN', name: 'China', short: 'CN', labelX: 800, labelY: 112 },
]

export const countryByCode = (code) => COUNTRIES.find((c) => c.code === code?.toUpperCase())
