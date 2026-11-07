// Regenerates src/worldMap.json from public-domain Natural Earth outlines. The site
// ships only the result, so these tools aren't project dependencies. To run it:
//   npm i --no-save d3-geo topojson-client world-atlas && node scripts/build-map.mjs
import { readFileSync, writeFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { geoNaturalEarth1, geoPath } from 'd3-geo'
import { feature, merge } from 'topojson-client'

const require = createRequire(process.env.MAP_MODULES ? `${process.env.MAP_MODULES}/` : import.meta.url)
const topology = JSON.parse(readFileSync(require.resolve('world-atlas/countries-110m.json')))
const outDir = process.env.MAP_OUT ?? new URL('../src/', import.meta.url).pathname

const WIDTH = 960
const ANTARCTICA = '010'
const COVERED = {
  '840': { code: 'US', at: [-98, 39.5] },
  '643': { code: 'RU', at: [97, 62] },
  '156': { code: 'CN', at: [104, 34] },
  '826': { code: 'GB', at: [-1.5, 53] },
  '276': { code: 'DE', at: [10.3, 51] },
  '356': { code: 'IN', at: [78.5, 22] },
  '804': { code: 'UA', at: [31.5, 49] },
  '376': { code: 'IL', at: [34.9, 31.3] },
}

const countries = topology.objects.countries
const shown = countries.geometries.filter((g) => g.id !== ANTARCTICA)
const collection = { type: 'FeatureCollection', features: shown.map((g) => feature(topology, g)) }
const projection = geoNaturalEarth1().rotate([-11, 0]).fitWidth(WIDTH, collection)
const path = geoPath(projection).digits(1)
const height = Math.ceil(path.bounds(collection)[1][1])

const rest = shown.filter((g) => !COVERED[g.id])
// Natural Earth draws Crimea as Russian. Most countries and the UN recognise it as
// Ukraine, so it's drawn as part of Ukraine here, on top of the source outline.
const CRIMEA = [[32.4, 45.35], [33.6, 46.2], [35.3, 45.9], [36.7, 45.4], [35.4, 44.95], [34.1, 44.35], [33.3, 44.55]]
const crimeaPath = `M${CRIMEA.map((point) => projection(point).map((n) => n.toFixed(1)).join(',')).join('L')}Z`

const covered = shown
  .filter((g) => COVERED[g.id])
  .map((g) => {
    const { code, at } = COVERED[g.id]
    const [x, y] = projection(at)
    const outline = path(feature(topology, g))
    return { code, path: code === 'UA' ? outline + crimeaPath : outline, x: +x.toFixed(1), y: +y.toFixed(1) }
  })
  // Ukraine is drawn last, so its Crimea sits above Russia's outline.
  .sort((a, b) => (a.code === 'UA') - (b.code === 'UA'))

writeFileSync(
  `${outDir}/worldMap.json`,
  JSON.stringify({ width: WIDTH, height, land: path(merge(topology, rest)), covered }),
)
