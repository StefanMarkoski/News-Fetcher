import { useState } from 'react'
import { COUNTRIES } from './countries.js'
import map from './worldMap.json'

const percentX = (x) => `${(x / map.width) * 100}%`
const percentY = (y) => `${(y / map.height) * 100}%`

export function WorldMap({ counts }) {
  const [active, setActive] = useState(null)
  const anchors = Object.fromEntries(map.covered.map((c) => [c.code, c]))

  return (
    <div className="atlas">
      <svg viewBox={`0 0 ${map.width} ${map.height}`} aria-hidden="true">
        <path className="atlas-land" d={map.land} />
        {map.covered.map((c) => (
          <a
            key={c.code}
            href={`#/country/${c.code.toLowerCase()}`}
            tabIndex={-1}
            onMouseEnter={() => setActive(c.code)}
            onMouseLeave={() => setActive(null)}
          >
            <path className={`atlas-country atlas-${c.code}${active === c.code ? ' is-active' : ''}`} d={c.path} />
            {/* Small countries still need a comfortable place to click. */}
            <circle className="atlas-target" cx={c.x} cy={c.y} r="9" />
          </a>
        ))}
        {COUNTRIES.map(({ code, labelX, labelY }) => (
          <g key={code} className={active === code ? 'atlas-mark is-active' : 'atlas-mark'}>
            <line x1={anchors[code].x} y1={anchors[code].y} x2={labelX} y2={labelY} />
            <circle cx={anchors[code].x} cy={anchors[code].y} r="2.6" />
          </g>
        ))}
      </svg>
      {COUNTRIES.map(({ code, name, short, labelX, labelY }) => (
        <a
          key={code}
          className={`atlas-label${active === code ? ' is-active' : ''}`}
          href={`#/country/${code.toLowerCase()}`}
          style={{ left: percentX(labelX), top: percentY(labelY) }}
          aria-label={counts[code] == null ? `${name} news` : `${name} news, ${counts[code]} stories`}
          onMouseEnter={() => setActive(code)}
          onMouseLeave={() => setActive(null)}
          onFocus={() => setActive(code)}
          onBlur={() => setActive(null)}
        >
          <span className="atlas-label-full">{name}</span>
          <span className="atlas-label-short">{short}</span>
        </a>
      ))}
    </div>
  )
}

export function Locator({ code }) {
  return (
    <svg className="locator" viewBox={`0 0 ${map.width} ${map.height}`} aria-hidden="true">
      <path className="atlas-land" d={map.land} />
      {map.covered.map((c) => (
        <path key={c.code} className={c.code === code ? 'locator-current' : 'atlas-land'} d={c.path} />
      ))}
    </svg>
  )
}
