import { useCallback, useEffect, useState } from 'react'
import { fetchLatestStories, fetchStoryCount, fetchTopStories } from './api.js'
import { COUNTRIES, countryByCode } from './countries.js'
import { LeadStory, StoryDialog, StoryRow } from './Stories.jsx'
import { Locator, WorldMap } from './WorldMap.jsx'

const TOP_LIMIT = 5
const PAGE_SIZE = 20

function useRoute() {
  const read = () => window.location.hash.match(/^#\/country\/([a-z]{2})$/i)?.[1] ?? null
  const [code, setCode] = useState(read)
  useEffect(() => {
    const update = () => {
      setCode(read())
      window.scrollTo(0, 0)
    }
    window.addEventListener('hashchange', update)
    return () => window.removeEventListener('hashchange', update)
  }, [])
  return code
}

function useStories(country) {
  const [state, setState] = useState({ status: 'loading', top: [], latest: [], hasMore: false })

  const load = useCallback(async (isCurrent) => {
    setState({ status: 'loading', top: [], latest: [], hasMore: false })
    try {
      const [top, latest] = await Promise.all([
        fetchTopStories(country, TOP_LIMIT),
        fetchLatestStories(country, PAGE_SIZE, 0),
      ])
      if (isCurrent()) setState({ status: 'ready', top, latest, hasMore: latest.length === PAGE_SIZE })
    } catch {
      if (isCurrent()) setState({ status: 'error', top: [], latest: [], hasMore: false })
    }
  }, [country])

  useEffect(() => {
    let current = true
    load(() => current)
    return () => { current = false }
  }, [load])

  const loadMore = async () => {
    setState((s) => ({ ...s, loadingMore: true, moreFailed: false }))
    try {
      const more = await fetchLatestStories(country, PAGE_SIZE, state.latest.length)
      setState((s) => ({ ...s, latest: [...s.latest, ...more], hasMore: more.length === PAGE_SIZE, loadingMore: false }))
    } catch {
      setState((s) => ({ ...s, loadingMore: false, moreFailed: true }))
    }
  }

  return { ...state, loadMore, reload: () => load(() => true) }
}

function StorySections({ country, emptyMessage, onOpen }) {
  const { status, top, latest, hasMore, loadingMore, moreFailed, loadMore, reload } = useStories(country)

  if (status === 'loading') return <p className="status" role="status">Loading stories…</p>
  if (status === 'error') {
    return (
      <div className="status" role="alert">
        <p>Stories couldn't be loaded. Check your connection, then try again.</p>
        <button type="button" className="button" onClick={reload}>Try again</button>
      </div>
    )
  }
  if (!latest.length) return <p className="status">{emptyMessage}</p>

  const topIds = new Set(top.map((story) => story.id))
  const rest = latest.filter((story) => !topIds.has(story.id))
  const [lead, ...others] = top

  return (
    <>
      {lead && (
        <section className="section" aria-labelledby="top-heading">
          <div className="section-head">
            <h2 id="top-heading">Top stories</h2>
            <p>Ranked by how many publishers are covering each story</p>
          </div>
          <LeadStory story={lead} onOpen={onOpen} />
          {others.length > 0 && (
            <ul className="rows rows-top">
              {others.map((story) => <StoryRow key={story.id} story={story} onOpen={onOpen} />)}
            </ul>
          )}
        </section>
      )}
      <section className="section" aria-labelledby="latest-heading">
        <div className="section-head">
          <h2 id="latest-heading">Latest</h2>
        </div>
        <ul className="rows">
          {rest.map((story) => <StoryRow key={story.id} story={story} onOpen={onOpen} />)}
        </ul>
        {moreFailed && <p className="status" role="alert">More stories couldn't be loaded. Try again.</p>}
        {hasMore && (
          <button type="button" className="button" onClick={loadMore} disabled={loadingMore}>
            {loadingMore ? 'Loading…' : 'Show more stories'}
          </button>
        )}
      </section>
    </>
  )
}

function CountryList({ counts, current }) {
  return (
    <ul className="country-list">
      {COUNTRIES.map(({ code, name }) => (
        <li key={code}>
          <a
            href={`#/country/${code.toLowerCase()}`}
            aria-current={current === code ? 'page' : undefined}
          >
            <span>{name}</span>
            {counts[code] != null && <span className="country-count">{counts[code]}</span>}
          </a>
        </li>
      ))}
    </ul>
  )
}

function useCounts() {
  const [counts, setCounts] = useState({})
  useEffect(() => {
    let current = true
    COUNTRIES.forEach(({ code }) => {
      fetchStoryCount(code)
        .then((total) => { if (current) setCounts((c) => ({ ...c, [code]: total })) })
        .catch(() => {}) // The counts are a detail, and the page works without them.
    })
    return () => { current = false }
  }, [])
  return counts
}

function HomePage({ counts, onOpen }) {
  useEffect(() => { document.title = 'News Fetcher' }, [])
  return (
    <>
      <section className="map-section" aria-labelledby="map-heading">
        <h1 id="map-heading">Choose a country to read its news</h1>
        <WorldMap counts={counts} />
        <CountryList counts={counts} />
      </section>
      <div className="page-width">
        <StorySections emptyMessage="No stories are available yet. New stories arrive several times a day." onOpen={onOpen} />
      </div>
    </>
  )
}

function CountryPage({ country, counts, onOpen }) {
  useEffect(() => { document.title = `${country.name} news | News Fetcher` }, [country])
  return (
    <div className="page-width">
      <header className="country-header">
        <div>
          <a className="back-link" href="#/">All countries</a>
          <h1>{country.name}</h1>
          <p>Stories about {country.name} from English-language publishers, from the last 7 days.</p>
        </div>
        <Locator code={country.code} />
      </header>
      <nav aria-label="Countries" className="country-nav">
        <CountryList counts={counts} current={country.code} />
      </nav>
      <StorySections
        key={country.code}
        country={country.code}
        emptyMessage={`No stories about ${country.name} from the last 7 days.`}
        onOpen={onOpen}
      />
    </div>
  )
}

function NotFound() {
  return (
    <div className="page-width status">
      <h1>This page doesn't exist</h1>
      <a className="button" href="#/">See all countries</a>
    </div>
  )
}

export default function App() {
  const code = useRoute()
  const country = countryByCode(code)
  const counts = useCounts()
  const [openStory, setOpenStory] = useState(null)

  return (
    <>
      <header className="masthead">
        <div className="page-width masthead-inner">
          <a className="wordmark" href="#/">News Fetcher</a>
          <p className="masthead-note">World news by country, linked to its sources</p>
        </div>
      </header>
      <main>
        {!code && <HomePage counts={counts} onOpen={setOpenStory} />}
        {code && country && <CountryPage country={country} counts={counts} onOpen={setOpenStory} />}
        {code && !country && <NotFound />}
      </main>
      <footer className="footer">
        <div className="page-width">
          <p>Headlines, excerpts and images belong to their publishers. Each story links to the original articles.</p>
          <p>Coverage counts show how many publishers in our news feeds ran a story, not how many people read it.</p>
        </div>
      </footer>
      <StoryDialog story={openStory} onClose={() => setOpenStory(null)} />
    </>
  )
}
