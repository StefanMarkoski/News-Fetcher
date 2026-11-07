const BASE_URL = import.meta.env.VITE_SUPABASE_URL?.replace(/\/$/, '')
const KEY = import.meta.env.VITE_SUPABASE_PUBLISHABLE_KEY

const STORY_FIELDS = 'id,title,published_at,country_codes,excerpt,thumbnail_url,source_count,sources'
const TOP_STORY_HOURS = 48

async function request(query, headers = {}) {
  if (!BASE_URL || !KEY) throw new Error('The site is missing its Supabase settings.')
  const response = await fetch(`${BASE_URL}/rest/v1/articles?${query}`, {
    headers: { apikey: KEY, ...headers },
  })
  if (!response.ok) throw new Error(`Stories request failed with HTTP ${response.status}.`)
  return response
}

function storyQuery({ country, order, limit, offset = 0, since, minSources }) {
  const params = new URLSearchParams({ select: STORY_FIELDS, order, limit, offset })
  if (country) params.set('country_codes', `cs.{${country}}`)
  if (since) params.set('published_at', `gte.${since.toISOString()}`)
  if (minSources) params.set('source_count', `gte.${minSources}`)
  return params.toString()
}

const NEWEST_FIRST = 'published_at.desc'
const MOST_COVERED_FIRST = 'source_count.desc,published_at.desc'

export async function fetchTopStories(country, limit) {
  const since = new Date(Date.now() - TOP_STORY_HOURS * 3_600_000)
  const load = async (query) => (await request(storyQuery(query))).json()
  const base = { country, order: MOST_COVERED_FIRST, limit, minSources: 2 }
  const recent = await load({ ...base, since })
  // A quiet country may have no shared story lately, so widen to the whole 7-day archive.
  return recent.length ? recent : load(base)
}

export async function fetchLatestStories(country, limit, offset) {
  return (await request(storyQuery({ country, order: NEWEST_FIRST, limit, offset }))).json()
}

export async function fetchStoryCount(country) {
  const params = new URLSearchParams({ select: 'id', limit: '1', country_codes: `cs.{${country}}` })
  const response = await request(params.toString(), { Prefer: 'count=exact' })
  const total = response.headers.get('Content-Range')?.split('/')[1]
  return total && /^\d+$/.test(total) ? Number(total) : null
}
