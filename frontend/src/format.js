const relative = new Intl.RelativeTimeFormat('en', { numeric: 'auto' })
const fullDate = new Intl.DateTimeFormat('en', { day: 'numeric', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' })

export function timeAgo(value) {
  const minutes = Math.round((new Date(value) - Date.now()) / 60_000)
  if (Math.abs(minutes) < 60) return relative.format(minutes, 'minute')
  const hours = Math.round(minutes / 60)
  if (Math.abs(hours) < 24) return relative.format(hours, 'hour')
  return relative.format(Math.round(hours / 24), 'day')
}

export const exactTime = (value) => fullDate.format(new Date(value))

export const coverageLabel = (story) =>
  story.source_count > 1 ? `Covered by ${story.source_count} sources` : story.sources[0]?.name
