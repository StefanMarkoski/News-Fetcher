import { useEffect, useRef, useState } from 'react'
import { coverageLabel, exactTime, timeAgo } from './format.js'

function StoryImage({ story, className }) {
  const [failed, setFailed] = useState(false)
  if (!story.thumbnail_url || failed) return null
  return (
    <img
      className={className}
      src={story.thumbnail_url}
      alt=""
      loading="lazy"
      referrerPolicy="no-referrer"
      onError={() => setFailed(true)}
    />
  )
}

function StoryMeta({ story }) {
  return (
    <p className="story-meta">
      <span className={story.source_count > 1 ? 'story-coverage' : undefined}>{coverageLabel(story)}</span>
      <time dateTime={story.published_at} title={exactTime(story.published_at)}>{timeAgo(story.published_at)}</time>
    </p>
  )
}

export function LeadStory({ story, onOpen }) {
  return (
    <article className="lead">
      <StoryImage story={story} className="lead-image" />
      <div className="lead-body">
        <h3 className="lead-title">
          <button type="button" onClick={() => onOpen(story)}>{story.title}</button>
        </h3>
        {story.excerpt && <p className="lead-excerpt">{story.excerpt}</p>}
        <StoryMeta story={story} />
      </div>
    </article>
  )
}

export function StoryRow({ story, onOpen }) {
  return (
    <li className="row">
      <div className="row-body">
        <h3 className="row-title">
          <button type="button" onClick={() => onOpen(story)}>{story.title}</button>
        </h3>
        {story.excerpt && <p className="row-excerpt">{story.excerpt}</p>}
        <StoryMeta story={story} />
      </div>
      <StoryImage story={story} className="row-image" />
    </li>
  )
}

export function StoryDialog({ story, onClose }) {
  const dialog = useRef(null)

  useEffect(() => {
    if (story) dialog.current?.showModal()
    else dialog.current?.close()
  }, [story])

  return (
    <dialog ref={dialog} className="story-dialog" onClose={onClose} aria-labelledby="story-dialog-title">
      {story && (
        <>
          <div className="dialog-bar">
            <button type="button" className="dialog-close" onClick={onClose}>Close</button>
          </div>
          <StoryImage story={story} className="dialog-image" />
          <h2 id="story-dialog-title" className="dialog-title">{story.title}</h2>
          <p className="story-meta">
            <time dateTime={story.published_at}>{exactTime(story.published_at)}</time>
          </p>
          {story.excerpt
            ? <p className="dialog-excerpt">{story.excerpt}</p>
            : <p className="dialog-excerpt is-empty">No preview is available. Read this story at the source.</p>}
          <section aria-labelledby="sources-heading">
            <h3 id="sources-heading" className="sources-heading">
              {story.sources.length > 1 ? `Read at the source (${story.sources.length})` : 'Read at the source'}
            </h3>
            <ul className="sources">
              {story.sources.map((source) => (
                <li key={source.name}>
                  {source.url ? (
                    <a href={source.url} target="_blank" rel="noopener noreferrer">
                      <span className="source-name">{source.name}</span>
                      <span className="source-title">{source.title}</span>
                    </a>
                  ) : (
                    <span>
                      <span className="source-name">{source.name}</span>
                      <span className="source-title">{source.title}</span>
                    </span>
                  )}
                </li>
              ))}
            </ul>
          </section>
        </>
      )}
    </dialog>
  )
}
