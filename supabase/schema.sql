-- Run once in the Supabase SQL editor. Safe to run again.

-- One row per story. Articles from other publishers on the same story are merged
-- into `sources`, and `source_count` ranks the top stories.
create table if not exists public.articles (
  id            text primary key,         -- UUID of the first article found for the story
  title         text not null,
  published_at  timestamptz not null,
  country_codes text[] not null default '{}', -- anchor countries it's about, ISO alpha-2 (UK is GB)
  excerpt       text not null default '', -- whole sentences; empty if no clean text was found
  thumbnail_url text,                     -- hosted by the publisher, may be null
  topics        text[] not null default '{}',
  sources       jsonb not null default '[]', -- [{name, url, title, published_at}], one per publisher
  source_count  integer not null default 1,
  ingested_at   timestamptz not null default now()
);

-- Brings tables created by earlier versions of this file up to date.
alter table public.articles add column if not exists country_codes text[] not null default '{}';
alter table public.articles add column if not exists sources jsonb not null default '[]';
alter table public.articles add column if not exists source_count integer not null default 1;
alter table public.articles drop column if exists country_code;
alter table public.articles drop column if exists original_language;
alter table public.articles drop column if exists is_translated;
alter table public.articles drop column if exists source;
alter table public.articles drop column if exists original_url;

-- Dashboard: newest stories, top stories, and stories by country.
create index if not exists articles_published_at_idx
  on public.articles (published_at desc);
create index if not exists articles_top_stories_idx
  on public.articles (source_count desc, published_at desc);
create index if not exists articles_country_codes_idx
  on public.articles using gin (country_codes);
-- 7-day purge.
create index if not exists articles_ingested_at_idx
  on public.articles (ingested_at);

-- Every article handled, including those merged into a story, so none is fetched
-- twice. Only the ingestion script touches this.
create table if not exists public.seen_articles (
  id          text primary key,
  ingested_at timestamptz not null default now()
);
create index if not exists seen_articles_ingested_at_idx
  on public.seen_articles (ingested_at);

-- API requests used today. Only the ingestion script touches this.
create table if not exists public.api_quota (
  id   smallint primary key default 1 check (id = 1),
  day  date not null,
  used integer not null default 0
);

-- Some projects grant new tables to every role by default, so state access exactly.
revoke all on public.articles, public.seen_articles, public.api_quota from anon, authenticated;
grant select on public.articles to anon, authenticated;
grant select, insert, update, delete
  on public.articles, public.seen_articles, public.api_quota to service_role;

alter table public.articles enable row level security;
alter table public.seen_articles enable row level security;
alter table public.api_quota enable row level security;

-- The frontend can read stories and nothing else. Secret keys bypass these rules.
drop policy if exists "Articles are publicly readable" on public.articles;
create policy "Articles are publicly readable"
  on public.articles for select
  to anon, authenticated
  using (true);

-- Lets the ingestion script warn before the database nears the free plan limit.
create or replace function public.database_size_bytes()
returns bigint
language sql
security definer
set search_path = ''
as $$
  select pg_catalog.pg_database_size(pg_catalog.current_database());
$$;

revoke all on function public.database_size_bytes() from public, anon, authenticated;
grant execute on function public.database_size_bytes() to service_role;
