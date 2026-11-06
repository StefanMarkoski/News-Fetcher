-- Run once in the Supabase SQL editor. Safe to run again.

create table if not exists public.articles (
  id                text primary key,        -- the API's article UUID
  title             text not null,
  source            text not null default '',
  published_at      timestamptz not null,
  country_code      text not null,           -- ISO alpha-2, so the UK is GB
  excerpt           text not null default '',-- whole sentences, in English
  thumbnail_url     text,                    -- hosted by the publisher, may be null
  topics            text[] not null default '{}',
  original_url      text,
  original_language text,
  is_translated     boolean not null default false,
  ingested_at       timestamptz not null default now()
);

-- Dashboard: newest articles overall, and newest per country.
create index if not exists articles_published_at_idx
  on public.articles (published_at desc);
create index if not exists articles_country_published_at_idx
  on public.articles (country_code, published_at desc);
-- 7-day purge.
create index if not exists articles_ingested_at_idx
  on public.articles (ingested_at);

-- API requests used today. Only the ingestion script touches this.
create table if not exists public.api_quota (
  id   smallint primary key default 1 check (id = 1),
  day  date not null,
  used integer not null default 0
);

-- Some projects grant new tables to every role by default, so state access exactly.
revoke all on public.articles, public.api_quota from anon, authenticated;
grant select on public.articles to anon, authenticated;
grant select, insert, update, delete on public.articles, public.api_quota to service_role;

alter table public.articles enable row level security;
alter table public.api_quota enable row level security;

-- The frontend can read articles and nothing else. Secret keys bypass these rules.
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
