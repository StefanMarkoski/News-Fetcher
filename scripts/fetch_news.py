#!/usr/bin/env python3
"""Fetch one news cycle of English world and politics articles into Supabase.

One run:
  1. Purges articles ingested more than RETENTION_DAYS ago.
  2. Reserves its API budget against a daily counter kept in Supabase.
  3. Reads the English world and politics feeds, newest first, until it reaches
     articles it already has (at most 100 list requests).
  4. Fetches each new article (at most 1,000 detail requests), and stores a card
     with an excerpt of whole sentences, tagged with the anchor countries it is
     about. The full article text is never stored: the provider's terms restrict
     republishing it, so readers continue at the source.
  5. Warns if the database is getting close to the free plan's limit.

The API's country filter can't drive this: combined with language=en it returns
nothing for six of the ten anchor countries, and its tags don't reflect what an
article is about. So countries are tagged from the article's own text instead.

The tables are defined in supabase/schema.sql.

Required environment variables:
  FREENEWS_API_KEY     FreeNewsApi.io key
  SUPABASE_URL         https://<project-ref>.supabase.co
  SUPABASE_SECRET_KEY  sb_secret_... key (never expose this to the frontend)

Workflow logs are public on a public repository, so nothing here logs secrets,
request headers or article text.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Iterable

import aiohttp

API_BASE_URL = "https://api.freenewsapi.io"

FEED_TOPICS = ("world", "politics")
MAX_LIST_PAGES_PER_FEED = 50
MAX_REQUESTS_PER_CYCLE = 1_100
DAILY_REQUEST_QUOTA = 5_000

# Which articles belong to which anchor country. ISO alpha-2, so the UK is GB.
COUNTRY_TERMS = {
    "US": r"U\.S\.|\bUS\b|\bUSA\b|United States|(?<!South )(?<!Latin )(?<!Central )(?<!North )"
    r"\bAmerica(?:ns?|['’]s)?\b|\bWashington\b|White House|\bTrump\b|Pentagon",
    "RU": r"\bRussian?s?\b|\bMoscow\b|\bKremlin\b|\bPutin\b",
    "CN": r"\bChina\b|\bChinese\b|\bBeijing\b|Xi Jinping",
    "GB": r"\bUK\b|\bBritain\b|\bBritish\b(?! Columbia)|\bEngland\b|\bLondon\b|\bStarmer\b|Downing Street",
    "DE": r"\bGerman(?:y|s)?\b|\bBerlin\b|\bMerz\b",
    "IN": r"\bIndia\b|\bIndians?\b(?! (?:Act|Ocean|Reservation|Country))|New Delhi|\bModi\b",
    "JP": r"\bJapan(?:ese)?\b|\bTokyo\b",
    "UA": r"\bUkrain(?:e|ian|ians)\b|\bKyiv\b|\bZelensk",
    "IL": r"\bIsrael(?:i|is)?\b|\bGaza\b|\bNetanyahu\b|Tel Aviv|\bJerusalem\b",
    "BR": r"\bBrazil(?:ian|ians)?\b|\bLula\b|\bBras[ií]lia\b|S[aã]o Paulo",
}
_COUNTRY_PATTERNS = {code: re.compile(terms) for code, terms in COUNTRY_TERMS.items()}
# A title mention always counts. In the excerpt, one passing mention isn't enough.
MIN_EXCERPT_MENTIONS = 2

# The API allows 2 req/sec, but a live run at 0.55s still drew 429s, about 5% of
# requests. A full cycle takes about 12 minutes at this pace.
REQUEST_INTERVAL_SECONDS = 0.65
REQUEST_TIMEOUT_SECONDS = 30
MAX_ATTEMPTS = 4
MAX_BACKOFF_SECONDS = 60
# A longer wait than this means the quota is gone, so stop instead of sleeping.
MAX_RETRY_AFTER_SECONDS = 300
MAX_CONSECUTIVE_RATE_LIMITS = 12

# Articles in flight at once. Pacing comes from the rate limiter, not from this.
ARTICLE_CONCURRENCY = 6

MAX_EXCERPT_SENTENCES = 10
# Short articles get a proportional excerpt, so it never replaces the original.
MAX_EXCERPT_SHARE = 1 / 3
MAX_EXCERPT_CHARS = 2_500
# Longer runs of text with no sentence ending are page menus, not sentences.
MAX_SENTENCE_CHARS = 500
MIN_SENTENCE_WORDS = 4
# With fewer than this, the card shows only the title, image and link.
MIN_EXCERPT_CHARS = 100

RETENTION_DAYS = 7
SIZE_WARNING_BYTES = 400 * 1024 * 1024  # free plan limit is 500 MB

ARTICLES_PATH = "/rest/v1/articles"
QUOTA_PATH = "/rest/v1/api_quota"
DATABASE_SIZE_PATH = "/rest/v1/rpc/database_size_bytes"
ARTICLES_PER_WRITE = 200
EXISTENCE_CHECK_CHUNK = 100  # keeps the id=in.(...) URL at a safe length

log = logging.getLogger("fetch_news")

# A sentence ends at terminal punctuation plus any closing quote or bracket, then
# whitespace or the end of the text. A line break also ends one.
_SENTENCE_END = re.compile(r"""[.!?…]+["'”’)\]»]*(?:\s+|$)|\n+""")
_COMPLETE_SENTENCE = re.compile(r"""[.!?…]["'”’)\]»]*$""")
_ABBREVIATIONS = frozenset(
    "mr mrs ms dr prof sen rep gov gen col lt sgt capt st jr sr vs no inc ltd co corp "
    "jan feb mar apr jun jul aug sep sept oct nov dec".split()
)
_INITIALISM = re.compile(r"(?:^|[\s(])(?:[A-Za-z]\.)+$")
# Page furniture that the API sometimes returns as part of an article's body.
_PAGE_TEXT = re.compile(
    r"skip to (?:main )?content|all rights reserved|©|couldn.t load|enable javascript|"
    r"opens in a new (?:window|tab)|sign up|subscribe|newsletter|\blog ?in\b|\bcookies?\b|"
    r"advertisement|read more|click here|follow us|share this|terms of (?:use|service)|"
    r"privacy policy|photo:|image (?:source|caption)|getty images|click the|loading\.\.\.",
    re.IGNORECASE,
)


# Timestamps that some sites put in front of the first sentence.
_TIMESTAMP_PREFIX = re.compile(
    r"^(?:\s*(?:Published|Updated|Posted):? [A-Z][a-z]+\.? \d{1,2}, \d{4},? "
    r"\d{1,2}:\d{2} ?[ap]\.?m\.?(?: [A-Z]{2,4})?)+\s*"
)


class StopIngestion(Exception):
    """No more articles can be processed in this run."""


class ApiError(Exception):
    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class StoreError(Exception):
    pass


class StopSignal:
    """Shared flag that ends the run early and records why."""

    def __init__(self) -> None:
        self.reason: str | None = None
        self.fatal = False

    def trip(self, reason: str, fatal: bool = True) -> None:
        if self.reason is None:
            self.reason = reason
            self.fatal = fatal
            log.error("Stopping early: %s", reason)

    def check(self) -> None:
        if self.reason is not None:
            raise StopIngestion(self.reason)


class FailureStreak:
    """Trips the stop signal after too many failures in a row."""

    def __init__(self, threshold: int, reason: str, stop: StopSignal) -> None:
        self.threshold = threshold
        self.reason = reason
        self.stop = stop
        self.count = 0

    def success(self) -> None:
        self.count = 0

    def failure(self) -> None:
        self.count += 1
        if self.count >= self.threshold:
            self.stop.trip(self.reason)


class RateLimiter:
    """Evenly spaced request slots, shared by every worker."""

    def __init__(self, interval: float) -> None:
        self.interval = interval
        self._next_slot = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            delay = self._next_slot - now
            self._next_slot = max(now, self._next_slot) + self.interval
            if delay > 0:
                await asyncio.sleep(delay)

    def pause(self, seconds: float) -> None:
        """Hold back every worker, for example after a 429."""
        self._next_slot = max(self._next_slot, time.monotonic() + seconds)


class NewsClient:
    """Paced API client. Every attempt, retries included, counts against the
    budget, which keeps it a true ceiling on requests sent.
    """

    def __init__(self, session: aiohttp.ClientSession, budget: int, stop: StopSignal) -> None:
        self.session = session
        self.budget = budget
        self.used = 0
        self.stop = stop
        self.limiter = RateLimiter(REQUEST_INTERVAL_SECONDS)
        self.rate_limits = FailureStreak(
            MAX_CONSECUTIVE_RATE_LIMITS, "too many consecutive HTTP 429 responses", stop
        )

    async def get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        for attempt in range(MAX_ATTEMPTS):
            await self.limiter.wait()
            self.stop.check()
            if self.used >= self.budget:
                self.stop.trip(f"request budget of {self.budget} used up", fatal=False)
                self.stop.check()
            self.used += 1

            backoff = _backoff(attempt)
            try:
                async with self.session.get(API_BASE_URL + path, params=params) as response:
                    self._follow_api_quota(response.headers)
                    if response.status == 200:
                        payload = await response.json(content_type=None)
                        if not isinstance(payload, dict):
                            raise ApiError(f"{path}: response is not a JSON object")
                        self.rate_limits.success()
                        return payload
                    if response.status in (401, 403):
                        self.stop.trip(f"API rejected the key (HTTP {response.status})")
                        self.stop.check()
                    if response.status == 429:
                        self._handle_rate_limit(response.headers.get("Retry-After"), backoff)
                        continue
                    if response.status < 500:
                        raise ApiError(f"{path}: HTTP {response.status}", response.status)
                    log.warning("%s: HTTP %s (attempt %d)", path, response.status, attempt + 1)
            except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as exc:
                log.warning("%s: %s (attempt %d)", path, type(exc).__name__, attempt + 1)
            await asyncio.sleep(backoff)
        raise ApiError(f"{path}: gave up after {MAX_ATTEMPTS} attempts")

    def _follow_api_quota(self, headers: Any) -> None:
        """The API's count is the real one: its 24-hour window starts at the first
        request rather than at midnight, and it includes requests made elsewhere.
        """
        remaining = headers.get("X-RateLimit-Remaining-Day")
        if remaining is not None and remaining.isdigit():
            self.budget = min(self.budget, self.used + int(remaining))

    def _handle_rate_limit(self, retry_after: str | None, backoff: float) -> None:
        delay = _parse_retry_after(retry_after)
        if delay is not None and delay > MAX_RETRY_AFTER_SECONDS:
            self.stop.trip(f"API asked to wait {delay:.0f}s, so the quota is probably used up")
            self.stop.check()
        self.rate_limits.failure()
        self.stop.check()
        delay = delay if delay is not None else backoff
        log.warning("HTTP 429, pausing all requests for %.1fs", delay)
        self.limiter.pause(delay)


class SupabaseStore:
    """Talks to Supabase's REST API with a secret key, which bypasses row level
    security. Uses its own session so the news API key never reaches Supabase.
    """

    def __init__(self, session: aiohttp.ClientSession, base_url: str) -> None:
        self.session = session
        self.base_url = base_url

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        body: Any = None,
        prefer: str | None = None,
    ) -> tuple[Any, Any]:
        """Returns (parsed body or None, response headers)."""
        headers = {"Prefer": prefer} if prefer else None
        for attempt in range(MAX_ATTEMPTS):
            try:
                async with self.session.request(
                    method, self.base_url + path, params=params, json=body, headers=headers
                ) as response:
                    if response.status < 300:
                        text = await response.text()
                        return (json.loads(text) if text else None), response.headers
                    if response.status != 429 and response.status < 500:
                        # 402 here means the plan's database size quota was exceeded.
                        raise StoreError(f"{method} {path}: HTTP {response.status}")
                    log.warning(
                        "%s %s: HTTP %s (attempt %d)", method, path, response.status, attempt + 1
                    )
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                log.warning("%s %s: %s (attempt %d)", method, path, type(exc).__name__, attempt + 1)
            await asyncio.sleep(_backoff(attempt))
        raise StoreError(f"{method} {path}: gave up after {MAX_ATTEMPTS} attempts")

    async def existing_ids(self, ids: list[str]) -> set[str]:
        found: set[str] = set()
        for chunk in _chunks(ids, EXISTENCE_CHECK_CHUNK):
            quoted = ",".join(f'"{article_id}"' for article_id in chunk)
            rows, _ = await self._request(
                "GET", ARTICLES_PATH, params={"select": "id", "id": f"in.({quoted})"}
            )
            found.update(row["id"] for row in rows or [])
        return found

    async def write_articles(self, rows: list[dict[str, Any]]) -> None:
        await self._request(
            "POST",
            ARTICLES_PATH,
            params={"on_conflict": "id"},
            body=rows,
            prefer="resolution=merge-duplicates,return=minimal",
        )

    async def purge_older_than(self, cutoff: datetime) -> int:
        _, headers = await self._request(
            "DELETE",
            ARTICLES_PATH,
            params={"ingested_at": f"lt.{cutoff.strftime('%Y-%m-%dT%H:%M:%SZ')}"},
            prefer="return=minimal,count=exact",
        )
        # The header looks like "*/123".
        total = (headers.get("Content-Range") or "").rpartition("/")[2]
        return int(total) if total.isdigit() else 0

    async def requests_used_today(self, day: str) -> int:
        rows, _ = await self._request(
            "GET", QUOTA_PATH, params={"select": "day,used", "id": "eq.1"}
        )
        return int(rows[0]["used"]) if rows and rows[0]["day"] == day else 0

    async def save_requests_used(self, day: str, used: int) -> None:
        await self._request(
            "POST",
            QUOTA_PATH,
            params={"on_conflict": "id"},
            body={"id": 1, "day": day, "used": used},
            prefer="resolution=merge-duplicates,return=minimal",
        )

    async def database_size_bytes(self) -> int:
        size, _ = await self._request("POST", DATABASE_SIZE_PATH, body={})
        return int(size)


@dataclass
class Stats:
    listed: int = 0
    already_stored: int = 0
    too_old: int = 0
    fetched: int = 0
    not_english: int = 0
    without_excerpt: int = 0
    untagged: int = 0
    written: int = 0
    failed: int = 0
    write_failures: int = 0
    purged: int = 0
    purge_failed: bool = False


def split_sentences(text: str) -> list[str]:
    sentences: list[str] = []
    start = 0
    for match in _SENTENCE_END.finditer(text):
        end = match.end()
        candidate = text[start:end].strip()
        if not candidate:
            start = end
            continue
        if "\n" not in match.group() and end < len(text) and _is_false_sentence_end(candidate, text[end]):
            continue
        sentences.append(" ".join(candidate.split()))
        start = end
    remainder = text[start:].strip()
    if remainder:
        sentences.append(" ".join(remainder.split()))
    return sentences


def _is_false_sentence_end(candidate: str, next_char: str) -> bool:
    if next_char.islower():
        return True
    if not candidate.endswith("."):
        return False
    last_word = candidate.rsplit(None, 1)[-1].rstrip(".").lstrip("(\"'“‘").lower()
    return last_word in _ABBREVIATIONS or bool(_INITIALISM.search(candidate))


def is_story_sentence(sentence: str) -> bool:
    """False for page furniture: menus, legal lines, error notices, unfinished text."""
    words = sentence.split()
    if len(words) < MIN_SENTENCE_WORDS or len(sentence) > MAX_SENTENCE_CHARS:
        return False
    if not _COMPLETE_SENTENCE.search(sentence) or _PAGE_TEXT.search(sentence):
        return False
    # Menus read as a run of capitalised labels.
    capitalised = sum(word[0].isupper() for word in words)
    return not (len(words) >= 8 and capitalised / len(words) > 0.7)


def make_excerpt(body: str) -> str:
    """Whole sentences only. The limit is MAX_EXCERPT_SENTENCES, or a share of the
    article's real sentences if that is fewer. Empty if no real story text remains.
    """
    body = _TIMESTAMP_PREFIX.sub("", body)
    sentences = [s for s in split_sentences(body) if is_story_sentence(s)]
    limit = min(MAX_EXCERPT_SENTENCES, math.ceil(len(sentences) * MAX_EXCERPT_SHARE))

    picked: list[str] = []
    length = 0
    for sentence in sentences[:limit]:
        if length + len(sentence) + 1 > MAX_EXCERPT_CHARS:
            break
        picked.append(sentence)
        length += len(sentence) + 1

    excerpt = " ".join(picked)
    return excerpt if len(excerpt) >= MIN_EXCERPT_CHARS else ""


def tag_countries(title: str, excerpt: str) -> list[str]:
    return [
        code
        for code, pattern in _COUNTRY_PATTERNS.items()
        if pattern.search(title) or len(pattern.findall(excerpt)) >= MIN_EXCERPT_MENTIONS
    ]


def parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def http_url(value: Any) -> str | None:
    """The frontend renders these as links and images, so allow only web URLs."""
    if isinstance(value, str) and value.lower().startswith(("https://", "http://")):
        return value
    return None


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        return max(0.0, (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds())
    except (TypeError, ValueError):
        return None


def _backoff(attempt: int) -> float:
    return min(MAX_BACKOFF_SECONDS, 2**attempt) + random.uniform(0, 0.5)


def _chunks(items: list[Any], size: int) -> Iterable[list[Any]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


class Paginator:
    """The docs never name the paging request parameter. Both `cursor` and `offset`
    worked when tested against the live API. Starts with `cursor` and switches to
    `offset` for the rest of the run if the API ever rejects or ignores it.
    """

    def __init__(self) -> None:
        self.use_cursor = True

    def next_params(self, meta: dict[str, Any]) -> dict[str, Any]:
        if self.use_cursor and meta.get("next_cursor"):
            return {"cursor": meta["next_cursor"]}
        if meta.get("next_offset") is not None:
            return {"offset": meta["next_offset"]}
        return {}

    def fall_back(self) -> None:
        log.warning("The `cursor` parameter did not advance the list, switching to `offset`")
        self.use_cursor = False


async def read_feed(
    client: NewsClient,
    store: SupabaseStore,
    topic: str,
    paginator: Paginator,
    cutoff: datetime,
    stats: Stats,
) -> dict[str, dict[str, Any]]:
    """Returns new articles. Feeds run newest first, so reading stops at the first
    page with nothing new, which keeps a normal cycle well under its budget.
    """
    seen: set[str] = set()
    pending: dict[str, dict[str, Any]] = {}
    params: dict[str, Any] = {}
    previous_meta: dict[str, Any] = {}

    for _ in range(MAX_LIST_PAGES_PER_FEED):
        try:
            payload = await client.get("/v1/news", {"language": "en", "topic": topic, **params})
        except ApiError as exc:
            if "cursor" in params and exc.status in (400, 422):
                paginator.fall_back()
                params = paginator.next_params(previous_meta)
                if params:
                    continue
            log.error("%s: listing stopped after %d articles: %s", topic, len(seen), exc)
            break

        items = [item for item in payload.get("data") or [] if item.get("uuid")]
        new_items = [item for item in items if item["uuid"] not in seen]

        if "cursor" in params and not new_items:
            paginator.fall_back()
            params = paginator.next_params(previous_meta)
            if params:
                continue
            break
        if params and not new_items:
            log.error("%s: paging returned no new articles, stopping", topic)
            break

        seen.update(item["uuid"] for item in new_items)
        stats.listed += len(new_items)

        fresh = []
        for item in new_items:
            published = parse_timestamp(item.get("published_at"))
            if published is not None and published < cutoff:
                # Would be purged, then fetched again, on every later cycle.
                stats.too_old += 1
            else:
                fresh.append(item)

        stored = await store.existing_ids([item["uuid"] for item in fresh])
        stats.already_stored += len(stored)
        to_fetch = [item for item in fresh if item["uuid"] not in stored]
        pending.update((item["uuid"], item) for item in to_fetch)
        if not to_fetch:
            break

        previous_meta = payload.get("meta") or {}
        if not previous_meta.get("has_more"):
            break
        params = paginator.next_params(previous_meta)
        if not params:
            break

    log.info("%s: %d articles read, %d new", topic, len(seen), len(pending))
    return pending


async def discover(
    client: NewsClient, store: SupabaseStore, stats: Stats, cutoff: datetime
) -> dict[str, dict[str, Any]]:
    paginator = Paginator()
    pending: dict[str, dict[str, Any]] = {}
    for topic in FEED_TOPICS:
        try:
            # An article in both feeds is only fetched once.
            pending.update(await read_feed(client, store, topic, paginator, cutoff, stats))
        except StopIngestion:
            break
    return pending


async def build_row(
    client: NewsClient, uuid: str, item: dict[str, Any], now: datetime, stats: Stats
) -> dict[str, Any] | None:
    payload = await client.get("/v1/details", {"uuid": uuid})
    detail = payload.get("data") or {}
    stats.fetched += 1

    languages = detail.get("languages") or []
    if languages and not languages[0].lower().startswith("en"):
        stats.not_english += 1
        return None

    title = detail.get("title") or item.get("title") or ""
    excerpt = make_excerpt(detail.get("body") or "")
    countries = tag_countries(title, excerpt)
    stats.without_excerpt += not excerpt
    stats.untagged += not countries

    published = (
        parse_timestamp(detail.get("published_at"))
        or parse_timestamp(item.get("published_at"))
        or now
    )
    return {
        "id": uuid,
        "title": title,
        "source": detail.get("publisher") or item.get("publisher") or "",
        "published_at": published.isoformat(),
        "country_codes": countries,
        "excerpt": excerpt,
        "thumbnail_url": http_url(detail.get("thumbnail")),
        "topics": detail.get("topics") or [],
        "original_url": http_url(detail.get("original_url")),
        "ingested_at": now.isoformat(),
    }


async def flush(store: SupabaseStore, buffer: list[dict[str, Any]], stats: Stats) -> None:
    if not buffer:
        return
    try:
        await store.write_articles(buffer)
        stats.written += len(buffer)
    except StoreError as exc:
        stats.write_failures += len(buffer)
        log.error("Could not write %d articles: %s", len(buffer), exc)
    buffer.clear()


async def ingest(
    client: NewsClient, store: SupabaseStore, stats: Stats, now: datetime, cutoff: datetime
) -> None:
    pending = await discover(client, store, stats, cutoff)
    log.info("%d listed, %d already stored, %d to fetch", stats.listed, stats.already_stored, len(pending))

    semaphore = asyncio.Semaphore(ARTICLE_CONCURRENCY)

    async def process(uuid: str, item: dict[str, Any]) -> dict[str, Any] | None:
        async with semaphore:
            try:
                client.stop.check()
                return await build_row(client, uuid, item, now, stats)
            except StopIngestion:
                return None
            except ApiError as exc:
                stats.failed += 1
                log.warning("Skipping article %s: %s", uuid, exc)
                return None

    tasks = [asyncio.create_task(process(uuid, item)) for uuid, item in pending.items()]
    buffer: list[dict[str, Any]] = []
    for finished in asyncio.as_completed(tasks):
        row = await finished
        if row is not None:
            buffer.append(row)
        if len(buffer) >= ARTICLES_PER_WRITE:
            await flush(store, buffer, stats)
    await flush(store, buffer, stats)


async def warn_if_database_is_large(store: SupabaseStore) -> None:
    try:
        size = await store.database_size_bytes()
    except (StoreError, TypeError, ValueError) as exc:
        log.warning("Could not read the database size: %s", exc)
        return
    size_mb = size / (1024 * 1024)
    log.info("Database size: %.0f MB", size_mb)
    if size >= SIZE_WARNING_BYTES:
        # Shown as an annotation on the workflow run.
        print(f"::warning::Supabase database is {size_mb:.0f} MB, the free plan limit is 500 MB")


async def run_cycle(store: SupabaseStore, api_key: str) -> int:
    now = datetime.now(timezone.utc)
    # Our own running total, which caps re-runs. NewsClient also follows the API's count.
    day = now.strftime("%Y-%m-%d")
    cutoff = now - timedelta(days=RETENTION_DAYS)
    stats = Stats()

    try:
        stats.purged = await store.purge_older_than(cutoff)
        log.info("Purged %d expired articles", stats.purged)
    except StoreError as exc:
        # New articles can still be stored, but this must not go unnoticed.
        stats.purge_failed = True
        log.error("Purge failed: %s", exc)

    used_today = await store.requests_used_today(day)
    budget = min(MAX_REQUESTS_PER_CYCLE, DAILY_REQUEST_QUOTA - used_today)
    if budget <= 0:
        log.warning("Daily quota already used (%d requests), skipping this cycle", used_today)
        return 1 if stats.purge_failed else 0

    # Reserve the whole budget first, so a killed run errs towards too few requests.
    await store.save_requests_used(day, used_today + budget)

    stop = StopSignal()
    headers = {"x-api-key": api_key, "Accept": "application/json"}
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
    client: NewsClient | None = None
    try:
        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
            client = NewsClient(session, budget, stop)
            await ingest(client, store, stats, now, cutoff)
    finally:
        spent = client.used if client else 0
        await store.save_requests_used(day, used_today + spent)
        log.info("API requests: %d of %d budgeted, %d today", spent, budget, used_today + spent)
        log.info("Summary: %s", stats)

    await warn_if_database_is_large(store)

    if stop.fatal or stats.write_failures or stats.purge_failed:
        return 1
    if stats.listed == 0:
        log.error("The feeds returned no articles")
        return 1
    return 0


async def run() -> int:
    names = ("FREENEWS_API_KEY", "SUPABASE_URL", "SUPABASE_SECRET_KEY")
    missing = [name for name in names if not os.environ.get(name)]
    if missing:
        log.error("Missing environment variables: %s", ", ".join(missing))
        return 2

    # Secret keys go in the apikey header. They are not JWTs, so no Authorization header.
    headers = {"apikey": os.environ["SUPABASE_SECRET_KEY"], "Accept": "application/json"}
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        store = SupabaseStore(session, os.environ["SUPABASE_URL"].rstrip("/"))
        try:
            return await run_cycle(store, os.environ["FREENEWS_API_KEY"])
        except StoreError as exc:
            log.error("Supabase is unavailable: %s", exc)
            return 1


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sys.exit(asyncio.run(run()))


if __name__ == "__main__":
    main()
