#!/usr/bin/env python3
"""Fetch, translate and store one news cycle from FreeNewsApi.io into Supabase.

One run:
  1. Purges articles ingested more than RETENTION_DAYS ago.
  2. Reserves its API budget against a daily counter kept in Supabase.
  3. Lists up to 100 articles per anchor country (at most 100 list requests).
  4. Skips articles already stored, then fetches the rest (at most 1,000 detail
     requests), translates the title and opening text, and stores a card with an
     excerpt of whole sentences. The full article text is never stored: the
     provider's terms restrict republishing it, so readers continue at the source.
  5. Warns if the database is getting close to the free plan's limit.

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
from deep_translator import GoogleTranslator

API_BASE_URL = "https://api.freenewsapi.io"

# ISO alpha-2, as the API reports countries. The UK is "GB" there.
ANCHOR_COUNTRIES = ("US", "RU", "CN", "GB", "DE", "IN", "JP", "UA", "IL", "BR")

ARTICLES_PER_COUNTRY = 100
MAX_LIST_PAGES_PER_COUNTRY = 10
MAX_REQUESTS_PER_CYCLE = 1_100
DAILY_REQUEST_QUOTA = 5_000

# The API allows 2 req/sec. Stay just under it; a full cycle takes about 10 minutes.
REQUEST_INTERVAL_SECONDS = 0.55
REQUEST_TIMEOUT_SECONDS = 30
MAX_ATTEMPTS = 4
MAX_BACKOFF_SECONDS = 60
# A longer wait than this means the quota is gone, so stop instead of sleeping.
MAX_RETRY_AFTER_SECONDS = 300
MAX_CONSECUTIVE_RATE_LIMITS = 12

# Articles in flight at once. Pacing comes from the rate limiter, not from this.
ARTICLE_CONCURRENCY = 6

TRANSLATION_CONCURRENCY = 3
TRANSLATION_TIMEOUT_SECONDS = 60
TRANSLATION_ATTEMPTS = 3
MAX_CONSECUTIVE_TRANSLATION_FAILURES = 8

MAX_EXCERPT_SENTENCES = 10
# Short articles get a proportional excerpt, so it never replaces the original.
MAX_EXCERPT_SHARE = 1 / 3
# Only articles with very long or unpunctuated sentences ever reach this.
MAX_EXCERPT_CHARS = 2_500
# Enough source text for ten sentences, and under deep_translator's 5,000 limit.
EXCERPT_SOURCE_CHARS = 4_500
MAX_TITLE_CHARS = 500

RETENTION_DAYS = 7
SIZE_WARNING_BYTES = 400 * 1024 * 1024  # free plan limit is 500 MB

ARTICLES_PATH = "/rest/v1/articles"
QUOTA_PATH = "/rest/v1/api_quota"
DATABASE_SIZE_PATH = "/rest/v1/rpc/database_size_bytes"
ARTICLES_PER_WRITE = 200
EXISTENCE_CHECK_CHUNK = 100  # keeps the id=in.(...) URL at a safe length

log = logging.getLogger("fetch_news")

# A sentence ends at terminal punctuation plus any closing quote or bracket, then
# whitespace or the end of the text. Chinese and Japanese punctuation needs no
# space after it. A line break also ends one, which covers unpunctuated subheadings.
_SENTENCE_END = re.compile(
    r"""[.!?…]+["'”’)\]»]*(?:\s+|$)|[。！？]+["'”’）」』]*\s*|\n+"""
)
_ABBREVIATIONS = frozenset(
    "mr mrs ms dr prof sen rep gov gen col lt sgt capt st jr sr vs no inc ltd co corp "
    "jan feb mar apr jun jul aug sep sept oct nov dec".split()
)
_INITIALISM = re.compile(r"(?:^|[\s(])(?:[A-Za-z]\.)+$")


class StopIngestion(Exception):
    """No more articles can be processed in this run."""


class ApiError(Exception):
    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class StoreError(Exception):
    pass


class TranslationFailed(Exception):
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
    budget because the API does not document whether failed requests use quota.
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


class Translator:
    """deep_translator is blocking, so calls run in worker threads."""

    def __init__(self, stop: StopSignal) -> None:
        self.semaphore = asyncio.Semaphore(TRANSLATION_CONCURRENCY)
        self.failures = FailureStreak(
            MAX_CONSECUTIVE_TRANSLATION_FAILURES,
            "translation keeps failing, stopping before more API quota is spent",
            stop,
        )

    async def to_english(self, text: str) -> str:
        if not text.strip():
            return text
        for attempt in range(TRANSLATION_ATTEMPTS):
            try:
                async with self.semaphore:
                    translated = await asyncio.wait_for(
                        asyncio.to_thread(_google_translate, text),
                        TRANSLATION_TIMEOUT_SECONDS,
                    )
                if translated:
                    return translated
                log.warning("Translator returned nothing (attempt %d)", attempt + 1)
            except Exception as exc:  # deep_translator has no common error base class
                log.warning("Translation failed: %s (attempt %d)", type(exc).__name__, attempt + 1)
            await asyncio.sleep(_backoff(attempt))
        raise TranslationFailed("translation failed")


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
    translated: int = 0
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


def make_excerpt(english: str, total_sentences: int, source_was_cut: bool) -> str:
    """Whole sentences only. The limit is MAX_EXCERPT_SENTENCES, or a share of the
    original article (counted before translation) if that is fewer.
    """
    sentences = split_sentences(english)
    if source_was_cut and len(sentences) > 1:
        sentences.pop()  # the source was cut, so its last sentence may be partial
    limit = min(MAX_EXCERPT_SENTENCES, max(1, math.ceil(total_sentences * MAX_EXCERPT_SHARE)))

    picked: list[str] = []
    length = 0
    for sentence in sentences[:limit]:
        if picked and length + 1 + len(sentence) > MAX_EXCERPT_CHARS:
            break
        picked.append(sentence)
        length += len(sentence) + 1

    excerpt = " ".join(picked)
    if len(excerpt) > MAX_EXCERPT_CHARS:  # a single sentence over the limit
        excerpt = excerpt[:MAX_EXCERPT_CHARS].rsplit(" ", 1)[0] + "…"
    return excerpt


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


def _google_translate(text: str) -> str | None:
    return GoogleTranslator(source="auto", target="en").translate(text)


class Paginator:
    """The docs show next_cursor and next_offset in responses but never name the
    request parameter. Starts with `cursor` and switches to `offset` for the rest
    of the run if the API rejects or ignores it.
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


async def list_country(
    client: NewsClient, country: str, paginator: Paginator
) -> dict[str, dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}
    params: dict[str, Any] = {}
    previous_meta: dict[str, Any] = {}

    for _ in range(MAX_LIST_PAGES_PER_COUNTRY):
        if len(found) >= ARTICLES_PER_COUNTRY:
            break
        try:
            payload = await client.get("/v1/news", {"country": country.lower(), **params})
        except ApiError as exc:
            if "cursor" in params and exc.status in (400, 422):
                paginator.fall_back()
                params = paginator.next_params(previous_meta)
                if params:
                    continue
            log.error("%s: listing stopped after %d articles: %s", country, len(found), exc)
            break

        items = [item for item in payload.get("data") or [] if item.get("uuid")]
        new_items = [item for item in items if item["uuid"] not in found]

        if "cursor" in params and not new_items:
            paginator.fall_back()
            params = paginator.next_params(previous_meta)
            if params:
                continue
            break
        if params and not new_items:
            log.error("%s: paging returned no new articles, stopping", country)
            break

        for item in new_items[: ARTICLES_PER_COUNTRY - len(found)]:
            found[item["uuid"]] = item

        previous_meta = payload.get("meta") or {}
        if not previous_meta.get("has_more"):
            break
        params = paginator.next_params(previous_meta)
        if not params:
            break

    if not found:
        log.warning("%s: no articles listed", country)
    return found


async def discover(client: NewsClient, stats: Stats, cutoff: datetime) -> dict[str, tuple[str, dict]]:
    """Returns uuid -> (country, list item). Countries share the rate limiter, so
    running them one at a time is no slower and keeps the paging fallback simple.
    """
    paginator = Paginator()
    candidates: dict[str, tuple[str, dict]] = {}
    for country in ANCHOR_COUNTRIES:
        try:
            found = await list_country(client, country, paginator)
        except StopIngestion:
            break
        for uuid, item in found.items():
            published = parse_timestamp(item.get("published_at"))
            if published is not None and published < cutoff:
                # Would be purged, then fetched again, on every later cycle.
                stats.too_old += 1
            elif uuid not in candidates:
                candidates[uuid] = (country, item)
        log.info("%s: %d articles listed", country, len(found))
    stats.listed = len(candidates)
    return candidates


async def build_row(
    client: NewsClient,
    translator: Translator,
    uuid: str,
    country: str,
    item: dict[str, Any],
    now: datetime,
    stats: Stats,
) -> dict[str, Any]:
    payload = await client.get("/v1/details", {"uuid": uuid})
    detail = payload.get("data") or {}
    stats.fetched += 1

    title = (detail.get("title") or item.get("title") or "")[:MAX_TITLE_CHARS]
    body = detail.get("body") or ""
    total_sentences = len(split_sentences(body))
    source = body[:EXCERPT_SOURCE_CHARS]

    languages = detail.get("languages") or []
    language = languages[0].lower() if languages else None
    is_english = bool(language and language.startswith("en"))

    if not is_english:
        try:
            title = await translator.to_english(title)
            source = await translator.to_english(source)
        except TranslationFailed:
            translator.failures.failure()
            translator.failures.stop.check()
            raise
        translator.failures.success()
        stats.translated += 1

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
        "country_code": country,
        "excerpt": make_excerpt(source, total_sentences, len(body) > EXCERPT_SOURCE_CHARS),
        "thumbnail_url": http_url(detail.get("thumbnail")),
        "topics": detail.get("topics") or [],
        "original_url": http_url(detail.get("original_url")),
        "original_language": language,
        "is_translated": not is_english,
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
    client: NewsClient,
    translator: Translator,
    store: SupabaseStore,
    stats: Stats,
    now: datetime,
    cutoff: datetime,
) -> None:
    candidates = await discover(client, stats, cutoff)
    existing = await store.existing_ids(list(candidates))
    stats.already_stored = len(existing)
    pending = {uuid: value for uuid, value in candidates.items() if uuid not in existing}
    log.info("%d listed, %d already stored, %d to fetch", len(candidates), len(existing), len(pending))

    semaphore = asyncio.Semaphore(ARTICLE_CONCURRENCY)

    async def process(uuid: str, country: str, item: dict[str, Any]) -> dict[str, Any] | None:
        async with semaphore:
            try:
                client.stop.check()
                return await build_row(client, translator, uuid, country, item, now, stats)
            except StopIngestion:
                return None
            except (ApiError, TranslationFailed) as exc:
                stats.failed += 1
                log.warning("Skipping article %s: %s", uuid, exc)
                return None

    tasks = [
        asyncio.create_task(process(uuid, country, item))
        for uuid, (country, item) in pending.items()
    ]
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
    day = now.strftime("%Y-%m-%d")  # assumes the API's daily quota resets at UTC midnight
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
            await ingest(client, Translator(stop), store, stats, now, cutoff)
    finally:
        spent = client.used if client else 0
        await store.save_requests_used(day, used_today + spent)
        log.info("API requests: %d of %d budgeted, %d today", spent, budget, used_today + spent)
        log.info("Summary: %s", stats)

    await warn_if_database_is_large(store)

    if stop.fatal or stats.write_failures or stats.purge_failed:
        return 1
    if stats.listed == 0 or (stats.fetched == 0 and stats.listed > stats.already_stored):
        log.error("Nothing could be ingested in this cycle")
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
    code = asyncio.run(run())
    # deep_translator sets no HTTP timeout, and a hung worker thread would block a
    # normal exit until the job limit marks a finished run as failed. All data is
    # saved by this point.
    logging.shutdown()
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)


if __name__ == "__main__":
    main()
