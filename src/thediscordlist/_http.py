"""The one HTTP client every publisher shares.

There is a single client-side suspension rather than one per endpoint, because
the server keeps a single rate-limit counter per client IP across every bot
route -- only the threshold differs. Heartbeats bypass it: a dropped stats post
is a stale number, a dropped heartbeat is a false "down" on the user's public
status page.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

import aiohttp

log = logging.getLogger("thediscordlist")

DEFAULT_BASE_URL = "https://thediscordlist.com/api"
TIMEOUT_SECONDS = 10.0
MAX_ATTEMPTS = 3

OutcomeKind = Literal[
    "ok",  # 2xx
    "payload",  # 400/422 -- our body is wrong; the next tick may carry valid data
    "auth",  # 401 -- fatal, disable everything
    "plan",  # 403 -- Gold gate; reprobe, never disable
    "config",  # 404/409 -- fatal for this publisher
    "throttled",  # 429
    "transient",  # 408, 5xx, connection errors
    "dropped",  # never sent: client-side suspension
]


@dataclass(frozen=True, slots=True)
class Outcome:
    kind: OutcomeKind
    status: int | None = None
    body: Any = None
    retry_after: float = 0.0
    error: BaseException | None = None


def backoff(attempt: int, rand: Callable[[], float] = random.random) -> float:
    """Exponential with full jitter in the upper half, capped at 30s."""
    return min(2.0**attempt, 30.0) * (0.5 + rand() * 0.5)


def _classify(status: int, body: Any, retry_after: str | None) -> Outcome:
    """Server-side precedence is 404 -> 429 -> 401 -> 422 -> 403/409 -> 200.

    Gates fire *after* validation, so plan state is only inferable from a clean
    payload: a malformed body on a free-tier bot returns 422, not 403.
    """
    if 200 <= status < 300:
        return Outcome("ok", status, body)
    if status == 401:
        return Outcome("auth", 401)
    if status == 403:
        return Outcome("plan", 403)
    # 404 is "unknown id, or the id belongs to a server listing" -- deliberately
    # never "bad token", so probing cannot enumerate. Do not report it as auth.
    if status in (404, 409):
        return Outcome("config", status)
    if status in (400, 422):
        return Outcome("payload", status, body)
    if status == 429:
        return Outcome("throttled", 429, retry_after=_seconds(retry_after) or 60.0)
    return Outcome("transient", status, error=RuntimeError(f"TheDiscordList responded {status}."))


def _seconds(value: str | None) -> float:
    try:
        parsed = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0
    return parsed if parsed > 0 else 0.0


class ApiClient:
    """Posts JSON to TheDiscordList. Never raises; every failure is an `Outcome`."""

    def __init__(self, token: str, base_url: str, user_agent: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._headers = {"authorization": f"Bearer {token}", "user-agent": user_agent}
        self._session: aiohttp.ClientSession | None = None
        # Instance state, never module state: two Autoposters in one process must
        # not share a budget.
        self._suspended_until = 0.0

    async def post(
        self,
        path: str,
        body: Any,
        *,
        heartbeat: bool = False,
        next_tick: float | None = None,
    ) -> Outcome:
        """One request, retrying only transient failures.

        `next_tick` is the time until this publisher runs again. If a backoff
        would outlast it the attempt is dropped: the next tick carries fresher
        data, and a failed request still burns rate-limit budget.
        """
        outcome = Outcome("dropped")
        for attempt in range(MAX_ATTEMPTS):
            outcome = await self._once(path, body, heartbeat)
            if outcome.kind != "transient" or attempt == MAX_ATTEMPTS - 1:
                break
            delay = backoff(attempt)
            if next_tick is not None and delay >= next_tick:
                break
            await asyncio.sleep(delay)
        return outcome

    async def _once(self, path: str, body: Any, heartbeat: bool) -> Outcome:
        if not heartbeat and time.monotonic() < self._suspended_until:
            return Outcome("dropped")

        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS),
                headers=self._headers,
            )

        try:
            async with self._session.post(f"{self._base_url}{path}", json=body) as response:
                # Always read the body inside the context manager, or the
                # connection is returned to the pool unread.
                text = await response.text()
                status, headers = response.status, response.headers
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as error:
            return Outcome("transient", error=error)

        try:
            parsed: Any = json.loads(text) if text else None
        except ValueError:
            parsed = text

        outcome = _classify(status, parsed, headers.get("retry-after"))
        if outcome.kind == "throttled":
            reset = _seconds(headers.get("x-ratelimit-reset"))
            until = max(outcome.retry_after, reset - time.time() if reset else 0.0)
            self._suspended_until = time.monotonic() + until + random.uniform(0.0, 2.0)
        return outcome

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None
