"""The three publisher loops: stats, heartbeat and commands.

Every loop swallows its own exceptions and every ``post_*()`` returns a
``PostResult`` instead of raising, because a transient 502 must never take down
someone's bot. Cadence is a chained ``sleep``, never a fixed-rate ticker:
interval drift causes self-inflicted 429s.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from ._http import ApiClient, Outcome
from ._topology import ServerCountFn, Topology, collect_shards, measure_server_count

log = logging.getLogger("thediscordlist")

STATS_INTERVAL = 300.0
#: A guild join or removal posts once the changes have been quiet this long.
STATS_DEBOUNCE = 20.0
#: The hard floor between two stats posts, so the ceiling is structurally one
#: request per minute rather than statistically.
STATS_MIN_GAP = 60.0
#: Republish an unchanged count this often, so a listing never looks abandoned.
STATS_FORCE_AFTER = 1800.0
STATS_FIRST_DELAY = 5.0

HEARTBEAT_DEFAULT_INTERVAL = 30.0
HEARTBEAT_MIN_INTERVAL = 15.0
HEARTBEAT_MAX_INTERVAL = 60.0
HEARTBEAT_JITTER = 5.0

#: Offset from the first stats post: the two share one server-side rate budget.
COMMANDS_DELAY = 10.0

#: The Gold tier is derived per request from Stripe, so it can flip mid-run.
PLAN_REPROBE = 1800.0

PublisherKind = Literal["stats", "heartbeat", "commands"]


@dataclass(frozen=True, slots=True)
class PostResult:
    """What one publish attempt did. Falsy ``ok`` with a ``skipped`` reason means
    nothing was sent, which is usually correct rather than a failure."""

    ok: bool
    status: int | None = None
    skipped: str | None = None


@dataclass(slots=True)
class Deps:
    api: ApiClient
    client: Any
    topology: Topology
    #: The Discord *application* id, as a string.
    bot_id: str
    on_auth_failure: Callable[[], None] | None = None


class Publisher:
    """Shared plumbing: one task, one lock, one error taxonomy."""

    kind: PublisherKind

    def __init__(self, deps: Deps) -> None:
        self._d = deps
        self._lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._disabled: str | None = None
        self._suspended_until = 0.0
        self._reported: set[int] = set()

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._guarded(), name=f"tdl-{self.kind}")

    def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None

    # -- loop ---------------------------------------------------------------

    async def _prime(self) -> None:
        """Runs once before the first tick."""

    async def _tick(self) -> float:
        """Does the work and returns the seconds to wait before the next tick."""
        raise NotImplementedError

    async def _wait(self, delay: float) -> None:
        await asyncio.sleep(delay)

    async def _guarded(self) -> None:
        try:
            await self._prime()
            while self._disabled is None:
                try:
                    delay = await self._tick()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("[thediscordlist] the %s publisher failed", self.kind)
                    delay = 60.0
                await self._wait(delay)
        except asyncio.CancelledError:
            pass

    # -- sending ------------------------------------------------------------

    async def _send(
        self,
        path: str,
        body: Any,
        *,
        heartbeat: bool = False,
        next_tick: float | None = None,
    ) -> PostResult:
        outcome = await self._d.api.post(path, body, heartbeat=heartbeat, next_tick=next_tick)
        return self._react(outcome)

    def _react(self, outcome: Outcome) -> PostResult:
        if outcome.kind == "ok":
            log.debug("[thediscordlist] %s posted (%s)", self.kind, outcome.status)
            return PostResult(True, outcome.status)

        if outcome.kind == "dropped":
            return PostResult(False, None, "suspended")

        if outcome.kind == "payload":
            # Stays enabled: the next tick may carry valid data.
            self._report_once(
                outcome.status or 0,
                f"TheDiscordList rejected the {self.kind} payload "
                f"({outcome.status}): {outcome.body!r}",
            )
            return PostResult(False, outcome.status)

        if outcome.kind == "auth":
            self._disable(401, "unauthorized", "the stats token was rejected")
            if self._d.on_auth_failure is not None:
                self._d.on_auth_failure()
            return PostResult(False, 401)

        if outcome.kind == "plan":
            # Not a disable: a bot can flip free to Gold mid-run, and requiring a
            # restart to notice would be a support ticket every time.
            self._suspended_until = time.monotonic() + PLAN_REPROBE
            self._report_once(
                403,
                f"{self.kind} needs a higher plan on TheDiscordList; reprobing in 30 minutes",
            )
            return PostResult(False, 403, "plan")

        if outcome.kind == "config":
            # 404 is "unknown id, or the id belongs to a server listing" -- never
            # "bad token". Do not tell the user their token is wrong.
            conflict = outcome.status == 409
            self._disable(
                outcome.status,
                "conflict" if conflict else "not-found",
                "this bot is not configured to use application commands"
                if conflict
                else f"no bot listing found for application id {self._d.bot_id}",
            )
            return PostResult(False, outcome.status)

        if outcome.kind == "throttled":
            return PostResult(False, 429, "throttled")

        log.warning("[thediscordlist] %s post failed (transient): %s", self.kind, outcome.error)
        return PostResult(False, outcome.status)

    def _report_once(self, status: int, message: str) -> None:
        if status in self._reported:
            return
        self._reported.add(status)
        log.warning("[thediscordlist] %s", message)

    def _disable(self, status: int | None, reason: str, detail: str) -> None:
        if self._disabled is not None:
            return
        self._disabled = reason
        log.error("[thediscordlist] %s publishing disabled (%s): %s", self.kind, status, detail)

    def _rearm(self) -> None:
        """An explicit ``post_*()`` re-arms a disabled publisher; a restart is the other way."""
        self._disabled = None


class StatsPublisher(Publisher):
    """Posts the guild count when it moves: on a timer, and shortly after a guild change.

    The unchanged-value skip matters more than it looks -- every ``/stats``
    post re-indexes the listing, evaluates achievements and fires a
    ``bot.stats_reported`` webhook even when the number did not move, and there
    is no server-side dedupe.
    """

    kind: PublisherKind = "stats"

    def __init__(
        self,
        deps: Deps,
        interval: float = STATS_INTERVAL,
        server_count: ServerCountFn | None = None,
    ) -> None:
        super().__init__(deps)
        if interval < STATS_MIN_GAP:
            log.warning(
                "[thediscordlist] stats_interval=%.0fs is below the %.0fs rate-limit floor; "
                "using %.0fs",
                interval,
                STATS_MIN_GAP,
                STATS_MIN_GAP,
            )
        self._interval = max(interval, STATS_MIN_GAP)
        self._server_count = server_count
        self._last_post_at = 0.0
        self._last: tuple[int, int] | None = None
        self._changed = asyncio.Event()

    async def _prime(self) -> None:
        await self._d.client.wait_until_ready()
        await asyncio.sleep(STATS_FIRST_DELAY)

    async def _tick(self) -> float:
        result = await self._publish(manual=False)
        now = time.monotonic()
        if result.skipped == "plan":
            return self._suspended_until - now
        if result.skipped == "min-gap":
            return self._last_post_at + STATS_MIN_GAP - now
        return self._interval

    def on_guild_change(self) -> None:
        self._changed.set()

    async def _wait(self, delay: float) -> None:
        """A trailing debounce on guild changes, capped so a join storm cannot starve the post."""
        if not await self._changed_within(delay):
            return
        cap = time.monotonic() + STATS_MIN_GAP
        while (left := cap - time.monotonic()) > 0 and await self._changed_within(
            min(STATS_DEBOUNCE, left)
        ):
            pass

    async def _changed_within(self, seconds: float) -> bool:
        try:
            await asyncio.wait_for(self._changed.wait(), seconds)
        except asyncio.TimeoutError:
            return False
        self._changed.clear()
        return True

    async def post(self) -> PostResult:
        self._rearm()
        return await self._publish(manual=True)

    async def _publish(self, *, manual: bool) -> PostResult:
        async with self._lock:
            if not manual and self._disabled is not None:
                return PostResult(False, None, "disabled")

            now = time.monotonic()
            if now < self._suspended_until:
                return PostResult(False, None, "plan")
            # A hard monotonic floor. A join storm, a mass reconnect and a manual
            # post_stats() all route through it.
            if self._last_post_at and now - self._last_post_at < STATS_MIN_GAP:
                return PostResult(False, None, "min-gap")

            count = await measure_server_count(self._d.client, self._d.topology, self._server_count)
            if count is None:
                return PostResult(False, None, "not-elected")

            shard_count = self._d.topology.shard_count
            if self._last == (count, shard_count) and now - self._last_post_at < STATS_FORCE_AFTER:
                return PostResult(False, None, "unchanged")

            self._last_post_at = now
            # shard_count is ALWAYS sent: the server writes it unconditionally,
            # so omitting it nulls the stored value.
            result = await self._send(
                f"/bots/{self._d.bot_id}/stats",
                {"server_count": count, "shard_count": shard_count},
                next_tick=self._interval,
            )
            if result.ok:
                self._last = (count, shard_count)
            return result


class HeartbeatPublisher(Publisher):
    """Beats for this process only -- never elected, never aggregated.

    A manager process reporting "all fine" for a cluster whose host has died is
    the exact failure mode this endpoint exists to prevent.
    """

    kind: PublisherKind = "heartbeat"

    def __init__(self, deps: Deps, interval: float = HEARTBEAT_DEFAULT_INTERVAL) -> None:
        super().__init__(deps)
        self._interval = _clamp_heartbeat(interval)

    def start(self) -> None:
        # Every beat hard-deletes the listing's cluster rows whose
        # cluster_id >= cluster_count from that request, taking their
        # monitoring_since history with it. A process that guesses 1 wipes all of
        # its siblings, so an unknown count means silence.
        if self._d.topology.cluster_count is None:
            self._disable(
                None,
                "cluster-count-unknown",
                "this process runs only some of the bot's shards and nothing says how many "
                "processes there are. Pass cluster=(id, count) or set TDL_CLUSTER_COUNT -- "
                "reporting a wrong cluster_count deletes sibling clusters server-side",
            )
            return
        super().start()

    async def _tick(self) -> float:
        await self._beat(manual=False)
        delay = max(self._interval, self._suspended_until - time.monotonic())
        # Jitter de-locksteps N cluster processes that all started from one deploy.
        return delay + random.uniform(0.0, HEARTBEAT_JITTER)

    async def post(self) -> PostResult:
        self._rearm()
        return await self._beat(manual=True)

    async def _beat(self, *, manual: bool) -> PostResult:
        async with self._lock:
            if not manual and self._disabled is not None:
                return PostResult(False, None, "disabled")
            if time.monotonic() < self._suspended_until:
                return PostResult(False, None, "plan")
            cluster_count = self._d.topology.cluster_count
            if cluster_count is None:
                return PostResult(False, None, "cluster-count-unknown")

            shards = collect_shards(self._d.client, self._d.topology)
            if not shards:  # the API requires at least one entry
                return PostResult(False, None, "no-shards")

            return await self._send(
                f"/bots/{self._d.bot_id}/heartbeat",
                {
                    "cluster": self._d.topology.cluster_id,
                    "cluster_count": cluster_count,
                    "shards": shards,
                },
                heartbeat=True,
                next_tick=self._interval,
            )

    def _react(self, outcome: Outcome) -> PostResult:
        # The cadence comes from the response rather than a constant, so a change
        # to the server's grace window retunes existing installs without a release.
        if outcome.kind == "ok" and isinstance(outcome.body, dict):
            grace = outcome.body.get("next_beat_within_seconds")
            if isinstance(grace, (int, float)) and not isinstance(grace, bool) and grace > 0:
                self._interval = _clamp_heartbeat(grace / 3)
        return super()._react(outcome)


def _clamp_heartbeat(interval: float) -> float:
    return min(max(interval, HEARTBEAT_MIN_INTERVAL), HEARTBEAT_MAX_INTERVAL)


class CommandsPublisher(Publisher):
    """Posts the application command list once, shortly after ready.

    No periodic re-check and no content hashing: commands change on deploy, and
    a deploy is a restart. ``post_commands()`` covers an in-process re-register.
    """

    kind: PublisherKind = "commands"

    def __init__(self, deps: Deps, post_empty: bool = False) -> None:
        super().__init__(deps)
        self._post_empty = post_empty

    async def _guarded(self) -> None:
        try:
            if not self._d.topology.is_stats_publisher:
                return  # global commands are identical in every process
            await self._d.client.wait_until_ready()
            await asyncio.sleep(COMMANDS_DELAY)
            await self._publish()
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("[thediscordlist] the commands publisher failed")

    async def post(self) -> PostResult:
        self._rearm()
        return await self._publish()

    async def _publish(self) -> PostResult:
        async with self._lock:
            if self._disabled is not None:
                return PostResult(False, None, "disabled")
            if time.monotonic() < self._suspended_until:
                return PostResult(False, None, "plan")

            try:
                # The raw Discord payload, forwarded verbatim. Client-side
                # validation would be actively harmful: Discord sends numbers for
                # INTEGER and NUMBER choice values, which the published spec types
                # as strings and the server accepts.
                raw = await self._d.client.http.get_global_commands(int(self._d.bot_id))
            except Exception:
                log.exception("[thediscordlist] could not fetch application commands from Discord")
                return PostResult(False, None, "fetch-failed")

            if not isinstance(raw, list):
                log.warning("[thediscordlist] Discord returned a non-list command payload")
                return PostResult(False, None, "fetch-failed")

            # An empty list CLEARS the listing, so it is never sent by accident.
            if not raw and not self._post_empty:
                return PostResult(False, None, "empty")

            return await self._send(f"/bots/{self._d.bot_id}/commands", {"commands": raw})
