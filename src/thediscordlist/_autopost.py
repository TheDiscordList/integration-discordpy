"""One call wires a discord.py client to its TheDiscordList listing."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from ._http import DEFAULT_BASE_URL, ApiClient
from ._publishers import (
    HEARTBEAT_DEFAULT_INTERVAL,
    STATS_INTERVAL,
    CommandsPublisher,
    Deps,
    HeartbeatPublisher,
    PostResult,
    StatsPublisher,
)
from ._topology import ServerCountFn, Topology, detect_topology

log = logging.getLogger("thediscordlist")

__all__ = ["Autoposter", "autopost"]

_DISABLED = PostResult(False, None, "disabled")


class Autoposter:
    """Owns the three publishers and the HTTP session.

    Created by :func:`autopost`; you rarely construct one yourself. Every method
    returns rather than raises, so nothing here can take a bot down.
    """

    def __init__(
        self,
        client: Any,
        api: ApiClient,
        *,
        bot_id: str | None = None,
        stats: bool = True,
        heartbeat: bool = True,
        commands: bool = True,
        stats_interval: float = STATS_INTERVAL,
        heartbeat_interval: float = HEARTBEAT_DEFAULT_INTERVAL,
        post_empty_commands: bool = False,
        cluster: tuple[int, int] | None = None,
        server_count: ServerCountFn | None = None,
    ) -> None:
        self._client = client
        self._api = api
        self._bot_id = str(bot_id) if bot_id else None
        self._want = {"stats": stats, "heartbeat": heartbeat, "commands": commands}
        self._stats_interval = stats_interval
        self._heartbeat_interval = heartbeat_interval
        self._post_empty_commands = post_empty_commands
        self._cluster = cluster
        self._server_count = server_count

        self.topology: Topology | None = None
        self._stats: StatsPublisher | None = None
        self._heartbeat: HeartbeatPublisher | None = None
        self._commands: CommandsPublisher | None = None
        self._started = False
        self._closed = False

    # -- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        """Detects the topology and starts the publishers. Needs a running loop.

        Called for you from the client's ``setup_hook`` -- after login, so the
        application id is known, and before the gateway connects, so the first
        heartbeat reports "connecting" rather than letting a booting bot look
        dead on its own status page.
        """
        if self._started or self._closed:
            return
        self._started = True

        bot_id = self._bot_id or getattr(self._client, "application_id", None)
        if not bot_id:
            log.error(
                "[thediscordlist] could not read client.application_id; pass bot_id= with your "
                "Discord application id. Auto-posting is off."
            )
            return
        self._bot_id = str(bot_id)

        topology = detect_topology(self._client, self._cluster)
        self.topology = topology
        log.info(
            "[thediscordlist] topology=%s cluster=%s/%s shards=%s stats_publisher=%s",
            topology.kind,
            topology.cluster_id,
            topology.cluster_count,
            topology.shard_count,
            topology.is_stats_publisher,
        )

        intents = getattr(self._client, "intents", None)
        if intents is not None and not getattr(intents, "guilds", True):
            log.warning(
                "[thediscordlist] the Guilds intent is off. The guild cache never fills, so the "
                "reported server count will be wrong."
            )

        deps = Deps(self._api, self._client, topology, self._bot_id, self._on_auth_failure)

        if self._want["stats"] and not topology.aggregates and self._server_count is None:
            # This process holds a subset of the shards, so it sees a subset of the
            # guilds. Publishing that as the total would be a wrong number, which
            # is worse than no number.
            if topology.is_stats_publisher:
                log.warning(
                    "[thediscordlist] this process runs %d of %d shards, so it cannot count every "
                    "guild. Stats posting is off; pass server_count=<callable> that returns the "
                    "bot-wide total to turn it back on.",
                    len(topology.shard_ids),
                    topology.shard_count,
                )
            self._want["stats"] = False

        if self._want["stats"]:
            self._stats = StatsPublisher(deps, self._stats_interval, self._server_count)
            self._stats.start()
            _on_guild_change(self._client, self._stats.on_guild_change)
        if self._want["heartbeat"]:
            self._heartbeat = HeartbeatPublisher(deps, self._heartbeat_interval)
            self._heartbeat.start()
        if self._want["commands"]:
            self._commands = CommandsPublisher(deps, self._post_empty_commands)
            self._commands.start()

    async def close(self) -> None:
        """Stops every publisher and closes the HTTP session. Not restartable.

        Called for you when the client closes.
        """
        self._closed = True
        for publisher in (self._stats, self._heartbeat, self._commands):
            if publisher is not None:
                publisher.stop()
        await self._api.close()

    def _on_auth_failure(self) -> None:
        """A 401 is fatal for every publisher, not just the one that saw it."""
        for publisher in (self._stats, self._heartbeat, self._commands):
            if publisher is not None:
                publisher.stop()

    # -- manual posts -------------------------------------------------------

    async def post_stats(self) -> PostResult:
        """Posts the server count now, subject to the one-per-minute floor."""
        return await self._stats.post() if self._stats and not self._closed else _DISABLED

    async def post_heartbeat(self) -> PostResult:
        return await self._heartbeat.post() if self._heartbeat and not self._closed else _DISABLED

    async def post_commands(self) -> PostResult:
        """Re-publishes the command list; call it after re-registering in-process."""
        return await self._commands.post() if self._commands and not self._closed else _DISABLED


def autopost(
    client: Any,
    token: str | None,
    *,
    bot_id: int | str | None = None,
    base_url: str = DEFAULT_BASE_URL,
    stats: bool = True,
    heartbeat: bool = True,
    commands: bool = True,
    stats_interval: float = STATS_INTERVAL,
    heartbeat_interval: float = HEARTBEAT_DEFAULT_INTERVAL,
    post_empty_commands: bool = False,
    cluster: tuple[int, int] | None = None,
    server_count: ServerCountFn | None = None,
) -> Autoposter:
    """Wires a discord.py client to its TheDiscordList listing.

    .. code-block:: python

        bot = commands.Bot(command_prefix="!", intents=discord.Intents.default())
        autopost(bot, os.environ["TDL_TOKEN"])
        bot.run(os.environ["DISCORD_TOKEN"])

    Safe to call before or after the client starts, and with no running event
    loop. With a falsy ``token`` it warns once and returns an inert instance --
    a forgotten environment variable must never take down a deploy.

    :param client: any :class:`discord.Client`, including ``commands.Bot`` and
        the auto-sharded variants.
    :param token: the listing's stats token, from its manage page -> API tab.
    :param bot_id: the Discord *application* id. Defaults to
        ``client.application_id``, which is what the API keys on -- for legacy
        bots it differs from the bot user id.
    :param stats_interval: seconds between guild-count checks, 300 by default.
        A guild join or removal also posts, 20s after the last one. Clamped to
        a 60s minimum, which is also the hard floor between two posts.
    :param heartbeat_interval: the starting cadence, 30 by default and clamped
        to 15-60s. Each response retunes it from the server's grace window.
    :param post_empty_commands: send an empty command list, which *clears* the
        listing. Off by default.
    :param cluster: ``(cluster_id, cluster_count)`` when the bot's shards are
        split across processes. Required for heartbeats in that topology: a
        wrong ``cluster_count`` deletes sibling clusters server-side.
    :param server_count: a callable returning the bot-wide guild total. Needed
        only when one process cannot see every shard.
    """
    from . import __version__  # deferred: the package is mid-import at module load

    poster = Autoposter(
        client,
        ApiClient(
            token or "",
            base_url,
            f"thediscordlist-py/{__version__} (discord.py {_discord_version()})",
        ),
        bot_id=str(bot_id) if bot_id else None,
        stats=stats,
        heartbeat=heartbeat,
        commands=commands,
        stats_interval=stats_interval,
        heartbeat_interval=heartbeat_interval,
        post_empty_commands=post_empty_commands,
        cluster=cluster,
        server_count=server_count,
    )

    if not token:
        log.warning("[thediscordlist] no stats token supplied; auto-posting is disabled.")
        return poster

    _wire(client, poster)
    return poster


def _wire(client: Any, poster: Autoposter) -> None:
    """Hangs start/stop off the client's own lifecycle.

    ``setup_hook`` is the documented "after login, before connect" hook, which
    is exactly when the application id is known and the gateway is not up yet.
    Both hooks chain rather than replace, so a user override still runs.
    """
    if client.application_id is not None and _loop_running():
        poster.start()  # already logged in -- e.g. called from a cog or on_ready
    else:
        original_setup = client.setup_hook

        async def setup_hook() -> None:
            await original_setup()
            poster.start()

        client.setup_hook = setup_hook

    original_close = client.close

    async def close() -> None:
        await poster.close()
        await original_close()

    client.close = close


def _on_guild_change(client: Any, callback: Callable[[], None]) -> None:
    """Runs ``callback`` on every guild join and removal, beside the user's own handlers."""

    async def listener(*_: Any) -> None:
        callback()

    for event in ("on_guild_join", "on_guild_remove"):
        if hasattr(client, "add_listener"):  # commands.Bot
            client.add_listener(listener, event)
            continue

        # A plain Client dispatches only to its own on_<event> attribute, so chain onto it.
        original = getattr(client, event, None)

        async def chained(*args: Any, _original: Any = original) -> None:
            callback()
            if _original is not None:
                await _original(*args)

        setattr(client, event, chained)


def _loop_running() -> bool:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


def _discord_version() -> str:
    try:
        import discord

        return str(discord.__version__)
    except Exception:  # pragma: no cover - discord.py is a hard dependency
        return "unknown"
