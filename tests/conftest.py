"""Fakes shared by the tests, and the real libraries this environment has.

The whole package duck-types the client, so a handful of attributes stands in
for discord.py exactly as `Client`, `AutoShardedClient` and `commands.Bot` do.
"""

from __future__ import annotations

import importlib
from types import ModuleType
from typing import Any

from thediscordlist._http import Outcome


def _installed(name: str, title: str) -> ModuleType | None:
    """The library, if installed -- checked by title, since Pycord imports as `discord`."""
    try:
        module = importlib.import_module(name)
    except ModuleNotFoundError as error:
        if error.name != name:
            raise  # installed but broken: fail loudly rather than skip every test
        return None
    return module if getattr(module, "__title__", None) == title else None


#: Every supported library this environment has, by distribution name. CI installs
#: one per job: discord.py and Pycord cannot share an environment.
LIBRARIES = {
    dist: module
    for dist, name, title in [
        ("discord.py", "discord", "discord"),
        ("py-cord", "discord", "pycord"),
        ("nextcord", "nextcord", "nextcord"),
        ("disnake", "disnake", "disnake"),
    ]
    if (module := _installed(name, title)) is not None
}

collect_ignore = [] if "discord.py" in LIBRARIES else ["test_discordpy.py"]


class FakeGuild:
    def __init__(self, shard_id: int) -> None:
        self.shard_id = shard_id


class FakeShard:
    def __init__(self, shard_id: int, latency: float = 0.042, closed: bool = False) -> None:
        self.id = shard_id
        self.latency = latency
        self._closed = closed

    def is_closed(self) -> bool:
        return self._closed


class FakeHTTP:
    def __init__(self, commands: Any = None) -> None:
        self.commands = [] if commands is None else commands
        self.calls: list[int] = []

    async def get_global_commands(self, application_id: int) -> Any:
        self.calls.append(application_id)
        if isinstance(self.commands, Exception):
            raise self.commands
        return self.commands


class FakeIntents:
    def __init__(self, guilds: bool = True) -> None:
        self.guilds = guilds


class FakeClient:
    """A plain, unsharded client unless `shards` is passed."""

    def __init__(
        self,
        *,
        guilds: int = 3,
        shard_count: int | None = None,
        shard_id: int | None = None,
        shard_ids: list[int] | None = None,
        shards: dict[int, FakeShard] | None = None,
        ready: bool = True,
        closed: bool = False,
        latency: float = 0.05,
        application_id: int | None = 123456789012345678,
        commands: Any = None,
    ) -> None:
        count = shard_count or 1
        self.guilds = [FakeGuild((i * 7919) % count) for i in range(guilds)]
        self.shard_count = shard_count
        self.shard_id = shard_id
        self.latency = latency
        self.intents = FakeIntents()
        self.application_id = application_id
        self.http = FakeHTTP(commands)
        self._ready = ready
        self._closed = closed
        self.closed_by_us = False
        if shard_ids is not None:
            self.shard_ids = shard_ids
        if shards is not None:
            self.shards = shards

    def is_ready(self) -> bool:
        return self._ready

    def is_closed(self) -> bool:
        return self._closed

    async def wait_until_ready(self) -> None:
        return None

    async def setup_hook(self) -> None:
        return None

    async def close(self) -> None:
        self.closed_by_us = True


class FakeApi:
    """Stands in for `ApiClient`, recording posts and replaying queued outcomes."""

    def __init__(self, *outcomes: Outcome) -> None:
        self.queue = list(outcomes)
        self.default = Outcome("ok", 200, {})
        self.calls: list[tuple[str, Any, bool]] = []

    async def post(
        self, path: str, body: Any, *, heartbeat: bool = False, next_tick: float | None = None
    ) -> Outcome:
        self.calls.append((path, body, heartbeat))
        return self.queue.pop(0) if self.queue else self.default

    async def close(self) -> None:
        return None

    @property
    def bodies(self) -> list[Any]:
        return [body for _, body, _ in self.calls]
