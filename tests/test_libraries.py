"""The shared contract, checked against every real library installed here.

discord.py, Pycord, nextcord and disnake are forks of one codebase, so every
read this package makes has the same name in all four. These tests build real
clients and never connect: only the two REST calls a login makes are stubbed.
Clients are built inside a running loop because Pycord and disnake insist.
"""

from __future__ import annotations

import importlib
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from conftest import LIBRARIES, FakeApi
from thediscordlist import autopost
from thediscordlist._publishers import CommandsPublisher, Deps
from thediscordlist._topology import collect_shards, detect_topology

DEAD = "http://127.0.0.1:1/api"
APP_ID = 123456789012345678
USER = {"id": "987654321098765432", "username": "tdl", "discriminator": "0", "avatar": None}


@pytest.fixture(params=list(LIBRARIES))
def dist(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.fixture
def lib(dist: str) -> ModuleType:
    return LIBRARIES[dist]


def make_bot(lib: ModuleType, **kwargs: Any) -> Any:
    commands = importlib.import_module(f"{lib.__name__}.ext.commands")
    # `when_mentioned` keeps disnake from warning about the message-content intent.
    return commands.Bot(
        command_prefix=commands.when_mentioned, intents=lib.Intents.default(), **kwargs
    )


def stub_login(client: Any) -> AsyncMock:
    """Stubs the REST calls of a login; returns the `application_info` stub."""
    client.http.static_login = AsyncMock(return_value=USER)
    client.application_info = AsyncMock(
        return_value=SimpleNamespace(id=APP_ID, interactions_endpoint_url=None, flags=0)
    )
    return client.application_info


# -- topology ---------------------------------------------------------------


async def test_a_client_and_a_bot_are_detected_as_single(lib: ModuleType) -> None:
    for client in (lib.Client(intents=lib.Intents.default()), make_bot(lib)):
        topology = detect_topology(client)
        assert (topology.kind, topology.cluster_id, topology.cluster_count) == ("single", 0, 1)
        assert topology.aggregates
        await client.close()


async def test_an_auto_sharded_client_reports_every_declared_shard(lib: ModuleType) -> None:
    client = lib.AutoShardedClient(intents=lib.Intents.default(), shard_count=4)
    topology = detect_topology(client)

    assert (topology.kind, topology.shard_count, topology.shard_ids) == ("sharded", 4, (0, 1, 2, 3))
    assert topology.aggregates
    # No shards exist before launch; the declared ids stand in as "connecting".
    shards = collect_shards(client, topology)
    assert [s["id"] for s in shards] == [0, 1, 2, 3]
    assert {s["status"] for s in shards} == {"connecting"}


async def test_a_subset_of_shards_withholds_the_cluster_count(lib: ModuleType) -> None:
    client = lib.AutoShardedClient(intents=lib.Intents.default(), shard_count=8, shard_ids=[2, 3])
    topology = detect_topology(client)

    assert topology.kind == "multi-process"
    assert topology.cluster_count is None
    assert not topology.aggregates


async def test_a_client_reports_a_usable_shard_before_login(lib: ModuleType) -> None:
    client = lib.Client(intents=lib.Intents.default())
    entry = collect_shards(client, detect_topology(client))[0]

    assert (entry["id"], entry["status"], entry["guilds"]) == (0, "connecting", 0)
    assert "latency_ms" not in entry  # `latency` is nan without a websocket
    await client.close()


# -- wiring -----------------------------------------------------------------


async def test_autopost_starts_after_a_real_login_and_stops_with_close(
    dist: str, lib: ModuleType
) -> None:
    client = make_bot(lib)
    poster = autopost(client, "tok", base_url=DEAD)
    application_info = stub_login(client)

    assert poster.topology is None  # nothing happens before login
    assert poster._api._headers["user-agent"].endswith(f"({dist} {lib.__version__})")

    await client.login("discord-token")

    assert poster.topology is not None
    assert poster.topology.kind == "single"
    assert poster._bot_id == str(APP_ID)
    # discord.py's login() fetches it itself; for the forks the wiring does. Once either way.
    application_info.assert_awaited_once()

    await client.close()
    assert poster._closed


async def test_a_known_bot_id_saves_the_application_lookup(lib: ModuleType) -> None:
    client = make_bot(lib)
    if hasattr(client, "setup_hook"):
        pytest.skip("discord.py's own login() always fetches the application")
    poster = autopost(client, "tok", base_url=DEAD, bot_id=APP_ID)
    application_info = stub_login(client)

    await client.login("discord-token")

    assert poster.topology is not None
    application_info.assert_not_awaited()
    await client.close()


async def test_autopost_starts_at_once_when_the_application_id_is_known(lib: ModuleType) -> None:
    """E.g. called from on_ready, or with `application_id=` passed to the client."""
    client = make_bot(lib, application_id=APP_ID)
    poster = autopost(client, "tok", base_url=DEAD)

    assert poster.topology is not None
    await client.close()
    assert poster._closed


async def test_an_absent_token_leaves_a_real_bot_untouched(lib: ModuleType) -> None:
    client = make_bot(lib)
    autopost(client, "")
    assert not {"setup_hook", "login", "close"} & set(vars(client))
    await client.close()


# -- commands ---------------------------------------------------------------


async def test_commands_are_read_through_the_librarys_own_http_client(lib: ModuleType) -> None:
    """`http.get_global_commands(application_id)` has the same shape in all four."""
    client = lib.Client(intents=lib.Intents.default())
    payload = [{"name": "ping", "description": "Pong", "name_localizations": None}]
    routes: list[Any] = []

    async def request(route: Any, *args: Any, **kwargs: Any) -> Any:
        routes.append(route)
        return payload

    client.http.request = request
    api = FakeApi()

    result = await CommandsPublisher(
        Deps(api, client, detect_topology(client), str(APP_ID))  # type: ignore[arg-type]
    ).post()

    assert result.ok
    assert routes[0].method == "GET"
    assert routes[0].url.endswith(f"/applications/{APP_ID}/commands")
    assert api.bodies == [{"commands": payload}]
    await client.close()
