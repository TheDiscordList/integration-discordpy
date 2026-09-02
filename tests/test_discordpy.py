"""Duck-typing, checked against the real library rather than the fakes.

These construct genuine discord.py clients and never connect: everything this
package reads is available before login, which is the whole reason detection
can run in `setup_hook`.
"""

from __future__ import annotations

import discord
import pytest
from discord.ext import commands

from thediscordlist import autopost
from thediscordlist._topology import collect_shards, detect_topology

DEAD = "http://127.0.0.1:1/api"


@pytest.fixture
def intents() -> discord.Intents:
    return discord.Intents.default()


def test_a_real_client_is_detected_as_single(intents: discord.Intents) -> None:
    topology = detect_topology(discord.Client(intents=intents))
    assert topology.kind == "single"
    assert (topology.cluster_id, topology.cluster_count) == (0, 1)
    assert topology.aggregates


def test_a_real_bot_is_detected_as_single(intents: discord.Intents) -> None:
    bot = commands.Bot(command_prefix="!", intents=intents)
    assert detect_topology(bot).kind == "single"


def test_a_real_auto_sharded_client_reports_every_declared_shard(
    intents: discord.Intents,
) -> None:
    client = discord.AutoShardedClient(intents=intents, shard_count=4)
    topology = detect_topology(client)

    assert topology.kind == "sharded"
    assert topology.shard_count == 4
    assert topology.aggregates
    assert topology.shard_ids == (0, 1, 2, 3)

    # No shards exist before launch; the declared ids stand in as "connecting".
    assert [s["id"] for s in collect_shards(client, topology)] == [0, 1, 2, 3]


def test_a_real_subset_of_shards_withholds_the_cluster_count(
    intents: discord.Intents,
) -> None:
    client = discord.AutoShardedClient(intents=intents, shard_count=8, shard_ids=[2, 3])
    topology = detect_topology(client)

    assert topology.kind == "multi-process"
    assert topology.cluster_count is None
    assert not topology.aggregates


def test_a_real_client_reports_a_usable_shard_before_login(intents: discord.Intents) -> None:
    client = discord.Client(intents=intents)
    entry = collect_shards(client, detect_topology(client))[0]

    assert entry["id"] == 0
    assert entry["status"] == "connecting"
    assert entry["guilds"] == 0
    # `Client.latency` is nan without a websocket, and the API minimum is 0.
    assert "latency_ms" not in entry


async def test_autopost_hooks_a_real_bot_without_disturbing_it(
    intents: discord.Intents,
) -> None:
    bot = commands.Bot(command_prefix="!", intents=intents)
    assert bot.application_id is None  # not logged in

    poster = autopost(bot, "tok", base_url=DEAD)

    # Deferred to setup_hook, which discord.py awaits at the end of login().
    assert poster.topology is None
    assert "setup_hook" in bot.__dict__

    bot._connection.application_id = 123456789012345678  # what login() does
    await bot.setup_hook()

    assert poster.topology is not None
    assert poster.topology.kind == "single"

    await poster.close()


async def test_an_absent_token_leaves_a_real_bot_untouched(intents: discord.Intents) -> None:
    bot = commands.Bot(command_prefix="!", intents=intents)
    autopost(bot, "")
    assert "setup_hook" not in bot.__dict__
    assert "close" not in bot.__dict__
