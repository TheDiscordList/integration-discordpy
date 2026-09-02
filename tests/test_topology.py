from __future__ import annotations

from conftest import FakeClient, FakeShard
from thediscordlist._topology import (
    MAX_LATENCY_MS,
    collect_shards,
    detect_topology,
    local_shard_ids,
    measure_server_count,
)


def test_a_plain_client_is_cluster_zero_of_one() -> None:
    topology = detect_topology(FakeClient())
    assert topology.kind == "single"
    assert (topology.cluster_id, topology.cluster_count) == (0, 1)
    assert topology.shard_count == 1
    assert topology.is_stats_publisher
    assert topology.aggregates


def test_an_auto_sharded_client_holding_every_shard_still_aggregates() -> None:
    client = FakeClient(shard_count=4, shards={i: FakeShard(i) for i in range(4)})
    topology = detect_topology(client)
    assert topology.kind == "sharded"
    assert topology.cluster_count == 1
    assert topology.shard_count == 4
    assert topology.aggregates
    assert local_shard_ids(client) == (0, 1, 2, 3)


def test_a_subset_of_shards_means_multi_process_and_no_cluster_count() -> None:
    """cluster_count must stay None: a wrong one deletes sibling clusters."""
    client = FakeClient(shard_count=8, shard_ids=[2, 3], shards={2: FakeShard(2)})
    topology = detect_topology(client)
    assert topology.kind == "multi-process"
    assert topology.cluster_count is None
    assert not topology.aggregates
    assert not topology.is_stats_publisher  # shard 0 lives elsewhere


def test_the_process_holding_shard_zero_is_the_stats_publisher() -> None:
    client = FakeClient(shard_count=8, shard_ids=[0, 1], shards={0: FakeShard(0)})
    assert detect_topology(client).is_stats_publisher


def test_an_explicit_cluster_wins() -> None:
    client = FakeClient(shard_count=8, shard_ids=[4, 5])
    topology = detect_topology(client, cluster=(2, 4))
    assert topology.kind == "custom"
    assert (topology.cluster_id, topology.cluster_count) == (2, 4)
    assert not topology.is_stats_publisher


def test_the_environment_can_supply_the_cluster() -> None:
    client = FakeClient(shard_count=8, shard_ids=[0, 1])
    topology = detect_topology(client, env={"TDL_CLUSTER": "0", "TDL_CLUSTER_COUNT": "4"})
    assert (topology.cluster_id, topology.cluster_count) == (0, 4)
    assert topology.is_stats_publisher


def test_a_junk_cluster_count_is_ignored() -> None:
    client = FakeClient(shard_count=8, shard_ids=[0, 1])
    topology = detect_topology(client, env={"TDL_CLUSTER_COUNT": "nope"})
    assert topology.cluster_count is None


# -- shard reports ----------------------------------------------------------


def test_an_unsharded_client_reports_exactly_one_shard() -> None:
    client = FakeClient(guilds=5, latency=0.123)
    shards = collect_shards(client, detect_topology(client))
    assert shards == [{"id": 0, "status": "ready", "guilds": 5, "latency_ms": 123}]


def test_latency_is_omitted_before_the_first_heartbeat_ack() -> None:
    client = FakeClient(latency=float("inf"))
    assert "latency_ms" not in collect_shards(client, detect_topology(client))[0]


def test_latency_is_capped_at_the_api_maximum() -> None:
    client = FakeClient(latency=9999.0)
    assert collect_shards(client, detect_topology(client))[0]["latency_ms"] == MAX_LATENCY_MS


def test_a_booting_bot_reports_connecting_not_disconnected() -> None:
    """Idle is the state before connect; reporting it as down pages someone."""
    client = FakeClient(ready=False)
    assert collect_shards(client, detect_topology(client))[0]["status"] == "connecting"


def test_a_closed_shard_reports_disconnected() -> None:
    client = FakeClient(shard_count=2, shards={0: FakeShard(0), 1: FakeShard(1, closed=True)})
    statuses = [s["status"] for s in collect_shards(client, detect_topology(client))]
    assert statuses == ["ready", "disconnected"]


def test_guilds_are_counted_per_shard() -> None:
    client = FakeClient(guilds=20, shard_count=2, shards={0: FakeShard(0), 1: FakeShard(1)})
    shards = collect_shards(client, detect_topology(client))
    assert sum(s["guilds"] for s in shards) == 20


def test_declared_shards_are_reported_before_the_gateway_connects() -> None:
    """An AutoShardedClient has no `shards` until launch; silence would read as down."""
    client = FakeClient(shard_count=3, shards={}, ready=False)
    shards = collect_shards(client, detect_topology(client))
    assert [s["id"] for s in shards] == [0, 1, 2]
    assert {s["status"] for s in shards} == {"connecting"}


def test_a_shard_with_a_broken_websocket_does_not_raise() -> None:
    class Broken:
        id = 0

        def is_closed(self) -> bool:
            raise AttributeError("no websocket yet")

        @property
        def latency(self) -> float:
            raise AttributeError("no websocket yet")

    client = FakeClient(shard_count=1, shards={0: Broken()})  # type: ignore[dict-item]
    assert collect_shards(client, detect_topology(client))[0]["status"] == "connecting"


# -- counting ---------------------------------------------------------------


async def test_a_process_that_sees_every_shard_counts_locally() -> None:
    client = FakeClient(guilds=7)
    assert await measure_server_count(client, detect_topology(client)) == 7


async def test_a_process_that_sees_a_subset_refuses_to_guess() -> None:
    client = FakeClient(guilds=7, shard_count=8, shard_ids=[0, 1])
    assert await measure_server_count(client, detect_topology(client)) is None


async def test_the_callback_wins_and_may_be_async() -> None:
    client = FakeClient(guilds=7, shard_count=8, shard_ids=[0, 1])
    topology = detect_topology(client)

    async def total() -> int:
        return 4321

    assert await measure_server_count(client, topology, total) == 4321
    assert await measure_server_count(client, topology, lambda: 99) == 99


async def test_a_broken_callback_yields_no_count_rather_than_a_wrong_one() -> None:
    client = FakeClient()
    topology = detect_topology(client)

    def boom() -> int:
        raise RuntimeError("redis is down")

    assert await measure_server_count(client, topology, boom) is None
    assert await measure_server_count(client, topology, lambda: -1) is None  # type: ignore[arg-type,return-value]
    assert await measure_server_count(client, topology, lambda: "12") is None  # type: ignore[arg-type,return-value]
