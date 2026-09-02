"""Sharding-topology detection and the shard report the heartbeat sends.

discord.py exposes its whole topology on the client, so detection is a pure
function of `shard_id`, `shard_ids`, `shard_count` and `shards`. Everything is
duck-typed rather than `isinstance`-checked, so a subclass, a mock or a future
`AutoShardedBot` variant all work.

Unlike discord.js there is no cross-process aggregation bus. A process that
holds a *subset* of the shards can only see a fraction of the guilds, so it
reports no server count at all rather than a wrong one.
"""

from __future__ import annotations

import logging
import math
import os
from collections import Counter
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from inspect import isawaitable
from typing import Any, Literal

log = logging.getLogger("thediscordlist")

#: The API caps `latency_ms` here; a gateway ping over ten minutes is not a reading.
MAX_LATENCY_MS = 600_000

ShardStatus = Literal["ready", "connecting", "resuming", "disconnected"]
TopologyKind = Literal["single", "sharded", "multi-process", "custom"]

ServerCountFn = Callable[[], "int | Awaitable[int]"]


@dataclass(frozen=True, slots=True)
class Topology:
    kind: TopologyKind
    cluster_id: int
    #: `None` means undeterminable -- the heartbeat publisher must not start.
    cluster_count: int | None
    #: Whether this process is the one that posts stats.
    is_stats_publisher: bool
    shard_count: int
    #: Whether this process holds every shard, and so can see every guild.
    aggregates: bool
    #: The shard ids this process is responsible for.
    shard_ids: tuple[int, ...]


def _positive_int(value: str | None) -> int | None:
    try:
        parsed = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _non_negative_int(value: str | None) -> int | None:
    parsed = _positive_int(value)
    if parsed is not None:
        return parsed
    return 0 if value == "0" else None


def local_shard_ids(client: Any) -> tuple[int, ...]:
    """The shard ids this process runs, known before the gateway connects."""
    declared = getattr(client, "shard_ids", None)
    if declared:
        return tuple(sorted(declared))
    count = getattr(client, "shard_count", None) or 1
    # `shards` only exists on AutoShardedClient, and is empty until launch.
    if hasattr(client, "shards"):
        return tuple(range(count))
    return (getattr(client, "shard_id", None) or 0,)


def detect_topology(
    client: Any,
    cluster: tuple[int, int] | None = None,
    env: Mapping[str, str] = os.environ,
) -> Topology:
    ids = local_shard_ids(client)
    shard_count = getattr(client, "shard_count", None) or len(ids)
    # A process holding a subset of the shards sees a subset of the guilds.
    aggregates = len(ids) >= shard_count

    if cluster is None:
        env_count = _positive_int(env.get("TDL_CLUSTER_COUNT"))
        if env_count is not None:
            cluster = (_non_negative_int(env.get("TDL_CLUSTER")) or 0, env_count)

    if cluster is not None:
        cluster_id, cluster_count = cluster
        return Topology(
            "custom", cluster_id, cluster_count, cluster_id == 0, shard_count, aggregates, ids
        )

    if not aggregates:
        # Shards are split across processes but nothing says how many. Guessing
        # `cluster_count` is destructive server-side, so the heartbeat stays off
        # until `cluster=` or TDL_CLUSTER_COUNT says otherwise.
        return Topology(
            "multi-process",
            ids[0] // len(ids) if ids else 0,
            None,
            bool(ids) and ids[0] == 0,
            shard_count,
            False,
            ids,
        )

    kind: TopologyKind = "sharded" if shard_count > 1 else "single"
    return Topology(kind, 0, 1, True, shard_count, True, ids)


def _status(closed: bool, ready: bool) -> ShardStatus:
    # `connecting`, never `disconnected`, while the client is still booting:
    # reporting a booting shard as down opens an incident and pages someone.
    if closed:
        return "disconnected"
    return "ready" if ready else "connecting"


def _shard_entry(
    shard_id: int, latency: float, closed: bool, ready: bool, guilds: int
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "id": shard_id,
        "status": _status(closed, ready),
        "guilds": guilds,
    }
    # discord.py reports `inf` (or `nan`) before the first HEARTBEAT_ACK, and the
    # API minimum is 0, so the key is omitted rather than sent nonsense.
    if math.isfinite(latency) and latency >= 0:
        entry["latency_ms"] = min(round(latency * 1000), MAX_LATENCY_MS)
    return entry


def collect_shards(client: Any, topology: Topology) -> list[dict[str, Any]]:
    """One entry per shard this process owns, in the API's shape.

    Before the gateway connects, `AutoShardedClient.shards` is empty; the
    declared ids are reported as `connecting` instead, so a booting bot reads
    as booting rather than vanishing from its own status page.
    """
    ready = bool(client.is_ready())
    closed = bool(client.is_closed())
    per_shard = Counter(guild.shard_id for guild in client.guilds)

    shards: Mapping[int, Any] | None = getattr(client, "shards", None)
    if not shards:
        if shards is None:  # a plain, unsharded Client
            shard_id = topology.shard_ids[0] if topology.shard_ids else 0
            latency = float(getattr(client, "latency", float("nan")))
            return [_shard_entry(shard_id, latency, closed, ready, per_shard[shard_id])]
        return [
            _shard_entry(shard_id, float("nan"), closed, False, per_shard[shard_id])
            for shard_id in topology.shard_ids
        ]

    entries: list[dict[str, Any]] = []
    for shard_id, info in sorted(shards.items()):
        try:
            shard_closed = closed or bool(info.is_closed())
            latency = float(info.latency)
            shard_ready = ready
        except Exception:
            # A shard mid-reconnect has no usable websocket. "connecting" is the
            # honest middle: not confirmed up, and certainly not confirmed down.
            shard_closed, latency, shard_ready = False, float("nan"), False
        entries.append(
            _shard_entry(shard_id, latency, shard_closed, shard_ready, per_shard[shard_id])
        )
    return entries


async def measure_server_count(
    client: Any,
    topology: Topology,
    server_count: ServerCountFn | None = None,
) -> int | None:
    """The guild total, or `None` when it cannot be trusted.

    `None` makes the caller wait rather than publish a wrong number. The guild
    cache is complete at READY -- unavailable stubs included, which is exactly
    what a bot list wants -- and does not drain during a Discord outage, so a
    local read needs no sanity backstop.
    """
    if server_count is not None:
        try:
            value = server_count()
            if isawaitable(value):
                value = await value
        except Exception:
            log.exception("[thediscordlist] server_count callback raised")
            return None
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return None
        return value

    if topology.aggregates:
        return len(client.guilds)
    return None
