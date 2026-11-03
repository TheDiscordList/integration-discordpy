from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import pytest

from conftest import FakeApi, FakeClient, FakeShard
from thediscordlist._http import Outcome
from thediscordlist._publishers import (
    PLAN_REPROBE,
    STATS_FORCE_AFTER,
    STATS_INTERVAL,
    STATS_MIN_GAP,
    CommandsPublisher,
    Deps,
    HeartbeatPublisher,
    StatsPublisher,
    _worth_retrying,
)
from thediscordlist._topology import detect_topology

BOT_ID = "123456789012345678"


def make_deps(client: Any, api: FakeApi, **kwargs: Any) -> Deps:
    return Deps(api, client, detect_topology(client, **kwargs), BOT_ID)  # type: ignore[arg-type]


@pytest.fixture
def no_min_gap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("thediscordlist._publishers.STATS_MIN_GAP", 0.0)


# -- stats ------------------------------------------------------------------


async def test_stats_posts_the_count_and_always_sends_shard_count() -> None:
    """Omitting shard_count nulls the stored value server-side."""
    client = FakeClient(guilds=42, shard_count=4, shards={i: FakeShard(i) for i in range(4)})
    api = FakeApi()
    stats = StatsPublisher(make_deps(client, api))

    result = await stats.post()

    assert result.ok
    assert api.calls[0][0] == f"/bots/{BOT_ID}/stats"
    assert api.bodies[0] == {"server_count": 42, "shard_count": 4}


async def test_stats_sends_the_shard_count_an_auto_sharded_client_learned_after_setup() -> None:
    client = FakeClient(guilds=42, shards={})
    api = FakeApi()
    stats = StatsPublisher(make_deps(client, api))

    client.shard_count = 4  # what launch_shards() sets, after setup_hook
    await stats.post()

    assert api.bodies[0]["shard_count"] == 4


@pytest.mark.usefixtures("no_min_gap")
async def test_stats_skips_an_unchanged_count() -> None:
    """An unchanged post has no side effects, but still spends the shared budget."""
    client = FakeClient(guilds=42)
    api = FakeApi()
    stats = StatsPublisher(make_deps(client, api))

    assert (await stats.post()).ok
    second = await stats.post()

    assert second.skipped == "unchanged"
    assert len(api.calls) == 1


@pytest.mark.usefixtures("no_min_gap")
async def test_stats_reposts_an_unchanged_count_eventually() -> None:
    client = FakeClient(guilds=42)
    api = FakeApi()
    stats = StatsPublisher(make_deps(client, api))

    await stats.post()
    stats._last_post_at -= STATS_FORCE_AFTER + 1

    assert (await stats.post()).ok
    assert len(api.calls) == 2


@pytest.mark.usefixtures("no_min_gap")
async def test_stats_posts_again_when_the_count_moves() -> None:
    client = FakeClient(guilds=42)
    api = FakeApi()
    stats = StatsPublisher(make_deps(client, api))

    await stats.post()
    client.guilds.append(client.guilds[0])

    assert (await stats.post()).ok
    assert api.bodies[1]["server_count"] == 43


async def test_stats_enforces_a_hard_floor_between_posts() -> None:
    """A join storm, a mass reconnect and a manual post all route through it."""
    client = FakeClient(guilds=42)
    api = FakeApi()
    stats = StatsPublisher(make_deps(client, api))

    await stats.post()
    client.guilds.append(client.guilds[0])
    second = await stats.post()

    assert second.skipped == "min-gap"
    assert len(api.calls) == 1


async def test_a_min_gap_skip_retries_when_the_gap_ends_not_a_full_interval_later() -> None:
    stats = StatsPublisher(make_deps(FakeClient(), FakeApi()))
    await stats.post()

    delay = await stats._tick()

    assert stats._interval == STATS_INTERVAL
    assert 0 < delay <= STATS_MIN_GAP


async def test_a_guild_change_ends_the_wait_once_it_goes_quiet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("thediscordlist._publishers.STATS_DEBOUNCE", 0.01)
    stats = StatsPublisher(make_deps(FakeClient(), FakeApi()))

    waiting = asyncio.create_task(stats._wait(STATS_INTERVAL))
    await asyncio.sleep(0)
    stats.on_guild_change()

    await asyncio.wait_for(waiting, 1)


async def test_a_join_storm_cannot_hold_the_post_back_past_the_min_gap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("thediscordlist._publishers.STATS_DEBOUNCE", 0.05)
    monkeypatch.setattr("thediscordlist._publishers.STATS_MIN_GAP", 0.2)
    stats = StatsPublisher(make_deps(FakeClient(), FakeApi()))

    async def storm() -> None:
        while True:
            stats.on_guild_change()
            await asyncio.sleep(0.01)

    storming = asyncio.create_task(storm())
    started = time.monotonic()
    await asyncio.wait_for(stats._wait(STATS_INTERVAL), 2)
    storming.cancel()

    assert time.monotonic() - started < 1


async def test_stats_below_the_floor_is_clamped_and_warned(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING, logger="thediscordlist"):
        stats = StatsPublisher(make_deps(FakeClient(), FakeApi()), interval=5)
    assert stats._interval == 60
    assert "rate-limit floor" in caplog.text


async def test_stats_says_nothing_when_it_cannot_count() -> None:
    client = FakeClient(guilds=7, shard_count=8, shard_ids=[0, 1])
    api = FakeApi()
    stats = StatsPublisher(make_deps(client, api))

    assert (await stats.post()).skipped == "not-elected"
    assert api.calls == []


# -- heartbeat --------------------------------------------------------------


async def test_heartbeat_reports_this_process_only() -> None:
    client = FakeClient(guilds=6, shard_count=2, shards={0: FakeShard(0), 1: FakeShard(1)})
    api = FakeApi()
    heartbeat = HeartbeatPublisher(make_deps(client, api))

    result = await heartbeat.post()

    assert result.ok
    body = api.bodies[0]
    assert body["cluster"] == 0
    assert body["cluster_count"] == 1
    assert [s["id"] for s in body["shards"]] == [0, 1]
    assert api.calls[0][2] is True  # sent with heartbeat priority


async def test_a_shard_state_change_beats_out_of_band_at_most_once_per_gap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("thediscordlist._publishers.HEARTBEAT_OUT_OF_BAND_GAP", 0.1)
    monkeypatch.setattr("thediscordlist._publishers.HEARTBEAT_OUT_OF_BAND_DELAY", 0.01)
    heartbeat = HeartbeatPublisher(make_deps(FakeClient(), FakeApi()))
    await heartbeat.post()

    started = time.monotonic()
    waiting = asyncio.create_task(heartbeat._wait(60))
    await asyncio.sleep(0)
    heartbeat.beat_now()
    await asyncio.sleep(0.01)
    heartbeat.beat_now()  # a burst, while the first is still settling
    await asyncio.wait_for(waiting, 1)

    assert 0.05 < time.monotonic() - started < 1
    assert not heartbeat._changed.is_set()


async def test_heartbeat_refuses_to_start_without_a_cluster_count(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A wrong cluster_count hard-deletes the sibling clusters' history."""
    client = FakeClient(shard_count=8, shard_ids=[0, 1])
    api = FakeApi()
    heartbeat = HeartbeatPublisher(make_deps(client, api))

    with caplog.at_level(logging.ERROR, logger="thediscordlist"):
        heartbeat.start()

    assert heartbeat._task is None
    assert "cluster=(id, count)" in caplog.text
    assert (await heartbeat.post()).skipped == "cluster-count-unknown"
    assert api.calls == []


async def test_an_explicit_cluster_re_enables_the_heartbeat() -> None:
    client = FakeClient(shard_count=8, shard_ids=[0, 1], shards={0: FakeShard(0)})
    api = FakeApi()
    heartbeat = HeartbeatPublisher(make_deps(client, api, cluster=(0, 4)))

    assert (await heartbeat.post()).ok
    assert api.bodies[0]["cluster_count"] == 4


async def test_heartbeat_retunes_its_cadence_from_the_response() -> None:
    """Derived from the server's grace window, so a change to it needs no release."""
    client = FakeClient()
    api = FakeApi(Outcome("ok", 200, {"next_beat_within_seconds": 120}))
    heartbeat = HeartbeatPublisher(make_deps(client, api))

    await heartbeat.post()

    assert heartbeat._interval == 40  # 120 / 3


@pytest.mark.parametrize(("grace", "expected"), [(9, 15.0), (600, 60.0), (0, 30.0)])
async def test_the_retuned_cadence_is_clamped(grace: int, expected: float) -> None:
    api = FakeApi(Outcome("ok", 200, {"next_beat_within_seconds": grace}))
    heartbeat = HeartbeatPublisher(make_deps(FakeClient(), api))
    await heartbeat.post()
    assert heartbeat._interval == expected


@pytest.mark.parametrize(("interval", "expected"), [(1, 15.0), (45, 45.0), (600, 60.0)])
async def test_a_configured_cadence_is_clamped_before_the_first_beat(
    interval: float, expected: float
) -> None:
    heartbeat = HeartbeatPublisher(make_deps(FakeClient(), FakeApi()), interval)
    assert heartbeat._interval == expected


async def test_heartbeat_skips_a_beat_with_no_shards_to_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("thediscordlist._publishers.collect_shards", lambda *_: [])
    heartbeat = HeartbeatPublisher(make_deps(FakeClient(shard_count=2, shards={}), FakeApi()))

    assert (await heartbeat.post()).skipped == "no-shards"


# -- commands ---------------------------------------------------------------


async def test_commands_forwards_discords_payload_verbatim() -> None:
    """Discord sends numbers for INTEGER choice values; validating would break it."""
    payload = [{"name": "ping", "options": [{"choices": [{"name": "one", "value": 1}]}]}]
    client = FakeClient(commands=payload)
    api = FakeApi()

    result = await CommandsPublisher(make_deps(client, api)).post()

    assert result.ok
    assert api.bodies[0] == {"commands": payload}
    assert client.http.calls == [int(BOT_ID)]


async def test_commands_never_clears_the_listing_by_accident() -> None:
    client = FakeClient(commands=[])
    api = FakeApi()

    assert (await CommandsPublisher(make_deps(client, api)).post()).skipped == "empty"
    assert api.calls == []


async def test_commands_can_clear_the_listing_on_request() -> None:
    client = FakeClient(commands=[])
    api = FakeApi()

    assert (await CommandsPublisher(make_deps(client, api), post_empty=True).post()).ok
    assert api.bodies[0] == {"commands": []}


async def test_a_discord_failure_does_not_escape_the_publisher() -> None:
    client = FakeClient(commands=RuntimeError("Discord is down"))
    api = FakeApi()

    assert (await CommandsPublisher(make_deps(client, api)).post()).skipped == "fetch-failed"


@pytest.mark.parametrize(
    "failure",
    [
        Outcome("transient", 502, error=RuntimeError("Bad Gateway")),
        Outcome("transient", error=OSError("connection reset")),
        Outcome("dropped"),
        Outcome("throttled", 429, retry_after=1.0),
    ],
)
async def test_commands_tries_again_until_it_lands(
    monkeypatch: pytest.MonkeyPatch, failure: Outcome
) -> None:
    monkeypatch.setattr("thediscordlist._publishers.COMMANDS_DELAY", 0.0)
    monkeypatch.setattr("thediscordlist._publishers.COMMANDS_RETRY", 0.0)
    api = FakeApi(failure, failure)
    publisher = CommandsPublisher(make_deps(FakeClient(commands=[{"name": "ping"}]), api))

    publisher.start()
    assert publisher._task is not None
    await asyncio.wait_for(publisher._task, 1)

    assert len(api.calls) == 3


async def test_commands_tries_again_after_a_failed_discord_fetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("thediscordlist._publishers.COMMANDS_DELAY", 0.0)
    monkeypatch.setattr("thediscordlist._publishers.COMMANDS_RETRY", 0.0)
    client = FakeClient(commands=[{"name": "ping"}])
    fetch = client.http.get_global_commands
    attempts = 0

    async def flaky(application_id: int) -> Any:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("Discord is down")
        return await fetch(application_id)

    client.http.get_global_commands = flaky  # type: ignore[method-assign]
    api = FakeApi()
    publisher = CommandsPublisher(make_deps(client, api))

    publisher.start()
    assert publisher._task is not None
    await asyncio.wait_for(publisher._task, 1)

    assert attempts == 2
    assert len(api.calls) == 1


@pytest.mark.parametrize(
    "failure",
    [Outcome("payload", 422, {}), Outcome("auth", 401), Outcome("config", 404)],
)
async def test_commands_stops_at_a_4xx_the_same_request_would_only_get_again(
    monkeypatch: pytest.MonkeyPatch, failure: Outcome
) -> None:
    monkeypatch.setattr("thediscordlist._publishers.COMMANDS_DELAY", 0.0)
    monkeypatch.setattr("thediscordlist._publishers.COMMANDS_RETRY", 0.0)
    api = FakeApi(failure)
    publisher = CommandsPublisher(make_deps(FakeClient(commands=[{"name": "ping"}]), api))

    publisher.start()
    assert publisher._task is not None
    await asyncio.wait_for(publisher._task, 1)

    assert len(api.calls) == 1


async def test_a_manual_post_that_lands_ends_the_pending_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The retry would only post the same list again, against the budget stats share."""
    monkeypatch.setattr("thediscordlist._publishers.COMMANDS_DELAY", 0.0)
    monkeypatch.setattr("thediscordlist._publishers.COMMANDS_RETRY", 0.05)
    api = FakeApi(Outcome("transient", 502, error=RuntimeError("Bad Gateway")))
    publisher = CommandsPublisher(make_deps(FakeClient(commands=[{"name": "ping"}]), api))

    publisher.start()
    await asyncio.sleep(0.01)
    assert len(api.calls) == 1

    assert (await publisher.post()).ok
    await asyncio.sleep(0.1)

    assert len(api.calls) == 2


async def test_commands_stops_when_discord_lists_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("thediscordlist._publishers.COMMANDS_DELAY", 0.0)
    monkeypatch.setattr("thediscordlist._publishers.COMMANDS_RETRY", 0.0)
    client = FakeClient(commands=[])
    publisher = CommandsPublisher(make_deps(client, FakeApi()))

    publisher.start()
    assert publisher._task is not None
    await asyncio.wait_for(publisher._task, 1)

    assert len(client.http.calls) == 1


async def test_commands_reprobes_after_a_403_without_sending_until_the_suspension_ends() -> None:
    """The plan gate can lift mid-run, so a 403 keeps its 30-minute reprobe."""
    api = FakeApi(Outcome("plan", 403))
    publisher = CommandsPublisher(make_deps(FakeClient(commands=[{"name": "ping"}]), api))

    assert _worth_retrying(await publisher._publish())
    assert _worth_retrying(await publisher._publish())
    assert len(api.calls) == 1


# -- the error taxonomy, as publishers see it -------------------------------


async def test_a_401_disables_the_publisher_and_tells_the_owner() -> None:
    seen: list[str] = []
    client = FakeClient()
    api = FakeApi(Outcome("auth", 401))
    deps = make_deps(client, api)
    deps.on_auth_failure = lambda: seen.append("stopped")
    stats = StatsPublisher(deps)

    result = await stats.post()

    assert (result.ok, result.status) == (False, 401)
    assert stats._disabled == "unauthorized"
    assert seen == ["stopped"]


async def test_a_403_suspends_rather_than_disables(caplog: pytest.LogCaptureFixture) -> None:
    """Gold can be granted mid-run; needing a restart to notice is a support ticket."""
    stats = StatsPublisher(make_deps(FakeClient(), FakeApi(Outcome("plan", 403))))

    with caplog.at_level(logging.WARNING, logger="thediscordlist"):
        result = await stats.post()

    assert result.skipped == "plan"
    assert stats._disabled is None
    assert stats._suspended_until > 0
    assert "higher plan" in caplog.text


async def test_a_403_silences_the_heartbeat_until_the_reprobe() -> None:
    """A free-tier bot must not keep beating every 30s into a plan gate."""
    api = FakeApi(Outcome("plan", 403))
    heartbeat = HeartbeatPublisher(make_deps(FakeClient(), api))

    delay = await heartbeat._tick()

    assert delay >= PLAN_REPROBE - 1
    assert (await heartbeat._beat(manual=False)).skipped == "plan"
    assert len(api.calls) == 1

    heartbeat._suspended_until = 0.0
    assert (await heartbeat._beat(manual=False)).ok


async def test_a_404_never_reads_as_a_bad_token(caplog: pytest.LogCaptureFixture) -> None:
    stats = StatsPublisher(make_deps(FakeClient(), FakeApi(Outcome("config", 404))))

    with caplog.at_level(logging.ERROR, logger="thediscordlist"):
        await stats.post()

    assert stats._disabled == "not-found"
    assert "token" not in caplog.text
    assert "no bot listing found" in caplog.text


@pytest.mark.usefixtures("no_min_gap")
async def test_a_422_keeps_the_publisher_alive_and_is_reported_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    api = FakeApi(Outcome("payload", 422, {"message": "bad"}), Outcome("payload", 422, {}))
    stats = StatsPublisher(make_deps(FakeClient(), api))

    with caplog.at_level(logging.WARNING, logger="thediscordlist"):
        await stats.post()
        await stats.post()

    assert stats._disabled is None
    assert caplog.text.count("rejected the stats payload") == 1


@pytest.mark.usefixtures("no_min_gap")
async def test_a_manual_post_re_arms_a_disabled_publisher() -> None:
    api = FakeApi(Outcome("config", 404))
    stats = StatsPublisher(make_deps(FakeClient(), api))

    await stats.post()
    assert stats._disabled == "not-found"

    assert (await stats.post()).ok


@pytest.mark.usefixtures("no_min_gap")
async def test_a_manual_post_restarts_the_loop_a_disable_ended() -> None:
    """Without the loop, post_stats() works once and the count goes stale."""
    api = FakeApi(Outcome("config", 404))
    stats = StatsPublisher(make_deps(FakeClient(), api))

    await stats.post()
    assert stats._task is None

    await stats.post()
    assert stats._task is not None
    assert not stats._task.done()
    stats.stop()


@pytest.mark.usefixtures("no_min_gap")
async def test_a_manual_post_leaves_the_loop_off_when_it_disables_again() -> None:
    api = FakeApi(Outcome("config", 404), Outcome("config", 404))
    stats = StatsPublisher(make_deps(FakeClient(), api))

    await stats.post()
    await stats.post()

    assert stats._task is None


async def test_a_publisher_loop_survives_a_tick_that_raises(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Nothing in this package may take a bot down."""
    stats = StatsPublisher(make_deps(FakeClient(), FakeApi()))
    ticks = 0

    async def no_wait() -> None:
        return None

    async def exploding_tick() -> float:
        nonlocal ticks
        ticks += 1
        raise RuntimeError("boom")

    stats._prime = no_wait  # type: ignore[method-assign]
    stats._tick = exploding_tick  # type: ignore[method-assign]

    with caplog.at_level(logging.ERROR, logger="thediscordlist"):
        stats.start()
        for _ in range(10):
            await asyncio.sleep(0)
            if ticks:
                break

    assert ticks == 1
    assert stats._task is not None
    assert not stats._task.done()  # still looping
    assert "the stats publisher failed" in caplog.text
    stats.stop()
