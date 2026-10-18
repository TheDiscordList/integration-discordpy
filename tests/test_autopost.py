from __future__ import annotations

import logging
from typing import Any

import pytest

from conftest import FakeApi, FakeClient
from thediscordlist import Autoposter, autopost

# Nothing in the suite may reach the real API; port 1 refuses instantly.
DEAD = "http://127.0.0.1:1/api"


async def test_a_missing_token_is_inert_rather_than_fatal(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A forgotten environment variable must never take down a deploy."""
    client = FakeClient()
    original_setup, original_close = client.setup_hook, client.close

    with caplog.at_level(logging.WARNING, logger="thediscordlist"):
        poster = autopost(client, None)

    assert isinstance(poster, Autoposter)
    assert "setup_hook" not in client.__dict__  # nothing was hooked
    assert "close" not in client.__dict__
    assert (original_setup, original_close) != (None, None)
    assert "no stats token" in caplog.text
    assert (await poster.post_stats()).skipped == "disabled"


@pytest.mark.parametrize("token", ["  tok\n", "Bearer tok", "bearer   tok "])
async def test_a_pasted_token_is_cleaned_up(token: str) -> None:
    client = FakeClient()
    poster = autopost(client, token, base_url=DEAD, commands=False)

    assert poster._api._headers["authorization"] == "Bearer tok"
    await client.close()


async def test_a_blank_token_is_treated_as_missing() -> None:
    client = FakeClient()
    poster = autopost(client, "   ")
    assert "setup_hook" not in client.__dict__
    assert (await poster.post_stats()).skipped == "disabled"


def test_an_unknown_option_is_a_type_error() -> None:
    with pytest.raises(TypeError, match="stat_interval"):
        autopost(FakeClient(), None, stat_interval=120)  # type: ignore[call-arg]


async def test_a_guild_change_wakes_stats_beside_the_users_own_handler() -> None:
    client = FakeClient()
    seen: list[Any] = []

    async def on_guild_join(guild: Any) -> None:
        seen.append(guild)

    client.on_guild_join = on_guild_join  # type: ignore[attr-defined]
    poster = Autoposter(client, FakeApi())  # type: ignore[arg-type]
    poster.start()
    assert poster._stats is not None

    await client.on_guild_join("guild")  # type: ignore[attr-defined]
    assert seen == ["guild"]
    assert poster._stats._changed.is_set()

    poster._stats._changed.clear()
    await client.on_guild_remove("guild")  # type: ignore[attr-defined]
    assert poster._stats._changed.is_set()
    await poster.close()


async def test_it_starts_immediately_when_the_client_is_already_logged_in() -> None:
    client = FakeClient()
    poster = autopost(client, "tok", base_url=DEAD, commands=False)

    assert poster.topology is not None
    assert poster.topology.kind == "single"

    await client.close()


async def test_it_waits_for_setup_hook_when_the_application_id_is_unknown() -> None:
    """setup_hook runs after login -- the first moment the application id exists."""
    client = FakeClient(application_id=None)
    user_hook_ran = False

    async def user_setup_hook() -> None:
        nonlocal user_hook_ran
        user_hook_ran = True
        client.application_id = 42  # login populated it

    client.setup_hook = user_setup_hook  # type: ignore[method-assign]
    poster = autopost(client, "tok", base_url=DEAD)

    assert poster.topology is None  # nothing started yet

    await client.setup_hook()

    assert user_hook_ran  # the user's own hook still ran
    assert poster.topology is not None

    await client.close()


async def test_closing_the_client_closes_the_poster() -> None:
    client = FakeClient()
    poster = autopost(client, "tok", base_url=DEAD)

    await client.close()

    assert client.closed_by_us  # the client's own close still ran
    assert poster._closed
    assert (await poster.post_stats()).skipped == "disabled"


async def test_an_unknown_application_id_disables_posting(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = FakeClient(application_id=None)
    poster = Autoposter(client, FakeApi())  # type: ignore[arg-type]

    with caplog.at_level(logging.ERROR, logger="thediscordlist"):
        poster.start()

    assert poster.topology is None
    assert "bot_id=" in caplog.text


async def test_a_missing_guilds_intent_is_warned_about(caplog: pytest.LogCaptureFixture) -> None:
    client = FakeClient()
    client.intents.guilds = False
    poster = Autoposter(client, FakeApi())  # type: ignore[arg-type]

    with caplog.at_level(logging.WARNING, logger="thediscordlist"):
        poster.start()

    assert "Guilds intent is off" in caplog.text
    await poster.close()


async def test_stats_is_off_when_one_process_cannot_see_every_guild(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A fraction of the guilds published as the total is worse than no number."""
    client = FakeClient(shard_count=8, shard_ids=[0, 1])
    poster = Autoposter(client, FakeApi(), cluster=(0, 4))  # type: ignore[arg-type]

    with caplog.at_level(logging.WARNING, logger="thediscordlist"):
        poster.start()

    assert poster._stats is None
    assert "server_count=" in caplog.text
    assert poster._heartbeat is not None  # the heartbeat still beats for this cluster
    await poster.close()


async def test_a_server_count_callback_keeps_stats_on() -> None:
    client = FakeClient(shard_count=8, shard_ids=[0, 1])
    poster = Autoposter(client, FakeApi(), cluster=(0, 4), server_count=lambda: 900)  # type: ignore[arg-type]
    poster.start()

    assert poster._stats is not None
    assert (await poster.post_stats()).ok
    await poster.close()


async def test_a_401_stops_every_publisher() -> None:
    from thediscordlist._http import Outcome

    api = FakeApi(Outcome("auth", 401))
    client = FakeClient()
    poster = Autoposter(client, api)  # type: ignore[arg-type]
    poster.start()

    await poster.post_stats()

    assert poster._heartbeat is not None
    assert poster._heartbeat._task is None
    assert poster._commands is not None
    assert poster._commands._task is None
    await poster.close()


async def test_publishers_can_be_switched_off_individually() -> None:
    client = FakeClient()
    poster = Autoposter(client, FakeApi(), heartbeat=False, commands=False)  # type: ignore[arg-type]
    poster.start()

    assert poster._stats is not None
    assert poster._heartbeat is None
    assert (await poster.post_heartbeat()).skipped == "disabled"
    assert (await poster.post_commands()).skipped == "disabled"
    await poster.close()
