"""The HTTP client, exercised against a real aiohttp server.

Faking `aiohttp` would test the fake; a loopback server tests the status codes,
the headers and the body draining for real, and costs about ten lines.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest
from aiohttp import web

from thediscordlist._http import ApiClient, backoff


class Server:
    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.responses: list[web.Response] = []
        self.base_url = ""

    def reply(self, *responses: web.Response) -> None:
        self.responses.extend(responses)

    async def _handle(self, request: web.Request) -> web.Response:
        self.requests.append(
            {
                "path": request.path,
                "body": await request.json(),
                "headers": dict(request.headers),
            }
        )
        return self.responses.pop(0) if self.responses else web.json_response({"ok": True})


@pytest.fixture
async def server() -> AsyncIterator[Server]:
    fake = Server()
    app = web.Application()
    app.router.add_post("/api/{tail:.*}", fake._handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]
    fake.base_url = f"http://127.0.0.1:{port}/api"
    yield fake
    await runner.cleanup()


@pytest.fixture
def make_client(server: Server) -> Callable[[], ApiClient]:
    return lambda: ApiClient("tok", server.base_url, "tests/1.0")


async def test_a_successful_post_sends_the_token_and_parses_the_body(
    server: Server, make_client: Callable[[], ApiClient]
) -> None:
    server.reply(web.json_response({"message": "Stats updated.", "server_count": "7"}))
    client = make_client()

    outcome = await client.post("/bots/1/stats", {"server_count": 7, "shard_count": 1})
    await client.close()

    assert outcome.kind == "ok"
    assert outcome.status == 200
    assert outcome.body == {"message": "Stats updated.", "server_count": "7"}
    assert server.requests[0]["headers"]["Authorization"] == "Bearer tok"
    assert server.requests[0]["headers"]["User-Agent"] == "tests/1.0"
    assert server.requests[0]["body"] == {"server_count": 7, "shard_count": 1}


@pytest.mark.parametrize(
    ("status", "kind"),
    [
        (204, "ok"),
        (400, "payload"),
        (401, "auth"),
        (403, "plan"),
        (404, "config"),
        (405, "payload"),
        (408, "transient"),
        (409, "config"),
        (413, "payload"),
        (422, "payload"),
        (425, "transient"),
        (500, "transient"),
        (503, "transient"),
    ],
)
async def test_status_codes_map_to_the_error_taxonomy(
    server: Server, make_client: Callable[[], ApiClient], status: int, kind: str
) -> None:
    server.reply(*[web.json_response({"message": ""}, status=status) for _ in range(3)])
    client = make_client()

    # A tiny next_tick keeps the transient rows from actually sleeping a backoff.
    outcome = await client.post("/bots/1/stats", {}, next_tick=0.0001)
    await client.close()

    assert outcome.kind == kind


async def test_transient_failures_are_retried_three_times(
    server: Server, make_client: Callable[[], ApiClient]
) -> None:
    server.reply(
        web.json_response({}, status=503),
        web.json_response({}, status=503),
        web.json_response({"ok": True}),
    )
    client = make_client()

    # next_tick is generous, so no attempt is dropped for being too slow.
    outcome = await client.post("/bots/1/stats", {}, next_tick=600)
    await client.close()

    assert outcome.kind == "ok"
    assert len(server.requests) == 3


async def test_a_client_error_is_never_retried(
    server: Server, make_client: Callable[[], ApiClient]
) -> None:
    server.reply(web.json_response({}, status=413), web.json_response({"ok": True}))
    client = make_client()

    outcome = await client.post("/bots/1/stats", {}, next_tick=600)
    await client.close()

    assert outcome.kind == "payload"
    assert len(server.requests) == 1


async def test_a_retry_is_dropped_when_the_next_tick_arrives_first(
    server: Server, make_client: Callable[[], ApiClient]
) -> None:
    """The next tick carries fresher data, and a failed request still burns budget."""
    server.reply(*[web.json_response({}, status=503) for _ in range(3)])
    client = make_client()

    outcome = await client.post("/bots/1/stats", {}, next_tick=0.0001)
    await client.close()

    assert outcome.kind == "transient"
    assert len(server.requests) == 1


async def test_a_429_suspends_normal_traffic_but_never_heartbeats(
    server: Server, make_client: Callable[[], ApiClient]
) -> None:
    """One counter is shared server-side, and liveness outranks a stale number."""
    server.reply(
        web.json_response({}, status=429, headers={"retry-after": "30"}),
        web.json_response({"ok": True}),
    )
    client = make_client()

    throttled = await client.post("/bots/1/stats", {}, next_tick=1)
    assert throttled.kind == "throttled"
    assert throttled.retry_after == 30

    dropped = await client.post("/bots/1/stats", {})
    assert dropped.kind == "dropped"

    beat = await client.post("/bots/1/heartbeat", {}, heartbeat=True)
    assert beat.kind == "ok"

    await client.close()
    assert len(server.requests) == 2  # the dropped post never left the process


async def test_a_connection_failure_is_transient_not_a_crash() -> None:
    client = ApiClient("tok", "http://127.0.0.1:1/api", "tests/1.0")
    outcome = await client.post("/bots/1/stats", {}, next_tick=0.0001)
    await client.close()
    assert outcome.kind == "transient"
    assert outcome.error is not None


async def test_a_non_json_body_does_not_raise(
    server: Server, make_client: Callable[[], ApiClient]
) -> None:
    server.reply(web.Response(text="<html>502 Bad Gateway</html>", status=502))
    client = make_client()
    outcome = await client.post("/bots/1/stats", {}, next_tick=0.0001)
    await client.close()
    assert outcome.kind == "transient"


async def test_the_session_can_be_closed_twice(
    server: Server, make_client: Callable[[], ApiClient]
) -> None:
    client = make_client()
    await client.post("/bots/1/stats", {})
    await client.close()
    await client.close()


def test_backoff_grows_and_is_jittered_into_the_upper_half() -> None:
    assert backoff(0, lambda: 0.0) == pytest.approx(0.5)
    assert backoff(0, lambda: 1.0) == pytest.approx(1.0)
    assert backoff(3, lambda: 1.0) == pytest.approx(8.0)
    assert backoff(20, lambda: 1.0) == pytest.approx(30.0)  # capped


async def test_a_429_reset_header_beats_a_short_retry_after(
    server: Server, make_client: Callable[[], ApiClient]
) -> None:
    """Laravel sends an absolute unix reset; the later of the two wins."""
    server.reply(
        web.json_response(
            {},
            status=429,
            headers={"retry-after": "1", "x-ratelimit-reset": str(int(time.time()) + 120)},
        )
    )
    client = make_client()

    await client.post("/bots/1/stats", {}, next_tick=1)
    remaining = client._rate_limited_until - time.monotonic()
    await client.close()

    assert 110 < remaining < 125


async def test_a_429_is_retried_once_its_window_lifts_before_the_next_tick(
    server: Server, make_client: Callable[[], ApiClient], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("thediscordlist._http.random.uniform", lambda a, b: 0.0)
    server.reply(web.json_response({}, status=429, headers={"retry-after": "0.05"}))
    client = make_client()

    outcome = await client.post("/bots/1/stats", {}, next_tick=600)
    await client.close()

    assert outcome.kind == "ok"
    assert len(server.requests) == 2


async def test_a_429_never_suspends_for_more_than_an_hour(
    server: Server, make_client: Callable[[], ApiClient]
) -> None:
    server.reply(
        web.json_response(
            {},
            status=429,
            headers={"retry-after": "86400", "x-ratelimit-reset": str(int(time.time()) + 86400)},
        )
    )
    client = make_client()

    await client.post("/bots/1/stats", {}, next_tick=1)
    remaining = client._rate_limited_until - time.monotonic()
    await client.close()

    assert 3590 < remaining <= 3602
