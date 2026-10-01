from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any

import pytest

from thediscordlist.webhooks import (
    MAX_BODY_BYTES,
    WebhookContext,
    WebhookError,
    asgi_webhook_app,
    verify_webhook,
)

SECRET = "whsec_test"
NOW = 1_786_712_208.0

BODY = json.dumps(
    {
        "id": "5b1f0a2e-1f4d-4a51-9f26-2f9a9c0e1b77",
        "event": "listing.upvoted",
        "created_at": "2026-08-14T12:34:56+00:00",
        "listing": {
            "id": "161660517914509312",
            "type": "bot",
            "name": "Test",
            "url": "https://thediscordlist.com/bots/161660517914509312",
        },
        "data": {"voted_at": "2026-08-14T12:34:56+00:00", "total_upvotes": 42, "user": None},
    }
)


def sign(body: str = BODY, t: int = int(NOW), secret: str = SECRET) -> str:
    digest = hmac.new(secret.encode(), f"{t}.{body}".encode(), hashlib.sha256).hexdigest()
    return f"t={t},v1={digest}"


def now() -> float:
    return NOW


def test_accepts_a_valid_signature() -> None:
    event = verify_webhook(BODY, sign(), SECRET, now=now)
    assert event["event"] == "listing.upvoted"
    assert event["data"]["total_upvotes"] == 42


def test_accepts_bytes() -> None:
    assert verify_webhook(BODY.encode(), sign(), SECRET, now=now)["id"]


def test_the_shared_sdk_vector() -> None:
    """Computed with the server's PHP formula; every TheDiscordList SDK asserts it."""
    body = (
        '{"id":"d1","event":"webhook.ping","created_at":"2023-11-14T22:13:20+00:00",'
        '"listing":{"id":"1","type":"bot","name":"Bot",'
        '"url":"https://thediscordlist.com/bots/1"},"data":{"message":"hi"}}'
    )
    header = "t=1700000000,v1=b6eeea31d04c3f86990dcbb706dfaf988c9ff2fc234316ae77e168831127dc59"

    assert sign(body, 1_700_000_000, "whsec_test") == header
    event = verify_webhook(body, header, "whsec_test", now=lambda: 1_700_000_000.0)
    assert event["event"] == "webhook.ping"


def test_accepts_an_uppercase_signature() -> None:
    t, v1 = sign().split(",v1=")
    assert verify_webhook(BODY, f"{t},v1={v1.upper()}", SECRET, now=now)


def test_a_header_part_with_extra_equals_signs_still_parses() -> None:
    """Parts split on the first `=` only, so a value containing `=` is not malformed."""
    assert verify_webhook(BODY, f"{sign()},v0=YWJj==", SECRET, now=now)


@pytest.mark.parametrize(
    ("body", "signature", "code"),
    [
        (BODY, sign(secret="whsec_other"), "INVALID_SIGNATURE"),
        (BODY.replace("42", "43"), sign(), "INVALID_SIGNATURE"),
        (BODY, sign(t=int(NOW) - 301), "STALE_TIMESTAMP"),
        (BODY, sign(t=int(NOW) + 301), "STALE_TIMESTAMP"),
        (BODY, "v1=deadbeef", "MALFORMED_SIGNATURE"),
        (BODY, f"t={int(NOW)}", "MALFORMED_SIGNATURE"),
        (BODY, f"t=not-a-number,v1={'a' * 64}", "MALFORMED_SIGNATURE"),
        (BODY, None, "MISSING_SIGNATURE"),
        (BODY, "", "MISSING_SIGNATURE"),
    ],
)
def test_rejects(body: str, signature: str | None, code: str) -> None:
    with pytest.raises(WebhookError) as caught:
        verify_webhook(body, signature, SECRET, now=now)
    assert caught.value.code == code


def test_invalid_hex_is_a_rejection_not_a_crash() -> None:
    """`v1=zz` decodes to nothing; comparing decoded bytes is how other SDKs die."""
    with pytest.raises(WebhookError) as caught:
        verify_webhook(BODY, f"t={int(NOW)},v1=zz", SECRET, now=now)
    assert caught.value.code == "INVALID_SIGNATURE"


def test_non_ascii_signature_is_a_rejection_not_a_crash() -> None:
    with pytest.raises(WebhookError) as caught:
        verify_webhook(BODY, f"t={int(NOW)},v1=éé", SECRET, now=now)
    assert caught.value.code == "INVALID_SIGNATURE"


def test_rejects_a_body_that_is_not_json() -> None:
    with pytest.raises(WebhookError) as caught:
        verify_webhook("not json", sign("not json"), SECRET, now=now)
    assert caught.value.code == "MALFORMED_BODY"


def test_signature_covers_the_exact_bytes() -> None:
    """Re-serialising escapes slashes differently, so it must not verify."""
    reserialised = json.dumps(json.loads(BODY), separators=(",", ":"))
    with pytest.raises(WebhookError):
        verify_webhook(reserialised, sign(), SECRET, now=now)


# -- the ASGI app -----------------------------------------------------------


async def call_asgi(
    app: Any, body: bytes, headers: dict[str, str], method: str = "POST"
) -> tuple[int, list[dict[str, Any]]]:
    scope = {
        "type": "http",
        "method": method,
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
    }
    received = [{"type": "http.request", "body": body, "more_body": False}]
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return received.pop(0)

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    await app(scope, receive, send)
    return sent[0]["status"], sent


async def test_asgi_accepts_a_signed_delivery() -> None:
    seen: list[tuple[Any, WebhookContext]] = []

    async def on_event(event: Any, ctx: WebhookContext) -> None:
        seen.append((event, ctx))

    app = asgi_webhook_app(SECRET, on_event, tolerance_seconds=10**9)
    status, _ = await call_asgi(
        app,
        BODY.encode(),
        {"x-tdl-signature": sign(), "x-tdl-delivery": "abc", "x-tdl-attempt": "3"},
    )

    assert status == 204
    assert seen[0][0]["event"] == "listing.upvoted"
    assert seen[0][1] == WebhookContext("abc", 3)


async def test_asgi_rejects_a_bad_signature_with_401() -> None:
    app = asgi_webhook_app(SECRET, lambda event, ctx: None, tolerance_seconds=10**9)
    status, _ = await call_asgi(app, BODY.encode(), {"x-tdl-signature": sign(secret="nope")})
    assert status == 401


async def test_asgi_rejects_a_stale_delivery_with_400() -> None:
    app = asgi_webhook_app(SECRET, lambda event, ctx: None, tolerance_seconds=0)
    status, _ = await call_asgi(app, BODY.encode(), {"x-tdl-signature": sign(t=1)})
    assert status == 400


async def test_asgi_answers_405_to_anything_but_post() -> None:
    app = asgi_webhook_app(SECRET, lambda event, ctx: None)
    status, _ = await call_asgi(app, b"", {}, method="GET")
    assert status == 405


async def test_asgi_returns_500_so_the_delivery_is_retried() -> None:
    def boom(event: Any, ctx: WebhookContext) -> None:
        raise RuntimeError("handler exploded")

    app = asgi_webhook_app(SECRET, boom, tolerance_seconds=10**9)
    status, _ = await call_asgi(app, BODY.encode(), {"x-tdl-signature": sign()})
    assert status == 500


async def test_asgi_returns_202_when_the_handler_overruns() -> None:
    import asyncio

    finished = asyncio.Event()

    async def slow(event: Any, ctx: WebhookContext) -> None:
        await asyncio.sleep(0.05)
        finished.set()

    app = asgi_webhook_app(SECRET, slow, tolerance_seconds=10**9, handler_timeout=0.001)
    status, _ = await call_asgi(app, BODY.encode(), {"x-tdl-signature": sign()})

    assert status == 202
    await asyncio.wait_for(finished.wait(), 1)  # the work carried on


async def test_asgi_refuses_an_oversized_body() -> None:
    app = asgi_webhook_app(SECRET, lambda event, ctx: None)
    status, _ = await call_asgi(app, b"x" * (MAX_BODY_BYTES + 1), {"x-tdl-signature": sign()})
    assert status == 413


async def test_asgi_handles_the_lifespan_protocol() -> None:
    app = asgi_webhook_app(SECRET, lambda event, ctx: None)
    messages = [{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}]
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return messages.pop(0)

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    await app({"type": "lifespan"}, receive, send)
    assert [m["type"] for m in sent] == [
        "lifespan.startup.complete",
        "lifespan.shutdown.complete",
    ]


def test_the_webhooks_module_needs_no_discord_library() -> None:
    """A webhook receiver on a plain web server must not need discord.py."""
    import subprocess
    import sys

    code = (
        "import sys, builtins;"
        "real = builtins.__import__;"
        "builtins.__import__ = lambda n, *a, **k: "
        "(_ for _ in ()).throw(AssertionError(n)) if n.split('.')[0] == 'discord'"
        " else real(n, *a, **k);"
        "import thediscordlist.webhooks as w;"
        "assert w.verify_webhook"
    )
    assert subprocess.run([sys.executable, "-c", code], capture_output=True).returncode == 0
