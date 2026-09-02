"""Receive and verify TheDiscordList's outbound webhooks.

Nothing here imports a Discord library, and :func:`verify_webhook` needs only
the standard library, so this module also works in a plain web app that never
touches Discord.

.. code-block:: python

    from thediscordlist.webhooks import verify_webhook

    event = verify_webhook(raw_body, request.headers["X-TDL-Signature"], SECRET)
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from inspect import isawaitable
from typing import Any, Literal, TypedDict

log = logging.getLogger("thediscordlist")

SIGNATURE_HEADER = "x-tdl-signature"
DELIVERY_HEADER = "x-tdl-delivery"
ATTEMPT_HEADER = "x-tdl-attempt"

DEFAULT_TOLERANCE_SECONDS = 300
#: The sender gives up on a delivery after 10 seconds; answer well inside that.
DEFAULT_HANDLER_TIMEOUT = 8.0
MAX_BODY_BYTES = 1_048_576

__all__ = [
    "ATTEMPT_HEADER",
    "DEFAULT_HANDLER_TIMEOUT",
    "DEFAULT_TOLERANCE_SECONDS",
    "DELIVERY_HEADER",
    "MAX_BODY_BYTES",
    "SIGNATURE_HEADER",
    "BotAdded",
    "BotStatsReported",
    "ListingBumped",
    "ListingUpvoted",
    "ServerJoined",
    "WebhookContext",
    "WebhookError",
    "WebhookEventName",
    "WebhookListing",
    "WebhookPayload",
    "WebhookPing",
    "WebhookUser",
    "aiohttp_webhook_handler",
    "asgi_webhook_app",
    "verify_webhook",
]

# -- payload shapes ---------------------------------------------------------

#: ``Carbon::toIso8601String()`` -- ``"2026-08-14T12:34:56+00:00"``. Offset form,
#: never ``Z``, never fractional, never a unix integer.
IsoDate = str

WebhookEventName = Literal[
    "listing.bumped",
    "listing.upvoted",
    "server.joined",
    "bot.added",
    "bot.stats_reported",
    "webhook.ping",
]


class WebhookUser(TypedDict):
    #: Nullable even inside a present user object.
    id: str | None
    #: Snapshotted at last sign-in -- not a live Discord handle.
    username: str
    #: A full CDN URL, not a bare avatar hash.
    avatar: str | None


class WebhookListing(TypedDict):
    #: Guild id for a server, application id for a bot. Always a string.
    id: str
    type: Literal["server", "bot"]
    name: str
    url: str


class _Envelope(TypedDict):
    #: UUID, equal to ``X-TDL-Delivery`` and stable across all 5 retries.
    id: str
    #: Enqueue time, NOT send time. Do not use it for freshness.
    created_at: IsoDate
    listing: WebhookListing


class _BumpedData(TypedDict):
    points: int
    bumped_at: IsoDate
    next_bump_at: IsoDate
    user: WebhookUser | None


class ListingBumped(_Envelope):
    event: Literal["listing.bumped"]
    data: _BumpedData


class _UpvotedData(TypedDict):
    voted_at: IsoDate
    total_upvotes: int
    user: WebhookUser | None


class ListingUpvoted(_Envelope):
    event: Literal["listing.upvoted"]
    data: _UpvotedData


class _JoinedData(TypedDict):
    joined_at: IsoDate
    user: WebhookUser | None


class ServerJoined(_Envelope):
    event: Literal["server.joined"]
    data: _JoinedData


class _AddedData(TypedDict):
    added_at: IsoDate
    user: WebhookUser | None


class BotAdded(_Envelope):
    event: Literal["bot.added"]
    data: _AddedData


class _StatsData(TypedDict):
    guild_count: int | None
    shard_count: int | None
    reported_at: IsoDate


class BotStatsReported(_Envelope):
    event: Literal["bot.stats_reported"]
    data: _StatsData


class _PingData(TypedDict):
    message: str


class WebhookPing(_Envelope):
    event: Literal["webhook.ping"]
    data: _PingData


#: A bot listing only ever receives ``listing.bumped``, ``listing.upvoted``,
#: ``bot.added``, ``bot.stats_reported`` and ``webhook.ping``.
WebhookPayload = (
    ListingBumped | ListingUpvoted | ServerJoined | BotAdded | BotStatsReported | WebhookPing
)

ErrorCode = Literal[
    "MISSING_SIGNATURE",
    "MALFORMED_SIGNATURE",
    "STALE_TIMESTAMP",
    "INVALID_SIGNATURE",
    "MALFORMED_BODY",
]


class WebhookError(Exception):
    """Raised by :func:`verify_webhook`, and by nothing else in this module."""

    def __init__(self, code: ErrorCode, message: str) -> None:
        super().__init__(message)
        self.code: ErrorCode = code


@dataclass(frozen=True, slots=True)
class WebhookContext:
    #: ``X-TDL-Delivery``. Stable across retries -- use it as an idempotency key.
    delivery: str | None
    #: 1-based.
    attempt: int


# -- verification -----------------------------------------------------------


def verify_webhook(
    raw_body: str | bytes,
    signature: str | None,
    secret: str,
    *,
    tolerance_seconds: int = DEFAULT_TOLERANCE_SECONDS,
    now: Callable[[], float] = time.time,
) -> WebhookPayload:
    """Verifies ``X-TDL-Signature`` and returns the parsed envelope.

    ``raw_body`` must be the exact bytes that arrived. The sender signs the
    output of one ``json_encode($payload, JSON_UNESCAPED_SLASHES |
    JSON_UNESCAPED_UNICODE)`` and posts that same string, so re-serialising a
    parsed body escapes slashes and non-ASCII differently and can never verify.

    :raises WebhookError: and nothing else.
    """
    if not signature:
        raise WebhookError("MISSING_SIGNATURE", f"Missing {SIGNATURE_HEADER} header.")

    # Split on the first '=' only: a value may legitimately contain one.
    timestamp: int | None = None
    provided: str | None = None
    for part in signature.split(","):
        key, sep, value = part.strip().partition("=")
        if not sep:
            continue
        if key == "t" and timestamp is None:
            try:
                timestamp = int(value)
            except ValueError as error:
                raise WebhookError(
                    "MALFORMED_SIGNATURE", f"Malformed {SIGNATURE_HEADER} header."
                ) from error
        # Exactly one v1 value, ever: one secret column, no rotation grace period.
        elif key == "v1" and provided is None and value:
            provided = value

    if timestamp is None or provided is None:
        raise WebhookError("MALFORMED_SIGNATURE", f"Malformed {SIGNATURE_HEADER} header.")

    # Two-sided, so a sender skewed into the future is caught too. Checked against
    # `t`, never `created_at` -- that is stamped at enqueue time and can be over
    # an hour older on a retry.
    if abs(now() - timestamp) > tolerance_seconds:
        raise WebhookError("STALE_TIMESTAMP", "Webhook timestamp outside the tolerance window.")

    raw = raw_body.encode() if isinstance(raw_body, str) else bytes(raw_body)
    expected = hmac.new(secret.encode(), b"%d." % timestamp + raw, hashlib.sha256).hexdigest()

    # Compare the hex strings as bytes. Decoding the signature as hex first would
    # silently truncate an invalid value, and comparing the shortened result is
    # how a `v1=zz` body turns into a crash in other SDKs.
    if not hmac.compare_digest(provided.lower().encode(), expected.encode()):
        raise WebhookError("INVALID_SIGNATURE", "Webhook signature did not match.")

    try:
        payload: Any = json.loads(raw)
    except ValueError as error:
        raise WebhookError("MALFORMED_BODY", "Webhook body was not valid JSON.") from error
    if not isinstance(payload, dict):
        raise WebhookError("MALFORMED_BODY", "Webhook body was not a JSON object.")
    return payload  # type: ignore[return-value]


# -- handlers ---------------------------------------------------------------

EventHandler = Callable[[Any, WebhookContext], Awaitable[None] | None]


@dataclass(frozen=True, slots=True)
class _Options:
    secret: str
    on_event: EventHandler
    tolerance_seconds: int = DEFAULT_TOLERANCE_SECONDS
    handler_timeout: float = DEFAULT_HANDLER_TIMEOUT


def _context(headers: Mapping[str, str]) -> WebhookContext:
    try:
        attempt = max(int(headers.get(ATTEMPT_HEADER, "1")), 1)
    except ValueError:
        attempt = 1
    return WebhookContext(headers.get(DELIVERY_HEADER), attempt)


async def _dispatch(options: _Options, raw: bytes, headers: Mapping[str, str]) -> int:
    """Verifies, runs the handler and returns the HTTP status to answer with.

    ``202`` when the handler overran its budget: the sender disables an endpoint
    after 10 consecutive failed deliveries, so a slow handler must still ack
    while its work carries on in the background.
    """
    if len(raw) > MAX_BODY_BYTES:
        return 413

    try:
        event = verify_webhook(
            raw,
            headers.get(SIGNATURE_HEADER),
            options.secret,
            tolerance_seconds=options.tolerance_seconds,
        )
    except WebhookError as error:
        log.warning("[thediscordlist] rejected a webhook delivery: %s", error)
        return 401 if error.code in ("MISSING_SIGNATURE", "INVALID_SIGNATURE") else 400

    async def run() -> None:
        result = options.on_event(event, _context(headers))
        if isawaitable(result):
            await result

    task = asyncio.create_task(run())
    done, _ = await asyncio.wait({task}, timeout=options.handler_timeout)
    if not done:
        # Never abandon an exception: log it whenever it eventually lands.
        task.add_done_callback(_log_late_failure)
        return 202
    try:
        task.result()
    except Exception:
        log.exception("[thediscordlist] the webhook handler raised; the delivery will be retried")
        return 500
    return 204


def _log_late_failure(task: asyncio.Task[None]) -> None:
    if not task.cancelled() and task.exception() is not None:
        log.error(
            "[thediscordlist] the webhook handler raised after responding",
            exc_info=task.exception(),
        )


def aiohttp_webhook_handler(
    secret: str,
    on_event: EventHandler,
    *,
    tolerance_seconds: int = DEFAULT_TOLERANCE_SECONDS,
    handler_timeout: float = DEFAULT_HANDLER_TIMEOUT,
) -> Callable[[Any], Awaitable[Any]]:
    """An ``aiohttp.web`` route handler. aiohttp already ships with discord.py.

    .. code-block:: python

        app = web.Application()
        app.router.add_post("/tdl", aiohttp_webhook_handler(SECRET, on_event))
    """
    from aiohttp import web

    options = _Options(secret, on_event, tolerance_seconds, handler_timeout)

    async def handler(request: Any) -> Any:
        raw = await request.read()
        headers = {key.lower(): value for key, value in request.headers.items()}
        return web.Response(status=await _dispatch(options, raw, headers))

    return handler


def asgi_webhook_app(
    secret: str,
    on_event: EventHandler,
    *,
    tolerance_seconds: int = DEFAULT_TOLERANCE_SECONDS,
    handler_timeout: float = DEFAULT_HANDLER_TIMEOUT,
) -> Callable[[Any, Any, Any], Awaitable[None]]:
    """A bare ASGI app: FastAPI, Starlette, Litestar, Quart, uvicorn, hypercorn.

    .. code-block:: python

        app.mount("/tdl", asgi_webhook_app(SECRET, on_event))

    Mount it rather than reading the body in your own route -- a framework that
    has already parsed the JSON has destroyed the exact bytes the signature
    covers.
    """
    options = _Options(secret, on_event, tolerance_seconds, handler_timeout)

    async def app(scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] == "lifespan":
            while True:
                message = await receive()
                if message["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                elif message["type"] == "lifespan.shutdown":
                    await send({"type": "lifespan.shutdown.complete"})
                    return
            return

        if scope["type"] != "http":  # pragma: no cover - websockets are not ours
            return

        if scope["method"] != "POST":
            await _send_status(send, 405)
            return

        raw = bytearray()
        more = True
        while more:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            raw.extend(message.get("body", b""))
            more = message.get("more_body", False)
            if len(raw) > MAX_BODY_BYTES:
                await _send_status(send, 413)
                return

        headers = {key.decode().lower(): value.decode() for key, value in scope["headers"]}
        await _send_status(send, await _dispatch(options, bytes(raw), headers))

    return app


async def _send_status(send: Any, status: int) -> None:
    await send({"type": "http.response.start", "status": status, "headers": []})
    await send({"type": "http.response.body", "body": b""})
