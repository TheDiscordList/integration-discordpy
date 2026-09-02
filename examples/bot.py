"""A complete integration: auto-posting plus a webhook listener in one process.

    pip install thediscordlist
    TDL_TOKEN=... TDL_WEBHOOK_SECRET=... DISCORD_TOKEN=... python examples/bot.py

The webhook server uses aiohttp, which discord.py already depends on, so this
whole file needs nothing beyond the package itself. Point your listing's
Webhooks tab at `http://<your host>:8080/tdl`.
"""

from __future__ import annotations

import logging
import os

import discord
from aiohttp import web
from discord.ext import commands

from thediscordlist import autopost
from thediscordlist.webhooks import WebhookContext, aiohttp_webhook_handler

logging.basicConfig(level=logging.INFO)

ANNOUNCE_CHANNEL = int(os.environ.get("ANNOUNCE_CHANNEL", "0"))

bot = commands.Bot(command_prefix="!", intents=discord.Intents.default())


@bot.event
async def on_ready() -> None:
    print(f"logged in as {bot.user} in {len(bot.guilds)} servers")


# Stats, heartbeats and application commands now publish themselves.
poster = autopost(bot, os.environ["TDL_TOKEN"])


async def on_tdl_event(event: dict, ctx: WebhookContext) -> None:
    """Anything that raises here answers 500 and the delivery is retried.

    `ctx.delivery` is stable across all five retries, so it is the right
    idempotency key if you write to a database.
    """
    if event["event"] == "listing.upvoted":
        user = event["data"]["user"]
        name = user["username"] if user else "someone"
        channel = bot.get_channel(ANNOUNCE_CHANNEL)
        if isinstance(channel, discord.abc.Messageable):
            await channel.send(f"{name} just voted — thank you! 💜")

    # Everything else, including webhook.ping, falls through to a 204. Never
    # raise on an unknown event: ten failed deliveries disable the endpoint.


async def serve_webhooks() -> None:
    app = web.Application()
    app.router.add_post(
        "/tdl", aiohttp_webhook_handler(os.environ["TDL_WEBHOOK_SECRET"], on_tdl_event)
    )
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", 8080).start()  # noqa: S104


# setup_hook is the right place for background work: it runs after login and
# before the gateway connects. autopost() has already chained itself onto it.
original_setup_hook = bot.setup_hook


async def setup_hook() -> None:
    await original_setup_hook()
    await serve_webhooks()


bot.setup_hook = setup_hook  # type: ignore[method-assign]

bot.run(os.environ["DISCORD_TOKEN"])
