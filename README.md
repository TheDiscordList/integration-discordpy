# thediscordlist

The [TheDiscordList](https://thediscordlist.com) integration for **discord.py**.

One call keeps your listing up to date — server count, cluster heartbeats and
application commands publish themselves. A second entry point verifies the
webhooks TheDiscordList sends back.

```bash
pip install thediscordlist
```

## Quickstart

```python
import os

import discord
from discord.ext import commands
from thediscordlist import autopost

bot = commands.Bot(command_prefix="!", intents=discord.Intents.default())

autopost(bot, os.environ["TDL_TOKEN"])

bot.run(os.environ["DISCORD_TOKEN"])
```

That is the whole integration for most bots. `autopost` is safe to call before
or after the bot starts, and with no running event loop. With a falsy token it
logs a warning and does nothing — a forgotten environment variable must never
take down a deploy.

Get the token from your bot's manage page → **API** tab.

## What it publishes

| | Endpoint | When |
| --- | --- | --- |
| **Stats** | `POST /bots/{id}/stats` | the guild count is checked every 5 minutes and shortly after a guild join or removal, and posted when it moves, plus every 30 minutes regardless |
| **Heartbeat** | `POST /bots/{id}/heartbeat` | every ~30s per process, starting before the gateway connects so a booting bot reads *connecting* rather than *down* |
| **Commands** | `POST /bots/{id}/commands` | once, ~10s after ready — commands change on deploy, and a deploy is a restart |

Nothing here can raise into your bot. Every loop swallows its own exceptions,
every `post_*()` returns a `PostResult` instead of raising, and problems are
reported through the standard `logging` module under the `thediscordlist`
logger.

## Options

```python
poster = autopost(
    bot,
    token,
    bot_id=None,  # Discord application id; defaults to bot.application_id
    base_url="https://thediscordlist.com/api",
    stats=True,
    heartbeat=True,
    commands=True,
    stats_interval=300.0,  # guild-count check; joins and removals also post
    heartbeat_interval=30.0,  # starting cadence; retuned from the API's response
    post_empty_commands=False,  # an empty list CLEARS the listing
    cluster=None,  # (cluster_id, cluster_count) for multi-process bots
    server_count=None,  # callable returning the bot-wide guild total
)
```

`stats_interval` is clamped to a 60-second minimum, which is also the floor
between two posts: all three endpoints share one server-side rate-limit
counter, so a tighter loop only starves the heartbeat.

The token may be pasted with surrounding whitespace or a leading `Bearer `.

Manual posts, for the rare cases the loops do not cover:

```python
result = await poster.post_stats()  # PostResult(ok=True, status=200, skipped=None)
await poster.post_commands()  # after re-registering commands in-process
await poster.post_heartbeat()
```

A falsy `result.ok` with a `result.skipped` reason (`"unchanged"`, `"min-gap"`,
`"plan"`, …) means nothing was sent, which is usually correct rather than a
failure.

## Sharding and clustering

Everything is detected from the client, so a single process — sharded or not —
needs no configuration at all:

| Topology | Detected from | Behaviour |
| --- | --- | --- |
| `discord.Client` / `commands.Bot` | fallthrough | cluster 0 of 1, one shard |
| `AutoShardedClient` / `AutoShardedBot` | `client.shards` | cluster 0 of 1, every shard reported |
| shards split across processes | `shard_ids` ⊂ `shard_count` | see below |

**Split across processes**, each process is a *cluster* and must say so:

```python
autopost(
    bot,
    token,
    cluster=(0, 4),  # this is cluster 0 of 4
    server_count=lambda: my_redis_total(),  # the bot-wide guild total
)
```

or set `TDL_CLUSTER` and `TDL_CLUSTER_COUNT` in each process's environment.

Two things are deliberately switched off when a process cannot work them out:

- **Heartbeats stop** without a `cluster_count`. Every beat hard-deletes the
  listing's cluster rows numbered at or above the `cluster_count` in that
  request, along with their monitoring history — so a single process guessing
  `1` would wipe all of its siblings. Silence costs a status page; a wrong
  count destroys data.
- **Stats stop** without a `server_count` callback, because a process holding
  some of the shards only sees some of the guilds, and publishing that as the
  total is a wrong number.

Each process beats for **itself**. That is the point of the endpoint: a manager
process reporting "all fine" for a cluster whose host has died is the failure
mode it exists to catch.

## Webhooks

`thediscordlist.webhooks` never imports a Discord library, and `verify_webhook`
needs only the standard library, so it also works in a web app that never
touches Discord.

```python
from thediscordlist.webhooks import verify_webhook

event = verify_webhook(raw_body, request.headers.get("X-TDL-Signature"), SECRET)
```

`raw_body` must be the exact bytes that arrived. TheDiscordList signs the JSON
string it sends; re-serialising a parsed body escapes slashes and non-ASCII
differently and will never verify.

Two ready-made servers, both answering `204` on success, `202` if your handler
overruns 8 seconds, `500` if it raises (so the delivery is retried), `401` on a
missing or bad signature and `400` on a stale or malformed one. Both, and
`verify_webhook`, raise `ValueError` for an empty secret:

```python
# aiohttp — already installed, because discord.py depends on it
from aiohttp import web
from thediscordlist.webhooks import aiohttp_webhook_handler

app = web.Application()
app.router.add_post("/tdl", aiohttp_webhook_handler(SECRET, on_event))
```

```python
# FastAPI / Starlette / Litestar / Quart — any ASGI host
from thediscordlist.webhooks import asgi_webhook_app

app.mount("/tdl", asgi_webhook_app(SECRET, on_event))
```

Mount the ASGI app rather than reading the body in your own route: a framework
that has already parsed the JSON has destroyed the bytes the signature covers.

```python
async def on_event(event, ctx):
    if event["event"] == "listing.upvoted":
        user = event["data"]["user"]
        await bot.get_channel(CHANNEL).send(f"{user['username']} voted! 💜")
```

`ctx.delivery` is stable across all five attempts — use it as an idempotency
key, for example `redis.set(ctx.delivery, 1, ex=86400, nx=True)`. A manual
"redeliver" from the web UI deliberately mints a new id, so it is not dropped
as a duplicate.

**Always return without raising for event names you do not recognise**, and
never special-case `webhook.ping` into a failure: a disabled endpoint is probed
with a ping every 6 hours for 7 days, and a 2xx to that probe is what revives
it. Ten consecutive failed deliveries disable an endpoint.

### Events

| Event | Fires for | `data` |
| --- | --- | --- |
| `listing.bumped` | servers, bots | `points`, `bumped_at`, `next_bump_at`, `user` |
| `listing.bump_ready` | servers, bots | `ready_at` |
| `listing.upvoted` | servers, bots | `voted_at`, `total_upvotes`, `user` |
| `server.joined` | servers | `joined_at`, `user` |
| `bot.added` | bots | `added_at`, `user` |
| `bot.stats_reported` | bots | `guild_count`, `shard_count`, `reported_at` |
| `webhook.ping` | both | `message` |

Every payload carries `id`, `event`, `created_at`, `listing` and `data`, and is
typed — `ListingUpvoted`, `BotAdded` and friends are exported as `TypedDict`s
that narrow on `event["event"]`.

`bot.added` and `server.joined` are **click** events, dispatched on the install
hand-off; Discord never confirms completion. `bot.stats_reported` is **not** an
echo of your own post — it also fires when the owner edits the listing by hand.
`created_at` is enqueue time, not send time; never use it for freshness.

## Logging

```python
import logging

logging.getLogger("thediscordlist").setLevel(logging.INFO)
```

`INFO` reports the detected topology once at startup. `WARNING` and `ERROR`
carry the things worth acting on: a rejected token, a missing plan, a
`cluster_count` that could not be determined.

## Requirements

Python 3.10+, discord.py 2.4+.

## Links

- [API reference](https://thediscordlist.com/docs/api)
- [MIT licence](LICENSE)
