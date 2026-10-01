# thediscordlist

The [TheDiscordList](https://thediscordlist.com) integration for **discord.py**,
**Pycord**, **nextcord** and **disnake**.

One call keeps your listing up to date — server count, cluster heartbeats and
application commands publish themselves. A second entry point verifies the
webhooks TheDiscordList sends back.

```bash
pip install "thediscordlist[discordpy]"  # discord.py
pip install "thediscordlist[pycord]"     # Pycord (py-cord)
pip install "thediscordlist[nextcord]"   # nextcord
pip install "thediscordlist[disnake]"    # disnake
```

The extra only names the library you already use. A plain
`pip install thediscordlist` pulls in no Discord library at all (its one
dependency is aiohttp), which is all a webhook receiver needs and is safe next
to any pinned library.

## Supported libraries

| Library | Extra | Imports as | Tested with | `autopost` starts from |
| --- | --- | --- | --- | --- |
| [discord.py](https://github.com/Rapptz/discord.py) | `discordpy` | `discord` | 2.4 – 2.7 | `setup_hook` |
| [Pycord](https://github.com/Pycord-Development/pycord) | `pycord` | `discord` | 2.6 – 2.8 | the end of `login()` |
| [nextcord](https://github.com/nextcord/nextcord) | `nextcord` | `nextcord` | 2.6, 3.0 – 3.2 | the end of `login()` |
| [disnake](https://github.com/DisnakeDev/disnake) | `disnake` | `disnake` | 2.9 – 2.12 | the end of `login()` |

All four are forks of one codebase and keep the same names for everything this
package reads (`guilds`, `shard_ids`, `shards`, `latency`, `is_ready()`,
`http.get_global_commands()`, …), so the client is duck-typed rather than
special-cased. Any `Client`, `Bot`, `AutoShardedClient` or `AutoShardedBot` from
any of them works, as do your own subclasses.

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

The same `autopost(bot, token)` line works with a bot from any of the others:

```python
# Pycord
bot = discord.Bot(intents=discord.Intents.default())

# nextcord, with `from nextcord.ext import commands`
bot = commands.Bot(intents=nextcord.Intents.default())

# disnake, with `from disnake.ext import commands`
bot = commands.InteractionBot(intents=disnake.Intents.default())
```

That is the whole integration for most bots. `autopost` is safe to call before
or after the bot starts, and with no running event loop. With a falsy token it
logs a warning and does nothing — a forgotten environment variable must never
take down a deploy.

Get the token from your bot's manage page → **API** tab.

### Per-library notes

- **discord.py** — `autopost` chains onto `setup_hook`, which discord.py runs
  after login and before the gateway connects. Your own `setup_hook` still runs
  first.
- **Pycord, nextcord, disnake** — there is no `setup_hook`, and the application
  id only arrives with READY. `autopost` chains onto `login()` instead and makes
  one `client.application_info()` call right after it (the same call
  discord.py's own login makes). Pass `application_id=` to your client, or
  `bot_id=` to `autopost`, to skip it.
- **Pycord** shares the `discord` import name with discord.py, and the two
  packages overwrite each other's files. Install the `pycord` extra (or no
  extra), never `discordpy`. Pycord syncs its commands in its own `on_connect`;
  the commands publisher reads Discord's list ten seconds after READY, after
  that sync.
- **nextcord** 3 requires Python 3.12+. On 3.10 and 3.11 the extra resolves to
  nextcord 2.6, which imports the removed `pkg_resources`, so keep
  `setuptools<81` installed there. That is nextcord's constraint; this package
  passes the same suite on 2.6 and 3.x.

## What it publishes

| | Endpoint | When |
| --- | --- | --- |
| **Stats** | `POST /bots/{id}/stats` | the guild count is checked every 60s and posted when it moves, plus every 30 minutes regardless |
| **Heartbeat** | `POST /bots/{id}/heartbeat` | every ~30s per process, starting before the gateway connects so a booting bot reads *connecting* rather than *down* |
| **Commands** | `POST /bots/{id}/commands` | once, ~10s after ready — commands change on deploy, and a deploy is a restart |

The command list is read back from Discord (`GET /applications/{id}/commands`,
through your library's own HTTP client), so it is what is actually registered
rather than a local command tree, in Discord's own JSON shape whatever the
library.

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
    stats_interval=60.0,  # guild-count poll, and the hard floor between posts
    heartbeat_interval=30.0,  # starting cadence; retuned from the API's response
    post_empty_commands=False,  # an empty list CLEARS the listing
    cluster=None,  # (cluster_id, cluster_count) for multi-process bots
    server_count=None,  # callable returning the bot-wide guild total
)
```

`stats_interval` is clamped to a 60-second minimum: all three endpoints share
one server-side rate-limit counter, so a tighter loop only starves the
heartbeat.

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
| `Client` / `Bot` | fallthrough | cluster 0 of 1, one shard |
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
  total is a wrong number. None of the four libraries has a cross-process
  primitive to sum with, so the total has to come from you.

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
bad signature and `400` on a stale or malformed one:

```python
# aiohttp — already installed: it is this package's one dependency
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

`ctx.delivery` is stable across all five retries — use it as an idempotency
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

## Plans, limits and known behaviour

- **Heartbeats need Gold or higher.** On a `403` the heartbeat publisher
  suspends and reprobes every 30 minutes rather than disabling: the plan is
  checked per request, so a bot can gain it mid-run.
- **Rate limits are shared.** Stats and commands share 60 requests a minute per
  IP, heartbeats get 120. A `429` suspends stats and commands together for the
  `Retry-After` window; heartbeats bypass it, because a stale number beats a
  false "down" on your status page.
- **`404` never means "bad token".** It means the id is unknown or belongs to a
  server listing, and only that publisher is disabled. A `401` disables all
  three. A manual `post_*()` re-arms a disabled publisher.
- **An empty command list is never sent** unless `post_empty_commands=True`:
  posting `[]` clears the listing's commands.

## Requirements

Python 3.10+, and one of discord.py 2.4+, py-cord 2.6+, nextcord 2.6+ (3.x on
Python 3.12+) or disnake 2.9+ — or none, for webhook verification alone.

## Publishing (maintainers)

`.github/workflows/release.yml` publishes to PyPI when a `v*` tag is pushed,
through PyPI trusted publishing (OIDC): no API token lives in this repository.
One-time setup that CI cannot do:

1. On PyPI, open **Account settings → Publishing** and add a pending publisher:
   project `thediscordlist`, owner `TheDiscordList`, repository
   `integration-discordpy`, workflow `release.yml`, environment `pypi`. The
   first successful run creates the project.
2. In this repository's **Settings → Environments**, create an environment
   named `pypi`. Adding a required reviewer there turns every release into a
   one-click approval.

Each release:

1. Set `__version__` in `src/thediscordlist/__init__.py` — the build reads the
   version from there.
2. Move the `Unreleased` notes in `CHANGELOG.md` under that version.
3. Commit, then tag and push: `git tag v0.1.0 && git push origin main v0.1.0`.
   The tag must equal `__version__` with a `v` prefix; the workflow publishes
   whatever the build produces and does not compare the two.
4. The **Release** workflow runs `uv build` and uploads with
   `pypa/gh-action-pypi-publish`. Check
   [pypi.org/project/thediscordlist](https://pypi.org/project/thediscordlist/).

A version number can be uploaded only once. If a release goes wrong, yank it on
PyPI and tag the next patch version.

## Links

- [API reference](https://thediscordlist.com/docs/api)
- [Changelog](CHANGELOG.md)

## License

MIT — see [LICENSE](LICENSE).
