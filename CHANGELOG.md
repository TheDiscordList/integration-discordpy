# Changelog

All notable changes to this package are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

The first release, to ship as 0.1.0.

### Added

- `autopost(client, token)`: publishes the server count, per-process shard
  heartbeats and the registered application commands to TheDiscordList, with
  topology detection for single, auto-sharded and multi-process bots.
- Support for **discord.py** (2.4+), **Pycord** (2.6+), **nextcord** (2.6+, 3.x
  on Python 3.12+) and **disnake** (2.9+), chosen with the `discordpy`,
  `pycord`, `nextcord` and `disnake` extras. The only hard dependency is
  aiohttp, so installing for Pycord never pulls discord.py in over it.
- `post_stats()`, `post_heartbeat()` and `post_commands()` for manual posts,
  returning a `PostResult` instead of raising.
- `thediscordlist.webhooks`: `verify_webhook()`, typed event payloads, and
  ready-made aiohttp and ASGI handlers.
