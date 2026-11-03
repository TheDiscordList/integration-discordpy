# thediscordlist

## 0.2.1

### Patch Changes

- ece4b51: An auto-sharded bot no longer reports a reconnecting shard as `disconnected`, which opened a public Down incident on its status page. Only a closed client reports it now, and a shard not launched yet or mid-reconnect reports `connecting`.
- 5ddceee: A shard that is not ready leaves `guilds` out of its heartbeat entry, so a restart's first beat can no longer zero the public server count.
- 8e63a1c: `AutoShardedBot` reports its real `shard_count` instead of 1, which was read in `setup_hook` before discord.py knew it.
- b70fde9: A commands post that fails on a `5xx`, a network error, a rate limit or a Discord fetch tries again every 5 minutes until it lands or gets a `4xx` retrying cannot fix.
- 4f5efe5: `server_count` only runs on the elected publisher, so a large bot no longer exhausts its rate limit at startup with every cluster posting at once.
- 908ea64: A heartbeat `429` no longer suspends stats and commands, which have a limit of their own.
- 0f563e4: After a `404`, a successful `post_stats()` or `post_heartbeat()` restarts its publisher loop, instead of working once and going stale.
- cf76862: A shard state change sends a heartbeat soon after, at least 10 seconds after the previous one, as the other SDKs do.
- f54d12f: A response body that is not UTF-8 is a transient failure rather than an exception, a webhook timestamp too large for a float is rejected as stale, and `asgi_webhook_app` no longer raises on a header that is not UTF-8.

## 0.2.0

### Minor Changes

- 373eaa0: Stats check the guild count every five minutes and post 20 seconds after the last guild join or removal, instead of polling every minute. The heartbeat honours a 403: it stays quiet until the 30-minute reprobe instead of beating into the plan gate every 30 seconds, and a configured `heartbeat_interval` is clamped to 15–60 seconds from the start.
- 09ed045: `verify_webhook` and both webhook handlers raise `ValueError` for an empty secret instead of verifying against an empty key. Adds the `listing.bump_ready` event, typed as `ListingBumpReady` with `data.ready_at`.

### Patch Changes

- bcee69f: Only network errors, 408, 425, 429 and 5xx are retried; any other 4xx is reported once and not resent. A 429 is retried once its window lifts, if that comes before the next tick, and no rate-limit wait lasts longer than an hour.
- 913214a: A token pasted with surrounding whitespace or a leading `Bearer ` is accepted.

## 0.1.0

### Minor Changes

- 466ed33: Initial release: `autopost()` publishes server counts, cluster heartbeats and
  application commands from any discord.py client, and `thediscordlist.webhooks`
  verifies and handles inbound webhooks.
