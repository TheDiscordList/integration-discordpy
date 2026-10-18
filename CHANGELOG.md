# thediscordlist

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
