"""The fixture every TheDiscordList SDK shares, checked against this one's own values."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, get_args

import pytest

from thediscordlist import DEFAULT_BASE_URL
from thediscordlist._http import (
    MAX_ATTEMPTS,
    MAX_RATE_LIMIT_WAIT,
    RATE_LIMIT_DEFAULT,
    RATE_LIMIT_JITTER,
    TIMEOUT_SECONDS,
    backoff,
)
from thediscordlist._publishers import (
    COMMANDS_DELAY,
    COMMANDS_RETRY,
    HEARTBEAT_DEFAULT_INTERVAL,
    HEARTBEAT_JITTER,
    PLAN_REPROBE,
    STATS_DEBOUNCE,
    STATS_FIRST_DELAY,
    STATS_FORCE_AFTER,
    STATS_INTERVAL,
    STATS_MIN_GAP,
)
from thediscordlist._topology import ShardStatus
from thediscordlist.webhooks import DEFAULT_TOLERANCE_SECONDS, WebhookError, verify_webhook

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "sdk-conformance.json").read_text(encoding="utf-8")
)
SIGNATURE = FIXTURE["webhook_signature"]


@pytest.mark.parametrize("vector", SIGNATURE["vectors"], ids=lambda vector: vector["name"])
def test_a_webhook_vector_verifies_as_the_fixture_says(vector: dict[str, Any]) -> None:
    def verify() -> Any:
        return verify_webhook(
            vector["body"], vector["header"], SIGNATURE["secret"], now=lambda: vector["now"]
        )

    if vector["valid"]:
        assert verify()
    else:
        with pytest.raises(WebhookError):
            verify()


def test_every_default_is_the_shared_one() -> None:
    assert SIGNATURE["tolerance_seconds"] == DEFAULT_TOLERANCE_SECONDS
    assert {
        "base_url": DEFAULT_BASE_URL,
        "timeout_seconds": TIMEOUT_SECONDS,
        "retry": {
            "attempts": MAX_ATTEMPTS,
            "backoff_base_seconds": backoff(0, lambda: 1.0),
            "backoff_max_seconds": backoff(99, lambda: 1.0),
        },
        "rate_limit": {
            "default_seconds": RATE_LIMIT_DEFAULT,
            "max_seconds": MAX_RATE_LIMIT_WAIT,
            "jitter_max_seconds": RATE_LIMIT_JITTER,
        },
        "forbidden_reprobe_seconds": PLAN_REPROBE,
        "stats": {
            "first_delay_seconds": STATS_FIRST_DELAY,
            "interval_seconds": STATS_INTERVAL,
            "debounce_seconds": STATS_DEBOUNCE,
            "min_gap_seconds": STATS_MIN_GAP,
            "force_after_seconds": STATS_FORCE_AFTER,
        },
        "heartbeat": {
            "interval_seconds": HEARTBEAT_DEFAULT_INTERVAL,
            "jitter_max_seconds": HEARTBEAT_JITTER,
        },
        "commands": {
            "first_delay_seconds": COMMANDS_DELAY,
            "retry_seconds": COMMANDS_RETRY,
        },
    } == FIXTURE["defaults"]


def test_the_shard_states_are_the_shared_ones() -> None:
    assert set(get_args(ShardStatus)) == set(FIXTURE["shard_states"])
