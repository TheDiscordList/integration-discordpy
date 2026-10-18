"""TheDiscordList integration for discord.py.

    import os
    import discord
    from discord.ext import commands
    from thediscordlist import autopost

    bot = commands.Bot(command_prefix="!", intents=discord.Intents.default())
    autopost(bot, os.environ["TDL_TOKEN"])
    bot.run(os.environ["DISCORD_TOKEN"])

Webhooks live in :mod:`thediscordlist.webhooks`, which imports nothing but the
standard library.
"""

from __future__ import annotations

__version__ = "0.2.0"

from ._autopost import Autoposter, autopost
from ._http import DEFAULT_BASE_URL
from ._publishers import PostResult, PublisherKind
from ._topology import Topology, TopologyKind

__all__ = [
    "DEFAULT_BASE_URL",
    "Autoposter",
    "PostResult",
    "PublisherKind",
    "Topology",
    "TopologyKind",
    "__version__",
    "autopost",
]
