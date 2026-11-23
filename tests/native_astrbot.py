"""Installed AstrBot contract fixtures with an isolated framework data root.

Builders, events, DTO validation, hooks, Runner and Executor are real. Providers,
platform sends and registered test tools are synthetic and never contact services.
"""

import os
from tempfile import TemporaryDirectory

_FRAMEWORK_ROOT = TemporaryDirectory(prefix="bot-call-tests-astrbot-")
os.environ["ASTRBOT_ROOT"] = _FRAMEWORK_ROOT.name

from astrbot.core.agent.tool import FunctionTool, ToolSet
from astrbot.core.astr_agent_tool_exec import FunctionToolExecutor
from astrbot.core.db.po import Conversation
from astrbot.core.message.message_event_result import MessageChain
from astrbot.core.provider.entities import LLMResponse
from bot_call.astrbot_agent import load_agent
from astrbot.core.star.context import Context as AstrContext


def agent_fixture():
    api = load_agent()
    return api, {
        "FunctionTool": FunctionTool,
        "ToolSet": ToolSet,
        "FunctionToolExecutor": FunctionToolExecutor,
        "Conversation": Conversation,
        "MessageChain": MessageChain,
        "PlatformMetadata": api.platform_type,
        "EventType": api.event_types,
        "star_map": {},
    }


def register_hook(case, plugin, kind, handler, *, priority=0, reserved=False, enabled=True):
    """Scoped real registry entry; service IO remains synthetic."""
    from types import SimpleNamespace
    from unittest.mock import patch
    from astrbot.core.star.star import star_map
    from astrbot.core.star.star_handler import StarHandlerMetadata, star_handlers_registry

    module = "synthetic_plugin." + plugin
    handler.__module__ = module
    metadata = SimpleNamespace(name=plugin, display_name=plugin, activated=True, reserved=reserved)
    case.enterContext(patch.dict(star_map, {module: metadata}))
    if hasattr(case, "agent_env"):
        case.enterContext(patch.dict(case.agent_env["star_map"], {module: metadata}))
    row = StarHandlerMetadata(
        kind,
        module + "_" + handler.__name__,
        handler.__name__,
        module,
        handler,
        [],
        extras_configs={"priority": priority},
        enabled=enabled,
    )
    star_handlers_registry.append(row)
    case.addCleanup(star_handlers_registry.remove, row)
    return row
