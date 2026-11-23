"""The framework boundary stays lazy and preserves the pinned host contract."""

import ast
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]


class CompatTests(unittest.TestCase):
    def test_lazy_import_boundary_and_real_host_contract(self):
        sources = [ROOT / "main.py", *sorted((ROOT / "bot_call").glob("*.py"))]
        for path in sources:
            if path.name == "astrbot_compat.py":
                continue
            for node in ast.walk(ast.parse(path.read_text())):
                names = (
                    [node.module or ""]
                    if isinstance(node, ast.ImportFrom)
                    else [alias.name for alias in node.names]
                    if isinstance(node, ast.Import)
                    else []
                )
                self.assertFalse(
                    any(n == "astrbot" or n.startswith("astrbot.") for n in names), path
                )
        script = """
import importlib, pathlib, sys
for path in pathlib.Path("bot_call").glob("*.py"):
    importlib.import_module("bot_call." + path.stem)
assert not any(n == "astrbot" or n.startswith("astrbot.") for n in sys.modules)
"""
        result = subprocess.run(
            [sys.executable, "-B", "-c", script], capture_output=True, text=True
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        # Import the fixture first so AstrBot data stays outside the repository.
        from native_astrbot import AstrContext
        from bot_call.astrbot_compat import BASELINE_VERSION
        from bot_call.native_host import load_tools, ESTIMATE_RULE
        from bot_call.native_provider import COUNTER_RULE
        from astrbot.core.agent.context.manager import ContextManager
        from astrbot.core.agent.message import Message
        from astrbot.core.agent.context.token_counter import EstimateTokenCounter

        from bot_call.astrbot_audio import convert_stt_audio

        tools, estimator = load_tools(AstrContext.__new__(AstrContext))
        self.assertIs(tools.stt_converter, convert_stt_audio)
        self.assertIsNot(tools.stt_converter, tools.converter)
        self.assertIs(tools.context_manager_type, ContextManager)
        self.assertIs(tools.message_type, Message)
        self.assertIsInstance(estimator, EstimateTokenCounter)
        self.assertEqual(BASELINE_VERSION, "4.27.2")
        self.assertEqual(ESTIMATE_RULE, "astrbot4.27.2.canonical-json-estimate.v1")
        self.assertEqual(COUNTER_RULE, "astrbot4.27.2.native-public-call-estimate.v1")


class VersionedCompatTests(unittest.IsolatedAsyncioTestCase):
    async def test_native_layout_mapping_version_specifier_and_async_selection(self):
        # Install data-root isolation before importing the real native classes.
        from native_astrbot import agent_fixture
        from bot_call.astrbot_compat import (
            build_config,
            provider_settings,
            framework_version,
            SUPPORTED_VERSIONS,
            using_provider,
        )
        from astrbot.core.star.star_manager import PluginManager
        from native_config import NEW
        from copy import deepcopy
        from types import SimpleNamespace as NS
        import json

        metadata = json.loads((ROOT / "metadata.yaml").read_text())
        self.assertEqual(metadata["astrbot_version"], SUPPORTED_VERSIONS)
        self.assertEqual(
            PluginManager._validate_astrbot_version_specifier(SUPPORTED_VERSIONS), (True, None)
        )
        self.assertIn(framework_version(), ("4.27.2", "4.29.3"))
        api, _ = agent_fixture()
        if NEW:
            conf = {
                "provider_settings": {
                    "max_context_length": 999,
                    "default_personality": "WRONG_SHADOW",
                },
                "agent_runner": {
                    "runner_type": "local",
                    "config": {
                        "model": {"fallback_provider_ids": ["secondary"], "request_max_retries": 2},
                        "persona": {
                            "persona_id": "correct",
                            "safety_mode": False,
                            "safety_mode_strategy": "system_prompt",
                        },
                        "compression": {
                            "max_turns": -1,
                            "trim_turns": 7,
                            "overflow_strategy": "llm_compress",
                            "instruction": "synthetic",
                            "keep_recent_ratio": 0.2,
                            "provider_id": "compress",
                            "fallback_max_tokens": 9900,
                        },
                        "misc": {
                            "max_steps": 9,
                            "tool_call_timeout": 13,
                            "tool_schema_mode": "full",
                            "sanitize_context_by_modalities": True,
                        },
                    },
                },
            }
        else:
            conf = {
                "provider_settings": {
                    "max_context_length": -1,
                    "dequeue_context_length": 7,
                    "default_personality": "correct",
                    "max_agent_step": 9,
                    "tool_call_timeout": 13,
                }
            }
        before = deepcopy(conf)
        cfg = build_config(api, conf)
        canonical = provider_settings(conf)
        self.assertEqual(cfg.max_context_length, -1)
        self.assertEqual(cfg.dequeue_context_length, 7 if NEW else 1)
        self.assertEqual(cfg.tool_call_timeout, 13)
        self.assertEqual(canonical["default_personality"], "correct")
        self.assertEqual(canonical["max_agent_step"], 9)
        if NEW:
            self.assertEqual(cfg.fallback_provider_ids, ["secondary"])
            self.assertEqual(cfg.request_max_retries, 2)
            self.assertFalse(cfg.llm_safety_mode)
            self.assertEqual(cfg.llm_compress_provider_id, "compress")
            self.assertEqual(cfg.fallback_max_context_tokens, 9900)
        self.assertEqual(conf, before)
        selected = object()
        calls = []

        async def modern(**kw):
            calls.append(kw)
            return selected

        def forbidden(**kw):
            raise AssertionError("deprecated selector was called")

        for kind, method in (
            ("chat", "get_using_provider"),
            ("stt", "get_using_stt_provider"),
            ("tts", "get_using_tts_provider"),
        ):
            ctx = NS(**{method + "_async": modern, method: forbidden})
            self.assertIs(await using_provider(ctx, "synthetic-umo", kind), selected)
            ctx = NS(**{method: lambda **kw: selected})
            self.assertIs(await using_provider(ctx, "synthetic-umo", kind), selected)
        self.assertEqual(calls, [{"umo": "synthetic-umo"}] * 3)


class CompatibilityFallbackTests(unittest.TestCase):
    def test_newer_versions_warn_once_per_version(self):
        from bot_call import astrbot_compat as compat

        with (
            patch.object(compat, "_warned_versions", set()),
            patch.object(compat, "framework_version") as framework_version,
            self.assertLogs("bot_call.astrbot_compat", level="WARNING") as logs,
        ):
            for version in ("4.30.0", "4.31.2", "5.0.0", "4.30.0"):
                with self.subTest(version=version):
                    framework_version.return_value = version
                    self.assertEqual(compat.astrbot_version_status(), ("unverified_newer", version))
                    self.assertIs(compat.new_agent_config(), True)
                    self.assertIs(compat.new_agent_config(), True)
        self.assertEqual(
            [record.getMessage() for record in logs.records],
            [
                f"bot_call.compat_unverified_version version={v}"
                for v in ("4.30.0", "4.31.2", "5.0.0")
            ],
        )

    def plugin_config(self):
        import native_astrbot  # noqa: F401  # Isolate the real framework data root.
        from astrbot.core.config.astrbot_config import AstrBotConfig
        from tempfile import TemporaryDirectory

        directory = Path(self.enterContext(TemporaryDirectory()))
        path = directory / "astrbot_plugin_bot_call_config.json"
        path.write_text('{"review_style": "brief"}', encoding="utf-8")
        return AstrBotConfig(
            str(path), schema={"review_style": {"type": "string", "default": "default"}}
        )

    def test_unsupported_versions_keep_config_readonly(self):
        from bot_call import astrbot_compat as compat

        config = self.plugin_config()
        for version in ("4.27.1", "4.28.0", "4.28.5"):
            with (
                self.subTest(version=version),
                patch.object(compat, "framework_version", return_value=version),
                patch.object(compat, "_supports_plugin_page_post") as probe,
            ):
                self.assertEqual(compat.astrbot_version_status(), ("unsupported", version))
                with self.assertRaisesRegex(RuntimeError, "^compat.unsupported_version$"):
                    compat.new_agent_config()
                values, reason = compat.plugin_config_state(config)
                self.assertEqual(values, dict(config))
                self.assertEqual(reason, "控制面板无法安全保存设置，请到 AstrBot 的插件设置页修改")
                probe.assert_not_called()

    def test_config_writes_depend_on_registered_post_route_and_bridge_asset(self):
        from bot_call import astrbot_compat as compat

        config = self.plugin_config()
        from astrbot.dashboard.api.plugins import router

        path = Path(config.config_path)
        read_text = Path.read_text

        def read_config_only(target, *args, **kwargs):
            self.assertEqual(target, path)
            return read_text(target, *args, **kwargs)

        with patch.object(Path, "read_text", read_config_only):
            self.assertEqual(compat.plugin_config_state(config), ({"review_style": "brief"}, None))
        routes = [
            route
            for route in router.routes
            if not (
                route.path == "/plugins/extensions/{plugin_path:path}" and "POST" in route.methods
            )
        ]
        for missing, simulated in (
            ("route", patch.object(router, "routes", routes)),
            ("asset", patch("importlib.resources.files", return_value=path.parent)),
        ):
            with self.subTest(missing=missing), simulated:
                values, reason = compat.plugin_config_state(config)
                self.assertEqual(values, dict(config))
                self.assertEqual(
                    reason, "当前 AstrBot 版本无法在控制面板保存设置，请到 AstrBot 的插件设置页修改"
                )

    def test_build_config_accepts_reduced_dataclass_and_surfaces_new_required_field(self):
        from dataclasses import dataclass
        from types import SimpleNamespace
        from native_config import migrate
        from bot_call.astrbot_compat import build_config

        @dataclass
        class ReducedConfig:
            tool_call_timeout: int
            max_context_length: int
            request_max_retries: int

        @dataclass
        class IncompatibleConfig(ReducedConfig):
            required_setting: str

        conf = migrate(
            {
                "provider_settings": {
                    "tool_call_timeout": 17,
                    "max_context_length": 9,
                    "dequeue_context_length": 3,
                    "request_max_retries": 2,
                }
            }
        )
        self.assertEqual(
            build_config(SimpleNamespace(config_type=ReducedConfig), conf),
            ReducedConfig(tool_call_timeout=17, max_context_length=9, request_max_retries=2),
        )
        with self.assertRaisesRegex(TypeError, "required_setting"):
            build_config(SimpleNamespace(config_type=IncompatibleConfig), conf)
