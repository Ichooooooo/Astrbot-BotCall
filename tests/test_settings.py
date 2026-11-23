"""Real config saves + scoped management auth, no live models or listeners."""

import asyncio
from copy import deepcopy
import json
from types import SimpleNamespace as NS
from unittest.mock import patch

from bot_call.plugin import PluginLifecycle, PLUGIN_NAME, SETTINGS_ROUTE, REVIEWS_ROUTE
from bot_call.settings import SettingsController, schema, validate_config, SettingsInvalid
from bot_call.native_host import settings_from, NativeHostError
from test_backend import NativeCase
from test_call_model import ChatProvider


class SettingsTests(NativeCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        from astrbot.core.config.astrbot_config import AstrBotConfig

        self.config_class = AstrBotConfig
        self.path.joinpath("config").mkdir()
        self.config = AstrBotConfig(
            str(self.path / "config" / (PLUGIN_NAME + "_config.json")), schema=schema()
        )
        self.config.save_config(dict(self.settings(), SYNTHETIC_LEGACY="NEVER_EXPOSE_SECRET"))
        self.context.get_all_providers = lambda: list(self.context.by_id.values())
        self.routes = []
        self.context.register_web_api = lambda *args: self.routes.append(args)
        self.lifecycle = PluginLifecycle(
            self.config, lambda: str(self.path), context=self.context, version="candidate"
        )
        self.lifecycle._check_loop()
        self.lifecycle._register_management()

    async def invoke(
        self,
        method="GET",
        body=None,
        *,
        token="admin",
        username="local-admin",
        route=SETTINGS_ROUTE,
        plugin=PLUGIN_NAME,
        query="",
        auth_header=True,
        legacy=False,
    ):
        from astrbot.api.web import PluginRequest, bind_request_context
        from starlette.requests import Request

        class Dashboard:
            def _extract_dashboard_jwt(self, request):
                # Cookie fallback is intentionally present to exercise POST CSRF.
                header = request.headers.get("authorization", "")
                return header.removeprefix("Bearer ") if header else token

            def _validate_dashboard_token(self, value, path):
                if value == "admin":
                    return dict(username="local-admin", token_type="dashboard"), ""
                if value == "asset":
                    return dict(username="local-admin", token_type="plugin_page_asset"), ""
                return None, "private auth detail"

        headers = [(b"content-type", b"application/json")]
        if auth_header and token:
            headers.append((b"authorization", ("Bearer " + token).encode()))
        scope = dict(
            type="http",
            http_version="1.1",
            method=method,
            scheme="http",
            path=("/api/plug" if legacy else "/api/v1/plugins/extensions") + route,
            query_string=query.encode(),
            headers=headers,
            server=("127.0.0.1", 8000),
            client=("127.0.0.1", 1234),
            app=NS(state=NS(dashboard_app_adapter=NS(_dashboard_server=Dashboard()))),
        )

        async def receive():
            return dict(type="http.request", body=json.dumps(body).encode(), more_body=False)

        request = PluginRequest(Request(scope, receive), plugin_name=plugin, username=username)
        with bind_request_context(request):
            response = await (
                self.lifecycle._reviews_handler()
                if route == REVIEWS_ROUTE
                else self.lifecycle._settings_handler()
            )
        self.assertEqual(response.headers["cache-control"], "no-store")
        return response.status_code, json.loads(response.body)

    async def test_dedicated_stt_selection_choices_save_and_runtime_disable(self):
        from bot_call.astrbot_compat import stt_provider_choices
        from bot_call.astrbot_audio import STTUnavailable

        event, _ = self.environment()
        other, _ = self.environment("qq-B", "B")
        source = self.context.stt[event.unified_msg_origin]
        dedicated = self.context.stt[other.unified_msg_origin]
        host = self.host()
        self.assertEqual(settings_from(self.settings())["call_stt_provider_id"], "")
        self.assertIs((await host.check_stt(event.unified_msg_origin))[0], source)
        controller = self.lifecycle._settings

        async def save(pid):
            return await controller.post(
                dict(
                    revision=(await controller.get(0))["revision"],
                    values={"call_stt_provider_id": pid},
                )
            )

        self.assertIsNone((await save(dedicated.meta().id))[0])
        host.settings["call_stt_provider_id"] = dedicated.meta().id
        self.assertEqual(await host.check_stt(event.unified_msg_origin), (dedicated, "dedicated"))
        choices = stt_provider_choices(self.context)
        self.assertIn(
            dict(
                value=dedicated.meta().id, label=f"{dedicated.meta().id}（{dedicated.get_model()}）"
            ),
            choices,
        )
        self.assertNotIn("tts-A", [c["value"] for c in choices])
        dedicated.provider_config["enable"] = False
        self.assertNotIn(
            dedicated.meta().id, [c["value"] for c in stt_provider_choices(self.context)]
        )
        for pid in (dedicated.meta().id, "missing", "tts-A"):
            error, data = await save(pid)
            self.assertEqual(error, "bot_call.settings_invalid")
            self.assertIn("call_stt_provider_id", data["errors"])
        with self.assertRaises(STTUnavailable):
            await host.check_stt(event.unified_msg_origin)
        self.assertIsNone((await save(""))[0])

    async def test_missing_cloudflared_rejects_save_and_exposes_startup_reason(self):
        from bot_call.plugin import PluginLifecycleError

        controller = self.lifecycle._settings
        before = dict(self.config)
        with patch("shutil.which", return_value=None):
            error, data = await controller.post(
                dict(
                    revision=(await controller.get(0))["revision"],
                    values=dict(access_mode="quick_tunnel", public_origin=""),
                )
            )
            self.assertEqual(error, "bot_call.settings_invalid")
            self.assertEqual(
                data["errors"]["cloudflared_path"],
                "未找到 cloudflared，请先在 AstrBot 运行环境安装，或改用固定 HTTPS",
            )
            self.assertEqual(dict(self.config), before)
            self.config.update(access_mode="quick_tunnel", public_origin="")
            with self.assertLogs("bot_call.plugin", level="ERROR") as logs:
                with self.assertRaises(PluginLifecycleError):
                    await self.lifecycle.initialize()
            self.assertIn("临时隧道不可用：未找到 cloudflared", logs.output[0])
        data = await self.lifecycle._benchmark.get()
        self.assertFalse(data["status"]["plugin_ready"])
        self.assertEqual(data["status"]["error"], "临时隧道不可用：未找到 cloudflared")
        self.assertTrue(self.routes)
        await self.lifecycle.terminate()

    async def test_review_recovery_requires_dashboard_bearer_and_explicit_selection(self):
        from unittest.mock import AsyncMock

        self.lifecycle._ready = True
        self.lifecycle._server = NS(ready=True)
        setattr(self.context, "_bot_call_native_owner_v1", self.lifecycle)
        self.lifecycle._native = NS(counters={})
        action = AsyncMock(return_value={"acknowledgement": "preview"})
        self.lifecycle._backend = NS(ready=True, backfill_reviews=action)
        for args in (
            {"auth_header": False},
            {"token": "asset"},
            {"query": "asset_token=asset"},
            {"plugin": "other"},
            {"method": "GET"},
            {"username": "other"},
        ):
            self.assertEqual(
                (
                    await self.invoke(
                        method=args.get("method", "POST"),
                        body={"call_ids": ["id"]},
                        route=REVIEWS_ROUTE,
                        **{k: v for k, v in args.items() if k != "method"},
                    )
                )[0],
                401,
            )
        action.assert_not_awaited()
        code, _ = await self.invoke("POST", {"call_ids": ["exact-id"]}, route=REVIEWS_ROUTE)
        self.assertEqual(code, 200)
        action.assert_awaited_once_with(["exact-id"], counters={}, context_only=True, apply=False)
        self.assertEqual(
            (await self.invoke("POST", {"call_ids": ["id"], "all": True}, route=REVIEWS_ROUTE))[0],
            400,
        )

    async def test_get_complete_schema_metadata_choices_and_no_provider_secrets(self):
        _, provider = self.environment(provider_class=ChatProvider)
        provider.provider_config.update(
            api_key="NEVER_EXPOSE_KEY", api_base="https://private.invalid"
        )
        _, disabled = self.environment("qq-B", "B", provider_class=ChatProvider)
        disabled.provider_config["enable"] = False
        status, result = await self.invoke()
        self.assertEqual(status, 200)
        data = result["data"]
        self.assertTrue(data["writable"])
        tts_field = next(
            f for g in data["groups"] for f in g["fields"] if f["key"] == "call_tts_provider_id"
        )
        self.assertEqual(tts_field["choices"][0]["value"], "")
        self.assertIn("tts-A", [c["value"] for c in tts_field["choices"]])
        self.assertNotIn(provider.meta().id, [c["value"] for c in tts_field["choices"]])
        self.assertFalse(data["reload_required"])
        self.assertEqual(len(data["revision"]), 16)
        expected = [
            (
                "access",
                "接入",
                "plug",
                (
                    (
                        "entry",
                        "入口",
                        (
                            "enabled",
                            "access_mode",
                            "public_origin",
                            "listen_port",
                            "listen_host",
                            "trusted_proxies",
                        ),
                    ),
                    ("tunnel", "临时隧道", ("cloudflared_path", "tunnel_start_timeout")),
                ),
            ),
            (
                "conversation",
                "对话",
                "chat",
                (
                    (
                        "reply",
                        "通话模型与回复",
                        ("call_chat_provider_id", "call_reply_style", "call_skip_plugin_hooks"),
                    ),
                    (
                        "context",
                        "上下文",
                        ("call_history_turns", "call_history_summary", "call_history_max_chars"),
                    ),
                ),
            ),
            (
                "audio",
                "语音与断句",
                "mic",
                (
                    (
                        "vad",
                        "合成与自动断句",
                        (
                            "call_stt_provider_id",
                            "call_tts_provider_id",
                            "call_capture_mode",
                            "call_barge_in",
                            "call_backchannel_phrases",
                            "call_vad_silence_ms",
                            "call_vad_sensitivity",
                        ),
                    ),
                    (
                        "limits",
                        "录音限制",
                        ("max_audio_bytes", "max_wav_bytes", "call_max_continuous_segments"),
                    ),
                    ("retention", "音频留存", ("audio_retention_days",)),
                ),
            ),
            (
                "review",
                "回顾",
                "note",
                (
                    (
                        "style",
                        "回顾写法",
                        (
                            "review_style",
                            "review_voice",
                            "review_system_notes",
                            "review_instructions",
                        ),
                    ),
                    ("length", "篇幅", ("review_tokens", "safety_tokens")),
                ),
            ),
        ]
        self.assertEqual(
            [
                (
                    g["id"],
                    g["title"],
                    g["icon"],
                    tuple(
                        (s["id"], s["title"], tuple(f["key"] for f in s["fields"]))
                        for s in g["sections"]
                    ),
                )
                for g in data["groups"]
            ],
            expected,
        )
        for group in data["groups"]:
            self.assertEqual(
                group["fields"], [f for section in group["sections"] for f in section["fields"]]
            )
        legacy_fields = [f for g in data["groups"] for f in g["fields"]]
        fields = {f["key"]: f for f in legacy_fields}
        self.assertEqual(len(legacy_fields), len(schema()))
        self.assertEqual(set(fields), set(schema()))
        for key, field in fields.items():
            self.assertEqual(field["default"], schema()[key]["default"])
            self.assertEqual(field["applies"], "reload")
        choices = fields["call_chat_provider_id"]["choices"]
        self.assertEqual([c["value"] for c in choices], ["", provider.meta().id])
        for key in ("review_style", "review_voice", "review_system_notes"):
            for choice in fields[key]["choices"]:
                self.assertTrue(choice["description"])
                self.assertLessEqual(len(choice["example"]), 60)
                for text in (choice["label"], choice["description"], choice["example"]):
                    self.assertNotIn("保持当前", text)
                    self.assertNotIn("保持现状", text)
        voices = {c["value"]: c for c in fields["review_voice"]["choices"]}
        self.assertIn("不限定人称", voices["default"]["label"])
        self.assertIn("不用‘我们’", voices["neutral"]["description"])
        for key in ("review_style", "review_voice", "review_system_notes"):
            self.assertEqual([c["value"] for c in fields[key]["choices"]], schema()[key]["options"])
            self.assertEqual(fields[key]["value"], "default")
            self.assertEqual(fields[key]["default"], "default")
        serialized = json.dumps(data)
        for secret in ("NEVER_EXPOSE", "private.invalid", "api_key", "api_base"):
            self.assertNotIn(secret, serialized)
        self.assertEqual(provider.calls, [])
        self.assertIn("reload_hint", data)

    async def test_post_auth_rejects_anonymous_asset_cookie_api_key_and_foreign_paths(self):
        revision = (await self.invoke())[1]["data"]["revision"]
        body = dict(revision=revision, values={"review_style": "brief"})
        before = self.path.joinpath("config", PLUGIN_NAME + "_config.json").read_bytes()
        for overrides in (
            dict(token=None, username=None),
            dict(token="asset"),
            dict(query="asset_token=synthetic"),
            dict(token="plugin-api-key"),
            dict(plugin="other"),
            dict(route="/other/settings"),
            dict(username="someone-else"),
            dict(auth_header=False),
        ):
            with self.subTest(overrides=overrides):
                self.assertEqual((await self.invoke("POST", body, **overrides))[0], 401)
        self.assertEqual(
            before, self.path.joinpath("config", PLUGIN_NAME + "_config.json").read_bytes()
        )
        self.assertEqual((await self.invoke("POST", body, legacy=True))[1]["status"], "ok")

    async def test_atomic_validation_conflict_and_native_save_bytes_with_redacted_log(self):
        initial = (await self.invoke())[1]["data"]
        file = self.path / "config" / (PLUGIN_NAME + "_config.json")
        before = file.read_bytes()
        for values in (
            dict(review_style="invalid", call_history_turns=7),
            dict(call_history_turns=True),
            dict(call_vad_silence_ms=299),
            dict(call_capture_mode="invalid"),
            dict(call_max_continuous_segments=0),
            dict(call_max_continuous_segments=21),
            dict(call_max_continuous_segments=True),
            dict(access_mode="quick_tunnel"),
            dict(unknown="private"),
            dict(review_instructions="长" * 2001),
        ):
            response = (
                await self.invoke("POST", dict(revision=initial["revision"], values=values))
            )[1]
            self.assertEqual(response["status"], "error")
            message = response["message"]
            self.assertTrue(message.startswith("bot_call.settings_invalid:"))
            self.assertEqual(json.loads(message.split(":", 1)[1]), response["data"])
            self.assertTrue(response["data"]["errors"])
            self.assertEqual(file.read_bytes(), before)
        self.config.save_config(dict(call_history_turns=7))
        response = (
            await self.invoke(
                "POST", dict(revision=initial["revision"], values={"review_style": "brief"})
            )
        )[1]
        self.assertEqual(response["message"], "bot_call.settings_conflict")
        current = (await self.invoke())[1]["data"]
        edits = dict(
            review_style="brief",
            review_voice="bot",
            review_system_notes="on_issue",
            review_instructions="SYNTHETIC_PRIVATE_INSTRUCTIONS",
            call_reply_style="SYNTHETIC_PRIVATE_STYLE",
        )
        golden = self.path / "golden"
        golden.mkdir()
        expected = self.config_class(str(golden / file.name), schema=schema())
        expected.save_config(deepcopy(dict(self.config)))
        expected.save_config(edits)
        with self.assertLogs("bot_call.settings", level="INFO") as logs:
            result = (await self.invoke("POST", dict(revision=current["revision"], values=edits)))[
                1
            ]
        self.assertEqual(result["status"], "ok")
        self.assertEqual(set(result["data"]["saved"]), set(edits))
        self.assertTrue(result["data"]["reload_required"])
        self.assertEqual(file.read_bytes(), (golden / file.name).read_bytes())
        self.assertNotEqual(current["revision"], result["data"]["revision"])
        self.assertNotIn("SYNTHETIC_PRIVATE", "\n".join(logs.output))
        self.assertIn("review_instructions", "\n".join(logs.output))
        self.assertTrue((await self.invoke())[1]["data"]["reload_required"])
        fresh = SettingsController(self.config, self.context, "candidate")
        self.assertFalse((await fresh.get(0))["reload_required"])
        # Same validator is used by enabled startup, without exposing errors.
        for values in (
            dict(call_history_turns=True),
            dict(review_voice="invalid"),
            dict(public_origin="http://bad"),
        ):
            config = dict(self.config, **values)
            with self.assertRaises(SettingsInvalid):
                validate_config(config, require_ready=True)
            with self.assertRaises(NativeHostError):
                settings_from(config)

    async def test_audio_retention_setting_default_and_boundaries(self):
        config = self.settings()
        self.assertEqual(settings_from(config)["audio_retention_days"], 7)
        for value in (0, 3650):
            with self.subTest(value=value):
                checked = settings_from(dict(config, audio_retention_days=value))
                self.assertEqual(checked["audio_retention_days"], value)
        for value in (-1, 3651, False, "7"):
            with self.subTest(value=value), self.assertRaises(NativeHostError):
                settings_from(dict(config, audio_retention_days=value))

    async def test_readonly_capability_and_save_failure_preserve_existing_config(self):
        file = self.path / "config" / (PLUGIN_NAME + "_config.json")
        original = file.read_bytes()
        with patch("bot_call.astrbot_compat.new_agent_config", side_effect=RuntimeError("PRIVATE")):
            readonly = (await self.invoke())[1]["data"]
            self.assertFalse(readonly["writable"])
            self.assertTrue(readonly["readonly_reason"])
            result = (
                await self.invoke(
                    "POST", dict(revision=readonly["revision"], values={"review_style": "brief"})
                )
            )[1]
            self.assertEqual(result["message"], "bot_call.settings_readonly")
        current = (await self.invoke())[1]["data"]
        with patch.object(self.config_class, "save_config", side_effect=OSError("PRIVATE")):
            result = (
                await self.invoke(
                    "POST", dict(revision=current["revision"], values={"review_style": "brief"})
                )
            )[1]
            self.assertEqual(result["message"], "bot_call.settings_save_failed")
        self.assertEqual(file.read_bytes(), original)
        self.assertEqual(self.config["review_style"], "default")

    async def test_active_call_count_and_no_self_reload_even_while_busy(self):
        event, _ = self.environment()
        backend, _ = await self.backend()
        handle, web = await self.enter(backend, event)
        self.lifecycle._backend = backend
        result = (await self.invoke())[1]["data"]
        self.assertEqual(result["active_calls"], 1)
        self.assertIn("AstrBot 插件管理", result["reload_hint"])
        self.assertFalse(any(r[0].endswith("/reload") for r in self.routes))
        await backend.web_end(web.token, web.reference)
        await backend.drain()
        self.assertEqual((await self.invoke())[1]["data"]["active_calls"], 0)

    async def test_control_packaging_requires_all_assets_together(self):
        from tools.build_plugin import ALLOWLIST, CONTROL_FILES, package_files

        self.assertTrue(
            {"README.md", "LICENSE", "THIRD_PARTY_NOTICES.md", "docs/使用手册.md"} <= set(ALLOWLIST)
        )
        self.assertNotIn("CHANGELOG.md", ALLOWLIST)

        root = self.path / "package-fixture"
        root.mkdir()
        self.assertFalse(any(name in package_files(root) for name in CONTROL_FILES))
        with self.assertRaises(ValueError):
            package_files(root, require_control=True)
        target = root / CONTROL_FILES[0]
        target.parent.mkdir(parents=True)
        target.write_text("synthetic")
        with self.assertRaises(ValueError):
            package_files(root)
        for name in CONTROL_FILES[1:]:
            (root / name).write_text("synthetic")
        self.assertTrue(set(CONTROL_FILES) <= set(package_files(root)))
