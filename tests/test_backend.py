"""Public-provider composition fixtures. No SDK, real model, audio or QQ calls."""

import asyncio
from copy import deepcopy
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace as NS
import unittest

from bot_call.backend import BackendConfig, BackendTools, CallBackend
from bot_call.native_host import NativeCallbacks, settings_from
from bot_call.publication import Outcome
from bot_call.call_policy import CallPolicy
from bot_call.limits import RuntimeLimits
import test_astrbot_adapter as read_fixture
from native_astrbot import LLMResponse, AstrContext

ORIGIN = "https://calls.example.test"
PRIVATE = "SYNTHETIC_PRIVATE_BODY"
TEXT = " 开头🙂e\u0301\r\n中段犹豫；结尾未决定。 "
BOT = "Bot建议暂缓，用户尚未接受。"
FINAL = "完整回顾：开头讨论安排，中段犹豫，结尾未定；Bot建议非决定，播放未知。"
MP3 = bytes.fromhex("fffb10c4") + bytes(100)
WAV = b"RIFF" + (40).to_bytes(4, "little") + b"WAVE" + bytes(36)


def response(body=BOT, *, role="assistant", raw=None, names=None, args=None, ids=None, chunk=False):
    return LLMResponse(
        role=role,
        completion_text=body,
        tools_call_name=[] if names is None else names,
        tools_call_args=[] if args is None else args,
        tools_call_ids=[] if ids is None else ids,
        raw_completion=raw,
        is_chunk=chunk,
        reasoning_content=None,
    )


class Message:
    def __init__(self, **values):
        self.__dict__.update(deepcopy(values))

    def model_dump(self, *, mode="python"):
        return deepcopy(vars(self))


class Config:
    def __init__(self, **values):
        self.__dict__.update(values)


class NativeManager:
    force_compression = False
    observations = []

    def __init__(self, config):
        self.config = config

    async def process(self, messages, trusted_token_usage=0):
        values = [deepcopy(m if type(m) is dict else m.model_dump(mode="json")) for m in messages]
        type(self).observations.append(deepcopy(values))
        if type(self).force_compression and self.config.llm_compress_provider is not None:
            await self.config.llm_compress_provider.text_chat(contexts=messages)
        return messages


class Estimator:
    def count_tokens(self, messages, trusted_token_usage=0):
        return sum(len(m.content) for m in messages) // 4 + 1


class PublicProvider:
    """Intentionally has no client/api_keys/private SDK methods."""

    def __init__(self, identifier, model="native-model", window=32000):
        self.provider_config = dict(
            id=identifier, max_context_tokens=window, enable=True, modalities=["text"]
        )
        self.model = model
        self.calls = []
        self.responder = None

    def get_model(self):
        return self.model

    def meta(self):
        return NS(id=self.provider_config["id"], model=self.model)

    async def text_chat(self, **kwargs):
        # Provider-native serialization; the plugin passed actual Message objects.
        kwargs = dict(
            kwargs,
            contexts=[
                v if type(v) is dict else v.model_dump(mode="json")
                for v in (kwargs.get("contexts") or [])
            ],
        )
        # Cancellation signals contain live Tasks; preserve the public signal by identity.
        snapshot = deepcopy({k: v for k, v in kwargs.items() if k != "abort_signal"})
        if "abort_signal" in kwargs:
            snapshot["abort_signal"] = kwargs["abort_signal"]
        self.calls.append(snapshot)
        if self.responder is not None:
            return await self.responder(kwargs)
        contexts = kwargs.get("contexts") or []
        try:
            data = json.loads(contexts[-1]["content"])
        except (ValueError, TypeError, KeyError):
            data = {}
        if data.get("format") == "bot-call-review-input-v1":
            return response("本块事实：用户未定，Bot建议非决定。")
        if data.get("format") == "bot-call-review-reduction-v1":
            return response(FINAL if data["kind"] == "final" else "合并事实笔记。")
        return response()


class OtherPublicProvider(PublicProvider):
    # A separate native implementation shape, with provider-specific metadata.
    def __init__(self, identifier):
        super().__init__(identifier, "other-native-model")
        self.provider_config["native_setting"] = {"opaque": True}


from astrbot.core.provider.provider import STTProvider as NativeSTTProvider


class STTProvider(PublicProvider, NativeSTTProvider):
    def __init__(self, identifier):
        super().__init__(identifier)
        self.paths = []
        self.transcriber = None

    async def get_text(self, audio_url):
        self.paths.append(audio_url)
        assert Path(audio_url).read_bytes() == WAV
        return await self.transcriber(audio_url) if self.transcriber is not None else TEXT


from astrbot.core.provider.provider import TTSProvider as NativeTTSProvider


class TTSProvider(PublicProvider, NativeTTSProvider):
    def __init__(self, identifier, directory):
        super().__init__(identifier)
        self.directory = directory
        self.paths, self.texts = [], []
        self.synthesizer = None

    async def get_audio(self, text):
        self.texts.append(text)
        if self.synthesizer is not None:
            return await self.synthesizer(text)
        path = self.directory / ("native-" + str(len(self.paths)) + ".wav")
        path.write_bytes(WAV)
        self.paths.append(path)
        return str(path)


class Conversations(read_fixture.Conversations):
    def __init__(self, locks):
        super().__init__(locks)
        self.writes = []

    async def get_conversation(self, umo, cid, create_if_not_exists=False):
        assert create_if_not_exists is False
        return await super().get_conversation(umo, cid)

    async def update_conversation(self, umo, cid, *, history, token_usage):
        assert umo in self.locks.held
        self.writes.append((umo, cid, deepcopy(history), token_usage))
        self.items[cid].history = json.dumps(history, ensure_ascii=False)


class Context(read_fixture.Context, AstrContext):
    def __init__(self, locks):
        super().__init__(locks)
        self.conversation_manager = Conversations(locks)
        self.by_id, self.stt, self.tts, self.native_calls = {}, {}, {}, []

    async def get_using_provider_async(self, umo=None, **kwargs):
        return self.get_using_provider(umo)

    async def get_using_stt_provider_async(self, umo=None):
        return self.get_using_stt_provider(umo)

    async def get_using_tts_provider_async(self, umo=None):
        return self.get_using_tts_provider(umo)

    def get_using_provider(self, umo=None):
        return self.providers.get(umo)

    def get_config(self, umo=None):
        return (
            self.configs[umo]
            if umo is not None
            else {"subagent_orchestrator": {}, "timezone": "UTC"}
        )

    def get_all_stt_providers(self):
        return list(self.stt.values())

    def get_all_tts_providers(self):
        return list(self.tts.values())

    def get_provider_by_id(self, identifier):
        return self.by_id.get(identifier)

    async def get_current_chat_provider_id(self, umo):
        return self.providers[umo].meta().id

    def get_using_stt_provider(self, umo=None):
        return self.stt.get(umo)

    def get_using_tts_provider(self, umo=None):
        return self.tts.get(umo)

    async def llm_generate(self, *, chat_provider_id, contexts=None, tools=None, **kwargs):
        self.native_calls.append((chat_provider_id, deepcopy(contexts), tools, deepcopy(kwargs)))
        return await self.by_id[chat_provider_id].text_chat(
            contexts=contexts, func_tool=tools, **kwargs
        )


def agent_fixture():
    """Use the installed framework; only services and provider IO are synthetic."""
    from native_astrbot import agent_fixture as installed_fixture

    return installed_fixture()


class NativeCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name)
        self.locks = read_fixture.SharedLocks()
        self.context = Context(self.locks)
        self.hosts, self.backends, self.sent, self.conversions = [], [], [], []
        self.agent_api, self.agent_env = agent_fixture()
        from astrbot.core import astr_main_agent, astr_agent_hooks

        self.enterContext(patch.object(astr_main_agent, "star_map", self.agent_env["star_map"]))
        self.enterContext(
            patch.object(astr_agent_hooks, "call_event_hook", self.agent_api.call_hook)
        )
        self.context.get_llm_tool_manager = lambda: NS(
            get_full_tool_set=lambda: self.agent_env["ToolSet"](), get_builtin_tool=lambda *a: None
        )
        self.context.subagent_orchestrator = None
        self.context.platform_manager = NS(platform_insts=[])
        self.gates = []
        NativeManager.observations = []
        NativeManager.force_compression = False

    async def asyncTearDown(self):
        for gate in self.gates:
            gate.set()
        for backend in reversed(self.backends):
            self.assertTrue((await backend.shutdown(timeout=None)).stopped)
            self.assertFalse(backend._runtime._core_tasks)
        for host in self.hosts:
            host.privacy.close()
        NativeManager.force_compression = False

    def environment(self, platform="qq-A", cid="A", provider_class=PublicProvider):
        event = read_fixture.Event(platform)
        self.context.add(event, cid)
        old = self.context.conversation_manager.items[cid]
        self.context.conversation_manager.items[cid] = self.agent_env["Conversation"](**vars(old))
        meta = self.agent_env["PlatformMetadata"](
            name=event.get_platform_name(),
            id=platform,
            description="synthetic",
            support_proactive_message=False,
            support_streaming_message=False,
        )
        self.context.platform_manager.platform_insts.append(NS(meta=lambda: meta))
        umo = event.unified_msg_origin
        provider = provider_class("provider-" + cid)
        self.context.providers[umo] = provider
        self.context.by_id[provider.meta().id] = provider
        self.context.stt[umo] = STTProvider("stt-" + cid)
        self.context.tts[umo] = TTSProvider("tts-" + cid, self.path)
        self.context.by_id.update(
            {p.meta().id: p for p in (self.context.stt[umo], self.context.tts[umo])}
        )
        from native_config import set_setting

        set_setting(
            self.context.configs[umo], "context_limit_reached_strategy", "truncate_by_turns"
        )
        cfg = self.context.configs[umo]
        cfg.update(admins_id=[], plugin_set=["*"], subagent_orchestrator={})
        cfg["provider_settings"].update(
            streaming_response=False,
            computer_use_runtime="none",
            proactive_capability={"add_cron_tools": False},
        )
        cfg["platform_settings"] = {
            "enable_id_white_list": False,
            "id_whitelist": [],
            "wl_ignore_admin_on_group": False,
            "wl_ignore_admin_on_friend": False,
            "id_whitelist_log": False,
        }
        provider.provider_config["modalities"] = ["text", "tool_use"]
        return event, provider

    async def converter(self, source, *, output_format, output_path):
        self.conversions.append((source, output_path, Path(source).read_bytes()))
        Path(output_path).write_bytes(MP3 if output_format == "mp3" else WAV)
        return output_path

    def settings(self):
        return dict(
            enabled=True,
            public_origin=ORIGIN,
            main_reply_tokens=1024,
            reply_tokens=512,
            review_tokens=2048,
            safety_tokens=1024,
        )

    def tools(self):
        return BackendTools(
            self.context,
            self.locks,
            NativeManager,
            Config,
            Message,
            self.converter,
            agent_api=self.agent_api,
        )

    def host(self):
        host = NativeCallbacks(
            self.context, self.tools(), Estimator(), settings_from(self.settings())
        )
        self.hosts.append(host)
        return host

    async def backend(self, *, name="calls.db"):
        host = self.host()

        async def send(bound, page):
            self.sent.append((bound, page.fragment))
            return Outcome.SUBMITTED

        backend = CallBackend(
            BackendConfig(
                self.path / name,
                ORIGIN,
                3600,
                RuntimeLimits(),
                review_prompt=settings_from(self.settings())["review_prompt"],
            ),
            host.tools,
            select_policies=host.select,
            send_page=send,
            make_context_readable=host.import_context,
        )
        self.backends.append(backend)
        await backend.start()
        return backend, host

    async def enter(self, backend, event):
        handle = backend.begin(event)
        grant = await backend.invitation(handle)
        web = await backend.exchange_web_invitation(grant.secret.value)
        return handle, web


class BackendTests(NativeCase):
    async def test_entry_warmup_is_once_nonblocking_failure_safe_and_reaped_on_shutdown(self):
        from dataclasses import replace

        event, _ = self.environment()
        entered, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
        self.gates.append(release)
        calls = []

        async def warmup():
            calls.append(True)
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        original_tools = self.tools
        self.tools = lambda: replace(original_tools(), audio_warmup=warmup)
        backend, _ = await self.backend()
        handle, web = await asyncio.wait_for(self.enter(backend, event), 1)
        await asyncio.wait_for(entered.wait(), 1)
        self.assertFalse(release.is_set())
        record = backend._record(handle)
        backend._start_audio_warmup(record)
        self.assertEqual(len(calls), 1)
        self.assertFalse((await backend.web_state(web.token, web.reference)).ended)
        self.assertTrue((await backend.shutdown(timeout=None)).stopped)
        self.assertTrue(cancelled.is_set())
        self.assertFalse(backend._warmup_tasks)

        async def failed():
            raise RuntimeError("synthetic warmup failure")

        self.tools = lambda: replace(original_tools(), audio_warmup=failed)
        backend, _ = await self.backend(name="warmup-failed.db")
        handle, web = await self.enter(backend, event)
        await asyncio.gather(*tuple(backend._warmup_tasks))
        self.assertFalse((await backend.web_state(web.token, web.reference)).ended)
        self.assertEqual(self.context.stt[event.unified_msg_origin].paths, [])
        self.assertEqual(self.context.tts[event.unified_msg_origin].texts, [])

    async def test_full_original_source_audio_reply_review_and_context(self):
        event, provider = self.environment()
        other, other_provider = self.environment("qq-B", "B", OtherPublicProvider)
        untouched = self.context.conversation_manager.items["B"].history
        backend, host = await self.backend()
        handle, web = await self.enter(backend, event)
        record = backend._record(handle)
        self.assertTrue(record.policy.is_native)
        self.assertEqual(
            record.policy, CallPolicy(record.policy.canonical_json, record.policy.fingerprint)
        )
        self.assertEqual(
            (record.policies.reply.contract, record.policies.review.policy.output_token_limit),
            ("astrbot-main-agent-v1", 2048),
        )
        for index in range(2):
            await backend.web_audio(
                web.token,
                web.reference,
                "audio-" + str(index),
                b"synthetic-container",
                "audio/webm",
            )
            await backend.drain()
            state = await backend.read_state(handle)
            self.assertEqual(state.audio_jobs[-1].state, "succeeded")
            generation = state.generations[-1]
            self.assertEqual(generation.state, "succeeded")
            await backend.web_speech(web.token, web.reference, generation.generation_id)
            await backend.drain()
            job = await backend.web_speech(web.token, web.reference, generation.generation_id)
            self.assertEqual(job["state"], "succeeded")
        self.assertEqual([t.role for t in state.turns], ["user", "assistant"] * 2)
        frozen_turns = state.turns
        # A later current pointer must not redirect the fixed original cid write.
        self.context.conversation_manager.current[event.unified_msg_origin] = "B"
        await backend.web_end(web.token, web.reference)
        await backend.drain()
        state = await backend.read_state(handle)
        self.assertEqual(state.review.state, "succeeded")
        self.assertEqual(state.review_work.map_count, 1)
        self.assertEqual(json.loads(state.review.input_json), [vars_turn(t) for t in frozen_turns])
        self.assertTrue(state.publication.complete)
        self.assertEqual("".join(body for _, body in self.sent), FINAL)
        self.assertEqual(self.context.conversation_manager.writes[-1][1], "A")
        self.assertEqual(self.context.conversation_manager.items["B"].history, untouched)
        self.assertEqual(other_provider.calls, [])
        self.assertTrue(all(p.exists() for p in self.context.tts[event.unified_msg_origin].paths))
        self.assertTrue(
            all(not Path(p).exists() for p in self.context.stt[event.unified_msg_origin].paths)
        )
        self.assertEqual(
            len(provider.calls), 4
        )  # two replies + one map + one final, public calls only
        before = len(provider.calls)
        await backend.web_end(web.token, web.reference)
        await backend.drain()
        self.assertEqual(len(provider.calls), before)
        self.assertTrue(
            all(
                "max_tokens" not in kwargs and "max_completion_tokens" not in kwargs
                for _, _, _, kwargs in self.context.native_calls
            )
        )
        self.assertFalse(hasattr(provider, "client"))
        self.assertTrue(all(isinstance(value, dict) for value in provider.calls[0]["contexts"]))


def vars_turn(turn):
    from dataclasses import asdict

    return asdict(turn)


from contextlib import closing
from dataclasses import replace
from unittest.mock import patch
import hashlib
import sqlite3
import threading
import traceback
from bot_call.access import InvitationSecret, AccessError
from bot_call.astrbot_adapter import AstrBotReadAdapter
from bot_call.astrbot_context import AstrBotContextAdapter
from bot_call.astrbot_reply import ReplyModel, ReplyPolicy
from bot_call.astrbot_review import ReviewRequestPolicy
from bot_call.astrbot_audio import AudioRequestPolicy
from bot_call.backend import BackendPolicies, BackendError
from bot_call.review_router import ReviewPolicySelection
from bot_call.review_work import prepare_review_work
from bot_call.native_provider import NATIVE_CONTRACT, COUNTER_RULE
from bot_call.runtime import RuntimeOperationError
from bot_call.audio import audio_settled
from bot_call.store import Store
from bot_call import store as storage
import test_web_access as web_fixture


RECOVERY_TEXT = "前段🙂\r\n" + "中段犹豫未决定，Bot建议不是用户决定。\n" * 58 + "结尾仍未定"
RECOVERY_FINAL = FINAL * 8


class RecoveryTests(NativeCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.thread = threading.get_ident()
        self.now = 1800000000.0
        self.sends = []
        self.model_reads = []
        self.selection_calls = []
        self.dirs = []
        self.stt_providers = {}
        self.stt_mode = "ok"
        self.stt_entered = asyncio.Event()
        self.stt_cancelled = asyncio.Event()
        self.stt_release = asyncio.Event()
        self.stt_release.set()
        self.gates.append(self.stt_release)
        self.counter = lambda text: len(text)
        web_fixture.ObservedStore.instances = []
        web_fixture.ObservedStore.before_append = web_fixture.ObservedStore.after_append = None
        web_fixture.ObservedStore.lose_issue = web_fixture.ObservedStore.lose_exchange = False
        for p in [
            patch("bot_call.runtime.Store", web_fixture.ObservedStore),
            patch("bot_call.store.time", lambda: self.now),
        ]:
            p.start()
            self.addCleanup(p.stop)

    def environment(self, platform="qq-A", cid="A"):
        event, provider = super().environment(platform, cid)
        provider.provider_config["max_context_tokens"] = 2900

        async def reviewed(kwargs):
            data = json.loads(kwargs["contexts"][-1]["content"])
            return response(RECOVERY_FINAL if data.get("kind") == "final" else "已知分块笔记")

        provider.responder = reviewed
        stt = self.context.stt[event.unified_msg_origin]
        stt.calls = stt.paths
        self.stt_providers[event.unified_msg_origin] = stt

        async def transcribed(path):
            self.stt_entered.set()
            if self.stt_mode == "gate":
                await self.stt_release.wait()
            if self.stt_mode == "stubborn":
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    self.stt_cancelled.set()
                    await self.stt_release.wait()
            return RECOVERY_TEXT

        stt.transcriber = transcribed
        return event, provider, stt

    async def selection(self, bound, snapshot):
        self.selection_calls.append(bound)
        provider = self.context.providers[bound.unified_msg_origin]
        stt = self.stt_providers[bound.unified_msg_origin]
        main = ReplyModel(provider.meta().id, provider.get_model(), 2900, 180, NATIVE_CONTRACT)
        audio = AudioRequestPolicy(
            bound, stt.meta().id, stt.get_model(), 512, 4096, NATIVE_CONTRACT
        )
        review = ReviewRequestPolicy(
            bound, main.provider_id, main.model, 2900, 300, 100, NATIVE_CONTRACT, self.counter
        )
        from bot_call.astrbot_agent import SourceAgentPolicy

        return BackendPolicies(
            SourceAgentPolicy(bound), audio, ReviewPolicySelection(review, COUNTER_RULE)
        )

    def backend(self, name="recovery.db"):
        host = self.host()

        async def send(bound, page):
            self.sends.append((bound, page.number, page.fragment))
            return Outcome.CONFIRMED

        async def readable(bound, key, body):
            self.model_reads.append(bound)
            return await host.import_context(bound, key, body)

        backend = CallBackend(
            BackendConfig(
                self.path / name,
                ORIGIN,
                3600,
                RuntimeLimits(),
                max_audio_bytes=512,
                char_budget=145,
            ),
            host.tools,
            select_policies=self.selection,
            send_page=send,
            make_context_readable=readable,
        )
        self.backends.append(backend)
        return backend

    async def request(self, app, route, **kwargs):
        return await web_fixture.WebAccessTests.request(self, app, route, **kwargs)

    def safe(self, error):
        self.assertNotIn(PRIVATE, repr(error) + "".join(traceback.format_exception(error)))

    def on_business(self):
        self.assertEqual(threading.get_ident(), self.thread)

    async def test_explicit_frozen_recovery_pending_publication_fences_and_owned_shutdown(self):
        event, provider, stt = self.environment()
        self.environment("qq-B", "B")
        reader = read_fixture.AstrBotReadAdapter(self.context, session_lock_manager=self.locks)
        template = await reader.bind(event)
        selected = await self.selection(template, await reader.refresh(template))
        rule = selected.review.counter_rule_id
        seed_counter = self.counter  # Fixture planning is not a production worker count.
        path = Path(self.directory.name) / "recovery.db"
        targets = {}
        policies = {}
        old_secret = old_session = None
        skipped_node = None
        first_page = None
        names = (
            "pending",
            "ready",
            "uncertain-page",
            "unknown-pending",
            "unknown-running",
            "unknown-uncertain",
            "live",
            "legacy",
            "mismatch",
        )
        with Store(path) as store:
            for name in names:
                bound = replace(template, call_id="restore-" + name)
                targets[name] = bound
                review = replace(
                    selected.review.policy, binding=bound, count_payload_tokens=seed_counter
                )
                manifest = CallPolicy.from_policies(
                    replace(selected.reply, binding=bound),
                    replace(selected.audio, binding=bound),
                    storage.ReviewManifest.from_policy(review, rule),
                )
                policies[name] = manifest
                store.create_call(bound, policy=None if name == "legacy" else manifest)
                store.append_user(bound.call_id, "u", RECOVERY_TEXT * 2, source="synthetic")
                if name == "pending":
                    old_secret = InvitationSecret.create()
                    store.issue_web_invitation(bound.call_id, old_secret, session_ttl=3600)
                    old_session = store.exchange_web_invitation(old_secret.value)
                if name == "live":
                    active_secret = InvitationSecret.create()
                    store.issue_web_invitation(bound.call_id, active_secret, session_ttl=3600)
                    store.exchange_web_invitation(
                        active_secret.value, _limits=RuntimeLimits(60, 5, 10)
                    )
                    continue
                store.end_call(bound.call_id, prompt="原保存的回顾提示，不取当前配置")
                if name in {"ready", "uncertain-page"}:
                    store.claim_review(bound.call_id)
                    store.complete_review(bound.call_id, RECOVERY_FINAL)
                    plan = store.prepare_publication(bound.call_id, char_budget=145)
                    page, claimed = store.claim_publication_page(bound.call_id)
                    self.assertTrue(claimed)
                    store.finish_publication_page(
                        bound.call_id, page.number, "succeeded" if name == "ready" else "uncertain"
                    )
                    if name == "ready":
                        first_page = page
                elif name in {"pending", "unknown-running", "unknown-uncertain", "mismatch"}:
                    if name == "mismatch":
                        review = replace(review, output_token_limit=301)
                    review_manifest = storage.ReviewManifest.from_policy(review, rule)
                    work = prepare_review_work(
                        store,
                        bound.call_id,
                        input_token_budget=review.input_token_budget,
                        count_tokens=review.count_request,
                        manifest=review_manifest,
                    )
                    self.assertGreater(work.map_count, 1)
                    if name != "mismatch":
                        node, claimed = store.claim_review_work_node(
                            bound.call_id,
                            work.frontier[0].source_key,
                            manifest_sha256=review_manifest.fingerprint,
                        )
                        self.assertTrue(claimed)
                        if name == "pending":
                            skipped_node = node.source_key
                            store.finish_review_work_node(
                                bound.call_id,
                                node.source_key,
                                state="succeeded",
                                body="已成功的原分块笔记",
                                manifest_sha256=review_manifest.fingerprint,
                            )
                        elif name == "unknown-uncertain":
                            store.finish_review_work_node(
                                bound.call_id,
                                node.source_key,
                                state="uncertain",
                                manifest_sha256=review_manifest.fingerprint,
                            )

        def rows(call_ids):
            with closing(sqlite3.connect(path)) as db:
                result = {}
                for (table,) in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall():
                    columns = [r[1] for r in db.execute('PRAGMA table_info("' + table + '")')]
                    if table == "audio_jobs":
                        columns = [c for c in columns if not c.startswith("series_")]
                    if "call_id" not in columns:
                        continue
                    result[table] = db.execute(
                        'SELECT * FROM "'
                        + table
                        + '" WHERE call_id IN ('
                        + ",".join("?" for _ in call_ids)
                        + ") ORDER BY "
                        + ",".join(str(i + 1) for i in range(len(columns))),
                        call_ids,
                    ).fetchall()
                return result

        all_ids = [t.call_id for t in targets.values()]
        original = rows(all_ids)
        untouched = [
            targets[n].call_id for n in names if n not in {"pending", "ready", "uncertain-page"}
        ]
        untouched_rows = rows(untouched)
        backend = self.backend(path.name)
        await backend.start()
        await backend.drain()
        await backend._runtime.resume_pending()
        await backend.drain()
        self.assertEqual(backend._runtime._accepted_calls, {})
        self.assertEqual(rows(all_ids), original)
        self.assertEqual(
            (self.sends, self.model_reads, provider.calls, stt.calls), ([], [], [], [])
        )
        registry = {}
        for name in ("missing", "live", "legacy", "mismatch"):
            call_id = "not-in-database" if name == "missing" else targets[name].call_id
            handle = backend.begin_recovery(call_id, counters=registry)
            with self.assertRaises(BackendError) as refused:
                await backend.recover(handle)
            self.assertEqual(refused.exception.code, "recovery_unavailable")
            self.safe(refused.exception)
        pending = targets["pending"]
        handle = backend.begin_recovery(pending.call_id, counters=registry)
        with self.assertRaises(BackendError) as refused:
            await backend.recover(handle)
        self.assertEqual(refused.exception.code, "counter_unavailable")
        registry[rule] = self.counter
        provider.model = "changed-after-restart"
        with self.assertRaises(BackendError):
            await backend.recover(handle, retry=True)
        provider.model = selected.review.policy.model
        conversation = self.context.conversation_manager.items.pop("A")
        try:
            with self.assertRaises(BackendError):
                await backend.recover(handle, retry=True)
        finally:
            self.context.conversation_manager.items["A"] = conversation
        state, saved = await backend._runtime.inspect_frozen_call(pending.call_id)
        wrong = CallPolicy.from_policies(
            saved.reply, replace(saved.audio, max_wav_bytes=4097), saved.review_manifest
        )
        with self.assertRaises(RuntimeOperationError):
            await backend._runtime.recover_frozen_call(pending, wrong, state.review)
        self.assertEqual(rows(all_ids), original)
        self.assertEqual(backend._runtime._accepted_calls, {})
        self.assertEqual(provider.calls, [])
        # Cancel only the recovery waiter. The retained preparation proceeds;
        # lose its actual worker ACK once and retry the same immutable source.
        entered, release = asyncio.Event(), asyncio.Event()
        self.gates.append(release)
        original_check = backend._check_recovery_target

        async def check_gate(bound):
            await original_check(bound)
            entered.set()
            await release.wait()

        original_adopt = backend._runtime.recover_frozen_call
        ack = [True]

        async def lose_ack(*args):
            value = await original_adopt(*args)
            if ack and ack.pop():
                raise RuntimeError(PRIVATE)
            return value

        with (
            patch.object(backend, "_check_recovery_target", new=check_gate),
            patch.object(backend._runtime, "recover_frozen_call", new=lose_ack),
        ):
            waiter = asyncio.create_task(backend.recover(handle, retry=True))
            await asyncio.wait_for(entered.wait(), 2)
            waiter.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await waiter
            self.assertFalse(backend._recovery_record(handle).task.done())
            async with self.locks.acquire_lock(pending.unified_msg_origin):
                conversation.history = json.dumps(
                    [{"role": "user", "content": "恢复期间原QQ新增"}], ensure_ascii=False
                )
            before_history = json.loads(conversation.history)
            self.context.conversation_manager.current[pending.unified_msg_origin] = "B"
            b_history = self.context.conversation_manager.items["B"].history
            release.set()
            with self.assertRaises(BackendError) as lost:
                await backend.recover(handle)
            self.safe(lost.exception)
            self.assertEqual(await backend.recover(handle, retry=True), pending)
        self.assertIs(backend.begin_recovery(pending.call_id, counters=registry), handle)
        with self.assertRaises(BackendError):
            await backend.invitation(handle)
        await backend.drain()
        state = await backend.read_state(handle)
        self.assertTrue(state.publication.complete)
        self.assertEqual(state.review.body, RECOVERY_FINAL)
        self.assertEqual(
            [t["body"] for t in json.loads(state.review.input_json)], [RECOVERY_TEXT * 2]
        )
        sources = [
            json.loads(payload["contexts"][1]["content"])["source_key"]
            for payload in provider.calls
        ]
        self.assertNotIn(skipped_node, sources)
        self.assertEqual(len(sources), len(set(sources)))
        self.assertEqual(state.review_work.manifest, policies["pending"].review_manifest)
        imported = json.loads(conversation.history)
        self.assertEqual(imported[: len(before_history)], before_history)
        self.assertEqual(imported[-1], {"role": "assistant", "content": RECOVERY_FINAL})
        self.assertEqual(self.context.conversation_manager.current[pending.unified_msg_origin], "B")
        self.assertEqual(self.context.conversation_manager.items["B"].history, b_history)
        self.assertEqual(rows(untouched), untouched_rows)
        self.assertEqual(
            {web_fixture.ObservedStore.instances[0].owner},
            {web_fixture.ObservedStore.instances[0].owner},
        )
        self.assertFalse(backend._invitations)
        self.assertFalse(backend._sessions)
        for method, args in [
            (backend._runtime.web_state, (old_session.token, old_session.reference)),
            (backend._runtime.exchange_web_invitation, (old_secret.value,)),
            (backend._runtime.submit_utterance, (pending.call_id, "late", PRIVATE)),
            (backend._runtime.end_call, (pending.call_id,)),
        ]:
            with self.assertRaises((AccessError, RuntimeOperationError)):
                await method(*args)
        self.assertEqual(
            (await self.request(backend.app, "exchange", data={"invitation": old_secret.value}))[0],
            401,
        )
        self.assertEqual(
            (
                await self.request(
                    backend.app,
                    "state",
                    cookie="bot_call=" + old_session.token,
                    reference=old_session.reference,
                )
            )[0],
            401,
        )
        # A successful review needs no reply/STT/review Provider or counter.
        self.stt_providers.clear()
        self.context.providers.clear()
        self.context.by_id.clear()
        sdk_count = len(provider.calls)
        selected_count = len(self.selection_calls)
        ready = backend.begin_recovery(targets["ready"].call_id, counters={})
        await backend.recover(ready)
        await backend.drain()
        self.assertTrue((await backend.read_state(ready)).publication.complete)
        ready_counts = (len(self.sends), len(self.model_reads))
        await backend.recover(ready, retry=True)
        await backend.drain()
        self.assertEqual((len(self.sends), len(self.model_reads)), ready_counts)
        ready_sends = [
            (number, fragment)
            for bound, number, fragment in self.sends
            if bound == targets["ready"]
        ]
        self.assertNotIn(first_page.number, [n for n, _ in ready_sends])
        self.assertEqual(first_page.fragment + "".join(f for _, f in ready_sends), RECOVERY_FINAL)
        self.assertEqual(len(provider.calls), sdk_count)
        self.assertEqual(len(self.selection_calls), selected_count)
        self.assertEqual(stt.calls, [])
        # An actual publication callback swallows cancellation. Keep resources
        # until it really exits, and never retry its uncertain result on reopen.
        context_entered, cancelled, context_release = (
            asyncio.Event(),
            asyncio.Event(),
            asyncio.Event(),
        )
        self.gates.append(context_release)

        async def stubborn_context(bound, key, body):
            self.on_business()
            self.assertEqual((bound, body), (targets["uncertain-page"], RECOVERY_FINAL))
            context_entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                await context_release.wait()
            return Outcome.UNCERTAIN

        backend._make_context = stubborn_context
        uncertain = backend.begin_recovery(targets["uncertain-page"].call_id, counters={})
        await backend.recover(uncertain)
        await asyncio.wait_for(context_entered.wait(), 2)
        self.assertFalse(any(bound == targets["uncertain-page"] for bound, _, _ in self.sends))
        report = await backend.shutdown(timeout=0.03)
        await asyncio.wait_for(cancelled.wait(), 2)
        self.assertEqual(report.error_code, "still_stopping")
        self.assertFalse(report.stopped)
        self.assertTrue(backend._runtime._thread.is_alive())
        self.assertTrue(backend._runtime._core_tasks)
        waiting = asyncio.create_task(backend.shutdown(timeout=None))
        await asyncio.sleep(0)
        waiting.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiting
        self.assertFalse(backend._close_task.done())
        context_release.set()
        self.assertTrue((await backend.shutdown(timeout=3)).stopped)
        counts = (len(provider.calls), len(self.sends), len(self.model_reads))
        reopened = self.backend(path.name)
        await reopened.start()
        for name in ("pending", "ready", "uncertain-page"):
            h = reopened.begin_recovery(targets[name].call_id, counters={})
            await reopened.recover(h)
        await reopened.drain()
        self.assertEqual((len(provider.calls), len(self.sends), len(self.model_reads)), counts)
        self.assertEqual(rows(untouched), untouched_rows)
        after = rows(all_ids)
        for table in ("web_invitations", "web_sessions", "call_limits"):
            self.assertEqual(
                after[table], original[table]
            )  # No ticket adoption, renewal or old-slot release.
        with Store(path) as store:
            for name in ("pending", "ready", "uncertain-page"):
                self.assertEqual(store.get_call_policy(targets[name].call_id), policies[name])
                self.assertEqual(store.get_review(targets[name].call_id).body, RECOVERY_FINAL)
            blocked = store.get_publication(targets["uncertain-page"].call_id)
            self.assertEqual(
                (blocked.pages[0].state, blocked.context_state), ("uncertain", "uncertain")
            )
            self.assertFalse(blocked.complete)
        self.assertFalse(
            backend._runtime._core_tasks
            if backend._close_task and backend._close_task.done()
            else []
        )

    async def test_closing_recovery_finishes_received_tail_with_saved_source_and_no_reply(self):
        event, provider, stt = self.environment()
        self.environment("qq-B", "B")
        reader = read_fixture.AstrBotReadAdapter(self.context, session_lock_manager=self.locks)
        template = await reader.bind(event)
        selected = await self.selection(template, await reader.refresh(template))
        rule = selected.review.counter_rule_id
        front = "  前段原文🙂\r\n用户仍犹豫，未接受建议。  "
        prompt = "  原始挂断prompt🙂\n必须保留前段、尾段和不确定性。  "
        path = Path(self.directory.name) / "closing-recovery.db"
        targets = {}
        policies = {}
        closes = {}
        old_secret = old_session = None
        names = (
            "pending",
            "running",
            "uncertain",
            "await-freeze",
            "live",
            "legacy",
            "corrupt",
            "unknown",
        )
        with Store(path) as store:
            for name in names:
                bound = replace(template, call_id="closing-" + name)
                targets[name] = bound
                saved = CallPolicy.from_policies(
                    replace(selected.reply, binding=bound),
                    replace(selected.audio, binding=bound),
                    storage.ReviewManifest.from_policy(
                        replace(selected.review.policy, binding=bound), rule
                    ),
                )
                policies[name] = saved
                store.create_call(
                    bound,
                    policy=None if name == "legacy" else saved,
                    retained_missing=name == "pending",
                )
                if name == "pending":
                    store.receive_audio(
                        bound.call_id, "blank", b"retained-empty-front", "audio/wav"
                    )
                    store.claim_next_audio(bound.call_id)
                    store.finish_audio(
                        bound.call_id, "blank", state="uncertain", error_code="result_uncertain"
                    )
                    store.retain_audio_missing(bound.call_id, "blank")
                store.receive_audio(bound.call_id, "front", b"original-front", "audio/wav")
                _, claimed = store.claim_next_audio(bound.call_id)
                self.assertTrue(claimed)
                store.complete_audio(bound.call_id, "front", front)
                store.claim_generation(
                    bound.call_id, "reply:front", utterance_id="front", source="fake_reply"
                )
                store.complete_generation(bound.call_id, "reply:front", BOT)
                if name == "corrupt":
                    # A malformed existing source, inserted only in this synthetic fixture.
                    data = b"bad-tail"
                    store._db.execute(
                        "INSERT INTO audio_jobs (call_id,utterance_id,ordinal,mime,sha256,byte_count,received_at,state,data,user_sequence,result_sha256,error_code) VALUES (?,?,?,?,?,?,?,'pending',?,NULL,NULL,NULL)",
                        (
                            bound.call_id,
                            "tail",
                            2,
                            "audio/webm",
                            hashlib.sha256(b"expected").hexdigest(),
                            len(data),
                            self.now,
                            data,
                        ),
                    )
                else:
                    store.receive_audio(bound.call_id, "tail", b"received-final-clip", "audio/webm")
                if name in {"pending", "live"}:
                    secret = InvitationSecret.create()
                    store.issue_web_invitation(bound.call_id, secret, session_ttl=3600)
                    session = store.exchange_web_invitation(
                        secret.value, _limits=RuntimeLimits(60, 5, 10)
                    )
                    if name == "pending":
                        old_secret, old_session = secret, session
                if name == "live":
                    continue
                receipt = store.end_call(bound.call_id, prompt=prompt, _request_close=True)
                self.assertEqual(receipt.state, "waiting_for_audio")
                closes[name] = store.get_close_request(bound.call_id)
                if name in {"running", "uncertain", "await-freeze"}:
                    _, claimed = store.claim_next_audio(bound.call_id)
                    self.assertTrue(claimed)
                    if name == "uncertain":
                        store.finish_audio(
                            bound.call_id, "tail", state="uncertain", error_code="result_uncertain"
                        )
                    elif name == "await-freeze":
                        store.complete_audio(bound.call_id, "tail", RECOVERY_TEXT)

        def rows(call_ids):
            with closing(sqlite3.connect(path)) as db:
                result = {}
                for (table,) in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall():
                    columns = [r[1] for r in db.execute('PRAGMA table_info("' + table + '")')]
                    if table == "audio_jobs":
                        columns = [c for c in columns if not c.startswith("series_")]
                    if "call_id" not in columns:
                        continue
                    result[table] = db.execute(
                        'SELECT * FROM "'
                        + table
                        + '" WHERE call_id IN ('
                        + ",".join("?" for _ in call_ids)
                        + ") ORDER BY "
                        + ",".join(str(i + 1) for i in range(len(columns))),
                        call_ids,
                    ).fetchall()
                return result

        ids = [t.call_id for t in targets.values()]
        initial = rows(ids)
        untouched = [targets[n].call_id for n in names if n not in {"pending", "await-freeze"}]
        original_untouched = rows(untouched)
        backend = self.backend(path.name)
        await backend.start()
        await backend.drain()
        await backend._runtime.resume_pending()
        await backend.drain()
        self.assertEqual(backend._runtime._accepted_calls, {})
        self.assertEqual(rows(ids), initial)
        self.assertEqual((stt.calls, provider.calls), ([], []))
        registry = {rule: self.counter}
        for name in ("live", "legacy", "corrupt"):
            h = backend.begin_recovery(targets[name].call_id, counters=registry)
            with self.assertRaises(BackendError) as denied:
                await backend.recover(h)
            self.assertEqual(denied.exception.code, "recovery_unavailable")
            self.safe(denied.exception)
        pending = targets["pending"]
        handle = backend.begin_recovery(pending.call_id, counters=registry)
        # Saved STT identity is mandatory; no default or changed Provider fallback.
        # begin() has scheduled but cannot run until this coroutine first yields.
        stt_provider = self.stt_providers[pending.unified_msg_origin]
        original_model = stt_provider.model
        stt_provider.model = "changed-stt-model"
        with self.assertRaises(BackendError):
            await backend.recover(handle)
        stt_provider.model = original_model
        conversation = self.context.conversation_manager.items["A"]
        owner = conversation.user_id
        conversation.user_id = "wrong-origin"
        try:
            with self.assertRaises(BackendError):
                await backend.recover(handle, retry=True)
        finally:
            conversation.user_id = owner
        state, saved, source = await backend._runtime.inspect_recovery_call(pending.call_id)
        wrong = CallPolicy.from_policies(
            saved.reply, replace(saved.audio, max_wav_bytes=4097), saved.review_manifest
        )
        with self.assertRaises(RuntimeOperationError):
            await backend._runtime.recover_closing_call(pending, wrong, source)
        with self.assertRaises(RuntimeOperationError):
            await backend._runtime.recover_closing_call(
                pending,
                saved,
                replace(source, close=replace(source.close, prompt="changed close prompt")),
            )
        self.assertEqual(rows(ids), initial)
        self.assertEqual(backend._runtime._accepted_calls, {})
        self.assertEqual((stt.calls, provider.calls), ([], []))
        entered, release = asyncio.Event(), asyncio.Event()
        self.gates.append(release)
        original_check = backend._check_recovery_target

        async def check_gate(bound):
            await original_check(bound)
            entered.set()
            await release.wait()

        original_adopt = backend._runtime.recover_closing_call
        lose = [True]

        async def lose_ack(*args):
            value = await original_adopt(*args)
            if lose and lose.pop():
                await self.stt_entered.wait()
                raise RuntimeError(PRIVATE)
            return value

        self.stt_mode = "gate"
        self.stt_release.clear()
        self.stt_entered.clear()
        with (
            patch.object(backend, "_check_recovery_target", new=check_gate),
            patch.object(backend._runtime, "recover_closing_call", new=lose_ack),
        ):
            waiter = asyncio.create_task(backend.recover(handle, retry=True))
            await asyncio.wait_for(entered.wait(), 2)
            waiter.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await waiter
            self.assertFalse(backend._recovery_record(handle).task.done())
            release.set()
            with self.assertRaises(BackendError) as lost:
                await backend.recover(handle)
            self.safe(lost.exception)
            retained = backend._recovery_record(handle).closing_source
            self.assertEqual(retained, source)
            self.assertNotIn(prompt, repr(retained))
            during = await backend.read_state(handle)
            self.assertEqual([t.body for t in during.turns], [front, BOT])
            self.assertIsNone(during.review)
            self.assertEqual(
                [j.state for j in during.audio_jobs], ["uncertain", "succeeded", "running"]
            )
            self.assertEqual(await backend.recover(handle, retry=True), pending)
            self.assertEqual(len(stt.calls), 1)  # Running tail is not claimed a second time.
            async with self.locks.acquire_lock(pending.unified_msg_origin):
                conversation.history = json.dumps(
                    [{"role": "user", "content": "尾段转写期间原QQ新增"}], ensure_ascii=False
                )
            history = json.loads(conversation.history)
            self.context.conversation_manager.current[pending.unified_msg_origin] = "B"
            b_history = self.context.conversation_manager.items["B"].history
            self.stt_release.set()
        await backend.drain()
        state = await backend.read_state(handle)
        self.assertEqual([t.body for t in state.turns], [front, BOT, RECOVERY_TEXT])
        self.assertEqual(
            [t["body"] for t in json.loads(state.review.input_json)], [front, BOT, RECOVERY_TEXT]
        )
        self.assertEqual(state.review.ended_at, closes["pending"].requested_at)
        self.assertTrue(state.review.prompt.startswith(prompt))
        self.assertIn("缺失的相对位置", state.review.prompt)
        self.assertEqual(state.close_request, closes["pending"])
        self.assertTrue(state.publication.complete)
        self.assertEqual(state.review.body, RECOVERY_FINAL)
        self.assertEqual(len(stt.calls), 1)
        self.assertTrue(all(audio_settled(j) for j in state.audio_jobs))
        self.assertEqual(state.audio_jobs[0].data, b"retained-empty-front")
        self.assertEqual(state.audio_jobs[0].error_code, "result_uncertain")
        self.assertEqual(state.audio_jobs[0].resolution.decision, "retained_missing")
        tail = next(g for g in state.generations if g.generation_id == "reply:tail")
        self.assertEqual(tail.state, "cancelled")
        for payload in provider.calls:
            request = json.loads(payload["contexts"][1]["content"])
            self.assertTrue(
                request["format"].startswith("bot-call-review-")
            )  # No live reply requests.
            self.assertEqual(request["review_prompt"], state.review.prompt)
            self.assertEqual(provider.get_model(), selected.review.policy.model)
        self.assertEqual(json.loads(conversation.history)[: len(history)], history)
        self.assertEqual(
            json.loads(conversation.history)[-1],
            {"role": "assistant", "content": state.publication.body},
        )
        self.assertTrue(state.publication.body.startswith("材料说明："))
        self.assertTrue(state.publication.body.endswith(RECOVERY_FINAL))
        self.assertEqual(self.context.conversation_manager.current[pending.unified_msg_origin], "B")
        self.assertEqual(self.context.conversation_manager.items["B"].history, b_history)
        for method, args, kwargs in [
            (backend._runtime.web_state, (old_session.token, old_session.reference), {}),
            (backend._runtime.exchange_web_invitation, (old_secret.value,), {}),
            (
                backend._runtime.issue_web_invitation,
                (pending.call_id, old_secret),
                {"session_ttl": 3600},
            ),
            (backend._runtime.submit_utterance, (pending.call_id, "late", PRIVATE), {}),
            (backend._runtime.end_call, (pending.call_id,), {}),
        ]:
            with self.assertRaises((AccessError, RuntimeOperationError)):
                await method(*args, **kwargs)
        self.assertEqual(
            (await self.request(backend.app, "exchange", data={"invitation": old_secret.value}))[0],
            401,
        )
        self.assertEqual(
            (
                await self.request(
                    backend.app,
                    "state",
                    cookie=web_fixture.COOKIE + "=" + old_session.token,
                    reference=old_session.reference,
                )
            )[0],
            401,
        )
        with self.assertRaises(BackendError):
            await backend.invitation(handle)
        self.assertFalse(backend._invitations)
        self.assertFalse(backend._sessions)
        # A process can have died after audio COMMIT but before freezing. It can
        # finish from the same close without any STT Provider, using original text.
        self.stt_providers.clear()
        ready = backend.begin_recovery(targets["await-freeze"].call_id, counters=registry)
        await backend.recover(ready)
        await backend.drain()
        frozen = await backend.read_state(ready)
        self.assertTrue(frozen.publication.complete)
        self.assertEqual([t.body for t in frozen.turns], [front, BOT, RECOVERY_TEXT])
        self.assertEqual(
            (frozen.review.prompt, frozen.review.ended_at),
            (prompt, closes["await-freeze"].requested_at),
        )
        self.assertEqual(len(stt.calls), 1)
        self.context.providers.clear()
        self.context.by_id.clear()
        counts = (len(stt.calls), len(provider.calls), len(self.sends), len(self.model_reads))
        for name in ("running", "uncertain"):
            blocked = backend.begin_recovery(targets[name].call_id, counters={})
            await backend.recover(blocked)
            await backend.drain()
            blocked_state = await backend.read_state(blocked)
            self.assertIsNone(blocked_state.review)
            self.assertEqual(blocked_state.audio_jobs[-1].state, name)
        await backend.recover(handle, retry=True)
        await backend.drain()  # Original closing handle, now frozen.
        self.assertEqual(backend._recovery_record(handle).closing_source, retained)
        self.assertEqual(
            (len(stt.calls), len(provider.calls), len(self.sends), len(self.model_reads)), counts
        )
        self.assertEqual(rows(untouched), original_untouched)
        after = rows(ids)
        for table in (
            "calls",
            "call_policies",
            "call_closures",
            "web_invitations",
            "web_sessions",
            "call_limits",
        ):
            self.assertEqual(after[table], initial[table])
        self.assertFalse(
            backend._runtime._core_tasks
            if backend._close_task and backend._close_task.done()
            else []
        )
        self.assertEqual(
            {web_fixture.ObservedStore.instances[0].owner},
            {web_fixture.ObservedStore.instances[0].owner},
        )
        self.assertTrue((await backend.shutdown(timeout=None)).stopped)
        self.assertFalse(backend._recovery_record(handle).audio._tasks)
        self.assertTrue(all(not d.exists() for d in self.dirs))
        reopened = self.backend(path.name)
        await reopened.start()
        h = reopened.begin_recovery(pending.call_id, counters={})
        await reopened.recover(h)
        await reopened.drain()
        self.assertEqual(
            (len(stt.calls), len(provider.calls), len(self.sends), len(self.model_reads)), counts
        )
        self.assertIsNone(reopened._recovery_record(h).audio)
        self.assertEqual((await reopened.read_state(h)).review.body, RECOVERY_FINAL)
        self.assertEqual(rows(untouched), original_untouched)

    async def exchange(self, backend, grant):
        result = await self.request(
            backend.app, "exchange", data={"invitation": grant.secret.value}
        )
        self.assertEqual(result[0], 200)
        cookie = result[2][b"set-cookie"].decode().split(";", 1)[0]
        for value in [b"Secure", b"HttpOnly", b"SameSite=Strict", b"Path=/bot-call/"]:
            self.assertIn(value, result[2][b"set-cookie"])
        return cookie, result[1]["reference"]

    async def test_failed_cancelled_preparation_ack_loss_old_db_and_actual_shutdown(self):
        from contextlib import contextmanager
        import subprocess, sys
        from bot_call.runtime import CallRuntime
        from test_astrbot_context import binding
        import test_limits as limits_fixture

        self.wakeups = limits_fixture.Wakeups()

        def scoped_runtime(*args, **kwargs):
            kwargs["_limit_timer"] = self.wakeups
            return CallRuntime(*args, **kwargs)

        p = patch("bot_call.backend.CallRuntime", side_effect=scoped_runtime)
        p.start()
        self.addCleanup(p.stop)
        self.select_mode = "good"
        self.reply_mode = "good"
        self.send_mode = "good"
        self.select_entered = asyncio.Event()
        self.select_cancelled = asyncio.Event()
        self.select_release = asyncio.Event()
        self.gates.append(self.select_release)
        original_selection = self.selection

        async def select(bound, snapshot):
            if self.select_mode in {"gate", "stubborn"}:
                self.select_entered.set()
                try:
                    await self.select_release.wait()
                except asyncio.CancelledError:
                    self.select_cancelled.set()
                    if self.select_mode != "stubborn":
                        raise
                    await self.select_release.wait()
            result = await original_selection(bound, snapshot)
            return (
                replace(result, audio=replace(result.audio, max_audio_bytes=511))
                if self.select_mode == "bad"
                else result
            )

        self.selection = select
        original_converter = self.converter

        async def converter(source, **kwargs):
            self.dirs.append(Path(source).parent)
            return await original_converter(source, **kwargs)

        self.converter = converter
        original_environment = self.environment

        def environment(*args):
            event, provider, stt = original_environment(*args)

            async def respond(kwargs):
                try:
                    data = json.loads(kwargs["contexts"][-1]["content"])
                except (ValueError, KeyError, TypeError):
                    data = {}
                if data.get("format", "").startswith("bot-call-review-"):
                    return response(
                        RECOVERY_FINAL if data.get("kind") == "final" else "已知分块笔记"
                    )
                if self.reply_mode == "unknown":
                    raise RuntimeError(PRIVATE)
                return response(BOT)

            provider.responder = respond
            return event, provider, stt

        self.environment = environment
        original_backend = self.backend

        def backend(name):
            b = original_backend(name)
            original_send = b._send_page

            async def send(bound, page):
                result = await original_send(bound, page)
                if self.send_mode == "unknown":
                    raise RuntimeError(PRIVATE)
                return result

            b._send_page = send
            return b

        self.backend = backend
        from bot_call.observability import StageTimings, CAPACITY
        from bot_call.runtime import RuntimeCallbacks, RuntimeAudio

        tick = [0]

        def clock():
            tick[0] += 1_000_000
            return tick[0]

        timings = StageTimings(clock=clock)
        mode = ["ok"]
        entered, cleanup, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        self.gates.append(release)

        async def callback(*args):
            if mode[0] == "invalid":
                return None
            if mode[0] == "unknown":
                raise RuntimeError(PRIVATE)
            if mode[0] == "cancel":
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cleanup.set()
                    await release.wait()
            return "synthetic"

        async def wav(*args):
            return b"RIFF" + bytes(4) + b"WAVE" + bytes(32)

        observed = CallRuntime(
            Path(self.directory.name) / "not-opened.db",
            RuntimeCallbacks(callback, callback, callback, callback, wav),
            audio=RuntimeAudio(callback),
            timings=timings,
        )
        self.assertEqual((await observed._invoke(object(), "transcribe", ())).kind, "value")
        self.assertEqual((await observed._invoke(object(), "synthesize", ())).kind, "value")
        mode[0] = "invalid"
        self.assertIsNone((await observed._invoke(object(), "reply", ())).value)
        mode[0] = "unknown"
        self.assertEqual((await observed._invoke(object(), "review", ())).kind, "error")
        mode[0] = "cancel"
        token = object()
        pending = asyncio.create_task(observed._invoke(token, "reply", ()))
        await entered.wait()
        observed._cancel_core(token)
        await cleanup.wait()
        self.assertFalse(pending.done())
        self.assertEqual(timings.snapshot()["sample_count"], 4)
        release.set()
        self.assertEqual((await pending).kind, "cancelled")
        values = {row["stage"]: row for row in timings.snapshot()["stages"]}
        self.assertEqual(values["stt"]["outcomes"]["success"], 1)
        self.assertEqual(values["tts"]["outcomes"]["success"], 1)
        self.assertEqual(values["reply"]["outcomes"]["failed"], 1)
        self.assertEqual(values["reply"]["outcomes"]["cancelled"], 1)
        self.assertEqual(values["review"]["outcomes"]["uncertain"], 1)
        self.assertNotIn(PRIVATE, json.dumps(timings.snapshot()))

        # Both clock/collector entry and finish failures are isolated from the
        # exact same actual callback results and real cancellation ownership.
        class Broken:
            def begin(self, stage):
                raise RuntimeError(PRIVATE)

            def finish(self, *args):
                raise RuntimeError(PRIVATE)

        observed._timings = Broken()
        mode[0] = "ok"
        self.assertEqual((await observed._invoke(object(), "reply", ())).value, "synthetic")
        mode[0] = "unknown"
        self.assertEqual((await observed._invoke(object(), "reply", ())).kind, "error")
        for _ in range(CAPACITY + 10):
            timings.finish("reply", timings.begin("reply"), "success")
        self.assertEqual(timings.snapshot()["sample_count"], CAPACITY)
        self.assertEqual(timings.snapshot()["stages"][0]["last_ms"], 1)
        timings.close()
        timings.finish("reply", 0, "success")
        self.assertEqual(timings.snapshot()["sample_count"], 0)
        self.assertFalse((Path(self.directory.name) / "not-opened.db").exists())
        self.assertFalse(observed._core_tasks)
        event, pa, stt = self.environment()
        event_b, pb, stt_b = self.environment("qq-B", "B")
        # One real project DB with unclaimed success/pending/unknown work and
        # already-due limits. Opening/resuming/new progress must leave ALL old
        # call rows intact, not just avoid old external callbacks.
        old_path = Path(self.directory.name) / "old.db"
        old_ids = []
        # Shape only this newly created synthetic fixture to the retained v8
        # schema after seeding with current material methods. This is not an old
        # Store binary; genuine previous-source coverage lives in records test.
        with Store(old_path) as store:
            for name in ["pending", "running", "uncertain", "no-plan"]:
                target = binding("old-publication-" + name)
                old_ids.append(target.call_id)
                store.create_call(target)
                store.append_user(target.call_id, "u", PRIVATE, source="synthetic")
                store.end_call(target.call_id, prompt="synthetic prompt")
                store.claim_review(target.call_id)
                store.complete_review(target.call_id, RECOVERY_FINAL)
                if name == "no-plan":
                    continue
                publication = store.prepare_publication(target.call_id, char_budget=95)
                if name == "running":
                    page, claimed = store.claim_publication_page(target.call_id)
                    self.assertTrue(claimed)
                    store.finish_publication_page(target.call_id, page.number, "succeeded")
                    _, claimed = store.claim_publication_page(target.call_id)
                    self.assertTrue(claimed)  # Leave the next page actually running.
                    store.claim_publication_context(target.call_id)
                    store.finish_publication_context(target.call_id, "succeeded")
                if name == "uncertain":
                    page, claimed = store.claim_publication_page(target.call_id)
                    self.assertTrue(claimed)
                    store.finish_publication_page(target.call_id, page.number, "uncertain")
                    store.claim_publication_context(target.call_id)
                    store.finish_publication_context(target.call_id, "uncertain")
            for name in ["pending", "running"]:
                target = binding("old-review-" + name)
                old_ids.append(target.call_id)
                store.create_call(target)
                store.append_user(target.call_id, "u", PRIVATE, source="synthetic")
                store.end_call(target.call_id, prompt="old frozen prompt")
                if name == "running":
                    work = prepare_review_work(
                        store, target.call_id, input_token_budget=4000, count_tokens=len
                    )
                    _, claimed = store.claim_review_work_node(
                        target.call_id, work.frontier[0].source_key
                    )
                    self.assertTrue(claimed)
                target = binding("old-audio-" + name)
                old_ids.append(target.call_id)
                store.create_call(target)
                store.receive_audio(target.call_id, "audio", b"old-original-audio", "audio/wav")
                if name == "running":
                    _, claimed = store.claim_next_audio(target.call_id)
                    self.assertTrue(claimed)
            # This unknown call's earlier deadline is long overdue. Even its
            # pending audio must not be closed/claimed or arm a zero-delay loop.
            self.now = 500.0
            expired = binding("old-expired-limit")
            old_ids.append(expired.call_id)
            store.create_call(expired)
            old_secret = InvitationSecret.create()
            store.issue_web_invitation(expired.call_id, old_secret, session_ttl=20)
            store.exchange_web_invitation(old_secret.value, _limits=RuntimeLimits(60, 5, 10))
            store.receive_audio(expired.call_id, "audio", b"old-waiting-close", "audio/wav")
            # A different old session still occupies admission at t=1000. Its
            # permission expiry, not our scope, releases the original web slot.
            self.now = 1000.0
            active = binding("old-active")
            old_ids.append(active.call_id)
            store.create_call(active)
            active_secret = InvitationSecret.create()
            store.issue_web_invitation(active.call_id, active_secret, session_ttl=15)
            old_session = store.exchange_web_invitation(active_secret.value)
            # Remove only known post-v8 tables from this owned synthetic DB.
            store._db.execute("DROP TRIGGER review_query_barrier")
            store._db.execute("DROP TRIGGER review_audio_barrier")
            for table in (
                "turn_timings",
                "review_agent_notes",
                "agent_runs",
                "review_query_notes",
                "query_attempts",
                "query_policies",
                "review_audio_notes",
                "audio_resolutions",
                "audio_missing_policies",
                "speech_jobs",
                "call_policies",
            ):
                self.assertEqual(
                    store._db.execute("SELECT count(*) FROM " + table).fetchone()[0], 0
                )
                store._db.execute("DROP TABLE " + table)
            store._db.execute(
                "CREATE TRIGGER review_audio_barrier BEFORE INSERT ON reviews WHEN EXISTS(SELECT 1 FROM audio_jobs WHERE call_id=NEW.call_id AND state!='succeeded') BEGIN SELECT RAISE(ABORT,'received audio is unresolved'); END"
            )
            from bot_call.retention import audio_progress_trigger
            from schema_fixture import remove_series_columns

            store._db.execute("DROP TRIGGER audio_progress")
            store._db.execute(audio_progress_trigger())

            remove_series_columns(store._db)
            from schema_fixture import remove_retry_schema

            remove_retry_schema(store._db)
            store._db.execute("PRAGMA user_version=8")
        old_path.chmod(0o640)

        def snapshot():
            with closing(sqlite3.connect(old_path)) as db:
                result = {}
                tables = [
                    r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")
                ]
                for table in tables:
                    if table.startswith("review_retry_"):
                        self.assertEqual(
                            db.execute('SELECT count(*) FROM "' + table + '"').fetchone()[0], 0
                        )
                        continue
                    columns = [r[1] for r in db.execute('PRAGMA table_info("' + table + '")')]
                    if table == "audio_jobs":
                        columns = [c for c in columns if not c.startswith("series_")]
                    if "call_id" not in columns:
                        continue
                    order = ",".join(str(i + 1) for i in range(len(columns)))
                    rows = db.execute(
                        "SELECT "
                        + ",".join(columns)
                        + ' FROM "'
                        + table
                        + '" WHERE call_id IN ('
                        + ",".join("?" for _ in old_ids)
                        + ") ORDER BY "
                        + order,
                        old_ids,
                    ).fetchall()
                    if table in {
                        "barge_candidates",
                        "turn_timings",
                        "agent_runs",
                        "review_agent_notes",
                        "call_policies",
                        "speech_jobs",
                        "audio_missing_policies",
                        "audio_resolutions",
                        "review_audio_notes",
                        "query_policies",
                        "query_attempts",
                        "review_query_notes",
                    }:
                        self.assertEqual(rows, [])  # Upgrade never guesses a legacy policy.
                    else:
                        result[table] = rows
                return result

        original = snapshot()
        with closing(sqlite3.connect(old_path)) as db:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 8)
            self.assertIsNone(
                db.execute("SELECT name FROM sqlite_master WHERE name='call_policies'").fetchone()
            )
        backend = self.backend("old.db")
        await backend.start()
        self.assertEqual(backend._runtime._accepted_calls, {})
        with closing(sqlite3.connect(old_path)) as db:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 24)
        for call_id in old_ids:
            self.assertIsNone(await backend._runtime.read_call_policy(call_id))
        self.assertEqual(backend._runtime._accepted_calls, {})
        self.assertEqual(backend._runtime._recovered_limit_calls, set())
        self.assertEqual(self.wakeups.active(), [])
        await backend.drain()
        await backend._runtime.resume_pending()
        await backend.drain()
        self.assertEqual(self.wakeups.active(), [])
        self.assertEqual(snapshot(), original)
        diagnostic = await backend._runtime.read_state("old-publication-pending")
        self.assertEqual(
            diagnostic.review.body, RECOVERY_FINAL
        )  # Trusted read-only diagnostic only.
        for method, args, kwargs in [
            (backend._runtime.submit_utterance, ("old-audio-pending", "new", PRIVATE), {}),
            (backend._runtime.end_call, (expired.call_id,), {}),
            (
                backend._runtime.issue_web_invitation,
                (active.call_id, active_secret),
                {"session_ttl": 15},
            ),
            (backend._runtime.exchange_web_invitation, (active_secret.value,), {}),
            (backend._runtime.web_state, (old_session.token, old_session.reference), {}),
            (backend._runtime.web_heartbeat, (old_session.token, old_session.reference), {}),
            (backend._runtime.web_end, (old_session.token, old_session.reference), {}),
        ]:
            with self.assertRaises((RuntimeOperationError, AccessError)) as caught:
                await method(*args, **kwargs)
            # Runtime's existing Future boundary creates a fixed exception with
            # no context; it is not an exception raised inside another handler.
            self.assertIsNone(caught.exception.__context__)
            self.assertNotIn(PRIVATE, "".join(traceback.format_exception(caught.exception)))
        self.assertEqual(snapshot(), original)
        denied = await self.request(
            backend.app, "exchange", data={"invitation": active_secret.value}
        )
        self.assertEqual(denied[0], 401)
        self.assertEqual(self.selection_calls, [])
        self.assertEqual((self.sends, self.model_reads, pa.calls, stt.calls), ([], [], [], []))
        # File-system rejections happen without rewriting any old file. Foreign
        # SQLite ownership is still checked by the sole Runtime Store worker.
        alias = Path(self.directory.name) / "alias.db"
        alias.symlink_to(old_path)
        directory = Path(self.directory.name) / "directory.db"
        directory.mkdir()
        foreign = Path(self.directory.name) / "foreign.db"
        with closing(sqlite3.connect(foreign)) as db:
            db.execute("CREATE TABLE unrelated(value TEXT)")
        foreign_sha = hashlib.sha256(foreign.read_bytes()).hexdigest()
        for name in [alias.name, directory.name, foreign.name]:
            refused = self.backend(name)
            with self.assertRaises(BackendError) as caught:
                await refused.start()
            self.safe(caught.exception)
            self.assertEqual((await refused.shutdown()).error_code, "startup_failed")
            if refused._runtime is None:
                self.backends.remove(refused)
        self.assertEqual(hashlib.sha256(foreign.read_bytes()).hexdigest(), foreign_sha)
        self.assertEqual(old_path.stat().st_mode & 0o777, 0o640)
        self.select_mode = "bad"
        handle = backend.begin(event)
        with self.assertRaises(BackendError) as caught:
            await backend.invitation(handle)
        self.safe(caught.exception)
        record = backend._record(handle)
        bound, secret = record.binding, record.secret
        self.assertIsNone(record.grant)
        self.assertFalse(backend._invitations)
        with self.assertRaises(RuntimeOperationError):
            await backend._runtime.read_state(bound.call_id)
        self.assertEqual((len(pa.calls), len(stt.calls)), (0, 0))
        # Retry retains original fixed binding despite switching current cid.
        self.context.conversation_manager.current[bound.unified_msg_origin] = "B"
        self.select_mode = "gate"
        waiter = asyncio.create_task(backend.invitation(handle, retry=True))
        await asyncio.wait_for(self.select_entered.wait(), 2)
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        self.assertFalse(record.task.done())
        self.assertIs(record.binding, bound)
        self.assertIs(record.secret, secret)
        original_create = web_fixture.ObservedStore.create_call
        attempts = []
        staged = []
        mode = "commit_fail"

        def checked_create(store, target, **kwargs):
            if target.call_id != bound.call_id:
                return original_create(store, target, **kwargs)
            attempts.append(kwargs["policy"])
            if mode == "commit_fail":
                store._db.execute("""CREATE TEMP TRIGGER fail_policy_commit AFTER INSERT ON call_policies
                    BEGIN INSERT INTO generations VALUES
                    ('synthetic-missing-parent','synthetic-generation',1,'synthetic','failed',NULL); END""")
                store._db.execute("PRAGMA defer_foreign_keys=ON")
                original_transaction = store._transaction

                @contextmanager
                def before_commit():
                    with original_transaction():
                        yield
                        staged.append(
                            tuple(
                                store._db.execute(
                                    "SELECT count(*) FROM " + table + " WHERE call_id=?", (call_id,)
                                ).fetchone()[0]
                                for table, call_id in (
                                    ("calls", target.call_id),
                                    ("call_policies", target.call_id),
                                    ("generations", "synthetic-missing-parent"),
                                )
                            )
                        )

                try:
                    with patch.object(store, "_transaction", new=before_commit):
                        return original_create(store, target, **kwargs)
                finally:
                    store._db.execute("DROP TRIGGER fail_policy_commit")
            result = original_create(store, target, **kwargs)
            if mode == "lose_create":
                raise RuntimeError(PRIVATE)
            return result

        with patch.object(web_fixture.ObservedStore, "create_call", new=checked_create):
            self.select_release.set()
            with self.assertRaises(BackendError) as caught:
                await backend.invitation(handle)
            self.safe(caught.exception)
            frozen = record.policy
            self.assertEqual(
                staged, [(1, 1, 1)]
            )  # All inserts succeeded; deferred FK fails COMMIT.
            self.assertIsNotNone(frozen)
            self.assertIsNone(record.grant)
            self.assertEqual(backend._runtime._accepted_calls, {})
            self.assertFalse(backend._invitations)
            with closing(sqlite3.connect(old_path)) as db:
                for table in ("calls", "call_policies", "web_invitations"):
                    self.assertEqual(
                        db.execute(
                            "SELECT count(*) FROM " + table + " WHERE call_id=?", (bound.call_id,)
                        ).fetchone()[0],
                        0,
                    )
            mode = "lose_create"
            with self.assertRaises(BackendError) as caught:
                await backend.invitation(handle, retry=True)
            self.safe(caught.exception)
            self.assertIs(record.policy, frozen)
            self.assertEqual(await backend._runtime.read_call_policy(bound.call_id), frozen)
            self.assertEqual(
                backend._runtime._accepted_calls, {}
            )  # Lost ACK never registers scope.
            self.assertIsNone(record.grant)
            with closing(sqlite3.connect(old_path)) as db:
                self.assertEqual(
                    db.execute(
                        "SELECT count(*) FROM web_invitations WHERE call_id=?", (bound.call_id,)
                    ).fetchone()[0],
                    0,
                )
            mode = "normal"
            web_fixture.ObservedStore.lose_issue = True
            with self.assertRaises(BackendError) as caught:
                await backend.invitation(handle, retry=True)
            self.safe(caught.exception)
            self.assertTrue(all(policy is frozen for policy in attempts))
        self.assertIs(record.policy, frozen)
        self.assertEqual((len(pa.calls), len(stt.calls)), (0, 0))
        self.now += 10
        grant = await backend.invitation(handle, retry=True)
        self.assertIs(grant.secret, secret)
        self.assertEqual(grant.expires_at, 1900.0)  # Original committed expiry, no renewal.
        self.assertEqual(len(self.selection_calls), 2)  # bad + gate; no reselect on ACK loss
        self.assertIs(await backend.invitation(handle), grant)
        await backend._runtime.create_call(
            bound, policy=frozen
        )  # Same binding AND policy replay is idempotent.
        self.assertEqual(await backend._runtime.read_call_policy(bound.call_id), frozen)
        different = CallPolicy.from_policies(
            frozen.reply, replace(frozen.audio, max_wav_bytes=4097), frozen.review_manifest
        )
        with self.assertRaises(RuntimeOperationError):
            await backend._runtime.create_call(bound, policy=different)
        self.assertEqual(await backend._runtime.read_call_policy(bound.call_id), frozen)
        self.assertEqual(backend._runtime._accepted_calls, {bound.call_id: bound})
        busy = await self.request(backend.app, "exchange", data={"invitation": grant.secret.value})
        self.assertEqual(busy[0], 409)  # Never close unknown old call to force admission.
        self.assertEqual(snapshot(), original)
        self.now = 1016.0  # Its original server permission expired naturally; rows stay unchanged.
        cookie, reference = await self.exchange(backend, grant)
        self.assertEqual(self.wakeups.active()[0].delay, 120.0)
        heartbeat = await self.request(
            backend.app, "heartbeat", data={}, cookie=cookie, reference=reference
        )
        self.assertEqual(heartbeat[0], 200)
        self.assertEqual(self.wakeups.active()[0].delay, 120.0)
        self.reply_mode = "unknown"
        request = dict(
            data={"utterance_id": "u", "text": PRIVATE}, cookie=cookie, reference=reference
        )
        await self.request(backend.app, "utterances", **request)
        await backend.drain()
        state = await backend.read_state(handle)
        self.assertEqual([t.body for t in state.turns], [PRIVATE])
        self.assertEqual(state.generations[0].state, "failed")
        self.assertEqual(len(pa.calls), 1)
        await self.request(backend.app, "utterances", **request)
        await backend.drain()
        self.assertEqual(len(pa.calls), 1)
        self.send_mode = "unknown"
        # A prepared adapter must be revalidated before claim on later end;
        # changed Provider state leaves the frozen material/pending review alone.
        pa.model = "externally-changed-model"
        before_review = len(pa.calls)
        await self.request(backend.app, "end", data={}, cookie=cookie, reference=reference)
        await backend.drain()
        unavailable = await backend.read_state(handle)
        self.assertEqual(unavailable.review.state, "pending")
        self.assertIsNone(unavailable.review_work)
        self.assertEqual(unavailable.review_policy.status, "unavailable")
        self.assertEqual(len(pa.calls), before_review)
        pa.model = "native-model"
        await self.request(backend.app, "end", data={}, cookie=cookie, reference=reference)
        await backend.drain()
        state = await backend.read_state(handle)
        self.assertEqual(state.review.state, "succeeded")
        self.assertEqual(state.publication.pages[0].state, "uncertain")
        self.assertEqual(state.publication.context_state, "succeeded")
        self.assertFalse(state.publication.complete)
        counts = (len(pa.calls), len(self.sends), len(self.model_reads))
        await self.request(backend.app, "end", data={}, cookie=cookie, reference=reference)
        await backend.drain()
        self.assertEqual((len(pa.calls), len(self.sends), len(self.model_reads)), counts)
        # Actual adapter SDK task swallows cancellation; backend cannot finish
        # shutdown or discard its registry/files before actual SDK completion.
        self.select_mode = "good"
        self.reply_mode = "good"
        self.send_mode = "good"
        hb = backend.begin(event_b)
        gb = await backend.invitation(hb)
        cb, rb = await self.exchange(backend, gb)
        self.assertEqual(len(self.wakeups.active()), 1)
        self.assertEqual(self.wakeups.active()[0].delay, 120.0)
        self.assertEqual(snapshot(), original)
        self.stt_mode = "stubborn"
        self.stt_entered.clear()
        self.stt_release.clear()
        await self.request(
            backend.app,
            "audio",
            cookie=cb,
            reference=rb,
            raw=b"retained-B-audio",
            headers={"content-type": "audio/webm;codecs=opus", "x-bot-utterance-id": "b-audio"},
        )
        await asyncio.wait_for(self.stt_entered.wait(), 2)
        stopping = await backend.shutdown(timeout=0.03)
        await asyncio.wait_for(self.stt_cancelled.wait(), 2)
        self.assertFalse(stopping.stopped)
        self.assertEqual(stopping.error_code, "still_stopping")
        self.assertTrue(backend._runtime._thread.is_alive())
        self.assertTrue(backend._record(hb).audio._tasks)
        self.assertTrue(self.dirs[-1].exists())
        close_waiter = asyncio.create_task(backend.shutdown(timeout=None))
        await asyncio.sleep(0)
        close_waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await close_waiter
        self.assertFalse(backend._close_task.done())
        self.stt_release.set()
        self.assertTrue((await backend.shutdown(timeout=3)).stopped)
        self.assertFalse(backend._record(hb).audio._tasks)
        self.assertEqual(len(stt_b.calls), 1)
        self.assertEqual(len(pb.calls), 0)
        with Store(backend._config.database) as store:
            node = store.get_audio_job(backend._record(hb).binding.call_id, "b-audio")
            self.assertEqual(node.state, "cancelled")
            self.assertEqual(node.data, b"retained-B-audio")
        self.assertEqual(snapshot(), original)
        self.assertEqual(old_path.stat().st_mode & 0o777, 0o640)
        self.assertTrue(all(t.call_id not in old_ids for t, _, _ in self.sends))
        self.assertTrue(all(t.call_id not in old_ids for t in self.model_reads))
        # A preparing resolver itself may also suppress cancellation. Keep its
        # actual preparation task and original worker until it really returns.
        self.select_mode = "stubborn"
        self.select_entered.clear()
        self.select_cancelled.clear()
        self.select_release.clear()
        slow = self.backend("slow.db")
        await slow.start()
        hs = slow.begin(event_b)
        await asyncio.wait_for(self.select_entered.wait(), 2)
        # Empty scope does no startup SQL discovery. Verify the retained fixed
        # binding has not been committed while its policy is still preparing.
        with self.assertRaises(RuntimeOperationError):
            await slow._runtime.read_state(slow._record(hs).binding.call_id)
        report = await slow.shutdown(timeout=0.03)
        await asyncio.wait_for(self.select_cancelled.wait(), 2)
        self.assertFalse(report.stopped)
        self.assertTrue(slow._runtime._thread.is_alive())
        self.assertFalse(slow._record(hs).task.done())
        self.select_release.set()
        self.assertTrue((await slow.shutdown(timeout=3)).stopped)
        self.assertIsNone(slow._record(hs).grant)
        # Import and construction are passive; no SDK or business package loaded.
        script = """
import asyncio,sqlite3,socket,threading,os,sys
from pathlib import Path
def deny(*a,**k):raise AssertionError('construction side effect')
sqlite3.connect=deny;socket.socket=deny;threading.Thread.start=deny;os.open=deny;os.mkdir=deny
from bot_call.backend import BackendConfig,BackendTools,CallBackend
from bot_call.limits import RuntimeLimits
async def callback(*a,**k):pass
b=CallBackend(BackendConfig(Path('/synthetic/not-created.db'),'https://synthetic.invalid',3600,RuntimeLimits()),
 BackendTools(object(),object(),object,object,object,callback),
 select_policies=callback,send_page=callback,make_context_readable=callback)
assert b._runtime is None
assert 'openai' not in sys.modules
assert not any(n=='astrbot' or n.startswith('astrbot.') for n in sys.modules)
"""
        probe = subprocess.run([sys.executable, "-B", "-c", script], capture_output=True, text=True)
        self.assertEqual(probe.returncode, 0, probe.stderr)

    async def test_composed_audio_reply_playback_review_and_two_publication_results(self):
        from bot_call.runtime import CallRuntime
        from bot_call.call_policy import CallPolicyError
        from bot_call.review_policy import ReviewPolicyUnavailable
        from bot_call.store import MaterialConflict
        import test_limits as limits_fixture

        self.loop = asyncio.get_running_loop()
        self.wakeups = limits_fixture.Wakeups()

        def scoped_runtime(*args, **kwargs):
            kwargs["_limit_timer"] = self.wakeups
            return CallRuntime(*args, **kwargs)

        p = patch("bot_call.backend.CallRuntime", side_effect=scoped_runtime)
        p.start()
        self.addCleanup(p.stop)

        def counter(text):
            counter.threads.add(threading.get_ident())
            return len(text)

        counter.threads = set()
        self.counter = counter
        original_selection = self.selection

        async def select(bound, snapshot):
            result = await original_selection(bound, snapshot)
            return result

        self.selection = select
        original_environment = self.environment

        def environment(*args):
            event, provider, stt = original_environment(*args)
            self.context.persona_manager.results[event.unified_msg_origin][1]["prompt"] = (
                "人格-" + self.context.conversation_manager.current[event.unified_msg_origin]
            )

            async def respond(kwargs):
                try:
                    data = json.loads(kwargs["contexts"][-1]["content"])
                except (ValueError, KeyError, TypeError):
                    data = {}
                return (
                    response(RECOVERY_FINAL if data.get("kind") == "final" else "已知分块笔记")
                    if data.get("format", "").startswith("bot-call-review-")
                    else response(BOT)
                )

            provider.responder = respond
            return event, provider, stt

        self.environment = environment
        self.context_entered = asyncio.Event()
        self.context_release = asyncio.Event()
        self.context_release.set()
        self.gates.append(self.context_release)
        original_backend = self.backend

        def backend(*args):
            b = original_backend(*args)
            original = b._make_context

            async def readable(bound, key, body):
                self.context_entered.set()
                await self.context_release.wait()
                return await original(bound, key, body)

            b._make_context = readable
            return b

        self.backend = backend
        event, pa, stt = self.environment()
        event_b, pb, stt_b = self.environment("qq-B", "B")
        backend = self.backend()
        self.assertIsNone(backend._runtime)
        self.assertFalse(backend._config.database.exists())
        await asyncio.gather(backend.start(), backend.start())
        self.assertEqual(len(web_fixture.ObservedStore.instances), 1)
        self.assertEqual(backend._runtime._accepted_calls, {})
        self.assertEqual(self.wakeups.active(), [])
        a = backend.begin(event)
        b = backend.begin(event_b)
        ga, gb = await asyncio.gather(backend.invitation(a), backend.invitation(b))

        # On the actual worker, finish before _spawn's cleanup callback runs.
        # Model 3.12's synchronous gather(done) path on every test interpreter;
        # bound iterations so a regressed busy loop fails without needing timers.
        async def drain_done_probe():
            runtime = backend._runtime
            self.assertIs(asyncio.get_running_loop(), runtime._worker_loop)
            self.assertNotEqual(threading.get_ident(), self.thread)
            self.assertFalse(runtime._background)

            async def completed_failure():
                raise RuntimeError("synthetic completed background failure")

            done = runtime._spawn(completed_failure(), "drain-probe")
            await asyncio.sleep(0)
            self.assertTrue(done.done())
            self.assertIn(done, runtime._owned)
            self.assertEqual(runtime._background, {done})
            rounds = []

            def synchronous_gather(*tasks, return_exceptions=False):
                self.assertEqual(tasks, (done,))
                self.assertTrue(return_exceptions)
                rounds.append(1)
                if len(rounds) > 3:
                    raise AssertionError("drain did not yield to queued cleanup")
                result = asyncio.get_running_loop().create_future()
                result.set_result([done.exception()])
                return result

            with patch("bot_call.runtime.asyncio.gather", side_effect=synchronous_gather):
                self.assertIsNone(await runtime._operation("drain", ()))
            self.assertEqual(len(rounds), 1)
            self.assertNotIn(done, runtime._owned)
            self.assertFalse(runtime._background)

        probe = asyncio.run_coroutine_threadsafe(drain_done_probe(), backend._runtime._worker_loop)
        await asyncio.wait_for(asyncio.shield(asyncio.wrap_future(probe)), 2)
        bound = backend._record(a).binding
        saved_policies = {}
        for handle in (a, b):
            record = backend._record(handle)
            saved = await backend._runtime.read_call_policy(record.binding.call_id)
            saved_policies[record.binding.call_id] = saved
            self.assertEqual(saved, record.policy)
            self.assertEqual(saved.binding, record.binding)
            self.assertEqual(saved.reply, record.policies.reply)
            self.assertEqual(saved.audio, record.policies.audio)
            manifest = saved.review_manifest
            self.assertEqual(
                manifest.restore(record.binding, {manifest.counter_rule_id: self.counter}),
                record.policies.review.policy,
            )
            with self.assertRaises(ReviewPolicyUnavailable) as missing:
                manifest.restore(record.binding, {})
            self.assertEqual(missing.exception.code, "counter_unavailable")
            decoded = json.loads(saved.canonical_json)
            self.assertNotIn(PRIVATE, saved.canonical_json + repr(saved))
            self.assertNotIn(bound.sender_id, repr(saved))
            self.assertEqual(
                set(decoded["audio"]),
                {"provider_id", "model", "max_input_bytes", "max_wav_bytes", "selection"},
            )
            decoded["agent"]["settings"] = "changed-detached-copy"
            self.assertEqual(
                (await backend._runtime.read_call_policy(record.binding.call_id)).reply,
                record.policies.reply,
            )
        self.assertEqual(saved_policies[bound.call_id].reply.contract, "astrbot-main-agent-v1")
        self.assertEqual(
            saved_policies[backend._record(b).binding.call_id].reply,
            backend._record(b).policies.reply,
        )
        self.assertEqual(backend._record(b).policy.reply.contract, "astrbot-main-agent-v1")
        bad = json.loads(saved_policies[bound.call_id].canonical_json)
        bad["api_key"] = PRIVATE
        encoded = json.dumps(bad, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        with self.assertRaises(CallPolicyError) as invalid:
            CallPolicy(encoded, hashlib.sha256(encoded.encode()).hexdigest())
        self.safe(invalid.exception)
        self.assertIs(await backend.invitation(a), ga)
        self.assertNotIn(ga.secret.value, repr(a) + repr(ga) + repr(ga.link(ORIGIN + "/bot-call/")))
        cookie, reference = await self.exchange(backend, ga)
        busy = await self.request(backend.app, "exchange", data={"invitation": gb.secret.value})
        self.assertEqual(busy[0], 409)
        convo = self.context.conversation_manager
        convo.current[bound.unified_msg_origin] = "B"
        async with self.locks.acquire_lock(bound.unified_msg_origin):
            convo.items["A"].history = json.dumps(
                [{"role": "user", "content": "原A最新QQ前情"}], ensure_ascii=False
            )
        b_history = convo.items["B"].history
        self.stt_mode = "gate"
        audio = dict(
            cookie=cookie,
            reference=reference,
            raw=b"complete-synthetic-clip",
            headers={"content-type": "audio/webm;codecs=opus", "x-bot-utterance-id": "utterance-A"},
        )
        self.assertIsNone(await self.request(backend.app, "audio", send_failure=True, **audio))
        await asyncio.wait_for(self.stt_entered.wait(), 2)
        received = await self.request(backend.app, "state", cookie=cookie, reference=reference)
        self.assertEqual(received[1]["audio"][0]["state"], "running")
        self.assertEqual(received[1]["turns"], [])
        self.assertTrue(received[1]["audio_available"])
        self.assertNotIn("policy", received[1])
        self.assertNotIn("provider_id", json.dumps(received[1]))
        again = await self.request(backend.app, "audio", **audio)
        self.assertTrue(again[1]["audio_received"])
        self.assertEqual(len(stt.calls), 1)
        self.stt_release.set()
        await backend.drain()
        state = await backend.read_state(a)
        self.assertEqual([t.body for t in state.turns], [RECOVERY_TEXT, BOT])
        self.assertEqual(state.audio_jobs[0].state, "succeeded")
        self.assertIsNone(state.audio_jobs[0].data)
        self.assertEqual(len(stt.calls), 1)
        self.assertEqual(len(pa.calls), 1)
        self.assertEqual(len(pb.calls), 0)
        self.assertEqual(stt_b.calls, [])
        reply_payload = pa.calls[0]
        contents = [
            "".join(p["text"] for p in m["content"] if p["type"] == "text")
            if isinstance(m["content"], list)
            else m["content"]
            for m in reply_payload["contexts"]
        ]
        from astrbot.core.astr_main_agent_resources import LIVE_MODE_SYSTEM_PROMPT
        from bot_call.phone_style import DEFAULT_REPLY_STYLE

        self.assertTrue(
            contents[0].endswith(
                "\n# Persona Instructions\n\n人格-A\n\n"
                + LIVE_MODE_SYSTEM_PROMPT
                + "\n\n"
                + DEFAULT_REPLY_STYLE
                + "\n"
            )
        )
        self.assertIn("Safe Mode", contents[0])
        self.assertIn("原A最新QQ前情", contents)
        self.assertEqual(contents[-1], RECOVERY_TEXT)
        self.assertEqual(
            convo.items["A"].history,
            json.dumps([{"role": "user", "content": "原A最新QQ前情"}], ensure_ascii=False),
        )
        self.assertEqual(
            self.context.native_calls, []
        )  # Replies use the real main Agent, reviews use Context.llm_generate.
        playback = await self.request(
            backend.app,
            "playback",
            data={"generation_id": "reply:utterance-A", "revision": 1, "state": "ended"},
            cookie=cookie,
            reference=reference,
        )
        self.assertEqual(playback[1]["state"], "ended")
        async with self.locks.acquire_lock(bound.unified_msg_origin):
            history = json.loads(convo.items["A"].history)
            history.append({"role": "user", "content": "回顾之前QQ并行新增"})
            convo.items["A"].history = json.dumps(history, ensure_ascii=False)
        self.context_release.clear()
        ended = await self.request(backend.app, "end", data={}, cookie=cookie, reference=reference)
        self.assertTrue(ended[1]["ended"])
        await asyncio.wait_for(self.context_entered.wait(), 3)
        partial = await self.request(backend.app, "state", cookie=cookie, reference=reference)
        self.assertEqual(partial[1]["review_state"], "succeeded")
        self.assertTrue(all(s == "submitted" for s in partial[1]["page_states"]))
        self.assertEqual(partial[1]["context_state"], "running")
        self.assertFalse(partial[1]["complete"])
        # A close released admission; B cookie can never authorize old A tab.
        cookie_b, ref_b = await self.exchange(backend, gb)
        rejected = await self.request(
            backend.app,
            "utterances",
            data={"utterance_id": "old-tab", "text": "wrong"},
            cookie=cookie_b,
            reference=reference,
        )
        self.assertEqual(rejected[0], 401)
        self.assertEqual((await backend.read_state(b)).turns, ())
        self.context_release.set()
        await backend.drain()
        state = await backend.read_state(a)
        self.assertTrue(state.publication.complete)
        self.assertEqual(state.review.body, RECOVERY_FINAL)
        self.assertEqual(
            "".join(fragment for t, _, fragment in self.sends if t == bound), RECOVERY_FINAL
        )
        self.assertEqual(state.publication.body, RECOVERY_FINAL)
        frozen = json.loads(state.review.input_json)
        self.assertEqual([t["body"] for t in frozen], [RECOVERY_TEXT, BOT])
        self.assertEqual(frozen[-1]["playback_state"], "ended")
        self.assertGreater(state.review_work.map_count, 1)
        review_requests = [
            json.loads(payload["contexts"][1]["content"]) for payload in pa.calls[1:]
        ]
        self.assertEqual(sum(r.get("kind") == "final" for r in review_requests), 1)
        self.assertTrue(
            any(
                r.get("purpose") == "partial_material_for_later_review_processing"
                for r in review_requests
            )
        )
        worker = web_fixture.ObservedStore.instances[0].owner
        self.assertEqual(self.counter.threads, {worker})
        self.assertTrue(
            all(
                "max_tokens" not in payload and "max_completion_tokens" not in payload
                for payload in pa.calls
            )
        )
        imported = json.loads(convo.items["A"].history)
        self.assertEqual(imported[:2], history)
        self.assertEqual(imported[-2]["role"], "system")
        self.assertIn("系统导入的本通电话回顾，非用户原话", imported[-2]["content"])
        self.assertEqual(imported[-1], {"role": "assistant", "content": RECOVERY_FINAL})
        self.assertEqual(self.model_reads, [bound])
        self.assertEqual(convo.items["B"].history, b_history)
        self.assertEqual(convo.current[bound.unified_msg_origin], "B")
        counts = (len(pa.calls), len(stt.calls), len(self.sends), len(self.model_reads))
        await self.request(backend.app, "end", data={}, cookie=cookie, reference=reference)
        await backend.drain()
        self.assertEqual(
            (len(pa.calls), len(stt.calls), len(self.sends), len(self.model_reads)), counts
        )
        heartbeat = asyncio.Event()
        self.loop.call_soon(heartbeat.set)
        await heartbeat.wait()
        # Only newly registered B owns the remaining live timer. A is finished;
        # a server-side deadline closes B without a page refresh or model call.
        self.assertEqual(len(self.wakeups.active()), 1)
        self.assertEqual(self.wakeups.active()[0].delay, 120.0)
        self.now += 120
        self.wakeups.fire()
        await backend.drain()
        timed_out = await backend.read_state(b)
        self.assertIsNone(timed_out.review)
        self.assertIsNotNone(timed_out.close_request)
        self.assertEqual(timed_out.close_request.requested_at, self.now)
        timed_public = await self.request(backend.app, "state", cookie=cookie_b, reference=ref_b)
        self.assertEqual(timed_public[1]["limits"]["close_reason"], "disconnected")
        self.assertEqual(timed_public[1]["limits"]["closed_at"], self.now)
        self.assertEqual(len(pb.calls), 0)  # Empty material is not fake generation success.
        self.assertEqual(self.wakeups.active(), [])
        self.assertTrue((await backend.shutdown(timeout=None)).stopped)
        self.assertTrue((await backend.shutdown()).stopped)
        with Store(backend._config.database) as reopened:
            for call_id, saved in saved_policies.items():
                self.assertEqual(reopened.get_call_policy(call_id), saved)
                self.assertEqual(reopened.create_call(saved.binding, policy=saved), saved.binding)
                other = CallPolicy.from_policies(
                    saved.reply,
                    replace(saved.audio, max_wav_bytes=saved.audio.max_wav_bytes + 1),
                    saved.review_manifest,
                )
                with self.assertRaises(MaterialConflict):
                    reopened.create_call(saved.binding, policy=other)
                with self.assertRaises(MaterialConflict):
                    reopened.create_call(saved.binding)
                with self.assertRaises(MaterialConflict):
                    reopened.create_call(
                        replace(saved.binding, sender_id="other-user"), policy=saved
                    )
                for sql in (
                    "UPDATE call_policies SET fingerprint=fingerprint WHERE call_id=?",
                    "DELETE FROM call_policies WHERE call_id=?",
                ):
                    with self.assertRaises(sqlite3.IntegrityError):
                        reopened._db.execute(sql, (call_id,))
                self.assertEqual(reopened.get_call_policy(call_id), saved)
            self.assertEqual(
                reopened.get_review_manifest(bound.call_id),
                saved_policies[bound.call_id].review_manifest,
            )
        with self.assertRaises(BackendError):
            backend.begin(event)
