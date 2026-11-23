"""Phone expression: real native builder, synthetic services, no paid calls."""

import asyncio
import json
import unittest
from copy import deepcopy
from dataclasses import replace

from test_backend import TEXT, NativeCase, PublicProvider, response

from bot_call.astrbot_agent import spoken
from bot_call.native_host import NativeHostError, settings_from
from bot_call.phone_style import DEFAULT_REPLY_STYLE, sentence_count
from bot_call.segments import Sentences
from bot_call.store import Store

MARKDOWN = (
    "# 今日重点 ##\n"
    "1. **先处理睡眠**，之后再讨论。\n"
    "- [x] _预约医生_。\n"
    "> 这是引用，不是新决定。\n"
    "```python\nprint('保留代码内容')\n```\n"
    "| 项目 | 时间 |\n| :--- | ---: |\n| `午饭` | 十二点 |\n"
    "[参考资料](https://example.invalid/path?x=1)。\n"
    "![图片说明](https://example.invalid/a.png)\n"
    "~~旧计划~~先搁置。\n"
    "变量call_id保留；普通数字12:30不改造。"
)


class PlainPhoneTests(unittest.TestCase):
    def test_formats_all_stream_splits_hidden_filter_and_no_hard_limit(self):
        expected = spoken(MARKDOWN)
        for forbidden in (
            "**",
            "#",
            "_预约",
            "[x]",
            "```",
            "python",
            "|",
            ":---",
            "https://",
            "~~",
        ):
            self.assertNotIn(forbidden, expected)
        for retained in (
            "今日重点",
            "先处理睡眠",
            "预约医生",
            "这是引用",
            "print('保留代码内容')",
            "午饭",
            "十二点",
            "参考资料",
            "图片说明",
            "旧计划",
            "call_id",
            "12:30",
        ):
            self.assertIn(retained, expected)
        for width in (1, 2, 3, 7, 59, 60, 61, len(MARKDOWN)):
            splitter = Sentences()
            chunks = []
            for start in range(0, len(MARKDOWN), width):
                chunks += splitter.feed(MARKDOWN[start : start + width])
            chunks += splitter.feed(final=True)
            self.assertEqual("".join(chunks), expected)
            self.assertTrue(all(c.strip() for c in chunks))
        self.assertEqual(spoken("<think>```python\n隐藏。</think>**公开内容**。"), "公开内容。")
        self.assertEqual(spoken("|" + "-" * 100 + "|\n之后的正文。"), "\n之后的正文。")
        long = "完整保留这一句话。" * 100
        self.assertEqual(spoken(long), long)
        self.assertGreater(len(long), 600)
        self.assertEqual(sentence_count("先说结论，后面解释。要细说吗？！\n尾句"), 3)

    def test_reply_count_medians_use_numeric_counts_and_skip_missing_legacy_rows(self):
        from bot_call.observability import timing_summary

        rows = []
        for i, (chars, sentences) in enumerate(((54, 2), (604, 24), (222, 7))):
            for stage, value in (("reply_chars", chars), ("reply_sentences", sentences)):
                rows.append(
                    {
                        "call_id": "synthetic",
                        "generation_id": str(i),
                        "recorded_at": i,
                        "stage": stage,
                        "elapsed_ms": value,
                    }
                )
        rows.append(
            {
                "call_id": "synthetic",
                "generation_id": "legacy",
                "recorded_at": 0,
                "stage": "run",
                "elapsed_ms": 80,
            }
        )
        result = timing_summary(rows)
        self.assertEqual(
            result["reply_style"], {"rounds": 3, "chars_median": 222, "sentences_median": 7}
        )
        self.assertEqual([r["stage"] for r in result["stages"]], ["run"])
        self.assertEqual(
            timing_summary([])["reply_style"],
            {"rounds": 0, "chars_median": None, "sentences_median": None},
        )


class PhoneStyleTests(NativeCase):
    async def test_live_custom_and_empty_only_on_phone_native_builds(self):
        from astrbot.core.astr_main_agent_resources import LIVE_MODE_SYSTEM_PROMPT

        event, provider = self.environment()
        original_config = deepcopy(self.context.configs)
        original_personas = deepcopy(self.context.persona_manager.results)
        original_provider = deepcopy(provider.provider_config)
        original_history = self.context.conversation_manager.items["A"].history
        native_build = self.agent_api.build
        compared = []

        async def build(**kw):
            phone = kw["event"]
            self.assertEqual(phone.get_extra("action_type"), "live")
            normal = self.agent_api.event_type(
                "普通聊天", deepcopy(phone.message_obj), phone.platform_meta, phone.session_id
            )
            normal.role = phone.role
            normal.plugins_name = deepcopy(phone.plugins_name)
            normal_req = self.agent_api.request_type(
                prompt="普通聊天",
                contexts=[],
                conversation=deepcopy(kw["req"].conversation),
                session_id=normal.unified_msg_origin,
            )
            # Both builders run concurrently; the ordinary event has no phone extras.
            a, b = await asyncio.gather(
                native_build(**kw),
                native_build(
                    event=normal,
                    plugin_context=self.context,
                    config=deepcopy(kw["config"]),
                    req=normal_req,
                    apply_reset=False,
                ),
            )
            self.assertIsNone(normal.get_extra("action_type"))
            self.assertNotIn(LIVE_MODE_SYSTEM_PROMPT, b.provider_request.system_prompt)
            self.assertNotIn(DEFAULT_REPLY_STYLE, b.provider_request.system_prompt)
            self.assertIn(LIVE_MODE_SYSTEM_PROMPT, a.provider_request.system_prompt)
            self.assertEqual(
                a.provider_request.func_tool.names(), b.provider_request.func_tool.names()
            )
            b.reset_coro.close()
            compared.append(a.provider_request)
            return a

        self.agent_api = replace(self.agent_api, build=build)
        old = self.settings()
        style = DEFAULT_REPLY_STYLE
        self.settings = lambda: dict(old, call_reply_style=style)
        backend, _ = await self.backend()
        _handle, web = await self.enter(backend, event)
        await backend.web_submit(web.token, web.reference, "styled", TEXT)
        await backend.drain()
        prompt = compared[-1].system_prompt
        self.assertEqual(prompt.count(DEFAULT_REPLY_STYLE), 1)
        self.assertLess(
            prompt.index("PRIVATE_PERSONA_PROMPT"), prompt.index(LIVE_MODE_SYSTEM_PROMPT)
        )
        self.assertLess(prompt.index(LIVE_MODE_SYSTEM_PROMPT), prompt.index(DEFAULT_REPLY_STYLE))
        self.assertIn(
            DEFAULT_REPLY_STYLE, json.dumps(provider.calls[-1]["contexts"], ensure_ascii=False)
        )
        style = ""
        blank, _ = await self.backend(name="blank.db")
        _, w = await self.enter(blank, event)
        await blank.web_submit(w.token, w.reference, "blank", TEXT)
        await blank.drain()
        self.assertIn(LIVE_MODE_SYSTEM_PROMPT, compared[-1].system_prompt)
        self.assertNotIn(DEFAULT_REPLY_STYLE, compared[-1].system_prompt)
        style = "只用中文口语回答，先讲最重要的一点。"
        custom, _ = await self.backend(name="custom.db")
        _, custom_web = await self.enter(custom, event)
        await custom.web_submit(custom_web.token, custom_web.reference, "custom", TEXT)
        await custom.drain()
        self.assertIn(style, compared[-1].system_prompt)
        self.assertNotIn(DEFAULT_REPLY_STYLE, compared[-1].system_prompt)
        self.assertEqual(self.context.configs, original_config)
        self.assertEqual(self.context.persona_manager.results, original_personas)
        self.assertEqual(provider.provider_config, original_provider)
        self.assertEqual(self.context.conversation_manager.items["A"].history, original_history)
        self.assertTrue(
            all("max_tokens" not in r and "max_completion_tokens" not in r for r in provider.calls)
        )
        for invalid in (None, [], True, "字" * 2001, "\ud800"):
            with self.assertRaises(NativeHostError):
                settings_from(dict(old, call_reply_style=invalid))
        import pathlib

        schema = json.loads(
            (pathlib.Path(__file__).resolve().parents[1] / "_conf_schema.json").read_text()
        )
        self.assertEqual(schema["call_reply_style"]["default"], DEFAULT_REPLY_STYLE)

    async def test_stream_fallback_tts_material_and_numeric_reply_counts(self):
        class StyledStream(PublicProvider):
            async def text_chat_stream(self, **kwargs):
                self.calls.append(kwargs)
                for char in MARKDOWN:
                    yield response(char, chunk=True)
                yield response(MARKDOWN)

        expected = spoken(MARKDOWN)
        event, _provider = self.environment(provider_class=StyledStream)
        backend, _ = await self.backend()
        handle, web = await self.enter(backend, event)
        await backend.web_submit(web.token, web.reference, "stream", TEXT)
        await backend.drain()
        state = await backend.read_state(handle)
        self.assertEqual(state.generations[-1].state, "succeeded")
        bot = state.turns[-1]
        self.assertEqual(bot.body, expected)
        texts = self.context.tts[event.unified_msg_origin].texts
        self.assertEqual("".join(texts), bot.body)
        with Store(self.path / "calls.db") as store:
            rows = store._db.execute(
                "SELECT stage,elapsed_ms FROM turn_timings WHERE stage IN ('reply_chars','reply_sentences')"
            ).fetchall()
            self.assertEqual(
                dict(rows),
                {"reply_chars": len(expected), "reply_sentences": sentence_count(expected)},
            )
            projection = store.timing_snapshot()
            self.assertEqual(
                projection["reply_style"],
                {
                    "rounds": 1,
                    "chars_median": len(expected),
                    "sentences_median": sentence_count(expected),
                },
            )
            self.assertTrue(
                all(
                    r["stage"] not in {"reply_chars", "reply_sentences"}
                    for r in projection["stages"]
                )
            )
        # Whole-response fallback uses exactly the same presentation pipeline.
        other, p = self.environment("qq-B", "B")

        async def answer(payload):
            return response(MARKDOWN)

        p.responder = answer
        await backend.web_end(web.token, web.reference)
        await backend.drain()
        fallback, _ = await self.backend(name="fallback.db")
        h, w = await self.enter(fallback, other)
        await fallback.web_submit(w.token, w.reference, "fallback", TEXT)
        await fallback.drain()
        self.assertEqual((await fallback.read_state(h)).turns[-1].body, expected)
        await fallback.web_speech(w.token, w.reference, "reply:fallback")
        await fallback.drain()
        self.assertEqual(self.context.tts[other.unified_msg_origin].texts, [expected])
