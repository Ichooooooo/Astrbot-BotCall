"""Review presentation changes never weaken material facts or legacy bytes."""

import hashlib
import itertools
import json
from dataclasses import replace
import unittest
from bot_call.review import build_review_prompt
from test_backend import NativeCase


class PresentationTests(unittest.TestCase):
    def test_default_is_byte_identical_to_056_with_additional_instructions(self):
        prompt = build_review_prompt(bounded_output=True)
        self.assertEqual(
            hashlib.sha256(prompt.encode()).hexdigest(),
            "638480ebdd8636d2b9824fbefe149675043ae732b9bf78348f753a4dba6c6caa",
        )
        self.assertEqual(
            prompt,
            build_review_prompt(
                " \n ",
                bounded_output=True,
                style="default",
                voice="default",
                system_notes="default",
            ),
        )
        expected = (
            prompt
            + "\n\n管理端补充写作要求（仅调整组织方式、语气与详略；冲突时以上述事实与来源底线为准）：\n"
            + "SYNTHETIC_ADDITION"
        )
        self.assertEqual(build_review_prompt("SYNTHETIC_ADDITION", bounded_output=True), expected)

    def test_all_36_option_combinations_and_invalid_options(self):
        for style, voice, notes in itertools.product(
            ("default", "bullets", "brief", "sections"),
            ("default", "bot", "neutral"),
            ("default", "on_issue", "none"),
        ):
            with self.subTest(style=style, voice=voice, notes=notes):
                prompt = build_review_prompt(
                    "SYNTHETIC_ADDITION",
                    bounded_output=True,
                    style=style,
                    voice=voice,
                    system_notes=notes,
                )
                self.assertTrue(prompt.endswith("SYNTHETIC_ADDITION"))
                self.assertIn("map", prompt)
                self.assertIn("merge", prompt)
                self.assertIn("final", prompt)
                self.assertIn("用户决定", prompt)
                if style != "default":
                    self.assertIn(
                        {
                            "bullets": "三到五条要点",
                            "brief": "一两段口语小结",
                            "sections": "概括／聊了什么／决定／待办／未定",
                        }[style],
                        prompt,
                    )
                if voice != "default":
                    self.assertIn({"bot": "延续来源人格", "neutral": "中立纪要"}[voice], prompt)
                if notes != "default":
                    self.assertIn(
                        {"on_issue": "结尾一句通俗提示", "none": "不写系统说明"}[notes], prompt
                    )
                if (style, voice, notes) != ("default",) * 3:
                    self.assertIn("未知状态不写成已完成", prompt)
                    self.assertIn("不必逐条复述为免责声明", prompt)
        for fields in (dict(style="unknown"), dict(voice=[]), dict(system_notes="invalid")):
            with self.assertRaises(ValueError):
                build_review_prompt(**fields)


class PresentationNativeTests(NativeCase):
    async def test_long_call_preserves_nondefault_prompt_through_map_merge_and_final(self):
        from bot_call.store import Store
        from bot_call.review_work import prepare_review_work, advance_review_work
        from test_review_work import binding

        prompt = build_review_prompt(
            bounded_output=True, style="bullets", voice="neutral", system_notes="on_issue"
        )
        kinds = set()
        with Store(self.path / "long-review.db") as store:
            store.create_call(binding("synthetic-long"))
            store.append_user(
                "synthetic-long", "u", "周五再决定，今天只是讨论。" * 6000, source="text"
            )
            store.end_call("synthetic-long", prompt=prompt)
            prepare_review_work(store, "synthetic-long", input_token_budget=5000, count_tokens=len)

            async def note(node):
                payload = json.loads(json.loads(node.request_json)["messages"][-1]["content"])
                self.assertEqual(payload["review_prompt"], prompt)
                self.assertIn("结尾一句通俗提示", payload["review_prompt"])
                kinds.add(node.kind)
                return "合成事实笔记：用户还没决定。"

            completed = await advance_review_work(
                store, "synthetic-long", note, count_tokens=len, max_steps=500
            )
            self.assertEqual(completed.state, "succeeded")
            self.assertTrue({"map", "merge", "final"} <= kinds)

    async def test_four_styles_reach_map_and_final_with_detached_bot_persona(self):
        self.prompt_evidence = {}
        for i, style in enumerate(("default", "bullets", "brief", "sections")):
            event, provider = self.environment(f"qq-{i}", str(i))
            from copy import deepcopy

            original_persona = deepcopy(self.context.persona_manager.results)
            backend, _ = await self.backend(name=f"style-{i}.db")
            voice, notes = ("default", "default") if style == "default" else ("bot", "on_issue")
            prompt = build_review_prompt(
                bounded_output=True, style=style, voice=voice, system_notes=notes
            )
            backend._config = replace(backend._config, review_prompt=prompt, review_voice=voice)
            handle, web = await self.enter(backend, event)
            await backend.web_submit(
                web.token, web.reference, "synthetic", "合成材料：周五再讨论，暂未决定。"
            )
            await backend.drain()
            await backend.web_end(web.token, web.reference)
            await backend.drain()
            state = await backend.read_state(handle)
            self.assertEqual(state.review.state, "succeeded")
            if voice == "bot":
                self.assertIn("PRIVATE_PERSONA_PROMPT", state.review.prompt)
            else:
                self.assertEqual(state.close_request.prompt, prompt)
            requests = [
                p
                for p in provider.calls
                if any("bot-call-review-" in str(m.get("content")) for m in p.get("contexts", []))
            ]
            self.assertEqual(len(requests), 2)
            from bot_call.review_plan import planning_prompt

            base, annotations = planning_prompt(state.review)
            payloads = [json.loads(p["contexts"][-1]["content"]) for p in requests]
            self.assertTrue(all(p["review_prompt"] == base for p in payloads))
            self.assertEqual(base, state.close_request.prompt)
            self.assertEqual(
                "".join(
                    f["turn"]["body"]
                    for p in payloads
                    for f in p.get("fragments", [])
                    if f["turn"]["source"] == "frozen_review_annotations"
                ),
                annotations,
            )
            self.assertTrue(
                all(
                    p["prompt_sha256"]
                    == __import__("hashlib").sha256(state.review.prompt.encode()).hexdigest()
                    for p in payloads
                )
            )
            self.assertEqual(self.context.persona_manager.results, original_persona)
            self.prompt_evidence[style] = dict(
                review_prompt=state.review.prompt, requests=[p["contexts"] for p in requests]
            )
