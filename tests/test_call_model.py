"""Call-only Provider choice through the real builder; no network or SDK."""

from copy import deepcopy
from dataclasses import replace

from astrbot.core.provider import Provider
from test_backend import NativeCase, PublicProvider, TEXT
from bot_call.store import Store


class ChatProvider(PublicProvider, Provider):
    def get_current_key(self):
        raise AssertionError("No credentials in this synthetic provider")

    def set_key(self, *args):
        raise AssertionError("No credentials in this synthetic provider")

    async def get_models(self):
        return [self.get_model()]


class CallModelTests(NativeCase):
    async def test_empty_and_dedicated_builder_selection_preserve_chat_and_review(self):
        event, source = self.environment()
        fast = ChatProvider("fast", "fast-model")
        self.context.by_id["fast"] = fast
        before_config = deepcopy(self.context.configs)
        before_persona = deepcopy(self.context.persona_manager.results)
        seen = []
        native_build = self.agent_api.build

        async def build(**kw):
            seen.append(kw.get("provider"))
            return await native_build(**kw)

        self.agent_api = replace(self.agent_api, build=build)
        original_settings = self.settings()
        selection = ""
        self.settings = lambda: dict(original_settings, call_chat_provider_id=selection)
        inherited, host = await self.backend(name="inherited.db")
        handle, web = await self.enter(inherited, event)
        await inherited.web_submit(web.token, web.reference, "u1", TEXT)
        await inherited.drain()
        self.assertEqual(seen, [None])
        self.assertEqual(len(source.calls), 1)
        self.assertEqual(fast.calls, [])
        self.assertEqual(
            (await host.diagnostic_model(event.unified_msg_origin))["selection"], "inherited"
        )
        # Neither the page nor the zero-argument tool can override the trusted choice.
        from test_web_access import WebAccessTests

        code, _, _ = await WebAccessTests.request(
            self,
            inherited.app,
            "utterances",
            cookie="__Secure-bot_call=" + web.token,
            reference=web.reference,
            data=dict(utterance_id="forged", text=TEXT, call_chat_provider_id="fast"),
        )
        self.assertEqual(code, 400)
        selection = "fast"
        dedicated, host2 = await self.backend(name="dedicated.db")
        handle2, web2 = await self.enter(dedicated, event)
        await dedicated.web_submit(web2.token, web2.reference, "u2", TEXT)
        await dedicated.drain()
        self.assertIs(seen[-1], fast)
        self.assertEqual(len(fast.calls), 1)
        self.assertEqual(len(source.calls), 1)
        self.assertIs(self.context.get_using_provider(umo=event.unified_msg_origin), source)
        self.assertEqual(self.context.configs, before_config)
        self.assertEqual(self.context.persona_manager.results, before_persona)
        choice = await host2.diagnostic_model(event.unified_msg_origin)
        self.assertEqual(
            choice,
            dict(selection="dedicated", provider_id="fast", model="fast-model", available=True),
        )
        with Store(self.path / "dedicated.db") as store:
            run = dict(store._db.execute("SELECT * FROM turn_timings WHERE stage='run'").fetchone())
            self.assertEqual(
                (run["provider_id"], run["model"], run["selection"]),
                ("fast", "fast-model", "dedicated"),
            )
        await dedicated.web_end(web2.token, web2.reference)
        await dedicated.drain()
        self.assertEqual(len(fast.calls), 1)
        self.assertEqual(len(source.calls), 3)  # Review map + final still inherit source.
        self.assertTrue((await dedicated.read_state(handle2)).publication.complete)
        # A source model change remains visible to the unchanged inherited path.
        changed = PublicProvider("changed", "source-new")
        self.context.providers[event.unified_msg_origin] = changed
        self.context.by_id["changed"] = changed
        await inherited.web_submit(web.token, web.reference, "u3", TEXT)
        await inherited.drain()
        self.assertEqual(len(changed.calls), 1)
        self.assertEqual(len(fast.calls), 1)

    async def test_dedicated_streaming_and_removed_provider_never_fall_back(self):
        from test_backend import response

        event, source = self.environment()
        self.context.configs[event.unified_msg_origin]["provider_settings"][
            "streaming_response"
        ] = True
        fast = ChatProvider("fast", "fast-model")

        async def stream(**payload):
            yield response("synthetic partial", chunk=True)
            yield await fast.text_chat(**payload)

        fast.text_chat_stream = stream
        self.context.by_id["fast"] = fast
        original = self.settings()
        self.settings = lambda: dict(original, call_chat_provider_id="fast")
        backend, host = await self.backend()
        handle, web = await self.enter(backend, event)
        await backend.web_submit(web.token, web.reference, "stream", TEXT)
        await backend.drain()
        self.assertEqual((await backend.read_state(handle)).generations[-1].state, "succeeded")
        self.assertEqual(len(fast.calls), 1)
        self.assertEqual(source.calls, [])
        with Store(self.path / "calls.db") as store:
            llm = [
                dict(r) for r in store._db.execute("SELECT * FROM turn_timings WHERE stage='llm'")
            ]
            self.assertEqual(len(llm), 1)  # Chunks are not separate requests.
            self.assertEqual((llm[0]["provider_id"], llm[0]["selection"]), ("fast", "dedicated"))
        fast.provider_config["enable"] = False
        await backend.web_submit(web.token, web.reference, "removed", TEXT)
        await backend.drain()
        self.assertEqual((await backend.read_state(handle)).generations[-1].state, "failed")
        self.assertEqual(len(fast.calls), 1)
        self.assertEqual(source.calls, [])
