"""Phone state for live tools using the installed native Agent and local backend."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import patch

from test_backend import NativeCase, response


class ToolWaitTests(NativeCase):
    async def test_live_tool_start_success_failure_and_call_close(self):
        event, provider = self.environment()
        backend, _ = await self.backend()
        handle, web = await self.enter(backend, event)
        credentials = web.token, web.reference
        agent = backend._record(handle).reply
        binding = backend._record(handle).binding
        runtime = backend._runtime
        loop = asyncio.get_running_loop()
        tool_name = "synthetic_lookup"
        argument = "synthetic-private-argument"

        def gate():
            value = asyncio.Event()
            self.gates.append(value)
            return value

        async def poll_after(uid, release):
            checking = asyncio.Event()
            state_phase = runtime._state_phase

            def observed_phase(key):
                loop.call_soon_threadsafe(checking.set)
                return state_phase(key)

            with patch.object(runtime, "_state_phase", observed_phase):
                task = asyncio.create_task(backend.web_state(*credentials, 8000, uid))
                try:
                    await asyncio.wait_for(checking.wait(), 3)
                    release.set()
                    return await asyncio.wait_for(task, 3)
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

        def assert_public(state, processing):
            public = state.to_dict()
            self.assertIs(public["tool_processing"], processing)
            payload = json.dumps(public)
            self.assertNotIn(tool_name, payload)
            self.assertNotIn(argument, payload)

        for mode in ("success", "executor_error", "hangup"):
            with self.subTest(mode=mode):
                model_entered, can_start = asyncio.Event(), gate()
                tool_entered, can_end = asyncio.Event(), gate()
                can_finish = gate()

                async def lookup(event, query):
                    self.assertEqual(query, argument)
                    tool_entered.set()
                    await can_end.wait()
                    if mode == "executor_error":
                        raise RuntimeError("synthetic executor failure")
                    return "查询完成"

                env = self.agent_env
                tool = env["FunctionTool"](
                    name=tool_name,
                    description="Synthetic lookup",
                    parameters={"type": "object", "properties": {"query": {"type": "string"}}},
                    handler=lookup,
                )
                self.context.get_llm_tool_manager = lambda: SimpleNamespace(
                    get_full_tool_set=lambda: env["ToolSet"]([tool]),
                    get_func=lambda name: tool if name == tool.name else None,
                    get_builtin_tool=lambda *args: None,
                )

                async def answer(payload):
                    if not model_entered.is_set():
                        model_entered.set()
                        await can_start.wait()
                        return response(
                            "",
                            role="tool",
                            names=[tool_name],
                            args=[{"query": argument}],
                            ids=[mode],
                        )
                    return response("这是最后的回复。")

                original_journal = agent.journal

                async def journal(action, *args):
                    if action == "stream_done":
                        await can_finish.wait()
                    return await original_journal(action, *args)

                executor = env["FunctionToolExecutor"]
                execute = executor.execute

                async def invoke(tool, run_context, **kwargs):
                    if mode == "executor_error":
                        # Force an exception out of the executor iterator itself,
                        # before the native runner can send an on_tool_end hook.
                        yield await tool.handler(None, **kwargs)
                    else:
                        async for value in execute(tool, run_context, **kwargs):
                            yield value

                provider.responder = answer
                with (
                    patch.object(agent, "journal", journal),
                    patch.object(executor, "execute", staticmethod(invoke)),
                ):
                    try:
                        async with backend.watch(*credentials) as changes:
                            self.assertEqual(changes.get_nowait()["state"], "sync")
                            await backend.web_submit(*credentials, mode, "请帮我查一下")
                            await asyncio.wait_for(model_entered.wait(), 3)
                            assert_public(await backend.web_state(*credentials), False)

                            started = await poll_after(mode, can_start)
                            assert_public(started, True)
                            self.assertEqual(started.replies[-1].state, "running")
                            await asyncio.wait_for(tool_entered.wait(), 3)
                            self.assertEqual(
                                (await asyncio.wait_for(changes.get(), 3))["state"], "changed"
                            )
                            assert_public(await backend.web_state(*credentials), True)
                            if mode == "hangup":
                                await backend.web_end(*credentials)
                                closed = await backend.web_state(*credentials)
                                self.assertTrue(closed.close_requested)
                                assert_public(closed, False)
                            else:
                                stopped = await poll_after(mode, can_end)
                                assert_public(stopped, False)
                                self.assertEqual(stopped.replies[-1].state, "running")
                                self.assertEqual(
                                    (await asyncio.wait_for(changes.get(), 3))["state"], "changed"
                                )
                                row = await runtime.agent_journal(
                                    "read", binding.call_id, "reply:" + mode
                                )
                                kinds = [v["kind"] for v in json.loads(row["reports_json"])]
                                self.assertIn("tool_start", kinds)
                                self.assertEqual("framework_report" in kinds, mode == "success")
                    finally:
                        can_start.set()
                        can_end.set()
                        can_finish.set()
                        await backend.drain()
                assert_public(await backend.web_state(*credentials), False)
