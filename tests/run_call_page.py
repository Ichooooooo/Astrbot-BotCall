"""Run exactly two browser flows against in-memory ASGI + real worker SQLite.

WSL Node executes original ES modules from memory, no listener or dependency.
All credentials/bytes are synthetic and IPC only, never printed as traffic logs.
"""

import asyncio
import base64
import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
from node_runtime import node_runtime

NODE = node_runtime()
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
from bot_call.access import InvitationSecret
from bot_call.runtime import CallRuntime, RuntimeAudio, RuntimeCallbacks
from bot_call.limits import RuntimeLimits

from bot_call.publication import Outcome
from bot_call.web import CallWebApp
from test_astrbot_context import binding
import test_web_access as observed


async def main():
    print("PYTHON", sys.version.split()[0], flush=True)
    version = subprocess.run(
        [NODE, "--version"], capture_output=True, text=True, check=True
    ).stdout.strip()
    print("NODE", version, flush=True)
    loop = asyncio.get_running_loop()
    calls = []
    replies = []
    reviews = []
    pages = []
    contexts = []
    violations = []
    syntheses = []
    reply_release = asyncio.Event()
    segment_release = asyncio.Event()

    def business():
        if asyncio.get_running_loop() is not loop:
            violations.append("wrong loop")

    async def transcribe(bound, job):
        business()
        calls.append((bound.call_id, job.utterance_id, job.data))
        if bound.call_id in {"D", "G"} or (
            bound.call_id == "F" and job.data.startswith(b"missing")
        ):
            raise RuntimeError("synthetic unknown STT")
        if bound.call_id == "M" and job.series_index == 2:
            raise RuntimeError("synthetic middle-segment failure")
        return " 原样转写 " + job.data.decode("utf-8") + " \r\n"

    async def reply(bound, turn):
        business()
        replies.append(bound.call_id)
        if bound.call_id == "H":
            gid = "reply:" + turn.material_id
            for text in ("这是第一句话。", "这是第二句话。", "这是第三句话。"):
                await runtime.agent_journal("segment", bound.call_id, gid, text)
            await runtime.agent_journal("stream_done", bound.call_id, gid)
            return "这是第一句话。这是第二句话。这是第三句话。"
        if "reply-failure" in turn.body:
            await reply_release.wait()
            raise RuntimeError("synthetic local reply ended without output")
        return "Bot建议 <img src=x onerror=unsafe> " + turn.body

    async def synthesize(bound, turn, guard):
        business()
        assert await guard(bound, turn.material_id)
        syntheses.append((bound.call_id, turn.material_id, getattr(turn, "segment_seq", 0)))
        if bound.call_id == "H" and getattr(turn, "segment_seq", 0) == 3:
            await segment_release.wait()
        if bound.call_id == "H":
            return bytes.fromhex("fffb10c4") + bytes(100)
        return (ROOT / "bot_call/static/fixed-sample.wav").read_bytes()

    async def review(bound, turns, prompt):
        business()
        reviews.append((bound.call_id, turns))
        return "唯一最终回顾 " + bound.call_id

    async def page(bound, value):
        business()
        pages.append((bound.call_id, value.fragment))
        return Outcome.SUBMITTED

    async def readable(bound, key, body):
        business()
        contexts.append((bound.call_id, body))
        return Outcome.CONFIRMED

    with TemporaryDirectory() as directory, patch("bot_call.runtime.Store", observed.ObservedStore):
        observed.ObservedStore.instances = []
        runtime = CallRuntime(
            Path(directory) / "ui.db",
            RuntimeCallbacks(reply, review, page, readable, synthesize),
            audio=RuntimeAudio(transcribe),
            limits=RuntimeLimits(),
        )
        await runtime.start()
        app = CallWebApp(runtime, origin="https://calls.example.test")
        vendor = "vendor/vad-v5-ort-1.22.0-2623a295/"
        assert len(app._assets) == 22
        for name, item in app._assets.items():
            assert item.immutable == (vendor in name)
        assert (
            app._assets["/bot-call/" + vendor + "ort-wasm-simd-threaded.wasm"].content_type
            == b"application/wasm"
        )
        tokens = {}
        for name in ["A", "C", "B", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N"]:
            await runtime.create_call(
                binding(name, "qq-" + name, "cid-" + name), retained_missing=name in {"F", "G", "M"}
            )
            secret = InvitationSecret.create()
            await runtime.issue_web_invitation(name, secret, session_ttl=3600)
            tokens[name] = secret.value
        config = {
            "tokens": tokens,
            "module": base64.b64encode(
                (ROOT / "bot_call/static/call.js")
                .read_text()
                .replace(
                    '"./capture.js"',
                    '"data:text/javascript;base64,'
                    + base64.b64encode((ROOT / "bot_call/static/capture.js").read_bytes()).decode()
                    + '"',
                )
                .replace(
                    '"./echo.js"',
                    '"data:text/javascript;base64,'
                    + base64.b64encode((ROOT / "bot_call/static/echo.js").read_bytes()).decode()
                    + '"',
                )
                .replace(
                    '"./pcm.js"',
                    '"data:text/javascript;base64,'
                    + base64.b64encode((ROOT / "bot_call/static/pcm.js").read_bytes()).decode()
                    + '"',
                )
                .replace(
                    '"./barge.js"',
                    '"data:text/javascript;base64,'
                    + base64.b64encode(
                        (ROOT / "bot_call/static/barge.js")
                        .read_text()
                        .replace(
                            '"./echo.js"',
                            '"data:text/javascript;base64,'
                            + base64.b64encode(
                                (ROOT / "bot_call/static/echo.js").read_bytes()
                            ).decode()
                            + '"',
                        )
                        .encode()
                    ).decode()
                    + '"',
                )
                .encode()
            ).decode(),
            "tests": base64.b64encode((ROOT / "tests/test_call_page.mjs").read_bytes()).decode(),
            "vad": base64.b64encode((ROOT / "bot_call/static/vad.js").read_bytes()).decode(),
            "worklet": base64.b64encode(
                (ROOT / "bot_call/static/vad-worklet.js").read_bytes()
            ).decode(),
        }
        bootstrap = r"""
import readline from 'node:readline';
const lines=readline.createInterface({input:process.stdin});
let boot;const pending=new Map();let sequence=0;
const configPromise=new Promise(resolve=>{boot=resolve;});
lines.on('line',line=>{const data=JSON.parse(line);if(data.config){boot(data.config);return;}const wait=pending.get(data.id);pending.delete(data.id);wait?.(data);});
globalThis.requestASGI=request=>new Promise(resolve=>{const id=++sequence;pending.set(id,resolve);process.stdout.write(JSON.stringify({type:'request',id,...request})+'\n');});
globalThis.closeIPC=()=>{lines.close();process.stdin.unref();};
const config=await configPromise;
globalThis.fixture=config;
globalThis.pageModule=await import('data:text/javascript;base64,'+config.module);
globalThis.vadModule=await import('data:text/javascript;base64,'+config.vad);
await import('data:text/javascript;base64,'+config.tests);
"""
        process = subprocess.Popen(
            [NODE, "--input-type=module", "-e", bootstrap],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        process.stdin.write(json.dumps({"config": config}) + "\n")
        process.stdin.flush()
        pending = set()
        output_lock = asyncio.Lock()
        passed = []
        failure = None

        async def handle(request):
            # Private test IPC controls an injected fake; no public HTTP route.
            if request.get("control") == "release_segments":
                segment_release.set()
                async with output_lock:
                    await asyncio.to_thread(write, {"id": request["id"]})
                return
            if request.get("control") == "release_reply":
                reply_release.set()
                async with output_lock:
                    await asyncio.to_thread(write, {"id": request["id"]})
                return
            responses = []
            scope = {
                "type": "http",
                "scheme": "https",
                "method": request["method"],
                "path": request["path"],
                "query_string": b"",
                "headers": [
                    (k.encode("ascii"), v.encode("ascii")) for k, v in request["headers"].items()
                ],
            }
            events = [
                {
                    "type": "http.request",
                    "body": base64.b64decode(request["body"]),
                    "more_body": False,
                }
            ]

            async def receive():
                return events.pop(0) if events else {"type": "http.disconnect"}

            async def send(event):
                responses.append(event)

            await app(scope, receive, send)
            result = {
                "id": request["id"],
                "status": responses[0]["status"],
                "headers": [(k.decode(), v.decode()) for k, v in responses[0]["headers"]],
                "body": base64.b64encode(responses[1]["body"]).decode(),
            }
            async with output_lock:
                await asyncio.to_thread(write, result)

        def write(result):
            process.stdin.write(json.dumps(result) + "\n")
            process.stdin.flush()

        try:
            while True:
                line = await asyncio.wait_for(asyncio.to_thread(process.stdout.readline), 30)
                if not line:
                    break
                data = json.loads(line)
                if data["type"] == "request":
                    task = asyncio.create_task(handle(data))
                    pending.add(task)

                    def collected(done):
                        pending.discard(done)
                        if not done.cancelled():
                            done.exception()

                    task.add_done_callback(collected)
                elif data["type"] == "scenario":
                    passed.append(data["name"])
                    print("PASS", data["name"], flush=True)
                elif data["type"] == "facet":
                    print("PASS", data["name"], flush=True)
                elif data["type"] == "failed":
                    failure = data
                    break
                elif data["type"] == "done":
                    break
            if pending:
                await asyncio.gather(*pending)
            code = await asyncio.to_thread(process.wait, timeout=20)
            stderr = await asyncio.to_thread(process.stderr.read)
            if failure:
                import re

                frames = []
                for line in failure.get("stack", "").splitlines()[1:]:
                    location = re.search(r":([0-9]+):([0-9]+)\)?$", line)
                    if location:
                        source = (
                            "call.js"
                            if config["module"] in line
                            else "test_call_page.mjs"
                            if config["tests"] in line
                            else "module"
                        )
                        frames.append(source + ":" + ":".join(location.groups()))
                raise AssertionError(failure["message"] + " [" + ", ".join(frames) + "]")
            if code or stderr:
                raise AssertionError("Node process failed: " + stderr[:1500])
            assert len(passed) == 2
            await runtime.drain()
            assert violations == []
            assert len(syntheses) == len(set(syntheses))
            assert len([x for x in syntheses if x[0] == "A"]) == 4
            assert [(call, data) for call, uid, data in calls if call == "A"] == [
                ("A", v.encode())
                for v in (
                    "first-tail🙂",
                    "tts-failure-tail🙂",
                    "reply-failure-tail🙂",
                    "recovered-tail🙂",
                    "last-tail🙂",
                )
            ]
            assert len([c for c in calls if c[0] == "J"]) == 3
            assert replies.count("J") == 1
            assert len([c for c in calls if c[0] == "K"]) == 1
            assert len([c for c in calls if c[0] == "C"]) == 1
            assert len([c for c in calls if c[0] == "D"]) == 1
            assert not [c for c in calls if c[0] == "B"]
            for name in ["A", "C"]:
                state = await runtime.read_state(name)
                assert state.publication.complete
                assert "".join(body for call, body in pages if call == name) == state.review.body
                assert [body for call, body in contexts if call == name] == [state.review.body]
            partial = await runtime.read_state("F")
            assert partial.publication.complete and partial.review.body == "唯一最终回顾 F"
            assert partial.publication.body.startswith("材料说明：")
            assert "".join(body for call, body in pages if call == "F") == partial.publication.body
            assert [body for call, body in contexts if call == "F"] == [partial.publication.body]
            assert len([c for c in calls if c[0] == "F"]) == 2
            all_missing = await runtime.read_state("G")
            assert all_missing.review is None and all_missing.publication is None
            assert all_missing.audio_jobs[0].resolution.decision == "retained_missing"
            assert not [v for v in replies + syntheses + pages + contexts if v[0] == "G"]
            unknown = await runtime.read_state("D")
            assert unknown.review is None and unknown.audio_jobs[0].state == "uncertain"
            assert unknown.audio_jobs[0].data is not None
            print(
                "UI_SCENARIOS 2; real temporary SQLite/ASGI; no listener; fake microphone/STT/TTS; EXIT 0",
                flush=True,
            )
        finally:
            reply_release.set()
            segment_release.set()
            if process.poll() is None:
                process.terminate()
                await asyncio.to_thread(process.wait)
            for pipe in (process.stdin, process.stdout, process.stderr):
                pipe.close()
            await runtime.shutdown(timeout=None)
        for store in observed.ObservedStore.instances:
            assert store.threads == {store.owner} and store.closed_thread == store.owner


if __name__ == "__main__":
    asyncio.run(main())
