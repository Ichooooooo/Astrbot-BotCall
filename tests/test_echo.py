"""Only synthetic scalars; verify unknowns and direct durable-entry validation."""

import base64
from pathlib import Path
import subprocess
import unittest
from node_runtime import node_runtime
from bot_call.observability import ECHO_METRICS, echo_values_valid
from bot_call.store import Store
from test_astrbot_context import binding
from tempfile import TemporaryDirectory


class EchoTests(unittest.TestCase):
    def test_scalar_envelope_and_unknowns(self):
        module = base64.b64encode(
            (Path(__file__).resolve().parents[1] / "bot_call/static/echo.js").read_bytes()
        ).decode()
        code = """
import {EchoWindow,EchoProbe} from 'data:text/javascript;base64,MODULE';
import assert from 'node:assert/strict';
let seed=1234;const ref=Array.from({length:180},()=>{seed=(seed*1664525+1013904223)>>>0;return .01+(seed/4294967296)*.3;});
const echo=new EchoWindow();
for(let i=0;i<180;i++)echo.push({mic:i>=5?ref[i-5]*.2:.001,ref:ref[i],vad:.9,tick:i});
const e=echo.summary();assert.equal(e.corr_q,1000);assert.equal(e.lag_ms,160);assert.equal(e.run_65_ms,5760);assert.equal(e.candidates,1);
const mixed=new EchoWindow();for(let i=0;i<180;i++)mixed.push({mic:ref[(i*7)%180],ref:ref[i],vad:i>70&&i<90?.95:.1,tick:i});
assert.equal(mixed.summary().vad_65_ms,19*32);
const cold=new EchoWindow();for(let i=0;i<80;i++)cold.push({mic:.01,ref:.1,tick:i});
assert.equal(cold.summary().vad_ms,undefined);assert.equal(cold.summary().run_65_ms,undefined);assert.equal(cold.summary().corr_q,undefined);
const gap=new EchoWindow();for(let i=0;i<80;i++)gap.push({mic:ref[i],ref:ref[i],vad:i===40?undefined:.9,tick:i+(i>20?1:0)});
assert.equal(gap.summary().run_65_ms,undefined);assert.equal(gap.summary().corr_q,undefined);
const mp3=new EchoWindow();for(let i=0;i<300;i++)mp3.push({mic:.02,vad:.8,tick:i});
assert.equal(mp3.summary().frames,250);assert.equal(mp3.summary().ref_p90,undefined);assert.equal(mp3.summary().lag_ms,undefined);
// Gap metadata reaches the consumer in frame order; a clock gap alone
// cannot erase a candidate while earlier model inference is still pending.
const frames=[],page={bargeRun:{count:3},bargeEnabled:()=>true,bargeFrame:f=>frames.push(f)};
const probe=Object.assign(Object.create(EchoProbe.prototype),{page,recurrent:true,context:{currentTime:1},values:{client_echo_flags:0},
  base:new EchoWindow(),windows:{},refs:new Map(),pending:0,task:Promise.resolve(),model:{run:async()=>.9}});
page.echo=probe;
probe.message({type:'begun',start:0,rate:48000});
probe.message({type:'gap',start:960,count:1,lostMs:20,reason:'clock'});
assert.equal(page.bargeRun.count,3);
probe.message({type:'frame',seq:1,start:960,end:2496,valid:512,samples:new Float32Array(512).buffer});
await probe.task;assert.equal(frames[0].gap,true);assert.equal(frames[0].gapMs,20);assert.equal(frames[0].gapSerial,1);
probe.message({type:'frame',seq:2,start:2496,end:4032,valid:512,samples:new Float32Array(512).buffer});
await probe.task;assert.equal(frames[1].gap,false);
probe.message({type:'gap',start:4032,count:9,lostMs:20,reason:'clock'});
probe.message({type:'frame',seq:3,start:4032,end:5568,valid:512,samples:new Float32Array(512).buffer});
await probe.task;assert.equal(frames[2].gapMs,Infinity);assert.equal(frames[2].gapSerial,2);
console.log('ECHO_SCALARS_PASS');
""".replace("MODULE", module)
        subprocess.run([node_runtime(), "--input-type=module"], input=code, text=True, check=True)

    def test_direct_store_atomic_validation_and_dedup(self):
        gid = "page:echo:12345678-1234-1234-1234-123456789abc"
        fields = {"client_echo_schema": 1, "client_echo_pcm_frames": 20}
        self.assertEqual(len(ECHO_METRICS), 45)
        self.assertTrue(echo_values_valid(gid, fields))
        window = gid + ":12345678-1234-1234-1234-123456789abc:2:3"
        self.assertTrue(
            echo_values_valid(window, dict(fields, client_echo_sentence=2, client_echo_window=3))
        )
        self.assertFalse(
            echo_values_valid(window, dict(fields, client_echo_sentence=1, client_echo_window=3))
        )
        self.assertFalse(
            echo_values_valid(
                window[:-1] + "9999", dict(fields, client_echo_sentence=2, client_echo_window=9999)
            )
        )
        with TemporaryDirectory() as directory, Store(Path(directory) / "test.db") as store:
            store.create_call(binding("synthetic", "qq", "cid"))

            def record(generation, values):
                store.record_timings(
                    "synthetic",
                    generation,
                    [
                        dict(stage=k, sequence=0, start_ms=0, elapsed_ms=v)
                        for k, v in values.items()
                    ],
                )

            for generation, values in [
                ("reply:x", fields),
                (gid, dict(fields, client_echo_pcm_frames=251)),
                (gid, dict(fields, client_echo_route=True)),
                (gid, dict(fields, client_upload=1)),
            ]:
                record(generation, values)
            self.assertEqual(
                store._db.execute("SELECT count(*) FROM turn_timings").fetchone()[0], 0
            )
            record(gid, fields)
            record(gid, dict(fields, client_echo_pcm_frames=0))
            self.assertEqual(
                store._db.execute("SELECT count(*) FROM turn_timings").fetchone()[0], 2
            )
            self.assertEqual(store.timing_snapshot()["stages"], [])
            record("reply:older", {"stt_call": 123})
            for index in range(55):
                record(f"page:echo:{index:08x}-1234-1234-1234-123456789abc", fields)
            # Frequent observation windows must not displace business timing.
            stages = store.timing_snapshot()["stages"]
            speech = next(item for item in stages if item["stage"] == "stt_call")
            self.assertEqual(
                (speech["count"], speech["median_ms"], speech["p90_ms"]), (1, 123, 123)
            )
