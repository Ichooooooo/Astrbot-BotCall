"""Real Linux Worklet/Worker/Opus; generated sound, no microphone/provider."""

import json
import os
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import check_local_vad_browser as smoke

smoke.DRIVER = smoke.DRIVER.replace("result.peakRuns!==1", "false").replace(
    "wasmRequests!==1||modelRequests!==1", "wasmRequests!==0||modelRequests!==0"
)
smoke.BROWSER_JS = r"""
import {ContinuousCapture} from '/bot-call/capture.js';
import {CallPage} from '/bot-call/call.js';
const wait=ms=>new Promise(r=>setTimeout(r,ms));
const check=(v,s)=>{if(!v)throw Error(s);};
async function until(fn,ms=8000){const end=performance.now()+ms;while(!fn()&&performance.now()<end)await wait(10);check(fn(),'deadline');}
const result={ok:false,files:[],cases:[]};let context,capture,osc,destination;
const base64=async blob=>{const a=new Uint8Array(await blob.arrayBuffer());let s='';for(let i=0;i<a.length;i+=8192)s+=String.fromCharCode(...a.subarray(i,i+8192));return btoa(s);};
try {
  navigator.mediaDevices.getUserMedia=()=>{throw Error('real_mic_forbidden');};
  for(const rate of [48000,44100]) {
    result.stage='rate-'+rate;
    context=new AudioContext({sampleRate:rate});await context.resume();
    destination=context.createMediaStreamDestination();
    const gain=context.createGain();gain.gain.value=0;
    osc=context.createOscillator();osc.frequency.value=440;osc.connect(gain);gain.connect(destination);osc.start();
    const page={audioContext:context,stream:destination.stream,config:{rmsThreshold:.025,segmentMs:30000},
      e:{probeWindow:window,MediaRecorder,Blob,crypto}};
    capture=await ContinuousCapture.create(page);
    const reference=new MediaRecorder(destination.stream,{mimeType:'audio/webm;codecs=opus',audioBitsPerSecond:24000}),referenceParts=[];
    reference.ondataavailable=e=>{if(e.data.size)referenceParts.push(e.data);};reference.start(250);
    const recorder=capture.recorder(),parts=[];recorder.ondataavailable=e=>parts.push(e.data);
    recorder.start();await wait(800);
    check(!recorder.begun&&parts.length===0&&recorder.pcm.length===0,'idle encoded');
    const onset=context.currentTime;
    osc.frequency.setValueAtTime(300,onset);osc.frequency.linearRampToValueAtTime(1000,onset+.08);osc.frequency.setValueAtTime(440,onset+.1);
    gain.gain.value=.2;await wait(900);gain.gain.value=0;await wait(100);
    const done=new Promise(resolve=>recorder.onstop=resolve),referenceDone=new Promise(resolve=>reference.onstop=resolve);
    recorder.stop();reference.stop();await Promise.all([done,referenceDone]);
    result.captureState={failed:recorder.failed,clock:recorder.clock,samples:recorder.samples,prefix:recorder.prefixSamples,stats:recorder.stats};
    check(recorder.stats&&recorder.prefixSamples>=23040&&recorder.prefixSamples<=24000,'prefix');
    const blob=new Blob(parts,{type:recorder.mimeType});
    result.files.push({name:'capture-'+rate+'.webm',data:await base64(blob)});
    result.files.push({name:'legacy-'+rate+'.webm',data:await base64(new Blob(referenceParts,{type:reference.mimeType}))});
    result.cases.push({rate,samples:recorder.samples,prefix:recorder.prefixSamples,...recorder.stats,queue:recorder.peak,bytes:blob.size});
    capture.close();osc.stop();for(const t of destination.stream.getTracks())t.stop();await context.close();
  }
  result.stage='offline-long';
  context=new AudioContext({sampleRate:48000});await context.resume();
  destination=context.createMediaStreamDestination();osc=context.createOscillator();osc.connect(destination);osc.start();
  {
    const owner={audioContext:context,stream:destination.stream,config:{rmsThreshold:.025,segmentMs:30000,maxAudioBytes:2097152},e:{probeWindow:window,Blob,crypto,now:()=>performance.now()}};
    capture=await ContinuousCapture.create(owner);
    const r={id:crypto.randomUUID(),samples:1464000,pcm:Array.from({length:1525},(_,i)=>Float32Array.from({length:960},(_,j)=>.1*Math.sin(2*Math.PI*440*(i*960+j)/48000)))};
    const began=performance.now(),blob=await capture.offlineOpus(r),ms=performance.now()-began;
    check(ms<30500/4,'offline encoding near realtime');
    result.files.push({name:'recovered-offline-long.webm',data:await base64(blob)});
    result.cases.push({recovered:'offline-long',samples:r.samples,bytes:blob.size,kind:3,ms});r.pcm.forEach(a=>a.fill(0));
    capture.close();osc.stop();for(const t of destination.stream.getTracks())t.stop();await context.close();
  }
  result.stage='fallback';
  for(const mode of ['api','config','worker','worker-start','runtime','mux']) {
    context=new AudioContext({sampleRate:48000});await context.resume();
    destination=context.createMediaStreamDestination();
    osc=context.createOscillator();osc.connect(destination);osc.start();
    const nativeWorker=Worker;let workers=0;
    const win={Worker:mode==='api'?undefined:class extends nativeWorker {
      constructor(url){if(mode==='worker-start'&&++workers>1)throw Error('worker-start');super(url);
        if(mode==='config')queueMicrotask(()=>this.dispatchEvent(new MessageEvent('message',{data:{type:'error',id:'probe',code:2}})));
        if(mode==='worker')queueMicrotask(()=>this.dispatchEvent(new Event('error')));
      }},
      AudioWorkletNode,OfflineAudioContext,setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window)};
    const page=new CallPage({probeWindow:win,storage:{getItem:()=>'',setItem(){}},location:{hash:'',pathname:'/bot-call/'},history,
      now:()=>performance.now(),Audio,URL,Blob,crypto,AbortController,MediaRecorder,AudioContext,
      mediaDevices:{getUserMedia:async()=>destination.stream.clone()},setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window)});
    page.started=true;page.reference='synthetic';page.server={audio_available:true,speech_available:true};page.audioContext=context;page.pcmContext=context;
    page.api=async()=>({recorded:true});page.refresh=async()=>page.server;
    await page.listen();check(page.recording,'recording absent');
    if(!['worker-start','runtime','mux'].includes(mode))check(page.recording.recorder.samples===undefined&&page.captureFailure>0,'startup fallback');
    else {
      await until(()=>page.recording.recorder.samples>9600);
      const r=page.recording;
      page.submitSegment=async reason=>{await page.finishSegment(reason);};
      const failedWorker=r.recorder.worker;
      if(mode==='worker-start')check(r.recorder.failed===3,'worker construction fallback');
      else if(mode==='mux')failedWorker.dispatchEvent(new MessageEvent('message',{data:{type:'error',id:r.recorder.id,code:5,metrics:{client_capture_mux_reason:3,client_capture_packet:2}}}));
      else page.capture.fail(4);
      if(mode!=='worker-start')failedWorker.dispatchEvent(new MessageEvent('message',{data:{type:'data',id:r.recorder.id,data:new Uint8Array([1,2,3]).buffer}}));
      check(r.chunks.length===0,'stale encoder bytes mixed with recovery');
      await wait(200);check(page.recording===r&&!r.stopping&&r.recorder.samples>9600,'fallback truncated speech');
      await page.finishSegment('manual');await until(()=>page.clip,8000);
      result.fallbackState={mode,metrics:page.clip.metrics,error:page.clip.error,input:page.captureInputDiagnostics,failure:page.captureFailure,reason:page.clip.reason};
      check(page.clip.blob.size>0&&!page.clip.error&&page.clip.id===r.id,'runtime recovery');
      if(mode==='mux')check(page.clip.metrics.client_capture_mux_reason===3&&page.clip.metrics.client_capture_packet===2,'mux subreason lost');
      result.files.push({name:'recovered-'+mode+(page.clip.metrics.client_capture_recovery_kind===3?'.webm':'.wav'),data:await base64(page.clip.blob)});
      result.cases.push({recovered:mode,samples:r.recorder.samples,bytes:page.clip.blob.size,
        kind:page.clip.metrics.client_capture_recovery_kind,ms:page.clip.metrics.client_capture_recovery_ms});
      check(page.clip.metrics.client_capture_recovery_kind===(mode==='worker-start'?1:3),'offline Opus/WAV selection');
      page.clip=null;await page.listen();
      if(mode==='mux') {
        check(page.recording.recorder.samples!==undefined&&!page.captureDisabled&&page.captureMuxFailures===1,'mux first retry');
        check(page.recording.metrics.client_capture_fallback===undefined,'previous fallback leaked into retry metrics');
        await until(()=>page.recording.recorder.samples>9600);
        const second=page.recording;page.capture.fail(5);
        await wait(200);check(page.recording===second&&!second.stopping,'second failure truncated');
        page.capture.fail(7);
        check(second.error&&second.stopping&&page.capture.closed,'input loss after encoder failure did not stop');
        await page.finishSegment('manual');check(page.captureDisabled&&page.captureMuxFailures===2,'mux retry limit');
        page.clip=null;await page.listen();
      }
      check(page.recording.recorder.samples===undefined,'runtime fallback next capture');
    }
    result.cases.push({mode,fallback:page.captureFailure});
    await page.finishSegment('control');page.clip?.uploader?.abandon();page.clip=null;page.releaseMic();
    osc.stop();for(const t of destination.stream.getTracks())t.stop();await context.close();
  }
  result.stage='m5-recovery';
  context=new AudioContext({sampleRate:48000});await context.resume();
  destination=context.createMediaStreamDestination();osc=context.createOscillator();osc.connect(destination);osc.start();
  const recoveryPage={audioContext:context,stream:destination.stream,config:{rmsThreshold:.025,segmentMs:400},
    e:{probeWindow:window,MediaRecorder,Blob,crypto}};
  capture=await ContinuousCapture.create(recoveryPage);
  const recoveredSegments=[];
  for(let index=0;index<2;index++) {
    const r=capture.recorder(index>0),parts=[];
    r.ondataavailable=e=>parts.push(e.data);
    const done=new Promise(resolve=>r.onstop=resolve);
    r.onlimit=()=>{r.continueAfter=true;r.stop();};
    r.start();
    if(index===0){await until(()=>r.samples>=4800);capture.fail(4);}
    await done;
    check(r.samples>=19200&&r.failed===4&&parts.length>0,'M5 fallback segment');
    recoveredSegments.push({samples:r.samples,first:r.firstSample,last:r.lastSample});
  }
  check(recoveredSegments[0].last===recoveredSegments[1].first,'M5 recovery gap/duplicate');
  check(capture.carry.length<=1550,'M5 recovery carry unbounded');
  result.recoveryHandoff=recoveredSegments;
  capture.close();osc.stop();for(const t of destination.stream.getTracks())t.stop();await context.close();
  result.stage='m5-mux-retry';
  context=new AudioContext({sampleRate:48000});await context.resume();
  destination=context.createMediaStreamDestination();osc=context.createOscillator();osc.connect(destination);osc.start();
  const retryPage={audioContext:context,stream:destination.stream,config:{rmsThreshold:.025,segmentMs:1000},
    e:{probeWindow:window,MediaRecorder,Blob,crypto}};
  capture=await ContinuousCapture.create(retryPage);
  const retrySegments=[];
  for(let index=0;index<2;index++) {
    const r=capture.recorder(index>0);
    const done=new Promise(resolve=>r.onstop=resolve);
    r.onlimit=()=>{r.continueAfter=true;r.stop();};
    r.start();
    if(index===0){await until(()=>r.samples>=4800);capture.fail(5);}
    await done;
    check(r.samples>=48000,'M5 mux truncated');
    if(index===1) {
      result.retryState={samples:r.samples,failed:r.failed,stats:r.stats,gap:r.gapMetrics,workerMetrics:r.metrics,clock:r.clock,captureFailure:retryPage.captureFailure,queue:r.queue.length,pending:r.pending,input:retryPage.captureInputDiagnostics,disabled:retryPage.captureDisabled,muxFailures:retryPage.captureMuxFailures};
      check(r.stats&&!r.failed&&!retryPage.captureDisabled&&retryPage.captureMuxFailures===0,'M5 buffered encoder retry failed');
    }
    retrySegments.push({samples:r.samples,first:r.firstSample,last:r.lastSample,failed:r.failed||0});
  }
  check(retrySegments[0].last===retrySegments[1].first,'M5 mux retry gap/duplicate');
  result.muxRetryHandoff=retrySegments;
  capture.close();osc.stop();for(const t of destination.stream.getTracks())t.stop();await context.close();
  result.stage='cancel-preflight';
  context=new AudioContext({sampleRate:48000});await context.resume();
  destination=context.createMediaStreamDestination();
  const nativeWorker=Worker;
  let probeStartedResolve;const probeStarted=new Promise(resolve=>{probeStartedResolve=resolve;});
  const slow={AudioWorkletNode,OfflineAudioContext,setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window),
    Worker:class extends nativeWorker {
      postMessage(m){if(m.type==='check'){probeStartedResolve();this.delay=setTimeout(()=>super.postMessage(m),300);}else super.postMessage(m);}
      terminate(){clearTimeout(this.delay);super.terminate();}
    }};
  const cancelled=new CallPage({probeWindow:slow,storage:{getItem:()=>'',setItem(){}},location:{hash:'',pathname:'/bot-call/'},history,
    now:()=>performance.now(),Audio,URL,Blob,crypto,AbortController,MediaRecorder,AudioContext,
    mediaDevices:{getUserMedia:async()=>destination.stream.clone()},setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window)});
  cancelled.started=true;cancelled.reference='synthetic';cancelled.server={audio_available:true,speech_available:true};
  cancelled.audioContext=context;cancelled.pcmContext=context;
  const listening=cancelled.listen();await probeStarted;const granted=cancelled.stream,owner=cancelled.capture;
  check(owner?.probe,'initialization not owned');
  cancelled.muted=true;cancelled.epoch++;cancelled.releaseMic();check(owner.closed&&granted.getTracks().every(t=>t.readyState==='ended'),'cancel not immediate');await listening;await wait(350);
  check(!cancelled.capture&&!cancelled.recording&&!cancelled.stream&&granted.getTracks().every(t=>t.readyState==='ended'),'late capture after cancel');
  result.cancelledDuringPreflight=true;
  for(const t of destination.stream.getTracks())t.stop();await context.close();
  result.ok=true;
}catch(e){result.error=e.message;}
finally {
  capture?.close();try{osc?.stop();}catch(_){}
  for(const t of destination?.stream.getTracks()||[])t.stop();
  try{await context?.close();}catch(_){}
  result.contextClosed=context?.state==='closed';window.smokeResult=result;
}
"""


def main():
    with TemporaryDirectory(prefix="bot-call-capture-") as tmp:
        directory = Path(os.environ.get("BOT_CALL_CAPTURE_EVIDENCE", tmp))
        directory.mkdir(parents=True, exist_ok=True)
        smoke.DRIVER = smoke.DRIVER.replace(
            "observed=result;const urls=",
            "if(result?.files){for(const f of result.files)fs.writeFileSync("
            + json.dumps(str(directory) + "/")
            + "+f.name,Buffer.from(f.data,'base64'));delete result.files;}"
            + "if(result)fs.writeFileSync("
            + json.dumps(str(directory / "browser-result.json"))
            + ",JSON.stringify(result));observed=result;const urls=",
        )
        smoke.main()
        result = json.loads((directory / "browser-result.json").read_text())
        for case in result["cases"]:
            if "rate" not in case:
                continue
            path = directory / f"capture-{case['rate']}.webm"
            decoded = subprocess.run(
                [
                    "ffmpeg",
                    "-v",
                    "error",
                    "-i",
                    str(path),
                    "-f",
                    "f32le",
                    "-ac",
                    "1",
                    "-ar",
                    "48000",
                    "-",
                ],
                check=True,
                capture_output=True,
            ).stdout
            assert len(decoded) // 4 == case["samples"], (len(decoded) // 4, case)
            import array

            pcm = array.array("f", decoded)
            onset = next(i for i, value in enumerate(pcm) if abs(value) > 0.02)
            assert 20000 <= onset <= 25000, onset
            assert max(abs(v) for v in pcm[onset : onset + 2400]) > 0.15
            legacy = subprocess.run(
                [
                    "ffmpeg",
                    "-v",
                    "error",
                    "-i",
                    str(directory / f"legacy-{case['rate']}.webm"),
                    "-f",
                    "f32le",
                    "-ac",
                    "1",
                    "-ar",
                    "48000",
                    "-",
                ],
                check=True,
                capture_output=True,
            ).stdout
            reference = array.array("f", legacy)
            reference_onset = next(i for i, v in enumerate(reference) if abs(v) > 0.02)
            assert 30000 <= reference_onset <= 45000, reference_onset
            # Same chirp source, independent old recorder; compare the first
            # 80ms after onset. A saved rerun needs +336 samples (7ms) of
            # alignment; the v0.9.15 failure has a separate unresolved waveform
            # difference. Search one 20ms packet instead of the former +/-5ms,
            # but flag offsets above 15ms separately rather than hiding them.
            a = pcm[onset : onset + 3840]
            best = -1
            best_shift = None
            for shift in range(-960, 961):
                start = reference_onset + shift
                b = reference[start : start + len(a)]
                if len(b) != len(a):
                    continue
                dot = sum(x * y for x, y in zip(a, b))
                power = sum(x * x for x in a) * sum(y * y for y in b)
                if power:
                    correlation = dot / power**0.5
                    if correlation > best:
                        best, best_shift = correlation, shift
            assert best > 0.75, best
            assert abs(best_shift) <= 720, ("capture alignment exceeds 15ms", best_shift)
            comparison = dict(
                rate=case["rate"],
                same_source=True,
                first_80ms_correlation=best,
                alignment_samples=best_shift,
                alignment_ms=best_shift / 48,
                auto_onset_samples=onset,
                legacy_onset_samples=reference_onset,
            )
            (directory / f"prefix-comparison-{case['rate']}.json").write_text(
                json.dumps(comparison, indent=2)
            )
            print(json.dumps(comparison))

            print(
                json.dumps(
                    dict(
                        rate=case["rate"],
                        decoded_samples=len(pcm),
                        onset_samples=onset,
                        exact_length=True,
                    )
                )
            )
            probe = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-show_format",
                    "-show_streams",
                    "-of",
                    "json",
                    str(path),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            (directory / f"ffprobe-{case['rate']}.json").write_text(probe.stdout)
        for case in result["cases"]:
            if "rate" in case:
                verify_packet_timeline(directory / f"capture-{case['rate']}.webm", case["samples"])
        for case in result["cases"]:
            if "recovered" in case and case["kind"] == 3:
                verify_packet_timeline(
                    directory / f"recovered-{case['recovered']}.webm", case["samples"]
                )
                assert case["bytes"] < case["samples"] / 48 * 8
        verify_stt_inputs(directory)


def verify_packet_timeline(path, expected_samples):
    data = json.loads(
        subprocess.run(
            ["ffprobe", "-v", "error", "-show_format", "-show_packets", "-of", "json", str(path)],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    packets = data["packets"]
    last = packets[-1]
    discard = sum(side.get("discard_padding", 0) for side in last.get("side_data_list", []))
    # Opus CodecDelay is already subtracted from packet PTS by the demuxer.
    end = float(last["pts_time"]) + float(last["duration_time"]) - discard / 48000
    actual = subprocess.run(
        ["ffmpeg", "-v", "error", "-i", str(path), "-ar", "48000", "-ac", "1", "-f", "f32le", "-"],
        check=True,
        capture_output=True,
    ).stdout
    decoded = len(actual) // 4
    assert decoded == expected_samples, (path, decoded, expected_samples)
    assert abs(end * 48000 - expected_samples) <= 1, (path, end, expected_samples)
    summary = dict(
        file=path.name,
        expected_samples=expected_samples,
        decoded_samples=decoded,
        packet_end_seconds=end,
        discard_samples=discard,
        format_duration=data.get("format", {}).get("duration", "N/A"),
    )
    path.with_suffix(".ffprobe.json").write_text(json.dumps(data, indent=2))
    path.with_suffix(".timeline.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary))
    return summary


def verify_stt_inputs(directory):
    import hashlib
    import unittest
    import wave
    from test_backend import NativeCase
    from test_astrbot_context import binding
    from bot_call.astrbot_audio import AudioRequestPolicy, AstrBotAudioAdapter, convert_stt_audio
    from bot_call.native_provider import NATIVE_CONTRACT
    from bot_call.audio import AudioJob

    observations = []

    class CaptureSTTCase(NativeCase):
        async def test_real_converter_and_native_provider_input(self):
            event, _ = self.environment()
            target = binding("audio", "qq-A", "A")
            stt = self.context.stt[event.unified_msg_origin]
            host = self.host()
            policy = AudioRequestPolicy(
                target, stt.meta().id, stt.get_model(), 2097152, 16777216, NATIVE_CONTRACT
            )
            adapter = await AstrBotAudioAdapter.bind(
                self.context, policy, converter=convert_stt_audio, privacy=host.privacy
            )
            for path in sorted([*directory.glob("*.webm"), *directory.glob("*.wav")]):
                reference = subprocess.run(
                    [
                        "ffmpeg",
                        "-v",
                        "error",
                        "-i",
                        str(path),
                        "-ar",
                        "16000",
                        "-ac",
                        "1",
                        "-f",
                        "s16le",
                        "-",
                    ],
                    check=True,
                    capture_output=True,
                ).stdout
                paths = []

                async def get_text(audio_url):
                    paths.append(Path(audio_url))
                    with wave.open(audio_url) as audio:
                        self.assertEqual(
                            (audio.getframerate(), audio.getnchannels(), audio.getsampwidth()),
                            (16000, 1, 2),
                        )
                        self.assertEqual(audio.readframes(audio.getnframes()), reference)
                    return "synthetic_capture_ok"

                stt.get_text = get_text
                data = path.read_bytes()
                job = AudioJob(
                    "audio",
                    path.stem,
                    1,
                    "audio/wav" if path.suffix == ".wav" else "audio/webm;codecs=opus",
                    hashlib.sha256(data).hexdigest(),
                    len(data),
                    1.0,
                    "running",
                    data,
                    None,
                    None,
                    None,
                )
                self.assertEqual(await adapter.transcribe(target, job), "synthetic_capture_ok")
                self.assertEqual(len(paths), 1)
                self.assertFalse(paths[0].exists())
                observations.append(
                    dict(
                        file=path.name,
                        wav_samples=len(reference) // 2,
                        pcm_sha256=hashlib.sha256(reference).hexdigest(),
                        native_calls=1,
                        exact_input=True,
                        temporary_cleaned=True,
                    )
                )

    result = unittest.TextTestRunner(verbosity=1).run(
        unittest.TestSuite([CaptureSTTCase("test_real_converter_and_native_provider_input")])
    )
    if not result.wasSuccessful():
        raise SystemExit(1)
    (directory / "stt-roundtrip.json").write_text(json.dumps(observations, indent=2))
    print(json.dumps(dict(real_ffmpeg_native_stt=True, files=len(observations))))


if __name__ == "__main__":
    main()
