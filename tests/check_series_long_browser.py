"""Real 30s MediaRecorder/Silero regression; loopback synthetic speech, no providers.

The bundled public synthetic voice is cropped and repeated with short syllable
pauses, gain changes and quiet passages. --baseline records failures as evidence.
"""

import argparse
import array
import hashlib
import json
import math
import os
import subprocess
import sys
import wave
from pathlib import Path
from tempfile import TemporaryDirectory

import check_local_vad_browser as smoke

from bot_call.web import _Static

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument(
    "--case",
    choices=(
        "long",
        "boundary",
        "pause",
        "manual",
        "cold",
        "transition",
        "pauses",
        "noise",
        "quiet",
    ),
    default="long",
)
parser.add_argument("--capture-mode", choices=("auto", "legacy"), default="auto")
parser.add_argument("--baseline", action="store_true")
parser.add_argument("--single", action="store_true")
args, rest = parser.parse_known_args()
sys.argv = [sys.argv[0], "--capture-mode", args.capture_mode, *rest]
# Only synthetic speech enters the MediaStream. Playback starts after listen(),
# so model/recorder setup cannot consume the test audio before recording.
fixture_directory = TemporaryDirectory(prefix="bot-call-speech-")
fixture_path = Path(fixture_directory.name) / "speech.wav"
cold_case = args.case in {"cold", "transition", "noise"}
voice_seconds = (
    (22 if args.case == "transition" else 4)
    if cold_case
    else (
        29.4
        if args.case == "boundary"
        else 32
        if args.case == "pause"
        else 34
        if args.case == "quiet"
        else 103
    )
)
pauses = (
    [
        (14, 0.6),
        (29.7, 0.9),
        (31.5, 1.2),
        (44, 0.9),
        (59, 1.2),
        (61.5, 0.6),
        (74, 1.2),
        (89, 0.6),
        (91.5, 0.9),
    ]
    if args.case == "pauses"
    else []
)
with wave.open(str(smoke.ROOT / "bot_call/static/fixed-sample.wav")) as f:
    rate = f.getframerate()
    pcm = array.array("h", f.readframes(200000))
voice = pcm[round(0.55 * rate) : round(2.5 * rate)]
gains = [(0, 0.12 if cold_case else 1), (28, 0.07), (38, 0.65), (58.8, 0.10), (67, 0.8), (87, 0.18)]
out = array.array("h")
seed = 123456789
for i in range(round((voice_seconds + 5) * rate)):
    t = i / rate
    gain = next((g for at, g in reversed(gains) if at <= t), 0)
    if args.case == "noise":
        gain = 0.9
    if (
        t < 0.15
        or t >= voice_seconds
        or args.case == "quiet"
        or any(at <= t < at + d for at, d in pauses)
    ):
        gain = 0
    value = voice[max(0, i - round(0.15 * rate)) % len(voice)] * gain
    if args.case == "noise" and t >= 0:
        seed = (1664525 * seed + 1013904223) & 0xFFFFFFFF
        value += 32768 * (
            (seed / 4294967296 * 2 - 1) * 0.00693 + 0.002 * math.sin(2 * math.pi * 120 * t)
        )
    out.append(max(-32768, min(32767, round(value))))
with wave.open(str(fixture_path), "wb") as f:
    f.setparams((1, 2, rate, 0, "NONE", "not compressed"))
    f.writeframes(out.tobytes())
fixture_sha = hashlib.sha256(fixture_path.read_bytes()).hexdigest()
original_app = smoke.CallWebApp


class FixtureApp(original_app):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._assets["/bot-call/long-sample.wav"] = _Static(fixture_path.read_bytes(), b"audio/wav")
        if args.baseline:
            for asset in ("call.js", "vad.js", "vad-worklet.js"):
                body = subprocess.check_output(
                    ["git", "show", "ec6c43c:bot_call/static/" + asset], cwd=smoke.ROOT
                )
                self._assets["/bot-call/" + asset] = _Static(
                    body, b"text/javascript; charset=utf-8"
                )


smoke.CallWebApp = FixtureApp
# The standard driver's Linux ELF, loopback and process cleanup checks remain.
smoke.DRIVER = smoke.DRIVER.replace("90000*repeats", "150000*repeats").replace("i<600", "i<1400")
# Same driver/ELF check/process-group cleanup, longer real-time test deadline.
_original_communicate = smoke.subprocess.Popen.communicate


def communicate(self, *a, **kw):
    if kw.get("timeout") is not None:
        kw["timeout"] = max(160, kw["timeout"])
    return _original_communicate(self, *a, **kw)


smoke.subprocess.Popen.communicate = communicate
smoke.BROWSER_JS = (
    r"""
import {CallPage} from '/bot-call/call.js';
import {LocalVAD,SileroSignalModel} from '/bot-call/vad.js';
const kind=__CASE__,baseline=__BASELINE__,single=__SINGLE__;
const wait=ms=>new Promise(r=>setTimeout(r,ms));
const check=(v,m)=>{if(!v)throw Error(m);};
const result={ok:false,kind,baseline,segmentMs:single?120000:30000,silenceMs:1400,peakRuns:0,segments:[],events:[],trace:[],seals:[],levels:[],discards:[],captures:[],rejections:[]};
let page,context,detector,zero,generator,source;
try {
  navigator.mediaDevices.getUserMedia=()=>{throw Error('direct_mic_forbidden');};
  generator=new AudioContext({sampleRate:48000});await generator.resume();
  const bus=generator.createGain();
  zero=generator.createConstantSource();zero.offset.value=0;zero.connect(bus);zero.start();
  source=generator.createBufferSource();source.buffer=await generator.decodeAudioData(await (await fetch('/bot-call/long-sample.wav')).arrayBuffer());source.connect(bus);
  const cold=['cold','transition','noise'].includes(kind);
  const envVAD={document,AbortController,AudioWorkletNode,setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window),
    get ort(){return window.ort;},fetch:async(url,init)=>{if(cold && String(url).endsWith('.onnx'))await wait(20000);return fetch(url,init);}};
  detector=new LocalVAD(envVAD);
  let running=0;const run=SileroSignalModel.prototype.run;
  SileroSignalModel.prototype.run=async function(frame){running++;result.peakRuns=Math.max(result.peakRuns,running);try{return await run.call(this,frame);}finally{running--;}};
  if(!cold) {
    const prepare=detector.prepare.bind(detector);
    detector.prepare=async(...a)=>{const ready=detector.ready(a[1]);const answer=await prepare(...a);if(answer&&!ready)await wait(150);return answer;};
    result.preArmWarmMs=150;
  }
  const phases=[];
  const env={storage:{getItem:()=>'',setItem(){}},location:{hash:'',pathname:'/bot-call/'},history,
    now:()=>performance.now(),Audio,URL,Blob,crypto,AbortController,MediaRecorder,AudioContext,
    mediaDevices:{getUserMedia:async()=>{const out=generator.createMediaStreamDestination();bus.connect(out);return out.stream;}},
    createVAD:()=>detector,setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window)};
  page=new CallPage(env,v=>{phases.push(v.ui.phase);if(v.recording)result.levels.push({at:Math.round(performance.now()),level:v.level});},
    {segmentMs:single?120000:30000,silenceMs:1400,vadRedemptionMs:1400});
  page.started=true;page.reference='synthetic-only';page.server={audio_available:true,speech_available:true,turns:[],audio:[],replies:[],series:[]};
  page.seriesCapability({seamless_series:{version:1,max_segments:10}});
  // 0.8.0 advertises only 1..6; keep that legitimate baseline capability.
  if(!page.seamlessMax)page.seriesCapability({seamless_series:{version:1,max_segments:6}});
  page.refresh=async()=>page.server;
  if(!cold){await page.modelPreparation;check(page.modelReady,'model');}
  const finishSegment=page.finishSegment.bind(page),observedCaptures=new Set();
  page.finishSegment=async(...a)=>{
    const r=page.recording,edge=r?{at:Math.round(performance.now()-result.start),reason:a[0]||'manual',lastEnergyAgo:Math.round(performance.now()-(r.lastEnergyVoice??r.start)),seriesVoiceAgo:page.series?Math.round(performance.now()-page.series.lastVoiceAt):null,candidate:r.vad?.candidate,vadValid:r.vad?.valid,energyValid:r.energyValid,modelVoiceAgo:r.vad?.lastCandidate>0?Math.round(performance.now()-r.start-(r.vad.lastCandidate-r.vad.recordStart)/r.vad.rate*1000):null}:null;
    const clip=await finishSegment(...a);
    if(r&&clip&&!observedCaptures.has(r.id)){observedCaptures.add(r.id);result.captures.push({...edge,durationMs:clip.durationMs,bytes:clip.blob?.size,codecSamples:r.recorder.samples??null,firstSample:r.recorder.firstSample??null,lastSample:r.recorder.lastSample??null,skip:r.recorder.stats?.skip??null,padding:r.recorder.stats?.padding??null,error:clip.error||false,rejection:page.clipRejection(clip)});}return clip;
  };
  const silentClip=page.silentClip.bind(page);
  page.silentClip=clip=>{const discarded=silentClip(clip);result.discards.push({at:Math.round(performance.now()-result.start),discarded,qualityValid:clip.quality?.valid,zero:clip.quality?.zero,frames:clip.quality?.frames,vadValid:clip.vad?.valid,candidate:clip.vad?.candidate,energyVoiced:clip.energyVoiced});return discarded;};
  const staged=new Map(),committed=new Set();let generations=0;
  page.api=async(route,body,headers={})=>{
    if(route==='metrics'){if(body?.values?.client_clip_reject)result.rejections.push({...body.values});return {recorded:true};}
    if(route==='audio-series-control'){result.seals.push({...body,at:performance.now()-result.start});generations++;return {recorded:true,series:{series_id:body.series_id}};}
    const id=headers['X-Bot-Utterance-ID'];check(id,'id');
    const bytes=new Uint8Array(await body.arrayBuffer()),old=staged.get(id)||new Uint8Array();
    if(route==='audio-chunk'){
      check(Number(headers['X-Bot-Chunk-Offset'])===old.length,'offset');
      const merged=new Uint8Array(old.length+bytes.length);merged.set(old);merged.set(bytes,old.length);staged.set(id,merged);
      return {chunk_received:true,utterance_id:id,next_seq:Number(headers['X-Bot-Chunk-Seq'])+1,next_offset:merged.length};
    }
    check(['audio','audio-commit','audio-series','audio-series-commit'].includes(route),'route');
    const clip=page.series?.queue[0] || page.clip,index=Number(headers['X-Bot-Series-Index'] || 1),final=headers['X-Bot-Series-Final']!=='0';
    check(clip.id===id && !committed.has(id),'identity');
    const offset=route.endsWith('commit')?Number(headers['X-Bot-Tail-Offset']):0;
    const full=new Uint8Array(offset+bytes.length);full.set(old.subarray(0,offset));full.set(bytes,offset);
    const sha=[...new Uint8Array(await crypto.subtle.digest('SHA-256',full))].map(v=>v.toString(16).padStart(2,'0')).join('');
    check(full.length===clip.blob.size && sha===clip.sha && full.length>0,'container');
    if(final)generations++;
    result.segments.push({index,final,bytes:full.length,route,at:Math.round(performance.now()-result.start),reason:clip.reason,metrics:{...clip.metrics}});
    committed.add(id);staged.delete(id);
    return {audio_received:true,utterance_id:id,...(clip.series?{series_id:clip.series.id,series_index:index,series_final:final}:{})};
  };
  const emit=detector.emit.bind(detector);
  detector.emit=(s,event)=>{if(event!=='candidate')result.events.push({event,id:s.id,at:Math.round(performance.now()-(result.start||performance.now())),valid:s.valid,diagnostic:s.diagnostic,last:s.lastCandidate,start:s.recordStart,rate:s.rate});emit(s,event);};
  await page.listen();context=page.audioContext;check(page.recording,'recording');result.start=performance.now();source.start();
  const length=__VOICE_SECONDS__;
  result.microphone='webaudio_synthetic_stream';result.fixtureSha256=__FIXTURE_SHA__;result.pauses=__PAUSES__;
  result.backgroundRms=kind==='noise'?.00424:0;
  result.voiceEndMs=length*1000;result.quietGain=cold?.12:.07;result.fixtureSeconds=length;result.early=false;
  let lastSecond=-1;
  const end=result.start+(cold?26000:length*1000+5000);
  while(performance.now()<end && !generations){
    if(page.series?.failed){result.captureFailed=page.series.failed;break;}
    if(kind==='quiet' && page.capturedSegments>=2){result.silenceDiscarded=true;break;}
    const elapsed=performance.now()-result.start,r=page.recording;
    if(r && Math.floor(elapsed/1000)!==lastSecond){lastSecond=Math.floor(elapsed/1000);result.trace.push({at:Math.round(elapsed),index:page.series?.count+1||1,level:r.level,energyVoiced:r.energyVoiced||false,energyValid:r.energyValid,qualityValid:r.quality.valid,voice:r.lastEnergyVoice?Math.round(elapsed-(performance.now()-r.lastEnergyVoice)):null,candidate:r.vad?.candidate,valid:r.vad?.valid,last:r.vad?.lastCandidate,start:r.vad?.recordStart});}
    if(kind==='manual' && elapsed>=101000 && !page.recording?.stopping)await page.submitSegment();
    if(cold && baseline && elapsed>=7000 && !generations){result.missedFirstPause=true;break;}
    await wait(50);
  }
  result.captureState={message:page.message,notice:page.notice,failed:page.series?.failed,active:!!detector.active};
  result.elapsedMs=Math.round(performance.now()-result.start);result.generations=generations;
  result.earlyEndpoint=result.captures.find(c=>['auto_energy','auto_silence'].includes(c.reason)&&c.at<result.voiceEndMs-200)||null;
  result.early=(generations>0 && result.elapsedMs<result.voiceEndMs-200)||!!result.earlyEndpoint;
  result.maxLevel=Math.max(0,...result.levels.map(v=>v.level));
  result.minLevel=Math.min(1,...result.levels.map(v=>v.level));delete result.levels;
  result.releasedRoot=page.series?.rootClip? !page.series.rootClip.blob:true;
  if(baseline && !single){result.reproduced=cold?!!result.missedFirstPause:result.early;check(result.reproduced,'baseline_not_reproduced');}
  else if(kind==='quiet'){check(generations===0 && result.segments.length===0 && result.silenceDiscarded,'quiet_uploaded');}
  else {
    check(generations===1,'one_generation');
    if(!baseline)check(result.segments.every(s=>s.metrics.client_capture_mode===__MODE__), 'unexpected_capture_fallback');check(!result.early || kind==='manual','early_endpoint');
    if(['long','manual','pauses'].includes(kind) && !single)check(result.segments.length>=4,'four_segments');
    if(kind!=='manual')check(result.elapsedMs>=result.voiceEndMs+1200 && result.elapsedMs<result.voiceEndMs+3000,'pause_deadline');
    if(!baseline && result.segments.at(-1)?.reason==='auto_silence') {
      const quiet=result.segments.at(-1).metrics.client_endpoint_quiet_ms;
      check(quiet>=1400 && quiet<2200,'endpoint_quiet_metric');result.endpointQuietMs=quiet;
    }
    if(kind==='manual')check(result.segments.at(-1)?.reason==='manual' && result.elapsedMs>=101000,'manual_not_observed');
    if(cold)check(result.maxLevel>.10 && result.minLevel<.02,'quiet_meter');
    if(kind==='transition')check(page.modelReady && page.round?.metrics.client_vad_first_fallback===1,'transition_lost');
    if(!cold && !single)check(result.releasedRoot,'retained_root_blob');
  }
  result.ok=true;
} catch(error){result.error=String(error.message).slice(0,120);}
finally {
  // Await the single real model load so the resource/one-inference checks remain meaningful in cold runs.
  if(page?.recording)await page.finishSegment('control').catch(()=>{});
  if(page?.modelPreparation)await page.modelPreparation;
  page?.releaseMic();page?.closePCM();await detector?.close().catch(()=>{});
  try{source?.stop();zero?.stop();await generator?.close();}catch(_){}
  result.contextClosed=context?.state==='closed';window.smokeResult=result;
}
""".replace("__CASE__", repr(args.case))
    .replace("__MODE__", "1" if args.capture_mode == "auto" else "0")
    .replace("__BASELINE__", str(args.baseline).lower())
    .replace("__SINGLE__", str(args.single).lower())
    .replace("__VOICE_SECONDS__", str(voice_seconds))
    .replace("__FIXTURE_SHA__", json.dumps(fixture_sha))
    .replace("__PAUSES__", json.dumps(pauses))
)


audio_evidence = os.environ.get("BOT_CALL_LONG_AUDIO_DIR")
if audio_evidence:
    directory = Path(audio_evidence)
    directory.mkdir(parents=True, exist_ok=True)
    smoke.BROWSER_JS = smoke.BROWSER_JS.replace(
        "const result={ok:false,", "const result={files:[],ok:false,"
    )
    smoke.BROWSER_JS = smoke.BROWSER_JS.replace(
        "    committed.add(id);staged.delete(id);",
        """    let binary='';for(let i=0;i<full.length;i+=8192)binary+=String.fromCharCode(...full.subarray(i,i+8192));
    result.files.push({name:'segment-'+index+'.webm',data:btoa(binary)});
    committed.add(id);staged.delete(id);""",
    )
    smoke.DRIVER = smoke.DRIVER.replace(
        "observed=result;const urls=",
        "if(result?.files){for(const f of result.files)fs.writeFileSync("
        + json.dumps(str(directory) + "/")
        + "+f.name,Buffer.from(f.data,'base64'));delete result.files;}"
        + "if(result)fs.writeFileSync("
        + json.dumps(str(directory / "browser-result.json"))
        + ",JSON.stringify(result));observed=result;const urls=",
    )

if __name__ == "__main__":
    try:
        smoke.main()
    finally:
        fixture_directory.cleanup()
