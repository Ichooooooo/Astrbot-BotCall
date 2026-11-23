"""Idle input recovery and ten synthetic rounds; one owned Linux browser, no providers."""

import json
import os
from pathlib import Path
import subprocess
import check_local_vad_browser as smoke

# Fault injection exists only in this loopback fixture's served worklet.
from bot_call.web import _Static

_original_app = smoke.CallWebApp


class GapInjectionApp(_original_app):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        path = "/bot-call/capture-worklet.js"
        asset = self._assets[path]
        body = asset.body.replace(
            b"if(m.type==='ack')",
            b"if(m.type==='test-gap')this.testGap=m.frames;\n      if(m.type==='ack')",
        )
        assert body != asset.body
        # Apply only on a continuous render boundary: an incidental native clock
        # jump stays observable, but cannot compound the requested test gap.
        marker = b"if(this.expected!==undefined&&this.expected!==currentFrame)"
        assert marker in body
        body = body.replace(
            marker,
            b"if(this.testGap!==undefined&&this.expected===currentFrame){"
            b"const frames=this.testGap;delete this.testGap;this.expected-=frames;"
            b"this.port.postMessage({type:'test-gap-applied',frames});}" + marker,
        )
        self._assets[path] = _Static(body, asset.content_type)


smoke.CallWebApp = GapInjectionApp

smoke.DRIVER = (
    smoke.DRIVER.replace("result.peakRuns!==1", "false")
    .replace("wasmRequests!==1||modelRequests!==1", "wasmRequests!==1||modelRequests!==1")
    .replace("i<600", "i<1200")
)
_original_popen = subprocess.Popen


def popen(*args, **kwargs):
    process = _original_popen(*args, **kwargs)
    communicate = process.communicate

    def longer(*a, **kw):
        if kw.get("timeout"):
            kw["timeout"] = max(160, kw["timeout"])
        return communicate(*a, **kw)

    process.communicate = longer
    return process


subprocess.Popen = popen
smoke.BROWSER_JS = r"""
import {CallPage,CapabilityProbe} from '/bot-call/call.js';
import {LocalVAD} from '/bot-call/vad.js';
const wait=ms=>new Promise(r=>setTimeout(r,ms));
const check=(v,s)=>{if(!v)throw Error(s);};
async function until(fn,ms=8000){const end=performance.now()+ms;while(!fn()&&performance.now()<end)await wait(10);check(fn(),'deadline');}
const result={ok:false,cases:[],rounds:[],reports:[]};let p,c,osc,gain,destination,detector;
async function setup(realVAD=false) {
  c=new AudioContext({sampleRate:48000});await c.resume();
  destination=c.createMediaStreamDestination();gain=c.createGain();gain.gain.value=0;
  osc=c.createOscillator();osc.frequency.value=220;osc.connect(gain);gain.connect(destination);osc.start();
  const env={probeWindow:window,storage:{getItem:()=>'',setItem(){}},location:{hash:'',pathname:'/bot-call/'},history,
    now:()=>performance.now(),Audio,URL,Blob,crypto,AbortController,MediaRecorder,AudioContext,
    mediaDevices:{getUserMedia:async()=>destination.stream.clone()},setTimeout:setTimeout.bind(window),
    clearTimeout:clearTimeout.bind(window),isHidden:()=>false};
  if(realVAD){detector=new LocalVAD(window);await detector.prepareModel();env.createVAD=()=>detector;}
  p=new CallPage(env);p.started=true;p.reference='synthetic';p.server={audio_available:true,speech_available:true};
  p.audioContext=c;p.pcmContext=c;p.config.silenceMs=30000;
  p.api=async(route,body)=>{check(route==='metrics','audio/provider request');result.reports.push(body);return {recorded:true};};
  p.refresh=async()=>p.server;p.submitSegment=reason=>p.finishSegment(reason);
}
async function speak() {
  await p.listen();const r=p.recording;check(r?.metrics.client_capture_mode===1,'next mode != 1');
  await wait(600);gain.gain.value=.2;await wait(300);gain.gain.value=0;
  const clip=await p.finishSegment('manual');
  if(!clip||clip.error||!r.recorder.stats)result.failed={clip:{error:clip?.error,metrics:clip?.metrics},r:{error:r.error,failed:r.recorder.failed,metrics:r.metrics,stats:r.recorder.stats,samples:r.recorder.samples,reason:r.reason},capture:{failed:p.capture?.failed,closed:p.capture?.closed},pageFailure:p.captureFailure};
  check(clip&&!clip.error&&r.recorder.stats,'recording failed');
  check(clip.metrics.client_capture_prefix_ms===500,'prefix != 500');
  p.clip=null;await p.echo?.ready;
  return {clip,r};
}
async function cleanup() {
  if(p?.recording)await p.finishSegment('control');
  p?.clip?.uploader?.abandon();if(p)p.clip=null;p?.releaseMic();
  await detector?.closing;detector=null;
  try{osc?.stop();}catch(_){}
  for(const t of destination?.stream.getTracks()||[])t.stop();
  await c?.close();p=null;
}
function injectGap(frames){
 const port=p.capture.node.port;
 port.addEventListener('message',function applied({data:m}){
  if(m.type!=='test-gap-applied'||m.frames!==frames)return;
  port.removeEventListener('message',applied);
  result.gapInjections ||= [];result.gapInjections.push({stage:result.stage,frames});
 });
 port.postMessage({type:'test-gap',frames});
}
function fault(reason,credits=0,skip=0){p.capture.node.port.dispatchEvent(new MessageEvent('message',{data:{type:'fault',reason,credits,skip}}));}
try {
  navigator.mediaDevices.getUserMedia=()=>{throw Error('real_mic_forbidden');};
  result.stage='startup-timeout-recovery';await setup();
  let delayStartup=true;
  class ColdWorker extends Worker {
    postMessage(m,...args){
      if(delayStartup&&m.type==='check'){this.delay=setTimeout(()=>super.postMessage(m,...args),1800);return;}
      super.postMessage(m,...args);
    }
    terminate(){clearTimeout(this.delay);super.terminate();}
  }
  p.e.probeWindow={Worker:ColdWorker,AudioWorkletNode,setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window)};
  await p.listen();
  check(p.recording.metrics.client_capture_mode===0&&p.recording.metrics.client_capture_fallback===8,'cold startup did not fall back');
  check(p.recording.metrics.client_capture_timeout_stage===1&&p.recording.metrics.client_capture_startup_state===1,'pending startup metrics');
  check(!p.bargeEnabled(),'legacy barge unexpectedly enabled');
  await wait(300);await p.finishSegment('manual');p.clip=null;
  delayStartup=false;
  const recovered=await speak();
  check(recovered.clip.metrics.client_capture_startup_state===2&&recovered.clip.metrics.client_capture_startup_retries===1,'missing recovered metrics');
  check(Number.isFinite(recovered.clip.metrics.client_capture_startup_recovered_ms),'missing recovery time');
  p.config.bargeIn='auto';check(p.bargeEnabled(),'barge not restored after capture');
  result.cases.push({kind:'startup-recovered',metrics:recovered.clip.metrics});await cleanup();

  for(const [kind,reason] of [['gap',1],['credits',2],['suspended',7],['mute',11],['ended',12]]) {
    result.stage=kind;await setup();await speak();const old=p.capture;
    p.speech={playing:true};p.capability={running:true}; // fixed concurrent-state diagnostic flags
    if(kind==='gap')fault(1,4,55);
    if(kind==='credits')fault(2,64);
    if(kind==='suspended'){await c.suspend();await until(()=>old.closed);await c.resume();}
    if(kind==='mute'){const t=p.stream.getTracks()[0];t.dispatchEvent(new Event('mute'));t.dispatchEvent(new Event('unmute'));}
    if(kind==='ended'){const t=p.stream.getTracks()[0];t.stop();t.dispatchEvent(new Event('ended'));}
    check(old.closed&&!p.captureDisabled&&!p.recording,'idle failure disabled call');
    check(p.captureInputDiagnostics.client_capture_input_reason===reason,'wrong idle subreason');
    p.speech=null;p.capability=null;const {clip}=await speak();
    check(p.capture!==old&&p.captureRebuilds===1,'pipeline not rebuilt');
    const m=clip.metrics;check(m.client_capture_input_reason===reason&&m.client_capture_input_active===0&&m.client_capture_input_speaking===1&&m.client_capture_input_probe===1,'diagnostic context');
    check(m.client_capture_input_echo===1&&m.client_capture_input_idle_ms<=60000&&m.client_capture_input_recovery===2,'diagnostic bounds/recovery');
    if(kind==='gap')check(m.client_capture_input_skip_ms===55,'skip diagnostic');
    if(kind==='credits')check(m.client_capture_input_credits===64,'credit diagnostic');
    result.cases.push({kind,metrics:m});await cleanup();
  }
  result.stage='prestart-rebuild';await setup();
  let injected=false;
  p.e.createVAD=()=>({ready:()=>true,begin:async()=>{
    if(!injected){injected=true;fault(1,0,55);}return null;
  }});
  const prestart=await speak();
  check(injected&&p.captureRebuilds===1&&prestart.clip.metrics.client_capture_input_reason===1&&prestart.clip.metrics.client_capture_input_active===0,'prestart fault lost capture');
  result.cases.push({kind:'prestart',metrics:prestart.clip.metrics});await cleanup();
  result.stage='queued-prestart';await setup();let queued=false;
  p.e.createVAD=()=>({ready:()=>true,begin:async()=>{
    if(!queued){queued=true;const owner=p.capture;setTimeout(()=>owner.input(1,{credits:0,skip:55}),0);}return null;
  }});
  const queuedResult=await speak();
  check(queued&&p.captureRebuilds===1&&queuedResult.clip.metrics.client_capture_input_active===0,'queued idle fault classified active');
  result.cases.push({kind:'queued-prestart',metrics:queuedResult.clip.metrics});await cleanup();
  result.stage='preserve-disabled';await setup();await speak();
  p.captureDisabled=true;p.captureFailure=4;fault(1,0,55);
  check(p.captureDisabled&&!p.captureNeedsRebuild&&p.captureFailure===4,'idle fault revived disabled encoder');
  await p.listen();check(p.recording.metrics.client_capture_mode===0&&p.recording.metrics.client_capture_input_recovery===4,'prior fallback lost');
  result.cases.push({kind:'prior-disabled',metrics:{...p.recording.metrics}});await cleanup();
  result.stage='retry-limit';await setup();await speak();
  for(let n=1;n<=4;n++) {
    fault(1,0,55);
    if(n<=3) {
      check(!p.captureDisabled,'early fallback');await speak();check(p.captureRebuilds===n,'rebuild counter');
    }else {
      check(p.captureDisabled,'missing retry ceiling');await p.listen();
      check(p.recording.metrics.client_capture_mode===0&&p.recording.metrics.client_capture_input_recovery===3&&p.recording.metrics.client_capture_rebuilds===3,'limit diagnostic');
      result.cases.push({kind:'limit',metrics:{...p.recording.metrics}});
    }
  }
  await cleanup();
  for(const [kind,reason] of [['ended-active',12],['not-live-active',13],['gap-active',1],['back-active-large',1],['credits-active',2]]) {
    result.stage=kind;await setup();await p.listen();gain.gain.value=.2;
    await until(()=>p.recording.recorder.samples>4800);const r=p.recording;
    if(kind.startsWith('ended')){const t=p.stream.getTracks()[0];t.stop();t.dispatchEvent(new Event('ended'));}
    else if(kind.startsWith('not-live')){p.stream.getTracks()[0].stop();await until(()=>r.stopping);}
    else if(reason===1){injectGap(kind==='back-active-large'?-1200:2640);await until(()=>r.stopping);}
    else fault(reason,64,0);
    check(r.error&&r.stopping&&p.captureDisabled,'active loss did not stop');
    const clip=await r.done;check(clip.error&&clip.metrics.client_capture_input_reason===reason&&clip.metrics.client_capture_input_active===1,'active diagnostics');
    if(kind==='back-active-large')check(clip.metrics.client_capture_clock_back_count===1&&clip.metrics.client_capture_clock_back_max_ms===25,'large backward diagnostic');
    if(kind==='gap-active')check(clip.metrics.client_capture_gap_max_ms===55&&clip.metrics.client_capture_gap_ms>=55,'large gap diagnostic');
    result.cases.push({kind,metrics:clip.metrics});await cleanup();
  }
  result.stage='small-idle-gap';await setup();await speak();const idleOwner=p.capture;
  await wait(600);injectGap(960);await wait(80);
  await p.listen();const idleR=p.recording;gain.gain.value=.2;await wait(300);gain.gain.value=0;
  const idleClip=await p.finishSegment('manual');
  check(p.capture===idleOwner&&!p.captureRebuilds&&!idleClip.error&&idleClip.metrics.client_capture_mode===1,'small idle gap rebuilt');
  check(idleClip.metrics.client_capture_gap_count>=1&&idleClip.metrics.client_capture_gap_ms>=20&&idleClip.metrics.client_capture_gap_max_ms===20,'idle gap diagnostics');
  p.clip=null;result.cases.push({kind:'small-idle-gap',metrics:idleClip.metrics});await cleanup();
  result.stage='small-active-gap';await setup();await p.listen();gain.gain.value=.2;
  const padded=p.recording;await until(()=>padded.recorder.samples>4800);
  const before=padded.metrics.client_capture_gap_count;
  injectGap(128);await until(()=>padded.metrics.client_capture_gap_count>before);
  injectGap(1008);await until(()=>padded.metrics.client_capture_gap_ms>=24);
  check(!padded.error&&padded.metrics.client_capture_gap_max_ms>=21&&padded.metrics.client_capture_gap_max_ms<=50,'21ms forward gap rejected');
  check(result.gapInjections.some(x=>x.stage==='small-active-gap'&&x.frames===1008),'21ms injection missing');
  injectGap(2160);await until(()=>padded.metrics.client_capture_gap_ms>=69);
  check(p.recording===padded&&!padded.error&&padded.recorder.state==='recording','small gap lost segment');
  gain.gain.value=0;const paddedClip=await p.finishSegment('manual');
  check(!paddedClip.error&&paddedClip.metrics.client_capture_mode===1&&paddedClip.metrics.client_capture_gap_max_ms>=45&&paddedClip.metrics.client_capture_gap_max_ms<=50,'padded recording rejected');
  check(result.gapInjections.some(x=>x.stage==='small-active-gap'&&x.frames===2160),'45ms injection missing');
  const bytes=new Uint8Array(await paddedClip.blob.arrayBuffer());let b64='';
  for(let i=0;i<bytes.length;i+=8192)b64+=String.fromCharCode(...bytes.subarray(i,i+8192));
  result.gapClip={data:btoa(b64),samples:padded.recorder.samples};
  result.cases.push({kind:'small-active-gap',metrics:paddedClip.metrics});p.clip=null;await cleanup();
  result.stage='small-idle-back';await setup();await speak();const backIdleOwner=p.capture;
  injectGap(-144);await wait(80);
  const idleBackResult=await speak();
  check(p.capture===backIdleOwner&&!p.captureRebuilds&&!idleBackResult.clip.error&&idleBackResult.clip.metrics.client_capture_mode===1,'idle reverse clock rebuilt');
  check(idleBackResult.clip.metrics.client_capture_clock_back_count>=1&&idleBackResult.clip.metrics.client_capture_clock_back_max_ms===3,'idle reverse clock diagnostic');
  result.cases.push({kind:'small-idle-back',metrics:idleBackResult.clip.metrics});p.clip=null;await cleanup();
  result.stage='small-active-back';await setup();await p.listen();gain.gain.value=.2;
  const backR=p.recording;await until(()=>backR.recorder.samples>4800);
  const beforeBack=backR.metrics.client_capture_clock_back_count;
  injectGap(-144);await until(()=>backR.metrics.client_capture_clock_back_count>beforeBack);
  check(p.recording===backR&&!backR.error&&backR.recorder.state==='recording','small backward lost segment');
  const backClip=await p.finishSegment('manual');
  check(!backClip.error&&backClip.metrics.client_capture_mode===1&&backClip.metrics.client_capture_clock_back_max_ms===3,'small backward rejected');
  result.cases.push({kind:'small-active-back',metrics:backClip.metrics});p.clip=null;await cleanup();
  result.stage='cumulative-gap-limit';await setup();await p.listen();gain.gain.value=.2;
  const accumulated=p.recording;await until(()=>accumulated.recorder.samples>4800);
  const initialMs=accumulated.metrics.client_capture_gap_ms;let injections=0;
  while(injections<11&&!accumulated.stopping) {
    injectGap(960);injections++;
    await until(()=>accumulated.stopping||accumulated.metrics.client_capture_gap_ms>=initialMs+injections*20);
  }
  check(accumulated.error&&accumulated.stopping,'cumulative >200ms did not fail');
  const accumulatedClip=await accumulated.done;
  check(accumulatedClip.error&&accumulatedClip.metrics.client_capture_input_reason===1&&accumulatedClip.metrics.client_capture_gap_ms>200,'cumulative gap diagnostic');
  result.cases.push({kind:'cumulative-gap-limit',injections,metrics:accumulatedClip.metrics});await cleanup();
  result.stage='arm-timeout';await setup();
  p.e.probeWindow={Worker,AudioWorkletNode:class extends AudioWorkletNode {
    constructor(...args){super(...args);const post=this.port.postMessage.bind(this.port);this.port.postMessage=m=>{if(m.type!=='record')post(m);};}
  },setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window)};
  await p.listen();check(p.recording.metrics.client_capture_mode===0&&p.captureFailure===8,'arm timeout reason');
  result.cases.push({kind:'arm-timeout',metrics:{...p.recording.metrics}});await cleanup();
  result.stage='ten-rounds';await setup(true);
  let startupDelay=0;
  p.e.probeWindow={Worker:class extends Worker {
    postMessage(m,...args){if(m.type==='start')this.delay=setTimeout(()=>super.postMessage(m,...args),startupDelay);else super.postMessage(m,...args);}
    terminate(){clearTimeout(this.delay);super.terminate();}
  },AudioWorkletNode,OfflineAudioContext,setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window)};
  p.e.location.origin=location.origin;
  p.e.WebSocket=class {
    constructor(){this.readyState=0;queueMicrotask(()=>{this.readyState=1;this.onopen?.();this.onmessage?.({data:JSON.stringify({state:'sync'})});});}
    send(){}
    close(){this.readyState=3;this.onclose?.({code:1000});}
  };
  p.ensurePush();await until(()=>p.pushReady);
  // Throw if the synthetic diagnostic touches the live decoder at any point.
  c.decodeAudioData=()=>{throw Error('shared decode used');};
  for(let n=1;n<=10;n++) {
    startupDelay=({3:300,5:1000,8:2500})[n]||0;
    if(n===6){p.socket.onclose({code:1006});await until(()=>p.pushReady&&p.wsReconnects===1);}
    const {clip}=await speak();
    const e=p.echo;await e.ready;await wait(100);
    const speech={playing:true,seq:1,round:{id:crypto.randomUUID()}};p.speech=speech;
    e.path(speech,'pcm',c.currentTime);
    const source=c.createBufferSource(),buffer=c.createBuffer(1,48000,48000);
    const a=buffer.getChannelData(0);for(let i=0;i<a.length;i++)a[i]=.02*Math.sin(i*2*Math.PI*220/48000);
    source.buffer=buffer;source.connect(c.destination);source.start();e.scheduled(a,c.currentTime,48000);
    if(n===2) {
      // Hold an actual probe result during a real 1.4s main-thread stall.
      const probe=new CapabilityProbe(window,()=>{},true);p.capability=probe;probe.run();
      await wait(10);const start=performance.now();while(performance.now()-start<1400){}
      await probe.task;check(probe.values.client_cap_encode===1,'offline probe decode');
      result.stall={ms:Math.round(performance.now()-start),values:probe.values};
    } else await wait(650);
    e.end(speech);await e.stop();await until(()=>result.reports.filter(r=>r.generation_id?.startsWith('page:echo:')).length>=n);p.echo=null;p.speech=null;p.capability=null;
    check(!p.captureDisabled,'standby stall disabled capture');
    result.rounds.push({round:n,metrics:clip.metrics,echo:e.latest,stats:clip.durationMs,pendingInput:p.captureInputDiagnostics});
  }
  result.longTransport={disconnects:p.wsDisconnects,reconnects:p.wsReconnects};p.stopPush();
  check(result.longTransport.disconnects===1&&result.longTransport.reconnects===1,'long call disconnect/reconnect');
  check(result.rounds.length===10&&result.rounds.every(r=>r.metrics.client_capture_mode===1),'ten round modes');
  check(result.reports.filter(r=>r.generation_id?.startsWith('page:echo:')).length>=10,'echo observations missing');
  result.ok=true;
}catch(e){result.error=e.message;result.stack=e.stack;result.failureContext={input:p?.captureInputDiagnostics,
 captureFailure:p?.captureFailure,disabled:p?.captureDisabled,rebuilds:p?.captureRebuilds,clip:p?.clip?.metrics,recording:p?.recording?.metrics};}
finally{try{await cleanup();}catch(e){result.cleanup=e.message;}result.contextClosed=c?.state==='closed';window.smokeResult=result;}
"""
if __name__ == "__main__":
    directory = Path(os.environ.get("BOT_CALL_INPUT_EVIDENCE", "/tmp/bot-call-input-check"))
    directory.mkdir(parents=True, exist_ok=True)
    smoke.DRIVER = smoke.DRIVER.replace(
        "observed=result;const urls=",
        "if(result)fs.writeFileSync("
        + json.dumps(str(directory / "browser-result.json"))
        + ",JSON.stringify(result,null,2));observed=result;const urls=",
    )
    smoke.main()
    # Actual Opus/WebM round trip must retain the padded sample timeline.
    import base64

    result = json.loads((directory / "browser-result.json").read_text())
    clip = result["gapClip"]
    path = directory / "padded.webm"
    path.write_bytes(base64.b64decode(clip["data"]))
    pcm = subprocess.run(
        ["ffmpeg", "-v", "error", "-i", str(path), "-ar", "48000", "-ac", "1", "-f", "f32le", "-"],
        capture_output=True,
        check=True,
    ).stdout
    assert len(pcm) // 4 == clip["samples"], (len(pcm) // 4, clip["samples"])
    print(json.dumps({"padded_decode_samples": len(pcm) // 4, "exact_length": True}))
