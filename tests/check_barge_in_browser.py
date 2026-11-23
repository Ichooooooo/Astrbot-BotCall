"""P2 original page/audio/VAD/encoder, synthetic network, no real mic/Providers."""

import sys
import json
import check_local_vad_browser as smoke
from bot_call.barge_in import BargeStore, CandidateContext, decide_candidate
from bot_call.web import CallWebApp, _Static
from pathlib import Path
import subprocess


class BargeApp(CallWebApp):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        data = subprocess.run(
            [
                "ffmpeg",
                "-nostdin",
                "-v",
                "error",
                "-i",
                str(Path(__file__).resolve().parents[1] / "bot_call/static/fixed-sample.wav"),
                "-ar",
                "24000",
                "-ac",
                "1",
                "-b:a",
                "32k",
                "-f",
                "mp3",
                "pipe:1",
            ],
            check=True,
            capture_output=True,
        ).stdout
        decisions = {}
        for name, text in (
            ("middle", "嗯"),
            ("question", "对"),
            ("boundary", "对"),
            ("content", "嗯，但是我想问新问题"),
        ):
            parts = [
                dict(seq=0, body="", playback_state="unknown"),
                dict(
                    seq=1,
                    body="你觉得对吗？" if name == "question" else "继续介绍。",
                    playback_state="started",
                ),
            ]
            if name != "boundary":
                parts.append(dict(seq=2, body="还有下一句。", playback_state="unknown"))
            store = BargeStore()
            store._context_parts = lambda *a: parts
            kind = store._candidate_context(
                "fixture",
                dict(
                    seq=1,
                    generation_id="reply:fixture",
                    remaining_ms=500 if name == "boundary" else None,
                ),
            )
            choice = decide_candidate(text, CandidateContext(kind))
            decisions[name] = dict(kind=kind, decision=choice.action, reason=choice.reason)
        self._assets["/bot-call/barge-decisions.json"] = _Static(
            json.dumps(decisions).encode(), b"application/json"
        )
        # Scalar render-clock trace only in this loopback fixture. Product asset
        # remains unchanged; no audio samples or user data in the trace.
        path = "/bot-call/capture-worklet.js"
        asset = self._assets[path]
        body = asset.body.replace(
            b"const skip=currentFrame-this.expected;",
            b"const skip=currentFrame-this.expected;"
            b"this.port.postMessage({type:'test-clock',at:currentFrame/sampleRate,"
            b"skipMs:skip/sampleRate*1000,recording:!!this.recording});",
        )
        assert body != asset.body
        self._assets[path] = _Static(body, asset.content_type)
        # One-shot reverse clock on the real analysis processor, at a full
        # frame boundary. No flag or altered source is shipped to clients.
        path = "/bot-call/vad-worklet.js"
        asset = self._assets[path]
        body = asset.body.replace(b"currentFrame", b"testFrame")
        body = body.replace(
            b'if (m.type === "shutdown")',
            b'if(m.type==="test-back"){this.testBack=m.samples;return;}if (m.type === "shutdown")',
        ).replace(
            b"process(inputs,outputs) {",
            b"process(inputs,outputs) {let testFrame=currentFrame;"
            b"if(this.testBack&&this.active?.started&&this.active.pos===0){"
            b"testFrame-=this.testBack;this.testBack=0;"
            b"this.port.postMessage({type:'test-back-applied',id:this.active.id});}",
        )
        self._assets[path] = _Static(body, asset.content_type)
        self._assets["/bot-call/barge-test.mp3"] = _Static(data, b"audio/mpeg")
        self._assets["/bot-call/barge-meter.js"] = _Static(
            b"""
class Meter extends AudioWorkletProcessor {
 constructor(){super();this.id=null;this.heard=false;this.zero=null;this.port.onmessage=e=>{
  this.id=e.data;this.heard=false;this.zero=null;
 };}
 process(inputs){const a=inputs[0]?.[0];if(this.id!==null&&a){
  for(let i=0;i<a.length;i++){
   if(a[i]!==0){this.heard=true;this.zero=null;}
   else if(this.heard){this.zero??=currentFrame+i;
    if(currentFrame+i-this.zero>=512){this.port.postMessage({id:this.id,at:this.zero/sampleRate});this.id=null;break;}
   }
  }
 }return true;}
}
registerProcessor('barge-meter',Meter);
""",
            b"text/javascript",
        )


smoke.CallWebApp = BargeApp
# Each voiced trial can now wait 1 s longer at a configured 1800 ms endpoint.
# Budget for the 35 stop-latency trials, the functional cases and browser cleanup.
smoke.PROCESS_TIMEOUT = 420
smoke.DRIVER = smoke.DRIVER.replace("result.peakRuns!==1||", "")

smoke.DRIVER = smoke.DRIVER.replace("90000*repeats", "410000*repeats").replace("i<600", "i<4000")
smoke.DRIVER = smoke.DRIVER.replace(
    "const external=urls.filter(u=>!u.startsWith(origin+'/'));",
    "const external=urls.filter(u=>!u.startsWith(origin+'/')&&!u.startsWith('blob:'+origin+'/'));",
)
smoke.BROWSER_JS = r"""
import {CallPage} from '/bot-call/call.js';
import {LocalVAD} from '/bot-call/vad.js';
import {EchoProbe} from '/bot-call/echo.js';
import {BARGE} from '/bot-call/barge.js';
const wait=ms=>new Promise(r=>setTimeout(r,ms)),check=(v,m)=>{if(!v)throw Error(m);};
const result={ok:false,stage:'prepare',latencies:[],estimates:[],misses:0,missingGraph:0,captureRejects:[],clockTraces:[],callRestarts:0,cases:[],echo:[]};
let page,context,bus,destination,zero,feedTimer,activeVoice,meter,meterMute,tone;const graphStops=new Map();let onsetAudio;
let clockTrial;const observedInputs=new WeakSet();
function trace(kind,node,extra={}) {
 if(!clockTrial||clockTrial.events.length>=512)return;
 clockTrial.events.push({kind,node:node?.constructor?.name,at:context.currentTime,...extra});
}
for(const key of ['connect','disconnect']) {
 const original=AudioNode.prototype[key];AudioNode.prototype[key]=function(...args){
  trace(key,this);return original.apply(this,args);
 };
}
const nativeStop=AudioBufferSourceNode.prototype.stop;
AudioBufferSourceNode.prototype.stop=function(...args){trace('source.stop',this,{scheduled:args[0]});return nativeStop.apply(this,args);};
const beginBarge=CallPage.prototype.beginBarge;
CallPage.prototype.beginBarge=function(...args){
 if(clockTrial)clockTrial.pauseAt=context.currentTime;
 trace('soft-pause');return beginBarge.apply(this,args);
};
const outputGain=CallPage.prototype.outputGain;
CallPage.prototype.outputGain=function(value,...args){trace('output-gain',null,{value});return outputGain.call(this,value,...args);};
function observeInput(){
 const port=page.capture?.node.port;if(!port||observedInputs.has(port))return;
 observedInputs.add(port);port.addEventListener('message',({data:m})=>{
  if(m.type==='test-clock'&&clockTrial&&clockTrial.gaps.length<128)
   clockTrial.gaps.push({at:m.at,skipMs:m.skipMs,recording:m.recording});
 });
}
function endClockTrial(){
 if(!clockTrial)return;
 const t=clockTrial;clockTrial=null;
 result.clockTraces.push({attempt:t.attempt,pauseAt:t.pauseAt,eventsTruncated:t.events.length===512,
  gaps:t.gaps.map(g=>({...g,afterPauseMs:t.pauseAt===undefined?null:Math.round((g.at-t.pauseAt)*1000),
   nearby:t.events.filter(e=>Math.abs(e.at-g.at)<=.1).map(e=>({...e,relativeMs:Math.round((e.at-g.at)*1000)}))}))});
}

async function until(fn,ms=5500){const end=performance.now()+ms;while(!fn()&&performance.now()<end)await wait(10);check(fn(),'deadline:'+result.stage);}
try{
 context=new AudioContext({sampleRate:48000});await context.resume();
 await context.audioWorklet.addModule('/bot-call/barge-meter.js');
 meter=new AudioWorkletNode(context,'barge-meter');meterMute=context.createGain();meterMute.gain.value=0;
 meter.connect(meterMute);meterMute.connect(context.destination);
 meter.port.onmessage=({data})=>graphStops.set(data.id,data.at);
 destination=new MediaStreamAudioDestinationNode(context,{channelCount:1});
 bus=context.createGain();bus.connect(destination);zero=new ConstantSourceNode(context,{offset:0});zero.connect(bus);zero.start();
 const env={probeWindow:window,storage:{getItem:()=>'',setItem(){}},location:{hash:'',pathname:'/bot-call/'},history,
  now:()=>performance.now(),fetch:fetch.bind(window),Audio,URL,Blob,crypto,AbortController,atob:atob.bind(window),
  AudioContext:function(){return context;},MediaRecorder,mediaDevices:{getUserMedia:async()=>destination.stream.clone(),getSupportedConstraints:()=>({echoCancellation:true})},
  createVAD:()=>new LocalVAD(window),setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window)};
 page=new CallPage(env,()=>{},{bargeIn:'auto',captureMode:'auto',silenceMs:1800,vadRedemptionMs:1800});
 page.started=true;page.reference='synthetic';page.capability=null;page.ensurePush=()=>{};page.schedulePoll=()=>{};page.scheduleHeartbeat=()=>{};
 let mode='resume',rows=[],traffic=[],stt=0,networkDelay=0,uploads=[],p3Text='嗯',p3Scenario='middle',dispatches=0;
 const decisionFixtures=await (await fetch('/bot-call/barge-decisions.json')).json();
 const messages=[];
 const show=page.show;page.show=function(...args){messages.push(args.join(' '));return show.apply(this,args);};
 const state={audio_available:true,speech_available:true,audio:[],replies:[],segments:[],turns:[],barge_in:rows};
 page.server=state;
 page.api=async(route,body)=>{
  traffic.push({route,body:route==='metrics'?body:undefined});
  if(route==='metrics')return {recorded:true};
  if(['segment-playback','playback'].includes(route))return {recorded:true,generation_id:body.generation_id,seq:body.seq,revision:body.revision,state:body.state};
  if(route==='state')return {...state,barge_in:rows.slice()};
  if(route==='audio-series-control'){
   const row=rows.find(r=>r.utterance_id===body.series_id);
   if(body.action==='seal'&&mode!=='pending'){row.decision=mode;row.dispatch='started';result.seriesSeals=(result.seriesSeals||0)+1;}
   return {recorded:true,series:{series_id:body.series_id}};
  }
  if(route==='barge-in'){
   let row=rows.find(r=>r.utterance_id===body.utterance_id);
   if(!row){row={...body,decision:'pending',dispatch:'pending'};rows.push(row);
    if(mode==='p3'){
     row.parts=structuredClone(state.segments.filter(p=>p.generation_id===body.generation_id).map(p=>({...p,body:p.text||'继续说明。'})));
     row.choice=decisionFixtures[p3Scenario];
    }
   }
   if(body.action==='silent')row.decision='resume';
   if(body.action==='resumed')row.closed_reason='resumed';
   if(body.action==='long_speech')row.closed_reason='long_speech';
   if(['timeout','cancel','refresh'].includes(body.action)){row.closed_reason=body.action;if(body.action!=='timeout')row.decision='cancelled';}
   return {...row};
  }
  if(['audio','audio-commit','audio-series','audio-series-commit'].includes(route))throw Error('unwrapped upload');
  return {};
 };
 // Keep original commit/digest/ACK/state code. Only the network peer is replaced.
 const api=page.api;
 page.api=async(route,body,headers={},...rest)=>{
  if(['audio','audio-commit','audio-series','audio-series-commit'].includes(route)){
   const id=headers['X-Bot-Utterance-ID'],root=headers['X-Bot-Series-ID']||id;
   const series=!!headers['X-Bot-Series-ID'],final=!series||headers['X-Bot-Series-Final']==='1',index=Number(headers['X-Bot-Series-Index']||1);
   let row=rows.find(r=>r.utterance_id===root);
   const bytes=await body.arrayBuffer();stt++;
   uploads.push({id,bytes:bytes.slice(0),metrics:{...page.clip?.metrics}});
   if(networkDelay)await wait(networkDelay);
   state.audio.push({utterance_id:id,sha256:[...new Uint8Array(await crypto.subtle.digest('SHA-256',bytes))].map(x=>x.toString(16).padStart(2,'0')).join(''),byte_count:body.size,mime:body.type,state:'succeeded'});
   if(row&&mode==='interrupt'&&final){
     page.acceptPCM({generation_id:'reply:'+root,seq:1,index:0,rate:24000,format:'s16le',data:btoa(String.fromCharCode(...new Uint8Array(1920)))});
     check(page.barge.newFrames?.length===1&&!page.speech,'new audio escaped confirmation');
   }
   if(row&&mode==='p3'&&final){
    const choice=row.choice;
    row.decision=choice.decision;row.reason=choice.reason;row.dispatch=choice.decision==='interrupt'?'started':'pending';
    state.turns.push({role:'user',material_id:root,source:choice.reason==='backchannel'?'backchannel':'audio_transcription',body:p3Text});
    if(choice.decision==='interrupt')dispatches++;
   }else if(row&&mode!=='pending'&&final){row.decision=mode==='speech_only'?'interrupt':mode;row.dispatch=mode==='speech_only'?'speech_only':'started';}
   if(series){result.seriesSegments ||= [];result.seriesSegments.push({id,root,index,final});}
   return {audio_received:true,utterance_id:id,...(series?{series_id:root,series_index:index,series_final:final}:{})};
  }
  return api(route,body,headers,...rest);
 };
 page.config.chunkUpload=false;
 await until(()=>page.modelReady,20000);
 await page.listen();await wait(550);
 const warm=await page.finishSegment('fixture-warmup');warm.uploader?.abandon();page.clip=null;
 await page.echo?.ready;
 const speechBuffer=await context.decodeAudioData(await (await fetch('/bot-call/fixed-sample.wav')).arrayBuffer());
 // Same licensed speech excerpt as the existing gate fixture, 640 ms including
 // real consonant/vowel transitions. Ground truth is its known scheduled onset.
 const voice=context.createBuffer(1,30720,48000);
 voice.copyToChannel(speechBuffer.getChannelData(0).subarray(16000,46720),0);
 const shortVoice=context.createBuffer(1,voice.length*2,48000);
 for(let k=0;k<2;k++)shortVoice.copyToChannel(voice.getChannelData(0),0,k*voice.length);
 const longVoice=context.createBuffer(1,voice.length*4,48000);
 for(let k=0;k<4;k++)longVoice.copyToChannel(voice.getChannelData(0),0,k*voice.length);
 let serial=0;
 async function freshPage(){
  clearInterval(feedTimer);feedTimer=null;try{activeVoice?.stop();}catch(_){}activeVoice=null;
  const previous=page;
  if(previous.listenTask)await previous.listenTask;
  if(previous.recording)await previous.finishSegment('fixture-end-page');
  if(previous.sendTask)await previous.sendTask;
  previous.closeBarge('cancel');await previous.stopSpeaking(false);
  await previous.echo?.stop(true);if(previous.echo?.pending)await previous.echo.task;
  const model=previous.detector,mock=previous.api;
  previous.releaseMic();await model?.close(true);previous.playbackGain?.disconnect();
  page=new CallPage({...env,createVAD:()=>model},()=>{},{bargeIn:'auto',captureMode:'auto',silenceMs:1800,vadRedemptionMs:1800});
  page.started=true;page.reference='synthetic';page.capability=null;page.ensurePush=()=>{};page.schedulePoll=()=>{};page.scheduleHeartbeat=()=>{};
  page.show=function(...args){messages.push(args.join(' '));return show.apply(this,args);};
  rows=[];state.audio=[];state.replies=[];state.segments=[];state.turns=[];state.barge_in=rows;page.server=state;page.api=mock;
  await until(()=>page.modelReady,20000);await page.listen();await wait(550);
  const warm=await page.finishSegment('fixture-warmup');warm.uploader?.abandon();page.clip=null;await page.echo?.ready;
 }
 async function cleanup(){
  try{activeVoice?.stop();activeVoice?.disconnect();tone?.stop();tone?.disconnect();}catch(_){}activeVoice=null;tone=null;
  clearInterval(feedTimer);feedTimer=null;
  if(page.listenTask)await page.listenTask;
  if(page.sendTask)await page.sendTask;
  page.closeBarge('cancel');await page.stopSpeaking(false);
  if(page.recording)await page.finishSegment('fixture-cleanup');
  page.clip?.uploader?.abandon();page.clip=null;page.round=null;page.series=null;
  page.muted=false;page.suspended=false;page.ending=false;page.locked=false;page.captureResumeRequired=false;
  if(!page.capture||page.capture.closed){page.stream=destination.stream.clone();page.audioContext=context;await page.prepareCapture(false,page.epoch);}
  if(page.echo&&!page.echo.closed)await page.echo.stop();page.echo=new EchoProbe(page,true);await page.echo.ready;
 }
 async function start(path='pcm',beforeStart=null,short=false){
  await cleanup();const id='fixture-'+(++serial),gid='reply:'+id;
  page.round={id,metrics:{},endedAt:performance.now()};state.audio.push({utterance_id:id,state:'succeeded'});
  state.replies=[{utterance_id:id,generation_id:gid,state:'succeeded'}];
  state.segments=[{generation_id:gid,seq:0,state:'complete'},{generation_id:gid,seq:1,state:'succeeded',playback_state:'unknown',revision:0}];
  if(beforeStart)await beforeStart();
  if(path==='pcm'){
   let index=0;
   const input=speechBuffer.getChannelData(0);let sample=0;
   const packet=()=>{
     const raw=new Uint8Array(1920),view=new DataView(raw.buffer);
     for(let i=0;i<960;i++){view.setInt16(i*2,Math.round(input[sample%input.length]*32767),true);sample+=2;}
     return btoa(String.fromCharCode(...raw));
   };
   page.acceptPCM({generation_id:gid,seq:1,index:index++,rate:24000,format:'s16le',data:packet()});
   feedTimer=setInterval(()=>{
    if(short&&index>=30){clearInterval(feedTimer);feedTimer=null;page.acceptPCM({generation_id:gid,seq:1,index:index++,rate:24000,format:'s16le',end:true});}
    else page.acceptPCM({generation_id:gid,seq:1,index:index++,rate:24000,format:'s16le',data:packet()});
   },40);
  }else {
   const blob=new Blob([await (await fetch('/bot-call/barge-test.mp3')).arrayBuffer()],{type:'audio/mpeg'});
   const s=page.speech={id:gid,seq:1,round:page.round,revision:0,echoPath:'mp3',url:URL.createObjectURL(blob)};
   page.preparePlayer(s,s.url);
  }
  await until(()=>page.speech?.playing||page.barge);
  const owned=page.speech||page.barge.speech;
  if(page.speech)page.echo.path(page.speech,path);
  const output=page.playbackOutput();
  // A deterministic marker through the actual master gain lets a rendering
  // worklet observe its exact first zero sample, independently of JS timers.
  if(output){output.connect(meter);tone=new ConstantSourceNode(context,{offset:.02});tone.connect(output);tone.start();}
  meter.port.postMessage(gid);
  await wait(100);return owned;
 }
 async function inject(buffer=voice){
  const source=context.createBufferSource();source.buffer=buffer;source.connect(bus);activeVoice=source;
  const values=buffer.getChannelData(0);let first=0;
  // Known clean fixture onset: first 5ms block with RMS >= .002 (-54dBFS).
  // This only timestamps the injected test source; product detection uses VAD.
  for(let a=0;a<values.length;a+=240){let power=0;for(let i=a;i<Math.min(a+240,values.length);i++)power+=values[i]**2;
    if(Math.sqrt(power/240)>=.002){first=a;break;}}
  const at=context.currentTime+.02,onset=performance.now()+(at-context.currentTime)*1000+first/48;
  result.fixtureOnsetMs=first/48;
  onsetAudio=at+first/48000;source.start(at);source.onended=()=>source.disconnect();return onset;
 }
 function automatic(upload,r,minimum=1800) {
  const m=upload.metrics,valid=r.vad?.valid;
  check((valid?m.client_segment_vad_endpoint===1:m.client_segment_auto_energy===1)&&
   !m.client_segment_manual&&!m.client_segment_control&&!m.client_segment_auto_limit,'ordinary turn not automatically ended');
  check(m.client_endpoint_quiet_ms>=minimum,'ordinary endpoint shortened');
  if(valid)check(m.client_endpoint_quiet_ms<minimum+64,'VAD endpoint late');
  const energyQuietMs=r.endedAt-Math.max(r.start,r.lastFallbackActivity??-Infinity,r.lastEnergyVoice??-Infinity,r.energyGapAt??-Infinity);
  if(!valid)check(energyQuietMs>=minimum&&energyQuietMs<minimum+250,'energy fallback quiet budget');
  return {quietMs:m.client_endpoint_quiet_ms,energyQuietMs:valid?undefined:energyQuietMs,signalValid:valid,reason:r.reason,gaps:r.vad?.gapCount||0};
 }
 function injectSignalDiscontinuity(r,kind) {
  const detector=page.detector,message=detector.message,trace={kind,injected:false};let added=0;
  detector.message=function(m) {
   const s=this.active;
   if(s&&s===r.vad&&m.id===s.id){
    if(m.type==='fault')trace.nativeFault={reason:m.reason,at:performance.now(),voiced:s.voiced};
    if(m.type==='gap')m={...m,count:m.count+added};
    if(!trace.injected&&m.type==='frame'&&s.voiced&&s.valid&&
       (s.lastFrame-s.lastCandidate)/s.rate*1000>=320) {
     trace.injected=true;trace.before={voiced:s.voiced,positiveMs:s.positiveMs,echoModel:!!page.echo?.model};
     trace.at=performance.now();
     if(kind==='gap'){
      // A discontinuity at a complete frame boundary has no missing duration,
      // but still resets recurrence/quiet, just like repeated native clocks.
      message.call(this,{type:'gap',id:s.id,start:m.start,count:(s.gapCount||0)+1,lostMs:s.lostMs||0,reason:'clock'});added++;
     }else this.fail(s,'quality','clock');
    }
   }
   return message.call(this,m);
  };
  return {trace,restore(){detector.message=message;}};
 }
 for(let i=0;i<35;i++){
  endClockTrial();result.stage='voice-'+i;mode='resume';const s=await start(),before=stt;
  clockTrial={attempt:i+1,events:[],gaps:[]};observeInput();const onset=await inject();
  try{await until(()=>page.barge,2400);}catch(_){result.misses++;continue;}
  const b=page.barge;result.estimates.push(Math.round(b.pausedAt+BARGE.rampMs-onset));
  try{await until(()=>graphStops.has(s.id),700);result.latencies.push(Math.round((graphStops.get(s.id)-onsetAudio)*1000));}
  catch(_){result.missingGraph++;}
  check(context.state==='running'&&page.capture&&!page.capture.closed,'shared input stopped');
  await until(()=>!page.barge,5500);if(page.speech!==s||s.terminal){
   const clip=page.clip,m=clip?.metrics;
   if(clip?.error && m?.client_capture_input_reason && page.captureDisabled){
    check(s.terminal&&!page.speech,'damaged input resumed old sound');
    result.captureRejects.push({attempt:i+1,reason:m.client_capture_input_reason,gapMs:m.client_capture_input_skip_ms,
      stopMs:result.latencies.at(-1),resumed:false});
    // A rejected input remains rejected in this call; create a new CallPage
    // after full owned cleanup instead of clearing the production disable flag.
    endClockTrial();await freshPage();
    result.callRestarts++;continue;
   }
   result.debug={localClosed:b.localClosed,pcmFailure:s.pcmFailure,row:rows.at(-1),recording:!!page.recording,
     clip:!!page.clip,message:page.message,metrics:m,error:clip?.error,overflow:clip?.overflow};
   throw Error('resume lost original player');
  }
  check(stt-before===1,'candidate transcribed more than once');
  check(!traffic.slice().reverse().find(t=>t.route==='segment-playback'&&t.body?.state==='stopped'),'soft pause sent terminal receipt');
  check(traffic.some(t=>t.body?.values?.client_barge_resumed===1),'resume missing');
 }
 endClockTrial();
 for(const path of ['pcm','mp3'])for(const attenuationDb of [-80,-30,-18,-6]){
  result.stage='echo-'+path+'-'+attenuationDb;mode='resume';await start(path);const before=rows.length;
  const residual=context.createBuffer(1,voice.length,48000);residual.copyToChannel(Float32Array.from(voice.getChannelData(0),v=>v*10**(attenuationDb/20)),0);
  await inject(residual);await wait(1400);if(page.barge)await until(()=>!page.barge,7000);
  result.echo.push({path,windows:1,candidates:rows.length-before,attenuationDb,kind:'synthetic_residual_not_device_AEC'});
 }
 await freshPage();result.stage='standby-repair';mode='resume';const repairedSpeech=await start();
 const beforeRepair=page.capture,contextBefore=context;
 beforeRepair.input(2,{credits:64});
 await until(()=>page.capture&&page.capture!==beforeRepair&&!page.capture.closed);
 check(context===contextBefore&&context.state==='running'&&page.speech===repairedSpeech,'repair disturbed output');
 await inject(shortVoice);await until(()=>page.barge);await until(()=>!page.barge,7000);
 check(page.speech===repairedSpeech&&!repairedSpeech.terminal,'repaired input did not resume');
 result.cases.push({kind:'standby-repair',inputRepairs:page.bargeInputRepairs,rebuilds:page.captureRebuilds});
 await freshPage();result.stage='mp3-resume';mode='resume';const mp3=await start('mp3');await inject(shortVoice);
 await until(()=>page.barge);const b=page.barge;await b.pauseReady;const time=b.mediaTime;
 check(mp3.media.paused,'MP3 did not pause');
 await until(()=>!page.barge);check(page.speech===mp3&&!mp3.media.paused&&mp3.media.currentTime>=time,'MP3 source/time lost');
 result.cases.push({kind:'mp3-resume',saved:time,errorUs:traffic.filter(t=>t.body?.values?.client_barge_offset_error_us!==undefined).at(-1).body.values.client_barge_offset_error_us});
 await freshPage();result.stage='configured-pause-onset';mode='interrupt';const pauseOld=await start(),beforePause=stt;
 await inject(shortVoice);await until(()=>page.barge);
 await until(()=>page.recording?.vad?.voiced);
 const pauseRecording=page.recording,signal=pauseRecording.vad;
 check(page.bargeSilenceMs()===1800&&signal.options.vadRedemptionMs===1800,'initial barge endpoint shortened');
 result.stage='configured-pause-quiet';
 const quiet=()=>((signal.lastFrame-signal.lastCandidate)/signal.rate*1000);
 await until(()=>quiet()>=1000||pauseRecording.stopping);
 const pauseMs=quiet(),firstVoiceEnd=signal.lastCandidate;
 check(pauseMs>=1000&&pauseMs<1800&&page.recording===pauseRecording&&!pauseRecording.stopping&&stt===beforePause,'one-second pause split barge speech');
 result.stage='configured-pause-continued';
 await inject(shortVoice);
 await until(()=>signal.lastCandidate>firstVoiceEnd||pauseRecording.stopping);
 check(signal.lastCandidate>firstVoiceEnd&&!pauseRecording.stopping,'voice after the pause was lost');
 await until(()=>!page.barge);
 const quietMs=uploads.at(-1).metrics.client_endpoint_quiet_ms;
 check(stt===beforePause+1&&quietMs>=1800&&pauseOld.terminal,'barge did not end once after configured silence');
 result.cases.push({kind:'configured-pause',pauseMs,quietMs,stt:1});
 await freshPage();result.stage='interrupt';mode='interrupt';const old=await start();await inject(longVoice);await until(()=>page.barge);const interrupted=page.barge;
 await until(()=>!page.barge);check(old.terminal&&page.speech?.id!==old.id,'confirmed old group alive');
 const sources=old.pcm?.sources.size;page.acceptPCM({generation_id:old.id,seq:1,index:999,rate:24000,format:'s16le',data:''});
 check(page.bargeLate>0&&page.speech?.id!==old.id,'late PCM revived old group');
 // New-generation audio can acquire output after the old fence.
 const newRound=page.round;clearInterval(feedTimer);feedTimer=null;
 const raw=btoa(String.fromCharCode(...new Uint8Array(1920)));
 page.acceptPCM({generation_id:'reply:'+newRound.id,seq:1,index:0,rate:24000,format:'s16le',data:raw});
 page.acceptPCM({generation_id:'reply:'+newRound.id,seq:1,index:1,rate:24000,format:'s16le',end:true});
 await until(()=>!page.speech);
 result.cases.push({kind:'interrupt',oldTerminal:old.terminal,lateRejected:true,newOutput:true});
 await freshPage();result.stage='slow-confirmation';mode='resume';networkDelay=3200;const slow=await start();await inject();
 await until(()=>page.barge);const slowB=page.barge;await until(()=>!page.barge,7000);networkDelay=0;
 check(page.speech===slow&&!slow.terminal,'slow confirmation lost cursor');
 result.cases.push({kind:'slow-confirmation',pauseMs:Math.round(performance.now()-slowB.pausedAt),networkDelayMs:3200});
 await freshPage();result.stage='timeout';mode='pending';const timed=await start();await inject();await until(()=>page.barge);
 const timedB=page.barge;await until(()=>timedB.localClosed==='timeout',BARGE.maxPauseMs+500);
 check(timed.terminal&&page.barge===timedB,'timeout pretended resume');
 result.cases.push({kind:'timeout',pauseMs:Math.round(performance.now()-timedB.pausedAt)});
 // Muted input must not make a candidate, even when real speech reaches the
 // fixture bus. The page has released its cloned input tracks and worklets.
 await freshPage();result.stage='mute-no-candidate';mode='pending';const mutedSpeech=await start();
 const mutedTracks=page.stream.getTracks(),count=rows.length,uploadCount=uploads.length,sttCount=stt;
 await page.toggleMute();await inject();await wait(1100);
 check(page.muted&&!page.capture&&mutedTracks.every(t=>t.readyState==='ended'),'mute retained live capture');
 check(rows.length===count&&!page.barge&&page.speech===mutedSpeech&&mutedSpeech.playing,'mute created barge or stopped playback');
 check(uploads.length===uploadCount&&stt===sttCount,'mute submitted new recording');
 result.cases.push({kind:'mute-no-candidate',candidates:rows.length-count});
 for(const control of ['stop','mute','hangup']){
  await freshPage();result.stage=control;mode='pending';const s=await start();
  const long=context.createBuffer(1,voice.length*4,48000);for(let k=0;k<4;k++)long.copyToChannel(voice.getChannelData(0),0,k*voice.length);
  await inject(long);await until(()=>page.barge);
  const b=page.barge;
  if(control==='stop')await page.stopSpeaking(false);
  if(control==='mute')await page.toggleMute();
  if(control==='background')await page.suspend();
  if(control==='hangup'){page.ending=true;page.releaseMic();}
  await wait(40);await page.settleBarge({utterance_id:b.id,decision:'resume',dispatch:'started'});
  check(s.terminal&&page.speech!==s&&!page.barge,'control resurrected old sound');
  result.cases.push({kind:control,lateResumeIgnored:true});
 }
 await freshPage();result.stage='off';page.config.bargeIn='off';await start();check(!page.playbackGain&&!page.speech.pcm.callbacks.output&&page.speech.pcm.callbacks.lookahead===Infinity,'off changed playback graph');const before=rows.length;await inject();await wait(1100);
 check(rows.length===before&&page.speech?.playing,'off changed legacy behavior');page.config.bargeIn='auto';
 result.cases.push({kind:'off',candidates:0});
 await freshPage();result.stage='stale-group';mode='pending';const stale=await start();await inject(shortVoice);await until(()=>page.barge);
 const staleB=page.barge;await until(()=>!page.recording&&!page.clip);
 state.segments[0].state='stopped';await page.settleBarge({utterance_id:staleB.id,decision:'resume',dispatch:'pending'});
 check(stale.terminal&&page.speech!==stale,'stale generation resumed');
 result.cases.push({kind:'stale-group',resumed:false});
 await freshPage();result.stage='candidate-continuation';mode='interrupt';await start();
 page.seriesCapability({seamless_series:{version:1,max_segments:10}});page.config.segmentMs=1000;
 await inject(longVoice);await until(()=>page.barge);const continued=page.barge;
 await until(()=>!page.barge,9000);page.config.segmentMs=30000;
 check(result.seriesSegments?.length>=2&&new Set(result.seriesSegments.map(s=>s.id)).size===result.seriesSegments.length,'continuation duplicate or missing');
 check(result.seriesSegments.every(s=>s.root===continued.id)&&result.seriesSegments.filter(s=>s.final).length+(result.seriesSeals||0)===1,'continuation lost candidate identity');
 result.cases.push({kind:'candidate-continuation',parts:result.seriesSegments.length,finals:1});

 // P3: actual Python classifier + original browser playback/capture. The
 // network peer stores synthetic material; native tests cover durable SQL/review.
 for(const path of ['pcm','mp3']){
  await freshPage();result.stage='backchannel-'+path;mode='p3';p3Text='嗯';p3Scenario='middle';const old=await start(path),before=stt,dispatched=dispatches;
  state.segments.push({generation_id:old.id,seq:2,state:'pending',playback_state:'unknown',text:'还有下一句。'});
  await inject(shortVoice);await until(()=>page.barge);const saved=page.barge;await saved.register;
  check(saved.remainingMs===null||saved.remainingMs>1000,'fixture not mid-sentence');
  await until(()=>!page.barge,6500);
  check(page.speech===old&&!old.terminal&&dispatches===dispatched&&stt===before+1,'backchannel replayed/dispatched');
  check(rows.at(-1).reason==='backchannel'&&state.turns.at(-1).source==='backchannel'&&state.turns.at(-1).body==='嗯','backchannel original/tag lost');
  check(traffic.filter(t=>t.body?.generation_id==='capture:'+saved.id).some(t=>t.body.values.client_barge_resumed===1&&t.body.values.client_barge_offset_error_us<=1000),'resume cursor mismatch');
  result.cases.push({kind:'backchannel',path,stt:1,dispatches:0,reason:rows.at(-1).reason});
  if(path==='pcm'){
   result.stage='backchannel-then-real';p3Scenario='content';p3Text='嗯，但是我想问新问题';
   await inject(shortVoice);await until(()=>page.barge);await until(()=>!page.barge,6500);
   check(old.terminal&&dispatches===dispatched+1&&rows.at(-1).reason==='content','real speech swallowed after backchannel');
   result.cases.push({kind:'backchannel-then-real',dispatches:1});
  }
 }
 for(const scenario of ['question','boundary','content']){
  await freshPage();result.stage='p3-'+scenario;mode='p3';p3Scenario=scenario;p3Text=scenario==='content'?'嗯，但是我想问':'对';const old=await start(scenario==='boundary'?'mp3':'pcm'),dispatched=dispatches;
  if(scenario==='question')state.segments[1].text='你觉得对吗？';
  if(scenario==='boundary'){
   old.media.currentTime=old.media.duration-.8;
  }else state.segments.push({generation_id:old.id,seq:2,state:'pending',playback_state:'unknown',text:'还有下一句。'});
  await inject(shortVoice);await until(()=>page.barge);await until(()=>!page.barge,6500);
  const expected=scenario==='question'?'question_context':scenario;
  check(old.terminal&&dispatches===dispatched+1&&rows.at(-1).reason===expected,'P3 answer/content misclassified:'+scenario+':'+rows.at(-1).reason);
  result.cases.push({kind:'p3-'+scenario,dispatches:1,reason:rows.at(-1).reason});
 }
 // P2b: no pre-start/between-sentence window may reset active speech.
 for(const enabled of [true,false]) {
  await freshPage();page.config.bargeIn=enabled?'auto':'off';mode='interrupt';
  result.stage='before-first-'+enabled;
  const before=stt,old=await start('pcm',async()=>{
   await wait(2300); // Exceeds the old 62-frame baseline observation cap.
   await inject(shortVoice);
   if(enabled)await until(()=>page.bargeRun?.count===BARGE.frames,1500);
   else await wait(520);
   check(!page.barge,'candidate without actual playback');
  });
  if(enabled){
   await until(()=>page.barge,1000);const b=page.barge;
   await until(()=>!page.barge,6500);
   check(old.terminal&&stt-before===1,'pre-start speech duplicate/lost');
   result.cases.push({kind:'before-first',oldTerminal:true,stt:stt-before,stopMs:Math.round(b.pausedAt-b.round.playAt)});
  } else {await wait(700);check(!page.barge&&old.playing&&stt===before,'off pre-start changed');}
  await freshPage();page.config.bargeIn=enabled?'auto':'off';mode='interrupt';
  result.stage='short-only-'+enabled;const beforeShort=stt,short=await start('pcm',null,true);
  await inject(shortVoice);
  if(enabled){await until(()=>page.barge,1200);await until(()=>!page.barge,6500);check(short.terminal&&stt-beforeShort===1,'short reply not interrupted once');}
  else {await wait(1000);check(!page.barge&&stt===beforeShort,'off short reply interrupted');}
  result.cases.push({kind:'single-short',enabled,candidates:enabled?1:0});
  if(enabled) {
   await freshPage();mode='interrupt';result.stage='short-candidate-gap';
   const beforeGap=stt,shortGap=await start('pcm',null,true),echo=page.echo,message=echo.message;
   const probe={injected:false};let added=0;
   echo.message=function(m) {
    if(m.type==='gap'&&added)m={...m,count:m.count+added,lostMs:m.lostMs+32};
    if(!probe.injected&&m.type==='frame'&&page.bargeRun?.count===3) {
     probe.injected=true;probe.count=page.bargeRun.count;probe.missingMs=32;
     // Drop exactly one real 32 ms analysis frame, retaining its capture PCM.
     // This exercises EchoProbe's gap delivery AND the candidate accumulator.
     message.call(this,{type:'gap',id:m.id,start:m.end,count:this.gapCount+1,lostMs:this.lostMs+32,reason:'clock'});
     added=1;this.node.port.postMessage({type:'ack',id:m.id,seq:m.seq});return;
    }
    return message.call(this,m);
   };
   try {
    await inject(shortVoice);await until(()=>page.barge,1200);await until(()=>!page.barge,6500);
    check(probe.injected&&shortGap.terminal&&stt-beforeGap===1,'short gap lost/duplicated candidate');
    result.cases.push({kind:'short-candidate-gap',...probe,candidates:1,stt:stt-beforeGap});
   }finally{echo.message=message;}
  }
  await freshPage();page.config.bargeIn=enabled?'auto':'off';mode='interrupt';
  result.stage='sentence-gap-'+enabled;const first=await start(),round=page.round,input=page.capture,echo=page.echo;
  clearInterval(feedTimer);feedTimer=null;state.segments[0].state='open';
  const next=first.pcm.index;
  page.acceptPCM({generation_id:first.id,seq:1,index:next,rate:24000,format:'s16le',end:true});
  await until(()=>!page.speech&&page.round===round);
  check(!page.recording&&!page.barge,'sentence gap became an ordinary turn');
  await wait(2200);await inject(shortVoice);
  if(enabled)await until(()=>page.bargeRun?.count===BARGE.frames,1500);else await wait(520);
  check(!page.barge,'gap alone triggered candidate');
  const blob=new Blob([await (await fetch('/bot-call/barge-test.mp3')).arrayBuffer()],{type:'audio/mpeg'});
  const second=page.speech={id:first.id,seq:2,round,revision:0,echoPath:'mp3',url:URL.createObjectURL(blob)};
  state.segments.push({generation_id:first.id,seq:2,state:'succeeded',playback_state:'unknown',revision:0});
  const beforeGap=stt;page.preparePlayer(second,second.url);
  if(enabled){
   await until(()=>page.barge,1000);check(page.barge.speech===second,'wrong gap sentence');
   await until(()=>!page.barge,6500);
   check(second.terminal&&stt-beforeGap===1,'gap speech lost/replayed');
   check(input===page.capture&&!input.closed,'gap rebuilt healthy input');
  }else{await wait(700);check(!page.barge&&second.playing&&stt===beforeGap,'off gap interrupted');}
  result.cases.push({kind:'pcm-to-mp3-gap',enabled,candidates:enabled?1:0});
  await freshPage();page.config.bargeIn=enabled?'auto':'off';result.stage='ordinary-turn-'+enabled;
  await start('pcm',null,true);
  await until(()=>!page.speech&&!page.round&&page.recording,3500);
  const normalBefore=stt,candidates=rows.length,normalRecording=page.recording;
  await inject(shortVoice);
  await until(()=>stt===normalBefore+1,5500);
  const normal=uploads.at(-1);
  check(rows.length===candidates&&!page.barge,'ordinary turn became a candidate');
  result.cases.push({kind:'ordinary-turn',enabled,...automatic(normal,normalRecording),candidates:0,stt:stt-normalBefore});
 }
 // P2c: reproducible post-speech discontinuity, with the real page/model/PCM.
 for(const enabled of [true,false])for(const fault of ['gap','fault']) {
  await freshPage();page.config.bargeIn=enabled?'auto':'off';result.stage='ordinary-'+fault+'-'+enabled;
  await start('pcm',null,true);await until(()=>!page.speech&&!page.round&&page.recording,3500);
  const r=page.recording,before=stt,candidates=rows.length,probe=injectSignalDiscontinuity(r,fault);
  result.endpointProbes ||= [];result.endpointProbes.push(probe.trace);
  try {
   await inject(shortVoice);await until(()=>stt===before+1,5500);
   probe.trace.ended={valid:r.vad?.valid,reason:r.reason,diagnostic:r.vad?.diagnostic,
    voiced:r.vad?.voiced,positiveMs:r.vad?.positiveMs,fallbackMs:r.fallbackMs,energyGapAt:r.energyGapAt,lastFallbackActivity:r.lastFallbackActivity,lastEnergyVoice:r.lastEnergyVoice,endedAt:r.endedAt};
   // A native fault may win before the injection point. In that case demand
   // the same bounded fallback (including unconfirmed energy), and report it instead of claiming that
   // the planned injection ran. A valid signal may never skip injection.
   probe.trace.nativePreempted=!probe.trace.injected&&r.vad?.valid===false&&(r.vad.voiced||r.fallbackMs>0)&&r.reason==='auto_energy';
   check(probe.trace.injected||probe.trace.nativePreempted,'discontinuity was not exercised');
   check(rows.length===candidates&&!page.barge,'discontinuity triggered candidate');
   result.cases.push({kind:'ordinary-discontinuity',enabled,fault,probe:probe.trace,...automatic(uploads.at(-1),r),stt:stt-before});
  }finally{probe.restore();}
 }
 // Both planned paths must actually execute in this page run; a native
 // preemption in one mode cannot hide missing injection coverage altogether.
 check(result.endpointProbes.some(p=>p.kind==='gap'&&p.injected),'no valid-gap injection coverage');
 check(result.endpointProbes.some(p=>p.kind==='fault'&&p.injected),'no fault injection coverage');
 // P2c: native overlap recovery and unconfirmed-energy fallback.
 for(const scenario of ['reverse-clock','fault-before-short','fault-noise','fault-quiet-noise','valid-noise']) {
  await freshPage();page.config.bargeIn='off';result.stage=scenario;
  await start('pcm',null,true);await until(()=>!page.speech&&!page.round&&page.recording,3500);
  const r=page.recording,before=stt,metricStart=traffic.length;
  let injected=false,restoreEnergy;const detector=page.detector,original=detector.message,originalReady=detector.ready;
  if(scenario==='reverse-clock'){
   detector.message=function(m){if(m.type==='test-back-applied'&&m.id===r.vad.id)injected=true;return original.call(this,m);};
   detector.node.port.postMessage({type:'test-back',samples:128});
   await until(()=>injected,1000);
  }else if(scenario.startsWith('fault-')){
   check(!r.vad.voiced,'fault fixture already heard voice');detector.fail(r.vad,'quality','clock');injected=true;
   if(scenario.endsWith('noise'))detector.ready=()=>false; // Persistently unavailable live analysis, offline gate still real.
  }
  try {
   if(scenario.endsWith('noise')){
    // Keep both defined paths: real energy activity must settle once;
    // noise below every activity threshold must keep listening. With .018
    // uniform noise RMS is ~.0052, above 3 * the .0015 minimum (even after
    // a subthreshold first window learns a floor), but below strong .006.
    // The former .012 borderline input could validly take either path as
    // onset moved across the RMS window; its failure/phase traces are retained.
    const amplitude=scenario==='fault-noise'?.018:scenario==='fault-quiet-noise'?.003:.012;
    const energy={samples:0,crossings:0,peakRMS:0},analyser=page.analyser,read=analyser.getFloatTimeDomainData.bind(analyser);
    analyser.getFloatTimeDomainData=values=>{
     read(values);if(page.recording!==r)return;
     let power=0;for(const v of values)power+=v*v;
     const rms=Math.sqrt(power/values.length),threshold=Math.max(.0015,Math.min(.025,page.config.rmsThreshold,(r.noiseFloor||0)*3));
     energy.samples++;energy.peakRMS=Math.max(energy.peakRMS,rms);
     if(rms>=threshold){energy.crossings++;energy.first ||= {rms,threshold};}
    };
    restoreEnergy=()=>{analyser.getFloatTimeDomainData=read;};
    const noise=context.createBuffer(1,48000*5,48000),a=noise.getChannelData(0);let seed=17;
    for(let i=0;i<a.length;i++){seed=(1664525*seed+1013904223)>>>0;a[i]=(seed/4294967296-.5)*amplitude;}
    await inject(noise);await wait(8500);
    const drops=traffic.slice(metricStart).filter(x=>x.route==='metrics'&&x.body.values?.client_silent_drop);
    check(stt===before&&page.recording&&!page.clip,'noise uploaded or failed to return to listening');
    check(energy.samples>=10&&energy.peakRMS>0,'noise energy not observed');
    if(scenario==='fault-noise'){
     check(energy.crossings>0&&r.fallbackMs>0,'noise never crossed the activity threshold');
     check(!(r.retentionEnergy?.length),'noise reached strong retention');
     check(r.stopped&&r.reason==='auto_energy'&&drops.length===1,'invalid VAD noise did not silently settle once');
     check(drops[0].body.values.client_gate_recheck===2,'noise skipped offline gate');
     check(!page.silentNotice,'automatic noise drop flashed a notice');
     const next=page.recording;check(!next.vad&&page.silentEnergyFloor,'noise floor was not carried into unavailable VAD');
     await inject(noise);await wait(8500);
     check(page.recording===next&&!next.stopping&&stt===before,'same noise rearmed after silent discard');
     check(traffic.slice(metricStart).filter(x=>x.route==='metrics'&&x.body.values?.client_silent_drop).length===1,'steady noise repeated drops');
    }else {
     check(page.recording===r&&!r.stopping&&drops.length===0,'noise without endpoint evidence changed recording');
     if(scenario==='fault-quiet-noise')check(energy.crossings===0&&!r.fallbackMs&&!r.energyVoiced&&!page.silentNotice,'subthreshold noise became activity');
    }
    result.cases.push({kind:scenario,injected,amplitude,energy,fallbackMs:r.fallbackMs||0,stt:stt-before,drops:drops.length,recording:true});
   }else{
    await inject(voice);await until(()=>stt===before+1,5500);
    const upload=uploads.at(-1);
    if(scenario==='reverse-clock')check(r.vad.valid&&r.vad.lostMs>0,'small overlap invalidated VAD or hid loss');
    else check(upload.metrics.client_gate_recheck===2,'short fallback skipped offline VAD');
    result.cases.push({kind:scenario,injected,...automatic(upload,r),stt:stt-before,recheck:upload.metrics.client_gate_recheck});
   }
  }finally{detector.message=original;detector.ready=originalReady;restoreEnergy?.();}
 }
 // P2c end.
 await freshPage();result.stage='waiting-without-repair';mode='interrupt';const waiting=await start('mp3');
 const waitingCapture=page.capture,waitingEcho=page.echo;
 for(let i=0;i<5;i++){
  waiting.media.dispatchEvent(new Event('waiting'));
  check(page.capture===waitingCapture&&!waitingCapture.closed&&page.echo===waitingEcho&&!waitingEcho.closed,'waiting destroyed input');
  await wait(100);waiting.media.dispatchEvent(new Event('playing'));
 }
 check(!page.bargeInputRepairs&&!page.bargeEchoRepairs,'healthy waiting spent repair budget');
 await inject(shortVoice);await until(()=>page.barge);await until(()=>!page.barge,6500);
 result.cases.push({kind:'waiting-five-times',inputRepairs:page.bargeInputRepairs||0,vadRepairs:page.bargeEchoRepairs||0});

 await freshPage();result.stage='legacy-speech-only';mode='speech_only';const stopped=await start(),beforeStop=stt;
 await inject(shortVoice);await until(()=>page.barge);await until(()=>!page.barge&&page.recording,6500);
 check(stopped.terminal&&stt-beforeStop===1&&messages.some(m=>m.includes('已停止朗读，有需要再说')),'speech_only feedback/capture missing');
 check(page.recording.barge===null&&page.recording.vad?.options.vadRedemptionMs===1800&&!page.series,'speech_only retained candidate endpoint');
 const afterStop=stt,nextRecording=page.recording;await wait(250);await inject(shortVoice);await until(()=>stt===afterStop+1,5500);
 const nextUpload=uploads.at(-1);
 automatic(nextUpload,nextRecording);
 result.cases.push({kind:'speech-only-then-ordinary',nextQuietMs:nextUpload.metrics.client_endpoint_quiet_ms,stt:stt-beforeStop,prompt:true});

 await freshPage();result.stage='interrupt-speech-only-ordinary';mode='interrupt';
 const chainPage=page,chainInput=page.capture,chainBefore=stt,chainOld=await start();
 await inject(shortVoice);await until(()=>page.barge);await until(()=>!page.barge,6500);
 check(chainOld.terminal&&stt===chainBefore+1&&page.round,'chain initial interrupt failed');
 clearInterval(feedTimer);feedTimer=null;
 const replyId='reply:'+page.round.id;
 state.replies.push({utterance_id:page.round.id,generation_id:replyId,state:'succeeded'});
 state.segments.push({generation_id:replyId,seq:0,state:'complete'},{generation_id:replyId,seq:1,state:'succeeded',playback_state:'unknown',revision:0});
 let chainIndex=page.speech?.pcm?.index||1;
 const chainPacket=new Uint8Array(1920),chainView=new DataView(chainPacket.buffer);
 for(let i=0;i<960;i++)chainView.setInt16(i*2,Math.round(Math.sin(i*.08)*5000),true);
 const chainRaw=btoa(String.fromCharCode(...chainPacket));
 feedTimer=setInterval(()=>page.acceptPCM({generation_id:replyId,seq:1,index:chainIndex++,rate:24000,format:'s16le',data:chainRaw}),40);
 await until(()=>page.speech?.playing,2500);const chainReply=page.speech;mode='speech_only';
 await inject(shortVoice);await until(()=>page.barge);await until(()=>!page.barge&&page.recording,6500);
 clearInterval(feedTimer);feedTimer=null;
 check(chainReply.terminal&&stt===chainBefore+2&&!page.series&&!page.round,'chain speech_only did not restore ordinary capture');
 const chainRecording=page.recording;
 await inject(shortVoice);await until(()=>stt===chainBefore+3,5500);
 check(page===chainPage&&page.capture===chainInput&&rows.length===2,'chain reset page/input or duplicated candidates');
 result.cases.push({kind:'interrupt-speech-only-ordinary',...automatic(uploads.at(-1),chainRecording),stt:stt-chainBefore,candidates:rows.length,samePage:true});

 await freshPage();result.stage='long-early';mode='interrupt';const longOld=await start(),beforeLong=stt;
 const firstSamples=voice.length*12,quietSamples=52800;
 const complete=context.createBuffer(1,firstSamples+quietSamples+shortVoice.length,48000);
 for(let i=0;i<12;i++)complete.copyToChannel(voice.getChannelData(0),0,voice.length*i);
 complete.copyToChannel(shortVoice.getChannelData(0),0,firstSamples+quietSamples);
 const longStart=context.currentTime;
 await inject(complete);await until(()=>page.barge);const longB=page.barge;
 await until(()=>longB.longConfirmed,5000);
 check(longB.localClosed==='long_speech'&&longOld.terminal&&page.recording&&!longB.timer&&stt===beforeLong,'long confirmation did not keep whole capture');
 check(page.recording.vad.options.vadRedemptionMs===1800,'long endpoint remained short');
 while(context.currentTime<longStart+firstSamples/48000+.95){check(stt===beforeLong,'long 1.1 s pause split expression');await wait(50);}
 await until(()=>stt===beforeLong+1,6500);await until(()=>!page.barge,1500);
 const whole=uploads.at(-1),decoded=await context.decodeAudioData(whole.bytes.slice(0));
 check(decoded.duration>=complete.duration-.5&&whole.metrics.client_segment_vad_endpoint===1&&whole.metrics.client_endpoint_quiet_ms>=1800,'long expression truncated');
 check(!rows.some(r=>r.utterance_id===longB.id&&r.closed_reason==='timeout')&&stt===beforeLong+1,'long timeout or repeated STT');
 result.cases.push({kind:'long-early',pauseMs:Math.round(longB.pauseEndedAt-longB.pausedAt),closedReason:rows.at(-1).closed_reason,
  decodedSeconds:decoded.duration,fixtureSeconds:complete.duration,quietMs:whole.metrics.client_endpoint_quiet_ms,stt:1});

 await freshPage();result.stage='long-cancel';mode='pending';const revoked=await start();
 await inject(complete);await until(()=>page.barge);const revokedB=page.barge;
 await until(()=>revokedB.longConfirmed,5000);
 page.closeBarge('cancel');await revokedB.control;
 check(rows.find(r=>r.utterance_id===revokedB.id)?.decision==='cancelled'&&!page.barge&&revoked.terminal,'early-closed candidate escaped cancellation');
 result.cases.push({kind:'long-cancel',serverCancelled:true});
 await freshPage();result.stage='long-off';page.config.bargeIn='off';mode='pending';const offLong=await start(),offCount=stt;
 await inject(complete);await wait(6600);
 check(!page.barge&&offLong.playing&&stt===offCount&&!rows.length,'off entered early-confirm or timeout');
 result.cases.push({kind:'long-off',candidates:0,stt:0});
 await freshPage();result.stage='capacity';mode='pending';const capped=await start();
 await page.beginBarge(capped,{onset:performance.now()-160,decided:performance.now(),referenceKnown:1});
 const cappedB=page.barge;clearInterval(feedTimer);feedTimer=null;
 for(let i=0;i<300&&!cappedB.localClosed;i++){
  const p=capped.pcm;
  page.acceptPCM({generation_id:capped.id,seq:1,index:p.index,rate:24000,format:'s16le',data:btoa(String.fromCharCode(...new Uint8Array(1920)))});
 }
 check(cappedB.localClosed==='capacity'&&capped.terminal&&cappedB.addedBytes<=1048576,'pause buffer cap');
 result.cases.push({kind:'capacity',sealed:true});
 await freshPage();result.stage='prefetch-capacity';mode='pending';const prefetched=await start();
 await page.beginBarge(prefetched,{onset:performance.now()-160,decided:performance.now(),referenceKnown:1});
 const prefetchB=page.barge;let cancelledReader=false,failedDownload=false;
 const response=new Response(new ReadableStream({pull(c){c.enqueue(new Uint8Array(131072));},cancel(){cancelledReader=true;}}),{headers:{'Content-Type':'audio/mpeg'}});
 try{await page.readBargeAudio(response,prefetched.round,prefetched.id);}catch(_){failedDownload=true;}
 check(failedDownload&&cancelledReader&&prefetchB.localClosed==='capacity'&&prefetchB.addedBytes<=1048576,'unbounded paused MP3 prefetch');
 let rejectedLate=false;
 try{await page.readBargeAudio(new Response(new Uint8Array(256)),prefetched.round,prefetched.id);}catch(_){rejectedLate=true;}
 check(rejectedLate&&prefetched.terminal&&!page.speech,'late prefetch retained output rights');
 result.cases.push({kind:'prefetch-capacity-and-late',sealed:true,readerCancelled:true});
 await freshPage();result.stage='prefetch-existing-copy';mode='pending';const copying=await start();
 let finishCopy,firstCopyRead=false;
 const copyResponse=new Response(new ReadableStream({start(c){c.enqueue(new Uint8Array(1500000));finishCopy=()=>c.close();},pull(){firstCopyRead=true;}}));
 const copyDownload=page.readBargeAudio(copyResponse,copying.round,copying.id).then(()=>false,()=>true);
 await until(()=>firstCopyRead);
 await page.beginBarge(copying,{onset:performance.now()-160,decided:performance.now(),referenceKnown:1});
 const copyingB=page.barge;finishCopy();
 check(await copyDownload&&copyingB.localClosed==='capacity'&&copyingB.addedBytes<=1048576,'pre-pause chunks copied outside pause budget');
 result.cases.push({kind:'prefetch-existing-copy',sealed:true});
 await freshPage();result.stage='network-source-loss';mode='pending';const disconnected=await start();
 await page.beginBarge(disconnected,{onset:performance.now()-160,decided:performance.now(),referenceKnown:1});
 const disconnectedB=page.barge;disconnected.pcm.interrupt();
 check(disconnectedB.localClosed==='unavailable'&&disconnected.terminal,'incomplete disconnected PCM remained resumable');
 result.cases.push({kind:'network-source-loss',sealed:true});
 await freshPage();result.stage='mp3-resume-failure';mode='pending';const failedMedia=await start('mp3');await inject(shortVoice);
 await until(()=>page.barge);const failedB=page.barge;
 await until(()=>!page.recording&&!page.clip,5500);
 const playMedia=page.playMedia;page.playMedia=()=>{};
 await page.settleBarge({utterance_id:failedB.id,decision:'resume',dispatch:'pending'});page.playMedia=playMedia;
 check(failedMedia.terminal&&!page.speech&&!!page.recording,'failed MP3 resume blocked capture or claimed success');
 result.cases.push({kind:'mp3-resume-failure',resumed:false,captureReady:true});
 await freshPage();result.stage='fade-control';mode='pending';const fading=await start();
 const pending=page.beginBarge(fading,{onset:performance.now()-160,decided:performance.now(),referenceKnown:1});
 page.closeBarge('cancel');await pending;await wait(40);
 check(fading.terminal&&!fading.pcm&&!page.barge,'fade control retained sources');
 result.cases.push({kind:'fade-control',sealed:true});
 await freshPage();result.stage='refresh';await cleanup();
 rows.push({utterance_id:'resumed-old',generation_id:'reply:resumed-old',seq:1,position_ms:70,started:1,decision:'resume',closed_reason:'resumed'});
 rows.push({utterance_id:'lost',generation_id:'reply:fixture-lost',seq:1,position_ms:70,started:1,decision:'pending'});
 await page.reconcileBarge({barge_in:rows});check(rows.at(-1).closed_reason==='refresh','refresh claimed lossless resume');
 check(page.bargeCandidates===2&&page.bargeFalse===1,'refresh lost per-call counters');
 result.cases.push({kind:'refresh',sealed:true,candidates:page.bargeCandidates,falseCandidates:page.bargeFalse});
 result.commandLatencies=traffic.filter(t=>t.body?.values?.client_barge_command_ms!==undefined).map(t=>t.body.values.client_barge_command_ms);
 check(result.latencies.length+result.misses+result.missingGraph===35,'missing denominator');
 check(result.latencies.length>=30,'fewer than 30 measured stops');
 const sorted=result.latencies.slice().concat(Array(result.misses+result.missingGraph).fill(Infinity)).sort((a,b)=>a-b),q=p=>sorted[Math.ceil(p*sorted.length)-1];
 result.quantiles={attempts:35,valid:result.latencies.length,misses:result.misses,missingGraph:result.missingGraph,median:q(.5),p90:q(.9),p95:Number.isFinite(q(.95))?q(.95):null};
 const command=result.commandLatencies.slice().sort((a,b)=>a-b);
 result.commandP95=command[Math.ceil(command.length*.95)-1];check(result.commandP95<=50,'command budget');
 check(q(.5)<=400&&q(.9)<=600,'graph stop budget');
 await freshPage();result.stage='background';mode='pending';const bg=await start();
 const long=context.createBuffer(1,voice.length*4,48000);for(let k=0;k<4;k++)long.copyToChannel(voice.getChannelData(0),0,k*voice.length);
 await inject(long);await until(()=>page.barge);const bgB=page.barge;await page.suspend();
 await page.settleBarge({utterance_id:bgB.id,decision:'resume',dispatch:'started'});
 check(bg.terminal&&!page.barge&&page.speech!==bg,'background resurrected old sound');
 result.cases.push({kind:'background',lateResumeIgnored:true});
 result.ok=true;
}catch(e){result.failure=e.message;result.pageState={barge:!!page?.barge,captureClosed:page?.capture?.closed,captureDisabled:page?.captureDisabled,captureFailure:page?.captureFailure,clipMetrics:page?.clip?.metrics,recording:!!page?.recording,recordingState:page?.recording?{
   vadValid:page.recording.vad?.valid,voiced:page.recording.vad?.voiced,positiveMs:page.recording.vad?.positiveMs,
   endpoint:page.recording.vad?.endpoint,options:page.recording.vad?.options,lastFrame:page.recording.vad?.lastFrame,
   lastCandidate:page.recording.vad?.lastCandidate,rate:page.recording.vad?.rate,vadDiagnostic:page.recording.vad?.diagnostic,
   energyValid:page.recording.energyValid,quality:page.recording.quality,metrics:page.recording.metrics,
   hasRound:!!page.round,hasClip:!!page.clip,series:!!page.series,stopping:page.recording.stopping}:null,echoClosed:page?.echo?.closed,echoModel:!!page?.echo?.model,detectorActive:!!page?.detector?.active,message:page?.message};}
finally{
 endClockTrial();clearInterval(feedTimer);page?.closeBarge('cancel');
 try{if(page?.recording)await page.finishSegment('fixture-cleanup');}catch(_){}
 try{await page?.stopSpeaking(false);page?.releaseMic();page?.closePCM();}catch(_){}
 try{tone?.stop();tone?.disconnect();meter?.disconnect();meterMute?.disconnect();zero?.stop();bus?.disconnect();await context?.close();}catch(_){}
 result.contextClosed=context?.state==='closed';window.smokeResult=result;
}
"""
if __name__ == "__main__":
    if "--endpoint-only" in sys.argv:
        sys.argv.remove("--endpoint-only")
        body = smoke.BROWSER_JS
        begin = body.index(" for(let i=0;i<35;i++){")
        cases = body.index(" // P2c: reproducible")
        end = body.index(" // P2c end.")
        finish = body.index(" result.ok=true;")
        smoke.BROWSER_JS = body[:begin] + body[cases:end] + body[finish:]
    if "--p2b-only" in sys.argv:
        sys.argv.remove("--p2b-only")
        body = smoke.BROWSER_JS
        begin = body.index(" for(let i=0;i<35;i++){")
        new_cases = body.index(" // P2b:")
        end = body.index(" await freshPage();result.stage='capacity';")
        finish = body.index(" result.ok=true;")
        smoke.BROWSER_JS = body[:begin] + body[new_cases:end] + body[finish:]
    if "--quick" in sys.argv:
        sys.argv.remove("--quick")
        smoke.BROWSER_JS = (
            smoke.BROWSER_JS.replace("i<35", "i<2")
            .replace("===35", "===2")
            .replace("length>=30", "length>=1")
            .replace("attempts:35", "attempts:2")
            .replace("check(q(.5)<=400&&q(.9)<=600", "check(true")
        )
    smoke.main()
