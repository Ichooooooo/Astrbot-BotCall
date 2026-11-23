"""Muted WSL headless Chrome smoke, loopback and synthetic/public audio only.

One Linux browser/profile per invocation, reused across --repeat page runs.
The driver owns and cleans its process group; no real microphone or providers.
"""

import argparse
import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import signal
from tempfile import TemporaryDirectory
from pathlib import Path
import subprocess
import sys
import threading

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from bot_call.web import CallWebApp
from tools.dev_paths import BROWSER
from node_runtime import node_runtime

BROWSER_JS = r"""
import {LocalVAD,SileroSignalModel,VAD_MAX_FRAMES} from '/bot-call/vad.js';
import {nativeFrame} from '/bot-call/call.js';
const wait=ms=>new Promise(r=>setTimeout(r,ms));
const check=(value)=>{if(!value)throw new Error('smoke.assertion');};
let detector,context,destination,zero;
const result={ok:false,stage:"start",processorErrors:0};
try {
  navigator.mediaDevices.getUserMedia=()=>{throw new Error('smoke.real_mic_forbidden');};
  context=new AudioContext({sampleRate:48000});await context.resume();
  // Real stereo stream source: no createMediaStreamSource replacement or
  // fixture-specific AudioWorkletNode subclass normalizes this graph.
  destination=new MediaStreamAudioDestinationNode(context,{channelCount:2,channelCountMode:'explicit',channelInterpretation:'speakers'});
  const bus=new GainNode(context,{gain:1,channelCount:2,channelCountMode:'explicit',channelInterpretation:'speakers'});
  bus.connect(destination);
  zero=new ConstantSourceNode(context,{offset:0});zero.connect(bus);zero.start();
  const nativeMediaSource=context.createMediaStreamSource;
  const fixtureEnv={document,fetch:fetch.bind(window),AbortController,setTimeout,clearTimeout,
    AudioWorkletNode,get ort(){return window.ort;}};
  // Native Window timers must keep their Window receiver in this fixture.
  fixtureEnv.setTimeout=setTimeout.bind(window);fixtureEnv.clearTimeout=clearTimeout.bind(window);
  let warmupRuns=0,warmupResets=0;
  const originalRun=SileroSignalModel.prototype.run,originalReset=SileroSignalModel.prototype.reset;
  SileroSignalModel.prototype.run=function(frame){if(!detector?.model){warmupRuns++;check(frame.length===512&&frame.every(x=>x===0));}return originalRun.call(this,frame);};
  SileroSignalModel.prototype.reset=function(){warmupResets++;return originalReset.call(this);};
  result.stage="prepare";detector=new LocalVAD(fixtureEnv);check(await detector.prepare(destination.stream,context));
  check(warmupRuns===1&&warmupResets===1&&detector.model.state.data.every(x=>x===0)&&detector.model.context.every(x=>x===0));
  result.warmupRuns=warmupRuns;result.warmupReset=true;result.warmupMs=detector.metrics.warmupMs;
  check(VAD_MAX_FRAMES===16&&Number.isSafeInteger(result.warmupMs));
  check(ort.env.wasm.numThreads===1&&ort.env.wasm.proxy===false);
  check(destination.channelCount===2&&bus.channelCount===2);
  check(detector.source instanceof MediaStreamAudioSourceNode&&context.createMediaStreamSource===nativeMediaSource);
  check(detector.node instanceof AudioWorkletNode&&fixtureEnv.AudioWorkletNode===AudioWorkletNode);
  detector.node.addEventListener('processorerror',()=>{result.processorErrors++;});
  result.nativeMediaStreamSource=true;result.destinationChannels=destination.channelCount;
  result.normalized={channelCount:detector.node.channelCount,mode:detector.node.channelCountMode,interpretation:detector.node.channelInterpretation};
  check(result.normalized.channelCount===1&&result.normalized.mode==='explicit'&&result.normalized.interpretation==='speakers');
  // The measured startup discontinuity preceded recordStart. Warm this
  // synthetic graph before arm; all recorded gaps remain strictly checked.
  await wait(150);result.preArmWarmMs=150;
  // Same true stereo MediaStream, original worklet and old default options:
  // output mono alone must still expose the strict two-channel input fault.
  result.stage='old-default-probe';
  const oldSource=context.createMediaStreamSource(destination.stream);
  const oldNode=new AudioWorkletNode(context,'bot-call-voice-signal',{numberOfInputs:1,numberOfOutputs:1,outputChannelCount:[1]});
  const oldGain=context.createGain();oldGain.gain.value=0;
  oldNode.addEventListener('processorerror',()=>{result.processorErrors++;});
  oldSource.connect(oldNode);oldNode.connect(oldGain);oldGain.connect(context.destination);
  let oldTimer;
  try {
    result.oldDefault=await new Promise(resolve=>{
      oldNode.port.onmessage=({data:m})=>{if(m.type==='fault')resolve({reason:m.reason,channels:m.channels,samples:m.samples});};
      oldTimer=setTimeout(()=>resolve({reason:'unconfirmed'}),1000);
      oldNode.port.postMessage({type:'begin',id:900001,maxFrames:VAD_MAX_FRAMES});
    });
    check(result.oldDefault.reason==='channels'&&result.oldDefault.channels===2&&result.oldDefault.samples>0);
  } finally {
    clearTimeout(oldTimer);oldNode.port.postMessage({type:'shutdown'});
    oldSource.disconnect();oldNode.disconnect();oldGain.disconnect();oldNode.port.close();
  }
  let peakQueue=0,peakOutstanding=0;
  const received={};result.received=received;
  const originalMessage=detector.message.bind(detector);
  detector.message=m=>{received[m.type]=(received[m.type]||0)+1;if(m.type==='fault')result.faultReason={reason:m.reason,channels:m.channels,samples:m.samples};originalMessage(m);const s=detector.active;if(s){peakQueue=Math.max(peakQueue,s.queue.length);peakOutstanding=Math.max(peakOutstanding,s.received-s.processed);}};
  const originalFail=detector.fail.bind(detector);
  detector.fail=(s,stage,reason)=>{result.fault={stage,reason,received:s.received,processed:s.processed,queued:s.queue.length,start:s.start,edge:s.edge,rate:s.rate};originalFail(s,stage,reason);};
  let running=0,peak=0,runs=0,pauseNext=false,pauses=0;
  const run=detector.model.run.bind(detector.model);
  detector.model.run=async frame=>{running++;runs++;peak=Math.max(peak,running);
    try {
      if(pauseNext){pauseNext=false;pauses++;const start=performance.now();await wait(180);result.processingPauseMs=Math.round(performance.now()-start);}
      return await run(frame);
    }finally{running--;}};
  const options={vadPositive:.5,vadNegative:.45,vadMinSpeechMs:32,vadRedemptionMs:1200};
  const phases=[];result.phases=phases;
  async function segment(kind,buffer) {
    if(kind==='public-wav')pauseNext=true;
    const events=[],s=await detector.begin(options,e=>events.push(e));check(s);
    s.recordStart=nativeFrame(context);check(Number.isSafeInteger(s.recordStart));
    const mime=['audio/webm;codecs=opus','audio/webm'].find(x=>MediaRecorder.isTypeSupported(x));check(mime);
    const recorder=new MediaRecorder(destination.stream,{mimeType:mime}),chunks=[];
    recorder.ondataavailable=e=>{if(e.data.size)chunks.push(e.data);};
    const stopped=new Promise(resolve=>{recorder.onstop=resolve;});recorder.start(250);
    if(buffer) {
      const source=context.createBufferSource();source.buffer=buffer;source.connect(bus);
      const ended=new Promise(resolve=>{source.onended=resolve;});source.start();await ended;source.disconnect();
      if(kind==='public-wav') {
        // Wait for the actual signal endpoint, bounded independently of host scheduling.
        // A fixed 1500ms wall delay can stop before 1200ms of model-negative frames.
        const deadline=performance.now()+4000;
        while(!events.includes('endpoint')&&performance.now()<deadline)await wait(50);
      } else await wait(100);
    } else await wait(400);
    recorder.stop();await stopped;
    const end=nativeFrame(context),answer=await detector.finish(s,end);
    result.last={answer,events,received:s.received,processed:s.processed,start:s.start,recordStart:s.recordStart,recordEnd:end,end:s.end,blobBytes:new Blob(chunks).size};
    check(answer.valid);check(new Blob(chunks).size>0);
    if(kind==='silence')check(!answer.candidate);
    if(kind==='public-wav')check(answer.candidate&&events.filter(e=>e==='endpoint').length===1);
    if(kind==='short-public-wav')check(answer.candidate);
    phases.push({kind,metrics:s.metrics,sourceChannels:buffer?.numberOfChannels||0,covered:answer.valid,candidate:answer.candidate,endpoint:events.filter(e=>e==='endpoint').length,frames:s.processed,blobBytes:new Blob(chunks).size});
  }
  result.stage='silence';await segment('silence');
  const tone=context.createBuffer(1,context.sampleRate*.25,context.sampleRate),data=tone.getChannelData(0);
  for(let i=0;i<data.length;i++)data[i]=.02*Math.sin(2*Math.PI*220*i/context.sampleRate);
  result.stage='tone';await segment('synthetic-tone',tone);
  const response=await fetch('/bot-call/fixed-sample.wav');check(response.ok);
  const speech=await context.decodeAudioData(await response.arrayBuffer());
  const short=context.createBuffer(2,Math.round(context.sampleRate*.32),context.sampleRate);
  short.copyToChannel(speech.getChannelData(0).subarray(Math.round(context.sampleRate*.5),Math.round(context.sampleRate*.82)),0);
  short.copyToChannel(Float32Array.from(short.getChannelData(0),v=>v*.8),1);
  result.stage='short-public-wav';await segment('short-public-wav',short);
  result.stage='public-wav';await segment('public-wav',speech);
  await detector.close();check(detector.model===null&&detector.active===null&&running===0);
  check(destination.stream.getTracks().every(t=>t.readyState==='live')); // detector never owns tracks
  result.stage="completed";result.phases=phases;result.runs=runs;result.peakRuns=peak;result.singleThread=true;result.released=true;result.peakQueue=peakQueue;result.peakOutstanding=peakOutstanding;check(peakQueue>=3&&peakQueue<=VAD_MAX_FRAMES-1&&peakOutstanding>=4&&peakOutstanding<=VAD_MAX_FRAMES&&result.processorErrors===0);
  check(pauses===1&&result.processingPauseMs>=150&&warmupRuns===1);result.pauses=pauses;result.maxFrames=VAD_MAX_FRAMES;result.ok=true;
} catch (_) {result.ok=false;result.failure='smoke.unconfirmed';}
finally {
  try{await detector?.close();}catch(_){}
  try{zero?.stop();zero?.disconnect();}catch(_){}
  for(const t of destination?.stream.getTracks()||[])t.stop();
  try{await context?.close();}catch(_){}
  result.contextClosed=context?.state==='closed';window.smokeResult=result;
}
"""


COLD_BROWSER_JS = r"""
import {CallPage} from '/bot-call/call.js';
import {LocalVAD,SileroSignalModel} from '/bot-call/vad.js';
const wait=ms=>new Promise(r=>setTimeout(r,ms));
const check=(v,message)=>{if(!v)throw Error(message);};
async function until(predicate,ms=6000) {
  const end=performance.now()+ms;while(!predicate() && performance.now()<end)await wait(50);
  check(predicate(),'cold.deadline');
}
const overlap=false;
const result={ok:false,stage:'start',segments:[],preArmWarmMs:0,overlap};
let page,context,generator,oscillator,destination,peakRuns=0,running=0;
try {
  navigator.mediaDevices.getUserMedia=()=>{throw Error('cold.real_mic_forbidden');};
  generator=new AudioContext({sampleRate:48000});await generator.resume();
  destination=generator.createMediaStreamDestination();
  const bus=generator.createGain();bus.connect(destination);
  oscillator=generator.createOscillator();const amplitude=generator.createGain();amplitude.gain.value=0;
  oscillator.connect(amplitude);amplitude.connect(bus);oscillator.start();
  const detector=new LocalVAD(window);
  const run=SileroSignalModel.prototype.run;
  SileroSignalModel.prototype.run=async function(frame) {
    running++;peakRuns=Math.max(peakRuns,running);try{return await run.call(this,frame);}finally{running--;}
  };
  const env={storage:{getItem:()=>'',setItem(){}},location:{hash:'',pathname:'/bot-call/'},history,
    now:()=>performance.now(),Audio,URL,Blob,crypto,AbortController,MediaRecorder,AudioContext,
    mediaDevices:{getUserMedia:async()=>destination.stream},createVAD:()=>detector,
    setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window),isHidden:()=>false};
  page=new CallPage(env);page.started=true;page.reference='synthetic-local-only';
  page.server={audio_available:true,speech_available:true};page.refresh=async()=>{};
  const staged=new Map(),committed=new Set();
  page.api=async(route,body,headers={})=>{
    if(route==='metrics')return {recorded:true};
    const id=headers['X-Bot-Utterance-ID'];check(!!id,'cold.id');
    const stored=staged.get(id)||new Uint8Array(),bytes=new Uint8Array(await body.arrayBuffer());
    if(route==='audio-chunk') {
      check(Number(headers['X-Bot-Chunk-Offset'])===stored.length,'cold.offset');
      const next=new Uint8Array(stored.length+bytes.length);next.set(stored);next.set(bytes,stored.length);staged.set(id,next);
      return {chunk_received:true,utterance_id:id,next_seq:Number(headers['X-Bot-Chunk-Seq'])+1,next_offset:next.length};
    }
    check(route==='audio-commit' || route==='audio','cold.route');check(!committed.has(id),'cold.duplicate');
    const offset=route==='audio-commit'?Number(headers['X-Bot-Tail-Offset']):0;
    check(offset<=stored.length,'cold.tail');const full=new Uint8Array(offset+bytes.length);full.set(stored.subarray(0,offset));full.set(bytes,offset);
    const clip=page.clip;check(full.length===clip.blob.size,'cold.truncated');
    const sha=[...new Uint8Array(await crypto.subtle.digest('SHA-256',full))].map(x=>x.toString(16).padStart(2,'0')).join('');
    check(sha===clip.sha,'cold.bytes');committed.add(id);return {audio_received:true,utterance_id:id};
  };
  async function record(kind,play) {
    page.round=null;await page.listen();const r=page.recording;check(!!r,'cold.recording');context=page.audioContext;
    check(page.analysisSource instanceof MediaStreamAudioSourceNode,'cold.source');
    check(page.analysisGain.gain.value===0 && context!==generator,'cold.sink');
    if(kind==='first')check(r.vad===null && page.modelReady!==true,'cold.not_cold');
    else check(r.vad?.valid,'cold.not_ready');
    let peakLevel=0,samples=0,maxGap=0,previous=performance.now();
    const original=page.sampleRecording.bind(page);
    page.sampleRecording=value=>{original(value);if(value===r){const now=performance.now();maxGap=Math.max(maxGap,now-previous);previous=now;peakLevel=Math.max(peakLevel,value.level||0);samples++;}};
    await play();await until(()=>page.round && !page.sendTask);
    page.sampleRecording=original;
    check(r.stopped && !r.error && r.clip.blob.size>0 && committed.has(r.id),'cold.lost');
    check(peakLevel>.025 && samples>5,'cold.meter');
    check(r.metrics.client_energy_frames>5 && Number.isSafeInteger(r.metrics.client_energy_first_ms),'cold.samples');
    check(r.metrics.client_audio_state_start===1 && r.metrics.client_audio_state_end===1,'cold.state');
    if(kind==='first') {
      check(r.reason==='auto_energy','cold.endpoint');
      const pause=r.endedAt-Math.max(r.lastEnergyVoice,r.energyGapAt ?? -Infinity);
      check(pause>=1200 && pause<1600,'cold.silence');
      check(r.vad===null,'cold.mid_segment_adoption');result.energyPauseMs=Math.round(pause);
      if(overlap)check(maxGap>500 && Number.isFinite(r.energyGapAt) && r.clip.quality.valid===false,'cold.overlap_not_observed');
    } else {
      check(r.reason==='auto_silence' && r.clip.vad.valid && r.clip.vad.candidate,'cold.handover');
      check(r.metrics.client_vad_gaps===0 && r.metrics.client_vad_lost_ms===0,'cold.graph_gap');
    }
    result.segments.push({kind,reason:r.reason,metrics:r.metrics,peakLevel:Math.round(peakLevel*1000)/1000,
      samples,maxGapMs:Math.round(maxGap),blobBytes:r.clip.blob.size,modelReady:page.modelReady===true});
  }
  if(overlap) {
    result.stage='wait-session';await until(()=>detector.diagnostic?.stage==='model',20000);await wait(10500);
  }
  result.stage='cold-first';await record('first',async()=>{
    // Schedule the sound's end on the audio clock, independent of blocked JS.
    const t=generator.currentTime;amplitude.gain.setValueAtTime(.1,t+.02);amplitude.gain.setValueAtTime(0,t+1.02);await wait(1200);
  });
  if(!overlap)check(page.modelReady!==true,'cold.download_finished_too_soon');
  result.stage='model';check(await page.modelPreparation,'cold.model');
  const response=await fetch('/bot-call/fixed-sample.wav');check(response.ok,'cold.fixture');
  const speech=await generator.decodeAudioData(await response.arrayBuffer());
  result.stage='handover';await record('ready',async()=>{
    const source=generator.createBufferSource();source.buffer=speech;source.connect(bus);
    const ended=new Promise(resolve=>{source.onended=resolve;});source.start();await ended;source.disconnect();
  });
  result.preparation=page.preparationMetrics;result.commits=committed.size;check(result.commits===2,'cold.commit_count');
  result.resources=performance.getEntriesByType('resource').filter(x=>/onnx|wasm/.test(x.name)).map(x=>({
    name:x.name.split('/').at(-1),startMs:Math.round(x.startTime),durationMs:Math.round(x.duration),
    encodedBytes:x.encodedBodySize,decodedBytes:x.decodedBodySize}));
  result.peakRuns=peakRuns;result.ok=true;result.stage='completed';
} catch(e) {result.failure=e instanceof Error?e.message:'cold.unconfirmed';}
finally {
  try{if(page?.recording)await page.finishSegment('control');}catch(_){}
  page?.releaseMic();page?.closePCM();try{await page?.detector?.close();}catch(_){}
  try{oscillator?.stop();oscillator?.disconnect();}catch(_){}
  for(const track of destination?.stream.getTracks()||[])track.stop();
  try{await generator?.close();}catch(_){}
  result.contextClosed=context?.state==='closed' && generator?.state==='closed';window.smokeResult=result;
}
"""

SILENT_GATE_BROWSER_JS = r"""
import {CallPage} from '/bot-call/call.js';
import {LocalVAD,SileroSignalModel} from '/bot-call/vad.js';
const wait=ms=>new Promise(r=>setTimeout(r,ms));
const check=(v,message)=>{if(!v)throw Error(message);};
const result={ok:false,stage:'start',phases:[],peakRuns:0};
let page,generator,destination,zero;
try {
  navigator.mediaDevices.getUserMedia=()=>{throw Error('gate.real_mic_forbidden');};
  generator=new AudioContext({sampleRate:48000});await generator.resume();
  destination=generator.createMediaStreamDestination();
  const bus=generator.createGain();bus.connect(destination);
  zero=new ConstantSourceNode(generator,{offset:0});zero.connect(bus);zero.start();
  const detector=new LocalVAD(window);let running=0;
  const run=SileroSignalModel.prototype.run;
  SileroSignalModel.prototype.run=async function(frame) {
    running++;result.peakRuns=Math.max(result.peakRuns,running);
    try{return await run.call(this,frame);}finally{running--;}
  };
  const env={storage:{getItem:()=>'',setItem(){}},location:{hash:'',pathname:'/bot-call/'},history,
    now:()=>performance.now(),Audio,URL,Blob,crypto,AbortController,MediaRecorder,AudioContext,
    mediaDevices:{getUserMedia:async()=>destination.stream.clone()},createVAD:()=>detector,
    setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window),isHidden:()=>false};
  page=new CallPage(env);page.started=true;page.reference='synthetic-local-only';
  page.server={audio_available:true,speech_available:true};page.refresh=async()=>{};
  const traffic=[];
  page.api=async(route,body,headers={})=>{
    traffic.push({route,values:body?.values});
    if(route==='metrics')return {recorded:true};
    const id=headers['X-Bot-Utterance-ID'];check(!!id,'gate.id');
    if(route==='audio-chunk')return {chunk_received:true,utterance_id:id,
      next_seq:Number(headers['X-Bot-Chunk-Seq'])+1,next_offset:Number(headers['X-Bot-Chunk-Offset'])+body.size};
    check(route==='audio' || route==='audio-commit','gate.route');
    return {audio_received:true,utterance_id:id};
  };
  result.stage='model';check(await page.modelPreparation,'gate.model');
  const response=await fetch('/bot-call/fixed-sample.wav');check(response.ok,'gate.fixture');
  const speech=await generator.decodeAudioData(await response.arrayBuffer());
  // Public speech, genuine 320 ms voiced excerpt, below vad-web's 400 ms.
  const short=generator.createBuffer(1,Math.round(generator.sampleRate*.32),generator.sampleRate);
  short.copyToChannel(speech.getChannelData(0).subarray(Math.round(generator.sampleRate*.5),Math.round(generator.sampleRate*.82)),0);
  for(const kind of ['silence','short-speech']) {
    result.stage=kind;page.muted=false;page.round=null;await page.listen();
    // Stabilize the synthetic stream's startup discontinuity before the
    // measured recorder, as in the existing VAD smoke. A real gap is unknown,
    // and must retain bytes; never weaken the measured coverage assertion.
    await wait(250);const warm=await page.finishSegment('fixture-warmup');
    warm.uploader?.abandon();page.clip=null;await page.listen();
    const r=page.recording;check(r?.vad?.valid,'gate.ready');
    const start=traffic.length;await wait(500);
    if(kind==='short-speech') {
      const source=generator.createBufferSource();source.buffer=short;source.connect(bus);
      const ended=new Promise(resolve=>{source.onended=resolve;});source.start();await ended;source.disconnect();
      await wait(100);
    }
    await page.toggleMute();
    const routes=traffic.slice(start).map(x=>x.route);
    const commits=routes.filter(x=>x==='audio' || x==='audio-commit').length;
    const phase={kind,reason:r.clip.reason,coverage:r.clip.quality.valid,vad:r.clip.vad,
      voiced:r.clip.voiced,energyVoiced:r.clip.energyVoiced,frames:r.clip.quality.frames,
      blobBytes:r.clip.blob.size,routes,commits,
      drops:traffic.slice(start).filter(x=>x.values?.client_silent_drop).map(x=>x.values)};
    result.phases.push(phase);
    check(r.clip.vad.valid && r.clip.quality.valid,'gate.coverage');
    check(r.stream.getTracks().every(t=>t.readyState==='ended'),'gate.track_release');
    if(kind==='silence') {
      check(!r.clip.vad.candidate && commits===0 && !routes.includes('audio-chunk'),'gate.silence_uploaded');
      check(page.clip===null && r.uploader.done && phase.drops.length===1 && phase.drops[0].client_silent_reason===1,'gate.discard');
    } else {
      check(r.clip.vad.candidate && commits===1 && phase.drops.length===0 && page.round,'gate.short_lost');
    }
  }
  result.ok=true;result.stage='completed';
} catch(e) {result.failure=e instanceof Error?e.message:'gate.unconfirmed';}
finally {
  try{if(page?.recording)await page.finishSegment('control');}catch(_){}
  page?.releaseMic();page?.closePCM();try{await page?.detector?.close();}catch(_){}
  try{zero?.stop();zero?.disconnect();}catch(_){}
  for(const track of destination?.stream.getTracks()||[])track.stop();
  try{await generator?.close();}catch(_){}
  result.contextClosed=generator?.state==='closed' && !page?.audioContext;
  window.smokeResult=result;
}
"""


DRIVER = r"""
const fs=require('node:fs'),path=require('node:path'),cp=require('node:child_process');
const delay=ms=>new Promise(r=>setTimeout(r,ms));
const origin=process.argv[1];
const chrome=process.argv[2],repeats=Number(process.argv[3]),cold=process.argv[5]==='cold';
let child,browser,page,stage="preflight",observed;
const profile=process.argv[4];
async function connect(url) {
  const socket=new WebSocket(url);await new Promise((resolve,reject)=>{socket.onopen=resolve;socket.onerror=reject;});
  let id=0;const pending=new Map(),events=[];
  socket.onmessage=e=>{const m=JSON.parse(e.data);if(m.id){const p=pending.get(m.id);pending.delete(m.id);p?.(m);}else events.push(m);};
  return {events,socket,async send(method,params={}) {const n=++id;return await new Promise((resolve,reject)=>{
    const timer=setTimeout(()=>{pending.delete(n);reject(new Error('cdp.timeout'));},10000);
    pending.set(n,m=>{clearTimeout(timer);m.error?reject(new Error('cdp.failed')):resolve(m.result);});socket.send(JSON.stringify({id:n,method,params}));
  });}};
}
const watchdog=setTimeout(()=>{
  if(child?.pid){try{child.kill('SIGKILL');}catch(_){} }
  if(profile){try{fs.rmSync(profile,{recursive:true,force:true,maxRetries:10,retryDelay:100});}catch(_){} }
  console.error('LOCAL_VAD_SMOKE_TIMEOUT');process.exit(1);
},90000*repeats);watchdog.unref();
(async()=>{
  // Verify WSL loopback before starting the sole browser process.
  if(!(await fetch(origin+'/smoke/',{signal:AbortSignal.timeout(5000)})).ok)throw new Error('loopback.unavailable');
  stage="executable";fs.accessSync(chrome);fs.accessSync(profile);
  stage='launch';child=cp.spawn(chrome,['--headless=new','--mute-audio','--no-first-run','--no-default-browser-check',
    '--disable-background-networking','--disable-component-update','--disable-sync','--disable-breakpad',
    '--disable-extensions','--disable-features=MediaRouter,AutofillAiWalletPrivatePasses','--no-proxy-server','--deny-permission-prompts',
    '--autoplay-policy=no-user-gesture-required','--remote-debugging-address=127.0.0.1','--remote-debugging-port=0',
    '--host-resolver-rules=MAP * 0.0.0.0, EXCLUDE localhost, EXCLUDE 127.0.0.1',
    '--user-data-dir='+profile,'about:blank'],{stdio:'ignore'});
  const portFile=path.join(profile,'DevToolsActivePort');
  for(let i=0;i<150&&!fs.existsSync(portFile);i++)await delay(100);
  if(!fs.existsSync(portFile))throw new Error('chrome.not_ready');
  const [port,endpoint]=fs.readFileSync(portFile,'utf8').trim().split(/\r?\n/);
  stage='cdp';browser=await connect('ws://127.0.0.1:'+port+endpoint);
  const version=await browser.send('Browser.getVersion');
  for(let iteration=1;iteration<=repeats;iteration++) {
  const target=await browser.send('Target.createTarget',{url:'about:blank'});
  const list=await (await fetch('http://127.0.0.1:'+port+'/json/list')).json();
  page=await connect(list.find(x=>x.id===target.targetId).webSocketDebuggerUrl);
  await page.send('Network.enable');await page.send('Runtime.enable');await page.send('Page.enable');
  if(cold) {
    await page.send('Network.setCacheDisabled',{cacheDisabled:true});
    await page.send('Network.emulateNetworkConditions',{offline:false,latency:150,downloadThroughput:256*1024,uploadThroughput:256*1024});
    await page.send('Emulation.setCPUThrottlingRate',{rate:6});
  }
  stage='page';await page.send('Page.navigate',{url:origin+'/smoke/'});
  let result;
  for(let i=0;i<600;i++) {
    const value=await page.send('Runtime.evaluate',{expression:'window.smokeResult || null',returnByValue:true});
    result=value.result?.value;if(result)break;await delay(100);
  }
  observed=result;const urls=page.events.filter(e=>e.method==='Network.requestWillBeSent').map(e=>e.params.request.url);
  const external=urls.filter(u=>!u.startsWith(origin+'/'));
  const wasmRequests=urls.filter(u=>u.endsWith('.wasm')).length,modelRequests=urls.filter(u=>u.endsWith('.onnx')).length;
  if(!result?.ok||external.length||result.peakRuns!==1||!result.contextClosed||wasmRequests!==1||modelRequests!==1)throw new Error('smoke.unconfirmed');
  console.log(JSON.stringify({iteration,browser:version.product,...result,externalRequests:external.length,
    wasmRequests:urls.filter(u=>u.endsWith('.wasm')).length,modelRequests:urls.filter(u=>u.endsWith('.onnx')).length}));
  page.socket.close();page=null;await browser.send('Target.closeTarget',{targetId:target.targetId});
  }
})().catch(()=>{console.error(JSON.stringify({failure:'LOCAL_VAD_SMOKE_FAILED',stage,launched:!!child,result:observed||null}));process.exitCode=1;}).finally(async()=>{
  try{await browser?.send('Browser.close');}catch(_){}
  page?.socket.close();browser?.socket.close();
  if(child) {
    for(let i=0;i<50&&child.exitCode===null;i++)await delay(100);
    try{child.kill('SIGKILL');}catch(_){}
    if(child.exitCode===null)await new Promise(resolve=>child.once('exit',resolve));
  }
  if(profile)fs.rmSync(profile,{recursive:true,force:true,maxRetries:30,retryDelay:100});
  clearTimeout(watchdog);console.log('OWN_BROWSER_PROFILE_CLEANED');
});
"""


def browser_executable(value):
    path = Path(value).expanduser().resolve()
    try:
        with path.open("rb") as executable:
            if executable.read(4) == b"\x7fELF" and os.access(path, os.X_OK):
                return path
    except OSError:
        pass
    raise argparse.ArgumentTypeError(
        f"Set BOTCALL_BROWSER or --browser to a Linux Chromium executable: {path}"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture-mode", choices=("auto", "legacy"), default="auto")
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument(
        "--cold-start",
        action="store_true",
        help="real CallPage: cache off, 256 KiB/s, CPU x6, no graph prewarm",
    )
    parser.add_argument("--browser", type=browser_executable, default=str(BROWSER))
    parser.add_argument(
        "--cold-overlap",
        action="store_true",
        help="schedule first voice across real ORT initialization, same cold limits",
    )
    parser.add_argument(
        "--silent-gate", action="store_true", help="real mute: silence versus short public speech"
    )
    args = parser.parse_args()
    args.cold_start = args.cold_start or args.cold_overlap
    if not 1 <= args.repeat <= 100:
        parser.error("--repeat must be 1..100")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            host = f"127.0.0.1:{self.server.server_port}"
            if self.headers.get("Host") != host:
                self.send_error(403)
                return
            custom = self.path in {"/smoke/", "/smoke.js"}
            route = "/bot-call/" if custom else self.path
            if route not in self.server.app._assets:
                self.send_error(404)
                return
            messages = []

            async def request():
                async def receive():
                    return {"type": "http.request", "body": b"", "more_body": False}

                async def send(message):
                    messages.append(message)

                await self.server.app(
                    {
                        "type": "http",
                        "scheme": "https",
                        "method": "GET",
                        "path": route,
                        "query_string": b"",
                        "headers": [
                            (b"host", host.encode()),
                            (b"accept-encoding", self.headers.get("Accept-Encoding", "").encode()),
                        ],
                    },
                    receive,
                    send,
                )

            asyncio.run(request())
            body = messages[1]["body"]
            headers = dict(messages[0]["headers"])
            if custom:
                body = (
                    b'<!doctype html><html><head><script type="module" src="/smoke.js"></script></head><body>Muted local VAD smoke</body></html>'
                    if self.path == "/smoke/"
                    else (
                        SILENT_GATE_BROWSER_JS
                        if args.silent_gate
                        else (
                            COLD_BROWSER_JS.replace("const overlap=false;", "const overlap=true;")
                            if args.cold_overlap
                            else COLD_BROWSER_JS
                        )
                        if args.cold_start
                        else BROWSER_JS
                    ).encode()
                )
                headers[b"content-type"] = (
                    b"text/html" if self.path == "/smoke/" else b"text/javascript"
                )
            self.send_response(messages[0]["status"])
            for key, value in headers.items():
                if key != b"content-length":
                    self.send_header(key.decode(), value.decode())
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        server.app = CallWebApp(None, origin=f"https://127.0.0.1:{server.server_port}")
        # Change the uncompressed asset before ASGI computes/caches gzip.
        # Replacing compressed response bytes silently left legacy tests in auto.
        from bot_call.web import _Static

        script = server.app._assets["/bot-call/call.js"]
        old = b'captureMode:"auto"'
        if old in script.body:
            body = script.body.replace(old, ('captureMode:"' + args.capture_mode + '"').encode())
            server.app._assets["/bot-call/call.js"] = _Static(body, script.content_type)

        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with TemporaryDirectory(prefix="bot-call-vad-") as profile:
                process = subprocess.Popen(
                    [
                        node_runtime(),
                        "-e",
                        DRIVER,
                        f"http://127.0.0.1:{server.server_port}",
                        str(args.browser),
                        str(args.repeat),
                        profile,
                        "cold" if args.cold_start else "warm",
                    ],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    start_new_session=True,
                )
                try:
                    stdout, stderr = process.communicate(
                        timeout=globals().get("PROCESS_TIMEOUT", 100) * args.repeat
                    )
                finally:
                    # Includes renderer descendants if the driver exits or times out.
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait(timeout=5)
                print(stdout, end="", flush=True)
                if process.returncode:
                    print(stderr, end="", file=sys.stderr)
                    raise SystemExit(1)
        finally:
            server.shutdown()
            thread.join()


if __name__ == "__main__":
    main()
