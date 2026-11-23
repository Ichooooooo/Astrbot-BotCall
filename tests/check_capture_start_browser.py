"""M7-P1d: real encoder delay/WAV recovery and foreground state-read faults."""

import json
import os
from pathlib import Path
import check_local_vad_browser as smoke

smoke.DRIVER = smoke.DRIVER.replace("result.peakRuns!==1", "false").replace(
    "wasmRequests!==1||modelRequests!==1", "wasmRequests!==0||modelRequests!==0"
)
smoke.BROWSER_JS = r"""
import {ContinuousCapture} from '/bot-call/capture.js';
import {CallPage,CapabilityProbe} from '/bot-call/call.js';
const wait=ms=>new Promise(r=>setTimeout(r,ms)),check=(v,s)=>{if(!v)throw Error(s);};
async function until(fn,ms=9000){const end=performance.now()+ms;while(!fn()&&performance.now()<end)await wait(10);check(fn(),'deadline:'+result.stage);}
const result={ok:false,cases:[],rounds:[]};let context,capture,osc,destination;
try {
  context=new AudioContext({sampleRate:48000});await context.resume();
  destination=context.createMediaStreamDestination();osc=context.createOscillator();osc.connect(destination);osc.start();

  // Real Worker/Worklet with bounded startup message delays. These isolate
  // network-delayed worker evaluation vs render/main-thread ACK starvation.
  for(const stage of [1,2,3]) {
    let fault=true;
    class StartupWorker extends Worker {
      postMessage(m,...args) {
        if(m.type==='check'&&fault&&stage===1) {
          this.startupDelay=setTimeout(()=>super.postMessage(m,...args),1800);return;
        }
        super.postMessage(m,...args);
      }
      terminate(){clearTimeout(this.startupDelay);super.terminate();}
    }
    class StartupNode extends AudioWorkletNode {
      constructor(...args) {
        super(...args);
        this.port.addEventListener('message',e=>{
          if(fault&&((stage===2&&e.data.type==='ready')||(stage===3&&e.data.type==='prefix')))e.stopImmediatePropagation();
        });
      }
    }
    const p=Object.assign(Object.create(CallPage.prototype),{
      audioContext:context,stream:destination.stream,epoch:0,callStartedAt:performance.now(),
      config:{captureMode:'auto',segmentMs:30000,rmsThreshold:.025},
      e:{now:()=>performance.now(),probeWindow:{Worker:StartupWorker,AudioWorkletNode:StartupNode,
        setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window)},MediaRecorder,Blob,crypto}
    });
    const attempt=async()=>{
      p.captureStartupDeferred=false;
      await p.prepareCapture(false,0);
      await p.prepareCapture(false,0,true);
    };
    await attempt();
    check(!p.capture&&!p.captureDisabled&&p.captureFailure===8,'startup fallback '+stage);
    check(p.captureStartupDiagnostics.client_capture_timeout_stage===stage,'timeout stage '+stage);
    check(!p.captureStartupRetries,'same segment retried');
    const legacy=new MediaRecorder(destination.stream);
    legacy.start();await wait(40);legacy.stop();
    fault=false;await attempt();
    check(p.capture&&!p.capture.closed&&p.capture.recordArmed&&!p.captureFailure,'startup recovery '+stage);
    check(p.captureStartupRetries===1&&Number.isFinite(p.captureStartupRecoveredMs),'recovery metrics');
    p.capture.close();p.capture=null;p.captureStartupPending=false;p.captureStartupRetries=0;p.captureStartupRecoveredMs=null;
    fault=true;
    for(let n=0;n<4;n++)await attempt();
    check(p.captureDisabled&&p.captureStartupRetries===3&&!p.capture,'retry limit '+stage);
    await attempt();check(p.captureStartupRetries===3,'retried after exhaustion');
    result.cases.push({startupTimeoutStage:stage,recovered:true,retriesExhausted:3});
  }
  let delay=0,stall=0,recoverFail=false,wavFail=false;
  class DelayedWorker extends Worker {
    postMessage(m,...args){
      if(m.type==='recover'&&recoverFail){queueMicrotask(()=>this.dispatchEvent(new MessageEvent('message',{data:{type:'error',id:m.id,code:4}})));return;}
      if(m.type==='pcm'&&stall){this.stallUntil ??= performance.now()+stall;setTimeout(()=>super.postMessage(m,...args),Math.max(0,this.stallUntil-performance.now()));return;}
      if(m.type==='start')this.delay=setTimeout(()=>super.postMessage(m,...args),delay);
      else super.postMessage(m,...args);
    }
    terminate(){clearTimeout(this.delay);super.terminate();}
  }
  const win={Worker:DelayedWorker,AudioWorkletNode,OfflineAudioContext:class extends OfflineAudioContext{constructor(...args){if(wavFail)throw Error('fixture.offline');super(...args);}},
    setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window)};
  const owner={audioContext:context,stream:destination.stream,config:{rmsThreshold:.025,segmentMs:30000,maxAudioBytes:2097152},
    e:{probeWindow:win,MediaRecorder,Blob,crypto,now:()=>performance.now()}};
  capture=await ContinuousCapture.create(owner);
  for(const ms of [300,1000,2500,4000]) {
    result.stage='delay-'+ms;delay=ms;
    const r=capture.recorder(),parts=[];r.ondataavailable=e=>parts.push(e.data);
    const stopped=new Promise(resolve=>r.onstop=resolve);r.start();
    await until(()=>r.begun);
    await wait(ms===4000?3200:ms+160);
    if(ms===4000)check(r.failed===6&&!owner.captureDisabled,'startup bound or first retry');
    else check(r.started&&!r.failed&&r.peak<=8,'startup queue failed before bounded deadline');
    const stopAt=performance.now();r.stop();await stopped;
    const blob=new Blob(parts,{type:r.mimeType});
    if(ms===4000) {
      check(r.mimeType==='audio/webm;codecs=opus'&&r.metrics.client_capture_recovery_kind===3,'not offline Opus recovery');
      check(blob.size===r.metrics.client_capture_recovery_bytes && blob.size<r.samples/48*8,'Opus recovery bytes');
      check(performance.now()-stopAt<r.samples/48/4,'recovery waited near realtime');
    } else check(r.stats&&r.stats.metrics.client_capture_start_ms>=ms-20,'missing started timing');
    result.cases.push({delay:ms,failed:r.failed||0,startMs:r.metrics?.client_capture_start_ms,
      recoveryMs:r.metrics?.client_capture_recovery_ms,kind:r.metrics?.client_capture_recovery_kind,segmentMs:r.samples/48,bytes:blob.size});
  }
  result.stage='cold-throughput';delay=0;stall=850;owner.modelReady=false;
  {
    const r=capture.recorder();const stopped=new Promise(resolve=>r.onstop=resolve);
    r.start();await until(()=>r.started);owner.modelReady=true;owner.modelReadyAt=performance.now();await wait(1100);r.stop();await stopped;
    check(r.stats&&!r.failed&&r.peak<=8,'cold encoder throughput queue');
    check(r.stats.metrics.client_capture_model_overlap===1&&r.stats.metrics.client_capture_model_overlap_ms>0,'model overlap diagnostics');
    result.coldThroughput={samples:r.samples,metrics:r.stats.metrics};
  }
  stall=0;
  // Same call retries the queue failure and completes >=10 further rounds.
  delay=0;
  let probeWorker,probeTerminated=false;
  const probe=new CapabilityProbe({...window,setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window)},()=>{},true);
  probe.running=true;
  probeWorker={terminate(){probeTerminated=true;}};probe.workers.add(probeWorker);
  owner.capability=probe;
  let supportStarted=false,releaseSupport,diagnosticWorkers=0;
  const liveProbe=new CapabilityProbe({
    AudioEncoder:class {static isConfigSupported(){supportStarted=true;return new Promise(resolve=>{releaseSupport=()=>resolve({supported:true});});}},
    AudioData,AudioWorkletNode,MediaRecorder,Worker:class extends Worker{constructor(...args){diagnosticWorkers++;super(...args);}},
    OfflineAudioContext,navigator,isSecureContext:true,performance,
    setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window)},()=>{},true);
  liveProbe.run();await until(()=>supportStarted);liveProbe.yieldToCapture();releaseSupport();await liveProbe.task;
  check(liveProbe.cancelled&&diagnosticWorkers===0,'capability continued after capture priority');
  result.probeYielded=true;
  for(let n=0;n<12;n++) {
    result.stage='round-'+n;delay=[300,0,1000,0,2500,0][n%6];
    const r=capture.recorder(),parts=[];r.ondataavailable=e=>parts.push(e.data);
    const stopped=new Promise(resolve=>r.onstop=resolve);r.start();await until(()=>r.started);
    await wait(130);r.stop();await stopped;
    check(r.stats&&!r.failed&&!owner.captureDisabled&&r.peak<=8,'long call mode1/retry');
    if(n===0)check(probeTerminated&&probe.cancelled&&r.metrics.client_capture_start_probe===1,'probe not yielded on onset');
    result.rounds.push({n,startMs:r.metrics.client_capture_start_ms,mode:1,samples:r.samples});
  }
  // A pre-existing M5 recovery backlog belongs to the previous capture clock;
  // preserve its established 31s bound, not the new live-start 3s budget.
  result.stage='carry-backlog';delay=300;
  for(let n=0;n<250;n++)capture.carry.push(new Float32Array(960).fill(.02));
  {
    const r=capture.recorder(true);const stopped=new Promise(resolve=>r.onstop=resolve);
    r.start();r.stop();await stopped;
    check(r.stats&&!r.failed&&r.samples>=240000&&r.peak<=8,'pre-existing continuation backlog lost');
    result.carryBacklog={samples:r.samples,startMs:r.metrics.client_capture_start_ms,peak:r.peak};
  }
  for(const kind of [1,2]) {
    result.stage='recovery-fallback-'+kind;delay=0;recoverFail=true;wavFail=kind===2;
    const r=capture.recorder(),parts=[];r.ondataavailable=e=>parts.push(e.data);r.onfallback=()=>{parts.length=0;};
    const stopped=new Promise(resolve=>r.onstop=resolve);r.start();await until(()=>r.started);await wait(400);
    capture.fail(5);r.stop();await stopped;
    const blob=new Blob(parts,{type:r.mimeType});
    check(blob.size>0 && r.metrics.client_capture_recovery_kind===kind,'WAV/realtime fallback');
    if(kind===1){const data=new DataView(await blob.arrayBuffer());check(data.getUint32(24,true)===16000&&data.getUint16(22,true)===1,'WAV format');check((blob.size-44)/2===Math.ceil(r.samples/3),'WAV sample count');}
    check(blob.size===r.metrics.client_capture_recovery_bytes,'fallback byte metric');
    result.cases.push({kind,bytes:blob.size,ms:r.metrics.client_capture_recovery_ms});
    owner.captureMuxFailures=0;owner.captureDisabled=false;
  }
  recoverFail=false;wavFail=false;
  for(const code of [6,8]) {
    result.stage='retry-streak-'+code;delay=0;
    const r=capture.recorder();const stopped=new Promise(resolve=>r.onstop=resolve);
    r.start();await until(()=>r.started);capture.fail(code);r.stop();await stopped;
    check(owner.captureDisabled===(code===8),'retry streak did not disable only at second failure');
  }
  result.retryStreakDisabled=true;
  capture.close();capture=null;
  for(const mode of ['preflight-timeout','arm-timeout']) {
    result.stage=mode;
    const probeWindow={Worker:class extends Worker {
      postMessage(m,...args){if(mode==='preflight-timeout'&&m.type==='check')return;super.postMessage(m,...args);}
    },AudioWorkletNode:class extends AudioWorkletNode {
      constructor(...args){super(...args);const post=this.port.postMessage.bind(this.port);this.port.postMessage=m=>{if(mode!=='arm-timeout'||m.type!=='record')post(m);};}
    },OfflineAudioContext,setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window)};
    const page=new CallPage({probeWindow,storage:{getItem:()=>'',setItem(){}},location:{hash:'',pathname:'/bot-call/'},history,
      now:()=>performance.now(),Blob,crypto,AbortController,MediaRecorder,Audio,URL,
      setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window)});
    page.audioContext=context;page.stream=destination.stream;page.started=true;
    for(let attempt=0;attempt<=3;attempt++) {
      page.captureStartupDeferred=false;
      await page.prepareCapture(false,page.epoch,true);
      await page.prepareCapture(false,page.epoch,true);
      check(page.captureFailure===8&&(page.captureStartupRetries||0)===attempt&&page.captureDisabled===(attempt===3)&&!page.captureMuxFailures,'setup timeout retry/double count');
    }
    result.cases.push({kind:mode,retries:page.captureStartupRetries,disabled:page.captureDisabled});
    page.capture?.close();
  }
  // Real api()/refresh wrappers; hang both fetch and body independently. Each
  // control path owns an already acknowledged round; no audio is retransmitted.
  for(const kind of ['capture','control','series','visible-hang','body-hang','front-disconnect']) {
    result.stage=kind;
    let hidden=false,hang=0,requests=0,plays=0,ready=false,seenAt,aborts=0;
    const id='fault-'+kind;
    const state=()=>({audio:[{utterance_id:id,state:ready?'succeeded':'running'}],
      replies:ready?[{utterance_id:id,generation_id:'reply:'+id,state:'succeeded'}]:[],
      turns:[],complete:false,audio_available:true,speech_available:true});
    const env={storage:{getItem:()=>'',setItem(){}},location:{hash:'',pathname:'/bot-call/',origin:location.origin},history,
      now:()=>performance.now(),Audio,URL,Blob,crypto,AbortController,MediaRecorder,isHidden:()=>hidden,
      setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window),
      fetch:async(url,init)=>{
        if(!url.endsWith('/state'))return {ok:true,status:200,json:async()=>({recorded:true})};
        requests++;init.signal.addEventListener('abort',()=>aborts++);
        if(hang-->0) {
          if(kind==='body-hang'&&ready)return {ok:true,status:200,json:()=>new Promise(()=>{})};
          return new Promise(()=>{});
        }
        return {ok:true,status:200,json:async()=>state()};
      }};
    const page=new CallPage(env);page.reference='synthetic';page.started=true;page.callStartedAt=performance.now();
    page.server=state();page.round={id,endedAt:performance.now(),uploadAt:performance.now(),...(kind==='series'?{seriesId:id}:{})};
    page.noteProgress=function(s){CallPage.prototype.noteProgress.call(this,s);if(s.replies.length)seenAt ??= performance.now();};
    page.reconcileRound=async()=>{if(page.server.replies.length)plays++;};
    if(kind==='front-disconnect') {
      page.pushRecovering=true;hang=1;page.recoverPushState();await wait(50);ready=true;
      const readyAt=performance.now();await until(()=>seenAt);
      check(seenAt-readyAt<1000,'foreground disconnect missed ready+1s');
      result.cases.push({kind,readyToSeenMs:seenAt-readyAt,requests,aborts});
    } else {
      // Hidden suspends capture/reading under the original control path.
      hidden=true;await page.hidden();await wait(80);ready=true;
      // Simulate a background short GET stuck until visibility returns.
      hang=1;void page.refresh().catch(()=>{});await wait(30);
      hidden=false;const visibleAt=performance.now();
      if(['visible-hang','body-hang'].includes(kind))hang=1;
      await page.foreground();
      await until(()=>seenAt);
      check(seenAt-visibleAt<1000&&plays>0,'foreground result/playback missing');
      check(page.captureResumeRequired&&!page.recording,'foreground reopened microphone');
      const metrics=page.round.foregroundResultMs;
      check(metrics>=0&&metrics<1000,'foreground timing');
      result.cases.push({kind,visibleToSeenMs:seenAt-visibleAt,requests,aborts,plays,metrics});
    }
    page.stopPush();clearTimeout(page.poll);page.cancelStateRead();
  }
  result.ok=true;
}catch(e){result.error=e.message;}
finally {
  capture?.close();try{osc?.stop();}catch(_){}
  for(const t of destination?.stream.getTracks()||[])t.stop();
  await context?.close();result.contextClosed=context?.state==='closed';window.smokeResult=result;
}
"""
if __name__ == "__main__":
    directory = Path(os.environ.get("BOT_CALL_START_EVIDENCE", "/tmp/bot-call-start-check"))
    directory.mkdir(parents=True, exist_ok=True)
    smoke.DRIVER = smoke.DRIVER.replace(
        "observed=result;const urls=",
        "if(result)fs.writeFileSync("
        + json.dumps(str(directory / "browser-result.json"))
        + ",JSON.stringify(result,null,2));observed=result;const urls=",
    )
    smoke.main()
