"""One Linux headless browser, synthetic stream only; original observer/worklet."""

import check_local_vad_browser as smoke
import json
from bot_call.observability import ECHO_METRICS

# This scenario intentionally doesn't load or download an inference model.
smoke.DRIVER = smoke.DRIVER.replace("modelRequests!==1", "modelRequests!==0").replace(
    "result.peakRuns!==1||", ""
)
smoke.DRIVER = smoke.DRIVER.replace("wasmRequests!==1", "wasmRequests!==0")
smoke.BROWSER_JS = r"""
import {CallPage} from '/bot-call/call.js';
import {EchoProbe} from '/bot-call/echo.js';
const wait=ms=>new Promise(r=>setTimeout(r,ms)),check=(v,m)=>{if(!v)throw Error(m);};
const result={ok:false,stage:'start',cases:[],peakRuns:1};
const limits=ECHO_LIMITS;
let page,context,oscillator,gain,destination;
try{
  navigator.mediaDevices.getUserMedia=()=>{throw Error('real-mic-forbidden');};
  context=new AudioContext({sampleRate:48000});await context.resume();
  destination=context.createMediaStreamDestination();
  oscillator=context.createOscillator();oscillator.frequency.value=220;
  gain=context.createGain();gain.gain.value=.1;oscillator.connect(gain);gain.connect(destination);oscillator.start();
  const traffic=[];
  const env={probeWindow:window,storage:{getItem:()=>'',setItem(){}},location:{hash:'',pathname:'/bot-call/'},history,
    now:()=>performance.now(),Audio,URL,Blob,crypto,AbortController,MediaRecorder,AudioContext,
    mediaDevices:{getSupportedConstraints:()=>({echoCancellation:true})},
    setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window),isHidden:()=>false};
  page=new CallPage(env);page.started=true;page.reference='synthetic';
  page.api=async(route,body)=>{traffic.push({route,body});return {recorded:true};};
  for(const mode of ['echo','double-talk','mute','hangup','background']){
    result.stage=mode;
    page.audioContext=context;page.stream=destination.stream.clone();page.muted=false;page.ending=false;page.suspended=false;
    page.detector=null;page.echo=new EchoProbe(page);await page.echo.ready;await wait(350);
    const probe=page.echo;check(!probe.closed&&probe.base.rows.length>0,'baseline');
    // Warm synthetic probability provider; genuine samples still come from worklet.
    if(mode==='echo'||mode==='double-talk')probe.model={run:async samples=>Math.sqrt(samples.reduce((n,v)=>n+v*v,0)/512)>.1?.9:.1};
    const speech={};
    probe.path(speech,'pcm',context.currentTime+.08);
    // Schedule both digital output reference and its synthetic input envelope.
    const start=context.currentTime+.1,seconds=3.1,buffer=context.createBuffer(1,Math.round(24000*seconds),24000),samples=buffer.getChannelData(0);
    for(let i=0;i<samples.length;i++){
      const tick=Math.floor(i/768),level=.015+.08*(.5+.5*Math.sin(tick*1.7));
      samples[i]=level*Math.sin(i*2*Math.PI*220/24000);
    }
    const output=context.createBufferSource();output.buffer=buffer;output.connect(context.destination);output.start(start);
    probe.scheduled(samples,start,24000);
    for(let i=0;i<95;i++){
      const t=start+i*.032,level=.015+.08*(.5+.5*Math.sin(i*1.7));
      gain.gain.setValueAtTime(mode==='double-talk'&&i>35&&i<60?.25:level,t+.096);
    }
    await wait(3200);
    if(mode==='echo'||mode==='double-talk'){
      probe.end(speech);probe.path({},'mp3');await wait(300);await probe.stop();
      const v=probe.values;
      check(v.client_echo_pcm_ms>2000&&v.client_echo_mp3_ms>0,'path coverage');
      check(v.client_echo_pcm_vad_ms>0,'VAD coverage');
      check(v.client_echo_mp3_corr_q===undefined&&v.client_echo_mp3_ref_p90===undefined,'MP3 unknown');
      check(v.client_echo_route===0&&!(v.client_echo_flags&192),'inferred control label');
      check(mode==='echo'?v.client_echo_pcm_vad_65_ms===0:v.client_echo_pcm_vad_65_ms>=160,'double-talk signal');
      check(v.client_echo_pcm_corr_q>=0&&v.client_echo_pcm_corr_q<=1000&&v.client_echo_pcm_lag_ms<=512,'scheduled reference');
    }else{
      // Lifecycle uses original page controls; fake only server and normal capture.
      page.finishSegment=async()=>{};page.stopHeartbeat=()=>{};page.stopPush=()=>{};page.stopSpeaking=async()=>{};page.closePCM=()=>{};
      const originalRelease=page.releaseAnalysis;page.releaseAnalysis=()=>{};
      if(mode==='mute')await page.toggleMute();
      else if(mode==='background')await page.suspend();
      else {page.ending=true;page.releaseMic();}
      page.releaseAnalysis=originalRelease;
      check(probe.closed&&probe.stream.getTracks().every(t=>t.readyState==='ended'),'control release');
      check(probe.values.client_echo_pcm_vad_ms===undefined,'cold unknown');
    }
    check(!page.recording&&!page.clip&&!page.round,'probe changed call flow');
    check(probe.stream.getTracks().every(t=>!t.enabled||t.readyState==='ended'),'track active');
    const reports=traffic.filter(t=>t.body?.generation_id?.startsWith('page:echo:'));
    check(reports.length===result.cases.length+1,'one report per probe');
    check(Object.entries(probe.values).every(([k,v])=>Object.hasOwn(limits,k)&&Number.isSafeInteger(v)&&v>=0&&v<=limits[k]),'metric whitelist/range');
    check(JSON.stringify(reports.at(-1).body).length<4096,'payload bound');
    result.cases.push({mode,values:probe.values});
    for(const t of page.stream?.getTracks()||[])t.stop();page.stream=null;
  }
  result.stage='player-pause';
  page.audioContext=context;page.stream=destination.stream.clone();page.muted=false;page.ending=false;page.suspended=false;
  page.echo=new EchoProbe(page);await page.echo.ready;
  const audio=env.Audio,play=page.playMedia;
  env.Audio=class {};page.playMedia=()=>{};
  const speech={echoPath:'mp3'};page.speech=speech;page.preparePlayer(speech,'synthetic-only');
  speech.media.onwaiting();check(!page.echo.closed,'initial buffering stopped probe');
  speech.playing=true;speech.media.onpause();check(page.echo.closed,'player pause did not stop probe');
  env.Audio=audio;page.playMedia=play;page.speech=null;
  for(const t of page.stream.getTracks())t.stop();page.stream=null;
  check(traffic.every(t=>t.route==='metrics'),'audio uploaded');
  // Exercise the real recorder-stop handoff and next capture, without sendClip.
  result.stage='recording-handoff';
  page.finishSegment=CallPage.prototype.finishSegment.bind(page);
  page.stopSpeaking=CallPage.prototype.stopSpeaking.bind(page);
  page.muted=false;page.suspended=false;page.ending=false;page.echo=undefined;page.audioContext=null;
  page.server={audio_available:true,speech_available:true};
  env.mediaDevices.getUserMedia=async()=>destination.stream.clone();
  await page.listen();check(page.recording?.recorder.state==='recording','normal capture');
  await wait(500);await page.finishSegment('manual');
  check(page.echo&&!page.echo.closed&&!page.recording,'stop handoff');
  const held=page.echo;await held.ready;await wait(200);
  check(page.stream.getTracks().every(t=>t.enabled),'retained authorized track');
  page.clip=null;await page.listen();
  check(held.closed&&page.recording?.recorder.state==='recording','next capture');
  const r=page.recording;await page.finishSegment('control');r.uploader?.abandon();page.clip=null;
  page.releaseMic();page.closePCM();check(!page.stream,'capture release');
  result.stage='recurrent-windows';
  page.audioContext=context;page.stream=destination.stream.clone();page.echoTurns=0;
  let warmed=0;
  const warm={cachedModel:{reset(){},run:async()=>.8},prepareModel:async()=>{warmed++;return true;}};
  page.detector=warm;page.echo=new EchoProbe(page,true);await page.echo.ready;await wait(120);
  const before=traffic.length,round={id:crypto.randomUUID()};
  for(const speech of [{round,seq:1},{round,seq:2},{round:{id:crypto.randomUUID()},seq:1}]) {
    page.echo.path(speech,speech.seq===2?'mp3':'pcm');await wait(350);page.echo.end(speech);await wait(140);
  }
  const windows=traffic.slice(before).filter(t=>t.body?.values?.client_echo_window).map(t=>t.body);
  check(warmed===1&&windows.length===3,'per-sentence reports');
  check(windows.every(t=>t.values.client_echo_pcm_vad_ms>0||t.values.client_echo_mp3_vad_ms>0),'permanent VAD');
  check(windows[0].values.client_echo_turn===1&&windows[1].values.client_echo_sentence===2&&windows[2].values.client_echo_turn===2,'window ownership');
  check(new Set(windows.map(t=>t.generation_id)).size===3,'window dedup');
  result.recurrent=windows.map(t=>({gid:t.generation_id,values:t.values}));
  await page.echo.stop();page.detector=null;for(const t of page.stream.getTracks())t.stop();page.stream=null;
  result.stage='delayed-inference';
  page.audioContext=context;page.stream=destination.stream.clone();page.echo=new EchoProbe(page);await page.echo.ready;await wait(180);
  let resolveInference,entered;
  const running=new Promise(resolve=>{entered=resolve;});
  page.echo.model={run:()=>{entered();return new Promise(resolve=>{resolveInference=resolve;});}};
  page.echo.path({},'pcm');await running;
  const heldBusy=page.echo,stopAt=performance.now();await heldBusy.stop();
  check(performance.now()-stopAt<350&&heldBusy.pending>0,'unbounded inference wait');
  const create=env.createVAD;let began=false;
  env.createVAD=()=>({ready:()=>true,begin:async()=>{began=true;throw Error('busy model reset');}});
  page.detector=env.createVAD();
  await page.listen();
  check(page.recording?.recorder.state==='recording'&&!page.recording.vad&&!began,'busy inference blocked normal capture');
  resolveInference(.9);await heldBusy.task;await page.finishSegment('control');
  page.clip?.uploader?.abandon();page.clip=null;
  page.detector=null;env.createVAD=create;page.pcmContext=context;page.releaseMic();page.pcmContext=null;
  // Metrics authentication failure is isolated from the normal page session.
  page.locked=false;page.ending=false;page.e.fetch=async()=>({status:403,ok:false});let failed=false;
  try{await CallPage.prototype.api.call(page,'metrics',{generation_id:'page:echo:'+crypto.randomUUID(),values:{client_echo_schema:1}});}catch(_){failed=true;}
  check(failed&&!page.locked,'metrics auth locked normal call');
  result.ok=true;
}catch(e){result.failure=e.message;}
finally{
  try{await page?.echo?.stop(true);}catch(_){}
  for(const t of destination?.stream.getTracks()||[])t.stop();
  try{oscillator?.stop();oscillator?.disconnect();gain?.disconnect();await context?.close();}catch(_){}
  result.contextClosed=context?.state==='closed';window.smokeResult=result;
}
"""

smoke.BROWSER_JS = smoke.BROWSER_JS.replace("ECHO_LIMITS", json.dumps(ECHO_METRICS))

if __name__ == "__main__":
    smoke.main()
