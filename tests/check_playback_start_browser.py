"""Input before shared output, real permission denial and sticky activation.

Uses the existing serial Linux browser harness with a fake input device, a real
trusted click, and the document activation autoplay policy (no autoplay bypass).
"""

from pathlib import Path
from tempfile import TemporaryDirectory
import wave

import check_local_vad_browser as smoke

smoke.DRIVER = (
    smoke.DRIVER.replace("result.peakRuns!==1", "false")
    .replace("wasmRequests!==1||modelRequests!==1", "wasmRequests!==0||modelRequests!==0")
    .replace(
        "--autoplay-policy=no-user-gesture-required",
        "--autoplay-policy=document-user-activation-required",
    )
    .replace(
        "'--deny-permission-prompts',",
        "'--deny-permission-prompts','--use-fake-device-for-media-stream',",
    )
    .replace(
        "let result;\n  for(let i=0;i<600;i++)",
        """async function flag(name) {
    for(let i=0;i<200;i++){
      const v=await page.send('Runtime.evaluate',{expression:'!!window.'+name,returnByValue:true});
      if(v.result?.value)return;await delay(100);
    }throw Error('startup.flag.'+name);
  }
  async function click() {
    await page.send('Input.dispatchMouseEvent',{type:'mousePressed',x:30,y:30,button:'left',clickCount:1});
    await page.send('Input.dispatchMouseEvent',{type:'mouseReleased',x:30,y:30,button:'left',clickCount:1});
  }
  await flag('startupReady');await click();await flag('deniedReady');
  await browser.send('Browser.grantPermissions',{permissions:['audioCapture'],origin});
  await click();
  let result;
  for(let i=0;i<600;i++)""",
    )
)
smoke.BROWSER_JS = r"""
import {CallPage} from '/bot-call/call.js';
const wait=ms=>new Promise(r=>setTimeout(r,ms)),check=(v,s)=>{if(!v)throw Error(s);};
async function until(fn,ms=6000){const end=performance.now()+ms;while(!fn()&&performance.now()<end)await wait(10);check(fn(),'deadline:'+result.stage);}
const result={ok:false,cases:[]},pages=[],contexts=[];
const button=document.createElement('button');button.textContent='Start';button.style='position:fixed;left:0;top:0;width:120px;height:80px';document.body.append(button);
function clicked(){return new Promise(resolve=>button.onclick=resolve);}
function fixture(mode) {
  const events=[],packets=[];let inputReady=false,constructors=0;
  class Output extends AudioContext {
    constructor(){check(inputReady,'output before input');super();constructors++;contexts.push(this);events.push('context');
      const source=this.createMediaStreamSource.bind(this);
      this.createMediaStreamSource=s=>{events.push('graph');return source(s);};
    }
  }
  const env={storage:{getItem:()=>'',setItem(){}},location:{hash:'',pathname:'/bot-call/',origin:location.origin},history,
    now:()=>performance.now(),Audio,URL,Blob,crypto,AbortController,AudioContext:Output,atob:atob.bind(window),probeWindow:window,
    MediaRecorder,mediaDevices:{getSupportedConstraints:()=>navigator.mediaDevices.getSupportedConstraints(),
      getUserMedia:async options=>{events.push('request');const s=await navigator.mediaDevices.getUserMedia(options);
        check(options.audio.echoCancellation.ideal===true,'AEC changed');inputReady=true;events.push('input');return s;}},
    setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window)};
  const page=new CallPage(env,()=>{},{captureMode:mode,chunkUpload:false});pages.push(page);
  page.connect=async()=>{await wait(mode==='auto'?5200:0);page.server={audio_available:true,speech_available:true};return page.server;};
  page.api=async(route,body)=>{packets.push({route,body});return {recorded:true};};
  page.observe=()=>{};page.sendMetrics=()=>{};page.afterSpeech=async()=>{};
  const sample=page.sampleRecording.bind(page);if(mode==="auto")page.sampleRecording=()=>{};
  return {page,events,packets,sample,get constructors(){return constructors;}};
}
try {
  let f=fixture('legacy');window.startupReady=true;await clicked();await f.page.start();
  check(!f.page.stream&&!f.page.recording&&!f.page.pcmContext&&f.constructors===0,'permission denial leaked graph');
  check(f.page.basicCapture===true&&f.page.message==='麦克风未能开启','permission denial lost recovery');
  f.page.testVoice();check(f.page.speech?.local,'permission denial blocked file sample');
  await until(()=>f.page.speech?.playing);await f.page.stopSpeaking(false);
  result.cases.push({mode:'denied',contexts:f.constructors,fileFallback:true});window.deniedReady=true;await clicked();
  for(const mode of ['auto','legacy']) {
    result.stage=mode;f=fixture(mode);const p=f.page,started=performance.now();await p.start();
    check(p.recording&&p.audioContext===p.pcmContext&&p.audioContext.state==='running','shared running context');
    check(f.events.indexOf('input')<f.events.indexOf('context')&&f.events.indexOf('context')<f.events.indexOf('graph'),'startup order');
    check(f.constructors===1&&p.playbackContextCreatedAfterInput===1,'startup context rebuilt');
    if(mode==='auto')check(!navigator.userActivation.isActive&&navigator.userActivation.hasBeenActive,'transient activation not expired');
    await wait(700);const r=p.recording;
    if(mode==='auto') {
      check(p.capture&&!r.recorder.begun,'idle capture encoded');
      p.sampleRecording=f.sample;r.fallbackVoice=true;await until(()=>r.recorder.started);
      check(r.recorder.prefixSamples>=23040&&r.recorder.prefixSamples<=24000,'first segment prefix');
      result.prefixSamples=r.recorder.prefixSamples;
    }
    await wait(200);const clip=await p.finishSegment('manual');
    check(clip.blob.size>0,'first segment missing');
    p.clip=null;p.round={id:'startup-'+mode,endedAt:performance.now()};p.playbackPaused=false;
    await p.echo?.ready;await wait(150);
    const context=p.pcmContext,at=performance.now();
    for(let i=0;i<8;i++)p.acceptPCM({generation_id:'reply:'+p.round.id,seq:1,index:i,rate:24000,format:'s16le',data:btoa('\x01\x00'.repeat(960))});
    await until(()=>p.speech?.playing);const firstToPlayingMs=Math.round(performance.now()-at);
    const values=f.packets.find(x=>x.body?.values?.client_playback_context_created_after_input!==undefined)?.body.values;
    check(values?.client_playback_context_created_after_input===1,'missing startup diagnostic');
    check(values.client_playback_output_us>=0,'missing first outputLatency');
    check(values.client_playback_lead_ms===(mode==='auto'?180:0),'lead changed');
    if(mode==='auto') {
      await p.echo?.ready;await wait(400);
      check(p.echo&&!p.echo.closed&&p.echo.refs.size>0,'echo reference missing');
    }
    result.cases.push({mode,events:f.events,contexts:f.constructors,startMs:Math.round(at-started),
      firstToPlayingMs,outputUs:values.client_playback_output_us,leadMs:values.client_playback_lead_ms,
      shared:true,sticky:navigator.userActivation.hasBeenActive,transient:navigator.userActivation.isActive});
    p.releasePlayback(p.speech);p.speech=null;p.round=null;await p.echo?.stop();
    p.releaseMic();p.closePCM();await wait(40);check(context.state==='closed','context cleanup');
  }
  result.ok=true;
}catch(e){result.error=e.message;}
finally {
  for(const p of pages){if(p.speech)p.releasePlayback(p.speech);await p.echo?.stop();p.releaseMic();p.closePCM();p.stopHeartbeat();p.e.clearTimeout(p.poll);}
  for(const c of contexts)if(c.state!=='closed')await c.close();
  result.contextClosed=contexts.every(c=>c.state==='closed');window.smokeResult=result;
}
"""

if __name__ == "__main__":
    with TemporaryDirectory(prefix="bot-call-input-order-") as directory:
        source = Path(directory) / "silence.wav"
        with wave.open(str(source), "wb") as audio:
            audio.setnchannels(1)
            audio.setsampwidth(2)
            audio.setframerate(48000)
            audio.writeframes(bytes(48000 * 2 * 10))
        smoke.DRIVER = smoke.DRIVER.replace(
            "'--use-fake-device-for-media-stream',",
            "'--use-fake-device-for-media-stream','--use-file-for-fake-audio-capture="
            + str(source)
            + "',",
        )
        smoke.main()
