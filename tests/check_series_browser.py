"""Continuous synthetic speech, real WSL Linux MediaRecorder/VAD; loopback only."""

import check_local_vad_browser as smoke

smoke.BROWSER_JS = r"""
import {CallPage} from '/bot-call/call.js';
import {LocalVAD,SileroSignalModel} from '/bot-call/vad.js';
const wait=ms=>new Promise(r=>setTimeout(r,ms));
const check=(value,message)=>{if(!value)throw Error(message);};
async function until(fn,ms=12000) {const end=performance.now()+ms;while(!fn()&&performance.now()<end)await wait(20);check(fn(),'deadline');}
const result={ok:false,stage:'prepare',peakRuns:0,segments:[],handoffs:[],nextBeforeAck:false};
let page,generator,source,context,detector;
try {
  navigator.mediaDevices.getUserMedia=()=>{throw Error('real_mic_forbidden');};
  generator=new AudioContext({sampleRate:48000});await generator.resume();
  const bytes=await (await fetch('/bot-call/fixed-sample.wav')).arrayBuffer();
  source=generator.createBufferSource();source.buffer=await generator.decodeAudioData(bytes);source.loop=true;
  const bus=generator.createGain();source.connect(bus);source.start();
  detector=new LocalVAD(window);
  const phases=[];
  const env={storage:{getItem:()=>'',setItem(){}},location:{hash:'',pathname:'/bot-call/'},history,
    now:()=>performance.now(),Audio,URL,Blob,crypto,AbortController,MediaRecorder,AudioContext,
    mediaDevices:{getUserMedia:async()=>{const out=generator.createMediaStreamDestination();bus.connect(out);return out.stream;}},
    createVAD:()=>detector,setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window)};
  page=new CallPage(env,value=>phases.push(value.ui.phase),{segmentMs:2000,silenceMs:4000,vadRedemptionMs:4000});
  page.started=true;page.reference='synthetic-only';
  page.server={audio_available:true,speech_available:true,turns:[],audio:[],replies:[],series:[]};
  page.seriesCapability({seamless_series:{version:1,max_segments:6}});
  page.refresh=async()=>page.server;
  await page.modelPreparation;check(page.modelReady,'model');
  let running=0;const run=SileroSignalModel.prototype.run;
  SileroSignalModel.prototype.run=async function(frame){running++;result.peakRuns=Math.max(result.peakRuns,running);try{return await run.call(this,frame);}finally{running--;}};
  const staged=new Map(),committed=new Set();let generations=0;
  page.api=async(route,body,headers={})=>{
    if(route==='metrics')return {recorded:true};
    const id=headers['X-Bot-Utterance-ID'];check(id,'id');
    const data=new Uint8Array(await body.arrayBuffer()),old=staged.get(id)||new Uint8Array();
    if(route==='audio-chunk'){
      check(Number(headers['X-Bot-Chunk-Offset'])===old.length,'chunk_offset');
      const merged=new Uint8Array(old.length+data.length);merged.set(old);merged.set(data,old.length);staged.set(id,merged);
      return {chunk_received:true,utterance_id:id,next_seq:Number(headers['X-Bot-Chunk-Seq'])+1,next_offset:merged.length};
    }
    check(['audio-series','audio-series-commit'].includes(route),'series_route');
    const clip=page.series.queue[0],index=Number(headers['X-Bot-Series-Index']),final=headers['X-Bot-Series-Final']==='1';
    check(clip.id===id && index===result.segments.length+1 && !committed.has(id),'identity_order');
    const offset=route.endsWith('commit')?Number(headers['X-Bot-Tail-Offset']):0;
    const full=new Uint8Array(offset+data.length);full.set(old.subarray(0,offset));full.set(data,offset);
    const sha=[...new Uint8Array(await crypto.subtle.digest('SHA-256',full))].map(v=>v.toString(16).padStart(2,'0')).join('');
    check(full.length===clip.blob.size && sha===clip.sha && full.length>0,'complete_container');
    if(index===1) {await wait(600);result.nextBeforeAck=!!page.recording && page.recording.id!==id;check(result.nextBeforeAck,'record_waited_for_ack');}
    if(final)generations++;
    result.segments.push({captureMode:clip.metrics.client_capture_mode,index,final,bytes:full.length,route});
    result.handoffs.push(clip.metrics.client_segment_handoff_ms ?? null);
    committed.add(id);return {audio_received:true,utterance_id:id,series_id:headers['X-Bot-Series-ID'],series_index:index,series_final:final};
  };
  result.stage='record';await page.listen();context=page.audioContext;
  await until(()=>result.segments.length>=2 && page.recording && page.series.count===2);
  check(!phases.includes('thinking'),'thinking_before_final');check(generations===0,'early_generation');
  await wait(500);result.stage='final';await page.submitSegment();
  check(result.segments.every(s=>s.captureMode===(page.config.captureMode==='auto'?1:0)),'unexpected fallback');
  check(result.segments.length===3 && result.segments.filter(s=>s.final).length===1 && generations===1,'one_final');
  check(result.segments.slice(0,2).every(s=>!s.final),'prefix_final');
  check(page.presentation().phase==='thinking','final_state');result.stage='completed';result.ok=true;
} catch(error) {result.error=String(error.message).slice(0,100);}
finally {
  if(page?.recording)await page.finishSegment('control').catch(()=>{});
  page?.releaseMic();page?.closePCM();await detector?.close().catch(()=>{});
  try{source?.stop();}catch(_){}await generator?.close().catch(()=>{});
  result.contextClosed=context?.state==='closed';window.smokeResult=result;
}
"""

if __name__ == "__main__":
    smoke.main()
