"""Long capture and recorder fault probes; Linux browser, synthetic loopback only.

--baseline disables recovery assertions when probing pre-fix page sources.
"""

import sys
import check_local_vad_browser as smoke

BROWSER_JS = r"""
import {CallPage} from '/bot-call/call.js';
import {LocalVAD,SileroSignalModel} from '/bot-call/vad.js';
const wait=ms=>new Promise(r=>setTimeout(r,ms));
const check=(v,m)=>{if(!v)throw Error(m);};
const expectRecovery=true;
const result={ok:false,stage:'prepare',segments:[],faults:[],peakRuns:0};
let page,generator,source,context;
async function until(fn,ms=7000){const end=performance.now()+ms;while(!fn()&&performance.now()<end)await wait(20);check(fn(),'deadline:'+result.stage);}
try {
  navigator.mediaDevices.getUserMedia=()=>{throw Error('real_mic_forbidden');};
  generator=new AudioContext({sampleRate:48000});await generator.resume();
  const response=await fetch('/bot-call/fixed-sample.wav');
  const speech=await generator.decodeAudioData(await response.arrayBuffer());
  source=generator.createBufferSource();source.buffer=speech;source.loop=true;
  const bus=generator.createGain();source.connect(bus);source.start();
  const detector=new LocalVAD(window);
  const env={storage:{getItem:()=>'',setItem(){}},location:{hash:'',pathname:'/bot-call/'},history,
    now:()=>performance.now(),Audio,URL,Blob,crypto,AbortController,MediaRecorder,AudioContext,
    mediaDevices:{getUserMedia:async()=>{const d=generator.createMediaStreamDestination();bus.connect(d);return d.stream;}},
    createVAD:()=>detector,setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window)};
  page=new CallPage(env);page.started=true;page.reference='synthetic-local-only';
  page.server={audio_available:true,speech_available:true};page.refresh=async()=>{};
  await page.modelPreparation;check(page.modelReady,'model');
  let running=0;const run=SileroSignalModel.prototype.run;
  SileroSignalModel.prototype.run=async function(frame){
    running++;result.peakRuns=Math.max(result.peakRuns,running);
    try{return await run.call(this,frame);}finally{running--;}
  };
  const staged=new Map(),committed=new Set();let rejectChunk=false;
  page.api=async(route,body,headers={})=>{
    if(route==='metrics')return {recorded:true};
    const id=headers['X-Bot-Utterance-ID'];check(id,'id');
    const bytes=new Uint8Array(await body.arrayBuffer()),stored=staged.get(id)||new Uint8Array();
    if(route==='audio-chunk'){
      if(rejectChunk)throw Error('injected_chunk_failure');
      check(Number(headers['X-Bot-Chunk-Offset'])===stored.length,'offset');
      const next=new Uint8Array(stored.length+bytes.length);next.set(stored);next.set(bytes,stored.length);staged.set(id,next);
      return {chunk_received:true,utterance_id:id,next_seq:Number(headers['X-Bot-Chunk-Seq'])+1,next_offset:next.length};
    }
    check(route==='audio-commit'||route==='audio','route');check(!committed.has(id),'duplicate');
    const offset=route==='audio-commit'?Number(headers['X-Bot-Tail-Offset']):0;
    const full=new Uint8Array(offset+bytes.length);full.set(stored.subarray(0,offset));full.set(bytes,offset);
    const sha=[...new Uint8Array(await crypto.subtle.digest('SHA-256',full))].map(v=>v.toString(16).padStart(2,'0')).join('');
    check(full.length===page.clip.blob.size && sha===page.clip.sha,'full_bytes');committed.add(id);
    return {audio_received:true,utterance_id:id};
  };
  result.stage='long';await page.listen();context=page.audioContext;
  let r=page.recording;check(r.metrics.client_capture_mode===(page.config.captureMode==='auto'?1:0),'unexpected capture mode');check(r.vad?.valid,'vad_ready');
  await until(()=>page.round&&!page.sendTask,37000);
  check(r.reason==='auto_limit'&&!r.error&&r.clip.blob.size>0&&committed.has(r.id),'long_failed');
  result.segments.push({reason:r.reason,ms:Math.round(r.endedAt-r.start),bytes:r.clip.blob.size,error:r.error,vadReady:true});
  result.stage='after_limit';page.round=null;await page.listen();r=page.recording;check(r.vad?.valid,'second_vad');
  await wait(5200);await page.submitSegment();
  check(!r.error&&committed.has(r.id),'handoff_failed');
  result.segments.push({reason:r.reason,ms:Math.round(r.endedAt-r.start),bytes:r.clip.blob.size,error:r.error});
  result.stage='chunk_failure';page.round=null;rejectChunk=true;await page.listen();r=page.recording;
  // Record enough looped speech that the silence gate cannot drop this clip;
  // a sub-second clip may end at a word onset and is (correctly) discarded.
  const chunkStart=performance.now();await until(()=>r.uploader.failed);await wait(Math.max(0,1500-(performance.now()-chunkStart)));
  await page.submitSegment();check(!r.error&&committed.has(r.id)&&r.uploader.failed,'chunk_fallback');
  result.segments.push({reason:'chunk_fallback',bytes:r.clip.blob.size,error:r.error});rejectChunk=false;
  for(const kind of ['recorder_error','inactive_stop','track_ended','empty','overflow','stop_threw','stop_missing']){
    result.stage=kind;page.round=null;await page.listen();r=page.recording;
    if(kind==='empty')r.recorder.ondataavailable=()=>{};
    if(kind==='overflow')page.config.maxAudioBytes=1;
    await wait(600);
    if(kind==='inactive_stop') {
      // This healthy clip must contain retainable voice before testing stop.
      // A fixed 600ms may cover only the looped WAV's pause/control tail.
      await until(()=>r.vad?.speechSpans?.some(([a,b])=>(b-a)/r.vad.rate*1000>=96));
      await wait(350); // Keep that evidence outside the existing 300ms control tail.
    }
    if(kind==='recorder_error'){
      // Browser spec: error sets inactive, then error, final dataavailable and stop are queued.
      r.recorder.stop();r.recorder.onerror(new Event('error'));await page.sendClip();
    } else if(kind==='inactive_stop'){
      r.recorder.stop();await page.submitSegment();
    } else if(kind==='track_ended'){
      for(const t of r.stream.getTracks())t.stop();await until(()=>r.stopped);await page.sendClip();
    } else if(kind==='stop_missing'){
      r.recorder.onstop=()=>{};r.recorder.stop();await page.submitSegment();
    } else if(kind==='stop_threw'){
      r.recorder.stop=()=>{throw new DOMException('synthetic','InvalidStateError');};await page.submitSegment();
    } else if(kind==='overflow'){
      await until(()=>page.clip||page.round);if(page.clip)await page.sendClip();
    } else await page.submitSegment();
    await wait(120);
    const clip=page.clip,phase=page.presentation().phase;
    result.faults.push({kind,phase,error:clip?.error||false,stopThrew:clip?.stopThrew||false,bytes:clip?.blob.size,submitted:committed.has(r.id),recovery:page.presentation().action||null});
    if(expectRecovery && ['recorder_error','empty','overflow','stop_threw','stop_missing'].includes(kind))check(phase==='error'&&page.presentation().action==='discard','recovery:'+kind);
    if(expectRecovery && kind==='inactive_stop')check(committed.has(r.id)&&!r.error,'inactive_queued_stop');
    // Fixture cleanup after recording the observed state. Production discard must be explicit.
    if(clip){clip.uploader?.abandon();page.clip=null;}page.releaseMic();page.config.maxAudioBytes=2097152;
  }
  result.stage='completed';result.ok=true;
}catch(e){result.failure=e.message;}
finally {
  page?.releaseMic();page?.closePCM();try{await page?.detector?.close();}catch(_){}
  try{source?.stop();source?.disconnect();}catch(_){}
  try{await generator?.close();}catch(_){}
  result.contextClosed=context?.state==='closed'&&generator?.state==='closed';window.smokeResult=result;
}
"""
if __name__ == "__main__":
    baseline = "--baseline" in sys.argv
    if baseline:
        sys.argv.remove("--baseline")
    smoke.BROWSER_JS = BROWSER_JS.replace(
        "const expectRecovery=true;",
        "const expectRecovery=false;" if baseline else "const expectRecovery=true;",
    )
    smoke.DRIVER = smoke.DRIVER.replace("i<600", "i<1400").replace(
        "90000*repeats", "160000*repeats"
    )
    smoke.main()
