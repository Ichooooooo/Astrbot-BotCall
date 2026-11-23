"""24 s generated idle/speech workload; coarse Linux proxy, not phone battery."""

import json
import os
import check_local_vad_browser as smoke

mode = os.environ.get("BOT_CALL_RESOURCE_MODE", "auto")
assert mode in ("auto", "legacy")
smoke.BROWSER_JS = r"""
import {CallPage} from '/bot-call/call.js';
import {LocalVAD,SileroSignalModel} from '/bot-call/vad.js';
const wait=ms=>new Promise(r=>setTimeout(r,ms));
const result={ok:false,peakRuns:0,mode:MODE};let page,context,generator,source,detector,destination;
try{
  generator=new AudioContext({sampleRate:48000});await generator.resume();
  destination=generator.createMediaStreamDestination();
  const gain=generator.createGain();gain.gain.value=0;
  source=generator.createOscillator();source.connect(gain);gain.connect(destination);source.start();
  detector=new LocalVAD(window);
  let running=0;const run=SileroSignalModel.prototype.run;
  SileroSignalModel.prototype.run=async function(frame){running++;result.peakRuns=Math.max(result.peakRuns,running);try{return await run.call(this,frame);}finally{running--;}};
  page=new CallPage({storage:{getItem:()=>'',setItem(){}},location:{hash:'',pathname:'/bot-call/'},history,
    now:()=>performance.now(),Audio,URL,Blob,crypto,AbortController,MediaRecorder,AudioContext,
    mediaDevices:{getUserMedia:async()=>destination.stream.clone()},createVAD:()=>detector,
    setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window)},
    ()=>{}, {captureMode:MODE,chunkUpload:false});
  page.started=true;page.reference='synthetic';page.server={audio_available:true,speech_available:true};
  page.api=async()=>({recorded:true});await page.modelPreparation;await page.listen();context=page.audioContext;
  await wait(12000);result.idleBytes=page.recording.size;result.idleSamples=page.recording.recorder.samples??null;
  gain.gain.value=.15;await wait(12000);
  const clip=await page.finishSegment('manual');result.clipBytes=clip.blob.size;result.clipMs=clip.durationMs;
  result.jsHeapBytes=performance.memory?.usedJSHeapSize;result.ok=clip.blob.size>0&&!clip.error;
}catch(e){result.error=e.message;}
finally{
  if(page?.recording)await page.finishSegment('control').catch(()=>{});
  page?.releaseMic();await detector?.close();
  try{source?.stop();for(const t of destination.stream.getTracks())t.stop();await generator?.close();}catch(_){}
  result.contextClosed=context?.state==='closed';window.smokeResult=result;
}
""".replace("MODE", json.dumps(mode))
if __name__ == "__main__":
    smoke.main()
