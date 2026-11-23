"""One synthetic second through the production Worker/mux and real Chromium."""

import json
import os
from pathlib import Path
import check_local_vad_browser as smoke

smoke.DRIVER = smoke.DRIVER.replace("result.peakRuns!==1", "false").replace(
    "wasmRequests!==1||modelRequests!==1", "wasmRequests!==0||modelRequests!==0"
)
smoke.BROWSER_JS = r"""
const result={ok:false};let context;
try {
  context=new AudioContext({sampleRate:48000});await context.resume();
  const observations=[];let encoder,inputs=0,beforeFlush=0;
  encoder=new AudioEncoder({output:(c,m)=>{
    const a=new Uint8Array(c.byteLength);c.copyTo(a);
    observations.push({timestamp:c.timestamp,duration:c.duration,bytes:c.byteLength,toc:a[0],count:a[1]&63,
      skip:m.decoderConfig?.description?new DataView(m.decoderConfig.description).getUint16(10,true):null});
  },error:()=>{throw Error('encoder');}});
  encoder.configure({codec:'opus',sampleRate:48000,numberOfChannels:1,bitrate:24000,opus:{frameDuration:20000}});
  for(let i=0;i<50;i++){
    const a=new AudioData({format:'f32',sampleRate:48000,numberOfChannels:1,numberOfFrames:960,timestamp:i*20000,
      data:Float32Array.from({length:960},(_,j)=>.1*Math.sin(2*Math.PI*440*(i*960+j)/48000))});
    encoder.encode(a);a.close();inputs++;
    if(encoder.encodeQueueSize>=8)await new Promise(r=>encoder.addEventListener('dequeue',r,{once:true}));
  }
  beforeFlush=observations.length;await encoder.flush();encoder.close();
  const worker=new Worker('/bot-call/capture-worker.js');
  const mux=await new Promise((resolve,reject)=>{
    const timer=setTimeout(()=>{worker.terminate();reject(Error('probe timeout'));},8000);
    worker.onmessage=({data})=>{if(data.type==='ready'||data.type==='error'){clearTimeout(timer);worker.terminate();resolve(data);}};
    worker.onerror=()=>{clearTimeout(timer);worker.terminate();reject(Error('worker'));};
    worker.postMessage({type:'probe'});
  });
  result.oldStrictFailure=observations.findIndex((c,i)=>c.duration!==20000||!Number.isFinite(c.timestamp)||(i>0&&Math.abs(c.timestamp-i*20000)>1));
  result.observations=observations;result.beforeFlush=beforeFlush;result.inputs=inputs;result.mux={...mux};delete result.mux.data;
  if(mux.type!=='ready')throw Error('mux failed');
  const audio=await context.decodeAudioData(mux.data.slice(0));
  result.decodedSamples=audio.length;if(audio.length!==48000)throw Error('decoded duration');
  let binary='';for(const b of new Uint8Array(mux.data))binary+=String.fromCharCode(b);
  result.file=btoa(binary);result.ok=true;
}catch(e){result.error=e.message;}
finally{await context?.close();result.contextClosed=context?.state==='closed';window.smokeResult=result;}
"""

if __name__ == "__main__":
    directory = Path(os.environ.get("BOT_CALL_CODEC_EVIDENCE", "/tmp/bot-call-codec-check"))
    directory.mkdir(parents=True, exist_ok=True)
    smoke.DRIVER = smoke.DRIVER.replace(
        "observed=result;const urls=",
        "if(result?.file){fs.writeFileSync("
        + json.dumps(str(directory / "real.webm"))
        + ",Buffer.from(result.file,'base64'));delete result.file;}"
        + "if(result)fs.writeFileSync("
        + json.dumps(str(directory / "result.json"))
        + ",JSON.stringify(result));observed=result;const urls=",
    )
    smoke.main()
    from check_capture_browser import verify_packet_timeline, verify_stt_inputs

    verify_packet_timeline(directory / "real.webm", 48000)
    verify_stt_inputs(directory)
