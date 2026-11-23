"""H085: actual CSP/Worker/Opus + existing capture graph, synthetic loopback only."""

import check_local_vad_browser as smoke

smoke.DRIVER = smoke.DRIVER.replace("modelRequests!==1", "modelRequests!==3").replace(
    "result.peakRuns!==1||", ""
)
smoke.BROWSER_JS = r"""
import {mountCallPage} from '/bot-call/call.js';
import {LocalVAD} from '/bot-call/vad.js';
const wait=ms=>new Promise(r=>setTimeout(r,ms));
const check=(v,m)=>{if(!v)throw Error(m);};
const result={ok:false,stage:'start',cases:[]};
let generator,source,page,detector;
const original=Object.getOwnPropertyDescriptor(window,'AudioEncoder');
const nativeContext=window.AudioContext;
try {
  generator=new AudioContext({sampleRate:48000});await generator.resume();
  const destination=generator.createMediaStreamDestination();
  source=new ConstantSourceNode(generator,{offset:0});source.connect(destination);source.start();
  window.AudioContext=class extends nativeContext {constructor(options){super(options || {sampleRate:48000});}};
  for(const mode of ['native','missing','throw']) {
    result.stage=mode;
    if(mode==='missing')Object.defineProperty(window,'AudioEncoder',{configurable:true,value:undefined});
    if(mode==='throw')Object.defineProperty(window,'AudioEncoder',{configurable:true,value:class {static isConfigSupported(){throw Error('private-error');}}});
    const markup=await (await fetch('/bot-call/')).text();
    document.body.replaceWith(new DOMParser().parseFromString(markup,'text/html').body);
    let microphones=0;
    navigator.mediaDevices.getUserMedia=async()=>{microphones++;return destination.stream.clone();};
    detector=new LocalVAD(window);
    page=mountCallPage(window,document,{createVAD:()=>detector});
    page.started=true;page.reference='synthetic';page.server={audio_available:true,speech_available:true};page.refresh=async()=>{};
    const traffic=[];page.api=async(route,body)=>{traffic.push({route,body});return {};};
    check(!page.capability.running,'probe competed with startup');page.preparePCM();
    check(microphones===0,'probe opened microphone');
    check(!traffic.some(t=>t.body?.generation_id?.startsWith('page:cap:')),'reported before runtime');
    check(await page.modelPreparation,'model');
    await page.listen();
    check(page.recording?.recorder.state==='recording','capture failed');
    check(page.recording.metrics.client_capture_mode===(page.config.captureMode==='auto'?1:0),'capture mode');
    const context=page.audioContext;
    check(detector.context===context,'probe did not reuse capture');
    check(context===page.pcmContext&&context.sampleRate===48000,'separate playback clock/rate');
    await wait(2200);
    check(page.recording.vad.valid,'VAD invalid with playback context');
    check(!page.capability.running,'probe competed with capture');
    check(!traffic.some(t=>t.body?.generation_id?.startsWith('page:cap:')),'expensive probe not deferred');
    const active=page.recording;
    // Playback graph changes must work while the SAME recording is active.
    const activeOutput=context.createBufferSource();
    activeOutput.buffer=context.createBuffer(1,4800,48000);activeOutput.loop=true;
    activeOutput.connect(context.destination);activeOutput.start();
    const activeBusy=performance.now();while(performance.now()-activeBusy<120){}
    await wait(100);
    check(page.recording===active&&active.recorder.state==='recording'&&!active.error&&active.metrics.client_capture_mode===(page.config.captureMode==='auto'?1:0),'active pause lost capture');
    if(page.config.captureMode==='auto') {
      for(const key of ['client_capture_gap_count','client_capture_gap_ms','client_capture_gap_max_ms','client_capture_clock_back_count','client_capture_clock_back_max_ms'])
        check(Number.isSafeInteger(active.metrics[key])&&active.metrics[key]>=0,'missing gap diagnostic');
      check(active.metrics.client_capture_gap_ms<=200&&active.metrics.client_capture_gap_max_ms<=50&&active.metrics.client_capture_clock_back_max_ms<=20,'gap budget exceeded');
    }
    const timeline=[];result.timeline=timeline;const mark=event=>timeline.push({event,ms:Math.round(performance.now())});
    let releaseProbe;const structure=page.capability.structure.bind(page.capability);
    page.capability.structure=async()=>{mark('probe-start');await new Promise(resolve=>{releaseProbe=resolve;});await structure();mark('probe-end');};
    const finished=await page.finishSegment('control');mark('capture-end');activeOutput.stop();activeOutput.disconnect();
    // Own the stop result: an endpoint/fallback submitter can consume page.clip
    // before the await resumes. The returned clip belongs to this recording.
    check(finished?.vad?.valid&&!finished.error&&finished.metrics.client_capture_mode===(page.config.captureMode==='auto'?1:0),'invalid final VAD coverage');
    result.activeStress ||= [];result.activeStress.push({mode,metrics:finished.metrics});
    check(!page.capability.running,'probe started with upload/reply pending');
    // Keep a native PCM output graph and a main-thread scheduling pause.
    const output=context.createBufferSource();output.buffer=context.createBuffer(1,4800,48000);output.connect(context.destination);output.start();
    const busy=performance.now();while(performance.now()-busy<120){}
    await wait(100);
    check(finished.vad.valid,'busy standby changed frozen VAD');
    page.clip=null;page.round={id:'first-reply'};mark('reply-push');await page.listen();await wait(50);
    check(!page.capability.running&&!page.recording,'probe competed with reply push');
    page.speech={};mark('reply-playing');await page.listen();await wait(50);
    check(!page.capability.running&&!page.recording,'probe competed with reply playback');
    page.speech=null;page.round=null;mark('reply-end');
    const nextListen=page.listen();await wait(20);
    check(page.capability.running,'probe did not start');
    await nextListen;mark('next-recording-start');
    check(!timeline.some(t=>t.event==='probe-end'),'probe blocked next capture');
    const resumed=page.recording,owner=page.capture;
    releaseProbe();await page.capability.task;
    if(page.recording?.recorder.state!=='recording')result.captureFailure={failure:page.captureFailure,input:page.captureInputDiagnostics,closed:owner?.closed,
      metrics:resumed?.metrics,error:resumed?.error,reason:resumed?.reason,stats:resumed?.recorder.stats,clip:page.clip?.metrics};
    check(page.recording?.recorder.state==='recording','recording did not resume after probe');
    await page.finishSegment('control');await page.capability.task;await wait(100);
    const reports=traffic.filter(t=>t.body?.generation_id?.startsWith('page:cap:'));
    check(reports.length===1,'not one report');
    const values=reports[0].body.values;
    check(values.client_cap_worklet_ready===1&&values.client_cap_frames>0,'missing runtime frames');
    check(values.client_cap_rate===context.sampleRate&&values.client_cap_context===1,'context');
    check(values.client_cap_first_ms>=0&&values.client_cap_gaps>=0,'timing');
    check(Object.values(values).every(Number.isSafeInteger),'private fields');
    if(mode==='native')check(values.client_cap_opus===7&&values.client_cap_encode===1&&values.client_cap_worker===31&&values.client_cap_roundtrip_samples===48000,'native codecs');
    else check(values.client_cap_opus===0&&values.client_cap_encode===0,'degradation');
    check(values.client_cap_worker_state===1,'CSP worker');
    check(microphones===1&&page.recording===null,'changed capture');
    page.releaseMic();page.closePCM();await detector.closing;await wait(50);
    check(context.state==='closed','capture context cleanup');
    page.capability.finish();page.sendCapability();
    check(traffic.filter(t=>t.body?.generation_id?.startsWith('page:cap:')).length===1,'duplicate report');
    check(!traffic.some(t=>t.route!=='metrics'),'probe uploaded audio');
    result.cases.push({mode,microphones,values,timeline});
  }
  result.ok=true;
}catch(e){result.ok=false;result.failure=e.message;result.failureContext={input:page?.captureInputDiagnostics,
 captureFailure:page?.captureFailure,disabled:page?.captureDisabled,rebuilds:page?.captureRebuilds,clip:page?.clip?.metrics,recording:page?.recording?.metrics};}
finally {
  window.AudioContext=nativeContext;
  if(original)Object.defineProperty(window,'AudioEncoder',original);else delete window.AudioEncoder;
  try{page?.releaseMic();page?.closePCM();await detector?.closing;}catch(_){}
  try{source?.stop();source?.disconnect();}catch(_){}
  if(generator)await generator.close();result.contextClosed=generator?.state==='closed';
  window.smokeResult=result;
}
"""

if __name__ == "__main__":
    smoke.main()
