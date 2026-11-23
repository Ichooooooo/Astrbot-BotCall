"""Noise gate check: deterministic PCM noise/clicks and licensed short public voice, local only."""

import subprocess
import sys
from pathlib import Path

import check_local_vad_browser as smoke

from bot_call.web import _Static

BASELINE = "--baseline" in sys.argv
if BASELINE:
    sys.argv.remove("--baseline")
NativeApp = smoke.CallWebApp


class FixtureApp(NativeApp):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if BASELINE:
            for name in ("call.js", "vad.js"):
                self._assets["/bot-call/" + name] = _Static(
                    subprocess.check_output(
                        ["git", "show", "v0.8.3:bot_call/static/" + name], cwd=smoke.ROOT
                    ),
                    b"text/javascript",
                )
        for name in ("ng", "hao", "dui"):
            self._assets["/bot-call/test-" + name + ".ogg"] = _Static(
                (Path(__file__).parent / "fixtures" / ("zh-" + name + ".ogg")).read_bytes(),
                b"audio/ogg",
            )


smoke.CallWebApp = FixtureApp
# Extended A/B/C matrix shares one browser; allow its bounded local replay.
smoke.DRIVER = smoke.DRIVER.replace("i<600", "i<1200")

smoke.BROWSER_JS = r"""
import {mountCallPage} from '/bot-call/call.js';
import {LocalVAD,SileroSignalModel} from '/bot-call/vad.js';
const wait=ms=>new Promise(r=>setTimeout(r,ms));
const check=(v,m)=>{if(!v)throw Error(m);};
const baseline=BASELINE_VALUE;
const result={ok:false,stage:'start',peakRuns:0,phases:[]};
let page,generator,zero;
try {
  navigator.mediaDevices.getUserMedia=()=>{throw Error('real_mic_forbidden');};
  generator=new AudioContext({sampleRate:48000});await generator.resume();
  const bus=generator.createGain(),destination=generator.createMediaStreamDestination();bus.connect(destination);
  zero=new ConstantSourceNode(generator,{offset:0});zero.connect(bus);zero.start();
  const detector=new LocalVAD(window);let running=0;
  const run=SileroSignalModel.prototype.run;
  SileroSignalModel.prototype.run=async function(frame){running++;result.peakRuns=Math.max(result.peakRuns,running);try{const p=await run.call(this,frame);if(detector.active){const s=detector.active;s.probes ||= [];s.probes.push(p);}return p;}finally{running--;}};
  const markup=await (await fetch('/bot-call/')).text();
  document.body.replaceWith(new DOMParser().parseFromString(markup,'text/html').body);
  navigator.mediaDevices.getUserMedia=async()=>destination.stream.clone();
  page=mountCallPage(window,document,{createVAD:()=>detector,silenceMs:4000,vadRedemptionMs:4000});
  page.started=true;page.reference='synthetic-only';page.server={audio_available:true,speech_available:true};page.refresh=async()=>{};
  const traffic=[];
  page.api=async(route,body,headers={})=>{
    traffic.push({route,values:body?.values});
    if(route==='metrics')return {};
    const id=headers['X-Bot-Utterance-ID'];check(!!id,'id');
    if(route==='audio-chunk')return {chunk_received:true,utterance_id:id,next_seq:Number(headers['X-Bot-Chunk-Seq'])+1,next_offset:Number(headers['X-Bot-Chunk-Offset'])+body.size};
    check(['audio','audio-commit'].includes(route),'route');return {audio_received:true,utterance_id:id};
  };
  check(await page.modelPreparation,'model');
  const words={};
  for(const name of ['ng','hao','dui'])words[name]=await generator.decodeAudioData(await (await fetch('/bot-call/test-'+name+'.ogg')).arrayBuffer());
  result.words=Object.fromEntries(Object.entries(words).map(([k,b])=>{
    const v=b.getChannelData(0),rate=b.sampleRate;let a=0,end=v.length-1;
    while(a<end && Math.abs(v[a])<.01)a++;while(end>a && Math.abs(v[end])<.01)end--;
    const rms=(x,y)=>{let power=0,count=0;for(let i=a+Math.round(x*rate);i<Math.min(v.length,a+Math.round(y*rate));i++){power+=v[i]*v[i];count++;}return Math.sqrt(power/Math.max(1,count));};
    return [k,{seconds:b.duration,rate,trimStartMs:a/rate*1000,audibleMs:(end-a)/rate*1000,
      last20msAt300Rms:rms(.28,.30),following100msRms:rms(.30,.40)}];
  }));
  const cases=[
    ['ng',.3,20],['hao',.3,20],['ng-soft',.3,20],['hao-soft',.3,20],
    ['ng-200',.2,20],['hao-400',.4,20],['dui',.3,20],['dui-soft',.3,20],['dui-400',.4,20],['dui-manual',.3,20],
    ['click-control',.08,0],['click-manual',.08,0],['tap-control',.16,0],['click-early',.08,500],
    ['fan',1,400],['street',1,400],['keyboard',1,400],
    ['noise-speech',.70,400],['silence-manual',.3,0],
    ['keyboard-coverage',1,400,'valid-gap'],['hao-soft-coverage',.3,20,'valid-gap'],
    ['silence-c',.3,0,'aux-gap'],['hao-soft-c',.3,20,'aux-gap'],
    ['silence-cold',.3,0,'cold'],['ng-soft-cold',.3,20,'cold'],
    ['hao-soft-cold',.3,20,'cold'],['dui-soft-cold',.3,20,'cold'],
    ['ng-200-cold',.2,20,'cold'],['noise-speech-cold',.7,400,'cold'],
    ['click-cold',.08,0,'cold'],['tap-cold',.16,0,'cold'],['fan-cold',1,400,'cold'],
    ['silence-gap',.3,0,'vad-gap'],['hao-soft-gap',.3,20,'vad-gap'],
    ['dui-soft-gap',.3,20,'vad-gap'],['ng-200-gap',.2,20,'vad-gap'],
    ['noise-speech-gap',.7,400,'vad-gap'],
    ['silence-both',.3,0,'both-gap'],['silence-energy-gap',.3,0,'cold-aux-gap'],
    ['hao-soft-immediate',.3,20,'cold-immediate'],
    ['street-recheck',1,100,'cold-recheck'],['hao-soft-recheck',.3,20,'cold-recheck'],
    ['street-timeout',1,100,'cold-recheck-timeout'],['hao-soft-timeout',.3,20,'cold-recheck-timeout'],
    ['street-unready',1,100,'cold-recheck-unready']
  ];
  for(const [kind,seconds,tail,mode] of cases) {
    result.stage=kind;page.muted=false;page.round=null;page.silentNotice=false;
    await page.listen();await wait(250);const warm=await page.finishSegment('fixture-warmup');warm.uploader?.abandon();page.clip=null;
    const cold=mode?.startsWith('cold'),auxGap=['aux-gap','both-gap','cold-aux-gap'].includes(mode);
    const ready=detector.ready.bind(detector);
    if(cold)detector.ready=()=>false;
    await page.listen();detector.ready=ready;
    const r=page.recording;
    result.captureState={warm:{error:warm.error,metrics:warm.metrics},metrics:r?.metrics,failure:page.captureFailure,disabled:page.captureDisabled,input:page.captureInputDiagnostics};
    check(r.metrics.client_capture_mode===(page.config.captureMode==='auto'?1:0),'unexpected capture mode');check(cold?!r.vad:r.vad?.valid,'vad');const start=traffic.length;
    if(auxGap) {
      // Delay only the auxiliary timer: real worklet PCM and recorder continue.
      const sample=page.sampleRecording.bind(page);let skipUntil=performance.now()+300;
      page.sampleRecording=value=>{if(value===r && performance.now()<skipUntil)return;sample(value);};
      await wait(350);page.sampleRecording=sample;
    }
    let restoreRun,restoreSample,restorePrepare,restoreMessage;const coverage={injected:false};
    if(mode==='valid-gap') {
      const message=detector.message;let added=0;
      restoreMessage=()=>{detector.message=message;};
      detector.message=function(m) {
        const s=this.active;
        if(s===r.vad && m.id===s.id) {
          if(m.type==='gap')m={...m,count:m.count+added};
          if(!coverage.injected && m.type==='frame' && s.valid) {
            // A clock discontinuity at a complete analysis-frame boundary.
            // The recorder PCM stays complete; the live VAD remains valid
            // but must report coverageGap, just as with a native small gap.
            message.call(this,{type:'gap',id:s.id,start:m.start,count:(s.gapCount||0)+1,lostMs:s.lostMs||0,reason:'clock'});
            added++;coverage.injected=true;
          }
        }
        return message.call(this,m);
      };
    }
    if(mode?.startsWith('cold-recheck')) {
      const sample=page.sampleRecording.bind(page),prepare=detector.prepareModel.bind(detector);
      restoreSample=()=>{page.sampleRecording=sample;};restorePrepare=()=>{detector.prepareModel=prepare;};
      page.sampleRecording=value=>{if(value===r){value.quality.valid=false;return;}sample(value);};
      detector.prepareModel=async()=>{if(mode.endsWith('unready'))return new Promise(()=>{});await wait(mode.endsWith('timeout')?2700:250);return prepare();};
    }
    if(mode==='vad-gap' || mode==='both-gap') {
      // Pause real async inference while main-thread energy continues. The
      // unchanged 16-frame worklet credit limit produces real coverage gaps.
      const model=detector.model,modelRun=model.run.bind(model);restoreRun=()=>{model.run=modelRun;};let held=false;
      detector.model.run=async frame=>{if(!held){held=true;await wait(1100);}return modelRun(frame);};
      await wait(700);
    } else if(mode!=='cold-immediate')await wait(500);
    const buffer=generator.createBuffer(1,Math.round(generator.sampleRate*seconds),generator.sampleRate),v=buffer.getChannelData(0);
    const data=words[kind.split('-')[0]]?.getChannelData(0);
    let a=0;
    if(data)while(a<data.length-1 && Math.abs(data[a])<.01)a++;
    let seed=12345,filtered=0;const random=()=>{seed=(Math.imul(seed,1664525)+1013904223)>>>0;return seed/2147483648-1;};
    for(let i=0;i<v.length;i++) {
      const t=i/generator.sampleRate,n=random();filtered=.96*filtered+.04*n;
      if(data) {
        // Native-rate crop after removing only leading silence; no pitch shift.
        v[i]=(data[a+i] || 0)*(kind.includes('soft')?.1:.5);
      }
      else if(kind.startsWith('click') || kind.startsWith('tap'))v[i]=.4*Math.exp(-t/(kind.startsWith('tap')?.025:.008))*(n+Math.sin(2*Math.PI*180*t))*.5;
      else if(kind.startsWith('fan'))v[i]=.008*(Math.sin(2*Math.PI*110*t)+filtered);
      else if(kind.startsWith('street'))v[i]=.015*filtered*(.6+.4*Math.sin(2*Math.PI*2*t))+.002*n;
      else if(kind.startsWith('keyboard'))v[i]=(t%.17<.028)?.12*n*Math.exp(-(t%.17)/.006):.002*filtered;
      else if(kind.startsWith('noise-speech'))v[i]=.012*filtered+(words.hao.getChannelData(0)[Math.round(generator.sampleRate*.15)+i]||0)*.3;
    }
    const hash=[...new Uint8Array(await crypto.subtle.digest('SHA-256',v.buffer))].map(x=>x.toString(16).padStart(2,'0')).join('');
    const source=generator.createBufferSource();source.buffer=buffer;source.connect(bus);
    const ended=new Promise(resolve=>source.onended=resolve);source.start();await ended;source.disconnect();await wait(tail);
    if(kind.endsWith('manual'))await page.submitSegment();else await page.toggleMute();
    restoreRun?.();restoreSample?.();restorePrepare?.();restoreMessage?.();
    const clip=r.clip,events=traffic.slice(start),commits=events.filter(x=>['audio','audio-commit'].includes(x.route)).length;
    result.phases.push({kind,pcmSHA256:hash,pcmSamples:v.length,coverage:clip.quality.valid,vad:clip.vad,
      mode,coverageInjection:mode==='valid-gap'?{...coverage,onlineValid:r.vad?.valid,onlineGap:r.vad?.coverageGap}:undefined,modelPositiveMs:r.vad?.positiveMs,probabilities:r.vad?.probes,voiced:clip.voiced,energyVoiced:clip.energyVoiced,noiseFloor:r.noiseFloor,
      bytes:clip.blob.size,metrics:clip.metrics,commits,drops:events.filter(x=>x.values?.client_silent_drop).length,
      label:page.presentation().label,mainState:document.getElementById('call-state').textContent});
    const noise=/^(click|tap|fan|street|keyboard|silence)/.test(kind);
    const uncertain=(['both-gap','cold-aux-gap'].includes(mode) && !clip.idleSilent && clip.metrics.client_gate_recheck!==2) || ['cold-recheck-timeout','cold-recheck-unready'].includes(mode);
    if(clip.idleSilent)check(clip.blob.size===0 && clip.metrics.client_capture_mode===1,'PCM idle proof');
    // With no model coverage, any plausible energy is retained, including
    // ambiguous clicks. A quiet, fully covered energy trace can still drop.
    const fallbackNoise=cold && /^(click|tap|fan)/.test(kind) && clip.metrics.client_gate_recheck!==2;
    if(!baseline)check(commits===((!noise || uncertain || fallbackNoise)?1:0),'disposition:'+kind);
    if(mode==='valid-gap') {
      check(coverage.injected && r.vad.valid && r.vad.coverageGap,'valid coverage gap not exercised');
      check(clip.metrics.client_gate_initial_reason & 16,'missing original coverage reason');
      check(clip.metrics.client_gate_recheck===2 && clip.vad.valid && !clip.vad.coverageGap,'complete PCM recheck');
      check(clip.metrics.client_gate_recheck_ms<=2600,'bounded coverage wait');
      check(commits===(noise?0:1),'coverage speech/noise disposition');
    }
    if(mode?.startsWith('cold-recheck')) {
      check(clip.metrics.client_gate_recheck===((mode.endsWith('timeout')||mode.endsWith('unready'))?3:2),'recheck outcome');
      check(clip.metrics.client_gate_recheck_ms<=2600,'bounded cold wait');
      check((mode.endsWith('timeout')||mode.endsWith('unready'))?commits===1:kind.startsWith('hao')?commits===1:commits===0,'cold speech/noise disposition');
      if(mode.endsWith('timeout')||mode.endsWith('unready')){await detector.offlineTask;check(!detector.offlineTask,'cold wait retained model lease');}
    }
    if(!baseline)check(Number.isSafeInteger(clip.metrics.client_gate_unknown_reason),'reason:'+kind);
    if(kind==='silence-manual') {
      await wait(100);check(!!page.recording,'notice.resume');
      const label=()=>document.getElementById('call-state').textContent;
      const visible=label().includes('没有听到声音，请再说一次');
      await wait(2600);const held=label().includes('没有听到声音，请再说一次');
      await wait(400);const expired=!label().includes('没有听到声音');
      result.notice={visible,held,expired};
      if(!baseline)check(visible && held && expired,'notice.duration');
    }
    if(!mode)check(clip.vad.valid && clip.quality.valid,'coverage');
    if(mode==='aux-gap')check(clip.vad.valid && !clip.vad.coverageGap && !clip.quality.valid,'C.not_reproduced');
    if(mode==='vad-gap' || mode==='both-gap')check(r.vad.metrics.gapCount>0,'B.not_reproduced');
    if(mode==='vad-gap')check(clip.quality.valid,'B.energy_coverage');
    // A manual submit starts listen() asynchronously. Own that setup before
    // tearing down its context, just as a real control invalidates/awaits it.
    if(page.listenTask)await page.listenTask;
    if(page.recording){const next=await page.finishSegment('fixture-warmup');next.uploader?.abandon();page.clip=null;}
    page.releaseMic();await detector.closing;
  }
  result.ok=true;result.stage='completed';
} catch(e) {result.failure=e.message;}
finally {
  try{if(page?.recording)await page.finishSegment('control');}catch(_){}
  page?.releaseMic();page?.closePCM();await page?.detector?.close();try{zero?.stop();}catch(_){}await generator?.close();
  result.contextClosed=generator?.state==='closed' && !page?.audioContext;window.smokeResult=result;
}
"""
smoke.BROWSER_JS = smoke.BROWSER_JS.replace("BASELINE_VALUE", "true" if BASELINE else "false")
if __name__ == "__main__":
    smoke.main()
