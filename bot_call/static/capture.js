// PCM lifetime belongs to the foreground call, not to an encoder or VAD model.
export const CAPTURE_FAILURE=Object.freeze({api:1,config:2,worker:3,encoder:4,mux:5,queue:6,input:7,timeout:8});
const RATE=48000,MAX_RING=25,MAX_PENDING=32,MAX_START=175,MAX_WARM=175; // 3 s live + 500 ms prefix.
// Native recovery can take one segment of realtime. Preserve the following
// M5 segment while it runs, bounded to 31 s (about 6 MB mono PCM).
const MAX_FORWARD_GAP_SAMPLES=2400; // 50 ms at 48 kHz, same bound as the worklet.
const MAX_CARRY=1550,MAX_GAP_SAMPLES=9600; // 200 ms per segment, including retained prefix.
const frameSlice=(values,start=0,end=values.length)=>{
  const out=values.slice(start,end);
  if(values.captureStart!==undefined)out.captureStart=values.captureStart+start;
  out.gapOwner=values.gapOwner;
  out.gaps=(values.gaps||[]).flatMap(g=>{
    const a=Math.max(start,g.offset),b=Math.min(end,g.offset+g.samples);
    return b>a?[{...g,offset:a-start,samples:b-a}]:[];
  });
  out.backs=(values.backs||[]).filter(b=>b.offset>=start&&b.offset<end).map(b=>({...b,offset:b.offset-start}));
  return out;
};
export const CAPTURE_INPUT=Object.freeze({gap:1,credits:2,sample:3,channels:4,rate:5,processor:6,
  suspended:7,interrupted:8,closed:9,context:10,mute:11,ended:12,notLive:13,length:14,values:15,sequence:16,message:17});
const bounded=(v,max)=>Math.max(0,Math.min(max,Math.round(Number(v)||0)));
export class ContinuousCapture {
  static async create(page) {
    const w=page.e.probeWindow||globalThis,c=page.audioContext;
    if(!w.Worker||!w.AudioWorkletNode||!c?.audioWorklet)throw 1;
    const capture=new ContinuousCapture(page,w,c);page.capture=capture;
    try{await capture.open();return capture;}catch(code){capture.close();if(page.capture===capture)page.capture=null;throw Number.isInteger(code)?code:3;}
  }
  constructor(page,w,context) {
    this.page=page;this.w=w;this.context=context;this.stream=page.stream;this.ring=[];this.carry=[];this.pending=0;this.floor=0;this.cursor=0;
  }
  worker() {return new this.w.Worker('/bot-call/capture-worker.js');}
  startupTimeout(stage) {
    const p=this.page;
    p.captureStartupDiagnostics={client_capture_timeout_stage:stage};
    return 8;
  }
  async open() {
    this.probe=this.worker();
    await new Promise((resolve,reject)=>{
      const timer=this.w.setTimeout(()=>reject(this.startupTimeout(1)),1500);
      const end=fn=>value=>{this.w.clearTimeout(timer);fn(value);};
      this.probeCancel=()=>end(reject)(7);
      this.probe.onmessage=({data:m})=>m.type==='ready'?end(resolve)():m.type==='error'?(this.lastClock=m.clock,this.page.captureDiagnostics=m.metrics,end(reject)(m.code)):null;
      this.probe.onerror=end(()=>reject(3));this.probe.postMessage({type:'check'});
    });
    this.probe.terminate();this.probe=null;this.probeCancel=null;
    await this.context.audioWorklet.addModule('/bot-call/capture-worklet.js');
    if(this.closed)throw 7;
    this.source=this.context.createMediaStreamSource(this.page.stream);
    this.node=new this.w.AudioWorkletNode(this.context,'bot-call-capture',{numberOfInputs:1,numberOfOutputs:1,
      outputChannelCount:[1],channelCount:1,channelCountMode:'explicit',channelInterpretation:'speakers'});
    this.gain=this.context.createGain();this.gain.gain.value=0;
    this.node.onprocessorerror=()=>this.input(CAPTURE_INPUT.processor);
    const ready=new Promise((resolve,reject)=>{
      this.armResolve=resolve;this.armReject=reject;
      this.armTimer=this.w.setTimeout(()=>reject(this.startupTimeout(2)),500);
    });
    this.node.port.onmessage=({data:m})=>{
      if(m.type==='ready'){this.w.clearTimeout(this.armTimer);this.armResolve();return;}
      if(this.closed&&!this.stopping)return;
      if(m.type==='prefix') {
        this.prefixGaps=m.gap;this.prefixGapIds=m.ids;this.prefixBack=m.back;this.prefixBackIds=m.backIds;
        if(this.active)this.initGaps(this.active);
        for(const frame of m.frames) {
          const values=new Float32Array(frame.samples);values.gaps=frame.gaps;values.backs=frame.backs;values.captureStart=this.cursor;this.cursor+=values.length;
          this.ring.push(values);if(this.ring.length>MAX_RING)this.ring.shift().fill(0);
          if(this.active)this.noteGaps(this.active,values);
        }
        this.recordResolve?.();return;
      }
      if(m.type==='tail' && this.stopping) {
        const r=this.stopping;this.stopping=null;this.w.clearTimeout(r.tailTimer);
        const values=new Float32Array(m.samples);values.gaps=m.gaps;values.backs=m.backs;values.captureStart=this.cursor;this.cursor+=values.length;
        if(!r.begun && !this.noteGaps(r,values)){r.completeStop();return;}
        if(!r.begun && values.length) {
          const rms=Math.sqrt(values.reduce((n,v)=>n+v*v,0)/values.length);
          if(rms>=Math.max(.0015,Math.min(.025,this.page.config.rmsThreshold,this.floor*3))||r.candidate?.())this.begin(r,true);
        }
        if(values.length) {
          if(r.begun)this.push(r,values);
          else {this.ring.push(values);if(this.ring.length>MAX_RING)this.ring.shift()?.fill(0);}
        }
        r.completeStop();if(this.closed){this.clear();this.node?.port.close();}return;
      }
      if(m.type==='fault'){this.input(m.reason,m);return;}
      if(m.type!=='pcm')return;
      try{const values=new Float32Array(m.samples);values.gaps=m.gaps;values.backs=m.backs;values.captureStart=this.cursor;this.cursor+=values.length;if(this.stopping)this.accept(this.stopping,values,true);else this.frame(values);}
      catch(_){this.input(CAPTURE_INPUT.message);}
      finally{this.node?.port.postMessage({type:'ack',seq:m.seq});}
    };
    this.source.connect(this.node);this.node.connect(this.gain);this.gain.connect(this.context.destination);
    await ready;
    this.listeners=[];
    const state=()=>{if(this.context.state!=='running')this.input(
      ({suspended:7,interrupted:8,closed:9})[this.context.state]||10);};
    this.context.addEventListener('statechange',state);this.listeners.push([this.context,'statechange',state]);
    for(const track of this.page.stream.getTracks())for(const name of ['mute','ended']) {
      const invalidate=()=>this.input(name==='mute'?11:12);
      track.addEventListener(name,invalidate);this.listeners.push([track,name,invalidate]);
    }
    if(this.context.state!=='running')state();
    if(this.stream.getTracks().some(t=>t.readyState!=='live'))this.input(13);
    if(this.closed)throw 7;
  }
  async armRecording() {
    if(this.closed)throw 7;
    let timer;
    try {
      await new Promise((resolve,reject)=>{
        this.recordResolve=resolve;this.recordReject=reject;
        timer=this.w.setTimeout(()=>{reject(this.startupTimeout(3));this.close();},500);
        this.node.port.postMessage({type:'record'});
      });
      if(this.closed)throw 7;
      this.recordArmed=true;
    } finally {this.w.clearTimeout(timer);this.recordResolve=null;this.recordReject=null;}
  }
  input(reason=17,details={}) {
    if(this.closed)return;
    const p=this.page,r=this.active||this.stopping||this.ending;
    if(reason===1 && r && !details.accounted && details.skip>0) {
      if(details.forward===false){r.clockBackCount++;r.clockBackMax=Math.max(r.clockBackMax,details.skip);}
      else {const samples=Math.round(details.skip*48);r.gapCount++;r.gapSamples+=samples;r.gapMax=Math.max(r.gapMax,samples);}
      this.gapMetrics(r);
    }
    const metrics={
      client_capture_input_reason:bounded(reason,17),
      client_capture_input_active:Number(!!r || !!this.continuing),
      client_capture_input_recovery:r||this.continuing?4:1,
      client_capture_input_speaking:Number(!!p.speech?.playing),
      client_capture_input_probe:Number(!!p.capability?.running && !p.capability?.structureDone),
      client_capture_input_echo:Number(!!p.echo && !p.echo.closed),
      client_capture_input_skip_ms:bounded(details.skip,60000),
      client_capture_input_rebuilds:bounded(p.captureRebuilds,3)};
    if(Number.isFinite(p.captureLastEnd))metrics.client_capture_input_idle_ms=bounded((p.e.now?.() ?? performance.now())-p.captureLastEnd,60000);
    if(Number.isFinite(details.credits))metrics.client_capture_input_credits=bounded(details.credits,64);
    p.captureRebuildMs=null;p.captureInputDiagnostics=metrics;
    if(p.recording?.metrics)Object.assign(p.recording.metrics,metrics);
    this.fail(7);this.close();
  }
  initGaps(r) {
    const stats=this.prefixGaps||{};
    r.gapCount=stats.count||0;r.gapSamples=stats.samples||0;r.gapMax=stats.max||0;
    r.gapIgnore=new Set(this.prefixGapIds||[]);r.gapSeen=new Set(r.gapIgnore);r.gapBudget=0;
    r.clockBackCount=this.prefixBack?.count||0;r.clockBackMax=this.prefixBack?.max||0;
    r.backSeen=new Set(this.prefixBackIds||[]);this.prefixBack=null;this.prefixBackIds=null;
    this.prefixGaps=null;this.prefixGapIds=null;this.gapMetrics(r);
  }
  gapMetrics(r) {
    r.gapMetrics={client_capture_gap_count:bounded(r.gapCount,1000),
      client_capture_gap_ms:bounded(r.gapSamples/48,60000),client_capture_gap_max_ms:bounded(r.gapMax/48,60000),
      client_capture_clock_back_count:bounded(r.clockBackCount,1000),
      client_capture_clock_back_max_ms:bounded(r.clockBackMax,60000)};
    if(this.page.recording?.recorder===r)Object.assign(this.page.recording.metrics,r.gapMetrics);
  }
  noteGaps(r,values) {
    if(values.gapOwner===r.id)return true;
    values.gapOwner=r.id;
    if(!values.gaps?.length&&!values.backs?.length)return true;
    for(const b of values.backs||[])if(!r.backSeen.has(b.id)) {
      r.backSeen.add(b.id);r.clockBackCount++;r.clockBackMax=Math.max(r.clockBackMax,b.ms);
    }
    for(const g of values.gaps||[]) {
      r.gapBudget+=g.samples;
      if(!r.gapIgnore.has(g.id)) {
        r.gapSamples+=g.samples;r.gapMax=Math.max(r.gapMax,g.max);
        if(!r.gapSeen.has(g.id)){r.gapSeen.add(g.id);r.gapCount++;}
      }
    }
    this.gapMetrics(r);
    if((values.gaps||[]).some(g=>g.max>MAX_FORWARD_GAP_SAMPLES)||r.gapBudget>MAX_GAP_SAMPLES){this.input(1,{skip:r.gapMax/48,accounted:true});return false;}
    return true;
  }
  frame(values) {
    if(this.stream.getTracks().some(t=>t.readyState!=='live')){this.input(13);return;}
    if(!values.length||values.length>960){this.input(14);return;}
    if(values.some(v=>!Number.isFinite(v)||Math.abs(v)>1)){this.input(15);return;}
    const r=this.active;
    if(r?.state==='recording'&&!r.stopping) {
      this.accept(r,values);
    } else if(this.continuing) {
      this.keepCarry(values);
    } else {
      this.ring.push(values);if(this.ring.length>MAX_RING)this.ring.shift()?.fill(0);
    }
  }
  keepCarry(values) {
    if(this.carry.length<MAX_CARRY){this.carry.push(values);return;}
    // A second failure must still poison the expression; never silently drop
    // a frame because fail() already recorded the original encoder failure.
    if(this.page.series)this.page.series.failed='capture';
    (this.active||this.ending)?.onerror?.();this.close();
  }
  accept(r,values,tail=false) {
    if(!r.begun) {
      if(!this.noteGaps(r,values))return;
      r.idleFrames=(r.idleFrames||0)+1;
      this.ring.push(values);if(this.ring.length>MAX_RING)this.ring.shift()?.fill(0);
      const rms=Math.sqrt(values.reduce((n,v)=>n+v*v,0)/values.length);
      const threshold=Math.max(.0015,Math.min(.025,this.page.config.rmsThreshold,this.floor*3));
      if(rms>=threshold||r.candidate?.())this.begin(r,tail);
      else if(rms>0)this.floor+=(rms-this.floor)*(rms<this.floor ? .5 : .01);
    } else this.push(r,values);
  }
  begin(r,tail=false) {
    if(r.begun||(r.stopping&&!tail))return;
    r.begun=true;
    r.startAt=this.page.e.now?.() ?? performance.now();
    r.modelColdAtStart=this.page.modelReady===false;
    r.metrics ||= {};
    r.metrics.client_capture_start_probe=Number(!!this.page.capability?.running&&!this.page.capability.structureDone);
    this.page.capability?.yieldToCapture();
    r.prefixSamples=this.ring.reduce((n,a)=>n+a.length,0);
    r.limit+=r.prefixSamples; // Prefix is past audio, outside the 30 s speech budget.
    r.onbegin?.();
    const prefix=this.ring.splice(0);
    for(const frame of prefix)this.push(r,frame);
  }
  push(r,values) {
    if(r.limitSent){this.keepCarry(values);return;}
    if(r.samples+values.length>r.limit) {
      const n=r.limit-r.samples;this.carry.push(frameSlice(values,n));values=frameSlice(values,0,n);
      if(!values.length)return;
    }
    if(values.captureStart!==undefined) {
      if(r.lastSample!==undefined&&r.lastSample!==values.captureStart){this.input(16);return;}
      r.firstSample ??= values.captureStart;r.lastSample=values.captureStart+values.length;
    }
    if(!this.noteGaps(r,values))return;
    r.samples+=values.length;
    // Retain one complete segment for bounded offline recovery and cold VAD.
    r.pcm.push(values);
    if(!r.failed) {
      if(r.queue.length+r.pending>=(r.backlog?MAX_CARRY:!r.started?MAX_START:(this.page.e.now?.() ?? performance.now())-r.startAt<3000?MAX_WARM:MAX_PENDING))this.fail(6);
      else {r.queue.push(values);this.pump(r);}
    }
    if(r.samples>=r.limit&&!r.limitSent&&!r.stopping){r.limitSent=true;this.continuing=true;r.onlimit?.();}
  }
  pump(r) {
    if(r.started&&r.queue.length+r.pending<MAX_PENDING)r.backlog=false;
    while(r.started&&!r.failed&&r.queue.length&&r.pending<8)this.encode(r,r.queue.shift());
    if(r.finishing&&r.started&&!r.queue.length&&!r.pending)this.finishWorker(r);
  }
  encode(r,values) {
    if(r.failed)return;
    if(r.pending>=MAX_PENDING){this.fail(6);return;}
    r.encodeAt ??= this.page.e.now?.() ?? performance.now();
    const copy=values.slice();r.pending++;r.peak=Math.max(r.peak,r.pending);
    r.worker.postMessage({type:'pcm',id:r.id,samples:copy.buffer},[copy.buffer]);
  }
  recorder(continuation=false,legacyMime="audio/webm;codecs=opus") {
    const c=this,r={mimeType:'audio/webm;codecs=opus',state:'inactive',samples:0,prefixSamples:0,pending:0,peak:0,
      legacyMime,pcm:[],queue:[],id:this.page.e.crypto.randomUUID(),limit:Math.round(this.page.config.segmentMs/20)*960};
    r.start=()=>{
      if(c.closed)throw Error('capture.closed');
      const recoveryOnly=continuation&&c.page.captureDisabled;
      c.failed=0;if(!recoveryOnly)c.page.captureFailure=0;
      c.active=r;r.state='recording';c.initGaps(r);
      try{r.worker=c.worker();}catch(_){c.fail(3);r.worker={terminate(){},postMessage(){}};}
      r.worker.onerror=()=>c.fail(3);
      r.worker.onmessage=({data:m})=>{
        if(m.id!==r.id||r.finished||r.failed)return;
        if(m.type==='started'){
          c.w.clearTimeout(r.startTimer);
          r.metrics ||= {};
          r.metrics.client_capture_start_ms=bounded((c.page.e.now?.() ?? performance.now())-r.startAt,60000);
          r.started=true;r.backlog=r.queue.length>=MAX_PENDING;c.pump(r);
        }
        if(m.type==='ack'){r.acked=(r.acked||0)+1;r.pending--;c.throughput(r);c.pump(r);}
        if(m.type==='data')r.ondataavailable?.({data:new c.page.e.Blob([m.data],{type:r.mimeType})});
        if(m.type==='error'){r.clock=m.clock;r.metrics={...r.metrics,...m.metrics};c.page.captureDiagnostics=m.metrics;c.fail(m.code);}
        if(m.type==='done'){
          r.finished=true;c.w.clearTimeout(r.startTimer);c.ending=null;r.worker.terminate();if(!r.retainPCM){r.pcm.forEach(a=>a.fill(0));r.pcm=[];}r.stats=m;Object.assign(r.stats.metrics,r.gapMetrics,r.metrics);
          c.page.captureMuxFailures=0;c.page.captureFailure=0;c.page.captureDiagnostics=null;r.state='inactive';r.onstop?.();
        }
      };
      // Defer encoder construction until onset; idle has no live encoder.
      r.open=()=>{
        r.startAt ??= c.page.e.now?.() ?? performance.now();
        r.modelColdAtStart ??= c.page.modelReady===false;
        r.metrics ||= {};
        r.metrics.client_capture_start_probe ??= Number(!!c.page.capability?.running&&!c.page.capability.structureDone);
        c.page.capability?.yieldToCapture();
        r.startTimer=c.w.setTimeout(()=>{if(!r.started&&!r.finished&&!r.failed)c.fail(6);},3000);
        r.worker.postMessage({type:'start',id:r.id});
      };
      r.onbegin=()=>r.open();
      if(continuation) {
        c.continuing=false;r.begun=true;
        if(recoveryOnly){r.failed=c.page.captureFailure||4;r.worker.terminate();r.onfallback?.(r.failed);}else r.open();
        const buffered=c.carry.splice(0);r.backlog=buffered.length>=MAX_PENDING;
        for(const frame of buffered)c.push(r,frame);
      } else {
        c.floor=0;c.continuing=false;
        if(c.recordArmed){c.recordArmed=false;c.carry.forEach(a=>a.fill(0));c.carry=[];}
        else {c.clear();c.node.port.postMessage({type:'record'});}
        for(const frame of c.ring)if(!c.noteGaps(r,frame))break;
      }
    };
    r.completeStop=()=>{
      if(c.active===r)c.active=null;
      c.page.captureLastEnd=c.page.e.now?.() ?? performance.now();
      if(!r.begun) {r.idlePCM=c.ring.splice(0);r.finished=true;r.worker?.terminate();queueMicrotask(()=>r.onstop?.());return;}
      if(r.failed){c.ending=r;r.finishing=true;void c.recover(r);return;}
      c.ending=r;r.finishing=true;c.pump(r);
    };
    r.stop=()=>{
      if(r.stopping)return;r.stopping=true;r.state='inactive';
      c.continuing=!!r.continueAfter;
      if(c.continuing||c.closed){r.completeStop();return;}
      c.stopping=r;c.node.port.postMessage({type:'flush'});
      r.tailTimer=c.w.setTimeout(()=>{if(c.stopping===r){c.stopping=null;r.onerror?.();r.completeStop();}},300);
    };
    return r;
  }
  async encodeIdle(r) {
    const frames=r.idlePCM||[];if(!frames.length)return false;
    r.idlePCM=[];r.finished=false;r.finishing=true;r.finishSent=false;r.started=false;r.failed=false;
    r.samples=0;r.pending=0;r.queue=[];r.prefixSamples=frames.reduce((n,a)=>n+a.length,0);
    const old=r.worker,stop=r.onstop;let timer;
    try {
      return await new Promise(resolve=>{
        r.worker=this.worker();
        r.onstop=()=>resolve(true);
        r.worker.onerror=()=>resolve(false);
        r.worker.onmessage=event=>{
          if(event.data.type==='error'){resolve(false);return;}
          old.onmessage(event);
        };
        r.open();for(const frame of frames)this.push(r,frame);
        timer=this.w.setTimeout(()=>resolve(false),1500);
      });
    } finally {
      this.w.clearTimeout(timer);r.worker.terminate();r.onstop=stop;
      r.pcm.forEach(a=>a.fill(0));r.pcm=[];
    }
  }
  finishWorker(r) {
    if(r.finishSent)return;r.finishSent=true;
    r.worker.postMessage({type:'finish',id:r.id});
  }
  fail(code) {
    if(this.closed)return;
    const active=this.active||this.stopping||this.ending;
    if(code===7 && !active && !this.continuing) {
      const disabled=this.page.captureDisabled;
      if(!disabled)this.page.captureFailure=7;
      this.page.captureNeedsRebuild=!disabled;
      this.page.captureDisabled=!!disabled||(this.page.captureRebuilds||0)>=3;
      if(this.page.captureInputDiagnostics)this.page.captureInputDiagnostics.client_capture_input_recovery=disabled?4:this.page.captureDisabled?3:1;
      this.armReject?.(7);this.close();return;
    }
    if(this.failed){
      if(code===7){(this.active||this.stopping||this.ending)?.onerror?.();this.close();}
      return;
    }
    this.armReject?.(code);this.failed=code;this.page.captureFailure=code;
    const r=this.active||this.stopping||this.ending;
    if(r){this.throughput(r);r.failed=code;this.w.clearTimeout(r.startTimer);r.worker?.terminate();r.queue=[];r.onfallback?.(code);if(r.finishing)void this.recover(r);}
    if(!r&&this.continuing&&this.page.series)this.page.series.failed="capture";
    // Mux/queue/timeout failures retry on the next segment. Successful completion
    // resets the streak; other failures immediately select native capture.
    if([5,6,8].includes(code))this.page.captureMuxFailures=(this.page.captureMuxFailures||0)+1;
    this.page.captureDisabled=![5,6,8].includes(code)||this.page.captureMuxFailures>=2;
  }
  throughput(r) {
    r.metrics ||= {};
    const ms=(this.page.e.now?.() ?? performance.now())-(r.encodeAt ?? r.startAt);
    if(Number.isFinite(ms))r.metrics.client_capture_encode_wall_ms=bounded(ms,60000);
    r.metrics.client_capture_ack_frames=bounded(r.acked,2000);
    r.metrics.client_capture_model_overlap=Number(!!r.modelColdAtStart);
    if(r.modelColdAtStart)r.metrics.client_capture_model_overlap_ms=bounded(
      Math.max(0,Math.min(this.page.modelReadyAt ?? this.page.e.now?.() ?? performance.now(),this.page.e.now?.() ?? performance.now())-r.startAt),60000);
    r.metrics.client_capture_pending_frames=bounded(r.queue.length+r.pending,1550);
  }
  async offlineOpus(r) {
    let worker,timer;
    try {
      return await new Promise((resolve,reject)=>{
        worker=this.worker();const parts=[];let bytes=0;
        timer=this.w.setTimeout(()=>reject(0),2500);
        worker.onerror=()=>reject(0);
        worker.onmessage=({data:m})=>{
          if(m.id!==r.id)return;
          if(m.type==='error')reject(0);
          if(m.type==='data') {
            bytes+=m.data.byteLength;
            if(bytes>(this.page.config.maxAudioBytes ?? 2097152)){reject(0);return;}
            parts.push(m.data);
          }
          if(m.type==='done')bytes?resolve(new this.page.e.Blob(parts,{type:'audio/webm;codecs=opus'})):reject(0);
        };
        const frames=r.pcm.map(a=>a.slice().buffer);
        worker.postMessage({type:'recover',id:r.id,frames},frames);
      });
    } finally {this.w.clearTimeout(timer);worker?.terminate();}
  }
  async recover(r) {
    if(r.recovering)return;r.recovering=true;
    const began=this.page.e.now?.() ?? performance.now();
    r.metrics ||= {};this.throughput(r);
    try {
      const blob=await this.offlineOpus(r);
      r.mimeType=blob.type;r.onrecover?.(r.mimeType);
      r.metrics.client_capture_recovery_kind=3;
      r.metrics.client_capture_recovery_ms=bounded((this.page.e.now?.() ?? performance.now())-began,60000);
      r.metrics.client_capture_recovery_bytes=blob.size;
      r.ondataavailable?.({data:blob});r.finished=true;this.ending=null;
      if(!r.retainPCM){r.pcm.forEach(a=>a.fill(0));r.pcm=[];}
      r.onstop?.();return;
    }catch(_){/* Unsupported/failed offline Opus falls back to bounded WAV. */}
    try {
      const count=Math.ceil(r.samples/3);
      if(!count || 44+count*2>(this.page.config.maxAudioBytes ?? 2097152))throw 0;
      // Offline resampling provides the browser's anti-aliasing filter, without
      // playing captured speech or waiting for its real-time duration.
      const offline=new this.w.OfflineAudioContext(1,count,16000);
      const buffer=offline.createBuffer(1,r.samples,RATE),pcm=buffer.getChannelData(0);
      let offset=0;for(const frame of r.pcm){pcm.set(frame,offset);offset+=frame.length;}
      if(offset!==r.samples)throw 0;
      const source=offline.createBufferSource();source.buffer=buffer;source.connect(offline.destination);source.start();
      let timeout;
      const rendered=await Promise.race([offline.startRendering(),new Promise((_,reject)=>{
        timeout=this.w.setTimeout(()=>reject(0),1500);
      })]).finally(()=>this.w.clearTimeout(timeout));
      const output=new ArrayBuffer(44+count*2),view=new DataView(output);
      const text=(at,value)=>{for(let i=0;i<value.length;i++)view.setUint8(at+i,value.charCodeAt(i));};
      text(0,'RIFF');view.setUint32(4,36+count*2,true);text(8,'WAVE');text(12,'fmt ');
      view.setUint32(16,16,true);view.setUint16(20,1,true);view.setUint16(22,1,true);
      view.setUint32(24,16000,true);view.setUint32(28,32000,true);view.setUint16(32,2,true);view.setUint16(34,16,true);
      text(36,'data');view.setUint32(40,count*2,true);
      const values=rendered.getChannelData(0);
      for(let i=0;i<count;i++){const v=Math.max(-1,Math.min(1,values[i]));view.setInt16(44+i*2,Math.round(v*(v<0?32768:32767)),true);}
      r.mimeType='audio/wav';r.onrecover?.(r.mimeType);
      r.metrics.client_capture_recovery_kind=1;
      r.metrics.client_capture_recovery_ms=bounded((this.page.e.now?.() ?? performance.now())-began,60000);
      r.metrics.client_capture_recovery_bytes=output.byteLength;
      r.ondataavailable?.({data:new this.page.e.Blob([output],{type:r.mimeType})});
      r.finished=true;this.ending=null;
      if(!r.retainPCM){r.pcm.forEach(a=>a.fill(0));r.pcm=[];}r.onstop?.();return;
    }catch(_){/* Last resort for browsers without offline resampling. */}
    r.metrics.client_capture_recovery_kind=2;
    // Native MediaRecorder cannot encode past PCM faster than realtime. Re-play
    // into a PRIVATE, non-audible destination, with the exact same segment ID.
    let source,destination,native,timer;
    try {
      destination=this.context.createMediaStreamDestination();
      try{native=new this.page.e.MediaRecorder(destination.stream,{mimeType:r.legacyMime,audioBitsPerSecond:24000});}
      catch(_){native=new this.page.e.MediaRecorder(destination.stream,{mimeType:r.legacyMime});}
      r.mimeType=native.mimeType;r.onrecover?.(native.mimeType);
      const data=[];
      await new Promise((resolve,reject)=>{
        native.ondataavailable=e=>{if(e.data?.size)data.push(e.data);};
        native.onerror=reject;native.onstop=resolve;
        const buffer=this.context.createBuffer(1,r.samples,RATE),pcm=buffer.getChannelData(0);
        let pos=0;for(const v of r.pcm){pcm.set(v,pos);pos+=v.length;}
        source=this.context.createBufferSource();source.buffer=buffer;source.connect(destination);
        source.onended=()=>{if(native.state!=='inactive')native.stop();};
        native.start(250);source.start();
        timer=this.w.setTimeout(reject,Math.ceil(r.samples/RATE*1000)+2000);
      });
      const blob=new this.page.e.Blob(data,{type:r.mimeType});r.metrics.client_capture_recovery_bytes=bounded(blob.size,2097152);
      r.ondataavailable?.({data:blob});
      r.metrics.client_capture_recovery_ms=bounded((this.page.e.now?.() ?? performance.now())-began,60000);
      r.finished=true;this.ending=null;r.onstop?.();
    }catch(_){r.onerror?.();r.finished=true;this.ending=null;r.onstop?.();}
    finally {
      this.w.clearTimeout(timer);try{source?.stop();source?.disconnect();}catch(_){}
      try{if(native?.state!=='inactive')native?.stop();}catch(_){}
      for(const track of destination?.stream.getTracks()||[])track.stop();
      if(!r.retainPCM){r.pcm.forEach(a=>a.fill(0));r.pcm=[];}r.worker?.terminate();
    }
  }
  clear(){for(const a of [...this.ring,...this.carry])a.fill(0);this.ring=[];this.carry=[];}
  close() {
    if(this.closed)return;this.closed=true;this.recordReject?.(7);this.recordArmed=false;this.probeCancel?.();this.armReject?.(7);for(const [target,name,fn] of this.listeners||[])target.removeEventListener(name,fn);this.listeners=[];this.w.clearTimeout(this.armTimer);if(!this.stopping)this.clear();this.probe?.terminate();
    for(const r of [this.active,this.stopping,this.ending])if(r)this.w.clearTimeout(r.startTimer);
    this.node?.port.postMessage({type:'close'});
    for(const node of [this.source,this.node,this.gain])try{node?.disconnect();}catch(_){}
    if(!this.stopping)this.node?.port.close();
  }
}
