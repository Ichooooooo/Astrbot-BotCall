// Local Silero v5 signal only: no permission, network, audio or inference on import.
// 512@16k, 64 context and [2,1,128] state adapted from vad-web@0.0.31
// dist/models/silero.js (ISC/MIT). Ownership/queue/cleanup are Bot-Call code.
const BASE = "/bot-call/vendor/vad-v5-ort-1.22.0-2623a295/";
export const VAD_MAX_FRAMES = 16; // 512ms of 512@16k frames, not a wall-clock deadline.
const METRIC_LIMITS = {resourceMs:600000,sessionMs:600000,warmupMs:600000,awaitPeakMs:600000,queuePeak:VAD_MAX_FRAMES-1,overflowFrames:VAD_MAX_FRAMES,gapCount:65,lostMs:60000,startGapCount:65,startSkippedMs:60000};
function metricValue(value,key,limitKey=key) {
  try {
    const number = Object.getOwnPropertyDescriptor(value,key)?.value;
    return typeof number === "number" && Number.isSafeInteger(number) && number>=0 && number<=METRIC_LIMITS[limitKey] ? number : null;
  } catch (_) {return null;}
}
function metric(target,key,value) {
  try {
    if (typeof value !== "number" || !Number.isSafeInteger(value) || value<0 || value>METRIC_LIMITS[key]) return;
    const old = Object.getOwnPropertyDescriptor(target,"metrics")?.value, next = {};
    for (const name of Object.keys(METRIC_LIMITS)) next[name] = metricValue(old,name);
    next[key] = Math.max(next[key] || 0,value); target.metrics = Object.freeze(next);
  } catch (_) { /* Observation never controls inference, ACK or cleanup. */ }
}
function clock() {
  try {const value=performance.now();return typeof value === "number" && Number.isFinite(value) && value>=0 ? value : null;}
  catch (_) {return null;}
}
function elapsed(start) {
  const end=clock();return start !== null && end !== null && end>=start ? Math.ceil(end-start) : null;
}
let runtimeTask;
function runtime(win) {
  if (!runtimeTask) runtimeTask = new Promise((resolve,reject) => {
    const script = win.document.createElement("script"); script.src = BASE+"ort.wasm.min.js";
    script.onload = () => resolve(win.ort); script.onerror = () => reject(new Error("vad.unavailable"));
    win.document.head.append(script);
  });
  return runtimeTask;
}
const DIAGNOSTIC_STAGES = new Set(["prepare","runtime","resource","model","warmup","worklet","arm","quality","queue","inference","tail","closed","ready"]);
const DIAGNOSTIC_REASONS = new Set(["preparing","ready","context_inactive","worklet_missing","sample_rate","resource_failed","initialization_failed","arm_unconfirmed","channels","clock","sample","overflow","capacity","sequence","queue","inference","tail","processor","analysis","track","cancelled","unknown"]);
const attempt = action => {try {action();} catch (_) { /* Auxiliary browser failure cannot steal model ownership. */ }};
const dispose = value => {try {value?.dispose();} catch (_) { /* Fixed failure only. */ }};
export class SileroSignalModel {
  static async create(ort,bytes) {
    const session = await ort.InferenceSession.create(bytes,{executionProviders:["wasm"]});
    try {return new SileroSignalModel(ort,session);} catch (_) {await session.release(); throw new Error("vad.unavailable");}
  }
  constructor(ort,session) {
    this.ort = ort; this.session = session; this.context = new Float32Array(64);
    this.state = new ort.Tensor("float32",new Float32Array(256),[2,1,128]);
    try {this.sr = new ort.Tensor("int64",[16000n]);} catch (_) {dispose(this.state); throw new Error("vad.unavailable");}
  }
  reset() {
    if (this.running || this.closed) throw new Error("vad.busy");
    const state = new this.ort.Tensor("float32",new Float32Array(256),[2,1,128]);
    dispose(this.state); this.state = state; this.context.fill(0);
  }
  run(frame) {
    if (this.running || this.closed) return Promise.reject(new Error("vad.busy"));
    this.running = this.perform(frame).finally(() => {this.running = null;});
    return this.running;
  }
  async perform(frame) {
    let input, out; const old = this.state;
    try {
      if (!(frame instanceof Float32Array) || frame.length !== 512) throw new Error("vad.invalid");
      const values = new Float32Array(576); values.set(this.context); values.set(frame,64);
      input = new this.ort.Tensor("float32",values,[1,576]);
      out = await this.session.run({input,state:old,sr:this.sr});
      const p = out?.output?.data?.[0], state = out?.stateN;
      if (!state || state.type !== "float32" || state.data?.length !== 256 ||
          state.dims?.join(",") !== "2,1,128" || state.data.some(v=>!Number.isFinite(v)) || !Number.isFinite(p) || p<0 || p>1)
        throw new Error("vad.invalid");
      this.state = state; this.context.set(frame.subarray(448));
      return p;
    } finally {
      const owned = new Set([input,...Object.values(out || {})]);
      if (old !== this.state) owned.add(old);
      for (const tensor of owned) if (tensor !== this.state && tensor !== this.sr) dispose(tensor);
    }
  }
  close() {
    if (!this.closeTask) this.closeTask = (async () => {
      this.closed = true;
      try {await this.running;} catch (_) { /* Still wait actual run. */ }
      try {await this.session.release();} finally {dispose(this.state);dispose(this.sr);this.context.fill(0);}
    })();
    return this.closeTask;
  }
}
export function validateVAD(options) {
  for (const key of ["vadPositive","vadNegative"]) if (!Number.isFinite(options[key]) || options[key]<=0 || options[key]>=1) throw new Error("vad.configuration");
  if (options.vadNegative >= options.vadPositive) throw new Error("vad.configuration");
  for (const [key,min,max] of [["vadRedemptionMs",100,5000],["vadMinSpeechMs",32,1000]])
    if (!Number.isSafeInteger(options[key]) || options[key]<min || options[key]>max) throw new Error("vad.configuration");
}
export class LocalVAD {
  constructor(win,loadRuntime=()=>runtime(win)) {
    this.w = win; this.loadRuntime = loadRuntime; this.generation = 0; this.sequence = 0;
    metric(this,"queuePeak",0);
  }
  note(stage,reason) {
    const valid = typeof stage === "string" && DIAGNOSTIC_STAGES.has(stage) &&
      typeof reason === "string" && DIAGNOSTIC_REASONS.has(reason);
    return this.diagnostic = Object.freeze(valid ? {stage,reason} : {stage:"prepare",reason:"unknown"});
  }
  ready(context) {return !!this.model && this.context === context && !this.preparing && !this.closing && !this.failed;}
  prepare(stream,context) {
    if (this.closing || this.failed) return Promise.resolve(false);
    if (this.ready(context)) return Promise.resolve(true);
    if (this.model) return Promise.resolve(false); // Close the old context owner first.
    if (this.preparing) return this.preparing;
    const generation = this.generation;
    this.note("prepare","preparing");
    this.preparing = this.initialize(stream,context,generation).catch(() => false)
      .finally(() => {this.preparing = null;});
    return this.preparing;
  }
  prepareModel() {
    if (this.cachedModel) return Promise.resolve(true);
    if (this.modelTask) return this.modelTask;
    const generation = this.modelGeneration || 0;
    this.modelTask = (async () => {
      let model, stage="runtime", reason="resource_failed";
      const owned = () => generation === (this.modelGeneration || 0), started=clock();
      try {
        this.note(stage,"preparing");
        const ort = await this.loadRuntime();
        if (!owned()) return false;
        ort.env.logLevel="fatal";ort.env.wasm.numThreads=1;ort.env.wasm.proxy=false;ort.env.wasm.wasmPaths=BASE;
        stage="resource";this.note(stage,"preparing");
        this.abort = new this.w.AbortController();
        const response=await this.w.fetch(BASE+"silero_vad_v5.onnx",{signal:this.abort.signal,priority:"low"});
        if (!response.ok) throw new Error("vad.unavailable");
        const bytes=await response.arrayBuffer();
        if (!owned()) return false;
        if(bytes.byteLength!==2327524)throw new Error("vad.invalid");
        stage="model";reason="initialization_failed";this.note(stage,"preparing");
        const sessionStarted=clock();
        try {model=await SileroSignalModel.create(ort,bytes);} finally {metric(this,"sessionMs",elapsed(sessionStarted));}
        if(!owned())return false;
        stage="warmup";reason="inference";this.note(stage,"preparing");
        const started=clock();
        try {await model.run(new Float32Array(512));} finally {metric(this,"warmupMs",elapsed(started));}
        if(!owned())return false;
        model.reset();this.cachedModel=model;this.note("ready","ready");return true;
      } catch (_) {if(owned())this.note(stage,reason);return false;}
      finally {
        // Same-origin Resource Timing includes runtime, ONNX, MJS and WASM.
        // Union intervals avoid double counting parallel requests; sessionMs
        // includes the WASM request and overlaps this figure.
        try {
          const entries=this.w.performance?.getEntriesByType("resource"), origin=this.w.location?.origin;
          if(started!==null && typeof origin==="string" && Array.isArray(entries)) {
            const spans=entries.filter(e=>typeof e.name==="string" && e.name.startsWith(origin+BASE) &&
              Number.isFinite(e.startTime) && e.startTime>=started && Number.isFinite(e.responseEnd) && e.responseEnd>=e.startTime)
              .map(e=>[e.startTime,e.responseEnd]).sort((a,b)=>a[0]-b[0]);
            if(spans.length) {
              let total=0,end=0;for(const [a,b] of spans){total+=Math.max(0,b-Math.max(a,end));end=Math.max(end,b);}
              metric(this,"resourceMs",Math.ceil(total));
            }
          }
        } catch (_) { /* Timing support/observation never controls model ownership. */ }
        if(model && this.cachedModel!==model)await model.close();this.abort=null;
      }
    })();
    return this.modelTask;
  }
  async classifyPCM(frames,options,budgetMs=2500,coverTail=false) {
    if(this.offlineTask || this.active || this.closing)return null;
    const began=clock(),generation=this.modelGeneration||0;
    const lease={cancelled:false},expired=()=>lease.cancelled || (elapsed(began)||0)>=budgetMs;
    let cancelWait;const cancelled=new Promise(resolve=>{cancelWait=resolve;});
    const task=this.offlineTask=(async()=>{
      let buffer,rendered;
      try {
        if(!await Promise.race([this.prepareModel(),cancelled]) || expired() || generation!==(this.modelGeneration||0))return null;
        const model=this.cachedModel;
        if(!model || model.running || this.active)return null;
        const count=frames.reduce((n,a)=>n+a.length,0);
        if(!count || count>1488000 || frames.some(a=>a.some(v=>!Number.isFinite(v)||Math.abs(v)>1)))return null;
        const offline=new this.w.OfflineAudioContext(1,Math.ceil(count/3),16000);
        buffer=offline.createBuffer(1,count,48000);const values=buffer.getChannelData(0);
        let offset=0;for(const frame of frames){values.set(frame,offset);offset+=frame.length;}
        // No references to caller-owned PCM survive the first asynchronous run.
        const source=offline.createBufferSource();source.buffer=buffer;source.connect(offline.destination);source.start();
        rendered=await offline.startRendering();
        if(expired() || generation!==(this.modelGeneration||0) || model.running)return null;
        model.reset();let speechMs=0,highConfidenceMs=0,highConfidenceFrames=0,peakProbability=0;
        const pcm=rendered.getChannelData(0);
        for(let at=0;at<pcm.length;at+=512) {
          if(expired() || generation!==(this.modelGeneration||0))return null;
          const frame=new Float32Array(512),valid=Math.min(512,pcm.length-at);frame.set(pcm.subarray(at,at+valid));
          const p=await model.run(frame);frame.fill(0);
          peakProbability=Math.max(peakProbability,p);
          if(p>=options.vadPositive)speechMs+=valid/16;
          if(p>=.8){highConfidenceMs+=valid/16;if(valid===512)highConfidenceFrames++;}
        }
        // A high partial tail is not a complete high-confidence frame.
        // For a valid online detector with missing coverage only, replay with
        // frames ending at the real PCM edge. Never pad missing input into
        // qualifying speech; keep the cold-start pass and its rules unchanged.
        const tail=pcm.length%512;
        if(coverTail && tail && pcm.length>=512 && speechMs<96 && !highConfidenceFrames && peakProbability>=.8) {
          if(expired() || generation!==(this.modelGeneration||0))return null;
          model.reset();let alignedSpeech=0,alignedHigh=0,alignedFrames=0;
          for(let at=tail;at+512<=pcm.length;at+=512) {
            if(expired() || generation!==(this.modelGeneration||0))return null;
            const frame=pcm.slice(at,at+512);
            const p=await model.run(frame);frame.fill(0);
            peakProbability=Math.max(peakProbability,p);
            if(p>=options.vadPositive)alignedSpeech+=32;
            if(p>=.8){alignedHigh+=32;alignedFrames++;}
          }
          // Independent passes overlap: take their votes/maxima, never add.
          speechMs=Math.max(speechMs,alignedSpeech);
          highConfidenceMs=Math.max(highConfidenceMs,alignedHigh);
          highConfidenceFrames=Math.max(highConfidenceFrames,alignedFrames);
        }
        if(expired())return null;
        return {valid:true,coverageGap:false,speechMs,highConfidenceMs,highConfidenceFrames,peakProbability,
          speechBeforeControlMs:speechMs,candidate:speechMs>0};
      }catch(_){return null;}
      finally {
        buffer?.getChannelData(0).fill(0);rendered?.getChannelData(0).fill(0);
        if(this.cachedModel && !this.cachedModel.running && !this.active)attempt(()=>this.cachedModel.reset());
      }
    })().finally(()=>{if(this.offlineTask===task)this.offlineTask=null;});
    let timer;
    try {
      return await Promise.race([task,new Promise(resolve=>{
        timer=this.w.setTimeout(()=>{lease.cancelled=true;cancelWait(false);resolve(null);},Math.max(1,budgetMs-(elapsed(began)||0)));
      })]);
    } finally {this.w.clearTimeout(timer);}
  }
  async initialize(stream,context,generation) {
    let model, source, node, gain, stage="prepare", reason="unknown";
    const owned = () => generation === this.generation && context.state === "running";
    try {
      if (!context.audioWorklet || !this.w.AudioWorkletNode) {this.note("worklet","worklet_missing");return false;}
      if (context.sampleRate<16000 || context.sampleRate>192000) {this.note("prepare","sample_rate");return false;}
      if (!await this.prepareModel()) return false;
      model=this.cachedModel;
      if (!owned()) {this.note("prepare","context_inactive");return false;}
      stage="worklet";reason="initialization_failed";this.note(stage,"preparing");
      await context.audioWorklet.addModule("/bot-call/vad-worklet.js"); if (!owned()) {if (generation === this.generation) this.note(stage,"context_inactive");return false;}
      node = new this.w.AudioWorkletNode(context,"bot-call-voice-signal",{numberOfInputs:1,numberOfOutputs:1,outputChannelCount:[1],
        channelCount:1,channelCountMode:"explicit",channelInterpretation:"speakers"});
      gain = context.createGain(); gain.gain.value = 0;
      source = context.createMediaStreamSource(stream); source.connect(node); node.connect(gain); gain.connect(context.destination);
      node.port.onmessage = ({data}) => {try {this.message(data);} catch (_) {if(this.active)this.fail(this.active);}};
      node.onprocessorerror = () => {if(this.active)this.fail(this.active,"worklet","processor");else this.note("worklet","processor");};
      this.model = model; this.context = context; this.node = node; this.source = source; this.gain = gain;
      this.note("ready","ready");
      return true;
    } catch (error) {
      if (generation === this.generation) this.note(stage,reason);
      throw error;
    } finally {
      if (this.model !== model || !owned()) {
        attempt(()=>source?.disconnect());attempt(()=>node?.port.postMessage({type:"shutdown"}));
        attempt(()=>node?.disconnect());attempt(()=>gain?.disconnect());attempt(()=>node?.port.close());
        if (model && model !== this.cachedModel) {
          try {await model.close();} catch (_) {this.failed = true;this.quarantined = model;}
        }
      }
      this.abort = null;
    }
  }
  async begin(options,event,continuation=false) {
    if (!this.model || this.active || this.offlineTask || this.closing) return null;
    this.note("arm","arm_unconfirmed");
    validateVAD(options);
    const previous=continuation && this.continuation;
    this.continuation=null;
    if(!previous)this.model.reset();
    const s = {id:++this.sequence,options,event,valid:true,candidate:false,voiced:false,
      speechSpans:[],highSpans:[],peakProbability:0,queue:[],received:0,processed:0,positiveMs:previous?.positiveMs || 0,lastCandidate:previous?.lastCandidate || 0,diagnostic:this.diagnostic};
    s.voiced=!!previous?.voiced;s.continuityLostMs=previous?.continuityLostMs||0;
    // Bounded numeric observation of the existing worklet; no PCM retained.
    attempt(()=>{if(!this.captureProbe)this.captureProbe={started:clock(),frames:0};});
    s.begun = new Promise(resolve => {s.beginResolve = resolve;});
    s.finished = new Promise(resolve => {s.finishResolve = resolve;}); this.active = s;
    attempt(()=>metric(s,"warmupMs",metricValue(Object.getOwnPropertyDescriptor(this,"metrics")?.value,"warmupMs")));
    metric(s,"gapCount",0);metric(s,"lostMs",0);metric(s,"startGapCount",0);metric(s,"startSkippedMs",0);
    metric(s,"queuePeak",0); // Await/overflow remain unknown until actually observed.
    attempt(()=>this.node.port.postMessage({type:"begin",id:s.id,maxFrames:VAD_MAX_FRAMES,settle:!previous}));
    const timer = this.w.setTimeout(() => {this.fail(s,"arm","arm_unconfirmed");s.beginResolve(false);},300);
    try {await s.begun; return s;} finally {this.w.clearTimeout(timer);}
  }
  emit(s,event) {
    try {s.event(event);} catch (_) {s.valid = false;}
  }
  fail(s,stage="quality",reason="unknown") {
    if (!s.valid) return;
    s.diagnostic = this.note(stage,reason);
    s.valid = false; this.emit(s,"unavailable");
  }
  message(m) {
    const s = this.active; if (!s || s.closed || m.id !== s.id) return;
    if(m.type==="startup-gap") {
      // Diagnostic only: these samples precede MediaRecorder.start.
      metric(s,"startGapCount",m.count);metric(s,"startSkippedMs",m.skippedMs);return;
    }
    if (m.type === "begun") {
      s.start = s.edge = m.start; s.rate = m.rate;
      if (m.maxFrames !== VAD_MAX_FRAMES) this.fail(s,"queue","capacity");
      else if (!Number.isSafeInteger(m.start) || (!Number.isSafeInteger(m.rate) || m.rate<16000 || m.rate>192000)) this.fail(s,"quality","clock");
      else if (s.valid) s.diagnostic = this.note("ready","ready");
      s.beginResolve(true); return;
    }
    if (m.type === "gap") {
      if(!Number.isSafeInteger(m.start) || m.start<s.edge || !Number.isSafeInteger(m.count) || m.count!==(s.gapCount || 0)+1 ||
          !Number.isSafeInteger(m.lostMs) || m.lostMs<0 || m.lostMs<(s.lostMs || 0)) {this.fail(s,"quality","clock");return;}
      const duration=Math.max((m.start-s.edge)/s.rate*1000,m.lostMs-(s.lostMs||0));
      // Same 50 ms single / 200 ms total budget, including discarded overlaps.
      // This preserves evidence only: it never supplies missing voice/quiet.
      s.continuityLostMs=(s.continuityLostMs||0)+duration;s.nextGapMs=(s.nextGapMs||0)+duration;
      s.nextKeepPositive=(s.nextKeepPositive??true)&&m.reason==='clock'&&s.nextGapMs<=50&&s.continuityLostMs<=200;
      s.gapCount=m.count;s.lostMs=m.lostMs;s.edge=m.start;s.nextReset=true;s.coverageGap=true;
      metric(s,"gapCount",m.count);metric(s,"lostMs",m.lostMs);
      if(duration>500 || m.lostMs>2000 || m.count>64)this.fail(s,"quality",m.reason==="overflow"?"overflow":"clock");
      return;
    }
    if (m.type === "fault") {
      const reason = typeof m.reason === "string" && ["channels","clock","sample","overflow","capacity","tail"].includes(m.reason) ? m.reason : "unknown";
      if (reason === "overflow") metric(s,"overflowFrames",metricValue(m,"unacked","overflowFrames"));
      this.fail(s,["overflow","capacity"].includes(reason) ? "queue" : reason === "tail" ? "tail" : "quality",reason);return;
    }
    attempt(()=>{
      const p=this.captureProbe,now=clock();
      if(!p || p.frames>=64 || now===null || p.started===null || now-p.started>2000)return;
      if(m.type==="frame") {
        if(!p.frames)p.first_ms=now-p.started;
        else p.gap_peak_ms=Math.max(p.gap_peak_ms || 0,now-p.last);
        p.last=now;p.frames++;
      }
    });
    if (m.type === "frame") {
      const samples = new Float32Array(m.samples);
      if (m.seq !== s.received+1 || m.start !== s.edge || !Number.isSafeInteger(m.end) || m.end<=m.start ||
          samples.length !== 512 || !Number.isSafeInteger(m.valid) || m.valid<1 || m.valid>512 ||
          samples.some(v=>!Number.isFinite(v)||Math.abs(v)>1) || s.queue.length >= VAD_MAX_FRAMES-1) {
        const reason = s.queue.length >= VAD_MAX_FRAMES-1 ? "queue" : m.seq !== s.received+1 || m.start !== s.edge ? "sequence" : "sample";
        this.fail(s,reason === "queue" ? "queue" : "quality",reason); attempt(()=>this.node?.port.postMessage({type:"ack",id:s.id,seq:m.seq})); return;
      }
      s.received = m.seq; s.edge = m.end; s.queue.push({...m,samples,reset:s.nextReset,keepPositive:s.nextKeepPositive});s.nextReset=false;s.nextKeepPositive=undefined;s.nextGapMs=0;
      if (!s.drain) s.drain = this.drain(s).finally(() => {s.drain = null;this.complete(s);});
      metric(s,"queuePeak",s.queue.length);
    } else if (m.type === "flushed") {s.end = m;if(m.bad)this.fail(s,"tail","tail");this.complete(s);}
  }
  async drain(s) {
    while (s.queue.length) {
      const frame = s.queue.shift();
      try {
        if (s.valid) {
          if(frame.reset) {
            // A gap restarts the quiet interval, but does not erase
            // the fact that this utterance already contained confirmed speech.
            // Otherwise a gap after speech leaves a valid detector listening
            // forever: subsequent silence can never set voiced again.
            this.model.reset();s.lastCandidate=frame.start;
            if(!frame.keepPositive){s.positiveMs=0;s.longBreak=frame.start;}
          }
          const started=clock();let p;
          try {p = await this.model.run(frame.samples);} finally {metric(s,"awaitPeakMs",elapsed(started));}
          if (this.active === s && s.valid) {
            s.peakProbability=Math.max(s.peakProbability,p);
            if(p>=.8)s.highSpans.push([frame.start,frame.end,frame.valid]);
            if (p >= s.options.vadNegative) {
              s.candidate = true; s.lastCandidate = frame.end;if(!s.stopping)this.emit(s,"candidate");
            }
            if (p >= s.options.vadPositive) {
              s.positiveMs += frame.valid/16;
              // Retention counts only this recorder's PCM, separately from
              // endpoint/continuation state. Merge adjacent positive frames.
              const spans=s.speechSpans,last=spans.at(-1);
              if(last && last[1]===frame.start)last[1]=frame.end;
              else spans.push([frame.start,frame.end]);
              if (s.positiveMs >= s.options.vadMinSpeechMs) s.voiced = true;
            }
            s.lastFrame=frame.end;
            if (!s.stopping && s.voiced && (frame.end-s.lastCandidate)/s.rate*1000 >= s.options.vadRedemptionMs && !s.endpoint) {
              s.endpoint = true; this.emit(s,"endpoint");
            }
          } else if (p >= s.options.vadNegative) s.candidate = true; // Final tail still latches retention.
        }
      } catch (_) {this.fail(s,"inference","inference");}
      finally {
        s.processed = frame.seq;
        attempt(()=>this.node?.port.postMessage({type:"ack",id:s.id,seq:frame.seq}));
      }
    }
  }
  complete(s) {
    if (!s.end || s.drain || s.queue.length) return;
    const end = s.end;
    const covered = s.valid && Number.isSafeInteger(s.recordStart) && Number.isSafeInteger(s.recordEnd) &&
      end.start === s.start && s.start <= s.recordStart && end.end >= s.recordEnd &&
      end.lastSeq === s.received && s.processed === s.received && end.lastSeq>0;
    if (!covered && s.valid) s.diagnostic = this.note("tail","tail");
    this.continuation=covered && s.continuing ? {positiveMs:s.positiveMs,lastCandidate:s.lastCandidate,voiced:s.voiced,continuityLostMs:s.continuityLostMs} : null;
    const positiveUntil = (edge,spans=s.speechSpans) => spans.reduce((ms,[a,b])=>
      ms+Math.max(0,Math.min(b,edge)-Math.max(a,s.recordStart))/s.rate*1000,0);
    s.finishResolve({valid:covered,candidate:s.candidate || !!s.coverageGap,coverageGap:!!s.coverageGap,
      speechMs:positiveUntil(s.recordEnd),highConfidenceMs:positiveUntil(s.recordEnd,s.highSpans),
      highConfidenceFrames:s.highSpans.filter(([a,b,valid])=>valid===512 && a>=s.recordStart && b<=s.recordEnd).length,
      peakProbability:s.peakProbability,speechBeforeControlMs:positiveUntil(Math.min(s.recordEnd,s.retentionEnd ?? s.recordEnd)),
      lastEvidenceMs:s.candidate && s.lastCandidate>=s.recordStart ? Math.max(0,(s.recordEnd-s.lastCandidate)/s.rate*1000) : undefined});
    if (this.active === s) this.active = null;
  }
  async finish(s,recordEnd,continuing=false) {
    if (!s || this.active !== s) return {valid:false,candidate:true};
    s.stopping = true; s.recordEnd = recordEnd;s.continuing=continuing;
    if (this.context.state !== "running") this.fail(s,"quality","context_inactive");
    try {this.node.port.postMessage({type:"flush",id:s.id,end:recordEnd});}
    catch (_) {this.fail(s,"tail","tail");return {valid:false,candidate:true};}
    let timer;
    try {
      return await Promise.race([s.finished,new Promise(resolve => {
        timer = this.w.setTimeout(() => {this.fail(s,"tail","tail");resolve({valid:false,candidate:true});},800);
      })]);
    } finally {this.w.clearTimeout(timer);}
  }
  close(keepModel=false) {
    if (this.closing) return keepModel ? this.closing : this.closing.then(()=>this.close(false));
    this.generation++;this.continuation=null;
    if (!keepModel) {this.modelGeneration=(this.modelGeneration || 0)+1;attempt(()=>this.abort?.abort());}
    const s = this.active;
    if (s) {s.closed = true;this.fail(s,"closed","cancelled");s.beginResolve(false);s.finishResolve({valid:false,candidate:true});s.stopping = true;}
    attempt(()=>this.node?.port.postMessage({type:"shutdown"}));
    attempt(()=>this.source?.disconnect());attempt(()=>this.node?.disconnect());attempt(()=>this.gain?.disconnect());
    this.closing = (async () => {
      try {await this.preparing;if(!keepModel)await this.modelTask;if (s?.drain) await s.drain;if(this.offlineTask)await this.offlineTask;}
      finally {
        try {if (!keepModel && (this.cachedModel || this.model)) await (this.cachedModel || this.model).close();}
        catch (_) {this.failed = true;this.quarantined = this.model;}
        finally {
          attempt(()=>this.node?.port.close());
          this.model = this.node = this.source = this.gain = this.context = this.active = null;
          if (!keepModel) this.cachedModel=this.modelTask=null;
        }
      }
    })().catch(() => {}).finally(() => {this.closing = null;});
    return this.closing;
  }
}
