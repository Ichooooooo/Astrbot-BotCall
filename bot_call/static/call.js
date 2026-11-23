import {ContinuousCapture} from "./capture.js";
import {EchoProbe} from "./echo.js";
import {PCMPlayer,PCM_FAILURE} from "./pcm.js";
import {BargeMethods,BARGE} from "./barge.js";
// No permission, HTTP or speech on import. Dependencies are explicit for offline tests.
const CONTROL_NOISE_WINDOW_MS = 300;
const TOOL_WAIT_CUE_DELAY_MS = 3000; // Reference: long tools take 3–15 s; short waits stay quiet.
const TOOL_WAIT_CUE_REPEAT_MS = 6000; // Space out reminders during a continuing wait.
const TOOL_WAIT_CUE_MAX_COUNT = 3; // Bound reminders even if a tool takes much longer.
const RETAIN_SPEECH_MS = 96; // Three Silero frames: above measured taps, below soft short words.
const CAPTURE_AUDIO_BITS_PER_SECOND = 24000; // Speech target; unsupported browsers use their default.
const TYPES = ["audio/webm;codecs=opus", "audio/mp4", "audio/ogg;codecs=opus", "audio/webm", "audio/ogg", "audio/wav"];
const ACCEPTED = new Set([...TYPES, "audio/webm; codecs=opus", "audio/ogg; codecs=opus"]);
const TERMINAL = new Set(["ended", "stopped", "failed"]);
const SPEECH_ERRORS = {
  "api-unavailable":"浏览器没有完整音频播放接口", "not-allowed":"播放需要点击恢复声音",
  "network":"音频获取失败", "decode":"浏览器无法解码音频", "unsupported":"浏览器不支持此音频",
  "synthesis-failed":"本轮TTS未取得可用音频，文字已保存", "unknown":"音频播放结果未确认"
};
const speechError = code => typeof code === "string" && Object.prototype.hasOwnProperty.call(SPEECH_ERRORS,code) ? code : "unknown";
const REFERENCE = "bot-call-reference";
const PUSH_AUDIO_MAX_BYTES = 48 * 1024;
const API_ROUTES = new Set(["barge-in","exchange","state","audio","utterances","end","heartbeat","playback","speech","speech-audio","segment-audio","segment-playback","metrics","audio-resolution","audio-chunk","audio-commit","audio-series","audio-series-commit","audio-series-control","local"]);
const API_PHASES = new Set(["local","clip_digest","preflight","fetch","http","body","validate"]);
const API_CONTEXTS = new Set(["request","clip_precheck","audio_submit","audio_readback","playback_readback","chunk_submit","audio_commit"]);
const CHUNK_ATTEMPTS = 3; // one request plus two identical re-sends before the whole clip goes the old way
const LANE_WAIT_MS = 90000; // safety net only: a lane holder that never reports must not block uploads forever
const CHUNK_UNUSABLE = [400,404,405,409,410,413,501]; // the staged chunks cannot lead to a commit; fall back to the whole clip
const API_REASONS = new Set(["local_unknown","session","network","timeout","http","invalid-response"]);
const API_CODES = new Set(["busy","ended","denied","conflict","invalid_request","unconfirmed"]);
const REQUEST_FAILURES = new WeakMap();
// Private identities only: browser errors/free cancelled properties confer no authority.
const STATE_READS = new WeakMap(), CANCELLED_READS = new WeakSet();
const LOCAL_FAILURE = Object.freeze({route:"local",stage:"local",reason:"local_unknown",status:0});
class RequestFailure extends Error {
  constructor(status=0, route="local", phase="local", reason="local_unknown", context="request", code=null) {
    super("call_page.request_unconfirmed");
    const safe = (value, allowed, fallback) => typeof value === "string" && allowed.has(value) ? value : fallback;
    const scope = safe(context,API_CONTEXTS,"request");
    REQUEST_FAILURES.set(this,Object.freeze({
      route:safe(route,API_ROUTES,"local"),
      stage:(scope === "request" ? "" : scope+".")+safe(phase,API_PHASES,"local"),
      reason:safe(reason,API_REASONS,"local_unknown"),
      code:safe(code,API_CODES,null),
      status:Number.isInteger(status) && status>=100 && status<=599 ? status : 0}));
  }
}
const failureData = error => REQUEST_FAILURES.get(error) || LOCAL_FAILURE;
const diagnostic = data => "接口诊断："+data.route+"/"+data.stage+"/"+data.reason+"，HTTP="+(data.status || "none")+"。";
function exchangeMessage(data) {
  if (data?.route !== "exchange") return null;
  if (data.status === 409 && data.code === "busy") return ["当前线路正在占线",
    "已有另一通电话正在进行。Bot-Call 同一时间仅支持一通通话。你可以稍后重试，或联系管理员在 AstrBot 后台“评测与诊断”查看并强制挂断占线通话。", "connect"];
  if (data.status === 409 && data.code === "ended") return ["这通电话或邀请已结束", "请回到聊天发送 /botcall 重新发起通话。"];
  if (data.status === 401 && data.code === "denied") return ["这条通话链接无效或已过期", "请回到聊天发送 /botcall 获取新链接。"];
  return null;
}
function closeMessage(state) {
  const detail = state?.close_detail ?? state?.limits?.close_detail;
  if (detail === "idle") return "长时间未发言，通话已自动挂断。如有已说内容仍会生成回顾。";
  if (detail === "admin_forced") return "管理员已结束本次通话。如有已说内容仍会生成回顾。";
  return "";
}
const VAD_STAGES = new Set(["prepare","runtime","resource","model","warmup","worklet","arm","quality","queue","inference","tail","closed","ready"]);
const VAD_REASONS = new Set(["preparing","ready","ready_next","context_missing","context_inactive","worklet_missing","sample_rate","resource_failed","initialization_failed","arm_unconfirmed","channels","clock","sample","overflow","capacity","sequence","queue","inference","tail","processor","analysis","track","cancelled","unknown"]);
function ownDiagnostic(value) {
  try {return Object.getOwnPropertyDescriptor(value,"diagnostic")?.value;}
  catch (_) {return null;}
}
function vadCode(value) {
  // Do not invoke diagnostic getters or stringify arbitrary browser objects.
  try {
    const stage = Object.getOwnPropertyDescriptor(value,"stage")?.value;
    const reason = Object.getOwnPropertyDescriptor(value,"reason")?.value;
    if (typeof stage === "string" && VAD_STAGES.has(stage) && typeof reason === "string" && VAD_REASONS.has(reason))
      return {stage,reason};
  } catch (_) { /* Fixed unknown below. */ }
  return null;
}
function ownMetrics(value) {
  try {return Object.getOwnPropertyDescriptor(value,"metrics")?.value;} catch (_) {return null;}
}
function metricDiagnostic(value) {
  try {
    const fields = [["warmupMs",600000],["awaitPeakMs",600000],["queuePeak",15],["overflowFrames",16],["gapCount",65],["lostMs",60000],["startGapCount",65],["startSkippedMs",60000]], parts=[];
    for (const [key,max] of fields) {
      const number = Object.getOwnPropertyDescriptor(value,key)?.value;
      if (typeof number === "number" && Number.isSafeInteger(number) && number>=0 && number<=max) parts.push(key+"="+number);
    }
    return parts.length ? " 本地检测："+parts.join("，")+"（ms为等待墙钟峰值，非纯计算）。" : "";
  } catch (_) {return "";}
}
function vadDiagnostic(value, metrics) {
  const {stage,reason} = vadCode(value) || {stage:"prepare",reason:"unknown"};
  const code = stage+"/"+reason;
  let startupGap=false;
  try{startupGap=Object.getOwnPropertyDescriptor(metrics || {},"startGapCount")?.value>0;}catch(_){}
  if (reason === "preparing") return "人声检测正在准备（"+code+"），本段使用音量停顿；准备后下一段启用人声断句。";
  if (reason === "ready_next") return "人声检测已准备完成（"+code+"），本段使用音量停顿；下一段启用人声断句。";
  return "人声检测："+code+"。"+(["preparing","ready","ready_next"].includes(reason) && !startupGap ? "" : metricDiagnostic(metrics));
}
const audioState = context => {
  try {const state=context?.state, codes={running:1,suspended:2,closed:3,interrupted:4};return Object.hasOwn(codes,state)?codes[state]:undefined;} catch (_) {return undefined;}
};
const stopTracks = stream => { for (const track of stream?.getTracks() || []) track.stop(); };

// AudioContext time is a native-frame clock represented as a double. Accept
// only its integer grid within floating-point roundoff, never an actual gap.
export function nativeFrame(context, actualRate=context?.sampleRate) {
  const time = context?.currentTime, rate = actualRate;
  if (!Number.isFinite(time) || time < 0 || !Number.isFinite(rate) || rate <= 0) return null;
  const value = time*rate, frame = Math.round(value);
  if(rate!==context?.sampleRate)return Number.isSafeInteger(frame)?frame:null;
  const tolerance = 4*Number.EPSILON*Math.max(1,Math.abs(value));
  if (!Number.isSafeInteger(frame) || !Number.isFinite(tolerance) || tolerance > 1e-6 ||
      Math.abs(value-frame) > tolerance) return null;
  return frame;
}

// Stream while recording; stop freezes the ACK cursor and commits the complete unconfirmed tail in one request.
// The server keeps one uncommitted staging per call, so segments take turns ("lane"): a segment's chunks wait until the
// previous segment has a definite result (committed, or sent whole after falling back) or was dropped without commit.
// Only the HTTP requests queue; recording is never delayed.
// One chunk request at a time; blobs that arrive meanwhile are merged into the next request (seq counts requests). Chunk ACKs
// only mean "staged": nothing is received until the commit. Any unrecoverable trouble just marks the upload failed and
// the page sends the complete clip through the old audio route with the same id.
export class ChunkUpload {
  constructor(page, id, mime) {
    this.page = page; this.id = id; this.mime = mime; this.reference = page.reference;
    this.held = []; this.pending = []; this.armed = false; this.failed = false; this.loop = null; this.done = false; this.after = null;
    this.finished = new Promise(resolve => {this.settle = resolve;});
    this.seq = 0; this.offset = 0; this.requests = 0; this.lastAckAt = null; this.total = 0;
  }
  // The lane is free again: after a definite result, or when the segment is dropped without commit.
  freeze() {
    this.frozen = true; this.pending = []; this.held = [];
    this.transport?.cancel?.();
  }
  release() {
    if (this.done) return;
    this.freeze();
    this.done = true; this.pending = []; this.held = []; this.settle();
    if (this.page.lane === this) this.page.lane = null;
  }
  abandon() {this.release();}
  push(blob) {
    if (this.failed || this.done || this.frozen) return;
    this.total += blob.size;
    if (this.total > this.page.config.maxAudioBytes) {this.fail(); return;}
    (this.armed ? this.pending : this.held).push(blob); if (this.armed) this.kick();
  }
  // Nothing leaves the page until speech was detected, exactly like before: silence is never uploaded.
  arm() {
    if (this.armed || this.failed || this.done || this.frozen) return;
    const previous = this.page.lane;
    this.after = previous && previous !== this && !previous.done ? previous.finished : null; this.page.lane = this;
    this.armed = true; this.pending = this.held; this.held = []; this.kick();
  }
  fail() {this.failed = true; this.pending = []; this.held = [];}
  kick() {
    if (this.loop || this.failed || this.done || this.frozen || !this.pending.length) return;
    this.loop = this.run().catch(() => this.fail()).finally(() => {this.loop = null; this.kick();});
  }
  async run() {
    if (this.after) {
      const turn = this.after; this.after = null;
      await Promise.race([turn, new Promise(resolve => this.page.e.setTimeout(resolve,LANE_WAIT_MS))]);
    }
    while (this.pending.length && !this.failed && !this.done && !this.frozen) {
      const body = new this.page.e.Blob(this.pending.splice(0), {type:this.mime});
      if (!body.size || !await this.send(body)) return;
    }
  }
  async send(body) {
    const seq = this.seq, offset = this.offset, page = this.page;
    for (let attempt = 0; attempt < CHUNK_ATTEMPTS; attempt++) {
      if (this.frozen || this.done) return false;
      if (page.locked || page.reference !== this.reference) {this.fail(); return false;}
      this.requests++;
      try {
        this.transport = {error:new Error("call_page.chunk_cancelled")};
        const ack = await page.api("audio-chunk",body,{"Content-Type":this.mime,"X-Bot-Utterance-ID":this.id,
          "X-Bot-Chunk-Seq":String(seq),"X-Bot-Chunk-Offset":String(offset)},false,"chunk_submit",null,() => !this.frozen && !this.done,this.transport);
        if (this.frozen || this.done) return false;
        if (ack.chunk_received !== true || ack.utterance_id !== this.id || ack.next_seq !== seq+1 || ack.next_offset !== offset+body.size) {this.fail(); return false;}
        this.seq = seq+1; this.offset = offset+body.size; this.lastAckAt = page.e.now(); return true;
      } catch (error) {
        if (this.frozen || this.done) return false;
        const data = failureData(error);
        // Timeouts, lost connections and server hiccups: re-send the very same request (the server accepts it once).
        if (["timeout","network","invalid-response"].includes(data.reason) || data.status >= 500) continue;
        if ([404,405,501].includes(data.status)) page.chunkUnavailable = true;
        this.fail(); return false;
      }
    }
    this.fail(); return false;
  }
}


// Read-only, memory-only diagnostics. Never await this from capture/control.
export class CapabilityProbe {
  constructor(w, changed=()=>{}, deferred=false) {
    this.w=w;this.changed=changed;this.values={client_cap_schema:1};
    this.workers=new Set();this.cancellations=new Set();
    this.task=new Promise(resolve=>{this.resolveStructure=resolve;});
    if(!deferred)this.run();
  }
  run() {
    if(this.running)return;this.running=true;
    void this.structure().catch(()=>{this.values.client_cap_error=1;}).finally(()=>{
      this.structureDone=true;this.resolveStructure();try{this.changed();}catch(_){}
    });
  }
  number(key,value,max=60000) {
    if(Number.isFinite(value)&&value>=0)this.values['client_cap_'+key]=Math.min(max,Math.round(value));
  }
  yieldToCapture() {
    if(!this.running||this.structureDone)return;
    this.cancelled=true;
    for(const worker of this.workers)worker.terminate();
    for(const cancel of this.cancellations)cancel();
  }
  async bounded(action,ms=300) {
    if(this.cancelled)throw 4;
    let timer,cancel;
    const cancellation=new Promise((_,reject)=>{cancel=()=>reject(4);this.cancellations.add(cancel);});
    try {return await Promise.race([Promise.resolve().then(()=>{if(this.cancelled)throw 4;return action();}),cancellation,new Promise((_,reject)=>{timer=this.w.setTimeout(()=>reject(3),ms);})]);}
    finally {this.w.clearTimeout(timer);this.cancellations.delete(cancel);}
  }
  async structure() {
    const w=this.w,v=this.values;
    const names=['AudioEncoder','AudioData','AudioWorkletNode','MediaRecorder','Worker','SharedArrayBuffer','AudioDecoder','ReadableStream'];
    let bits=0;
    for(let i=0;i<names.length;i++)try{if(typeof w[names[i]]==='function')bits|=1<<i;}catch(_){}
    try{if(w.isSecureContext)bits|=256;if(w.crossOriginIsolated)bits|=512;}catch(_){}
    v.client_cap_api=bits;
    try {
      const ua=w.navigator?.userAgent || '';
      v.client_cap_platform=/android/i.test(ua)?1:/iPhone|iPad|iPod/i.test(ua)||(/Macintosh/.test(ua)&&w.navigator?.maxTouchPoints>1)?2:0;
      v.client_cap_qq=Number(/\bQQ\//i.test(ua));
      const chromium=/\b(?:HeadlessChrome|Chrome|Chromium)\/(\d+)/.exec(ua);
      if(chromium)this.number('chromium',Number(chromium[1]),999);
    }catch(_){}
    const mime=['audio/webm;codecs=opus','audio/mp4','audio/ogg;codecs=opus','audio/webm','audio/ogg','audio/wav'];
    for(const [key,api,types] of [['recorder',w.MediaRecorder,mime],['mse',w.MediaSource,['audio/mpeg','audio/webm;codecs="opus"','audio/mp4;codecs="mp4a.40.2"']],['mms',w.ManagedMediaSource,['audio/mpeg','audio/webm;codecs="opus"','audio/mp4;codecs="mp4a.40.2"']]]) {
      let supported=0,checked=0;
      for(let i=0;i<types.length;i++)try{if(typeof api?.isTypeSupported==='function'){if(api.isTypeSupported(types[i]))supported|=1<<i;checked|=1<<i;}}catch(_){}
      v['client_cap_'+key]=supported;v['client_cap_'+key+'_checked']=checked;
    }
    let supported=0,checked=0;
    const config=rate=>({codec:'opus',sampleRate:rate,numberOfChannels:1,bitrate:24000});
    for(const [i,rate] of [16000,24000,48000].entries()) {
      try {if(bits&1){if((await this.bounded(()=>w.AudioEncoder.isConfigSupported(config(rate)))).supported)supported|=1<<i;checked|=1<<i;}}catch(_){}
    }
    if(this.cancelled)return;
    v.client_cap_opus=supported;v.client_cap_opus_checked=checked;v.client_cap_encode=0;
    // A complete synthetic second goes through the same Worker, mux and
    // browser demux/decode path. This background task never delays listen().
    if(supported && (bits&2) && (bits&16)) {
      let worker;
      const start=w.performance.now();
      try {
        const result=await this.bounded(()=>new Promise((resolve,reject)=>{
          worker=new w.Worker('/bot-call/capture-worker.js');this.workers.add(worker);
          worker.onmessage=({data})=>data.type==='ready'?resolve(data):data.type==='error'?reject(data):null;
          worker.onerror=()=>reject(2);worker.postMessage({type:'probe'});
        }),1500);
        v.client_cap_mux_reason=result.metrics?.client_capture_mux_reason||0;
        // Never decode the synthetic experiment on the live mic/playback graph.
        const decoder=new w.OfflineAudioContext(1,48000,48000);
        const audio=await this.bounded(()=>decoder.decodeAudioData(result.data),1000);
        const normalized=Math.round(audio.length/audio.sampleRate*48000);
        this.number('roundtrip_samples',normalized,96000);
        v.client_cap_encode=Math.abs(normalized-48000)<=1?1:2;
      }catch(e){if(e===4)delete v.client_cap_encode;else v.client_cap_encode=e===3?3:2;
        if(e?.metrics)v.client_cap_mux_reason=e.metrics.client_capture_mux_reason;
      }
      finally {try{worker?.terminate();this.workers.delete(worker);}catch(_){}if(!this.cancelled)this.number('encode_ms',w.performance.now()-start,3000);}
    }
    if(this.cancelled)return;
    v.client_cap_worker=0;v.client_cap_worker_state=0;
    if(bits&16) {
      let worker;
      try {
        const result=await this.bounded(()=>new Promise((resolve,reject)=>{
          worker=new w.Worker('/bot-call/capability-worker.js');this.workers.add(worker);
          worker.onmessage=({data})=>Number.isInteger(data)&&data>=0&&data<=31?resolve(data):reject(2);
          worker.onerror=()=>reject(2);
        }),1000);
        v.client_cap_worker=result;v.client_cap_worker_state=1;
      }catch(e){if(e===4){delete v.client_cap_worker;delete v.client_cap_worker_state;}else v.client_cap_worker_state=e===3?3:2;}
      finally{try{worker?.terminate();this.workers.delete(worker);}catch(_){}}
    }
  }
  start(context,detector) {
    if(this.runtimeStarted||this.done)return;this.runtimeStarted=true;
    try {
      this.context=context;this.detector=detector;this.started=this.w.performance.now();
      this.values.client_cap_runtime=1;
      this.sample();this.timer=this.w.setTimeout(()=>this.finish(),2000);
    }catch(_){this.finish();}
  }
  sample() {
    try {
      const c=this.context,d=this.detector;
      this.number('context',{running:1,suspended:2,closed:3,interrupted:4}[c?.state] || 0,4);
      this.number('rate',c?.sampleRate,192000);
      this.number('worklet_ready',Number(!!d?.node),1);
      const p=d?.captureProbe;
      if(p) {
        if(p.frames && d.active) {
          this.number('gaps',d.active.gapCount || 0,65);this.number('lost_ms',d.active.lostMs || 0,60000);
          this.number('overflow',d.active.metrics?.overflowFrames || 0,16);
        }
        for(const [key,max] of [['frames',64],['first_ms',60000],['gap_peak_ms',60000]])this.number(key,p[key],max);
      }
    }catch(_){this.values.client_cap_error=1;}
  }
  finish() {
    if(this.done)return;
    this.done=true;try{this.w.clearTimeout(this.timer);this.sample();}catch(_){}
    this.context=this.detector=null;
    void this.task.finally(()=>{try{this.changed();}catch(_){}});
  }
  summary() {
    const v=this.values;
    return '能力探测 v1：Opus='+ (v.client_cap_opus ?? '检测中')+'，合成编码='+(['未试','成功','失败','超时'][v.client_cap_encode] || '检测中')+
      '，采音帧='+(v.client_cap_frames ?? '未知')+'（仅诊断）。';
  }
}

export class CallPage {
  constructor(env, view = () => {}, options = {}) {
    this.e = env; this.view = view;
    // The listening threshold follows the Silero VAD v5 default (0.50).
    this.config = {bargeIn:"off",captureMode:"auto", silenceMs:1200, segmentMs:30000, sampleMs:100, rmsThreshold:0.025,
      maxAudioBytes:2097152, pollMs:1000, requestMs:15000, audioRequestMs:60000, heartbeatMs:15000,
      vadPositive:0.5, vadNegative:0.45, vadRedemptionMs:1200, vadMinSpeechMs:32, chunkUpload:true, ...options}; // final tail + commit need one RTT; same-id whole upload remains the fallback
    for (const name of ["silenceMs","segmentMs","sampleMs","maxAudioBytes","pollMs","requestMs","heartbeatMs"]) {
      if (!Number.isSafeInteger(this.config[name]) || this.config[name] <= 0) throw new Error("call_page.invalid_configuration");
    }
    if (!Number.isSafeInteger(this.config.audioRequestMs) || this.config.audioRequestMs <= 0 || this.config.audioRequestMs > 120000)
      throw new Error("call_page.invalid_configuration");
    if (!Number.isFinite(this.config.rmsThreshold) || this.config.rmsThreshold <= 0 || this.config.rmsThreshold > 1) throw new Error("call_page.invalid_configuration");
    if (!Number.isFinite(this.config.vadPositive) || !Number.isFinite(this.config.vadNegative) ||
        this.config.vadNegative <= 0 || this.config.vadPositive >= 1 || this.config.vadNegative >= this.config.vadPositive ||
        !Number.isSafeInteger(this.config.vadRedemptionMs) || this.config.vadRedemptionMs < 100 || this.config.vadRedemptionMs > 5000 ||
        !Number.isSafeInteger(this.config.vadMinSpeechMs) || this.config.vadMinSpeechMs < 32 || this.config.vadMinSpeechMs > 1000)
      throw new Error("call_page.invalid_configuration");
    this.detector = null;
    this.reference = env.storage.getItem(REFERENCE) || "";
    this.invitation = new URLSearchParams(env.location.hash.slice(1)).get("invitation") || "";
    if (env.location.hash) env.history.replaceState(null, "", env.location.pathname);
    this.server = null; this.message = "尚未开始"; this.notice = "";
    this.started = false; // Only explicit Start grants first microphone intent.
    this.listenTask = null; this.refreshTask = this.refreshOwner = null; this.vadDiagnostic = ""; this.apiDiagnostic = ""; this.analysisDiagnostic = "";
    this.muted = false; this.suspended = false; this.ending = false; this.locked = false;
    this.epoch = 0; this.stream = null; this.audioContext = null; this.recording = null;
    this.clip = null; this.round = null; this.speech = null; this.speechBlocked = false;
    this.playbackPaused = false; this.voices = []; this.voiceState = "ready";
    this.speechDiagnostic = "正在检查浏览器语音；试听不会录音或调用模型。";
    this.transportRef=null;this.wsDisconnects=0;this.wsReconnects=0;this.wsConnectedOnce=false;this.metricsRound=null;
    this.socket=null; this.pushReady=false; this.pushRetry=null; this.pushTask=null; this.pushDirty=false;
    this.pushFailures=0;this.pushRecovering=false;
    this.seen = new Set(); this.observations = []; this.poll = null;
    this.heartbeatTimer = this.heartbeatTask = this.heartbeatOwner = null; this.heartbeatControl = 0;
    this.startTask = this.sendTask = this.endTask = this.flushTask = null; this.tracking = false;
    this.openedAt=this.e.now();this.preparationMetrics={};
    this.render(); this.initPlayback();
    this.deviceChanges=0;
    try{this.e.mediaDevices?.addEventListener?.("devicechange",()=>{this.deviceChanges=Math.min(4096,this.deviceChanges+1);});}catch(_){}
    try {this.capability=new CapabilityProbe(this.e.probeWindow,()=>{this.sendCapability();},true);}catch(_){}
    try {
      if (this.e.createVAD) {
        this.detector=this.e.createVAD();
        if(typeof this.detector.prepareModel==="function") {
          this.watchModelPreparation();
          this.modelPreparation=Promise.resolve(this.detector.prepareModel()).then(ready=>{
            this.finishModelWatch();
            this.modelReady=ready===true;if(this.modelReady)this.modelReadyAt=this.e.now();
            const observed=ownMetrics(this.detector);
            for(const [key,name] of [["resourceMs","client_model_fetch_ms"],["sessionMs","client_model_session_ms"]]) {
              const value=Object.getOwnPropertyDescriptor(observed || {},key)?.value;
              if(Number.isSafeInteger(value) && value>=0 && value<=600000)this.preparationMetrics[name]=value;
            }
            if(this.modelReady)this.preparationMetrics.client_model_cold=Math.round(this.e.now()-this.openedAt);
            else this.vadDiagnostic="人声模型未就绪，使用音量停顿检测。";
            this.sendPreparationMetrics();this.render();return this.modelReady;
          },()=>{this.finishModelWatch();this.modelReady=false;this.sendPreparationMetrics();return false;});
        }
      }
    } catch (_) {this.finishModelWatch();this.modelReady=false;}

  }
  sendCapability() {
    try {
      const p=this.capability;
      if(!p?.done || p.sent || !this.reference || this.locked)return;
      // Structural task must settle before the single combined request.
      if(!p.structureSettled) {
        if(!p.sending)p.sending=p.task.then(()=>{p.structureSettled=true;this.sendCapability();});
        return;
      }
      p.sent=true;
      void this.api("metrics",{generation_id:this.capabilityId ||= "page:cap:"+this.e.crypto.randomUUID(),values:{...p.values}}).catch(()=>{});
    }catch(_){}
  }
  watchModelPreparation() {
    // Timer drift estimates scheduling stalls, including CPU/OS delays. Hidden
    // pages are excluded; this is not a measurement of pure JS computation.
    const watch=this.modelWatch={last:this.e.now(),hidden:!!this.e.isHidden?.()};
    const sample=()=>{
      if(this.modelWatch!==watch)return;
      this.sampleModelWatch(watch);
      watch.timer=this.e.setTimeout(sample,100);
    };
    watch.timer=this.e.setTimeout(sample,100);
  }
  sampleModelWatch(watch) {
    const now=this.e.now(), hidden=!!this.e.isHidden?.(), gap=now-watch.last;
    if(!hidden && !watch.hidden && Number.isFinite(gap) && gap>=100 && gap<=600100)
      watch.peak=Math.max(watch.peak ?? 0,Math.ceil(gap-100));
    watch.last=now;watch.hidden=hidden;
  }
  finishModelWatch() {
    const watch=this.modelWatch;if(!watch)return;
    this.e.clearTimeout(watch.timer);this.sampleModelWatch(watch);this.modelWatch=null;
    if(Number.isSafeInteger(watch.peak))this.preparationMetrics.client_model_lag_ms=watch.peak;
  }
  track(make) {
    // A task field is assigned only after its body ran up to the first await, and that body may
    // render. Without this guard a healthy hand-off looks idle and offers Continue for a moment.
    this.tracking = true;
    try {return make();} finally {this.tracking = false;}
  }
  presentation() {
    const toolClosing=Boolean(this.server?.background_processing);
    const toolWaiting=!this.speech?.playing && this.round?.playAt===undefined && !this.ending &&
      !this.locked && !this.muted && !this.suspended && !this.server?.close_requested && !this.server?.ended &&
      (this.round || this.sendTask || this.clip) && (toolClosing || this.server?.tool_processing===true);
    const capture=this.recording && !this.recording.stopping && !this.recording.stopped && this.recording.recorder?.state==="recording";
    // After a hang-up the cancelled round and the stopped playback are results, not problems to recover from.
    const closed=this.ending && !!(this.server?.ended || this.server?.close_requested);
    const failed=!this.ending && (this.server?.replies?.some(r=>r.utterance_id===this.round?.id && ["failed","uncertain","cancelled"].includes(r.state)) ||
      this.server?.audio?.some(r=>r.utterance_id===this.round?.id && ["failed","uncertain","cancelled"].includes(r.state)));
    const exchange = exchangeMessage(this.apiFailure);
    if (exchange) return {phase:"error",label:exchange[0],hint:exchange[1],action:exchange[2],exchangeEnded:!exchange[2]};
    if(this.locked)return {phase:"error",label:"出错",hint:"通话验证已失效，请回原聊天重新发起通话。"};
    if(this.clipRejection(this.clip))return {phase:"error",label:"出错",action:"discard",hint:this.ending
      ? "这段没录好，尚未提交；点击丢弃本段并挂断，已保存的文字仍保留。"
      : "这段没录好，尚未提交；点击继续会丢弃本页这段录音，重新开始收音。"};
    if(this.speech?.playing && !this.speech.terminal)return {phase:"speaking",label:this.speech.local?"试听中":"在说",hint:"正在播放声音，想插话时点下方“点击打断”。"};
    if(this.series?.failed && !capture && (!this.ending || this.series.queue.length || this.clip))return {phase:"error",label:"出错",action:this.ending || this.series.failed==="upload"?"retry":"continue",hint:"连续表达尚未完整，已保存部分仍保留；核查提交，或保留本段后重新说完整句。"};
    if(this.series && !this.series.finishing && !this.series.failed && !capture && !this.ending)return {phase:"listening",label:"在听",hint:"我在听，停顿后会自动提交。"};
    if(capture && this.silentNotice && this.e.now()<this.silentNoticeUntil)
      return {phase:"ready",label:"没有听到声音，请再说一次",silentNotice:true,hint:"没有听到声音，请再说一次。"};
    if(capture)return {phase:this.series || this.recording.voiced || this.recording.vad?.candidate?"listening":"ready",label:this.series || this.recording.voiced || this.recording.vad?.candidate?"在听":"可以说话",
      hint:this.silentNotice?"没有听到声音，请再说一次。":this.recording.vad?.valid?"我在听，停顿后会自动提交；也可以点下方“点击发送”。":this.analyser && this.recording.energyValid!==false && this.audioContext?.state==="running"?"正在用音量判断停顿；说完稍等，或点下方“点击发送”。":"自动断句暂不可用，请说完后点下方“点击发送”。"};
    const failedBarge=this.failedBarge();
    if(failedBarge || this.apiFailure && (!closed || this.clip) || !this.ending && this.playbackPaused || this.speechBlocked || failed || this.speech?.needsGesture) {
      const service=this.serviceFailure("stt") || this.serviceFailure("chat") || this.serviceFailure("tts");
      const action=this.speech?.needsGesture?"voice":this.speechBlocked && this.speech?"stop":
        this.ending || this.started && (this.clip || this.apiFailure || failedBarge)?"retry":"continue";
      return {phase:"error",label:"出错",action,hint:action==="voice"?"浏览器暂未允许声音，请点恢复声音继续播放。":
        action==="stop"?"停止播放尚未确认，请点停止朗读核查，不要关闭页面。":
        service?service+"；已保存文字保留。":
        this.ending?"结束请求或最后一段还没确认，请点核查，不要关闭页面；已保存的文字仍保留。":
        this.clip?"刚才那句是否传到还没确认，请点核查／重试，不要关闭页面。":
        action==="retry"?"网络不太稳定，请点核查／重试确认当前状态。":"暂时无法继续，请点继续通话，或挂断回到原聊天。"};
    }
    if(this.ending)return {phase:"ended",label:closed?"通话已结束":"正在结束",hint:this.clip?"本页仍有未确认片段，请在详情核查，不要关闭页面。":closeMessage(this.server) || (this.server?.complete?"回顾已提交，可以回原聊天继续聊。":this.server?.close_state==="closed_no_text"?"没有可整理的文字，不会生成回顾。":"正在收尾并整理回顾，处理结果以详情为准。")};
    if(this.muted || this.suspended)return {phase:"paused",label:this.muted?"已静音":"已暂停",hint:this.muted?"点取消静音后继续说，朗读单独控制。":"回到前台后点继续，已确认文字仍保留。"};
    if(toolWaiting)return {phase:"thinking",toolWaiting:true,toolClosing,
      label:toolClosing?"工具收尾中…":"工具处理中…",
      hint:toolClosing?"工具正在安全收尾，暂不可插话，请稍等。":"正在调用工具，暂不可插话，请稍等。"};
    if(this.barge && !this.recording)return {phase:"thinking",label:"正在确认插话",hint:this.message};
    if(this.round || this.speech || this.sendTask || this.clip)return {phase:"thinking",label:"在想",hint:this.speech?"正在准备回复的声音，请稍等。":"正在处理刚才那句话，请稍等。"};
    if(/麦克风未能|没有.*接口|没有.*格式|TTS尚未/.test(this.message))return {phase:"error",label:"出错",action:"continue",hint:"麦克风或声音没有准备好，请允许麦克风权限后点继续通话。"};
    return {phase:"preparing",label:"准备中",hint:this.listenTask || this.startTask?"正在准备，请允许使用麦克风，看到可以说话后再开口。":"点开始后授权麦克风，看到可以说话再开口。"};
  }
  hasLiveMicTrack() {
    return this.stream?.getTracks().some(track=>track.enabled && !track.muted && track.readyState==="live");
  }
  bargeMicListening() {
    const {speech,round}=this;
    if(this.barge || this.clip || !this.bargeEnabled() || !this.playbackGain ||
       !this.echo?.model || this.echo.closed || (this.pcmContext || this.audioContext)?.state!=="running" ||
       !this.hasLiveMicTrack())return false;
    if(speech?.playing)return Boolean(!speech.terminal && !speech.local && speech.seq && (speech.pcm || speech.outputNode));
    // Keep the standby input visible between sentences, including while the next player loads.
    return round?.playAt!==undefined && !round.terminal && !round.playbackFinished &&
      (!speech || speech.round===round && speech.seq>1 && !speech.terminal && !speech.needsGesture);
  }
  micListening() {
    const r=this.recording;
    if(r && !r.stopping && !r.stopped && r.recorder?.state==="recording")return true;
    if(this.bargeMicListening())return true;
    if(!this.capture || this.capture.closed || this.audioContext?.state!=="running" || !this.hasLiveMicTrack())return false;
    // The worklet still owns input while the recorder is being replaced or armed.
    if(this.series && !this.series.finishing && !this.series.failed)return true;
    if(this.barge && !this.barge.captureStarted && !this.barge.localClosed && this.bargeEnabled())return true;
    return !!this.listenTask && !this.round && !this.speech && !this.barge && !this.clip && !this.sendTask;
  }
  canCueToolWait(ui) {
    // A player owns audio as soon as it starts preparing, before its first sample.
    return ui.toolWaiting && !this.speech && !this.recording && !this.listenTask && !this.e.isHidden?.();
  }
  syncToolWaitCue(ui) {
    if(!this.canCueToolWait(ui)) {this.stopToolWaitCue();return;}
    if(this.toolWaitTimer!=null || this.toolWaitCount>=TOOL_WAIT_CUE_MAX_COUNT)return;
    this.toolWaitTimer=this.e.setTimeout(()=>{
      this.toolWaitTimer=null;
      const current=this.presentation();
      if(!this.canCueToolWait(current)) {this.stopToolWaitCue();return;}
      this.toolWaitCount=(this.toolWaitCount || 0)+1;
      this.playToolWaitCue();
      this.syncToolWaitCue(current);
    },this.toolWaitCount?TOOL_WAIT_CUE_REPEAT_MS:TOOL_WAIT_CUE_DELAY_MS);
  }
  stopToolWaitCue() {
    if(this.toolWaitTimer!=null)this.e.clearTimeout(this.toolWaitTimer);
    this.toolWaitTimer=null;this.toolWaitCount=0;
    this.releaseToolWaitTone?.();
  }
  playToolWaitCue() {
    const ctx=this.pcmContext || this.audioContext, echo=this.echo;
    if(ctx?.state!=="running" || !ctx.createOscillator || !ctx.createGain || this.releaseToolWaitTone)return;
    // Leave the live barge-in monitor and the echo baseline/playback windows
    // untouched. A filled baseline without monitoring no longer samples audio.
    if(echo?.pending || echo && !echo.closed &&
      (echo.current || this.bargeEnabled() || (!echo.windows.pcm && !echo.windows.mp3 && echo.base.rows.length<echo.base.limit)))return;
    let tone,gain;
    const contextChanged=()=>{if(ctx.state!=="running")stop();};
    const release=()=>{
      this.releaseToolWaitTone=null;
      ctx.removeEventListener?.("statechange",contextChanged);
      if(tone) {tone.onended=null;tone.disconnect();}
      gain?.disconnect();
    };
    const stop=()=>{
      try {tone?.stop();} catch (_) { /* The oscillator may not have started. */ }
      release();
    };
    this.releaseToolWaitTone=stop;
    try {
      tone=ctx.createOscillator();gain=ctx.createGain();
      // Quiet 440 Hz sine with a 20 ms attack and a fade to zero in 120 ms.
      const start=ctx.currentTime,end=start+.12;
      tone.type="sine";tone.frequency.value=440;
      gain.gain.setValueAtTime(0,start);
      gain.gain.linearRampToValueAtTime(.025,start+.02);
      gain.gain.linearRampToValueAtTime(0,end);
      tone.connect(gain);gain.connect(ctx.destination);
      tone.onended=release;
      ctx.addEventListener?.("statechange",contextChanged);
      tone.start(start);tone.stop(end);
    } catch (_) {stop(); /* Optional sound cannot interrupt call controls. */ }
  }
  render() {
    this.syncPCMCapability();
    if(!this.mayHeartbeat()) {this.e.clearTimeout(this.clockTimer);this.clockTimer=null;}
    else if(this.clockTimer==null)this.clockTimer=this.e.setTimeout(()=>{this.clockTimer=null;this.render();},1000);
    const limits = this.server?.limits;
    const timing = !this.ending && limits?.warning && !limits.close_reason
      ? `通话将在约${Math.ceil(limits.remaining_seconds/60)}分钟内结束（以上次服务器确认信息为准）。` : "";
    const ui=this.presentation();
    this.syncToolWaitCue(ui);
    const mic=this.muted || this.suspended || this.ending || this.locked || this.server?.ended ||
      this.server?.close_requested || ["ended","error","paused"].includes(ui.phase)?"muted":
      !ui.toolWaiting && this.micListening()?"listening":ui.phase==="preparing"?"muted":"waiting";
    if(this.ending && this.timerEnd===undefined)this.timerEnd=this.e.now();
    const elapsed=this.callStartedAt===undefined?0:Math.max(0,Math.floor(((this.timerEnd ?? this.e.now())-this.callStartedAt)/1000));
    this.view({ui,mic,botName:this.server?.bot_name || "Bot",elapsed,
      level:this.recording?.level || 0,closed:this.ending && !!(this.server?.ended || this.server?.close_requested),clipPending:!!this.clip || !!this.series?.queue.length,started:this.started,suspended:this.suspended,
      entered:this.started || this.callStartedAt!==undefined || !!this.reference && !this.invitation,
      continueNeeded:!this.recording && !this.speech && !this.round && !this.listenTask && !this.startTask && !this.sendTask && !this.tracking || !!this.apiFailure || this.playbackPaused,
      message:this.message, notice:this.notice, details:[this.echo?.summary(),this.capability?.summary(),this.apiDiagnostic,this.vadDiagnostic,this.analysisDiagnostic,timing].filter(Boolean).join("\n"), muted:this.muted,
      recording:!!this.recording, speaking:!!this.speech, needsGesture:!!this.speech?.needsGesture, ending:this.ending, locked:this.locked,
      speechDiagnostic:[this.speechDiagnostic,this.lastSpeechFailure !== this.speechDiagnostic ? this.lastSpeechFailure : ""].filter(Boolean).join("\n"), testDisabled:this.testBusy() || this.voiceState === "loading",
      retainLabel:this.round?.retainAvailable === true || !!this.series?.failed && this.series.failed!=="upload",
      turns:this.displayTurns(), complete:this.server?.complete === true && !this.clip && !this.series?.queue.length});
  }
  displayTurns() {
    const turns=this.server?.turns || [], gid=this.round && "reply:"+this.round.id;
    if(!gid || turns.some(t=>t.role==="assistant" && t.material_id===gid))return turns;
    const body=(this.server?.segments || []).filter(s=>s.generation_id===gid && s.seq>0 && typeof s.text==="string")
      .sort((a,b)=>a.seq-b.seq).map(s=>s.text).join("");
    if(!body)return turns;
    if(this.captionSource!==turns || this.captionBody!==body || this.captionGid!==gid) {
      this.captionSource=turns;this.captionBody=body;this.captionGid=gid;
      this.captionTurns=[...turns,{role:"assistant",body,material_id:gid}];
    }
    return this.captionTurns;
  }
  mediaAvailable() {
    return typeof this.e.Audio === "function" && typeof this.e.URL?.createObjectURL === "function" &&
      typeof this.e.URL?.revokeObjectURL === "function";
  }
  initPlayback() {
    this.speechDiagnostic = this.mediaAvailable()
      ? "可试听固定样本；仅测试网页播放，不验证当前配置TTS。"
      : "浏览器没有完整音频播放接口（api-unavailable）；请检查浏览器。";
    this.render();
  }
  testBusy() {
    return !!(this.recording || this.listenTask || this.startTask || this.sendTask || this.endTask ||
      this.clip || (!this.locked && this.round && this.server?.complete !== true) || this.speech || this.speechBlocked || this.stream);
  }
  testVoice() {
    // No await/connect/HTTP or microphone: keep speak in the actual click stack.
    if (this.testBusy()) return;
    const speech = {local:true, terminal:false};
    this.speech = speech; this.beginSpeech(speech);
  }
  finishSample(speech,notice) {
    this.e.clearTimeout(speech.timer);
    if (speech.stopUnconfirmed) {this.render(); return;}
    if (this.speech === speech) this.speech = null;
    this.hasSpeechResult = true; this.speechDiagnostic = notice; this.render();
  }
  failSpeech(speech,code) {
    if(this.barge?.speech===speech){this.closeBarge("unavailable");this.show("旧声音已不可用，继续听取本段");return;}
    if (speech.terminal || this.speech !== speech) return;
    const first=speech.seq===1;
    speech.terminal = true; this.releasePlayback(speech); code = speechError(code);
    const notice = `${SPEECH_ERRORS[code]}（${code}）。${speech.pcmFailure ? " PCM原因="+speech.pcmFailure+"（"+Object.keys(PCM_FAILURE).find(k=>PCM_FAILURE[k]===speech.pcmFailure)+"）。" : ""}文字已保留，跳过本句继续。`;
    this.hasSpeechResult = true; this.speechDiagnostic = notice; this.lastSpeechFailure = notice;
    if (speech.local) {this.finishSample(speech,notice); return;}
    this.lastFailedGeneration=speech.id;this.releaseMic();
    if(first && !speech.stopUnconfirmed) {
      speech.skipUnconfirmed=true;speech.round.firstAudioClosed=true;
      this.playbackNotice="首句播放结果未确认，不重播；后续句继续朗读。";
      this.observe(speech,"failed");void this.afterSpeech(speech).catch(error=>this.report(error));return;
    }
    this.playbackNotice = "浏览器朗读失败，文字已保存，自动继续倾听。";
    this.observe(speech,"failed"); void this.afterSpeech(speech).catch(error => this.report(error));
  }
  serviceFailure(service, round=this.round) {
    const row=this.server?.failures?.find(r=>r.service===service && (service==="summary" || r.generation_id==="reply:"+round?.id));
    const names={stt:"语音识别服务",chat:"对话服务",tts:"语音合成服务",summary:"前情摘要服务"};
    const messages={balance:"余额不足，请联系管理员充值",auth:"鉴权失败，请联系管理员检查配置",timeout:"响应超时，请点继续通话再说一遍",unavailable:"网络不稳，这句没接上。点继续通话再说一遍",other:"这次没接上，请点继续通话再说一遍",config:"配置不可用，请联系管理员检查通话模型"};
    if(service==="summary" && ["timeout","unavailable","other"].includes(row?.code))return "前情摘要服务暂时没准备好，本轮先按近期聊天继续。";
    return row && Object.hasOwn(names,service) && Object.hasOwn(messages,row.code) ? names[service]+messages[row.code] : "";
  }
  show(message, notice = this.notice) { this.message = message; this.notice = notice; this.render(); }
  report(error, localStage="local") {
    if (CANCELLED_READS.has(error)) return;
    let data = failureData(error);
    if (data === LOCAL_FAILURE && localStage === "clip_digest") data = {...LOCAL_FAILURE,stage:"clip_digest"};
    this.apiDiagnostic = diagnostic(data); this.apiFailure = data;
    const exchange = exchangeMessage(data);
    if (exchange) {this.show(exchange[0], "");return;}
    const audioTimeout = data.route === "audio" && data.reason === "timeout";
    this.show(this.locked ? "会话校验未通过，已暂停" : audioTimeout ? "音频上传等待超时，接收结果尚未确认" : "本次操作尚未确认",
      (audioTimeout ? "超时不代表未收到；" : "")+"已确认材料仍保留；未确认片段留在本页，请勿关闭。可核查状态或明确继续，不会自动重传本段。");
  }
  async api(route, body, headers = {}, binary = false, context = "request", stateRead = null, isCurrent = null, transport = null) {
    let phase = "preflight", status = 0, timer, expired = false, abort;
    const epoch = this.epoch, reference = this.reference, round = this.round;
    const capability = route === "metrics" && (body?.generation_id?.startsWith("page:cap:") || body?.generation_id?.startsWith("page:echo:"));
    const fail = (reason,code) => new RequestFailure(status,route,phase,reason,context,code);
    const read = route === "state" && body === undefined ? STATE_READS.get(stateRead) : null;
    const obsolete = isCurrent ? new Error("call_page.request_owner_changed") : null;
    if (obsolete) CANCELLED_READS.add(obsolete);
    const checkRead = () => {
      if (read?.cancelled) throw read.error;
      if (isCurrent && !isCurrent()) throw obsolete;
    };
    try {
      if (typeof route !== "string" || !API_ROUTES.has(route) || route === "local") throw fail("local_unknown");
      const init = {method:body === undefined ? "GET" : "POST", credentials:"same-origin", cache:"no-store",
        referrerPolicy:"no-referrer", headers:{...headers}};
      if (route !== "exchange") {
        if (!this.reference || this.locked) {status=401;throw fail("session");}
        init.headers["X-Bot-Call-Reference"] = this.reference;
      }
      if (body !== undefined) {
        if (body instanceof this.e.Blob) init.body = body;
        else {init.body = JSON.stringify(body); init.headers["Content-Type"] = "application/json";}
      }
      abort = new this.e.AbortController(); init.signal = abort.signal;
      const requestMs = ["audio","audio-commit","audio-series","audio-series-commit"].includes(route) && init.method === "POST" ? this.config.audioRequestMs : route==="state" ? Math.min(this.config.requestMs,read?.waiting?9000:read?.recovering?Math.min(2500,750*2**(this.stateReadFailures||0)):2500) : this.config.requestMs;
      phase = "fetch";
      const request = (async () => {
        const response = await this.e.fetch("/bot-call/"+route,init);
        checkRead();
        if (expired) throw fail("timeout");
        phase = "http";
        const code = response.status, ok = response.ok;
        if (!Number.isInteger(code) || code<100 || code>599 || typeof ok !== "boolean" || ok !== (code>=200 && code<300))
          throw fail("invalid-response");
        status = code;
        if (!ok) {
          let code;
          try {code = (await response.json())?.error;} catch (_) { /* The status is still usable. */ }
          checkRead();
          if (expired) throw fail("timeout");
          const error = fail("http",code);
          if (!capability && [401,403].includes(status) && epoch === this.epoch && reference === this.reference && round === this.round) {
            this.locked = true; this.stopToolWaitCue(); this.epoch++; this.stopHeartbeat(); this.releaseMic();
            if (this.recording) void this.finishSegment().catch(error => this.report(error));
            this.report(error);
          }
          throw error;
        }
        phase = "body";
        const value = binary ? await (transport?.read ? transport.read(response) : response.blob()) : await response.json();
        checkRead();
        if (expired) throw fail("timeout");
        phase = "validate";
        if (binary) {
          if (!(value instanceof this.e.Blob)) throw fail("invalid-response");
        } else {
          if (!value || typeof value !== "object" || Array.isArray(value)) throw fail("invalid-response");
          if (route === "state" && (!["audio","replies","turns"].every(key => Array.isArray(value[key])) ||
              typeof value.complete !== "boolean")) throw fail("invalid-response");
        }
        if (!capability && epoch === this.epoch && reference === this.reference && round === this.round && this.apiFailure?.route === route) {
          this.apiDiagnostic = ""; this.apiFailure = null;
        }
        return value;
      })();
      const owner = read || transport;
      const cancellation = owner ? new Promise((_,reject) => {
        owner.cancel = () => {
          owner.cancelled = true; reject(owner.error);
          try {abort.abort();} catch (_) { /* Wrapper cancellation already settled. */ }
        };
        if (owner.cancelled) owner.cancel();
      }) : new Promise(() => {});
      return await Promise.race([request,cancellation,new Promise((_,reject) => {
        timer = this.e.setTimeout(() => {
          expired = true; reject(fail("timeout"));
          try {abort.abort();} catch (_) { /* Timer still settles with a fixed reason. */ }
        },requestMs);
      })]);
    } catch (error) {
      checkRead(); // Obsolete rejection/timeout has no reporting or auth authority either.
      if (CANCELLED_READS.has(error) || REQUEST_FAILURES.has(error)) throw error;
      throw fail(phase === "fetch" ? "network" : ["http","body","validate"].includes(phase) ? "invalid-response" : "local_unknown");
    } finally {this.e.clearTimeout(timer); if (read) read.cancel = null; if (transport) transport.cancel = null;}
  }
  sendPreparationMetrics() {
    if(!this.reference || this.locked || !Object.keys(this.preparationMetrics).length || this.preparationSending)return;
    const signature=JSON.stringify(this.preparationMetrics);if(signature===this.preparationSent)return;
    let sent=false;
    this.preparationSending=this.api("metrics",{generation_id:"page",values:{...this.preparationMetrics}})
      .then(()=>{sent=true;this.preparationSent=signature;}).catch(()=>{}).finally(()=>{this.preparationSending=null;if(sent)this.sendPreparationMetrics();});
  }
  experience(value) {
    if(["auto","off"].includes(value?.barge_in))this.config.bargeIn=value.barge_in;
    if(["auto","legacy"].includes(value?.capture_mode))this.config.captureMode=value.capture_mode;
    if(!value || !Number.isInteger(value.silence_ms) || value.silence_ms<300 || value.silence_ms>3000 ||
       !Number.isInteger(value.sensitivity) || value.sensitivity<10 || value.sensitivity>90)return;
    this.config.vadRedemptionMs=this.config.silenceMs=value.silence_ms;
    this.config.vadPositive=(100-value.sensitivity)/100;
    this.config.vadNegative=Math.max(.05,this.config.vadPositive-.05);
  }
  async connect() {
    if (this.invitation) {
      const result = await this.api("exchange", {invitation:this.invitation});
      if (typeof result.reference !== "string" || !/^[0-9a-f]{64}$/.test(result.reference)) throw new RequestFailure(0,"exchange","validate","invalid-response");
      this.experience(result.experience);
      this.reference = result.reference; this.e.storage.setItem(REFERENCE,this.reference);
      this.invitation = "";
    }
    this.sendPreparationMetrics();this.sendCapability();
    return this.refresh(false);
  }
  mayHeartbeat() {
    const limits = this.server?.limits;
    return this.started && !this.ending && !this.suspended && !this.locked && !this.e.isHidden?.() &&
      !this.server?.close_requested && !this.server?.ended && this.server?.audio_available === true && limits?.enforced === true && !limits.error;
  }
  stopHeartbeat() {
    this.e.clearTimeout(this.heartbeatTimer); this.heartbeatTimer = null;
  }
  scheduleHeartbeat() {
    if (!this.mayHeartbeat()) {this.stopHeartbeat(); return;}
    // Polling state must not keep postponing a fixed heartbeat interval.
    if (this.heartbeatTimer !== null || this.heartbeatTask) return;
    this.heartbeatTimer = this.e.setTimeout(() => {
      this.heartbeatTimer = null; void this.heartbeat().catch(error => this.report(error));
    },this.config.heartbeatMs);
  }
  heartbeatIdentity() {
    return {...this.refreshIdentity(),control:this.heartbeatControl,muted:this.muted};
  }
  ownsHeartbeat(owner) {
    return !!owner && this.heartbeatOwner === owner && owner.control === this.heartbeatControl &&
      owner.muted === this.muted && this.ownsRefresh(owner) && !this.e.isHidden?.();
  }
  invalidateHeartbeat() {
    // Controls detach obsolete wrappers, so their delayed cleanup cannot starve
    // a newer foreground owner. Their own finally may only release themselves.
    this.heartbeatControl++; this.heartbeatOwner = this.heartbeatTask = null;
    this.scheduleHeartbeat();
  }
  usableConnectionLimits(limits) {
    return !!limits && limits.enforced === true && limits.error === null &&
      limits.close_reason === null && limits.closed_at === null && typeof limits.warning === "boolean" &&
      ["entered_at","deadline_at","server_now"].every(key => Number.isFinite(limits[key]) && limits[key]>=0) &&
      ["remaining_seconds","reconnect_remaining_seconds"].every(key => Number.isFinite(limits[key]) && limits[key]>0);
  }
  heartbeat() {
    if (this.heartbeatTask && this.ownsHeartbeat(this.heartbeatOwner)) return this.heartbeatTask;
    if (this.heartbeatTask) this.heartbeatTask = this.heartbeatOwner = null;
    if (!this.mayHeartbeat()) return Promise.resolve();
    let owner = this.heartbeatIdentity(); this.heartbeatOwner = owner;
    const task = (async () => {
      try {
        const sending = owner;
        const result = await this.api("heartbeat",{}, {},false,"request",null,() => this.ownsHeartbeat(sending));
        if (!this.ownsHeartbeat(owner)) return;
        this.server = {...this.server,limits:result.limits}; this.render();
      } catch (error) {
        if (!this.ownsHeartbeat(owner)) return;
        // Unknown delivery is never a reason to upload a retained segment.
        // At most one fresh read and one idempotent heartbeat follow this pause.
        this.apiFailure = failureData(error); this.apiDiagnostic = diagnostic(this.apiFailure);
        this.suspended = true; this.stopToolWaitCue(); this.stopPush(); this.epoch++; this.stopHeartbeat();
        this.e.clearTimeout(this.poll); this.poll = null; this.cancelStateRead();
        owner = this.heartbeatIdentity(); this.heartbeatOwner = owner;
        const stopped = this.finishSegment("control"); this.releaseMic(); await stopped;
        if (!this.ownsHeartbeat(owner)) return;
        try {
          const state = await this.refresh(false);
          if (!this.ownsHeartbeat(owner)) return; // Closed/locked projection is authoritative.
          if (state !== this.server || state.ended !== false || state.close_requested !== false ||
              state.audio_available !== true || !this.usableConnectionLimits(state.limits)) {
            this.show("通话状态已读取，暂不能恢复", "录音已暂停；请点继续核查。未确认片段仍留在本页，不会自动上传，请勿关闭。");
            return;
          }
          const result = await this.api("heartbeat",{}, {},false,"request",null,() => this.ownsHeartbeat(owner));
          if (!this.ownsHeartbeat(owner)) return;
          if (!this.usableConnectionLimits(result.limits)) throw new RequestFailure(200,"heartbeat","validate","invalid-response");
          // A limits receipt confirms this renewal only. Preserve the full fresh
          // state and keep capture paused until an explicit Continue action.
          this.server = {...state,limits:result.limits}; this.apiDiagnostic = ""; this.apiFailure = null;
          this.show("连接已核实，录音已暂停", "请点击继续恢复；未确认片段仍留在本页，不会自动上传，请勿关闭。");
        } catch (failure) {
          if (!this.ownsHeartbeat(owner)) return;
          this.apiFailure = failureData(failure); this.apiDiagnostic = diagnostic(this.apiFailure);
          this.show("连接尚未确认，录音已暂停", "请点击继续核查通话状态；本页未确认片段仍需确认，不会自动重传，请勿关闭。");
        }
      }
    })().catch(error => {
      if (this.ownsHeartbeat(owner)) this.report(error);
    }).finally(() => {
      if (this.heartbeatTask !== task) return;
      this.heartbeatTask = this.heartbeatOwner = null; this.scheduleHeartbeat();
    });
    this.heartbeatTask = task; return task;
  }
  refreshIdentity() {
    return {epoch:this.epoch,round:this.round,ending:this.ending,suspended:this.suspended,locked:this.locked,reference:this.reference,message:this.message,control:this.heartbeatControl};
  }
  sameRefresh(owner) {
    return owner.epoch === this.epoch && owner.round === this.round && owner.ending === this.ending &&
      owner.suspended === this.suspended && owner.locked === this.locked && owner.reference === this.reference;
  }
  ownsRefresh(owner) {
    return this.sameRefresh(owner) && owner.control === this.heartbeatControl;
  }
  initTransport() {
    if(this.transportRef===this.reference)return;
    this.transportRef=this.reference;this.wsDisconnects=0;this.wsReconnects=0;this.wsConnectedOnce=false;
    try {
      const saved=JSON.parse(this.e.storage.getItem(REFERENCE+"-transport")||"null");
      if(saved?.reference===this.reference && [saved.disconnects,saved.reconnects].every(n=>Number.isSafeInteger(n)&&n>=0&&n<=4096)) {
        this.wsDisconnects=saved.disconnects;this.wsReconnects=saved.reconnects;this.wsConnectedOnce=saved.connected===true;
      }
    } catch (_) {}
  }
  saveTransport() {
    try {this.e.storage.setItem(REFERENCE+"-transport",JSON.stringify({reference:this.reference,disconnects:this.wsDisconnects,reconnects:this.wsReconnects,connected:this.wsConnectedOnce}));}catch(_){}
  }
  markTransport(kind,round=this.round) {
    this.initTransport();if(!round)return;
    round.transportMask=(round.transportMask||0)|(kind==="ws"?2:1);
    this.sendMetrics(round);
  }
  canReceivePCM() {
    return !!(this.started && !this.suspended && !this.ending && !this.locked &&
      !this.speechBlocked &&
      !this.playbackPaused && !this.speech?.needsGesture && (!this.speech || this.speech.pcm) &&
      !this.e.isHidden?.() && !this.server?.close_requested && (this.pcmContext?.state==="running" || !!this.speech?.pcm));
  }
  declinePCM(packet) {
    const target=this.round || this.clip, socket=this.socket;
    if(!target || this.speech || target.pcmFirstAt!==undefined || target.segmentSeq ||
       packet && packet.generation_id!=="reply:"+target.id)return;
    target.pcmDeclined=true;
    // Rejection only stops optional streaming. The archive remains revision 0
    // even if this message is lost or delayed.
    try {socket?.send(JSON.stringify({type:"pcm-unavailable",generation_id:"reply:"+target.id}));}catch(_){}
    this.pendingPCM=null;
  }
  syncPCMCapability() {
    const socket=this.socket;
    const enabled=this.canReceivePCM();
    if(!socket || socket.readyState!==1 || socket.pcmEnabled===enabled)return;
    if(socket.pcmEnabled)this.declinePCM();
    try {socket.send(JSON.stringify({type:"pcm-capability",enabled}));socket.pcmEnabled=enabled;}catch(_){}
  }
  recoverPushState() {
    this.cancelStateRead(true);
    const owner=this.refreshIdentity(),observed=this.round;
    // An immediate read also cancels an outstanding 8s long-poll wrapper.
    void this.refresh().then(()=>{
      if(this.ownsRefresh(owner))this.markTransport("poll",observed);
    }).catch(error=>{
      if(!CANCELLED_READS.has(error) && this.ownsRefresh(owner))this.report(error);
    });
  }
  retryPush() {
    if(this.pushRetry || this.suspended || this.locked || this.server?.complete || this.server?.close_state==="closed_no_text")return;
    // Retain backoff across flapping handshakes; only a stable sync resets it.
    const delay=Math.min(4000,200*2**Math.min(this.pushFailures,5))*(0.75+Math.random()*0.5);
    this.pushFailures=Math.min(6,this.pushFailures+1);
    const firstFailure=!this.pushRecovering;this.pushRecovering=true;if(firstFailure)this.stateReadFailures=0;
    this.pushRetry=this.e.setTimeout(()=>{this.pushRetry=null;this.ensurePush();},delay);
    if(firstFailure)this.recoverPushState();
    this.schedulePoll();
  }
  ensurePush() {
    this.initTransport();
    if (!this.e.WebSocket || this.socket || this.pushRetry || !this.reference || this.suspended || this.locked || this.server?.complete || this.server?.close_state==="closed_no_text") return;
    try {
      const url=new URL("/bot-call/ws",this.e.location.origin); url.protocol="wss:";
      const protocols=["bot-call."+this.reference];
      if(this.canReceivePCM())protocols.push("bot-call.pcm-v1");
      const socket=new this.e.WebSocket(url.href,protocols.length===1?protocols[0]:protocols); this.socket=socket;socket.pcmEnabled=protocols.length===2;
      // After a delayed watchdog, let already queued messages run before closing.
      // Dense PCM renews the same activity marker as alive/state messages.
      const arm=()=>{
        const activity=socket.activity={};this.e.clearTimeout(socket.watchdog);
        socket.watchdog=this.e.setTimeout(()=>{
          socket.watchdog=this.e.setTimeout(()=>{if(this.socket===socket && socket.activity===activity)socket.onerror();},1000);
        },socket.readyState===0?5000:20000);
      }; arm();
      socket.onopen=()=>{if(this.socket===socket) {this.cancelStateRead(true);this.syncPCMCapability();arm();if(this.pushRecovering)this.recoverPushState();}};
      socket.onmessage=event=>{
        if(this.socket!==socket || this.suspended || this.locked) return;
        let value; try {value=JSON.parse(event.data);} catch (_) {socket.close();return;}
        if(!value || !["sync","changed","alive"].includes(value.state)) {socket.close();return;}
        arm();
        if(value.pcm) {this.acceptPCM(value.pcm);return;}
        this.noteProgress(value);
        if(socket.synced) this.acceptPushedAudio(value);
        if(value.state==="alive") return;
        this.pushDirty=true;
        if(!this.pushTask) this.pushTask=(async()=>{
          while(this.pushDirty && this.socket && !this.suspended && !this.locked) {
            const current=this.socket, identity=this.refreshIdentity(),observed=this.round;this.pushDirty=false;
            await this.refresh();
            if(!this.sameRefresh(identity)) {this.pushDirty=true;continue;}
            if(this.socket===current && !this.locked) {
              if(!current.synced) {
                if(this.wsConnectedOnce)this.wsReconnects=Math.min(4096,this.wsReconnects+1);
                this.wsConnectedOnce=true;current.synced=true;current.syncedAt=this.e.now();this.saveTransport();
              }
              this.pushReady=true;this.pushRecovering=false;this.markTransport("ws",observed);this.e.clearTimeout(this.poll);this.poll=null;
              this.sendMetrics(this.round||this.metricsRound);
            }
          }
        })().catch(()=>this.socket?.close()).finally(()=>{this.pushTask=null;});
      };
      // Detach immediately on local transport failure; a native close handshake
      // may itself stall on a dead network. Its eventual onclose is obsolete.
      socket.onerror=()=>{socket.onclose();socket.close();};
      socket.onclose=event=>{
        this.e.clearTimeout(socket.watchdog);
        if(this.socket!==socket) return;
        if(socket.synced) {this.wsDisconnects=Math.min(4096,this.wsDisconnects+1);this.saveTransport();this.sendMetrics(this.round||this.metricsRound);}
        const codes=[1000,1001,1006,1008,1011,1012,1013,1015];
        this.connectionMetrics ||= {};
        this.connectionMetrics.client_ws_close_code=codes.indexOf(event?.code)+1;
        if(Number.isFinite(this.callStartedAt))this.connectionMetrics.client_ws_disconnect_ms=Math.min(86400000,Math.max(0,Math.round(this.e.now()-this.callStartedAt)));
        this.socket=null;this.pushReady=false;this.pendingPCM=null;
        this.speech?.pcm?.interrupt();this.barge?.speech?.pcm?.interrupt();
        if(socket.synced && this.e.now()-socket.syncedAt>=10000)this.pushFailures=0;
        this.retryPush();
      };
    } catch (_) {this.pushReady=false;this.retryPush();}
  }
  stopPush() {
    this.e.clearTimeout(this.pushRetry);this.pushRetry=null;this.pushFailures=0;this.pushRecovering=false;
    const socket=this.socket;this.socket=null;this.pushReady=false;this.pendingPCM=null;
    if(socket) {this.e.clearTimeout(socket.watchdog);this.speech?.pcm?.interrupt();this.barge?.speech?.pcm?.interrupt();socket.close();}
  }
  preparePCM() {
    // Create only after authorized input, before any capture/playback graph.
    // Chromium's document activation survives the asynchronous permission wait.
    if(this.pcmContext) {void this.pcmContext.resume().then(()=>this.syncPCMCapability()).catch(()=>{});return;}
    if(!this.e.AudioContext)return;
    try {
      const context=this.audioContext || new this.e.AudioContext();
      if(!this.audioContext)this.playbackContextCreatedAfterInput=Number(!!this.stream?.getTracks().some(t=>t.readyState==="live"));
      if(!context.createBufferSource || !context.createBuffer){void context.close();return;}
      this.pcmContext=context;
      context.onstatechange=()=>{
        if(this.pcmContext!==context)return;
        if(this.speech?.playbackDiag) {
          const d=this.speech.playbackDiag;d.changes=Math.min(4096,d.changes+1);
          if(["suspended","interrupted"].includes(context.state))d.interrupts=Math.min(4096,d.interrupts+1);
        }
        if(context.state!=="running")this.echo?.stop(true);
        this.speech?.pcm?.checkContext();
        this.syncPCMCapability();
      };
      void context.resume().then(()=>{
        if(this.pcmContext!==context)return;
        this.syncPCMCapability();
      }).catch(()=>{});
    } catch(_) {}
  }
  closePCM() {
    const context=this.pcmContext;this.pcmContext=null;this.syncPCMCapability();
    if(context && context!==this.audioContext)void context.close().catch(()=>{});
  }
  acceptPCM(packet) {
    const candidate=this.barge;
    if(candidate && packet.generation_id==="reply:"+candidate.id) {
      candidate.newFrames ||= [];candidate.newSocket ??= this.socket;
      if(candidate.newDeclined)return;
      if(candidate.newFrames.length>=128 || candidate.newSocket!==this.socket ||
         !this.reserveBargeCache((packet.data?.length||0)*2+256,true)) {
        candidate.newDeclined=true;candidate.newFrames=[];this.declinePCM(packet);return;
      }
      candidate.newFrames.push({...packet,receivedAt:this.e.now()});return;
    }
    if(this.closedBargeGenerations?.has(packet.generation_id)) {this.rejectBargeBlock();return;}
    if(this.barge && this.barge.speech.id===packet.generation_id) {
      const b=this.barge;
      if(!b.localClosed)b.speech.pcm?.feed(packet);
      else this.rejectBargeBlock();
      return;
    }
    const round=this.round,target=round || this.clip;
    if(target?.pcmDeclined)return;
    if(!this.canReceivePCM()) {
      if(packet.index===0)this.declinePCM(packet);
      this.syncPCMCapability();return;
    }
    if(!round && this.clip && packet.generation_id==="reply:"+this.clip.id) {
      if(this.pendingPCM?.id!==this.clip.id)this.pendingPCM={id:this.clip.id,frames:[]};
      if(this.pendingPCM.frames.length>=128){this.pendingPCM=null;this.socket?.close();return;}
      this.pendingPCM.frames.push({...packet,receivedAt:this.e.now()});return;
    }
    if(!round || packet.generation_id!=="reply:"+round.id || packet.seq!==1 ||
       !this.started || this.suspended || this.ending || this.locked ||
       this.recording || this.speechBlocked || this.playbackPaused || round.terminal ||
       round.playbackFinished || round.firstAudioClosed || round.segmentSeq ||
       this.server?.close_requested || !this.speech?.pcm && this.pcmContext?.state!=="running")return;
    let speech=this.speech;
    if(!speech) {
      if(packet.index!==0 || packet.end)return;
      this.stopToolWaitCue();
      // No await before owning the stream: state reads cannot create a second player.
      speech={id:packet.generation_id,seq:1,round,revision:0,terminal:false,waiting:false};
      this.speech=speech;
      round.pcmFirstAt=packet.receivedAt??this.e.now();round.readyAt??=round.pcmFirstAt;
      round.downloadAt??=round.pcmFirstAt;round.firstAudioPath="push";
      speech.pcm=new PCMPlayer(this.pcmContext,this.e,{
        reserve:bytes=>this.reserveBargeCache(bytes),
        output:this.playbackOutput(),lookahead:this.bargeEnabled()?0.26:Infinity,
        leadMs:this.config.captureMode==="legacy"?0:180,
        scheduled:(samples,start,rate)=>{
          try{this.echo?.path(speech,"pcm",start);this.echo?.scheduled(samples,start,rate);}catch(_){this.echo?.stop();}
        },
        playing:()=>{
          if(this.speech!==speech || speech.terminal)return;
          speech.playing=true;speech.started=true;round.playAt??=this.e.now();try{this.playbackDiagnostics(speech,1);}catch(_){}
          this.observe(speech,"started");this.sendMetrics(round);this.show("正在播放（浏览器观测）");
          this.bargePlayback();
        },
        ended:()=>{
          if(this.speech!==speech || speech.terminal)return;
          speech.terminal=true;this.releasePlayback(speech);this.observe(speech,"ended");
          void this.afterSpeech(speech).catch(error=>this.report(error));
        },
        failed:reason=>{
          if(this.barge?.speech===speech) {
            this.closeBarge(reason===PCM_FAILURE.capacity?"capacity":"unavailable");
            this.show("旧朗读已结束，无法从停点恢复，继续听取本段");return;
          }
          speech.pcmFailure=reason;
          round.metrics ||= {};round.metrics.client_pcm_failure=reason;
          this.sendMetrics(round);this.failSpeech(speech,"unknown");
        }
      });
      // Send only after creating the sole local player, on the owning WS.
      // Playback does not await a round trip; local ownership prevents replay.
      try {this.socket?.send(JSON.stringify({type:"pcm-accepted",generation_id:packet.generation_id}));}catch(_){}
      this.sendMetrics(round);
    }
    if(speech.round!==round || !speech.pcm || speech.terminal)return;
    speech.pcm.feed(packet);
  }
  acceptPushedAudio(value) {
    if(this.barge && value.first_audio?.generation_id==="reply:"+this.barge.id) {
      if(!this.barge.newPushed && this.reserveBargeCache((value.first_audio.data?.length||0)*2+4096,true))
        this.barge.newPushed=value;
      return;
    }
    const round=this.round, a=value.first_audio;
    if(!round && this.clip && value.tts_pcm_selection && a?.generation_id==="reply:"+this.clip.id) {
      // Same bound as inline payloads; revalidate all fields after upload ACK.
      if(typeof a.data==="string" && a.data.length<=4*Math.ceil(PUSH_AUDIO_MAX_BYTES/3))
        this.clip.pendingFirstAudio={tts_pcm_selection:true,first_audio:a,segments:value.segments?.filter(r=>r.generation_id===a.generation_id)};
      return;
    }
    if(!round || !a || !this.started || this.ending || this.suspended || this.locked ||
       this.recording || this.speechBlocked || this.playbackPaused || round.terminal || round.playbackFinished ||
       round.firstAudioClosed || round.pcmFirstAt!==undefined || round.segmentSeq || value.closed || this.server?.close_requested ||
       a.generation_id!=="reply:"+round.id || a.seq!==1 ||
       !Number.isInteger(a.byte_count) || a.byte_count<24 || a.byte_count>PUSH_AUDIO_MAX_BYTES ||
       !["audio/mpeg","audio/wav"].includes(a.mime) || typeof a.data!=="string" ||
       a.data.length>4*Math.ceil(PUSH_AUDIO_MAX_BYTES/3)) return;
    const rows=value.segments?.filter(r=>r.generation_id===a.generation_id);
    if(!rows?.some(r=>r.seq===0 && ["open","complete"].includes(r.state)) ||
       !rows.some(r=>r.seq===1 && r.state==="succeeded" && r.playback_state==="unknown") ||
       this.server?.segments?.some(r=>r.generation_id===a.generation_id &&
         (r.seq===0 && r.state==="stopped" || r.seq===1 && TERMINAL.has(r.playback_state)))) return;
    if(this.speech && (this.speech.round!==round || this.speech.seq!==1 || this.speech.terminal || this.speech.media)) return;
    try {
      const raw=this.e.atob(a.data);
      if(raw.length!==a.byte_count) return;
      const blob=new this.e.Blob([Uint8Array.from(raw,c=>c.charCodeAt(0))],{type:a.mime});
      round.pushedFirstBlob=blob; round.downloads ||= new Map();round.downloads.set(1,Promise.resolve(blob));
      // Only merge this generation's status. Full-state resync still runs,
      // but the first player need not wait for another tunnel round trip.
      const existing=this.server.segments||[];
      const additions=rows.filter(r=>!existing.some(old=>old.generation_id===r.generation_id && old.seq===r.seq));
      this.server.segments=existing.map(old=>old.generation_id===a.generation_id && old.seq===1?rows.find(r=>r.seq===1):old).concat(additions);
      if(this.speech) {
        round.firstAudioPath="push";round.downloadAt ??= this.e.now();this.sendMetrics(round);
        this.speech.url=this.e.URL.createObjectURL(blob);this.preparePlayer(this.speech,this.speech.url);
        void this.prefetchSegment(round,a.generation_id,2);
      } else this.reconcileSegments(round,{generation_id:a.generation_id,state:"running"});
    } catch (_) { /* Invalid/unsupported inline audio falls back to HTTP. */ }
  }
  noteProgress(state) {
    const round=this.round;if(!round || !Number.isFinite(round.uploadAt))return;
    const now=this.e.now();round.visibilityObservedAt=now;
    if(Number.isFinite(this.foregroundAt) && this.foregroundAt>=round.endedAt && now>=this.foregroundAt && !this.e.isHidden?.() &&
       (state.audio?.some(a=>a.utterance_id===round.id&&a.state==="succeeded") || state.replies?.some(a=>a.utterance_id===round.id))) {
      round.foregroundResultMs ??= Math.min(60000,Math.round(now-this.foregroundAt));
    }
    if(round.seriesId ? state.replies?.some(r=>r.utterance_id===round.id) : state.audio?.some(a=>a.utterance_id===round.id && a.state==="succeeded")) round.transcriptAt ??= now;
    if(state.segments?.some(a=>a.generation_id==="reply:"+round.id && a.seq===1 && a.state==="succeeded")) round.readyAt ??= now;
    this.sendMetrics(round);
  }
  sendMetrics(round) {
    if(!round)return;
    this.initTransport();this.metricsRound=round;
    const pairs={client_upload:[round.uploadAt,round.endedAt],client_transcript:[round.transcriptAt,round.uploadAt],
      client_audio_ready:[round.readyAt,round.transcriptAt],client_download:[round.downloadAt,round.readyAt],
      client_play:[round.playAt,round.downloadAt],first_sound:[round.playAt,round.endedAt]};
    const values=Object.fromEntries(Object.entries(pairs).filter(([,v])=>v.every(Number.isFinite)&&v[0]>=v[1]&&v[0]-v[1]<=3600000).map(([k,v])=>[k,Math.round(v[0]-v[1])]));
    Object.assign(values,round.metrics || {},this.connectionMetrics||{});
    round.visibilityObservedAt ??= this.e.now();
    const end=round.playAt ?? round.visibilityObservedAt,begin=round.endedAt;
    if(Number.isFinite(begin)) {
      const spans=[...(this.hiddenSpans||[])];
      if(this.hiddenAt!==undefined)spans.push([this.hiddenAt,this.e.now()]);
      const hidden=spans.reduce((n,[a,b])=>n+Math.max(0,Math.min(b,end)-Math.max(a,begin)),0);
      values.client_hidden_ms=Math.min(86400000,Math.round(hidden));
    }
    if(Number.isFinite(round.foregroundResultMs))values.client_foreground_result_ms=round.foregroundResultMs;
    if(Number.isFinite(round.pcmFirstAt) && Number.isFinite(round.endedAt))
      values.client_pcm_first=Math.max(0,Math.round(round.pcmFirstAt-round.endedAt));
    if(Number.isFinite(round.playAt) && Number.isFinite(round.pcmFirstAt))
      values.client_pcm_play=Math.max(0,Math.round(round.playAt-round.pcmFirstAt));
    if(Number.isFinite(values.first_sound) && Number.isFinite(values.client_endpoint_quiet_ms) &&
       values.first_sound+values.client_endpoint_quiet_ms<=3600000)
      values.speech_end_first_sound=values.first_sound+values.client_endpoint_quiet_ms;
    if(round.firstAudioPath) values[round.firstAudioPath==="push"?"client_first_audio_push":"client_first_audio_download"]=1;
    values.client_ws_disconnects=this.wsDisconnects;values.client_ws_reconnects=this.wsReconnects;
    if(round.transportMask)values.client_transport=round.transportMask===3?2:round.transportMask===2?1:0;
    const signature=JSON.stringify(values);
    if(!Object.keys(values).length || signature===round.metricsSent || round.metricsTask || !this.server?.replies?.some(r=>r.generation_id==="reply:"+round.id)) return;
    let sent=false;
    round.metricsTask=this.api("metrics",{generation_id:"reply:"+round.id,values}).then(()=>{sent=true;round.metricsSent=signature;}).catch(()=>{}).finally(()=>{round.metricsTask=null;if(sent)this.sendMetrics(round);});
  }
  schedulePoll(retry=false) {
    this.e.clearTimeout(this.poll); this.poll = null;
    // A healthy WS reports server changes, not a locally failed HTTP receipt.
    const receiptPending=this.round?.playbackFinished && this.observations.length>0;
    if (this.pushReady && !receiptPending || this.locked || this.suspended || this.server?.complete || this.server?.close_state === "closed_no_text") return;
    // Existing closing/review projection keeps polling; local hangup alone does not.
    if ((!this.round && !this.series && !this.server?.close_requested) || (this.ending && !this.server?.close_requested)) return;
    const owner = this.refreshIdentity();
    const timer = this.e.setTimeout(() => {
      if (this.poll !== timer) return;
      this.poll = null;
      if (!this.ownsRefresh(owner)) return;
      const observed=this.round;
      void this.refresh(true,!this.pushRecovering).then(()=>{if(this.ownsRefresh(owner))this.markTransport("poll",observed);}).catch(error => {
        if (CANCELLED_READS.has(error)) return;
        if (this.ownsRefresh(owner) && this.message === owner.message) {
          this.report(error);
          if (this.retryState(error)) this.schedulePoll(true);
        }
      });
    },retry?0:this.pushRecovering && (this.round||this.series) ? Math.min(500,this.config.pollMs) : this.config.pollMs);
    this.poll = timer;
  }
  retryState(error) {
    const data = failureData(error);
    return data.route === "state" && (["network","timeout","invalid-response"].includes(data.reason) ||
      data.reason === "http" && data.status >= 500);
  }
  cancelStateRead(force=false) {
    const read = STATE_READS.get(this.refreshRead);
    if (read && !read.cancelled && (force||read.waiting)) {
      read.cancelled = true;
      if (read.cancel) read.cancel();
    }
  }
  mayWaitState() {
    if (!this.round || this.clip || this.speech || this.round.playbackFinished || this.ending ||
        this.suspended || this.locked || this.muted || this.server?.close_requested || this.server?.complete) return false;
    const audio = this.server?.audio?.find(item => item.utterance_id === this.round.id);
    const reply = this.server?.replies?.find(item => item.utterance_id === this.round.id);
    return ["pending","running"].includes(audio?.state) ||
      audio?.state === "succeeded" && (!reply || ["not_started","pending","running"].includes(reply.state));
  }
  refresh(reconcile = true, waiting = false) {
    const owner = this.refreshIdentity();
    if (this.refreshTask) {
      if ((!waiting && STATE_READS.get(this.refreshRead)?.waiting || !this.sameRefresh(this.refreshOwner)) && this.refreshRead) this.cancelStateRead(true);
      else if (this.sameRefresh(this.refreshOwner) && !STATE_READS.get(this.refreshRead)?.cancelled) {
        // Start/Retry may share the same immediate read and ACK ownership. An
        // explicit reader adopts that unchanged source for the current control;
        // controls that do not request a read cannot revive obsolete responses.
        this.refreshOwner.control = owner.control;
        return this.refreshTask;
      }
      // Serialize a newer owner's read after the obsolete read, without reviving
      // an obsolete caller or starting a second simultaneous GET.
      return this.refreshTask.catch(() => {}).then(() => this.ownsRefresh(owner) ? this.refresh(reconcile,waiting) : this.server);
    }
    this.e.clearTimeout(this.poll); this.poll = null;
    const readToken = {};
    if (readToken) {
      const error = new Error("call_page.state_read_cancelled"); CANCELLED_READS.add(error);
      STATE_READS.set(readToken,{cancelled:false,cancel:null,error,recovering:this.pushRecovering,waiting:waiting && this.mayWaitState() && !this.pushRecovering});
    }
    let failed = false, error;
    const task = this.refreshOnce(reconcile,owner,readToken).catch(caught => {
      failed = true; error = caught;
      if(this.retryState(caught))this.stateReadFailures=Math.min(2,(this.stateReadFailures||0)+1);
      // A superseded read has no state or error authority. Settle its awaiting
      // control/ACK caller benignly too; api() still rejects stale transport
      // results before they can mutate state or lock a newer session.
      if (CANCELLED_READS.has(caught)) return this.server;
      throw caught;
    }).finally(() => {
      if (this.refreshTask === task) this.refreshTask = this.refreshOwner = this.refreshRead = null;
      // Also covers confirmed audio ACK -> first state read failure. Only read
      // recovery is scheduled; an unconfirmed local clip is never retransmitted.
      if (this.ownsRefresh(owner) && (!failed || CANCELLED_READS.has(error) || this.retryState(error))) this.schedulePoll(failed && this.retryState(error));
    });
    this.refreshRead = readToken; this.refreshOwner = owner; this.refreshTask = task;
    return task;
  }
  async refreshOnce(reconcile, owner, readToken) {
    const headers = STATE_READS.get(readToken)?.waiting ? {"X-Bot-Call-Wait-Ms":"8000","X-Bot-Call-Utterance":owner.round.id} : {};
    const recovering=this.pushRecovering,started=this.e.now();
    if(recovering){this.connectionMetrics ||= {};this.connectionMetrics.client_ws_pull_attempts=Math.min(4096,(this.connectionMetrics.client_ws_pull_attempts||0)+1);}
    let state;
    try {
      state=await this.api("state",undefined,headers,false,"request",readToken,() => this.ownsRefresh(owner));
      if(recovering && this.ownsRefresh(owner))this.connectionMetrics.client_ws_pull_successes=Math.min(4096,(this.connectionMetrics.client_ws_pull_successes||0)+1);
    } finally {
      if(recovering)this.connectionMetrics.client_ws_pull_max_ms=Math.min(60000,Math.max(this.connectionMetrics.client_ws_pull_max_ms||0,Math.round(this.e.now()-started)));
    }
    this.stateReadFailures=0;
    this.experience(state.experience);
    const read = STATE_READS.get(readToken);
    if (read?.cancelled) throw read.error;
    if (!this.ownsRefresh(owner)) return state;
    this.server = state; this.seriesCapability(state.capabilities); this.noteProgress(state);
    if(state.close_requested || state.ended || !state.tool_processing && !state.background_processing)this.stopToolWaitCue();
    if ((state.close_requested || state.ended) && !this.ending) {
      this.ending = true; this.epoch++; this.stopHeartbeat();
      const stopped = this.finishSegment("control"); this.releaseMic();
      if (this.speech) {
        const speech = this.speech;
        speech.terminal = true; this.e.clearTimeout(speech.timer);
        if (this.releasePlayback(speech)) this.speech = null;
      }
      await stopped;
    }
    await this.reconcileBarge(state);
    if(!state.close_requested && !state.ended)await this.reconcileSeries();
    if (state.close_requested || state.ended) {
      const closedByLineGuard = closeMessage(state);
      this.show(closedByLineGuard || (this.clip ? "通话已结束，本页仍有未确认片段" :
        state.close_state === "closed_no_text" ? "已结束，没有可整理文字" :
        state.complete ? "回顾已提交，已写回原会话" :
        state.submission_status === "uncertain" ? "回顾提交结果不确定，需要核查" :
        state.submission_status === "not_applied" ? "回顾尚未全部提交" :
        state.submission_status === "submitted" ? "回顾已提交，原会话衔接尚未确认" :
        state.close_state === "waiting_for_audio" ? "已保存挂断请求，等待已收音频转写" :
        state.close_state === "audio_blocked" ? "音频处理遇到问题，暂不能整理完整回顾" :
        "通话已结束，回顾与投递仍在处理"),
        this.clip ? "本页未确认片段不等于服务器已收到，不能视为整通完整接收；关闭页面可能丢失。" :
        state.close_state === "closed_no_text" ? "未生成或发送回顾；未识别音频仍保留，没有可整理文字不代表没有讲话。" :
        state.audio?.some(a => a.resolution?.decision === "retained_missing" || (a.state === "failed" && a.error_code === "empty_transcription"))
          ? "有未识别片段；回顾仅基于已保存文字，不代表覆盖全部口头发言。" :
        state.complete ? "请到来源聊天确认是否收到。" :
        ["uncertain","not_applied"].includes(state.submission_status)
          ? "不会自动重发回顾，已保存的材料仍保留供核查。" :
        state.submission_status === "submitted" ? "请到来源聊天确认；上下文状态需另行核查。" :
        !closedByLineGuard && state.limits?.close_reason === "duration" ? "已达到通话时长上限，正在处理已收到的材料。" :
        !closedByLineGuard && state.limits?.close_reason === "disconnected" ? "连接中断已超过重连时间，正在处理已收到的材料。" :
        ["resolving","unresolved"].includes(state.review_policy_status)
          ? "正在准备回顾，请稍候。"
          : state.review_policy_status && !["ready","not_configured","complete"].includes(state.review_policy_status)
            ? "回顾暂时需要核查，已保存的文字仍保留。" : "挂断不代表全部处理完成。");
    } else if (reconcile && this.round && !this.clip) await this.reconcileRound();
    const failedRound=this.round || (this.playbackPaused && this.lastFailedGeneration ? {id:this.lastFailedGeneration.slice(6)} : null);
    const failure=["stt","chat","tts"].map(service=>this.serviceFailure(service,failedRound)).find(Boolean);
    if(failure && !this.speech && !this.ending) this.show(failure,"已保存的材料仍保留，结果不确定的收费请求不会自动重试。");
    const summaryFailure=this.serviceFailure("summary");
    if(summaryFailure && !failure && !this.ending) this.notice=summaryFailure+"；本轮使用近期原文继续回复。";
    this.render(); this.schedulePoll(); this.scheduleHeartbeat();
    if(state.complete || state.close_state==="closed_no_text")this.stopPush();else this.ensurePush();
    return state;
  }
  start() {
    if (this.startTask) return this.startTask;
    if(this.recording)return Promise.resolve();
    this.invalidateHeartbeat(); this.cancelStateRead();
    this.clickedAt=this.e.now();this.warmStart=this.modelReady===true;
    this.callStartedAt ??= this.e.now();
    this.started = true;
    // A previously created context may need gesture recovery. Never open the
    // initial Android output before getUserMedia has enabled communication mode.
    if(this.pcmContext)this.preparePCM();
    this.startTask = this.track(() => this.startOnce()).catch(error => this.report(error)).finally(() => {this.startTask = null; this.render();});
    return this.startTask;
  }
  async startOnce() {
    if (this.ending || this.locked || this.recording || this.speech || this.speechBlocked) return;
    this.captureResumeRequired=false;this.backgroundSuspended=false;
    this.suspended = false; this.apiDiagnostic = ""; this.apiFailure = null;
    await this.connect();
    if (this.ending || this.locked) return;
    if(this.series?.failed) {
      if(this.series.reviewNeeded) {this.series.reviewNeeded=false;this.show("上次连续表达尚未收尾", "已收部分保留；再次点击保留并继续后，请重新说完整句。");return;}
      if(this.series.failed!=="upload") {await this.abandonSeries(this.series.failed==="reopened"?"page_reopened":"stt_missing");return;}
      await this.retry();return;
    }
    if (this.clip || this.failedBarge()) { await this.retry(); return; }
    const round = this.round;
    if (round) {
      const audio = this.server?.audio?.find(a => a.utterance_id === round.id);
      if (this.canRetain(audio)) {
        await this.retainRound(round);
        if (this.round) return;
      } else {
      // Continue can confirm a finished playback whose receipt was previously
      // unknown. Until confirmation succeeds, keep its round and pause intact.
      if (round.playbackFinished) await this.flushObservations();
      else {
        await this.reconcileRound();
        if (this.round === round && !round.terminal) return;
      }
      // afterSpeech/Retry may finish the same receipt while Continue waits.
      // Never release a different round that acquired ownership meanwhile.
      if (this.round === round) this.round = null;
      else if (this.round) return;
      }
    }
    if (this.muted) {this.show("已静音"); return;}
    this.playbackPaused = false;
    await this.listen();
  }
  listen() {
    // Every entry (start/retry/afterSpeech) owns the same pending permission
    // and stream-setup task. Never replace an unresolved getUserMedia request.
    if (this.listenTask) return this.listenTask;
    this.listenTask = this.track(() => this.listenOnce()).finally(() => {this.listenTask = null; this.render();});
    return this.listenTask;
  }
  captureSetupFailure(code,before) {
    this.captureFailure=Number.isInteger(code)?code:3;
    if(this.captureFailure===8) {
      // One attempt per segment. Never consume the retry budget in the two
      // readiness checks of the same listen, or touch a live legacy recorder.
      this.captureStartupPending=true;this.captureStartupDeferred=true;
      this.captureDisabled=(this.captureStartupRetries||0)>=3;
      return;
    }
    if([5,6].includes(this.captureFailure)) {
      if((this.captureMuxFailures||0)===before)this.captureMuxFailures=before+1;
      this.captureDisabled=this.captureMuxFailures>=2;
    } else this.captureDisabled=code!==7||!this.captureNeedsRebuild||(this.captureRebuilds||0)>=3;
  }
  async prepareCapture(bufferedContinuation,epoch,arm=false) {
    do {
      if(this.captureStartupDeferred)return;
      if(this.capture?.closed || this.captureDisabled && this.capture && !bufferedContinuation){this.capture.close();this.capture=null;}
      if(this.config.captureMode==="legacy" || this.captureDisabled && !bufferedContinuation)return;
      if(!this.capture) {
        if(this.captureStartupPending) {
          if((this.captureStartupRetries||0)>=3){this.captureDisabled=true;return;}
          this.captureStartupRetries=(this.captureStartupRetries||0)+1;
        }
        const rebuilding=this.captureNeedsRebuild,start=this.e.now(),failures=this.captureMuxFailures||0;
        if(rebuilding) {
          this.captureRebuilds=(this.captureRebuilds||0)+1;this.captureNeedsRebuild=false;
        }
        try{this.capture=await ContinuousCapture.create(this);}
        catch(code){this.captureSetupFailure(code,failures);}
        finally{if(rebuilding)this.captureRebuildMs=Math.max(0,Math.min(3000,Math.round(this.e.now()-start)));}
      }
      if(epoch!==this.epoch || this.ending || this.suspended || this.muted || this.locked)return;
      if(arm && this.capture && !bufferedContinuation && !this.capture.closed) {
        const failures=this.captureMuxFailures||0;
        try{
          await this.capture.armRecording();
          if(this.captureStartupPending) {
            this.captureStartupPending=false;this.captureFailure=0;
            this.captureStartupRecoveredMs=Math.max(0,Math.min(600000,Math.round(this.e.now()-(this.callStartedAt ?? this.openedAt))));
          }
        }
        catch(code) {
          if(!this.captureNeedsRebuild){this.captureSetupFailure(code,failures);this.capture?.close();this.capture=null;}
        }
      }
    } while((!this.capture || this.capture.closed) && this.captureNeedsRebuild && !this.captureDisabled);
  }
  async listenOnce() {
    this.captureStartupDeferred=false;
    if (!this.started) {this.show("请先点击开始，再授权麦克风"); return;}
    if(this.failedBarge() || this.barge?.captureStarted && !this.series)return;
    if(this.series?.finishing && this.series.finalSubmitted && !this.series.queue.length && !this.series.failed && !this.round)this.series=null;
    if (this.captureResumeRequired || this.ending || this.locked || this.muted || this.suspended || this.clip || this.round || this.speech || this.recording || this.sendTask || this.series?.failed || this.series?.finishing || this.speechBlocked || this.playbackPaused) return;
    if (this.server?.audio_available !== true) {
      this.releaseMic(); this.show("语音暂不可用", "请稍后再试，或在原聊天继续交流。"); return;
    }
    if (!this.mediaAvailable()) {this.show("浏览器没有完整音频播放接口，未申请录音"); return;}
    if (this.server?.speech_available !== true) {
      this.show("TTS尚未就绪，未申请录音", "请先在AstrBot为来源会话选择可用TTS，再重新发起通话。"); return;
    }
    const limits = this.server?.limits;
    if (limits && (!limits.enforced || limits.error)) {
      this.show("通话暂不能恢复", "请稍后核查，已保存的材料仍保留。"); return;
    }
    if (limits && (limits.remaining_seconds <= 0 || limits.reconnect_remaining_seconds <= 0)) {
      this.show("正在核查通话是否已结束", "请点击核查，不能仅按本页计时认定已挂断。"); return;
    }
    this.stopToolWaitCue();
    // First capture has supplied runtime diagnostics. With no clip/reply/player
    // or continuation in flight, start a bounded background probe. It never
    // holds the next microphone behind encoding, muxing or decoding.
    if(this.capability?.runtimeStarted && !this.capability.running && !this.series) {
      this.capability.run();
    }
    if(this.echo){await this.echo.stop();if(!this.echo.pending)this.echo=null;}
    const epoch = this.epoch;
    if(this.config.captureMode!=="legacy" && this.stream?.getTracks().some(t=>t.readyState!=='live'))this.releaseMic();
    if (!this.stream) {
      this.show("等待麦克风授权", "权限提示可能一直等待；可随时挂断取消，迟到授权的麦克风会立即关闭。");
      let audio = true;
      try {
        const supported = this.basicCapture ? {} : this.e.mediaDevices.getSupportedConstraints?.() || {};
        const desired = {};
        for (const key of ["echoCancellation", "noiseSuppression", "autoGainControl"])
          if (supported[key] === true) desired[key] = {ideal:true};
        if (supported.channelCount === true) desired.channelCount = {ideal:1};
        if (Object.keys(desired).length) audio = desired;
      } catch (_) { /* Unsupported inspection keeps the basic request. */ }
      let stream;
      try {stream = await this.e.mediaDevices.getUserMedia({audio});}
      catch (_) {this.show("麦克风未能开启", "请检查权限后点继续；若浏览器不支持音频约束，可使用基本采音重试。"); this.basicCapture = true; return;}
      if (epoch !== this.epoch || this.ending || this.muted || this.suspended || this.locked) {stopTracks(stream); return;}
      this.stream = stream;
      this.captureSettings = {};
      try {
        const settings = stream.getTracks()[0]?.getSettings?.() || {};
        for (const key of ["echoCancellation", "noiseSuppression", "autoGainControl"])
          if (typeof settings[key] === "boolean") this.captureSettings[key] = settings[key];
      } catch (_) { /* No device ID/label or settings object retained. */ }
    }
    if(!this.pcmContext && !this.audioContext)this.preparePCM();
    const mime = TYPES.find(type => this.e.MediaRecorder.isTypeSupported(type));
    if (!mime) {this.releaseMic(); this.show("浏览器没有可协商的录音格式"); return;}
    try {
      if (!this.audioContext && this.e.AudioContext) {
        try {
          this.audioContext = this.pcmContext || new this.e.AudioContext();
          if(!this.pcmContext)this.playbackContextCreatedAfterInput=1;
          this.analyser = this.audioContext.createAnalyser();
          // Smallest supported power-of-two window with overlapping samples.
          let size = 32;
          const needed = this.audioContext.sampleRate*this.config.sampleMs/1000*1.5;
          while (size < needed && size < 32768) size *= 2;
          this.analyser.fftSize = size;
          // Keep the first energy graph pulled before Silero attaches its own
          // output. Never monitor microphone audio through the loudspeaker.
          this.analysisSource = this.audioContext.createMediaStreamSource(this.stream);
          this.analysisGain = this.audioContext.createGain(); this.analysisGain.gain.value = 0;
          this.analysisSource.connect(this.analyser); this.analyser.connect(this.analysisGain);
          this.analysisGain.connect(this.audioContext.destination);
        } catch (_) {
          this.releaseAnalysis();
        }
      }
      if (this.audioContext?.state==="suspended" || this.config.captureMode!=="legacy" && this.audioContext?.state==="interrupted") {
        try {await this.audioContext.resume();} catch (_) { /* Record intact, analysis cannot discard. */ }
      }
      if (epoch !== this.epoch || this.ending || this.suspended || this.muted || this.locked || this.series?.failed || this.series?.finishing) {this.releaseMic(); return;}
      for (const track of this.stream.getTracks()) track.enabled = true;
      const bufferedContinuation=!!this.series&&this.capture?.carry.length>0;
      await this.prepareCapture(bufferedContinuation,epoch);
      if (epoch !== this.epoch || this.ending || this.suspended || this.muted || this.locked) {this.releaseMic();return;}
      let r, signal = null, preparation = null;
      // A delayed observation may still own the model. Keep ordinary capture
      // responsive through the existing energy fallback, never reset a busy model.
      if (this.e.createVAD && this.audioContext && !this.echo?.pending) {
        if (!this.detector) this.detector = this.e.createVAD();
        if (this.modelPreparation) {
          // Loading must not hold the microphone behind a slow model download.
          // Energy endpointing works immediately; attach Silero next segment.
          if(this.detector.closing)await this.detector.closing;
          if(this.modelReady)await this.detector.prepare(this.stream,this.audioContext);
        }
        if (this.detector.ready(this.audioContext)) {
          signal = await this.detector.begin(this.config,event => {
            if (!r || this.recording !== r || r.stopping || r.stopped || r.epoch !== this.epoch ||
                this.ending || this.locked || this.muted || this.suspended || this.round || this.speech) return;
            if (event === "endpoint") {
              if(this.series)return; // The series tick combines both signals across recorder boundaries.
              this.sampleRecording(r); // Recheck real capture safety at this edge.
              if (r.vad?.valid !== true) return;
            }
            if (event === "candidate") {
              if(r.vad.positiveMs>=RETAIN_SPEECH_MS || r.vad.highSpans?.some(([a,b,valid])=>valid===512 && a>=r.vad.recordStart))
                this.silentNotice=false;
              if (this.message !== "检测到人声候选，正在听取") this.show("检测到人声候选，正在听取");
            } else if (event === "endpoint") {
              void this.submitSegment("auto_silence").catch(error => this.report(error));
            } else {
              // Keep confirmed speech when the signal fails, but require a new
              // covered quiet interval before energy can finish this utterance.
              r.energyGapAt=this.e.now();
              this.vadDiagnostic = vadDiagnostic(ownDiagnostic(r.vad),ownMetrics(r.vad));
              this.show("正在听取", "正在用音量判断停顿，说完稍等，或点下方“点击发送”。");
            }
          },!!this.series);
        } else {
          // Keep capture/control responsive during real model initialization.
          // This segment can use only independently covered energy for discard.
          if(!this.modelPreparation)preparation = Promise.resolve(this.detector.prepare(this.stream,this.audioContext));
        }
      }
      if (epoch !== this.epoch || this.ending || this.suspended || this.muted || this.locked || this.series?.failed || this.series?.finishing) {this.releaseMic(); return;}
      // VAD preparation yielded after the first capture readiness check.
      // A standby fault in that window must not start a closed recorder.
      await this.prepareCapture(bufferedContinuation,epoch,!this.series);
      if (epoch !== this.epoch || this.ending || this.suspended || this.muted || this.locked) {this.releaseMic();return;}
      let recorder;
      if(this.capture && (!this.captureDisabled||bufferedContinuation))recorder=this.capture.recorder(!!this.series,mime);
      else try {recorder = new this.e.MediaRecorder(this.stream, {mimeType:mime,audioBitsPerSecond:CAPTURE_AUDIO_BITS_PER_SECOND});}
      catch (_) {recorder = new this.e.MediaRecorder(this.stream, {mimeType:mime});}
      if (!ACCEPTED.has(recorder.mimeType)) throw new Error("call_page.unsupported_mime");
      r = {recorder, vad:signal, id:this.barge&&!this.barge.captureStarted?this.barge.id:this.e.crypto.randomUUID(), mime:recorder.mimeType, chunks:[], size:0,
        start:this.e.now(), lastVoice:this.e.now(), voiced:false, overflow:false, error:false, stopping:false,
        epoch, barge:this.barge, stream:this.stream, context:this.audioContext, detector:this.detector, captureOwner:this.capture, reason:null,
        vadPreparation:preparation ? {pending:true,ready:false} : null, uploader:null,
        quality:{valid:true, zero:true, frames:0, lastWall:this.e.now(),
          startContext:this.audioContext?.currentTime, lastContext:this.audioContext?.currentTime}, listeners:[]};
      if (signal) signal.recordStart = nativeFrame(this.audioContext,signal.rate);
      for (const track of r.stream.getTracks()) {
        const invalidate = () => {
          r.quality.valid = false;
          if(r.vad)try {r.detector.fail(r.vad,"quality","track");} catch (_) {r.vad.valid = false;}
        };
        if (typeof track.addEventListener !== "function" || typeof track.removeEventListener !== "function") r.quality.valid = false;
        else for (const name of ["mute", "ended"]) {track.addEventListener(name,invalidate);r.listeners.push([track,name,invalidate]);}
      }
      r.done = new Promise(resolve => {r.resolve = resolve;});
      const stale = this.lane; // a holder nobody will ever finish (its recording was dropped) must not block this one
      if (stale && !stale.done && this.clip?.uploader !== stale && !this.sendTask && !this.series?.queue.some(c=>c.uploader===stale)) stale.abandon();
      if (this.config.chunkUpload && !this.chunkUnavailable) r.uploader = new ChunkUpload(this,r.id,r.mime);
      recorder.ondataavailable = event => {
        if (event.data?.size) {r.chunks.push(event.data); r.size += event.data.size; r.uploader?.push(event.data); this.armUpload(r);}
        if (r.size > this.config.maxAudioBytes) {r.overflow = true; void this.finishSegment().catch(error => this.report(error));}
      };
      recorder.onerror = () => {r.error = true; void this.finishSegment().catch(error => this.report(error));};
      recorder.onstop = async () => {
        if (r.stopped) return;
        r.stopped = true;
        if(recorder.samples!==undefined) {
          r.metrics.client_capture_prefix_ms=Math.round(recorder.prefixSamples/48);
          r.metrics.client_capture_queue=recorder.peak;
          Object.assign(r.metrics,recorder.gapMetrics||{},recorder.stats?.metrics||recorder.metrics||{});
        }
        r.uploader?.freeze(); this.e.clearTimeout(r.timer);this.e.clearTimeout(r.stopTimer);
        let durationMs=recorder.samples===undefined?Math.max(0,Math.round(this.e.now()-r.start)):Math.round(recorder.samples/48);
        // Sample the real final edge, after final dataavailable and BEFORE
        // disabling tracks. An uncovered/changed tail always preserves bytes.
        if(!r.capturedFinal)this.sampleRecording(r);
        if(r.energyObserved)r.metrics.client_energy_frames=r.quality.frames;
        const state=audioState(r.context);if(state!==undefined)r.metrics.client_audio_state_end=state;
        const recordEnd = r.captureEnd ?? nativeFrame(r.context,r.vad?.rate || r.context?.sampleRate);
        // Stop track use at the real recorder stop edge, before waiting on VAD.
        const owned = this.recording === r;
        r.echoHold=owned && (!this.echo||this.echo.closed) && this.e.probeWindow?.AudioWorkletNode &&
          this.audioContext?.audioWorklet && !this.ending && !this.locked && !this.muted &&
          !this.suspended && !this.e.isHidden?.() && r.reason!=="auto_limit";
        if (owned && !r.echoHold && !this.capture) for (const track of r.stream.getTracks()) track.enabled = false;
        let vadResult = {valid:false,candidate:!!r.vad?.candidate};
        if (r.vad) {
          if(!r.reason?.startsWith("auto_"))r.vad.retentionEnd=Math.min(recordEnd,r.controlFrame ?? recordEnd)-r.vad.rate*CONTROL_NOISE_WINDOW_MS/1000;
          try {vadResult = await this.detector.finish(r.vad,recordEnd,r.reason==="auto_limit" && !!this.series);}
          catch (_) { /* Preserve this full container on any detector failure. */ }
          if (this.recording === r && r.epoch === this.epoch && !vadResult.valid)
            this.vadDiagnostic = vadDiagnostic(ownDiagnostic(r.vad) || {stage:"tail",reason:"tail"},ownMetrics(r.vad));
        }
        if(recorder.samples===0 && recorder.candidate?.() && !r.error && recorder.idlePCM?.length) {
          const ok=await r.captureOwner?.encodeIdle(recorder);
          if(!ok)r.error=true;
          durationMs=Math.round(recorder.samples/48);
        }
        if(recorder.samples!==undefined) {
          r.metrics.client_capture_prefix_ms=Math.round(recorder.prefixSamples/48);
          r.metrics.client_capture_queue=recorder.peak;
          Object.assign(r.metrics,recorder.gapMetrics||{},recorder.stats?.metrics||recorder.metrics||{});
          for(const frame of recorder.idlePCM||[])frame.fill(0);recorder.idlePCM=[];
        }
        for (const [track,name,fn] of r.listeners) track.removeEventListener(name,fn);
        const observed=ownMetrics(r.vad);
        for(const [name,key,max] of [["client_vad_gaps","gapCount",65],["client_vad_lost_ms","lostMs",60000],["client_vad_start_gaps","startGapCount",65],["client_vad_start_skip_ms","startSkippedMs",60000]]) {
          const value=Object.getOwnPropertyDescriptor(observed || {},key)?.value;
          if(Number.isSafeInteger(value) && value>=0 && value<=max)r.metrics[name]=value;
        }
        const blob = new this.e.Blob(r.chunks, {type:r.mime});
        if(Number.isFinite(r.endedAt) && r.metrics)r.metrics.client_upload_flush=Math.max(0,Math.round(this.e.now()-r.endedAt));
        const clip = {barge:r.barge,idleSilent:recorder.samples===0 && recorder.idleFrames>=2 && !recorder.failed, id:r.id, mime:r.mime, blob, metrics:r.metrics, overflow:r.overflow || blob.size > this.config.maxAudioBytes,
          error:r.error, durationMs, endedAt:r.endedAt, reason:r.reason || "interrupted", voiced:!!r.voiced, energyVoiced:!!r.energyVoiced, noiseFloor:r.noiseFloor, quality:r.quality, vad:vadResult, uploader:r.uploader};
        const cutoff=!clip.reason.startsWith("auto_") ? (clip.endedAt ?? r.start+durationMs)-CONTROL_NOISE_WINDOW_MS : r.start+durationMs;
        const energyMs=(r.retentionEnergy || []).reduce((ms,[a,b])=>ms+Math.max(0,Math.min(b,cutoff)-a),0);
        const energyRunMs=(r.retentionEnergy || []).reduce((ms,[a,b])=>Math.max(ms,Math.max(0,Math.min(b,cutoff)-a)),0);
        clip.gate={energyMs,energyRunMs,fallbackVoice:!!r.fallbackVoice};
        const observe=(key,value)=>{if(Number.isFinite(value) && value>=0)clip.metrics[key]=Math.min(3600000,Math.round(value));};
        observe("client_gate_vad_ms",vadResult.speechMs);
        observe("client_gate_vad_high_ms",vadResult.highConfidenceMs);
        observe("client_gate_vad_peak_permille",vadResult.peakProbability*1000);
        observe("client_gate_vad_prior_ms",vadResult.speechBeforeControlMs);
        observe("client_gate_energy_ms",energyMs);
        observe("client_gate_energy_run_ms",energyRunMs);
        observe("client_gate_fallback_ms",r.fallbackMs || 0);
        const lastEnergy=r.retentionEnergy?.at(-1)?.[1];
        observe("client_gate_last_ms",Math.min(vadResult.lastEvidenceMs ?? Infinity,
          Number.isFinite(lastEnergy)?Math.max(0,(clip.endedAt ?? r.start+durationMs)-lastEnergy):Infinity));
        // Quantized RMS in millionths, never a waveform or device identifier.
        observe("client_gate_floor_ppm",Number.isFinite(r.noiseFloor)?r.noiseFloor*1000000:undefined);
        this.silentClip(clip);
        const unknown=clip.metrics.client_gate_unknown_reason;
        // A valid detector can still have uncovered input. Weak fallback
        // energy is deliberately conservative, not proof of speech: settle it
        // with the retained full PCM before discarding or sending this clip.
        // Complete online voice/high-confidence and the existing 300 ms
        // strong-energy retention need no extra pass. Recheck a VAD gap,
        // retaining its positive evidence even if the replay is negative.
        const definiteKeep=!vadResult.coverageGap && [1,4].includes(clip.metrics.client_gate_decision) || energyRunMs>=300;
        const incomplete=!!(unknown & (8|16|32|64|128|256|512)) || clip.metrics.client_vad_gaps>0;
        // Only continuous capture owns full PCM. Keep the original cold,
        // automatic-energy and barge rechecks, including their 2500 ms budget.
        if(owned && recorder.samples>0 && !clip.error && !clip.overflow && !clip.postStarted &&
            (this.barge || vadResult.valid!==true && (clip.reason==="auto_energy" || !clip.quality?.valid || energyMs===0) ||
              incomplete && !definiteKeep) && this.detector?.classifyPCM) {
          const began=this.e.now();clip.metrics.client_gate_recheck=1;
          try {
            const result=await this.detector.classifyPCM(recorder.pcm,this.config,2500,vadResult.valid===true && incomplete);
            if(result?.valid && this.recording===r && this.reference && !this.locked) {
              // Coverage gaps cannot erase frames already observed online. Keep
              // each pass separate: summing them would count the same PCM twice.
              clip.onlineVAD=vadResult;clip.vad=vadResult=result;clip.metrics.client_gate_recheck=2;
              const online=clip.onlineVAD.valid===true?clip.onlineVAD:{};
              const union=key=>Math.max(result[key] || 0,online[key] || 0);
              observe("client_gate_vad_ms",union("speechMs"));observe("client_gate_vad_high_ms",union("highConfidenceMs"));
              observe("client_gate_vad_peak_permille",union("peakProbability")*1000);observe("client_gate_vad_prior_ms",union("speechBeforeControlMs"));
            }else clip.metrics.client_gate_recheck=3;
          }catch(_){clip.metrics.client_gate_recheck=3;}
          clip.metrics.client_gate_recheck_ms=Math.min(60000,Math.max(0,Math.round(this.e.now()-began)));
          clip.metrics.client_gate_initial_reason=unknown;this.silentClip(clip);
        }
        for(const frame of recorder.pcm||[])frame.fill(0);recorder.pcm=[];
        r.clip = clip;
        if(r.releaseAfterStop && owned)this.releaseMic();
        if (!owned || this.recording !== r) {r.uploader?.abandon(); r.resolve(clip); return;}
        this.clip = clip; this.recording = null; this.syncPCMCapability();
        if(r.echoHold && !this.ending && !this.locked && !this.muted && !this.suspended &&
            !this.e.isHidden?.() && this.stream===r.stream && !this.clipRejection(clip)) {
          try{this.echo=new EchoProbe(this,true);}catch(_){for(const t of r.stream.getTracks())t.enabled=false;}
        } else if(r.echoHold && !this.capture)for(const t of r.stream.getTracks())t.enabled=false;
        r.resolve(clip);
        if (this.clipRejection(clip)) {
          this.releaseMic();this.reportRejectedClip(clip);
          this.show("这段没录好，尚未提交", "本段仍留在本页；点击继续会丢弃本段并重新收音。");
        }
        else if (!r.stopping) this.show("录音已中断，本段尚未确认接收", "已收齐停止事件的片段留在本页；请核查或重试提交，不会假装仍在倾听。");
      };
      r.metrics={};
      if(this.config.captureMode!=="legacy") {
        Object.assign(r.metrics,this.captureInputDiagnostics||{});
        if(this.captureInputDiagnostics) {
          if(Number.isFinite(this.captureRebuildMs))r.metrics.client_capture_rebuild_ms=this.captureRebuildMs;
          r.metrics.client_capture_input_recovery=recorder.samples!==undefined?2:this.captureInputDiagnostics.client_capture_input_recovery===4||this.captureFailure!==7?4:this.captureDisabled?3:1;
          this.captureInputDiagnostics=null;
        }
        r.metrics.client_capture_rebuilds=this.captureRebuilds||0;
        Object.assign(r.metrics,this.captureStartupDiagnostics||{});
        r.metrics.client_capture_startup_retries=this.captureStartupRetries||0;
        r.metrics.client_capture_startup_state=this.captureStartupPending?(this.captureDisabled?3:1):Number.isFinite(this.captureStartupRecoveredMs)?2:0;
        if(Number.isFinite(this.captureStartupRecoveredMs))r.metrics.client_capture_startup_recovered_ms=this.captureStartupRecoveredMs;
      }
      if(recorder.samples!==undefined) {
        recorder.retainPCM=true;
        r.metrics.client_capture_mode=1;
        recorder.candidate=()=>!!this.barge || !!r.vad?.candidate || !!r.fallbackVoice;
        recorder.onlimit=()=>{void this.submitSegment("auto_limit").catch(e=>this.report(e));};
        recorder.onfallback=code=>{
          r.metrics.client_capture_fallback=code;
          if(code===7)r.error=true;
          // No audio commit has occurred: abandon the incompatible partial container.
          r.uploader?.abandon();r.uploader=null;r.chunks=[];r.size=0;
          if(code===7)void this.submitSegment("capture_error").catch(e=>this.report(e));
        };
        recorder.onrecover=mime=>{
          r.mime=mime;
          this.e.clearTimeout(r.stopTimer);
          r.stopTimer=this.e.setTimeout(()=>{if(!r.stopped)this.rejectRecording(r);},Math.ceil(recorder.samples/48)+3000);
          this.show("正在恢复本段录音","正在保留本段完整内容，请保持前台。");
        };
      } else r.metrics.client_capture_mode=0;
      if(this.captureFailure && recorder.samples===undefined){r.metrics.client_capture_fallback=this.captureFailure;Object.assign(r.metrics,this.captureDiagnostics||{});}
      r.energyObserved=!!this.analyser;
      const state=audioState(r.context);if(state!==undefined)r.metrics.client_audio_state_start=state;
      if(this.modelPreparation) {
        if(Number.isFinite(this.clickedAt))this.preparationMetrics[this.warmStart?"client_start_warm":"client_start_cold"]=Math.round(this.e.now()-this.clickedAt);
        this.clickedAt=null;this.sendPreparationMetrics();
        r.metrics[signal?.valid?"client_vad_ready":"client_vad_fallback"]=1;
        if(!this.capturedSegments)r.metrics[signal?.valid?"client_vad_first_ready":"client_vad_first_fallback"]=1;
        this.capturedSegments=(this.capturedSegments || 0)+1;
      }
      if(this.barge)this.barge.captureStarted=true;
      this.recording = r; this.syncPCMCapability(); recorder.start(250);
      try{this.capability?.start(this.audioContext,this.detector);}catch(_){}
      if(this.series?.handoffClip) {this.series.handoffClip.metrics.client_segment_handoff_ms=Math.max(0,Math.round(this.e.now()-this.series.handoffClip.endedAt));this.series.handoffClip=null;}
      this.analysisDiagnostic = "";
      this.apiDiagnostic = ""; this.apiFailure = null;
      this.vadDiagnostic = !this.e.createVAD ? "" : vadDiagnostic(signal?.valid
        ? {stage:"ready",reason:"ready"} : !this.audioContext ? {stage:"prepare",reason:"context_missing"}
        : this.audioContext.state !== "running" ? {stage:"prepare",reason:"context_inactive"}
        : preparation ? {stage:"prepare",reason:"preparing"} : ownDiagnostic(this.detector));
      if (preparation) {
        const detector = this.detector;
        const update = ready => {
          if (this.recording !== r || r.stopping || r.stopped || r.epoch !== this.epoch ||
              this.detector !== detector || this.ending || this.locked || this.muted || this.suspended || this.round || this.speech) return;
          r.vadPreparation.pending = false; r.vadPreparation.ready = ready === true;
          this.updateVADPreparation(r); // Notice only: this recording stays unanalysed.
        };
        void preparation.then(update,() => update(false)).catch(() => {});
      }
      const captureNotice = this.e.createVAD
        ? signal?.valid ? `人声停顿约${this.config.vadRedemptionMs/1000}秒切段；也可以点下方“点击发送”。`
          : `正在用音量判断停顿，说完稍等，或点下方“点击发送”；最长约${this.config.segmentMs/1000}秒提交。`
        : this.analyser
        ? `约停顿${this.config.silenceMs/1000}秒或连续${this.config.segmentMs/1000}秒提交；也可以点下方“点击发送”。`
        : `本浏览器无法检测停顿，请点下方“点击发送”；最长约${this.config.segmentMs/1000}秒切段。`;
      this.show(this.modelPreparation ? "可以说话" : "等待声音", [this.silentNotice?"没有听到声音，请再说一次。":"", this.playbackNotice, captureNotice].filter(Boolean).join("\n"));
      this.tick(r);
    } catch (_) {if(this.series)this.series.failed="capture";this.releaseMic(); this.show("录音未能开始，请检查浏览器权限与格式");}
  }
  vadPreparationCode(r) {
    if (!r.vadPreparation || r.vad || this.recording !== r || r.epoch !== this.epoch ||
        r.detector !== this.detector || r.stopping || r.stopped || this.ending || this.locked ||
        this.muted || this.suspended || this.round || this.speech) return null;
    const current = vadCode(ownDiagnostic(r.detector));
    if (!r.vadPreparation.pending && !r.vadPreparation.ready &&
        (!current || ["preparing","ready","ready_next"].includes(current.reason)))
      return {stage:current?.stage || "prepare",reason:"unknown"};
    if (current && !(current.stage === "ready" && current.reason === "ready" && r.vadPreparation.ready)) return current;
    return r.vadPreparation.ready ? {stage:"ready",reason:"ready_next"}
      : r.vadPreparation.pending ? {stage:"prepare",reason:"preparing"} : current;
  }
  updateVADPreparation(r) {
    const code = this.vadPreparationCode(r);
    if (!code) return;
    const value = vadDiagnostic(code,ownMetrics(r.detector));
    if (value !== this.vadDiagnostic) {
      this.vadDiagnostic = value;
      try {this.render();} catch (_) { /* Preparation observation cannot interrupt the capture tick. */ }
    }
  }
  updateVADFailure(r) {
    if (!r.vad || r.vad.valid !== false || this.recording !== r || r.epoch !== this.epoch ||
        r.detector !== this.detector || r.stopping || r.stopped || this.ending || this.locked ||
        this.muted || this.suspended || this.round || this.speech) return;
    const value = vadDiagnostic(ownDiagnostic(r.vad),ownMetrics(r.vad));
    if (value !== this.vadDiagnostic) {
      this.vadDiagnostic = value;
      try {this.render();} catch (_) { /* Observation cannot stop capture/control. */ }
    }
  }
  sampleRecording(r) {
    const q = r.quality, context = r.context, now = this.e.now();
    let reason = "identity", fatal = true, schedulingGap = false;
    try {
      if (this.recording !== r || this.stream !== r.stream || this.audioContext !== context ||
          // A control invalidates asynchronous work before requesting stop.
          // Its own final sample still owns the same recording/stream/graph.
          (r.epoch !== this.epoch && !(r.stopping && (r.releaseAfterStop || r.recorder.samples!==undefined))) || (r.vad && this.detector !== r.detector))
        throw new Error("call_page.analysis_identity");
      reason = "context";
      if (!context || context.state !== "running" || !Number.isFinite(context.currentTime) || context.currentTime < 0 ||
          !Number.isFinite(context.sampleRate) || context.sampleRate < (r.vad ? 16000 : 8000) || context.sampleRate > 192000)
        throw new Error("call_page.analysis_context");
      reason = "track";
      const tracks = r.stream.getTracks();
      if (!tracks.length || tracks.some(t => t.readyState !== "live" || t.enabled !== true || t.muted !== false))
        throw new Error("call_page.analysis_track");
      // The independent worklet validates its own PCM/clock/sequence. Auxiliary
      // analyser coverage cannot revoke an otherwise healthy signal.
      fatal = false; reason = "analyser_missing";
      if (!this.analyser) throw new Error("call_page.analysis_unavailable");
      reason = "read";
      const ct = context.currentTime, windowMs = this.analyser.fftSize/context.sampleRate*1000;
      reason = "gap";
      const wallGap = now-q.lastWall, contextGap = (ct-q.lastContext)*1000;
      // Adjacent start/stop callbacks can observe the same quantized audio
      // clock. No new PCM exists to sample yet; keep the previous edge so a
      // real stall or coverage hole still fails on the next scheduled sample.
      if(contextGap===0 && wallGap>0 && wallGap<Math.min(this.config.sampleMs,windowMs*.8))return;
      // Coverage holes prohibit silent-audio discard, but small scheduler
      // jitter must not disable either the worklet or subsequent energy samples.
      if(wallGap>windowMs*.8 || contextGap>windowMs*.8 || wallGap>0 && contextGap<=0) {
        q.valid=false;r.metrics.client_gate_aux_fault=1;
        r.metrics.client_gate_aux_wall_ms=Math.min(60000,Math.max(0,Math.round(wallGap)));
        r.metrics.client_gate_aux_clock_ms=Math.min(60000,Math.max(0,Math.round(contextGap)));
      }
      q.lastWall=now;q.lastContext=ct;
      q.stalledMs=contextGap<=0?(q.stalledMs || 0)+Math.max(0,wallGap):0;
      if (!Number.isFinite(now) || !Number.isFinite(windowMs) || windowMs<=0 ||
          !Number.isFinite(wallGap) || !Number.isFinite(contextGap) || wallGap<0 || contextGap<0 ||
          wallGap>500 || contextGap>500 || q.stalledMs>500) {
        // Missing samples cannot prove silence or revoke voice already heard.
        // Restart the quiet interval here; never endpoint across the missing span.
        schedulingGap=Number.isFinite(now) && Number.isFinite(windowMs) && windowMs>0 &&
          Number.isFinite(wallGap) && wallGap>500 && Number.isFinite(contextGap) && contextGap>0;
        r.energySpeechMs=0;if(schedulingGap)r.energyGapAt=now;
        throw new Error("call_page.analysis_gap");
      }
      reason = "read";
      const values = new Float32Array(this.analyser.fftSize);
      this.analyser.getFloatTimeDomainData(values);
      let power = 0; reason = "sample";
      for (const value of values) {
        if (!Number.isFinite(value) || Math.abs(value) > 1) throw new Error("call_page.analysis_sample");
        if (value !== 0) q.zero = false;
        power += value*value;
      }
      const elapsed=now-r.start;
      if(power>0 && r.metrics.client_energy_first_ms===undefined && Number.isFinite(elapsed) && elapsed>=0 && elapsed<=3600000)
        r.metrics.client_energy_first_ms=Math.round(elapsed);
      const rms=Math.sqrt(power/values.length);
      // Fallback retention includes the control tail and every adaptive-energy
      // excursion, without the stronger model-assisted duration requirement.
      // Test BEFORE learning this sample: first-sample soft speech must not
      // become its own quiet floor. Ambiguous clicks/noise are kept here.
      if(r.vad?.valid!==true && r.noiseFloor===undefined && this.silentEnergyFloor?.reference===this.reference)
        r.noiseFloor=this.silentEnergyFloor.value;
      const fallbackThreshold=Math.max(.0015,Math.min(.025,this.config.rmsThreshold,(r.noiseFloor || 0)*3));
      if(rms>=fallbackThreshold) {
        r.fallbackVoice=true;r.lastFallbackActivity=now;
        r.fallbackMs=(r.fallbackMs || 0)+Math.max(1,Math.min(Math.max(0,wallGap),this.config.sampleMs*1.5));
      }
      // Follow the quiet floor, not the loudest syllable. A fixed .025 gate
      // misses soft phone speech; scale the meter independently of detection.
      // Ignore graph-start zeros when learning the first real quiet floor;
      // otherwise a steady fan is misread as speech for many seconds.
      if(rms>0) {
        r.noiseFloor ??= rms;
        r.noiseFloor+=(rms-r.noiseFloor)*(rms<r.noiseFloor ? .5 : .01);
      }
      // Keep the quiet-floor learner and endpointing intact. Retention has a
      // stronger independent threshold and requires a continuous run.
      if(rms>=Math.max(.006,(r.noiseFloor || 0)*6)) {
        const a=now-Math.min(Math.max(0,wallGap),this.config.sampleMs*1.5),spans=r.retentionEnergy ||= [],last=spans.at(-1);
        if(last && a<=last[1]+1)last[1]=now;else spans.push([a,now]);
      }
      const threshold=Math.max(.0015,Math.min(.025,this.config.rmsThreshold,(r.noiseFloor || 0)*3));
      r.level=rms===0?0:Math.min(1,Math.sqrt(rms)*2.5);
      q.frames++; q.lastWall = now; q.lastContext = ct;
      r.energyValid=contextGap>0 || wallGap===0;
      if (rms >= threshold) {
        if (now-(r.lastEnergyVoice ?? -Infinity)>400) r.energySpeechMs=0;
        r.energySpeechMs=(r.energySpeechMs || 0)+Math.min(Math.max(0,wallGap),this.config.sampleMs*1.5);
        r.lastEnergyVoice=now;if(r.energySpeechMs>=300) {r.energyVoiced=true;this.silentNotice=false;}
        r.voiced = true; r.lastVoice = now;
        if (q.valid && !r.stopping && this.message !== "检测到声音，正在听取") this.show("检测到声音，正在听取");
      } else if (now-(r.lastEnergyVoice ?? -Infinity)>400) r.energySpeechMs=0;
    } catch (_) {
      r.metrics.client_gate_aux_fault=({identity:2,context:3,track:4,analyser_missing:5,read:6,gap:1,sample:7})[reason]||8;
      const first = q.valid; q.valid = false;r.energyValid=false;r.energySpeechMs=0;
      if(!schedulingGap)r.energyVoiced=false;
      if (fatal && r.vad) {
        const code = reason === "track" ? "track" : reason === "context" ? "context_inactive" : "unknown";
        try {r.detector.fail(r.vad,"quality",code);} catch (_) {r.vad.valid = false;}
      }
      if (this.recording === r && r.epoch === this.epoch && !this.ending && !this.locked && !this.muted && !this.suspended) {
        // reason is a local constant at each boundary, never an exception field.
        this.analysisDiagnostic = "辅助分析：analysis/"+reason+"。";
        if (fatal && r.vad) this.vadDiagnostic = vadDiagnostic(ownDiagnostic(r.vad),ownMetrics(r.vad));
        const preparation = this.vadPreparationCode(r);
        if (first && !r.stopping) this.show("正在听取", r.vad?.valid === true
          ? "辅助音量分析已降级，人声检测仍在工作；也可以点下方“点击发送”。"
          : preparation?.reason === "preparing"
            ? "本段使用音量停顿，说完稍等，或点下方“点击发送”。"
            : preparation?.reason === "ready_next"
              ? "本段使用音量停顿，说完稍等，或点下方“点击发送”。"
              : "停顿检测不可用，请点下方“点击发送”；到达时长上限仍会保留并提交整段。");
      }
    }
  }
  armUpload(r) {
    if (r.uploader && !r.uploader.armed && !this.chunkUnavailable && (r.energyVoiced || r.vad?.candidate)) r.uploader.arm();
  }
  tick(r) {
    if (this.recording !== r || r.stopping) return;
    this.armUpload(r);
    this.updateVADPreparation(r); this.updateVADFailure(r);
    const now = this.e.now(), previousLevel=r.level,previousEnergy=r.energyValid; this.sampleRecording(r);
    if(r.level!==previousLevel || r.energyValid!==previousEnergy)this.render();
    this.promoteLongBarge(r);
    if(this.barge && !this.series && (r.vad?.valid || r.energyValid && r.quality.valid)) {
      const edge=r.vad?.valid?r.start+(r.vad.lastFrame-r.vad.recordStart)/r.vad.rate*1000:now;
      const voice=Math.max(r.start,r.lastEnergyVoice??-Infinity,
        r.vad?.lastCandidate>0?r.start+(r.vad.lastCandidate-r.vad.recordStart)/r.vad.rate*1000:-Infinity);
      if(Math.min(now,edge)-voice>=this.bargeSilenceMs(r.vad?.valid)) {
        void this.submitSegment("auto_silence").catch(e=>this.report(e));return;
      }
    }
    if(this.series) {
      const series=this.series;
      if(r.voiced)series.lastVoiceAt=Math.max(series.lastVoiceAt,r.lastVoice);
      if(r.vad?.valid && r.vad.lastCandidate>0)series.lastVoiceAt=Math.max(series.lastVoiceAt,r.start+(r.vad.lastCandidate-r.vad.recordStart)/r.vad.rate*1000);
      // A valid detector must cover the quiet span; a newly armed detector is
      // not evidence of silence. Energy also vetoes model-negative soft voice.
      const edge=r.vad?.valid ? r.start+(r.vad.lastFrame-r.vad.recordStart)/r.vad.rate*1000 : now;
      if((r.vad?.valid || r.energyValid) && Math.min(now,edge)-Math.max(series.lastVoiceAt,r.energyGapAt ?? -Infinity)>=(this.barge?this.bargeSilenceMs(r.vad?.valid):this.config.silenceMs)) {
        void this.submitSegment(r.vad?.valid?"auto_silence":"auto_energy").catch(e=>this.report(e));return;
      }
    }
    const limit = r.recorder.samples===undefined && now-r.start >= this.config.segmentMs;
    if (limit || (!this.series && r.vad?.valid!==true && r.energyValid && (r.fallbackMs>0 || r.energyVoiced || r.vad?.voiced) && now-Math.max(r.start,r.lastFallbackActivity ?? -Infinity,r.lastEnergyVoice ?? -Infinity,r.energyGapAt ?? -Infinity) >= this.config.silenceMs)) {
      void this.submitSegment(limit ? "auto_limit" : "auto_energy").catch(error => this.report(error)); return;
    }
    r.timer = this.e.setTimeout(() => this.tick(r), this.config.sampleMs);
  }
  async finishSegment(reason = "manual") {
    const r = this.recording;
    try{if(this.capability?.runtimeStarted)this.capability.finish();}catch(_){}
    if (!r) {
      if (this.clip && !reason.startsWith("auto_")) this.clip.reason = reason;
      return this.clip;
    }
    // Explicit controls upgrade an auto stop already waiting for final bytes.
    if(!reason.startsWith("auto_") && r.controlFrame===undefined)r.controlFrame=nativeFrame(r.context,r.vad?.rate);
    if (!r.reason || !reason.startsWith("auto_")) r.reason = reason;
    if(r.metrics) {
      for(const key of ["client_segment_vad_endpoint","client_segment_auto","client_segment_auto_energy","client_segment_auto_limit","client_segment_manual","client_segment_control"])delete r.metrics[key];
      if(r.reason==="auto_silence")r.metrics.client_segment_vad_endpoint=1;
      r.metrics[r.reason==="auto_energy"?"client_segment_auto_energy":r.reason==="auto_limit"?"client_segment_auto_limit":r.reason.startsWith("auto_")?"client_segment_auto":r.reason==="manual"?"client_segment_manual":"client_segment_control"]=1;
    }
    if (!r.stopping) {
      const vadQuiet=r.reason==="auto_silence" && r.vad?.valid && r.vad.lastCandidate>0 &&
        Number.isFinite(r.vad.lastFrame) ? (r.vad.lastFrame-r.vad.lastCandidate)/r.vad.rate*1000 : undefined;
      const last=Math.max(this.series?.lastVoiceAt ?? -Infinity,r.lastEnergyVoice ?? -Infinity,
        r.vad?.valid!==true ? r.lastFallbackActivity ?? -Infinity : -Infinity,
        r.vad?.valid && r.vad.lastCandidate>0 ? r.start+(r.vad.lastCandidate-r.vad.recordStart)/r.vad.rate*1000 : -Infinity);
      if(Number.isFinite(vadQuiet) || Number.isFinite(last))r.metrics.client_endpoint_quiet_ms=Math.max(0,Math.round(vadQuiet ?? this.e.now()-last));
      if(r.recorder.samples!==undefined)r.captureEnd=nativeFrame(r.context,r.vad?.rate || r.context?.sampleRate);
      r.stopping = true; r.endedAt = this.e.now(); this.e.clearTimeout(r.timer);
      if(r.recorder.samples!==undefined){this.sampleRecording(r);r.capturedFinal=true;}
      r.uploader?.freeze();
      // A broken recorder may never dispatch stop, even after going inactive.
      // Preserve its bytes as unsubmitted and offer recovery instead of waiting forever.
      r.stopTimer=this.e.setTimeout(()=>{if(this.recording===r && !r.stopped)this.rejectRecording(r);},5000);
      // stop event is after final dataavailable; never upload timeslice blobs.
      try {
        // An error or track end makes the recorder inactive BEFORE its queued
        // final dataavailable + stop. Wait for those events and keep the final bytes.
        r.recorder.continueAfter=r.reason==="auto_limit" && !!this.series;
        if (r.recorder.state !== "inactive") r.recorder.stop();
      } catch (_) {
        this.rejectRecording(r,true);
      }
    }
    return r.done;
  }
  rejectRecording(r,stopThrew=false) {
    if(this.recording!==r || r.stopped)return;
    r.recorder.worker?.terminate();
    for(const frame of [...(r.recorder.pcm||[]),...(r.recorder.idlePCM||[])])frame.fill(0);
    r.recorder.pcm=[];r.recorder.idlePCM=[];
    r.error=true;r.stopped=true;this.e.clearTimeout(r.stopTimer);this.releaseMic();
    for(const [track,name,fn] of r.listeners)track.removeEventListener(name,fn);
    const clip={id:r.id,mime:r.mime,blob:new this.e.Blob(r.chunks,{type:r.mime}),error:true,stopThrew,metrics:r.metrics,
      durationMs:Math.max(0,Math.round((r.endedAt ?? this.e.now())-r.start)),reason:r.reason,endedAt:r.endedAt,quality:r.quality};
    r.uploader?.abandon();this.clip=clip;this.recording=null;r.clip=clip;r.resolve(clip);this.reportRejectedClip(clip);
    this.show("这段没录好，尚未提交", "录音结束未确认，已收片段保留在本页；点击继续会丢弃本段并重新收音。");
  }
  seriesCapability(value) {
    const cap=value?.seamless_series;
    this.seamlessMax=cap?.version===1 && Number.isInteger(cap.max_segments) && cap.max_segments>=1 && cap.max_segments<=20?cap.max_segments:0;
  }
  silentClip(clip) {
    const vad=clip.vad,q=clip.quality,gate=clip.gate;
    // Bit flags describe missing evidence even when the other independent
    // source can decide. Keep every flag, rather than hiding secondary faults.
    const reason=(clip.postStarted?1:0) | (clip.error?2:0) | (clip.overflow?4:0) |
      (vad?.valid!==true?8:0) | (vad?.coverageGap?16:0) | (!q?.valid?32:0) |
      (!(q?.frames>=2)?64:0) | (vad && !Number.isFinite(vad.speechMs)?128:0) |
      (vad && !Number.isFinite(vad.speechBeforeControlMs)?256:0) |
      (!gate || !Number.isFinite(gate.energyRunMs)?512:0);
    const safe=!(reason & (1|2|4|512)) && (vad?.valid!==true || !(reason & (128|256)));
    const modelKnown=!(reason & (8|16|128|256));
    const energyKnown=!(reason & (32|64)) && typeof gate?.fallbackVoice==="boolean";
    // Incomplete coverage cannot prove silence, but valid observed frames
    // can prove speech. An offline negative cannot revoke that evidence.
    const evidence=[vad,clip.onlineVAD].filter(v=>v?.valid===true &&
      Number.isFinite(v.speechMs) && Number.isFinite(v.speechBeforeControlMs));
    const voice=safe && evidence.some(v=>v.speechMs>=RETAIN_SPEECH_MS);
    const high=safe && evidence.some(v=>v.highConfidenceFrames>=1);
    const energy=safe && (modelKnown ? gate.energyRunMs>=300 : energyKnown && gate.fallbackVoice);
    const nativeIdle=clip.idleSilent && !clip.error && !clip.overflow && !clip.postStarted;
    const known=nativeIdle || safe && (modelKnown || energyKnown || voice || high);
    clip.metrics ||= {};
    clip.metrics.client_gate_unknown_reason=reason;
    clip.metrics.client_gate_decision=!known?0:voice?1:high?4:energy?2:3;
    return known && !voice && !high && !energy;
  }
  dropSilentClip(clip) {
    // Only a settled, never-submitted capture with an independently complete
    // source can be discarded. Uncertain POSTs and recorder errors keep bytes.
    if(!this.silentClip(clip)) {this.silentEnergyFloor=null;this.silentEnergyDrops=0;return false;}
    if(clip.reason==="auto_energy" && clip.vad?.valid && !clip.vad.coverageGap && Number.isFinite(clip.noiseFloor)) {
      // Retain the learned fan/room floor across silent automatic segments.
      // A new segment must not rediscover the same steady noise as first voice.
      this.silentEnergyFloor={reference:this.reference,value:clip.noiseFloor};
    }
    clip.uploader?.abandon();if(this.clip===clip)this.clip=null;
    const b=clip.barge;
    if(b && !b.registerFailed && !b.cancelled && this.barge===b && !this.series?.count)
      void b.register.then(()=>this.bargeRequest(b,"silent")).then(row=>this.settleBarge(row)).catch(()=>{
        if(this.barge===b)this.closeBarge("error");
      });
    if(clip.reason==="manual") {
      this.silentNotice=true;this.silentNoticeUntil=this.e.now()+3000;
      this.e.clearTimeout(this.silentNoticeTimer);
      this.silentNoticeTimer=this.e.setTimeout(()=>{this.silentNotice=false;this.render();},3000);
      this.show("没有听到声音，请再说一次", "请再说一次。");
    }
    // Automatic drops never show a notice. After three consecutive drops,
    // telemetry backs off to at most one report per 30s; capture stays live.
    const autoEnergy=clip.reason==="auto_energy";
    if(autoEnergy)this.silentEnergyDrops=(this.silentEnergyDrops||0)+1;
    const reportDrop=!autoEnergy || this.silentEnergyDrops<=3 || this.e.now()-(this.silentEnergyReportAt??-Infinity)>=30000;
    if(!clip.silentDropReported && reportDrop && this.reference && !this.locked) {
      if(autoEnergy)this.silentEnergyReportAt=this.e.now();
      clip.silentDropReported=true;
      const reason={control:1,manual:2,auto_limit:3,auto_silence:4,auto_energy:5};
      void this.api("metrics",{generation_id:"capture:"+clip.id,
        values:{...Object.fromEntries(Object.entries(clip.metrics).filter(([key])=>key.startsWith("client_gate_")||key.startsWith("client_capture_"))),
          client_silent_drop:1,client_silent_reason:reason[clip.reason] || 6}}).catch(()=>{});
    }
    return true;
  }
  enqueueSeriesClip(clip,final) {
    const series=this.series;
    if(!series || clip.seriesQueued)return;
    if(series.count>=series.limit)throw new RequestFailure();
    clip.ownerSeries=series;clip.series={id:series.id,index:++series.count,final};clip.seriesQueued=true;
    series.rootClip ||= {id:clip.id,sha:clip.sha,mime:clip.mime,byte_count:clip.blob.size};series.queue.push(clip);if(this.clip===clip)this.clip=null;
    if(final) {series.finishing=true;this.seriesRound(series,clip);}
  }
  seriesRound(series,clip) {
    const root=series.rootClip || this.server?.audio?.find(a=>a.utterance_id===series.id);
    this.round ||= {id:series.id,seriesId:series.id,sha:root?.sha || root?.sha256,mime:root?.mime,byte_count:root?.blob?.size || root?.byte_count,
      endedAt:clip?.endedAt,metrics:clip?.metrics || {}};
  }
  async submitSeriesClip(clip) {
    const series=this.series;if(!series || clip.seriesQueued)return series?.uploadTask || true;
    if(this.clipRejection(clip)) {
      // The first rejected capture never created a server-side series.
      if(!series.count) {this.series=null;return this.sendClip();}
      series.failed="capture";series.rejected=clip;this.releaseMic();this.reportRejectedClip(clip);this.render();return false;
    }
    if(this.dropSilentClip(clip)) {
      if(!series.count) {this.series=null;await this.listen();return true;}
      if(!this.ending)return this.sealSeries(clip.endedAt,clip.metrics);
      return true;
    }
    const final=!this.ending && (clip.reason!=="auto_limit" || series.count+1>=series.limit);
    this.enqueueSeriesClip(clip,final);
    const uploading=this.flushSeries();
    if(!final && !this.ending && !this.muted && !this.suspended) {
      series.handoffClip=clip;await this.listen();
    }
    return uploading;
  }
  async pauseSeries(reason) {
    const series=this.series;if(!series)return;
    series.failed=reason;
    const clip=await this.finishSegment("series_failure");
    if(this.series!==series)return;
    if(clip && !clip.seriesQueued) {
      if(this.clipRejection(clip)) {series.rejected=clip;this.reportRejectedClip(clip);}
      else this.enqueueSeriesClip(clip,false);
    }
    this.render();
  }
  flushSeries() {
    const series=this.series;if(!series)return Promise.resolve(true);
    if(series.uploadTask)return series.uploadTask;
    const task=(async()=>{
      while(this.series===series && series.queue.length) {
        const clip=series.queue[0];
        // At the cap a hangup/abandon must disable generation before delivering the tail.
        if(!clip.series.final && clip.series.index===series.limit) {
          const reason=series.abandonIntent || (this.ending?"local_discard":null);
          if(!reason) {await this.pauseSeries("capture");return false;}
          await this.recordSeriesDecision(series,"abandon",reason,series.abandonLast || series.count);
        }
        if(clip.series.final)this.seriesRound(series,clip);
        if(!await this.sendClipCore(clip,true)) {await this.pauseSeries("upload");return false;}
        if(this.series!==series)return false;
        if(clip.series.index===1) {
          series.rootClip.sha=clip.sha;
          if(this.round?.seriesId===series.id)this.round.sha=clip.sha;
        }
        series.queue.shift();series.confirmedIndex=clip.series.index;clip.uploader?.release();
        // The ACK validated all bytes/identity; keep metadata, release audio.
        clip.blob=null;clip.uploader=null;
        if(clip.series.final)series.finalSubmitted=true;
        else {
          // capture metrics accept gate observations, not reply upload times.
          // Keep this existing request within that contract so diagnostics land.
          const values=Object.fromEntries(Object.entries(clip.metrics || {}).filter(([k])=>k.startsWith("client_gate_")||k.startsWith("client_capture_")));
          void this.api("metrics",{generation_id:"capture:"+clip.id,values}).catch(()=>{});
        }
        void this.refresh().catch(e=>this.report(e));
      }
      return true;
    })().catch(async error=>{this.report(error);await this.pauseSeries("upload");return false;}).finally(()=>{
      if(series.uploadTask===task)series.uploadTask=null;this.render();
    });
    series.uploadTask=task;return task;
  }
  async recordSeriesDecision(series,action,reason,last) {
    try {
      const ack=await this.api("audio-series-control",{series_id:series.id,action,last_index:last,reason});
      if(ack.recorded!==true || ack.series?.series_id!==series.id)throw new RequestFailure();
    } catch(error) {
      const state=await this.api("state"),row=state.series?.find(r=>r.series_id===series.id);
      if(row?.disposition!==(action==="seal"?"sealed":"abandoned_"+reason) || row.series_end_index!==last)throw error;
    }
  }
  async sealSeries(endedAt,metrics={}) {
    const series=this.series;series.sealIntent={endedAt};series.finishing=true;this.seriesRound(series,{endedAt,metrics});
    if(!await this.flushSeries())return false;
    try {
      await this.recordSeriesDecision(series,"seal","no_audio_tail",series.count);
      series.finalSubmitted=true;this.round.uploadAt=this.e.now();await this.refresh();return true;
    } catch(error) {this.report(error);series.failed="upload";return false;}
  }
  async abandonSeries(reason) {
    const series=this.series;if(!series)return;
    await this.pauseSeries(series.failed || "capture");
    series.abandonIntent=reason;series.abandonLast ||= Math.min(series.limit,series.count+(series.rejected?1:0));
    if(!await this.flushSeries())return;
    try {
      await this.recordSeriesDecision(series,"abandon",reason,series.abandonLast);
      series.rejected?.uploader?.abandon();
      if(this.clip===series.rejected)this.clip=null;
      this.series=null;this.round=null;this.apiFailure=null;this.apiDiagnostic="";
      this.show("已保存未完成的表达", "请重新说完整句，缺失部分不会交给Bot补答。");
      if(this.ending)await this.hangup();else await this.listen();
    } catch(error) {this.report(error);series.failed="upload";}
  }
  async reconcileSeries() {
    let series=this.series;
    if(!series && !this.round) {
      const row=this.server?.series?.find(r=>!["abandoned","replied","interrupted"].includes(r.state));
      if(!row)return;
      this.series=series={id:row.series_id,limit:row.max_segments,count:row.received_segments,queue:[],finishing:true,resumed:true};
      if(row.disposition==="open") {series.failed="reopened";series.reviewNeeded=true;return;}
      this.seriesRound(series);series.finalSubmitted=true;
    }
    if(!series)return;
    const row=this.server?.series?.find(r=>r.series_id===series.id);
    if(row?.state==="blocked" && series.failed!=="stt") {
      await this.pauseSeries("stt");this.show("这次表达有一段未识别", "保留已收到部分后，请重新说完整句；不会重发不确定的识别请求。");
    }
  }

  async submitSegment(reason = "manual") {
    const r = this.recording;
    if(!this.series && reason==="auto_limit" && this.seamlessMax>1 && r) {
      this.series={id:r.id,limit:this.seamlessMax,count:0,queue:[],lastVoiceAt:r.voiced?r.lastVoice:this.e.now(),finishing:false};
    }
    if(this.series && r?.vad?.valid && r.vad.lastCandidate>0)this.series.lastVoiceAt=Math.max(this.series.lastVoiceAt,r.start+(r.vad.lastCandidate-r.vad.recordStart)/r.vad.rate*1000);
    const clip = await this.finishSegment(reason);
    if(this.series && clip) return this.submitSeriesClip(clip);
    return this.sendClip();
  }
  received(clip,state) {
    const audio = state.audio.find(item => item.utterance_id === clip.id);
    if (!audio) return false;
    if (audio.sha256 !== clip.sha || audio.byte_count !== clip.blob.size || audio.mime !== clip.mime || clip.series && (audio.series_id!==clip.series.id || audio.series_index!==clip.series.index || !!audio.series_final!==clip.series.final)) throw new RequestFailure(409,"state","validate","invalid-response");
    return true;
  }
  sendClip() {
    if(this.series && this.clip)return this.submitSeriesClip(this.clip);
    if (this.sendTask) return this.sendTask;
    this.sendTask = this.track(() => this.sendClipOnce()).finally(() => {
      this.sendTask = null; this.render();
      if (!this.round && !this.clip && !this.ending) void this.listen().catch(error => this.report(error));
    });
    return this.sendTask;
  }
  async sendClipOnce() {
    const clip = this.clip;
    try {return await this.sendClipCore();}
    finally {clip?.uploader?.release();} // definite result, or reported: the next segment's chunks may go
  }
  clipRejection(clip) {
    // Fixed codes only; never export a browser exception or audio contents.
    if (!clip?.blob) return 0;
    return clip.overflow || clip.blob.size>this.config.maxAudioBytes ? 1 : clip.stopThrew ? 3 : clip.error ? 2 : !clip.blob.size && !clip.idleSilent ? 4 : 0;
  }
  reportRejectedClip(clip) {
    if (clip.rejectionSent || clip.rejectionTask || !this.reference || this.locked) return;
    const values={client_clip_reject:this.clipRejection(clip),client_clip_bytes:clip.blob.size,
      ...Object.fromEntries(Object.entries(clip.metrics||{}).filter(([k])=>k.startsWith('client_capture_')))};
    if(Number.isSafeInteger(clip.durationMs) && clip.durationMs>=0)values.client_clip_ms=clip.durationMs;
    clip.rejectionTask=this.api("metrics",{generation_id:"capture:"+clip.id,values})
      .then(()=>{clip.rejectionSent=true;}).catch(()=>{}).finally(()=>{clip.rejectionTask=null;});
  }
  async discardClip() {
    const clip=this.clip;
    // Only locally rejected, never-submitted bytes may be discarded here.
    if (!this.clipRejection(clip) || this.sendTask || this.locked) return;
    if(this.series) {await this.abandonSeries("local_discard");return;}
    this.reportRejectedClip(clip);clip.uploader?.abandon();this.clip=null;
    this.apiFailure=null;this.apiDiagnostic="";
    this.show("已丢弃本页未提交片段", "已保存的文字仍保留。");
    if(this.ending)await this.hangup();
    else await this.start();
  }
  async sendClipCore(clip = this.clip, seriesDelivery = false) {
    if (!clip) return true;
    // Keep the original identity fence even after mute/background/stop clears
    // the page candidate. A delayed begin must never become an ordinary turn.
    const b=clip.barge;
    if(b) {
      try {await b.register;if(b.control)await b.control;}
      catch(error) {
        if(seriesDelivery)throw error;
        if(this.clip===clip && (!b.registerFailed || !this.dropSilentClip(clip)))this.report(error);
        return false;
      }
    }
    const reference = this.reference, round = this.round, epoch = this.epoch;
    let confirmedRound = null;
    const ownsClip = () => (seriesDelivery ? this.series===clip.ownerSeries : this.clip === clip && this.round === round) && this.reference === reference && !this.locked;
    const seriesHeaders=clip.series?{"X-Bot-Series-ID":clip.series.id,"X-Bot-Series-Index":String(clip.series.index),"X-Bot-Series-Final":String(Number(clip.series.final))}:{};
    if (this.clipRejection(clip)) {
      this.releaseMic(); this.reportRejectedClip(clip);
      this.show("这段没录好，尚未提交", "本段仍留在本页；点击继续会丢弃本段并重新收音，挂断时也可明确丢弃后结束。"); return false;
    }
    if(!seriesDelivery && ownsClip() && this.dropSilentClip(clip))return true;
    // Recovery may enqueue a capture without passing submitSeriesClip.
    if(seriesDelivery && clip.metrics?.client_gate_unknown_reason===undefined)this.silentClip(clip);
    // Upload timing is reported in pieces so a slow step can be named: flush (set when the recorder finished), digest,
    // tail (stop -> frozen ACK cursor; no ACK wait), chunks (requests, re-sends included), precheck (retries only) and post
    // (the commit, or the whole-clip POST).
    const metrics = clip.metrics ||= {}, tick = () => this.e.now(), uploader = clip.uploader;
    const digest = async () => {
      if (clip.sha) return true;
      try {
        const began = tick();
        clip.sha = [...new Uint8Array(await this.e.crypto.subtle.digest("SHA-256",await clip.blob.arrayBuffer()))].map(n => n.toString(16).padStart(2,"0")).join("");
        metrics.client_upload_digest = Math.max(0,Math.round(tick()-began));
        return true;
      } catch (error) {if(ownsClip())this.report(error,"clip_digest");return false;}
    };
    // The first submission goes straight to the upload; only a retry (an earlier commit/POST may already have reached
    // the server: failure, unknown result, recovered page) reads the state first, so nothing is uploaded twice.
    const retry = clip.postStarted === true;
    // Chunks went out while speaking: tail and commit share one POST. A clip whose speech was never
    // detected (or whose chunks failed) goes whole through the old audio route with the same id.
    let streaming = !!uploader && uploader.requests > 0 && !clip.chunkDegraded;
    uploader?.freeze();
    if (!await digest() || !ownsClip()) return false;
    if (uploader?.requests) {
      metrics.client_upload_tail = 0; // No wait for the in-flight chunk ACK.
      metrics.client_upload_chunks = uploader.requests;
    }
    if (uploader?.failed) {clip.chunkDegraded = true; streaming = false;}

    try {
      let state = null;
      if (retry) {
        this.show("正在确认音频接收", "正在确认本段是否收到，未确认前请勿关闭页面。");
        const began = tick();
        state = await this.api("state",undefined,{},false,"clip_precheck");
        metrics.client_upload_precheck = Math.max(0,Math.round(tick()-began));
        if (!ownsClip()) return false;
      }
      if (!(retry && this.received(clip,state))) {
        let accepted = false;
        if (streaming) {
          this.show("正在提交音频，等待接收确认", "正在提交完整片段，请勿关闭；等待到期后只核查接收状态，不会自动重传。");
          clip.postStarted = true;
          try {
            const began = tick();
            clip.tailOffset ??= uploader.offset;
            const ack = await this.api(clip.series?"audio-series-commit":"audio-commit",clip.blob.slice(clip.tailOffset,clip.blob.size,clip.mime),
              {"Content-Type":clip.mime,"X-Bot-Utterance-ID":clip.id,"X-Bot-Tail-Offset":String(clip.tailOffset),
               "X-Bot-Total-Bytes":String(clip.blob.size),"X-Bot-Audio-Sha256":clip.sha,...seriesHeaders},false,"audio_commit");
            if (ack.audio_received !== true || ack.utterance_id !== clip.id || clip.series && (ack.series_id!==clip.series.id || ack.series_index!==clip.series.index || ack.series_final!==clip.series.final)) throw new RequestFailure(0,"audio-commit","validate","invalid-response","audio_commit");
            metrics.client_upload_post = Math.max(0,Math.round(tick()-began));
            accepted = true;
          } catch (error) {
            if (!ownsClip()) return false;
            if (CHUNK_UNUSABLE.includes(failureData(error).status)) clip.chunkDegraded = true; // same id, complete Blob, old route
            else {
              state = await this.api("state",undefined,{},false,"audio_readback");
              if (!this.received(clip,state)) throw error;
              accepted = true;
            }
          }
        }
        if (!accepted) {
          this.show("正在上传音频，等待接收确认", "正在提交完整片段，请勿关闭；等待到期后只核查接收状态，不会自动重传。");
          clip.postStarted = true;
          try {
            const began = tick();
            const ack = await this.api(clip.series?"audio-series":"audio",clip.blob,{"Content-Type":clip.mime,"X-Bot-Utterance-ID":clip.id,...seriesHeaders},false,"audio_submit");
            if (ack.audio_received !== true || ack.utterance_id !== clip.id || clip.series && (ack.series_id!==clip.series.id || ack.series_index!==clip.series.index || ack.series_final!==clip.series.final)) throw new RequestFailure(0,"audio","validate","invalid-response","audio_submit");
            metrics.client_upload_post = Math.max(0,Math.round(tick()-began));
          } catch (error) {
            if (!ownsClip()) return false;
            state = await this.api("state",undefined,{},false,"audio_readback");
            if (!this.received(clip,state)) throw error;
          }
        }
      }
      if (!ownsClip()) return false;
      if(seriesDelivery) {
        if(this.clip===clip)this.clip=null;
        if(clip.series.final && this.round?.seriesId===clip.series.id)this.round.uploadAt=this.e.now();
        return true;
      }
      this.round = {pcmDeclined:clip.pcmDeclined,id:clip.id,sha:clip.sha,mime:clip.mime,byte_count:clip.blob.size,endedAt:clip.endedAt,uploadAt:this.e.now(),metrics:clip.metrics}; this.clip = null; confirmedRound = this.round;
      const earlyAudio=clip.pendingFirstAudio;if(earlyAudio){this.noteProgress(earlyAudio);this.acceptPushedAudio(earlyAudio);}
      const early=this.pendingPCM;this.pendingPCM=null;
      if(early?.id===this.round.id)for(const packet of early.frames)this.acceptPCM(packet);
      if (epoch === this.epoch && !this.ending && !this.suspended && !this.muted) {
        this.show("音频已收到，等待转写", "接收确认不代表已经识别或已生成回复。");
        await this.refresh();
      }
      return true;
    } catch (error) {if (ownsClip() || confirmedRound && this.round === confirmedRound && this.reference === reference) this.report(error);return false;}
  }
  canRetain(audio) {
    return this.server?.audio_missing_policy === true && audio &&
      ["failed","uncertain","cancelled"].includes(audio.state) && audio.error_code !== "empty_transcription";
  }
  retained(audio, round) {
    return !!(audio && round && audio.utterance_id === round.id &&
      audio.resolution?.decision === "retained_missing" &&
      Number.isFinite(audio.resolution.decided_at) &&
      (!round.sha || (audio.sha256 === round.sha && audio.mime === round.mime && audio.byte_count === round.byte_count)));
  }
  async retainRound(round) {
    const epoch = this.epoch;
    try {
      try {
        const ack = await this.api("audio-resolution",{utterance_id:round.id});
        if (ack.recorded !== true || !this.retained(ack.audio,round)) throw new RequestFailure();
      } catch (error) {
        if (this.locked) throw error;
        const state = await this.api("state");
        if (!this.retained(state.audio.find(a => a.utterance_id === round.id),round)) throw error;
      }
      // Re-read close/limits before releasing this round. No SDK is reissued.
      await this.refresh(false);
      if (this.round !== round) return;
      if (!this.retained(this.server.audio.find(a => a.utterance_id === round.id),round)) throw new RequestFailure();
      this.round = null;
      if (epoch !== this.epoch || this.ending || this.suspended || this.muted || this.locked) return;
      this.show("本段已保留为未识别片段", "不会重试本段；后续回顾会明确标注缺失。可继续说话。");
    } catch (_) {
      this.show("本段保留决定尚未确认", "可能仍在收尾，请稍后点“保留本段并继续”或核查；原音频不重试，未确认前不会开始新段。");
    }
  }
  async reconcileRound() {
    if(this.barge && this.round?.id===this.barge.id) {
      const row=this.server?.barge_in?.find(r=>r.utterance_id===this.barge.id);
      if(row)await this.settleBarge(row);
      if(this.barge){this.show(row?.decision==="interrupt"?"后台处理中，等待工具安全收尾":"正在确认插话");return;}
    }
    if (this.speech) {
      if (this.speech.pcm) {
        const rows=this.server?.segments?.filter(r=>r.generation_id===this.speech.id);
        if(rows?.some(r=>r.seq===0 && r.state==="stopped" || r.seq===1 && ["uncertain","failed","cancelled"].includes(r.state)))
          this.failSpeech(this.speech,"synthesis-failed");
        return;
      }
      if (this.speech.seq) {void this.prefetchSegment(this.speech.round,this.speech.id,this.speech.seq+1); return;}
      if (!this.speech.local && this.speech.waiting) void this.loadSpeech(this.speech);
      return; // Polling never replaces the currently owned player or sample.
    }
    if(this.round?.playbackFinished) {await this.finishPlaybackRound(this.round);return;}
    const round = this.round;
    if (!round) return;
    const audio = this.server.audio.find(a => a.utterance_id === round.id);
    if (!audio) {this.show("尚未查到本段接收状态，请核查"); return;}
    if (this.retained(audio,round) || (audio.state === "failed" && audio.error_code === "empty_transcription")) {
      if (this.round !== round) return;
      this.round = null;
      this.show("本段未取得可识别文字", "音频已保留，不重试本段；回顾仅基于已识别文字并标注缺失，继续等待声音。");
      if (!this.sendTask) await this.listen(); // send's finalizer resumes if it still owns the ACK.
      return;
    }
    if (["failed","uncertain","cancelled"].includes(audio.state)) {
      if (this.canRetain(audio)) {
        round.retainAvailable = true;
        this.show(this.serviceFailure("stt") || "本段未取得转写结果，音频已保留", "可点“保留本段并继续”，或挂断后整理已识别文字并标注缺失；不会重发本段转写。");
        return;
      }
      this.show(this.serviceFailure("stt") || "本段未取得转写结果，音频已保留", "当前不能继续转写，可挂断保留材料供核查；不会重发上一段转写请求。"); return;
    }
    if (audio.state !== "succeeded") {this.show("正在转写，文字保存后会显示在下方"); return;}
    const reply = this.server.replies.find(r => r.utterance_id === this.round.id);
    if (reply && this.reconcileSegments(round,reply)) return;
    if (!reply || ["not_started","running"].includes(reply.state)) {
      this.show(this.server.background_processing?"后台处理中，正在等待工具安全收尾":
        this.server.tool_processing?"正在调用工具，暂不可插话，请稍等":"文字已保存，正在等待回复"); return;
    }
    if (["failed","empty","cancelled"].includes(reply.state)) {
      this.round.terminal = true;
      this.show(this.serviceFailure("chat") || "本轮未取得回复，文字已保存", "可点继续开始新一轮，或挂断；不会重发上一轮模型请求。"); return;
    }
    if (reply.state !== "succeeded") {this.show("回复结果尚未确认，请核查", "状态未确认前不会开始下一轮；可挂断保留已有材料。"); return;}
    const turn = this.server.turns.find(t => t.sequence === reply.bot_sequence && t.material_id === reply.generation_id && t.role === "assistant");
    if (!turn) {this.show("回复文字尚未核对"); return;}
    if (!this.suspended && !this.ending && !this.seen.has(reply.generation_id)) this.speak(reply.generation_id,turn);
  }
  prefetchSegment(round,id,seq) {
    round.downloads ||= new Map();
    if (round.downloads.has(seq)) return round.downloads.get(seq);
    const row = this.server?.segments?.find(r => r.generation_id===id && r.seq===seq);
    if (row?.state!=="succeeded") return Promise.resolve(null);
    const transport=this.config.bargeIn==="auto"&&this.config.captureMode==="auto"?
      {read:response=>this.readBargeAudio(response,round,id)}:null;
    const task = this.api("segment-audio",{generation_id:id,seq},{},true,"request",null,null,transport).then(blob => {
      if (!blob.size || blob.size>8388608 || !["audio/wav","audio/mpeg"].includes(blob.type)) throw new RequestFailure();
      if(this.closedBargeGenerations?.has(id)) {this.rejectBargeBlock();return null;}
      const b=this.barge;
      if(!transport && b?.round===round && !b.localClosed) {
        if(!this.reserveBargeCache(blob.size)){this.closeBarge("capacity");this.show("暂停缓存已满，旧朗读已结束，继续听取本段");return null;}
      }
      return blob;
    });
    task.catch(() => {}); round.downloads.set(seq,task); return task;
  }
  reconcileSegments(round,reply) {
    const rows = this.server.segments?.filter(r => r.generation_id===reply.generation_id);
    if (!rows?.length) return false;
    const group=rows.find(r=>r.seq===0), seq=(round.segmentSeq||0)+1;
    if (group?.state==="stopped") {void this.finishPlaybackRound(round).catch(error=>this.report(error));return true;}
    const row=rows.find(r=>r.seq===seq);
    if(row?.playback_state==="ended") {round.segmentSeq=seq;return this.reconcileSegments(round,reply);}
    if(row && (row.playback_state==="started" || TERMINAL.has(row.playback_state) ||
       row.playback_state==="unknown" && row.revision===-1)) {
      if(seq===1 && row.playback_state!=="stopped") {
        round.segmentSeq=seq;round.firstAudioClosed=true;
        this.playbackNotice="首句播放结果未确认，保留文字且不重播；后续句继续朗读。";
        return this.reconcileSegments(round,reply);
      }
      round.terminal=true;round.firstAudioClosed=true;
      void this.finishPlaybackRound(round).catch(error=>this.report(error));return true;
    }
    if (!row && group?.state==="complete" && !["running","not_started"].includes(reply.state)) {
      this.round=null; if (!this.ending && !this.suspended && !this.muted) void this.listen(); return true;
    }
    if (["failed","uncertain","cancelled"].includes(row?.state)) {
      void this.finishPlaybackRound(round).catch(error=>this.report(error));return true;
    }
    if (!row || row.state!=="succeeded" || this.suspended || this.ending || this.locked || this.playbackPaused) return true;
    const speech={id:reply.generation_id,seq,round,revision:row.revision,terminal:false,waiting:true};
    this.speech=speech; this.beginSpeech(speech); return true;
  }
  async loadSegment(speech) {
    try {
      const blob=await this.prefetchSegment(speech.round,speech.id,speech.seq);
      if (!blob || speech.terminal || speech.media || this.speech!==speech || this.ending || this.locked || this.round!==speech.round) return;
      if(speech.seq===1) {
        speech.round.firstAudioPath=blob===speech.round.pushedFirstBlob?"push":"download";
        speech.round.downloadAt ??= this.e.now();this.sendMetrics(speech.round);
      }
      speech.echoPath=blob.type==="audio/mpeg"?"mp3":null;
      speech.url=this.e.URL.createObjectURL(blob); this.preparePlayer(speech,speech.url);
      void this.prefetchSegment(speech.round,speech.id,speech.seq+1);
    } catch (_) {if (!speech.terminal && !speech.media && this.speech===speech) this.failSpeech(speech,"network");}
  }
  observe(speech,state) {
    speech.observedState=state;
    const item = {generation_id:speech.id, revision:++speech.revision, state};
    if (speech.seq) item.seq=speech.seq;
    if (!speech.seq && state === "started" && Number.isFinite(speech.round?.endedAt)) {
      const ms = this.e.now()-speech.round.endedAt;
      if (Number.isFinite(ms) && ms >= 0 && ms <= 3600000) item.first_sound_ms = Math.round(ms);
    }
    this.observations.push(item); void this.flushObservations().catch(error => this.report(error));
  }
  flushObservations() {
    if (this.flushTask) return this.flushTask;
    this.flushTask = (async () => {
      while (this.observations.length) {
        const item = this.observations[0];
        try {
          const route=item.seq ? "segment-playback" : "playback";
          const ack = await this.api(route,item);
          const superseded=item.seq && ack.seq===item.seq && ack.superseded===true && TERMINAL.has(ack.state);
          if (ack.recorded !== true || ack.generation_id !== item.generation_id || (item.seq && ack.seq!==item.seq) || !superseded && (ack.revision < item.revision ||
              !(ack.state === item.state || (item.state === "started" && TERMINAL.has(ack.state))))) throw new RequestFailure(409,route,"validate","invalid-response");
        }
        catch (error) {
          if (this.locked) throw error;
          const state = await this.api("state");
          const segment=item.seq && state.segments?.find(t=>t.generation_id===item.generation_id && t.seq===item.seq);
          const turn = item.seq ? segment && {...segment,playback_revision:segment.revision} : state.turns.find(t => t.material_id === item.generation_id && t.role === "assistant");
          const cancelled=item.seq && state.segments?.some(r=>r.generation_id===item.generation_id && r.seq===0 && r.state==="stopped") && TERMINAL.has(turn?.playback_state);
          if (!turn || !cancelled && (turn.playback_revision < item.revision || !(turn.playback_state === item.state || (item.state === "started" && TERMINAL.has(turn.playback_state))))) throw error;
        }
        this.observations.shift();
      }
    })().finally(() => {this.flushTask = null;});
    return this.flushTask;
  }
  speak(id,turn) {
    if (this.speech || this.speechBlocked || this.recording || this.playbackPaused) return;
    this.seen.add(id);
    const speech = {id, round:this.round, revision:turn.playback_revision, text:turn.body, terminal:false, waiting:true};
    this.speech = speech;
    this.beginSpeech(speech);
  }
  beginSpeech(speech) {
    this.stopToolWaitCue();
    if (!this.mediaAvailable()) {this.failSpeech(speech,"api-unavailable"); return;}
    speech.timer = this.e.setTimeout(() => {
      if (this.speech !== speech || speech.terminal) return;
      this.speechDiagnostic = "播放尚未确认结束，不代表听到；可停止或挂断，未确认前不采音。";
      this.render();
    },10000);
    if (speech.local) this.preparePlayer(speech,"/bot-call/fixed-sample.wav");
    else if (speech.seq) void this.loadSegment(speech);
    else {this.show("正在准备回复音频"); void this.loadSpeech(speech);}
  }
  async loadSpeech(speech) {
    if (speech.fetching || speech.terminal || this.speech !== speech) return;
    speech.fetching = true;
    try {
      const result = await this.api("speech",{generation_id:speech.id,wait_ms:8000});
      if (speech.terminal || this.speech !== speech || this.ending || this.locked) return;
      if (result.generation_id !== speech.id) throw new RequestFailure(0,"speech","validate","invalid-response");
      if (["pending","running"].includes(result.state)) return;
      if (result.state !== "succeeded") {this.failSpeech(speech,"synthesis-failed"); return;}
      speech.round.readyAt ??= this.e.now();this.sendMetrics(speech.round);
      const blob = await this.api("speech-audio",{generation_id:speech.id},{},true);
      if (speech.terminal || this.speech !== speech || this.ending || this.locked) return;
      if (!blob.size || blob.size > 8388608 || blob.type !== "audio/wav") throw new RequestFailure(0,"speech-audio","validate","invalid-response");
      speech.round.firstAudioPath="download";speech.round.downloadAt ??= this.e.now();this.sendMetrics(speech.round);
      speech.url = this.e.URL.createObjectURL(blob);
      this.preparePlayer(speech,speech.url);
    } catch (error) {
      if (!speech.terminal && this.speech === speech) {
        this.failSpeech(speech,"network");
        this.report(error);
      }
    } finally {speech.fetching = false;}
  }
  playbackDiagnostics(speech,path) {
    if(speech.local || !speech.seq || speech.playbackDiag)return;
    const c=this.pcmContext||this.audioContext,round=speech.round,now=this.e.now();
    const bound=(v,max)=>Math.max(0,Math.min(max,Math.round(v)));
    const latency=prop=>{try{const v=c?.[prop];return Number.isFinite(v)&&v>=0?v:null;}catch(_){return null;}};
    const d=speech.playbackDiag={changes:0,interrupts:0,jump:0};
    const values={client_playback_sentence:speech.seq,client_playback_path:path,
      client_playback_context:audioState(c)||0,client_playback_devicechanges:this.deviceChanges||0,
      client_playback_context_created_after_input:c && this.playbackContextCreatedAfterInput===1?1:0,
      client_playback_lead_ms:path===1 && this.config.captureMode!=="legacy"?180:0};
    for(const [prop,key] of [["baseLatency","base_us"],["outputLatency","output_us"]]) {
      const v=latency(prop);if(v!==null)values["client_playback_"+key]=bound(v*1000000,2000000);
    }
    try {
      const session=this.e.probeWindow?.navigator?.audioSession;
      values.client_playback_session_type=({auto:1,playback:2,transient:3,"transient-solo":4,"ambient":5,"play-and-record":6})[session?.type]||0;
      values.client_playback_session_state=({inactive:1,active:2,interrupted:3})[session?.state]||0;
    }catch(_){}
    if(path===1)round.pcmPlaybackAt=now;
    if(path===2 && speech.echoPath==="mp3" && !round.mp3PlaybackSeen) {
      round.mp3PlaybackSeen=true;
      if(Number.isFinite(round.pcmPlaybackAt))values.client_playback_pcm_mp3_ms=bound(now-round.pcmPlaybackAt,60000);
    }
    const send=data=>{void this.api("metrics",{generation_id:speech.id,values:{client_playback_sentence:speech.seq,...data}}).catch(()=>{});};
    send(values);
    this.playbackLatencies ||= {};
    const sample=()=>{
      for(const prop of ["baseLatency","outputLatency"]) {
        const v=latency(prop);if(v===null)continue;
        if(Number.isFinite(this.playbackLatencies[prop]))d.jump=Math.max(d.jump,Math.abs(v-this.playbackLatencies[prop])*1000);
        this.playbackLatencies[prop]=v;
      }
    };
    sample();
    let listening=this.bargeMicListening();
    d.poll=()=>{
      if(this.speech!==speech || speech.terminal)return;
      this.maintainBargeInput(speech);sample();
      const active=this.bargeMicListening();
      if(active!==listening) {listening=active;this.render();}
      d.timer=this.e.setTimeout(d.poll,100);
    };
    d.timer=this.e.setTimeout(d.poll,100);
    d.finish=()=>{this.e.clearTimeout(d.timer);sample();send({client_playback_statechanges:d.changes,
      client_playback_interrupts:d.interrupts,client_playback_latency_jump_ms:bound(d.jump,2000),
      client_playback_end_context:audioState(c)||0});};
  }
  preparePlayer(speech, source) {
    try {
      if (speech.terminal || this.speech !== speech) return;
      speech.waiting = false;
      const media = new this.e.Audio(); speech.media = media;
      media.preload = "auto"; media.src = source;
      const output=this.playbackOutput(),context=this.pcmContext||this.audioContext;
      if(output && context?.createMediaElementSource && !speech.local) {
        speech.outputNode=context.createMediaElementSource(media);speech.outputNode.connect(output);
      }
      if(!this.barge)this.outputGain(1);
      media.onplaying = () => {
        if (speech.terminal || this.speech !== speech) return;
        speech.needsGesture = false;speech.playing=true;try{this.playbackDiagnostics(speech,2);}catch(_){}
        try{if(speech.echoPath==="mp3")this.echo?.path(speech,"mp3");}catch(_){this.echo?.stop();}
        this.speechDiagnostic = "播放器报告开始；请自行确认声音。";
        if (!speech.local && !speech.started) {
          speech.started = true;
          if(!speech.seq || speech.seq===1) {speech.round.playAt ??= this.e.now();this.sendMetrics(speech.round);}
          this.observe(speech,"started");
        }
        this.show("正在播放（浏览器观测）");
        this.bargePlayback();
      };
      media.onended = () => {
        if (speech.terminal || this.speech !== speech) return;
        speech.terminal = true; this.releasePlayback(speech);
        if (speech.local) this.finishSample(speech,"固定样本播放结束；请自行确认是否听到，不验证当前TTS配置。");
        else {
          this.playbackNotice = "播放器报告结束，不代表听完或接受。";
          this.observe(speech,"ended"); void this.afterSpeech(speech).catch(error => this.report(error));
        }
      };
      for(const name of ["onwaiting","onstalled","onpause"])media[name]=()=>{const active=speech.playing;speech.playing=false;if(speech.softPaused||this.bargeEnabled()){this.render();return;}if(active){this.echo?.stop(true);this.capture?.close();this.capture=null;if(!this.recording)for(const track of this.stream?.getTracks()||[])track.enabled=false;}this.render();};
      media.onerror = () => this.failSpeech(speech,({2:"network",3:"decode",4:"unsupported"})[media.error?.code] || "unknown");
      this.playMedia(speech);
    } catch (_) {this.failSpeech(speech,"unknown");}
  }
  playMedia(speech) {
    // Both a fixed sample and explicit permission recovery call play in click.
    if (speech.terminal || this.speech !== speech || !speech.media) return;
    try {
      const playing = speech.media.play();
      this.speechDiagnostic = "等待播放器开始；声音是否可听仍需自行确认。"; this.render();
      Promise.resolve(playing).catch(error => {
        if (speech.terminal || this.speech !== speech) return;
        if (error?.name === "NotAllowedError") {
          speech.needsGesture = true;
          if (!speech.local) {this.playbackPaused = true; this.releaseMic();}
          this.speechDiagnostic = "播放需要点击恢复声音（not-allowed）；不会重新合成或重跑模型。";
          this.render();
        } else this.failSpeech(speech,"unknown");
      });
    } catch (_) {this.failSpeech(speech,"unknown");}
  }
  resumeVoice() {
    const speech = this.speech;
    if (!speech?.needsGesture || speech.terminal || this.speechBlocked) return;
    if (!speech.local && (this.ending || this.locked || this.suspended)) return;
    // Permission recovery is the same attempt/Blob, with no HTTP or synthesis.
    if (!speech.local) this.playbackPaused = false;
    this.playMedia(speech);
  }
  releasePlayback(speech) {
    try{if(speech.playbackDiag?.finish){speech.playbackDiag.finish();speech.playbackDiag.finish=null;}}catch(_){}
    try{this.echo?.end(speech);}catch(_){this.echo?.stop();}
    speech.playing=false;
    try{speech.outputNode?.disconnect();speech.outputNode=null;}catch(_){}
    try {
      if(speech.pcm){speech.pcm.stop();speech.pcm=null;}
      if (speech.media) {
        speech.media.pause();
        if (!speech.media.paused && !speech.media.ended) throw new Error("call_page.stop_unconfirmed");
        speech.media.onplaying = speech.media.onended = speech.media.onerror = null;
        speech.media.removeAttribute("src"); speech.media.load(); speech.media = null;
      }
      if (speech.url) {this.e.URL.revokeObjectURL(speech.url); speech.url = null;}
      if (speech.stopUnconfirmed) {speech.stopUnconfirmed = false; this.speechBlocked = false;}
      return true;
    } catch (_) {
      speech.stopUnconfirmed = true;
      this.speechBlocked = true;
      this.speechDiagnostic = "停止播放未确认，已阻止试听和采音；请挂断核查浏览器。";
    }
  }
  async afterSpeech(speech) {
    this.e.clearTimeout(speech.timer);
    if (this.speech === speech && !speech.stopUnconfirmed) this.speech = null;
    const round = speech.round;
    if (speech.seq && this.round===round && (speech.observedState==="ended" || speech.skipUnconfirmed)) {
      round.downloads?.delete(speech.seq); round.segmentSeq=speech.seq;
      // The next prefetched sentence starts before waiting for receipt ACKs.
      await this.reconcileRound();
      void this.flushObservations().catch(error=>this.report(error));
      return;
    }
    if(speech.stopUnconfirmed) {this.show("停止播放未确认，已暂停采音");return;}
    await this.finishPlaybackRound(round);
  }
  async finishPlaybackRound(round) {
    if(!round || this.round!==round || this.speechBlocked)return;
    round.playbackFinished=true;
    try {await this.flushObservations();}catch(error){this.schedulePoll();throw error;}
    if (this.round !== round) return;
    this.round = null;
    if (!this.ending && !this.muted && !this.suspended && !this.locked && !this.speechBlocked) await this.listen();
  }
  async stopSpeaking(resume = true) {
    if(this.barge)this.closeBarge('cancel');
    this.echo?.stop(true);
    this.invalidateHeartbeat(); this.cancelStateRead();
    const speech = this.speech; if (!speech) return;
    // Set terminal BEFORE cancel: synchronous/late end/error cannot overwrite it.
    if(!speech.local && speech.round) speech.round.firstAudioClosed=true;
    const wasTerminal = speech.terminal;
    speech.terminal = true;
    this.e.clearTimeout(speech.timer);
    if (!speech.local && !wasTerminal) {
      this.playbackNotice = "已请求停止朗读，仅为浏览器观测，不代表听完。";
      this.observe(speech,"stopped");
    }
    this.releasePlayback(speech);
    if (speech.stopUnconfirmed) {this.show("停止播放未确认，已保留播放器供再次停止或挂断"); return;}
    if (speech.local) {
      this.finishSample(speech,this.speechBlocked ? this.speechDiagnostic : "已请求停止试听，不代表听完。" ); return;
    }
    if (!resume) {
      this.speech = null; await this.flushObservations();
      if (this.round === speech.round) this.round = null;
    }
    else await this.afterSpeech(speech);
    if (this.speechBlocked) this.show("停止朗读未确认，已暂停采音");
  }
  releaseAnalysis() {
    for(const key of ["analysisSource","analyser","analysisGain"]) {
      try {this[key]?.disconnect();} catch (_) { /* Cleanup continues through closed graphs. */ }
      this[key]=null;
    }
    const context=this.audioContext;this.audioContext=null;
    if(context && context!==this.pcmContext)void context.close().catch(()=>{});
  }
  releaseMic() {
    if(this.barge)this.closeBarge('cancel');
    this.echo?.stop(true);
    this.capture?.close();this.capture=null;
    // Controls stop capture immediately, but keep the analysis graph until
    // the recorder's final edge and VAD flush have settled (bounded above).
    const r=this.recording;
    if(r?.recorder?.samples!==undefined)for(const track of r.stream.getTracks())track.enabled=false;
    if(r?.echoHold)for(const track of r.stream.getTracks())track.enabled=false;
    if(r?.stopping && !r.clip && !r.error) {r.releaseAfterStop=true;return;}
    if (this.detector?.close) void this.detector.close(true);
    stopTracks(this.stream); this.stream = null;
    this.releaseAnalysis();
  }
  async toggleMute() {
    if(this.barge)this.closeBarge('cancel');
    this.invalidateHeartbeat(); this.cancelStateRead();
    this.muted = !this.muted; this.stopToolWaitCue(); this.epoch++; this.syncPCMCapability();
    if (!this.started) {this.show("请先点击开始，再授权麦克风"); return;}
    if (this.muted) {
      const stopped = this.finishSegment("control"); this.releaseMic(); await stopped;
      if (this.clip) await this.sendClip(); this.show("麦克风已静音（朗读独立控制）");
    } else await this.start();
    // Either toggle revokes capture/state readers, not reply audio. In-flight
    // playback can make start() return early, so both edges need a fresh owner.
    if (this.round) this.recoverPushState();
    this.render();
  }
  hidden() {
    if(this.hiddenAt===undefined)this.hiddenAt=this.e.now();
    if(this.round)this.round.visibilityObservedAt=this.e.now();
    if(!this.suspended)this.backgroundSuspended=true;
    this.captureResumeRequired=true;
    return this.suspend();
  }
  foreground() {
    if(this.e.isHidden?.() || !this.started || !this.reference || this.locked || this.ending)return Promise.resolve();
    const now=this.e.now();
    if(this.hiddenAt!==undefined) {
      this.hiddenSpans ||= [];this.hiddenSpans.push([this.hiddenAt,now]);
      // Bound memory independently from the number of focus events.
      if(this.hiddenSpans.length>128)this.hiddenSpans.shift();
      this.hiddenAt=undefined;this.foregroundAt=now;
      if(this.round){delete this.round.foregroundResultMs;this.round.visibilityObservedAt=now;}
    }
    if(this.foregroundTask&&!this.backgroundSuspended)return this.foregroundTask;
    if(!this.backgroundSuspended && this.lastForeground!==undefined && now-this.lastForeground<250)return Promise.resolve();
    this.lastForeground=now;this.stateReadFailures=0;
    if(this.backgroundSuspended){this.backgroundSuspended=false;this.suspended=false;this.epoch++;}
    if(this.suspended)return Promise.resolve();
    this.cancelStateRead(true);this.stopPush();this.pushRecovering=true;this.ensurePush();
    const task=this.refresh().catch(error=>{if(!CANCELLED_READS.has(error))this.report(error);})
      .finally(()=>{if(this.foregroundTask!==task)return;this.foregroundTask=null;if(this.poll===null)this.schedulePoll();});
    this.foregroundTask=task;return task;
  }
  async suspend() {
    if(this.barge)this.closeBarge('cancel');
    this.invalidateHeartbeat(); this.cancelStateRead();
    this.suspended = true; this.stopToolWaitCue(); this.closePCM(); this.epoch++; this.stopHeartbeat(); this.e.clearTimeout(this.poll);
    const stopped = this.finishSegment("control"); this.releaseMic();
    try {await this.stopSpeaking(false); await stopped; if (this.clip) await this.sendClip();}
    catch (_) { /* Explicit fixed notice below; never log a browser exception. */ }
    if(this.suspended)this.show("后台已暂停，回到前台后点继续", "正在尽力提交；手机关闭事件不可靠，不能保证最后片段送达。");
  }
  failedBarge(clip = this.clip || this.series?.queue[0]) {
    const b=clip?.barge || this.barge || this.lastBarge;
    return b?.registerFailed?b:null;
  }
  recoverBarge(clip = this.clip || this.series?.queue[0]) {
    const b=this.failedBarge(clip);
    if(!b)return;
    b.recovery ??= this.recoverBargeOnce(b,clip).finally(()=>{b.recovery=null;});
    return b.recovery;
  }
  async recoverBargeOnce(b,clip) {
    // The original begin has settled locally. Use the server's candidate and
    // playback state before either issuing a control or releasing the upload fence.
    const read=()=>this.api("state");
    const candidate=state=>state.barge_in?.find(row=>row.utterance_id===b.id);
    const canBegin=state=>{
      const parts=state.segments?.filter(row=>row.generation_id===b.speech.id) || [];
      return !state.close_requested && !state.ended &&
        parts.some(row=>row.seq===0 && ["open","complete"].includes(row.state)) &&
        parts.some(row=>row.seq===b.speech.seq && !["ended","stopped","failed"].includes(row.playback_state));
    };
    let state=await read(),row=candidate(state);
    if(!row && clip?.barge===b && !b.cancelled && canBegin(state)) {
      try {row=await this.bargeRequest(b,"begin");}
      catch(error) {
        if(failureData(error).status!==409)throw error;
        state=await read();row=candidate(state);
        if(!row && !b.cancelled && canBegin(state))throw error;
      }
    }
    if(row?.decision==="pending" && !row.closed_reason && !b.cancelled && clip?.barge===b)
      row=await this.bargeRequest(b,"timeout");
    if(row?.decision==="pending" && (b.cancelled || clip?.barge!==b))
      await this.bargeRequest(b,"cancel");

    if(!row) {
      if(clip?.barge===b)clip.barge=null;
      for(const queued of this.series?.queue || [])if(queued.barge===b)queued.barge=null;
    }
    if(this.barge===b && (!row || clip?.barge!==b))this.barge=null;
    b.registerFailed=false;b.register=Promise.resolve();b.control=null;
    if(this.apiFailure?.route==="barge-in"){this.apiFailure=null;this.apiDiagnostic="";}
  }
  async retry() {
    if(this.failedBarge()) {
      if(this.ending)return this.hangup();
      try {
        await this.recording?.done;
        await this.recoverBarge();
        if(!this.e.isHidden?.() && !this.suspended)this.captureResumeRequired=false;
      } catch(error) {this.report(error);return;}
    }
    if(this.series) {
      if(this.series.abandonIntent)return this.abandonSeries(this.series.abandonIntent);
      this.apiFailure=null;this.apiDiagnostic="";this.series.failed=null;
      if(this.series.sealIntent && !this.series.finalSubmitted)return this.sealSeries(this.series.sealIntent.endedAt);
      if(!await this.flushSeries())return;
      if(this.ending)return this.hangup();
      await this.refresh();if(!this.series?.failed)await this.listen();return;
    }
    this.invalidateHeartbeat(); this.cancelStateRead();
    if (!this.started) {this.show("请先点击开始，再授权麦克风"); return;}
    if (this.locked) {this.report(); return;}
    const round = this.round;
    try {
      await this.connect(); await this.flushObservations();
      if (round?.playbackFinished && this.round === round) this.round = null;
      if (this.clip && !(await this.sendClip())) return;
      if (this.ending) return this.hangup();
      await this.refresh();
      if (!this.round && !this.clip) await this.listen();
    } catch (error) {this.report(error);}
  }
  hangup() {
    if (this.endTask) return this.endTask;
    this.invalidateHeartbeat(); this.cancelStateRead();
    this.endTask = this.hangupOnce().catch(error => this.report(error)).finally(() => {this.endTask = null; this.render();});
    return this.endTask;
  }
  async hangupOnce() {
    if(!this.ending){this.ending=true;this.epoch++;}
    this.stopHeartbeat(); this.e.clearTimeout(this.poll);
    this.show("正在收尾最后片段");
    const stopped = this.finishSegment("control"); this.releaseMic();
    try {await this.stopSpeaking(false);} catch (_) {this.notice = "播放观测尚未确认；不会伪造听完。";}
    this.closePCM();
    await stopped;
    if(this.detector && this.modelPreparation)void this.detector.close();
    const b=this.clip?.barge || this.series?.queue[0]?.barge || this.lastBarge;
    // A hangup can overtake the original begin while the last recording stops.
    // Let that request settle so this same hangup can recover a lost ACK.
    await b?.register?.catch(()=>{});
    await this.recoverBarge();
    if(this.series) {
      this.series.finishing=true;
      if(this.clip && !this.clip.seriesQueued && !this.dropSilentClip(this.clip)) {if(this.clipRejection(this.clip))return;this.enqueueSeriesClip(this.clip,false);}
      if(!await this.flushSeries())return;
    } else if (this.clip && !(await this.sendClip())) return;
    // No getUserMedia wait here: a late permission stream is stopped by epoch.
    if (!this.reference) {
      if (this.invitation) await this.connect();
      else {this.show("尚未进入通话，麦克风授权已取消"); return;}
    }
    try {await this.api("end",{});}
    catch (error) {
      if (this.locked) throw error;
      const state = await this.api("state");
      if (!state.close_requested && !state.ended) throw error;
    }
    await this.refresh();
    // No further capture/reply can start after end; keep the one report even
    // when the user hangs up before returning to listening.
    try {this.capability?.finish();this.capability?.run();}catch(_){}
  }
}

// Glow motion. Size and brightness are only ever moved toward a target by exponential smoothing, so a jump in
// the microphone level, a change of phase or the end of the call glides instead of snapping.
const MOTION = {attackMs:90, releaseMs:320, glideMs:170, settle:0.002};
export function glowTarget(phase, voice, t) {
  const wave = period => (1 - Math.cos(2 * Math.PI * t / period)) / 2;
  if (phase === "ready" || phase === "listening") return {s:1 + .5 * voice, o:.78 + .22 * voice};
  if (phase === "thinking") {const w = wave(4400); return {s:.9 + .2 * w, o:.6 + .4 * w};}
  if (phase === "speaking") {const w = wave(2200); return {s:1 + .16 * w, o:.82 + .18 * w};}
  if (phase === "paused") return {s:.94, o:.4};
  if (phase === "error") return {s:.86, o:.2};
  if (phase === "ended") return {s:.88, o:.14};
  return {s:1, o:.75};
}
const glide = (current, target, dt, tau) => current + (target - current) * (1 - Math.exp(-dt / tau));
export class GlowMotion {
  constructor(orb, bars, win) {
    this.orb = orb; this.bars = bars; this.win = win;
    this.phase = "preparing"; this.level = 0; this.voice = 0; this.s = 1; this.o = .75; this.last = null; this.frame = 0; this.written = {};
  }
  reduced() {return !!this.win.matchMedia?.("(prefers-reduced-motion: reduce)")?.matches;}
  update(phase, level) {
    this.phase = phase; this.level = level;
    if (this.reduced() || typeof this.win.requestAnimationFrame !== "function") {
      // No continuous animation: show the state's size/brightness directly, without the scale from volume.
      const target = glowTarget(phase, level, 0);
      this.voice = level; this.s = this.reduced() ? 1 : target.s; this.o = target.o; this.write(); return;
    }
    if (!this.frame) this.frame = this.win.requestAnimationFrame(now => this.step(now));
  }
  step(now) {
    this.frame = 0;
    if (this.reduced()) {this.last = null; this.update(this.phase, this.level); return;}
    const dt = this.last === null ? 16 : Math.max(1, Math.min(100, now - this.last)); this.last = now;
    this.voice = glide(this.voice, this.level, dt, this.level > this.voice ? MOTION.attackMs : MOTION.releaseMs);
    const target = glowTarget(this.phase, this.voice, now);
    this.s = glide(this.s, target.s, dt, MOTION.glideMs); this.o = glide(this.o, target.o, dt, MOTION.glideMs);
    this.write();
    const still = !["thinking","speaking"].includes(this.phase) && Math.abs(this.voice - this.level) < MOTION.settle &&
      Math.abs(this.s - target.s) < MOTION.settle && Math.abs(this.o - target.o) < MOTION.settle;
    if (still) this.last = null;
    else this.frame = this.win.requestAnimationFrame(next => this.step(next));
  }
  put(node, name, value) {
    const text = value.toFixed(3), key = name + (node === this.bars ? "b" : "o");
    if (this.written[key] !== text) {this.written[key] = text; node.style.setProperty(name, text);}
  }
  write() {this.put(this.orb, "--s", this.s); this.put(this.orb, "--o", this.o); this.put(this.bars, "--voice", this.voice);}
}

Object.assign(CallPage.prototype,BargeMethods);

const RECOVERY_LABELS = {connect:"点击重试接通",discard:"这段没录好，点击继续对话", continue:"出了一点问题，点击继续对话", retry:"刚才那句可能没发出去，点击重试",
  voice:"声音被浏览器拦住了，点击恢复声音", stop:"停止还没确认，点击再试"};
export function mountCallPage(win, doc, options = {}) {
  const nodes = Object.fromEntries(["status","notice","speech-diagnostic","test-voice","resume-voice","start","mute","retry","end","bot-name","call-timer","call-state","call-hint","voice-level","conversation","captions","jump-latest","glow-orb","bars","main-action","main-label","subtitles","details","details-open","details-close","detail-stop","call-shell"].map(id => [id,doc.getElementById(id)]));
  let detailHint="", displayedTurns=null, displayedName=null, rows=[], shownPhase=null, textOn=true, textShown=null, following=true, mainKind="idle", previousFocus;
  try {textOn=win.localStorage.getItem("bot-call-captions")!=="0";} catch (_) { /* Display preference only. */ }
  const atEnd=()=>!(nodes.conversation.scrollHeight-nodes.conversation.scrollTop-nodes.conversation.clientHeight>48);
  const toEnd=()=>{nodes.conversation.scrollTop=nodes.conversation.scrollHeight;following=true;nodes["jump-latest"].hidden=true;};
  const motion = new GlowMotion(nodes["glow-orb"], nodes.bars, win);
  const page = new CallPage({probeWindow:win, createVAD:options.createVAD, fetch:win.fetch.bind(win), mediaDevices:win.navigator.mediaDevices,
    MediaRecorder:win.MediaRecorder, AudioContext:win.AudioContext || win.webkitAudioContext,
    Audio:win.Audio, URL:win.URL, WebSocket:win.WebSocket, atob:win.atob?.bind(win),
    crypto:win.crypto, Blob:win.Blob, AbortController:win.AbortController,
    isHidden:() => doc.hidden === true,
    now:() => win.performance.now(), setTimeout:win.setTimeout.bind(win), clearTimeout:win.clearTimeout.bind(win),
    storage:win.sessionStorage, location:win.location, history:win.history}, state => {
      const startLabel=state.entered?"继续通话":"开始通话";
      const ui=state.ui, phase=ui.phase, sample=ui.label==="试听中";
      if(nodes["call-state"]) {
        const labels={ready:"可以说话",listening:"正在听…",thinking:"想一想…",speaking:sample?"正在试听声音":"正在说，点击可打断",error:"暂时没接上",preparing:state.started?"正在准备…":"点开始，聊聊吧"};
        nodes["call-state"].textContent=ui.toolWaiting?(ui.toolClosing?"工具正在安全收尾，暂不可插话":"工具处理中，暂不可插话"):
          ui.silentNotice?ui.label:labels[phase] || ui.label;
        nodes["call-shell"].setAttribute("data-mic",state.mic);
        nodes["call-shell"].setAttribute("data-tool-wait",String(Boolean(ui.toolWaiting)));
        nodes["bot-name"].textContent=state.botName;
        nodes["call-timer"].textContent=String(Math.floor(state.elapsed/60)).padStart(2,"0")+":"+String(state.elapsed%60).padStart(2,"0");
        if(shownPhase!==phase) {nodes["call-shell"].setAttribute("data-phase",phase);shownPhase=phase;}
        motion.update(phase,Number.isFinite(state.level)?Math.max(0,Math.min(1,state.level)):0);
        nodes["voice-level"].hidden=!state.recording;nodes["voice-level"].value=state.level;
        nodes.subtitles.setAttribute("aria-pressed",String(textOn));
        nodes.subtitles.setAttribute("aria-label",textOn?"隐藏文字":"显示文字");
        nodes.conversation.hidden=!textOn;
        let changed=false;
        if(displayedTurns!==state.turns || displayedName!==state.botName) {
          // Keep unchanged rows so a poll with identical text never rebuilds the DOM or moves the scroll position.
          const next=state.turns.map((turn,index) => {
            const old=rows[index];
            if(old && old.role===turn.role && old.body===turn.body && old.name===state.botName)return old;
            const row=doc.createElement("li"),label=doc.createElement("span"),body=doc.createElement("span");
            row.setAttribute("data-role",turn.role);row.setAttribute("data-speaking","false");label.setAttribute("class","sr-only");
            label.textContent=turn.role==="user"?"你：":state.botName+"：";body.textContent=turn.body;row.replaceChildren(label,body);
            return {role:turn.role,body:turn.body,name:state.botName,row,speaking:false};
          });
          changed=next.length!==rows.length || next.some((item,index) => item!==rows[index]);
          rows=next;displayedTurns=state.turns;displayedName=state.botName;
          if(changed)nodes.captions.replaceChildren(...rows.map(item => item.row));
        }
        let speakingRow=null;
        if(phase==="speaking" && !sample)for(let index=rows.length-1;index>=0;index--)if(rows[index].role==="assistant") {speakingRow=rows[index];break;}
        for(const item of rows) {
          const on=item===speakingRow;
          if(item.speaking!==on) {item.speaking=on;item.row.setAttribute("data-speaking",String(on));}
        }
        if(!textOn)nodes["jump-latest"].hidden=true;
        else if(textShown!==true)toEnd();
        else if(changed) {if(following)toEnd();else nodes["jump-latest"].hidden=false;}
        textShown=textOn;
        // One context button: send or interrupt while the call runs, and the plain-language recovery prompt
        // whenever something needs the user. Only the very first entry keeps a separate start button.
        let label="准备中…",kind="idle",disabled=true;
        if(ui.action) {
          kind="recover";label=state.ending && ui.action==="discard"?"这段没录好，点击丢弃并挂断":state.ending && ui.action==="retry"?(state.clipPending?"最后一段还没确认，点击核查":"结束还没确认，点击核查"):
            ui.action==="continue" && state.retainLabel?"这段没听清，点击保留本段并继续":RECOVERY_LABELS[ui.action];
          disabled=state.locked || state.ending && ui.action!=="retry" && ui.action!=="stop" && ui.action!=="discard";
        } else if(ui.toolWaiting)label=ui.label;
        else if(state.speaking) {kind="stop";label=sample?"点击停止试听":"点击打断";disabled=false;}
        else if(state.recording && phase==="listening") {kind="send";label="点击发送";disabled=false;}
        else if(state.recording && phase==="ready") {kind="send";label=ui.hint.includes("暂不可用")?"点击发送":"请说话";disabled=false;}
        else if(ui.exchangeEnded)label=ui.label;
        else if(state.locked)label="通话已失效，请回原聊天重新发起";
        else if(phase==="ended")label=state.closed?"通话已结束":"正在结束通话…";
        else if(state.suspended) {kind="recover";label="通话已暂停，点击继续";disabled=state.ending;}
        else if(phase==="paused")label=state.muted?"已静音":"已暂停";
        else if(phase==="thinking")label="想一想…";
        else if(state.started && state.continueNeeded) {kind="recover";label="点击继续通话";disabled=false;}
        const entered=state.started || state.speaking || state.recording;
        nodes.start.hidden=entered;
        nodes["main-action"].hidden=!entered;
        mainKind=kind;
        nodes["main-action"].setAttribute("data-kind",kind);
        nodes["main-action"].disabled=disabled;
        nodes["main-label"].textContent=label;
        // The long explanation moves into Details whenever the prompt is a recovery or an error.
        const thinkingHint=phase==="thinking" && kind==="idle" && !ui.action && !ui.toolWaiting;
        const hintNeeded=ui.toolWaiting || thinkingHint ||
          ui.action==="connect" || kind!=="recover" && !ui.action && (["paused","ended","preparing"].includes(phase) || ui.hint.includes("暂不可用"));
        nodes["call-hint"].textContent=thinkingHint?"正在思考，暂未收音，请稍等。":hintNeeded?ui.hint:"";
        nodes["call-hint"].hidden=!hintNeeded;
        detailHint=phase==="error" || kind==="recover"?ui.hint:"";
      }
      nodes.status.textContent = state.message; nodes.notice.textContent = [state.notice,detailHint,state.details].filter(Boolean).join("\n");
      nodes["speech-diagnostic"].textContent = state.speechDiagnostic;
      nodes["test-voice"].disabled = state.testDisabled;
      nodes["resume-voice"].disabled = !state.needsGesture;
      nodes.mute.disabled = state.closed;nodes.end.disabled = state.closed;
      nodes.mute.setAttribute("aria-pressed",String(state.muted));
      nodes.mute.setAttribute("aria-label",state.muted?"取消麦克风静音":"静音麦克风");
      nodes.start.disabled = state.ending || state.locked;
      nodes.start.textContent = startLabel;
      nodes["detail-stop"].disabled=!state.speaking;
    },options);
  const closeDetails=()=>{
    nodes.details.open=false;nodes["details-open"].setAttribute("aria-expanded","false");
    nodes["call-shell"].inert=false;nodes["call-shell"].setAttribute("aria-hidden","false");
    (previousFocus || nodes["details-open"]).focus?.();
  };
  nodes["details-open"].addEventListener("click",()=>{
    previousFocus=nodes["details-open"];nodes.details.open=true;
    nodes["details-open"].setAttribute("aria-expanded","true");
    nodes["call-shell"].inert=true;nodes["call-shell"].setAttribute("aria-hidden","true");
    nodes["details-close"].focus?.();
  });
  nodes["details-close"].addEventListener("click",closeDetails);
  nodes.details.addEventListener("click",event=>{if(event?.target===nodes.details)closeDetails();});
  doc.addEventListener("keydown",event=>{
    if(!nodes.details.open)return;
    if(event.key==="Escape"){event.preventDefault();closeDetails();return;}
    if(event.key!=="Tab")return;
    const focusable=[...(nodes.details.querySelectorAll?.("button:not(:disabled), summary, a[href], [tabindex='0']") || [])].filter(node=>node.tabIndex>=0 && node.getClientRects().length);
    const first=focusable[0],last=focusable.at(-1);
    if(event.shiftKey && doc.activeElement===first){event.preventDefault();last?.focus();}
    else if(!event.shiftKey && doc.activeElement===last){event.preventDefault();first?.focus();}
  });
  nodes.subtitles.addEventListener("click",()=>{
    textOn=!textOn;
    try {win.localStorage.setItem("bot-call-captions",textOn?"1":"0");} catch (_) { /* Unavailable storage does not block the control. */ }
    page.render();
  });
  nodes.conversation.addEventListener("scroll",()=>{following=atEnd();if(following)nodes["jump-latest"].hidden=true;});
  nodes["jump-latest"].addEventListener("click",toEnd);
  nodes["main-action"].addEventListener("click",()=>{
    // The label tracks the latest render; the page methods re-check the live state themselves.
    const recover=()=>({connect:()=>page.start(),discard:()=>page.discardClip(),voice:()=>page.resumeVoice(),retry:()=>page.retry(),continue:()=>page.start(),stop:()=>page.stopSpeaking()})[page.presentation().action || "continue"];
    const action=mainKind==="stop"?()=>page.stopSpeaking():mainKind==="send" && page.recording?()=>page.submitSegment():mainKind==="recover"?recover():null;
    // Voice recovery has to be invoked synchronously inside the user gesture, as in Details.
    if(action)void Promise.resolve(action()).catch(error=>page.report(error));
  });
  // Synchronous listener: do not await initialization, connection or permission.
  nodes["test-voice"].addEventListener("click",() => page.testVoice());
  nodes["resume-voice"].addEventListener("click",() => page.resumeVoice());
  for (const [id, action] of [["start",() => page.start()], ["mute",() => page.toggleMute()],
      ["detail-stop",() => page.stopSpeaking()], ["retry",() => page.retry()], ["end",() => page.hangup()]]) {
    nodes[id].addEventListener("click",() => {void action().catch(error => page.report(error));});
  }
  doc.addEventListener("visibilitychange",() => {void (doc.hidden?page.hidden():page.foreground()).catch(error=>page.report(error));});
  for(const name of ["pageshow","focus"])win.addEventListener(name,()=>{void page.foreground().catch(error=>page.report(error));});
  win.addEventListener("pagehide",() => {try{page.capability?.finish();}catch(_){}void page.hidden().catch(error => page.report(error));});
  return page;
}
