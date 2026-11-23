// Exactly two user-flow scenarios. Real page module + fake browser + real ASGI IPC.
import assert from "node:assert/strict";
import {webcrypto,createHash} from "node:crypto";
import vm from "node:vm";
const {mountCallPage,nativeFrame} = globalThis.pageModule;
const fixture = globalThis.fixture;
// Startup timeouts have an independent, bounded retry budget; genuine
// incompatibility and the existing mux/queue policy still disable capture.
{
  const proto=globalThis.pageModule.CallPage.prototype;
  const p=Object.create(proto);
  for(let retry=0;retry<=3;retry++) {
    p.captureStartupRetries=retry;
    p.captureSetupFailure(8,0);
    assert.equal(p.captureDisabled,retry===3);
    assert.equal(p.captureStartupPending,true);
    assert.equal(p.captureStartupDeferred,true);
    assert.equal(p.captureMuxFailures,undefined);
  }
  // A separate idle-input rebuild must not spend a fourth startup retry.
  const mixed=Object.assign(Object.create(proto),{config:{captureMode:"auto"},captureStartupPending:true,captureStartupRetries:3});
  await mixed.prepareCapture(false,0);
  assert.equal(mixed.captureDisabled,true);
  assert.equal(mixed.captureStartupRetries,3);
  for(const code of [1,2,3,4]) {
    const fatal=Object.create(proto);fatal.captureSetupFailure(code,0);
    assert.equal(fatal.captureDisabled,true);
    assert.equal(fatal.captureStartupPending,undefined);
  }
  for(const code of [5,6]) {
    const mux=Object.create(proto);mux.captureSetupFailure(code,0);
    assert.equal(mux.captureDisabled,false);mux.captureSetupFailure(code,1);
    assert.equal(mux.captureDisabled,true);
  }
}
let cookie = "";
const requests = [], responses = [];
let failBeforeAudio = 0, loseAudioAck = 0, losePlaybackAck = 0;
async function fetchASGI(url, init = {}) {
  const headers = {host:"calls.example.test", ...Object.fromEntries(Object.entries(init.headers || {}).map(([k,v]) => [k.toLowerCase(),v]))};
  if (cookie) headers.cookie = cookie;
  if (init.method === "POST") headers.origin = "https://calls.example.test";
  const bytes = init.body instanceof Blob ? Buffer.from(await init.body.arrayBuffer()) : Buffer.from(init.body || "");
  const request = {path:url, method:init.method || "GET", headers, body:bytes.toString("base64")};
  requests.push(request);
  if (url.endsWith("/audio") && failBeforeAudio > 0) {failBeforeAudio--; throw new Error("synthetic transport unavailable");}
  const result = await globalThis.requestASGI(request);
  responses.push(request);
  const setCookie = result.headers.find(([key]) => key === "set-cookie");
  if (setCookie) cookie = setCookie[1].split(";")[0];
  if (url.endsWith("/audio") && loseAudioAck > 0) {loseAudioAck--; throw new Error("synthetic ACK lost");}
  if (url.endsWith("/playback") && losePlaybackAck > 0) {losePlaybackAck--; throw new Error("synthetic playback ACK lost");}
  return {ok:result.status < 400, status:result.status,
    json:async () => JSON.parse(Buffer.from(result.body,"base64").toString()),
    text:async () => Buffer.from(result.body,"base64").toString(),
    blob:async () => new Blob([Buffer.from(result.body,"base64")],{type:result.headers.find(([k])=>k==="content-type")?.[1]})};
}
let clicking = false;
class Element {
  constructor() {this.listeners={}; this.attributes={}; this.style={vars:{},setProperty(name,value){this.vars[name]=value;}}; this.textContent=""; this.children=[]; this.disabled=false;this.open=false;this.hidden=false;}
  addEventListener(name,fn) {this.listeners[name]=fn;}
  setAttribute(key,value) {this.attributes[key]=value;}
  replaceChildren(...children) {this.children=children;}
  set innerHTML(_) {throw new Error("HTML injection sink used");}
  click() {if (!this.disabled) {clicking=true;try {this.listeners.click?.();} finally {clicking=false;}}}
}
class Clock {
  constructor() {this.time=0;this.next=1;this.tasks=new Map();}
  setTimeout(fn,ms) {const id=this.next++;this.tasks.set(id,{fn,at:this.time+ms});return id;}
  clearTimeout(id) {this.tasks.delete(id);}
  advance(ms) {
    this.time+=ms;
    for (const [id,item] of [...this.tasks]) if (item.at<=this.time && this.tasks.delete(id)) item.fn();
  }
}
function browser(token, {storage=new Map(), pendingPermission=false, maxAudioBytes, audioAvailable=true, missingMedia=false, noAnalysis=false,createVAD,audioRequestMs,rejectBitrate=false,captionStorage=new Map(),brokenCaptionStorage=false,chunk=false} = {}) {
  const clock=new Clock(), recorders=[], streams=[], utterances=[], replaced=[];
  const nodes=Object.fromEntries(["status","notice","speech-diagnostic","test-voice","resume-voice","start","mute","retry","end","bot-name","call-timer","call-state","call-hint","voice-level","captions","conversation","jump-latest","main-action","main-label","details","subtitles","details-open","details-close","detail-stop","call-shell","glow-orb","bars"].map(id => [id,new Element()]));
  const doc={hidden:false, listeners:{}, getElementById:id=>nodes[id], createElement:()=>new Element(),
    addEventListener(name,fn){this.listeners[name]=fn;}};
  let permissionResolve, permissionCalls=0, rms=0.1, audioClockStalled=false;const captureRequests=[];
  function stream() {const listeners=new Map();const track={enabled:true,muted:false,readyState:"live",stopped:false,
      addEventListener(name,fn){if(!listeners.has(name))listeners.set(name,new Set());listeners.get(name).add(fn);},
      removeEventListener(name,fn){listeners.get(name)?.delete(fn);},
      getSettings(){return {echoCancellation:true,noiseSuppression:false,autoGainControl:true};},
      stop(){this.stopped=true;this.enabled=false;this.readyState="ended";for(const fn of listeners.get("ended")||[])fn();}};
    const value={getTracks:()=>[track]};streams.push(value);return value;}
  class Recorder {
    static isTypeSupported(type) {return type==="audio/webm;codecs=opus";}
    constructor(s,options) {if(rejectBitrate && options.audioBitsPerSecond)throw new Error("unsupported bitrate");this.options=options;this.stream=s;this.mimeType=options.mimeType;this.state="inactive";recorders.push(this);}
    start(timeslice) {this.timeslice=timeslice;this.state="recording";}
    data(text) {this.ondataavailable?.({data:new Blob([text],{type:this.mimeType})});}
    stop() {this.state="inactive";queueMicrotask(()=>{this.beforeStop?.();this.data("-tail🙂");this.onstop?.();});}
  }
  class AudioContext {
    constructor(){this.state="running";this.sampleRate=48000;this.destination={};this.graph=[];this.pulled=false;}
    get currentTime(){return audioClockStalled || !this.pulled?0:clock.time/1000;}
    node(extra={}){const context=this,node={...extra,connections:[],connect(to){this.connections.push(to);if(to===context.destination)context.pulled=true;},disconnect(){this.disconnected=true;this.connections=[];}};this.graph.push(node);return node;}
    createGain(){return this.node({gain:{value:1}});}
    createAnalyser(){const context=this;return this.node({fftSize:1024,getFloatTimeDomainData(values){values.fill(context.pulled?rms:0);}});}
    createMediaStreamSource(){return this.node();}
    async close(){this.state="closed";}
  }
  let rejectNextPlay=false;const urls=new Set();
  class Media {
    constructor(){this.paused=true;this.ended=false;}
    play(){this.clicked=clicking;if(!utterances.includes(this))utterances.push(this);
      this.plays=(this.plays||0)+1;
      if(rejectNextPlay){rejectNextPlay=false;return Promise.reject({name:"NotAllowedError"});}
      this.paused=false;return Promise.resolve();}
    pause(){this.paused=true;this.onended?.();}
    removeAttribute(name){if(name==="src")this.src="";}
    load(){}
  }
  let resolutionOffline=false,resolutionReadbackFailure=false,resolutionGate=null;
  let audioCapability=audioAvailable, playbackOffline=false, failReceiptCheck=false, playbackGate=null, mediaGate=null, speechGate=null;
  const win={fetch:async(url,init)=>{
      if(url.endsWith("/audio-resolution") && resolutionOffline) {resolutionReadbackFailure=true;throw new Error("synthetic resolution not sent");}
      if(url.endsWith("/state") && resolutionReadbackFailure) {resolutionReadbackFailure=false;throw new Error("synthetic resolution readback unavailable");}
      if (url.endsWith("/playback")) {
        if (playbackGate) {const gate=playbackGate;playbackGate=null;gate.entered=true;await gate.promise;}
        if (playbackOffline) {failReceiptCheck=true;throw new Error("synthetic playback not sent");}
      }
      if (url.endsWith("/state") && failReceiptCheck) {failReceiptCheck=false;throw new Error("synthetic receipt check unavailable");}
      const response=await fetchASGI(url,init);
      if(url.endsWith("/audio-resolution") && resolutionGate) {
        const gate=resolutionGate;resolutionGate=null;gate.entered=true;await gate.promise;
        if(gate.loseAck)throw new Error("synthetic resolution ACK lost");
      }
      if (url.endsWith("/speech") && speechGate) {
        const gate=speechGate;speechGate=null;gate.entered=true;await gate.promise;
      }
      if (url.endsWith("/speech-audio") && mediaGate) {
        const gate=mediaGate;mediaGate=null;gate.entered=true;await gate.promise;
      }
      if (url.endsWith("/state") && response.ok && !audioCapability) {
        const state=await response.json();
        return {...response,json:async()=>({...state,audio_available:false})};
      }
      return response;
    }, navigator:{mediaDevices:{getSupportedConstraints(){return {echoCancellation:true,noiseSuppression:true,autoGainControl:true,channelCount:true};},getUserMedia(options){
      captureRequests.push(options);permissionCalls++;
      return pendingPermission ? new Promise(resolve=>{permissionResolve=()=>resolve(stream());}) : Promise.resolve(stream());
    }}}, MediaRecorder:Recorder, AudioContext:noAnalysis?undefined:AudioContext,
    crypto:webcrypto, Blob, atob:globalThis.atob, AbortController, performance:{now:()=>clock.time},
    setTimeout:clock.setTimeout.bind(clock), clearTimeout:clock.clearTimeout.bind(clock),
    sessionStorage:{getItem:key=>storage.get(key)||null,setItem:(key,value)=>storage.set(key,value)},
    localStorage:{getItem:key=>{if(brokenCaptionStorage)throw new Error('storage blocked');return captionStorage.get(key)||null;},setItem:(key,value)=>{if(brokenCaptionStorage)throw new Error('storage blocked');captionStorage.set(key,value);}},
    location:{hash:token?"#invitation="+token:"",pathname:"/bot-call/"},
    history:{replaceState(unused,title,url){replaced.push(url);win.location.hash="";}},
    listeners:{}, addEventListener(name,fn){this.listeners[name]=fn;}};
  win.Audio=missingMedia?undefined:Media;
  win.URL={createObjectURL(){const u="blob:synthetic-"+urls.size;urls.add(u);return u;},revokeObjectURL(u){urls.delete(u);}};
  const page=mountCallPage(win,doc,{createVAD,chunkUpload:chunk,...(audioRequestMs===undefined?{}:{audioRequestMs})});
  if (maxAudioBytes) page.config.maxAudioBytes=maxAudioBytes;
  return {page,win,doc,nodes,clock,recorders,streams,utterances,replaced,storage,captureRequests,
    level(value){rms=value;}, stall(){audioClockStalled=true;},
    blockNextPlay(){rejectNextPlay=true;}, urls,
    setResolutionOffline(value){resolutionOffline=value;},
    holdResolution(loseAck=false){const gate={entered:false,loseAck};gate.promise=new Promise(resolve=>{gate.release=resolve;});resolutionGate=gate;return gate;},
    holdNextMedia(){const gate={entered:false};gate.promise=new Promise(resolve=>{gate.release=resolve;});mediaGate=gate;return gate;},
    holdNextSpeech(){const gate={entered:false};gate.promise=new Promise(resolve=>{gate.release=resolve;});speechGate=gate;return gate;},
    setPlaybackOffline(value){playbackOffline=value;}, holdNextPlayback(){
      const gate={entered:false};gate.promise=new Promise(resolve=>{gate.release=resolve;});playbackGate=gate;return gate;
    },
    get permissions(){return permissionCalls;}, enableAudio(){audioCapability=true;}, allowPermission(){permissionResolve();}, silence(){rms=0;}, voice(){rms=0.1;}};
}

// The main page displays capture availability independently of the mute control.
{
  const {page:p,nodes:n}=browser("");
  const shell=n["call-shell"].attributes,hint=n["call-hint"];
  assert.equal(shell["data-mic"],"muted");
  p.started=true;p.round={id:"u"};
  p.server={audio:[{utterance_id:"u",state:"succeeded"}],replies:[],turns:[]};
  p.render();
  assert.equal(shell["data-phase"],"thinking");
  assert.equal(shell["data-mic"],"waiting");
  assert.equal(n["call-state"].textContent,"想一想…");
  assert.equal(n["main-label"].textContent,"想一想…");
  assert.equal(hint.textContent,"正在思考，暂未收音，请稍等。");
  for(const closing of [false,true]) {
    p.server.tool_processing=!closing;p.server.background_processing=closing;
    await p.reconcileRound();
    assert.equal(shell["data-phase"],"thinking");
    assert.equal(shell["data-tool-wait"],"true");
    assert.equal(shell["data-mic"],"waiting");
    assert.equal(n["main-label"].textContent,closing?"工具收尾中…":"工具处理中…");
    assert.equal(n["call-state"].textContent,closing?"工具正在安全收尾，暂不可插话":"工具处理中，暂不可插话");
    assert.equal(hint.textContent,closing?"工具正在安全收尾，暂不可插话，请稍等。":"正在调用工具，暂不可插话，请稍等。");
    assert.equal(hint.hidden,false);
    assert.equal(n.status.textContent,closing?"后台处理中，正在等待工具安全收尾":"正在调用工具，暂不可插话，请稍等");
    assert.equal(n.mute.disabled,false);
  }
  p.speech={playing:true};p.render();
  assert.equal(shell["data-phase"],"speaking");
  assert.equal(shell["data-tool-wait"],"false");
  assert.equal(shell["data-mic"],"waiting");
  assert.equal(n["call-state"].textContent,"正在说，点击可打断");
  assert.equal(n["main-label"].textContent,"点击打断");
  assert.equal(hint.hidden,true);
  p.round.playAt=10;p.speech=null;p.render();
  assert.equal(shell["data-tool-wait"],"false","a gap after first sound is not tool wait");
  p.round=null;p.recording={recorder:{state:"recording"}};p.render();
  assert.equal(shell["data-mic"],"listening");
  p.recording=null;p.muted=true;p.render();
  assert.equal(shell["data-mic"],"muted");
}

// A continuous input remains visible through recorder and player hand-offs.
{
  let armedOptions;
  const detector={ready:()=>true,async begin(options){armedOptions={...options};return null;}};
  const {page:p,nodes,win,clock}=browser("",{createVAD:()=>detector});
  const mic=()=>{p.render();return nodes["call-shell"].attributes["data-mic"];};
  p.started=true;p.config.bargeIn="auto";
  p.config.silenceMs=1400;p.config.vadRedemptionMs=1800;
  p.stream=await win.navigator.mediaDevices.getUserMedia();
  p.audioContext=new win.AudioContext();
  p.capture={closed:false,recorder:()=>new win.MediaRecorder(p.stream,{mimeType:"audio/webm;codecs=opus"})};
  p.prepareCapture=async()=>{};
  p.series={queue:[]};p.recording={stopping:true,recorder:{state:"inactive"}};
  assert.equal(mic(),"listening","the old recorder is stopping");
  p.recording=null;
  assert.equal(mic(),"listening","the next recorder has not started");
  p.series.finishing=true;
  assert.equal(mic(),"muted");
  p.series=null;p.listenTask=Promise.resolve();
  assert.equal(mic(),"listening","a silent drop is reopening capture");
  p.listenTask=null;
  p.server={audio_available:true,speech_available:true};
  p.round={id:"old",playAt:10};p.playbackGain={};
  p.echo={model:{},summary:()=>"",stop:async()=>{}};
  assert.equal(mic(),"listening","the next sentence has no player yet");
  const track=p.stream.getTracks()[0];
  track.muted=true;assert.equal(mic(),"waiting");track.muted=false;
  p.config.bargeIn="off";assert.equal(mic(),"waiting");p.config.bargeIn="auto";
  p.round.playbackFinished=true;assert.equal(mic(),"waiting");p.round.playbackFinished=false;
  p.ending=true;assert.equal(mic(),"muted");p.ending=false;
  p.muted=true;assert.equal(mic(),"muted");p.muted=false;

  const media=new win.Audio();media.currentTime=.2;media.duration=1;
  const speech=p.speech={id:"reply:old",seq:1,round:p.round,media,playing:true,started:true};
  p.api=async()=>({});
  const starting=p.beginBarge(speech,{onset:0,decided:0,referenceKnown:1});
  assert(p.barge && !p.recording && !p.speech);
  assert.equal(mic(),"listening","MP3 pause is pending before capture can start");
  assert.equal(p.bargeSilenceMs(),1400);
  assert.equal(p.bargeSilenceMs(true),1800);
  clock.advance(25);await starting;
  assert(p.recording);
  assert.equal(armedOptions.vadRedemptionMs,1800);
  assert.equal(mic(),"listening");
}

// The cue uses only a running existing context, and all work is owned by the wait.
{
  const {page:p,clock}=browser("");
  const tones=[],gains=[];
  let newContexts=0;
  p.e.AudioContext=class {constructor(){newContexts++;}};
  const node=()=>({connect(){},disconnect(){this.disconnected=true;}});
  const ctx={
    state:"running",destination:{},get currentTime(){return clock.time/1000;},
    createOscillator(){const tone={...node(),frequency:{},stops:[],start(at){this.started=at;},stop(at){this.stops.push(at);}};tones.push(tone);return tone;},
    createGain(){const envelope=[];const gain={...node(),envelope,gain:{
      setValueAtTime:(value,time)=>envelope.push([value,time]),
      linearRampToValueAtTime:(value,time)=>envelope.push([value,time])
    }};gains.push(gain);return gain;}
  };
  p.pcmContext=ctx;p.started=true;p.round={id:"u"};
  p.server={turns:[],tool_processing:true};p.render();
  const firstTimer=p.toolWaitTimer;
  p.render();assert.equal(p.toolWaitTimer,firstTimer);
  clock.advance(2999);assert.equal(tones.length,0);
  clock.advance(1);assert.equal(tones.length,1);
  assert.equal(tones[0].type,"sine");assert.equal(tones[0].frequency.value,440);
  assert.deepEqual(gains[0].envelope,[[0,3],[.025,3.02],[0,3.12]]);
  assert.deepEqual(tones[0].stops,[3.12]);
  tones[0].onended();assert(tones[0].disconnected && gains[0].disconnected);
  clock.advance(5999);assert.equal(tones.length,1);
  clock.advance(1);assert.equal(tones.length,2);tones[1].onended();
  clock.advance(6000);assert.equal(tones.length,3);tones[2].onended();
  clock.advance(6000);assert.equal(tones.length,3);assert.equal(p.toolWaitTimer,null);

  p.server.tool_processing=false;p.render();
  p.server.tool_processing=true;p.muted=true;p.render();
  clock.advance(3000);assert.equal(tones.length,3);assert.equal(p.toolWaitTimer,null);

  p.muted=false;ctx.state="suspended";p.render();
  clock.advance(3000);assert.equal(tones.length,3);
  p.server.tool_processing=false;p.render();
  assert.equal(p.toolWaitTimer,null);
  ctx.state="running";p.server.tool_processing=true;
  p.echo={closed:false,current:{},summary(){}};p.render();
  clock.advance(3000);assert.equal(tones.length,3);
  p.echo=null;clock.advance(6000);assert.equal(tones.length,4);

  p.server.tool_processing=false;p.render();
  assert.equal(p.toolWaitTimer,null);assert.deepEqual(tones[3].stops.slice(1),[undefined]);
  assert(tones[3].disconnected && gains[3].disconnected);
  p.server.tool_processing=true;p.render();
  clock.advance(3000);assert.equal(tones.length,5);
  const pending=p.toolWaitTimer,speech={local:true};
  p.speech=speech;p.beginSpeech(speech);
  assert.deepEqual(tones[4].stops.slice(1),[undefined]);
  assert(tones[4].disconnected && gains[4].disconnected);
  assert(!clock.tasks.has(pending));assert.equal(p.toolWaitTimer,null);
  clock.advance(6000);assert.equal(tones.length,5);
  assert.equal(newContexts,0);
}
async function until(predicate, message) {
  for(let i=0;i<150;i++) {if(await predicate())return; await new Promise(resolve=>setTimeout(resolve,2));}
  throw new Error(message);
}
async function settleSpeech(browser) {
  await until(()=>!browser.page.flushTask,"playback observation did not settle");
}

async function phoneUIFacets() {
  const b=browser(null),p=b.page,n=b.nodes;
  assert.equal(p.config.vadPositive,.5);
  assert.equal(p.config.vadNegative,.45);
  const experience={silence_ms:1200,sensitivity:50,capture_mode:"auto",barge_in:"auto"};
  for(const [sensitivity,positive,negative] of [[70,.3,.25],[50,.5,.45]]) {
    p.experience({...experience,sensitivity});
    assert.equal(p.config.vadPositive,positive);
    assert.equal(p.config.vadNegative,negative);
  }
  assert.equal(n.start.textContent,'开始通话');assert.equal(n.start.hidden,false);assert.equal(n['main-action'].hidden,true);
  assert.equal(n['call-state'].textContent,'点开始，聊聊吧');assert.equal(n.details.open,false);
  p.server={bot_name:'小声',turns:[{role:'user',body:'今天很累。'},{role:'assistant',body:'先休息一下。喝点水！要聊聊吗？'}]};p.render();
  assert.equal(n['bot-name'].textContent,'小声');
  assert.equal(n.captions.children.length,2);assert.equal(n.captions.children[0].attributes['data-role'],'user');assert.equal(n.captions.children[0].children[1].textContent,'今天很累。');
  assert.equal(n.captions.children[1].children[1].textContent,'先休息一下。喝点水！要聊聊吗？','whole turns must stay visible, not only the last sentence');
  n['details-open'].click();assert.equal(n.details.open,true);assert.equal(n['call-shell'].inert,true);assert.equal(b.permissions,0);
  p.render();assert(n.details.open);
  n['details-close'].click();assert.equal(n.details.open,false);assert.equal(n['call-shell'].inert,false);
  n['details-open'].click();b.doc.listeners.keydown({key:'Escape',preventDefault(){}});assert.equal(n.details.open,false);
  n.subtitles.click();assert.equal(n.conversation.hidden,true);assert.equal(n.subtitles.attributes['aria-pressed'],'false');
  p.render();assert.equal(n.conversation.hidden,true);assert.equal(n.captions.children.length,2);assert.equal(b.permissions,0);
  n.subtitles.click();assert.equal(n.conversation.hidden,false);
  p.recording={recorder:{state:'recording'},vad:{valid:true},level:.4};p.render();assert.equal(n['call-state'].textContent,'可以说话');
  p.recording.voiced=true;p.render();assert.equal(n['call-state'].textContent,'正在听…');assert.equal(n['voice-level'].value,.4);assert.equal(n.bars.style.vars['--voice'],'0.400');assert.equal(n['glow-orb'].style.vars['--s'],'1.200');
  p.recording=null;p.round={id:'u'};p.render();assert.equal(n['call-state'].textContent,'想一想…');
  p.server.segments=[{generation_id:'reply:u',seq:1,text:'正在为你查看。',state:'succeeded'}];p.render();
  assert.equal(n.captions.children.at(-1).children[1].textContent,'正在为你查看。');
  p.server.turns=[...p.server.turns,{role:'assistant',body:'正在为你查看。',material_id:'reply:u'}];p.render();
  assert.equal(n.captions.children.length,3,'final material must replace the provisional caption');
  p.speech={waiting:true};p.render();assert.equal(n['call-state'].textContent,'想一想…','waiting audio must not pretend playing');
  p.speech={playing:true};p.render();assert.equal(n['call-state'].textContent,'正在说，点击可打断');assert.equal(n['main-action'].hidden,false);assert.equal(n['main-label'].textContent,'点击打断');
  let projection;const oldView=p.view;p.view=value=>{projection=value;oldView(value);};
  p.vadDiagnostic='quality/clock';p.analysisDiagnostic='analysis/gap';
  p.started=true;p.speech=null;p.round=null;p.clip={};p.report(new Error('PRIVATE raw browser failure'));assert.equal(n['call-state'].textContent,'暂时没接上');
  assert(!/quality|analysis|HTTP|接口诊断/.test(projection.notice));assert(projection.details.includes('quality/clock'));
  assert.equal(n['call-hint'].hidden,true,'the long explanation moves into Details');assert(n.notice.textContent.includes('核查'));assert(!/PRIVATE/.test(n.notice.textContent));assert(n.notice.textContent.includes('接口诊断'));
  assert.equal(n.start.hidden,true,'recovery must not come back as a separate start button');assert.equal(n['main-action'].hidden,false);assert.equal(n['main-action'].attributes['data-kind'],'recover');
  assert.equal(n['main-label'].textContent,'刚才那句可能没发出去，点击重试');
  assert(!n['call-hint'].textContent.includes('详情'));n.details.open=false;
  let retried=0;p.retry=()=>{retried++;};n['main-action'].click();assert.equal(retried,1);assert.equal(n.details.open,false);
  p.clip=null;p.apiFailure=null;p.speech={needsGesture:true};p.render();
  assert.equal(n['main-label'].textContent,'声音被浏览器拦住了，点击恢复声音');
  let resumed=0;p.resumeVoice=()=>{resumed++;};n['main-action'].click();assert.equal(resumed,1);
  assert.equal(n['resume-voice'].disabled,false,'Details recovery must remain usable');assert.equal(n['main-action'].attributes['data-kind'],'recover');assert.equal(n['detail-stop'].disabled,false);
  p.speech={stopUnconfirmed:true};p.speechBlocked=true;p.render();assert.equal(n['main-label'].textContent,'停止还没确认，点击再试');
  let stopped=0;p.stopSpeaking=()=>{stopped++;};n['main-action'].click();assert.equal(stopped,1);
  p.speech=null;p.speechBlocked=false;p.playbackPaused=true;p.render();assert.equal(n['main-label'].textContent,'出了一点问题，点击继续对话');
  let continued=0;p.start=()=>{continued++;};n['main-action'].click();assert.equal(continued,1);
  p.started=false;p.reference='previously-entered';p.render();assert.equal(n.start.hidden,false);assert.equal(n.start.textContent,'继续通话','page recovery must not grant new microphone intent or say Start');
  assert.equal(p.started,false);p.locked=true;p.render();assert.equal(n.start.disabled,true,'invalid invitation cannot be retried');
  p.started=true;p.render();assert.equal(n['main-label'].textContent,'通话已失效，请回原聊天重新发起');assert.equal(n['main-action'].disabled,true);p.started=false;
  p.locked=false;p.playbackPaused=false;
  for(const id of ['retry','resume-voice','test-voice','main-action','jump-latest','detail-stop','mute','end'])assert.equal(typeof n[id].listeners.click,'function');assert.equal(typeof n.conversation.listeners.scroll,'function');
  p.apiFailure=null;p.clip=null;p.callStartedAt=0;b.clock.advance(65000);p.render();assert.equal(n['call-timer'].textContent,'01:05');
}
async function contextActionFacet() {
  const b=browser(null),p=b.page,n=b.nodes,action=n['main-action'],label=()=>n['main-label'].textContent,kind=()=>action.attributes['data-kind'];
  let sent=0,stopped=0;p.submitSegment=()=>{sent++;return Promise.resolve();};p.stopSpeaking=()=>{stopped++;return Promise.resolve();};
  p.started=true;p.callStartedAt=0;
  p.recording={recorder:{state:'recording'},vad:{valid:true},level:0};p.render();
  assert.equal(action.hidden,false);assert.equal(n.start.hidden,true);
  assert.equal(label(),'请说话');assert.equal(kind(),'send');assert.equal(action.disabled,false);
  p.recording.voiced=true;p.render();assert.equal(label(),'点击发送');assert.equal(n['call-shell'].attributes['data-phase'],'listening');
  action.click();assert.equal(sent,1);assert.equal(stopped,0);
  p.recording={recorder:{state:'recording'},vad:{valid:false},level:0};p.render();
  assert.equal(label(),'点击发送','without automatic detection the user must be told to send');
  p.recording=null;p.round={id:'u'};p.render();
  assert.equal(label(),'想一想…');assert.equal(kind(),'idle');assert.equal(action.disabled,true);action.click();assert.equal(sent+stopped,1);
  p.speech={playing:true};p.render();assert.equal(label(),'点击打断');assert.equal(kind(),'stop');assert.equal(action.disabled,false);
  action.click();assert.equal(stopped,1);assert.equal(sent,1);
  p.speech={playing:true,local:true};p.render();assert.equal(label(),'点击停止试听');
  p.speech=null;p.round=null;p.muted=true;p.listenTask={};p.render();
  assert.equal(label(),'已静音');assert.equal(action.disabled,true);assert.equal(n.mute.attributes['aria-pressed'],'true');
  p.muted=false;p.listenTask=null;p.playbackPaused=true;p.render();
  assert.equal(kind(),'recover');assert.equal(label(),'出了一点问题，点击继续对话');assert.equal(action.hidden,false);assert.equal(action.disabled,false);
  assert.equal(n.start.hidden,true,'recovery shares the context button, no separate pill');assert.equal(n['call-hint'].hidden,true);
  let continued=0;p.start=()=>{continued++;return Promise.resolve();};action.click();assert.equal(continued,1);
  p.playbackPaused=false;p.render();assert.equal(kind(),'recover');assert.equal(label(),'点击继续通话','idle with nothing running offers Continue');
  p.suspended=true;p.render();assert.equal(label(),'通话已暂停，点击继续');assert.equal(kind(),'recover');p.suspended=false;
}
async function conversationScrollFacet() {
  const b=browser(null),p=b.page,n=b.nodes,c=n.conversation,jump=n['jump-latest'];
  const turn=(role,body)=>({role,body});
  c.scrollHeight=1000;c.clientHeight=400;c.scrollTop=600;
  p.server={bot_name:'小声',turns:[turn('user','你好。'),turn('assistant','你好，想聊点什么？')]};p.render();
  assert.equal(c.scrollTop,1000,'following the latest line scrolls new text into view');assert.equal(jump.hidden,true);
  const before=[...n.captions.children];
  p.server={bot_name:'小声',turns:[turn('user','你好。'),turn('assistant','你好，想聊点什么？')]};p.render();
  assert.deepEqual(n.captions.children.map((row,i)=>row===before[i]),[true,true],'identical text must keep the same rows');
  c.scrollTop=100;c.listeners.scroll();
  p.server={bot_name:'小声',turns:[...p.server.turns,turn('user','随便聊聊。')]};p.render();
  assert.equal(c.scrollTop,100,'reading older text must not be pulled back');assert.equal(jump.hidden,false);
  p.server={bot_name:'小声',turns:[...p.server.turns]};p.render();assert.equal(c.scrollTop,100);
  jump.click();assert.equal(c.scrollTop,1000);assert.equal(jump.hidden,true);
  c.scrollTop=100;c.listeners.scroll();p.server={bot_name:'小声',turns:[...p.server.turns,turn('assistant','好呀。')]};p.render();assert.equal(jump.hidden,false);
  c.scrollTop=570;c.listeners.scroll();assert.equal(jump.hidden,true,'reaching the bottom clears the hint');
  // The Bot line being read is emphasised; a local sample and a finished reply are not.
  const rows=()=>n.captions.children.map(row=>row.attributes['data-speaking']);
  assert.deepEqual(rows(),['false','false','false','false']);
  p.speech={playing:true};p.render();assert.deepEqual(rows(),['false','false','false','true']);
  p.speech={playing:true,local:true};p.render();assert.deepEqual(rows(),['false','false','false','false']);
  p.speech=null;p.render();assert.deepEqual(rows(),['false','false','false','false']);
  n.subtitles.click();assert.equal(c.hidden,true);assert.equal(jump.hidden,true);
  c.scrollTop=0;n.subtitles.click();assert.equal(c.hidden,false);assert.equal(c.scrollTop,1000,'showing text again starts at the latest line');
}
async function transitionFacet() {
  // Healthy hand-offs must never offer recovery. Every render of each hand-off is audited.
  const make=options=>{
    const b=browser(null,options),p=b.page,n=b.nodes,frames=[],view=p.view;
    p.view=value=>{view(value);frames.push({label:n['main-label'].textContent,kind:n['main-action'].attributes['data-kind'],phase:value.ui.phase,
      recovery:n['main-action'].attributes['data-kind']==='recover' || p.started && !n.start.hidden || value.started && value.continueNeeded});};
    p.reference='synthetic-reference';
    const state={audio:[],replies:[],turns:[],audio_available:true,speech_available:true,complete:false};
    const gate={entered:false};gate.promise=new Promise(resolve=>{gate.release=resolve;});
    p.api=async(route,body,headers)=>{
      if(route==='audio') {
        gate.entered=true;await gate.promise;
        const sha=[...new Uint8Array(await webcrypto.subtle.digest('SHA-256',await body.arrayBuffer()))].map(x=>x.toString(16).padStart(2,'0')).join('');
        state.audio.push({utterance_id:headers['X-Bot-Utterance-ID'],sha256:sha,byte_count:body.size,mime:body.type,state:'running'});
        return {audio_received:true,utterance_id:headers['X-Bot-Utterance-ID']};
      }
      if(route==='state')return {...state};
      throw new Error('unexpected route '+route);
    };
    return {b,p,n,frames,gate,bad:()=>frames.filter(frame=>frame.recovery)};
  };
  for(const mode of ['manual','auto']) {
    const {b,p,n,frames,gate,bad}=make();
    if(mode==='auto') {b.voice();p.config.silenceMs=200;}
    await p.start();assert.ok(p.recording,'recording did not start: '+mode);assert.equal(bad().length,0,'start flashed recovery: '+mode);
    if(mode==='manual') {n['main-action'].click();}
    else {
      for(let i=0;i<3;i++){b.clock.advance(100);await new Promise(resolve=>setImmediate(resolve));}
      b.silence();for(let i=0;i<4;i++){b.clock.advance(100);await new Promise(resolve=>setImmediate(resolve));}
    }
    await until(()=>gate.entered,'upload not reached: '+mode);
    assert.equal(n['main-label'].textContent,'想一想…','uploading must read as thinking: '+mode);assert.equal(n['main-action'].disabled,true);
    assert.equal(n.start.hidden,true,'upload showed the start button: '+mode);assert.equal(bad().length,0,'upload flashed recovery: '+mode);
    gate.release();await until(()=>!!p.round && !p.sendTask,'upload did not finish: '+mode);
    assert.equal(n['main-label'].textContent,'想一想…');assert.equal(bad().length,0,'reply wait flashed recovery: '+mode);
    // The reply plays and ends; listening restarts on the retained stream without a recovery frame.
    const speech={round:p.round,timer:null,playing:true,seq:0};p.speech=speech;p.render();
    assert.equal(n['main-label'].textContent,'点击打断');
    const resumed=p.afterSpeech(speech);await resumed;await until(()=>b.recorders.length===2 && !!p.recording,'listening did not restart: '+mode);
    assert.equal(n['main-action'].attributes['data-kind'],'send');assert.equal(bad().length,0,'listening restart flashed recovery: '+mode);
    assert.ok(frames.some(frame=>frame.phase==='thinking'),'thinking frames were not rendered: '+mode);
    p.releaseMic();
  }
  // The permission prompt renders inside the synchronous part of listen(), before listenTask exists.
  const {b,p,n,bad}=make({pendingPermission:true});
  p.started=true;p.server={audio_available:true,speech_available:true,audio:[],replies:[],turns:[]};
  const listening=p.listen();
  assert.equal(n['main-action'].attributes['data-kind'],'idle','waiting for the microphone prompt must not offer Continue');
  assert.equal(n['main-label'].textContent,'准备中…');assert.equal(bad().length,0);
  b.allowPermission();await listening;assert.ok(p.recording);assert.equal(bad().length,0);p.releaseMic();
}
async function endedFacet() {
  // After hang-up the call is over: a cancelled round, stale failures and stopped playback are results, not problems.
  const closedState=(extra={})=>({audio:[{utterance_id:'r',state:'cancelled',error_code:'cancelled_closed'}],
    replies:[{utterance_id:'r',generation_id:'reply:r',state:'cancelled'}],turns:[],segments:[],audio_available:true,speech_available:true,
    complete:false,close_requested:true,ended:false,close_state:'waiting_for_audio',...extra});
  const make=()=>{
    const b=browser(null),p=b.page,n=b.nodes,frames=[],view=p.view;
    p.view=value=>{view(value);frames.push({label:n['main-label'].textContent,kind:n['main-action'].attributes['data-kind'],hint:n['call-hint'].textContent,phase:value.ui.phase});};
    p.started=true;p.reference='synthetic-reference';p.round={id:'r'};
    return {b,p,n,frames};
  };
  const noProblem=(frames,message)=>assert.ok(frames.every(f=>f.kind!=='recover' && !f.label.includes('出了一点问题') && !f.hint.includes('出了一点问题')),message);
  {// Server confirmed the close; the round that was being generated is cancelled.
    const {p,n,frames}=make();
    p.ending=true;p.server=closedState();p.playbackPaused=true;p.render();
    assert.equal(n['main-label'].textContent,'通话已结束');assert.equal(n['main-action'].attributes['data-kind'],'idle');assert.equal(n['main-action'].disabled,true);
    assert.equal(n.start.hidden,true);assert.equal(n.mute.disabled,true);assert.equal(n.end.disabled,true);assert.equal(n['call-state'].textContent,'通话已结束');
    p.server=closedState({close_requested:false,ended:true});p.render();assert.equal(n['main-label'].textContent,'通话已结束');
    p.apiFailure={route:'state'};p.render();assert.equal(n['main-label'].textContent,'通话已结束','a stale read failure after the confirmed close is not a problem');
    noProblem(frames,'ended state offered recovery');
    p.apiFailure=null;p.server=closedState({complete:true});p.render();assert.equal(n['call-hint'].textContent,'回顾已提交，可以回原聊天继续聊。');
    p.server=closedState({close_state:'closed_no_text'});p.render();assert.equal(n['call-hint'].textContent,'没有可整理的文字，不会生成回顾。');
  }
  {// The end request is still on its way: no claim yet, but nothing to recover either.
    const {p,n,frames}=make();
    p.ending=true;p.server=closedState({close_requested:false});p.render();
    assert.equal(n['main-label'].textContent,'正在结束通话…');assert.equal(n['main-action'].disabled,true);assert.equal(n.end.disabled,false);noProblem(frames,'closing offered recovery');
  }
  {// Real hang-up while a reply is being generated.
    const {p,n,frames}=make();let ended=0;
    p.api=async route=>{if(route==='end'){ended++;return {};}if(route==='state')return closedState();throw new Error('unexpected route '+route);};
    await p.hangup();
    assert.equal(ended,1);assert.equal(n['main-label'].textContent,'通话已结束');assert.equal(n.mute.disabled,true);assert.equal(n.end.disabled,true);
    assert.ok(frames.some(f=>f.phase==='ended'),'the ended state was never rendered');noProblem(frames,'hang-up flashed a problem');
  }
  {// The server closes first and the page only learns it from a state read.
    const {p,n,frames}=make();
    p.api=async route=>{if(route==='state')return closedState();throw new Error('unexpected route '+route);};
    await p.refresh();
    assert.equal(p.ending,true);assert.equal(n['main-label'].textContent,'通话已结束');noProblem(frames,'server-side close flashed a problem');
  }
  {// A real failure to end is still shown, and recovers through the existing check/retry path.
    const {p,n}=make();let attempts=0;
    p.api=async route=>{attempts++;throw new Error('offline');};
    await p.hangup();
    assert.equal(n['main-action'].attributes['data-kind'],'recover');assert.equal(n['main-label'].textContent,'结束还没确认，点击核查');
    assert.equal(n['main-action'].disabled,false);assert.equal(n.end.disabled,false);assert.equal(n.mute.disabled,false,'not confirmed: controls stay available');
    p.clip={};p.render();assert.equal(n['main-label'].textContent,'最后一段还没确认，点击核查');p.clip=null;
    p.api=async route=>route==='end'?{}:closedState();
    n['main-action'].click();await until(()=>n['main-label'].textContent==='通话已结束','recovery did not finish the end');
    assert.equal(n.end.disabled,true);
  }
}
async function glowMotionFacet() {
  const {GlowMotion,glowTarget}=globalThis.pageModule;
  const queue=[];let now=0;
  const win={requestAnimationFrame(callback){queue.push(callback);return queue.length;},matchMedia:()=>({matches:false})};
  const orb=new Element(),bars=new Element(),glow=new GlowMotion(orb,bars,win);
  const size=()=>Number(orb.style.vars['--s']);
  const run=ms=>{const seen=[size()];for(let i=0;i<ms/16;i++){const frame=queue.shift();if(!frame)break;now+=16;frame(now);seen.push(size());}return seen;};
  const steps=seen=>seen.slice(1).map((v,i)=>Math.abs(v-seen[i]));
  glow.update('ready',0);run(800);assert.ok(Math.abs(size()-1)<.01);
  // Level jumps 0 -> 1: the glow grows over several frames, never in one jump.
  glow.update('listening',1);let seen=run(1200);
  assert.ok(Math.max(...steps(seen))<.03,'growth jumped: '+Math.max(...steps(seen)));assert.ok(seen.at(-1)>1.4,'did not reach the loud size');assert.ok(Number(bars.style.vars['--voice'])>.9);
  // Release is slower than attack.
  const attack=seen.findIndex(v=>v>1.25);glow.update('listening',0);seen=run(1600);
  const release=seen.findIndex(v=>v<1.25);assert.ok(release>attack,'release must be slower than attack');assert.ok(Math.max(...steps(seen))<.03);
  // Changing phase glides as well, and a calm phase settles without any continuing frame requests.
  for(const phase of ['speaking','thinking','listening']) {glow.update(phase,0);seen=run(1200);assert.ok(Math.max(...steps(seen))<.03,'phase change jumped: '+phase);}
  glow.update('ended',0);seen=run(3000);assert.ok(Math.max(...steps(seen))<.03);assert.ok(Math.abs(size()-.88)<.01);assert.equal(queue.length,0,'a finished call must not keep animating');
  glow.update('thinking',0);run(500);assert.equal(queue.length,1,'thinking breathes');
  // Reduced motion: no frame requests at all and no scale.
  const still={requestAnimationFrame(){throw new Error('animation frame requested');},matchMedia:()=>({matches:true})};
  const quiet=new GlowMotion(new Element(),new Element(),still);quiet.update('listening',.8);quiet.update('speaking',0);assert.equal(quiet.orb.style.vars['--s'],'1.000');
  // No animation frames available: the target is applied directly.
  const bare=new GlowMotion(new Element(),new Element(),{});bare.update('listening',.4);assert.equal(bare.orb.style.vars['--s'],'1.200');assert.equal(bare.bars.style.vars['--voice'],'0.400');
  assert.ok(glowTarget('speaking',0,0).s>=1);
}
async function uploadStepsFacet() {
  // First submission: straight to the POST. Retry: read the state first. Both report where the time went.
  const sha256 = async blob => [...new Uint8Array(await webcrypto.subtle.digest("SHA-256", await blob.arrayBuffer()))].map(n => n.toString(16).padStart(2, "0")).join("");
  const make = () => {
    const b = browser(null), p = b.page, routes = [], sent = [];
    const state = {audio: [], replies: [], turns: [], segments: [], audio_available: true, speech_available: true, complete: false};
    p.reference = "synthetic-reference"; p.server = state;
    const server = {failPost: false, post: 250, pre: 120};
    p.api = async (route, body, headers, unused, context) => {
      routes.push(route + (context ? ":" + context : ""));
      if (route === "audio") {
        b.clock.advance(server.post);
        if (server.failPost) throw poison();
        const id = headers["X-Bot-Utterance-ID"];
        state.audio.push({utterance_id: id, sha256: await sha256(body), byte_count: body.size, mime: body.type, state: "running"});
        state.replies.push({utterance_id: id, generation_id: "reply:" + id, state: "running"});
        return {audio_received: true, utterance_id: id};
      }
      if (route === "state") { if (context === "clip_precheck") b.clock.advance(server.pre); return {...state, audio: [...state.audio], replies: [...state.replies]}; }
      if (route === "metrics") { if(body.generation_id.startsWith("reply:"))sent.push(JSON.parse(JSON.stringify(body))); return {}; }
      throw new Error("unexpected route " + route);
    };
    return {b, p, routes, sent, state, server};
  };
  const poison = () => new Error("synthetic transport failure");
  const upload = values => Object.fromEntries(Object.entries(values).filter(([key]) => key.startsWith("client_upload_")));
  {const {p}=make();assert.equal(new globalThis.pageModule.CallPage(p.e).config.chunkUpload,true);}
  {// Real recorder path: flush, digest and post are measured, there is no precheck, and the numbers reach the server.
    const {b, p, routes, sent} = make();
    const digest = p.e.crypto.subtle.digest.bind(p.e.crypto.subtle);
    p.e.crypto = {randomUUID: () => webcrypto.randomUUID(), subtle: {digest: async (...args) => { b.clock.advance(7); return digest(...args); }}};
    await p.start(); assert.ok(p.recording); routes.length = 0; // start() reads the state once while connecting
    b.recorders[0].beforeStop = () => b.clock.advance(35);
    await p.submitSegment(); await until(() => !!p.round && !p.sendTask && sent.length > 0, "upload did not finish");
    assert.deepEqual(routes.filter(route => route !== "metrics").slice(0, 2), ["audio:audio_submit", "state:request"], "first submission: POST before anything else, no precheck");
    assert.ok(!routes.includes("state:clip_precheck"));
    assert.deepEqual(upload(p.round.metrics), {client_upload_flush: 35, client_upload_digest: 7, client_upload_post: 250});
    assert.equal(p.round.uploadAt - p.round.endedAt, 292, "the pieces add up to the whole");
    assert.deepEqual(upload(sent[0].values), {client_upload_flush: 35, client_upload_digest: 7, client_upload_post: 250});
    assert.equal(sent[0].values.client_upload, 292);
    assert.ok(Object.values(upload(sent[0].values)).every(value => Number.isSafeInteger(value) && value >= 0));
    p.releaseMic();
  }
  {// A failed first attempt: the retry checks the state, and an upload that did arrive is never sent again.
    const {b, p, routes, server, state} = make();
    const blob = new Blob(["retained container"], {type: "audio/webm;codecs=opus"});
    const clip = {id: "retry-owned", mime: blob.type, blob, metrics: {}}; p.clip = clip;
    server.failPost = true; assert.equal(await p.sendClip(), false);
    assert.equal(clip.postStarted, true); assert.deepEqual(routes, ["audio:audio_submit", "state:audio_readback"]); assert.equal(p.clip, clip);
    state.audio.push({utterance_id: clip.id, sha256: await sha256(blob), byte_count: blob.size, mime: blob.type, state: "running"});
    state.replies.push({utterance_id: clip.id, generation_id: "reply:" + clip.id, state: "running"});
    routes.length = 0; server.failPost = false; assert.equal(await p.sendClip(), true);
    assert.deepEqual(routes.slice(0, 2), ["state:clip_precheck", "state:request"], "retry reads first and finds the bytes already there");
    assert.ok(!routes.includes("audio:audio_submit"), "never uploaded twice");
    assert.equal(p.clip, null); assert.equal(p.round.id, clip.id);
    assert.deepEqual(upload(p.round.metrics), {client_upload_digest: 0, client_upload_precheck: 120});
  }
  {// A retry that finds nothing posts after the precheck, and reports both.
    const {b, p, routes, server} = make();
    const blob = new Blob(["second attempt"], {type: "audio/webm;codecs=opus"});
    const clip = {id: "retry-missing", mime: blob.type, blob, metrics: {}}; p.clip = clip;
    server.failPost = true; assert.equal(await p.sendClip(), false);
    routes.length = 0; server.failPost = false; assert.equal(await p.sendClip(), true);
    assert.deepEqual(routes.slice(0, 2), ["state:clip_precheck", "audio:audio_submit"]);
    assert.deepEqual(upload(p.round.metrics), {client_upload_digest: 0, client_upload_precheck: 120, client_upload_post: 250});
  }
}
async function chunkUploadFacet() {
  // A stand-in server that follows the chunk contract: exact next_seq/next_offset, idempotent identical re-sends,
  // 409 for gaps/conflicts/expiry, commit checks, 404 when the routes do not exist.
  const sha256 = bytes => createHash("sha256").update(bytes).digest("hex");
  const make = (options = {}) => {
    const b = browser(null, {chunk: true, createVAD: options.vad ? () => options.vad : undefined}), p = b.page, log = [], audio = [], opts = {chunkRoutes: true, ...options};
    const server = {stage: null, expired: false};
    const respond = (body, status = 200) => ({ok: status >= 200 && status < 300, status, json: async () => body});
    const record = (id, bytes, mime) => audio.push({utterance_id: id, sha256: sha256(bytes), byte_count: bytes.length, mime, state: "pending"});
    p.reference = "synthetic-reference";
    p.e.fetch = async (url, init) => {
      const route = url.slice("/bot-call/".length), h = Object.fromEntries(Object.entries(init.headers || {}).map(([k, v]) => [k.toLowerCase(), v]));
      if (route === "state") return respond({audio: audio.map(item => ({...item})), replies: [], turns: [], segments: [], audio_available: true, speech_available: true, complete: false});
      if (route === "metrics" || route === "heartbeat") return respond({});
      if (route === "audio-chunk") {
        const bytes = Buffer.from(await init.body.arrayBuffer()), seq = Number(h["x-bot-chunk-seq"]), offset = Number(h["x-bot-chunk-offset"]), id = h["x-bot-utterance-id"];
        log.push({route, seq, offset, size: bytes.length, text: bytes.toString(), id, type: h["content-type"]});
        if (!opts.chunkRoutes) return respond({error: "not_found"}, 404);
        if (opts.gate && !opts.gate.used) { opts.gate.used = true; opts.gate.entered = true; opts.gate.signal=init.signal; await opts.gate.promise; }
        if (server.expired) return respond({error: "audio_chunk_missing"}, 409);
        let stage = server.stage;
        if (stage && stage.id !== id) { if (seq === 0) stage = server.stage = null; else return respond({error: "audio_chunk_missing"}, 409); }
        if (!stage) {
          if (seq !== 0 || offset !== 0) return respond({error: "audio_chunk_missing"}, 409);
          stage = server.stage = {id, seq: 0, offset: 0, parts: [], seen: new Map()};
        }
        const before = stage.seen.get(seq);
        if (before) return before.offset === offset && before.text === bytes.toString() ? respond({chunk_received: true, utterance_id: id, next_seq: stage.seq, next_offset: stage.offset}) : respond({error: "audio_conflict"}, 409);
        if (seq !== stage.seq || offset !== stage.offset) return respond({error: "audio_conflict"}, 409);
        stage.parts.push(bytes); stage.seen.set(seq, {offset, text: bytes.toString()}); stage.seq++; stage.offset += bytes.length;
        if (opts.expireAfter && bytes.toString().includes(opts.expireAfter)) server.expired = true; // staged, then lost before the commit
        if (opts.loseChunkAck && !opts.loseChunkAck.done) { opts.loseChunkAck.done = true; throw new Error("synthetic lost chunk ACK"); }
        return respond({chunk_received: true, utterance_id: id, next_seq: stage.seq, next_offset: stage.offset});
      }
      if (route === "audio-commit") {
        const tail=Buffer.from(await init.body.arrayBuffer()), offset=Number(h["x-bot-tail-offset"]);
        const body={utterance_id:h["x-bot-utterance-id"],total_bytes:Number(h["x-bot-total-bytes"]),sha256:h["x-bot-audio-sha256"],mime:h["content-type"]};
        log.push({route, body, offset, text:tail.toString()});
        if(opts.expireAfter && tail.toString().includes(opts.expireAfter))server.expired=true;
        if (!opts.chunkRoutes) return respond({error: "not_found"}, 404);
        if (opts.gateCommit && !opts.gateCommit.used) { opts.gateCommit.used = true; opts.gateCommit.entered = true; await opts.gateCommit.promise; }
        let stage = server.stage;
        if(!stage && offset===0)stage=server.stage={id:body.utterance_id,parts:[],offset:0,seq:0,seen:new Map()};
        if (server.expired || !stage || stage.id !== body.utterance_id) return respond({error: "audio_chunk_missing"}, 409);
        const prefix=Buffer.concat(stage.parts);
        if(offset>prefix.length || !prefix.subarray(offset).equals(tail.subarray(0,prefix.length-offset)))return respond({error:"audio_conflict"},409);
        const bytes=Buffer.concat([prefix.subarray(0,offset),tail]);
        stage.parts=[bytes];stage.offset=bytes.length;
        if (body.total_bytes !== bytes.length || body.sha256 !== sha256(bytes) || body.utterance_id !== stage.id) return respond({error: "audio_conflict"}, 409);
        if (opts.failCommit && !opts.failCommit.done) { opts.failCommit.done = true; throw new Error("synthetic commit lost before persisting"); }
        if (!audio.some(item => item.utterance_id === stage.id)) record(stage.id, bytes, body.mime);
        if (opts.loseCommitAck && !opts.loseCommitAck.done) { opts.loseCommitAck.done = true; throw new Error("synthetic lost commit ACK"); }
        return respond({audio_received: true, utterance_id: stage.id, audio_state: "pending"});
      }
      if (route === "audio") {
        const bytes = Buffer.from(await init.body.arrayBuffer()), id = h["x-bot-utterance-id"]; log.push({route, id, text: bytes.toString()});
        if (!audio.some(item => item.utterance_id === id)) record(id, bytes, init.body.type);
        return respond({audio_received: true, utterance_id: id, audio_state: "pending"});
      }
      throw new Error("unexpected route " + route);
    };
    const calls = route => log.filter(entry => entry.route === route);
    const speak = async () => { for (let i = 0; i < 4; i++) { b.clock.advance(100); await new Promise(resolve => setImmediate(resolve)); } };
    const settle = async () => { for (let i = 0; i < 8; i++) await new Promise(resolve => setImmediate(resolve)); };
    return {b, p, log, audio, server, calls, speak, settle, opts};
  };
  const upload = values => Object.fromEntries(Object.entries(values).filter(([key]) => key.startsWith("client_upload_")));

  {// Streaming: silence uploads nothing; speech releases the backlog; one request at a time with merging; only the tail and the commit remain.
    const gate = {entered: false}; gate.promise = new Promise(resolve => { gate.release = resolve; });
    const {b, p, calls, speak, settle, audio, server} = make({gate});
    await p.start(); const recorder = b.recorders[0], clipId = p.recording.id; assert.ok(p.recording.uploader);
    b.silence(); recorder.data("A1"); await settle(); assert.equal(calls("audio-chunk").length, 0, "no speech yet: nothing leaves the page");
    b.voice(); await speak(); await until(() => gate.entered, "first chunk was not sent");
    recorder.data("B2"); recorder.data("C3"); await settle(); assert.equal(calls("audio-chunk").length, 1, "one request in flight at a time");
    gate.release(); await until(() => calls("audio-chunk").length === 2, "merged chunk was not sent");
    assert.deepEqual(calls("audio-chunk").map(c => [c.seq, c.offset, c.text, c.id, c.type]), [[0, 0, "A1", clipId, "audio/webm;codecs=opus"], [1, 2, "B2C3", clipId, "audio/webm;codecs=opus"]], "held backlog first, then the waiting blobs merged");
    assert.equal(calls("audio").length, 0); assert.equal(audio.length, 0, "chunk ACKs mean staged, not received");
    b.clock.advance(50); await p.submitSegment(); await until(() => !!p.round && !p.sendTask, "streamed upload did not finish");
    assert.deepEqual(calls("audio-chunk").map(c => [c.seq, c.offset]), [[0, 0], [1, 2]]);
    assert.equal(calls("audio-commit")[0].text,"-tail🙂","final flush goes with commit");
    assert.equal(calls("audio").length, 0, "no whole-clip upload"); assert.equal(calls("audio-commit").length, 1);
    const commit = calls("audio-commit")[0].body, staged = Buffer.concat(server.stage.parts);
    assert.deepEqual(Object.keys(commit).sort(), ["mime", "sha256", "total_bytes", "utterance_id"]); assert.equal(commit.utterance_id, clipId);
    assert.equal(commit.total_bytes, staged.length); assert.equal(commit.sha256, sha256(staged)); assert.equal(commit.mime, "audio/webm;codecs=opus");
    assert.equal(staged.toString(), "A1B2C3-tail🙂", "what the server holds is exactly the recording");
    assert.equal(p.round.id, clipId); assert.equal(p.round.byte_count, staged.length); assert.equal(p.round.sha, commit.sha256); assert.equal(p.clip, null);
    const metrics = upload(p.round.metrics);
    assert.deepEqual(Object.keys(metrics).sort(), ["client_upload_chunks", "client_upload_digest", "client_upload_flush", "client_upload_post", "client_upload_tail"]);
    assert.equal(metrics.client_upload_chunks, 2); assert.ok(Object.values(metrics).every(value => Number.isSafeInteger(value) && value >= 0));
    p.releaseMic();
  }
  {// Stop while the FIRST request is still in flight: commit does not await its ACK.
    const gate={entered:false};gate.promise=new Promise(resolve=>{gate.release=resolve;});
    const {b,p,calls,speak,settle}=make({gate});
    await p.start();b.voice();b.recorders[0].data("inflight");await speak();await until(()=>gate.entered,"inflight chunk");
    const upload=p.recording.uploader,offset=upload.offset;assert.equal(offset,0);
    const sending=p.submitSegment();await until(()=>calls("audio-commit").length===1,"commit waited for chunk ACK");
    await sending;assert(p.round);assert.equal(gate.signal.aborted,true,"stop releases the in-flight request");assert.equal(calls("audio-chunk").length,1);assert.equal(calls("audio-commit")[0].offset,0);
    gate.release();await settle();assert.equal(upload.offset,0,"late ACK cannot alter frozen commit cursor");p.releaseMic();
  }
  {// A server without the routes: one 404, then every clip goes whole through the old route with the same id.
    const {b, p, calls, speak, settle, audio} = make({chunkRoutes: false});
    await p.start(); const recorder = b.recorders[0], clipId = p.recording.id;
    b.voice(); recorder.data("one"); await speak(); await settle(); recorder.data("two"); await settle();
    assert.equal(calls("audio-chunk").length, 1, "a 404 is final: no more chunk attempts"); assert.equal(p.chunkUnavailable, true);
    await p.submitSegment(); await until(() => !!p.round && !p.sendTask, "fallback did not finish");
    assert.equal(calls("audio-commit").length, 0); assert.deepEqual(calls("audio").map(c => [c.id, c.text]), [[clipId, "onetwo-tail🙂"]], "the complete Blob, same id");
    assert.equal(audio.length, 1); assert.equal(p.round.id, clipId); assert.equal(upload(p.round.metrics).client_upload_chunks, 1);
    p.round = null; p.speech = null; await p.listen(); assert.equal(p.recording.uploader, null, "later recordings do not try the missing routes");
    p.releaseMic();
  }
  {// Expired staging mid-stream (409): the uploader gives up quietly, the whole clip follows.
    const {b, p, calls, speak, settle, server} = make();
    await p.start(); const recorder = b.recorders[0], clipId = p.recording.id;
    b.voice(); recorder.data("first"); await speak(); await until(() => calls("audio-chunk").length === 1 && !p.recording.uploader.loop, "first chunk");
    server.expired = true; recorder.data("second"); await until(() => calls("audio-chunk").length === 2, "second chunk"); await settle();
    assert.equal(p.recording.uploader.failed, true); assert.equal(p.chunkUnavailable, undefined, "a 409 does not disable the routes for later clips");
    await p.submitSegment(); await until(() => !!p.round && !p.sendTask, "fallback did not finish");
    assert.equal(calls("audio-commit").length, 0); assert.deepEqual(calls("audio").map(c => [c.id, c.text]), [[clipId, "firstsecond-tail🙂"]]);
    assert.equal(calls("audio-chunk").length, 2, "nothing is sent after the failure"); assert.equal(p.apiFailure, null, "the user is not told about the quiet fallback");
    p.releaseMic();
  }
  {// Staging lost between the last chunk and the commit: commit answers 409, the same clip goes whole.
    const {b, p, calls, speak, settle} = make({expireAfter: "-tail"});
    await p.start(); const recorder = b.recorders[0], clipId = p.recording.id;
    b.voice(); recorder.data("alpha"); await speak(); await until(() => calls("audio-chunk").length === 1, "chunk");
    await p.submitSegment(); await until(() => !!p.round && !p.sendTask, "fallback did not finish");
    assert.equal(calls("audio-commit").length, 1); assert.deepEqual(calls("audio").map(c => [c.id, c.text]), [[clipId, "alpha-tail🙂"]]);
    assert.equal(p.round.id, clipId);
    p.releaseMic();
  }
  {// A chunk ACK is lost: the identical request is sent again (idempotent), never skipped or renumbered.
    const loseChunkAck = {};
    const {b, p, calls, speak, settle, server} = make({loseChunkAck});
    await p.start(); const recorder = b.recorders[0];
    b.voice(); recorder.data("lost-ack"); await speak(); await until(() => calls("audio-chunk").length === 2, "re-send");
    assert.deepEqual(calls("audio-chunk").map(c => [c.seq, c.offset, c.text]), [[0, 0, "lost-ack"], [0, 0, "lost-ack"]]); assert.equal(server.stage.parts.length, 1, "staged once");
    await p.submitSegment(); await until(() => !!p.round && !p.sendTask, "upload did not finish");
    assert.equal(calls("audio").length, 0); assert.equal(upload(p.round.metrics).client_upload_chunks, 2, "requests counted including the re-send");
    p.releaseMic();
  }
  {// The commit goes through but its ACK is lost: the state shows it, so nothing is sent twice.
    const loseCommitAck = {};
    const {b, p, calls, speak, settle} = make({loseCommitAck});
    await p.start(); const recorder = b.recorders[0];
    b.voice(); recorder.data("committed"); await speak(); await until(() => calls("audio-chunk").length === 1, "chunk");
    await p.submitSegment(); await until(() => !!p.round && !p.sendTask, "readback did not confirm");
    assert.equal(calls("audio-commit").length, 1); assert.equal(calls("audio").length, 0); assert.equal(p.apiFailure, null);
    p.releaseMic();
  }
  {// The commit is lost before the server stored it: unconfirmed, the clip is kept; the retry reads the state, then commits again.
    const failCommit = {};
    const {b, p, calls, speak, settle, audio} = make({failCommit});
    await p.start(); const recorder = b.recorders[0], clipId = p.recording.id;
    b.voice(); recorder.data("retry-me"); await speak(); await until(() => calls("audio-chunk").length === 1, "chunk");
    await p.submitSegment(); await settle();
    assert.equal(p.round, null); assert.ok(p.clip, "unconfirmed: the whole clip is kept"); assert.ok(p.apiFailure); assert.equal(p.clip.id, clipId); assert.equal(audio.length, 0);
    assert.equal(calls("audio-commit").length, 1); assert.equal(calls("audio").length, 0, "no blind whole upload after a lost commit");
    const reads = []; const fetch = p.e.fetch; p.e.fetch = async (url, init) => { reads.push(url.slice("/bot-call/".length)); return fetch(url, init); };
    assert.equal(await p.sendClip(), true);
    assert.deepEqual(reads.slice(0, 2), ["state", "audio-commit"], "retry: read the state first, then repeat the same commit"); assert.equal(calls("audio").length, 0); assert.equal(audio.length, 1);
    assert.ok(Number.isSafeInteger(upload(p.round.metrics).client_upload_precheck));
    p.releaseMic();
  }
  {// Segments take turns. A is dropped without commit: B starts from 0 at once and the server's staging is replaced.
    // Speculative chunks do not commit a capture whose final model and energy coverage both prove silence.
    const detector = {ready: () => true, async begin() { return {valid: true}; }, async finish() { return {valid: true, candidate: !!p.recording.voiced,speechMs:p.recording.voiced?192:0,speechBeforeControlMs:0}; }, fail() {}, close: async () => {}};
    const {b, p, calls, speak, settle, server} = make({vad: detector});
    b.silence(); await p.start(); const first = b.recorders[0], idA = p.recording.id, uploaderA = p.recording.uploader;
    b.silence(); first.data("drop-me"); uploaderA.arm(); await speak(); await until(() => calls("audio-chunk").length === 1, "A chunk");
    assert.equal(await p.submitSegment("auto_limit"), true); await settle();
    assert.equal(uploaderA.done, true, "a dropped segment gives the lane back"); assert.equal(p.clip, null); assert.equal(p.lane, null); assert.equal(calls("audio-commit").length, 0, "never committed");
    await until(() => b.recorders.length === 2 && !!p.recording, "B recording"); const second = b.recorders[1], idB = p.recording.id; assert.notEqual(idA, idB);
    b.voice(); second.data("beta"); await speak(); await until(() => calls("audio-chunk").length === 2, "B chunk did not start at once");
    assert.deepEqual(calls("audio-chunk").map(c => [c.id, c.seq, c.offset]), [[idA, 0, 0], [idB, 0, 0]], "A's pieces, then B at seq 0 of its own staging, no waiting");
    await p.submitSegment(); await until(() => !!p.round && !p.sendTask, "B upload did not finish");
    assert.equal(calls("audio-commit").length, 1); assert.equal(calls("audio-commit")[0].body.utterance_id, idB); assert.equal(calls("audio").length, 0); assert.equal(p.lane, null);
    assert.equal(Buffer.concat(server.stage.parts).toString(), "beta-tail🙂", "only B's bytes are in the staging");
    p.releaseMic();
  }
  {// A's commit is still in flight while B is already recording: B's chunks wait for A's result, recording is not delayed.
    const gateCommit = {entered: false}; gateCommit.promise = new Promise(resolve => { gateCommit.release = resolve; });
    const {b, p, calls, speak, settle, server} = make({gateCommit});
    const {ChunkUpload} = globalThis.pageModule;
    await p.start(); const recorder = b.recorders[0], idA = p.recording.id;
    b.voice(); recorder.data("alpha"); await speak(); await until(() => calls("audio-chunk").length === 1, "A chunk");
    const sendingA = p.submitSegment(); await until(() => gateCommit.entered, "A commit is in flight");
    const blob = text => new Blob([text], {type: "audio/webm;codecs=opus"}), laneB = new ChunkUpload(p, "segment-b", "audio/webm;codecs=opus");
    laneB.push(blob("b1")); laneB.arm(); laneB.push(blob("b2")); await settle();
    assert.equal(calls("audio-chunk").filter(c => c.id === "segment-b").length, 0, "B waits while A is not decided");
    assert.equal(laneB.pending.length + laneB.held.length >= 1 || !!laneB.loop, true, "B keeps its pieces queued");
    gateCommit.release(); await sendingA; await until(() => !!p.round, "A committed");
    await until(() => calls("audio-chunk").filter(c => c.id === "segment-b").length === 1, "B did not go after A's commit");
    assert.deepEqual(calls("audio-chunk").filter(c => c.id === "segment-b").map(c => [c.seq, c.offset, c.text]), [[0, 0, "b1b2"]]);
    assert.ok(calls("audio-commit")[0].body.utterance_id === idA && server.stage.id === "segment-b"); laneB.release(); assert.equal(p.lane, null);
    p.releaseMic();
  }
  {// A is abandoned while B waits behind it: B goes immediately. A third segment queues behind B.
    const {p, calls, settle} = make(); const {ChunkUpload} = globalThis.pageModule, blob = text => new Blob([text], {type: "audio/webm;codecs=opus"});
    const a = new ChunkUpload(p, "lane-a", "audio/webm;codecs=opus"), c = new ChunkUpload(p, "lane-c", "audio/webm;codecs=opus"), d = new ChunkUpload(p, "lane-b", "audio/webm;codecs=opus");
    a.push(blob("a")); a.arm(); await until(() => calls("audio-chunk").length === 1, "A");
    d.push(blob("b")); d.arm(); c.push(blob("c")); c.arm(); await settle(); assert.equal(calls("audio-chunk").length, 1, "B and C wait for A");
    a.abandon(); await until(() => calls("audio-chunk").some(x => x.id === "lane-b"), "B after A was dropped"); await settle();
    assert.equal(calls("audio-chunk").filter(x => x.id === "lane-c").length, 0, "C still waits for B"); d.release(); await until(() => calls("audio-chunk").some(x => x.id === "lane-c"), "C after B"); c.release(); assert.equal(p.lane, null);
  }
  {// Speech was never detected: the clip goes whole, exactly as before; nothing was streamed.
    const {b, p, calls, speak, settle} = make();
    await p.start(); const recorder = b.recorders[0], clipId = p.recording.id; b.silence();
    recorder.data("quiet"); await speak(); await settle();
    await p.submitSegment(); await until(() => !!p.round && !p.sendTask, "whole upload did not finish");
    assert.equal(calls("audio-chunk").length, 0); assert.equal(calls("audio-commit").length, 0); assert.deepEqual(calls("audio").map(c => [c.id, c.text]), [[clipId, "quiet-tail🙂"]]);
    assert.equal(upload(p.round.metrics).client_upload_chunks, undefined);
    p.releaseMic();
  }
}
async function captionPreferenceFacet() {
  const storage=new Map();const b=browser(null,{captionStorage:storage});
  b.nodes.subtitles.click();assert.equal(storage.get('bot-call-captions'),'0');
  const restored=browser(null,{captionStorage:storage});assert.equal(restored.nodes.conversation.hidden,true);
  restored.nodes.subtitles.click();assert.equal(storage.get('bot-call-captions'),'1');
  const blocked=browser(null,{brokenCaptionStorage:true});assert.equal(blocked.nodes.conversation.hidden,false);
  assert.doesNotThrow(()=>blocked.nodes.subtitles.click());assert.equal(blocked.nodes.conversation.hidden,true);assert.equal(blocked.permissions,0);
}
async function transientContinueFacet() {
  const b=browser(null),p=b.page;
  const state={audio_available:true,speech_available:true,audio:[],turns:[],segments:[],
    replies:[{utterance_id:'failed',generation_id:'reply:failed',state:'failed'}],
    failures:[{service:'chat',generation_id:'reply:failed',code:'unavailable'}]};
  p.started=true;p.reference='synthetic-reference';p.round={id:'failed',terminal:true};p.server=state;
  const routes=[];p.api=async route=>{routes.push(route);assert.equal(route,'state','recovery must not repeat a paid request');return state;};
  p.render();assert.equal(b.nodes['call-state'].textContent,'暂时没接上');
  assert(!b.nodes['call-hint'].textContent.includes('管理员'));assert.equal(b.nodes['main-label'].textContent,'出了一点问题，点击继续对话');
  b.nodes['main-action'].click();await until(()=>!!p.recording,'failed chat could not continue recording');
  assert.equal(p.round,null);assert.equal(b.permissions,1);assert(routes.length>0);
  await p.finishSegment();p.releaseMic();
}
async function preparedVADFacets() {
  let release,loads=0,attached=0,armed=false;
  const detector={prepareModel(){loads++;return new Promise(r=>{release=r;});},ready(){return armed;},
    async prepare(){attached++;armed=true;return true;},async begin(){return {valid:true};},
    async finish(){return {valid:true,candidate:true};},async close(){armed=false;}};
  const b=browser(null,{createVAD:()=>detector}),p=b.page;b.silence();
  const base=()=>({audio_available:true,speech_available:true,audio:[],turns:[],segments:[],replies:[]});
  p.reference='synthetic-reference';p.api=async route=>route==='state'?base():{recorded:true};
  assert.equal(loads,1);assert.equal(b.permissions,0);assert.equal(b.recorders.length,0);
  const start=p.start();await until(()=>b.permissions===1,'permission not requested on explicit start');
  await start;assert(p.recording,'model download must not delay energy capture');
  const graph=p.audioContext,source=p.analysisSource,analyser=p.analyser,gain=p.analysisGain;
  assert.equal(source.connections[0],analyser);assert.equal(analyser.connections[0],gain);
  assert.equal(gain.connections[0],graph.destination);assert.equal(gain.gain.value,0);assert(graph.pulled);
  b.clock.advance(100);release(true);await p.modelPreparation;
  assert.equal(p.recording.vad,null);assert.equal(b.nodes.status.textContent,'可以说话');assert.equal(attached,0);
  await p.finishSegment('manual');assert.equal(p.clip.metrics.client_segment_manual,1);
  assert.equal(p.clip.metrics.client_audio_state_start,1);assert.equal(p.clip.metrics.client_audio_state_end,1);
  assert(p.clip.metrics.client_energy_frames>=2);assert.equal(p.clip.metrics.client_energy_first_ms,undefined,'silence is not a first nonzero observation');
  assert.equal(p.preparationMetrics.client_model_lag_ms,0);
  p.clip=null;p.releaseMic();
  assert(source.disconnected && analyser.disconnected && gain.disconnected);assert.equal(graph.state,'closed');
  assert.equal(p.analysisSource,null);assert.equal(p.analysisGain,null);
  await p.start();assert.equal(loads,1);assert.equal(attached,1);assert(p.recording.vad.valid);
  await p.finishSegment('auto_silence');assert.equal(p.clip.metrics.client_segment_auto,1);p.releaseMic();
  for(const readyDuringSpeech of [false,true]) {
    let ready;const cold=browser(null,{createVAD:()=>({prepareModel:()=>new Promise(r=>{ready=r;}),ready:()=>false,close:async()=>{}})});
    const c=cold.page;c.reference='synthetic-reference';c.api=async route=>route==='state'?base():{recorded:true};cold.silence();await c.start();
    let reason;c.submitSegment=async value=>{reason=value;await c.finishSegment(value);};
    // Three voice samples separated by sub-threshold syllable gaps never formed 300ms continuously.
    for(let i=0;i<3;i++){cold.level(.06);cold.clock.advance(100);cold.silence();cold.clock.advance(100);}
    if(readyDuringSpeech){ready(true);await c.modelPreparation;}
    for(let i=0;i<10;i++)cold.clock.advance(100);
    assert.equal(reason,undefined,"must wait a full 1.2s since last voice");
    cold.clock.advance(100);await until(()=>reason==='auto_energy'&&c.clip,"first segment never energy-endpointed");
    assert.equal(c.clip.metrics.client_vad_first_fallback,1);assert.equal(c.clip.metrics.client_segment_auto_energy,1);
    assert.equal(c.clip.metrics.client_energy_first_ms,100);assert(c.clip.metrics.client_energy_frames>=15);
    assert.equal(c.clip.metrics.client_audio_state_start,1);assert.equal(c.clip.metrics.client_audio_state_end,1);
    const round={id:c.clip.id,metrics:c.clip.metrics};c.server.replies=[{generation_id:'reply:'+round.id}];
    let posted;c.api=async(route,body)=>{assert.equal(route,'metrics');posted=body.values;return {recorded:true};};
    c.sendMetrics(round);await round.metricsTask;
    assert.equal(posted.client_energy_first_ms,100);assert.equal(posted.client_audio_state_end,1);c.releaseMic();
    if(!readyDuringSpeech){ready(false);await c.modelPreparation;}
  }
  // A model/CPU scheduling gap cannot erase voice already observed. Missing
  // audio is retained, and a fresh full quiet interval is still required.
  {
    let done;const gap=browser(null,{createVAD:()=>({prepareModel:()=>new Promise(r=>{done=r;}),ready:()=>false,close:async()=>{}})});
    const c=gap.page;c.reference='synthetic-reference';c.api=async route=>route==='state'?base():{recorded:true};gap.silence();await c.start();
    let reason;c.submitSegment=async value=>{reason=value;await c.finishSegment(value);};
    gap.level(.1);for(let i=0;i<4;i++)gap.clock.advance(100);
    assert.equal(c.recording.energyVoiced,true);gap.silence();gap.clock.advance(1500);
    assert.equal(c.recording.energyVoiced,true);assert.equal(c.recording.energyValid,false);assert.equal(reason,undefined);
    done(true);await c.modelPreparation;assert.equal(c.recording.vad,null);
    gap.clock.advance(100);assert.equal(c.recording.level,0);assert.equal(gap.nodes['voice-level'].value,0,'q.valid=false must not freeze the meter');
    for(let i=0;i<10;i++)gap.clock.advance(100);assert.equal(reason,undefined,'gap is not evidence of silence');
    gap.clock.advance(100);await until(()=>c.clip,'voice latch lost during model scheduling gap');
    assert.equal(reason,'auto_energy');assert.equal(c.clip.quality.valid,false);assert(c.clip.blob.size>0);c.releaseMic();
  }
  // Stable fan-like floors do not arm a cold segment, but actual voice
  // above each floor does. Keeping the floor after voice must not delay stop.
  for(const floor of [.002,.008,.018]) {
    const noise=browser(null,{createVAD:()=>({prepareModel:async()=>false,ready:()=>false,close:async()=>{}})}),c=noise.page;
    noise.silence();c.reference='synthetic';c.api=async route=>route==='state'?base():{recorded:true};await c.start();
    let reason;c.submitSegment=async value=>{reason=value;await c.finishSegment(value);};
    for(let i=0;i<12;i++){noise.level(floor*(i%2?1.05:.95));noise.clock.advance(100);}
    assert.equal(c.recording.energyVoiced,undefined);assert.equal(reason,undefined);
    noise.level(.08);for(let i=0;i<10;i++)noise.clock.advance(100);
    assert(c.recording.energyVoiced);
    noise.level(floor);for(let i=0;i<12;i++)noise.clock.advance(100);
    await until(()=>c.clip,'stationary noise prevented cold endpoint');assert.equal(reason,'auto_energy');c.releaseMic();
  }
  // Drift is measured only while model preparation is pending and visible.
  for(const hidden of [false,true]) {
    let done;const lag=browser(null,{createVAD:()=>({prepareModel:()=>new Promise(r=>{done=r;}),close:async()=>{}})});
    lag.doc.hidden=hidden;lag.clock.advance(750);done(false);await lag.page.modelPreparation;
    assert.equal(lag.page.preparationMetrics.client_model_lag_ms,hidden?undefined:650);
    assert.equal(lag.page.modelWatch,null);
  }
  const fast=browser(null,{createVAD:()=>({prepareModel:async()=>true,close:async()=>{}})});
  await fast.page.modelPreparation;assert.equal(fast.page.preparationMetrics.client_model_lag_ms,undefined,'no callback observed, do not invent zero');
  const bad=browser(null,{createVAD:()=>({prepareModel:async()=>false,ready:()=>false,close:async()=>{}})});
  bad.page.reference='synthetic-reference';bad.page.api=async route=>route==='state'?base():{recorded:true};
  bad.silence();await bad.page.start();assert(bad.page.recording);assert.equal(bad.page.recording.vad,null);
  assert.equal(bad.nodes.status.textContent,'可以说话');
  let submitted;bad.page.submitSegment=async reason=>{submitted=reason;await bad.page.finishSegment(reason);};
  // Even short energy ends an unavailable-VAD segment; retention remains
  // the separate gate's decision, never a manufactured voice latch.
  bad.level(.1);bad.clock.advance(100);assert(!bad.page.recording.energyVoiced);
  bad.silence();for(let i=0;i<13;i++)bad.clock.advance(100);
  await until(()=>submitted==='auto_energy' && !!bad.page.clip,'energy fallback did not submit');
  assert.equal(bad.page.clip.metrics.client_segment_auto_energy,1);assert.equal(bad.page.clip.metrics.client_segment_auto,undefined);
  bad.page.releaseMic();
  // A failed VAD hands confirmed short speech to covered energy endpointing,
  // including speech that never reached energy's 300ms arming threshold.
  for(const heard of [true,false,'energy']) {
    let event,signal,reason;
    const d={ready:()=>true,async begin(o,fn){event=fn;return signal={valid:true,voiced:heard===true,candidate:heard===true};},
      async finish(){return {valid:false,candidate:heard};},close:async()=>{}};
    const b=browser(null,{createVAD:()=>d}),p=b.page;b.silence();
    p.started=true;p.reference='synthetic';p.server=base();await p.listen();
    p.submitSegment=async value=>{reason=value;await p.finishSegment(value);};
    for(let i=0;i<10;i++)b.clock.advance(100);
    signal.valid=false;event('unavailable');
    if(heard==='energy'){b.level(.004);b.clock.advance(100);b.silence();}
    assert(!p.recording.energyVoiced);
    for(let i=0;i<11;i++)b.clock.advance(100);
    assert.equal(reason,undefined,'energy fallback borrowed quiet before VAD failure');
    b.clock.advance(100);await Promise.resolve();
    assert.equal(reason,heard?'auto_energy':undefined,'failed VAD lost confirmed voice or invented it');
    if(p.recording)await p.finishSegment('control');p.releaseMic();
  }
}
async function localVADFacets() {
  // Real candidate accumulator: gaps contain neither positive nor negative
  // frames. Only bounded known loss may bridge an unfinished short utterance.
  {
    const p=browser().page,playback=p.bargePlayback;p.maintainBargeInput=()=>{};p.bargeEnabled=()=>true;p.bargePlayback=()=>{};
    let edge=0;
    const frame=(vad=.9,gapMs=undefined,rate=48000)=>{
      const start=edge+(Number.isFinite(gapMs)?gapMs*48:0);edge=start+1536;
      p.bargeFrame({vad,valid:512,start,end:edge,rate,gap:gapMs!==undefined,gapMs});
    };
    for(const missing of [0,20,50,51,1000,Infinity]) {
      p.bargeRun=null;edge=0;frame();frame();frame();frame(.9,missing);frame();
      assert.equal(p.bargeRun.count,missing<=50?5:2,'candidate gap budget');
    }
    p.bargeRun=null;edge=0;frame();
    for(let i=0;i<4;i++)frame(.9,50);
    assert.equal(p.bargeRun.count,5);frame(.9,1);assert.equal(p.bargeRun.count,1,'cumulative loss joined unrelated speech');
    frame();frame(.1,20);assert.equal(p.bargeRun,null,'negative probability bridged speech');
    frame();frame(.9,undefined,44100);assert.equal(p.bargeRun.count,1,'rate change bridged speech');
    p.pcmContext={currentTime:.2};p.playbackGain={};p.speech={playing:true,seq:1,pcm:{}};
    p.echo={gapSerial:1};p.bargeRun={count:5,start:0,end:9600,rate:48000,gapSerial:0};
    let started=0;p.beginBarge=async()=>{started++;};
    playback.call(p);assert.equal(started,0,'queued pre-gap inference triggered playback');
    p.bargeRun.gapSerial=1;playback.call(p);assert.equal(started,1);
    // Long confirmation sums actual high-confidence samples, not gap duration.
    const r={id:'long',vad:{valid:true,rate:48000,recordStart:0,highSpans:[]}};
    const b=p.barge=r.barge={};p.closeBarge=reason=>{b.localClosed=reason;};p.show=()=>{};
    let start=0;
    for(let i=0;i<37;i++){r.vad.highSpans.push([start,start+1536,512]);start+=1536+(i===20?2400:0);}
    p.promoteLongBarge(r);assert.equal(b.voiceMs,1184);assert(!b.longConfirmed);
    r.vad.highSpans.push([start,start+1536,512]);p.promoteLongBarge(r);assert.equal(b.voiceMs,1216);assert(b.longConfirmed);
    assert.equal(b.localClosed,"long_speech");
    b.longConfirmed=false;b.localClosed=null;r.vad.longBreak=start;p.promoteLongBarge(r);
    assert.equal(b.voiceMs,32);assert(!b.longConfirmed,'large loss retained prior long speech');
  }
  const {SileroSignalModel,LocalVAD,validateVAD,VAD_MAX_FRAMES}=globalThis.vadModule;
  assert.equal(VAD_MAX_FRAMES,16);
  assert.equal(nativeFrame({currentTime:128/48000,sampleRate:48000},44100),118);
  assert.equal(nativeFrame({currentTime:260736/48000,sampleRate:48000}),260736);
  for(const clock of [{currentTime:.25/48000,sampleRate:48000},{currentTime:NaN,sampleRate:48000},
      {currentTime:Infinity,sampleRate:48000},{currentTime:0,sampleRate:NaN},
      {currentTime:-1,sampleRate:48000},{currentTime:1e9,sampleRate:48000}]) assert.equal(nativeFrame(clock),null);
  const low=Array.from({length:1000},(_,i)=>i+1).find(n=>n/48000*48000<n);
  assert(low);assert.equal(nativeFrame({currentTime:low/48000,sampleRate:48000}),low);

  const options={vadPositive:.3,vadNegative:.25,vadRedemptionMs:1200,vadMinSpeechMs:32};
  assert.throws(()=>validateVAD({...options,vadMinSpeechMs:0}));
  assert.throws(()=>validateVAD({...options,vadNegative:.4}));
  // Actual adapted model, synthetic tensors: one real run and exact tensor ownership.
  const tensors=[];let release=0,runs=0,finishRun;
  class Tensor {
    constructor(type,data,dims){this.type=type;this.data=data;this.dims=dims;tensors.push(this);}
    dispose(){assert.equal(this.dead,undefined,"tensor double disposed");this.dead=true;}
  }
  const ort={Tensor,env:{wasm:{}},InferenceSession:{async create(){return {async run(){runs++;await new Promise(r=>{finishRun=r;});return {stateN:new Tensor('float32',new Float32Array(256),[2,1,128]),output:new Tensor('float32',[.4],[1])};},async release(){release++;}};}}};
  const model=await SileroSignalModel.create(ort,new Uint8Array());
  model.reset();const actual=model.run(new Float32Array(512));
  await assert.rejects(model.run(new Float32Array(512)));assert.throws(()=>model.reset());
  const close=model.close();assert.equal(release,0);finishRun();await actual;await close;
  assert.equal(runs,1);assert.equal(release,1);assert(tensors.every(t=>t.dead),"owned model tensors leaked");
  // Actual worklet source. Credit bounds include the port, with explicit silent output.
  const messages=[];let Processor;
  const scope={AudioWorkletProcessor:class {constructor(){this.port={onmessage:null,postMessage:m=>messages.push(m)};}},
    registerProcessor:(name,value)=>{Processor=value;},sampleRate:48000,currentFrame:0,Float32Array,Number,Set};
  vm.runInNewContext(Buffer.from(fixture.worklet,'base64').toString(),scope);
  const work=new Processor(),out=new Float32Array(128).fill(1);
  work.port.onmessage({data:{type:'begin',id:1,maxFrames:VAD_MAX_FRAMES}});
  work.process([[]],[[out]]);assert.equal(messages.length,0,'empty pre-record graph fabricated coverage');
  for(let i=0;i<210;i++){scope.currentFrame=i*128;work.process([[new Float32Array(128).fill(.01)]],[[out]]);assert(out.every(v=>v===0));}
  assert.equal(messages.filter(m=>m.type==='frame').length,16);
  assert.equal(messages.find(m=>m.type==='begun').maxFrames,VAD_MAX_FRAMES);
  assert.equal(messages.filter(m=>m.type==='fault').length,0);assert(messages.some(m=>m.type==='gap' && m.reason==='overflow'));
  work.port.onmessage({data:{type:'flush',id:1,end:scope.currentFrame+128}});
  assert.equal(messages.at(-1).type,'flushed');assert.equal(messages.at(-1).bad,false);
  messages.length=0;scope.currentFrame+=128;work.port.onmessage({data:{type:'begin',id:2,maxFrames:VAD_MAX_FRAMES}});
  work.process([[new Float32Array(128).fill(.1)]],[[out]]);
  scope.currentFrame+=128;work.process([[new Float32Array(128).fill(.1)]],[[out]]);
  work.port.onmessage({data:{type:'flush',id:2,end:scope.currentFrame+128}});
  const tail=messages.find(m=>m.type==='frame'),end=messages.at(-1);
  assert(tail.valid>0&&tail.valid<512);assert.equal(tail.end-tail.start,256);assert.equal(end.end,tail.end);
  scope.currentFrame+=128;work.port.onmessage({data:{type:'begin',id:3,maxFrames:VAD_MAX_FRAMES}});
  work.process([[new Float32Array(128)]],[[out]]);
  work.port.onmessage({data:{type:'flush',id:3,end:scope.currentFrame+129}});
  assert.equal(messages.at(-1).bad,true,'unknown tail was invented from padding');
  scope.currentFrame+=128;work.port.onmessage({data:{type:'begin',id:4,maxFrames:VAD_MAX_FRAMES}});
  work.process([[new Float32Array(128),new Float32Array(128)]],[[out]]);
  assert.equal(messages.at(-1).reason,'channels');
  work.port.onmessage({data:{type:'flush',id:4,end:scope.currentFrame+128}});
  scope.currentFrame+=128;work.port.onmessage({data:{type:'begin',id:5,maxFrames:VAD_MAX_FRAMES}});
  work.process([[new Float32Array(128).fill(NaN)]],[[out]]);
  assert.equal(messages.at(-1).reason,'sample',"invalid worklet PCM was accepted");
  messages.length=0;scope.currentFrame+=128;work.port.onmessage({data:{type:'flush',id:5,end:scope.currentFrame}});
  work.port.onmessage({data:{type:'begin',id:6,maxFrames:16}});work.process([[new Float32Array(128)]],[[out]]);
  scope.currentFrame+=4928;work.process([[new Float32Array(128)]],[[out]]);
  for(let i=0;i<13;i++){scope.currentFrame+=128;work.process([[new Float32Array(128)]],[[out]]);}
  assert(messages.some(m=>m.type==='gap'));assert(messages.some(m=>m.type==='frame' && m.id===6));assert(!messages.some(m=>m.type==='fault' && m.id===6));
  scope.currentFrame+=30000;work.process([[new Float32Array(128)]],[[out]]);assert(messages.some(m=>m.type==='fault' && m.id===6 && m.reason==='clock'));
  work.port.onmessage({data:{type:'flush',id:6,end:scope.currentFrame+128}});
  messages.length=0;
  scope.currentFrame+=128;
  work.port.onmessage({data:{type:'begin',id:7,maxFrames:16,settle:true}});
  work.process([[new Float32Array(128)]],[[out]]);
  scope.currentFrame+=109968; // Same 2291ms startup discontinuity as phone evidence.
  for(let i=0;i<24;i++){work.process([[new Float32Array(128)]],[[out]]);scope.currentFrame+=128;}
  assert(messages.some(m=>m.type==='begun' && m.id===7));
  assert(!messages.some(m=>['gap','fault'].includes(m.type)),'pre-recorder startup invalidated VAD');
  assert(messages.some(m=>m.type==='startup-gap'&&m.count===1&&m.skippedMs===2291),'startup skip was not separately observable');
  scope.currentFrame+=109968;
  work.process([[new Float32Array(128)]],[[out]]);
  assert(messages.some(m=>m.type==='fault' && m.reason==='clock'),'recorded loss became valid');
  work.port.onmessage({data:{type:'shutdown'}});assert.equal(work.process([],[[out]]),false);
  // Real analysis worklet: bounded reverse clocks discard overlap only,
  // including the old failure exactly at a complete frame boundary.
  for(const rate of [44100,48000,96000])for(const backMs of [3,20,21]) {
    const received=[];let C;
    const e={AudioWorkletProcessor:class {constructor(){this.port={postMessage:m=>received.push(m)};}},
      registerProcessor:(name,value)=>{C=value;},sampleRate:rate,currentFrame:0,Float32Array,Number,Set};
    vm.runInNewContext(Buffer.from(fixture.worklet,'base64').toString(),e);
    const w=new C(),input=new Float32Array(128).fill(.25),output=new Float32Array(128);
    w.port.onmessage({data:{type:'begin',id:1,maxFrames:16}});
    const step=()=>{w.process([[input]],[[output]]);e.currentFrame+=128;
      for(const m of received)if(m.type==='frame')w.port.onmessage({data:{type:'ack',id:1,seq:m.seq}});};
    while(!received.some(m=>m.type==='frame'))step();
    const edge=w.active.expected;e.currentFrame-=Math.floor(rate*backMs/1000);step();
    if(backMs>20){assert(received.some(m=>m.type==='fault'&&m.reason==='clock'));continue;}
    assert(!received.some(m=>m.type==='fault'));assert(w.active.expected>=edge);
    for(let i=0;i<40;i++)step();
    const gaps=received.filter(m=>m.type==='gap'),frames=received.filter(m=>m.type==='frame');
    assert(gaps.length>0&&gaps.at(-1).lostMs>=backMs-1);
    assert(frames.length>=2);for(let i=1;i<frames.length;i++)assert(frames[i].start>=frames[i-1].end);
    assert(frames.every(m=>new Float32Array(m.samples).subarray(0,m.valid).every(v=>v===.25)),'overlap became fabricated silence');
    assert(!received.some(m=>m.type==='fault'),'bounded reverse failed after catch-up');
  }
  // Actual adapter serial queue, coverage, short candidate and endpoint signals.
  const env={setTimeout,clearTimeout,AbortController},events=[],acks=[];
  const detector=new LocalVAD(env),context={sampleRate:48000,state:'running'};
  let gate,active=0,peak=0,calls=0;
  detector.model={reset(){},async run(){active++;peak=Math.max(peak,active);calls++;if(gate)await gate;active--;return calls===1?.4:0;},async close(){release++;}};
  detector.context=context;detector.node={port:{postMessage:m=>acks.push(m),close(){}},disconnect(){}};
  async function begin() {
    const task=detector.begin(options,event=>events.push(event));const s=detector.active;
    detector.message({type:'begun',id:s.id,start:0,rate:48000,maxFrames:VAD_MAX_FRAMES});await task;s.recordStart=0;return s;
  }
  function frame(s,seq){detector.message({type:'frame',id:s.id,seq,start:(seq-1)*1536,end:seq*1536,valid:512,samples:new Float32Array(512).buffer});}
  let s=await begin();
  detector.message({type:'startup-gap',id:s.id,count:1,skippedMs:2291});
  assert.equal(s.metrics.startGapCount,1);assert.equal(s.metrics.startSkippedMs,2291);
  assert.equal(s.metrics.gapCount,0);assert.equal(s.valid,true);
  for(let i=1;i<=40;i++){frame(s,i);await until(()=>!s.drain,'VAD drain');}
  assert(events.includes('candidate'));assert.equal(events.filter(e=>e==='endpoint').length,1);
  s.retentionEnd=768; // The control cutoff may split a positive frame.
  const done=detector.finish(s,40*1536);detector.message({type:'flushed',id:s.id,lastSeq:40,start:0,end:40*1536,bad:false});
  assert.deepEqual(await done,{valid:true,candidate:true,coverageGap:false,speechMs:32,highConfidenceMs:0,highConfidenceFrames:0,peakProbability:.4,speechBeforeControlMs:16,lastEvidenceMs:1248});assert.equal(peak,1);
  s=await begin();frame(s,1);await until(()=>!s.drain,'pre-gap inference');
  detector.message({type:'gap',id:s.id,start:6336,count:1,lostMs:100,reason:'clock'});
  detector.message({type:'frame',id:s.id,seq:2,start:6336,end:7872,valid:512,samples:new Float32Array(512).buffer});
  await until(()=>!s.drain,'post-gap inference');assert.equal(s.valid,true);assert.equal(s.metrics.gapCount,1);assert.equal(s.metrics.lostMs,100);
  const gapDone=detector.finish(s,7872);detector.message({type:'flushed',id:s.id,lastSeq:2,start:0,end:7872,bad:false});assert.equal((await gapDone).valid,true);
  // Confirmed speech survives a bounded post-speech gap. Quiet must be fully
  // covered AFTER that gap; a never-voiced gap must not invent a submission.
  for(const heard of [true,false]) {
    events.length=0;calls=heard?0:1;s=await begin();frame(s,1);await until(()=>!s.drain,'endpoint pre-gap');
    assert.equal(s.voiced,heard);
    detector.message({type:'gap',id:s.id,start:6336,count:1,lostMs:100,reason:'clock'});
    for(let seq=2;seq<=40;seq++) {
      const start=6336+(seq-2)*1536;
      detector.message({type:'frame',id:s.id,seq,start,end:start+1536,valid:512,samples:new Float32Array(512).buffer});
      await until(()=>!s.drain,'endpoint post-gap');
      if(seq<39)assert(!events.includes('endpoint'),'endpoint crossed unobserved gap');
    }
    assert.equal(events.filter(e=>e==='endpoint').length,heard?1:0,'post-speech gap lost endpoint');
    assert.equal(s.valid,true);assert.equal(s.coverageGap,true);assert.equal(s.voiced,heard);
    detector.active=null;
  }
  // Unconfirmed positive duration also pauses over bounded loss; big or
  // cumulative loss resets it without deleting the retained high-span history.
  {
    const originalModel=detector.model;detector.model={reset(){},async run(){return .9;}};
    for(const missing of [0,20,50,51]) {
      s=await begin();s.options={...s.options,vadMinSpeechMs:96};frame(s,1);await until(()=>!s.drain,'small gap before');
      const start=1536+missing*48;
      detector.message({type:'gap',id:s.id,start,count:1,lostMs:missing,reason:'clock'});
      detector.message({type:'frame',id:s.id,seq:2,start,end:start+1536,valid:512,samples:new Float32Array(512).buffer});
      await until(()=>!s.drain,'small gap after');
      assert.equal(s.positiveMs,missing<=50?64:32);assert.equal(s.highSpans.length,2);
      assert.equal(s.longBreak,missing<=50?undefined:start);assert(!s.voiced);detector.active=null;
    }
    s=await begin();s.options={...s.options,vadMinSpeechMs:300};frame(s,1);await until(()=>!s.drain,'cumulative gap before');
    let end=1536;
    for(let count=1;count<=5;count++) {
      const start=end+2400;end=start+1536;
      detector.message({type:'gap',id:s.id,start,count,lostMs:count*50,reason:'clock'});
      detector.message({type:'frame',id:s.id,seq:count+1,start,end,valid:512,samples:new Float32Array(512).buffer});
      await until(()=>!s.drain,'cumulative gap after');
      assert.equal(s.positiveMs,count<=4?(count+1)*32:32);
    }
    detector.active=null;s=await begin();frame(s,1);await until(()=>!s.drain,'adjacent gap before');
    detector.message({type:'gap',id:s.id,start:2736,count:1,lostMs:25,reason:'clock'});
    detector.message({type:'gap',id:s.id,start:3984,count:2,lostMs:51,reason:'clock'});
    detector.message({type:'frame',id:s.id,seq:2,start:3984,end:5520,valid:512,samples:new Float32Array(512).buffer});
    await until(()=>!s.drain,'adjacent gap after');assert.equal(s.positiveMs,32,'adjacent gaps evaded single-loss bound');
    for(const lost of [50,51]) {
      detector.active=null;s=await begin();frame(s,1);await until(()=>!s.drain,'overlap before');
      detector.message({type:'gap',id:s.id,start:1536,count:1,lostMs:lost,reason:'clock'});
      frame(s,2);await until(()=>!s.drain,'overlap after');
      assert.equal(s.positiveMs,lost<=50?64:32,'discarded overlaps escaped continuity budget');
    }
    detector.active=null;detector.model=originalModel;
  }
  s=await begin();detector.message({type:'gap',id:s.id,start:30000,count:1,lostMs:625,reason:'clock'});assert.equal(s.valid,false);detector.active=null;
  const otherRate=detector.begin(options,()=>{});s=detector.active;detector.message({type:'begun',id:s.id,start:0,rate:44100,maxFrames:16});await otherRate;assert.equal(s.valid,true);assert.equal(s.rate,44100);detector.active=null;
  let releaseGate;gate=new Promise(resolve=>{releaseGate=resolve;});s=await begin();
  const recovered=s;const previousCalls=calls;
  for(let seq=1;seq<=7;seq++)frame(s,seq); // 224ms source audio, no dropped coverage.
  assert.equal(s.queue.length,6);assert.equal(s.valid,true);assert.equal(active,1);
  assert.equal(acks.filter(m=>m.type==='ack'&&m.id===s.id).length,0,"ACK before actual processing exit");
  const perf=Object.getOwnPropertyDescriptor(globalThis,"performance");
  Object.defineProperty(globalThis,"performance",{configurable:true,value:{now(){throw new Error("synthetic clock unavailable");}}});
  try {releaseGate();await until(()=>!s.drain,'short queue recovery');}finally{Object.defineProperty(globalThis,"performance",perf);}
  gate=null;assert.equal(s.valid,true);assert.equal(calls-previousCalls,7);assert.equal(s.processed,7);assert.equal(peak,1);
  assert.deepEqual(acks.filter(m=>m.type==='ack'&&m.id===s.id).map(m=>m.seq),[1,2,3,4,5,6,7]);
  const recoveredFinish=detector.finish(s,7*1536);detector.message({type:'flushed',id:s.id,lastSeq:7,start:0,end:7*1536,bad:false});
  assert.equal((await recoveredFinish).valid,true);
  gate=new Promise(resolve=>{releaseGate=resolve;});s=await begin();
  for(let seq=1;seq<=16;seq++)frame(s,seq);
  assert.equal(s.queue.length,15);assert.equal(s.valid,true);assert.equal(s.metrics.queuePeak,15);frame(s,17);
  assert.equal(s.queue.length,15);assert.equal(s.valid,false);assert.equal(active,1);
  assert.deepEqual(s.diagnostic,{stage:"queue",reason:"queue"});
  detector.message({type:'fault',id:s.id,reason:'overflow',unacked:16});assert.equal(s.metrics.overflowFrames,16);
  s.event=()=>{throw new Error('synthetic view failure');};s.valid=true;
  detector.node.port.postMessage=()=>{throw new Error('synthetic closed port');};
  detector.node.disconnect=()=>{throw new Error('synthetic node failure');};
  detector.node.port.close=()=>{throw new Error('synthetic port failure');};
  const stopped=detector.close();assert.equal(active,1,"close pretended inference exited");releaseGate();await stopped;assert.equal(active,0);
  assert.equal(detector.model,null);assert.equal(release,2,"auxiliary cleanup interrupted model release");
  // The recurrent model and native voice clock survive a limit handoff, but
  // candidate retention still belongs solely to the new segment.
  const carry=new LocalVAD(env);let resets=0;
  carry.model={reset(){resets++;},async run(){return .9;}};carry.context=context;
  const carryMessages=[];carry.node={port:{postMessage(m){carryMessages.push(m);}}};
  const first=carry.begin(options,()=>{});assert(carryMessages.at(-1).settle);let a=carry.active;
  carry.message({type:'begun',id:a.id,start:1440000,rate:48000,maxFrames:16});await first;a.recordStart=1440000;
  carry.message({type:'frame',id:a.id,seq:1,start:1440000,end:1441536,valid:512,samples:new Float32Array(512).buffer});
  await until(()=>!a.drain,'carry inference');
  const finish=carry.finish(a,1441536,true);carry.message({type:'flushed',id:a.id,lastSeq:1,start:1440000,end:1441536,bad:false});const high=await finish;assert(high.valid);assert.equal(high.highConfidenceFrames,1);assert.equal(high.highConfidenceMs,32);
  const next=carry.begin(options,()=>{},true);assert.equal(carryMessages.at(-1).settle,false,'continuous recorder handoff added a settle gap');a=carry.active;carry.message({type:'begun',id:a.id,start:1442000,rate:48000,maxFrames:16});await next;
  assert.equal(resets,1);assert.equal(a.candidate,false);assert.equal(a.voiced,true);assert.equal(a.lastCandidate,1441536);assert.deepEqual(a.speechSpans,[],"retention must not inherit the previous recorder's positives");
  carry.active=null;await carry.close(true);assert.equal(carry.continuation,null);
  // Each original await projects one fixed preparation phase, without listeners.
  const warmOutput=()=>({stateN:new Tensor('float32',new Float32Array(256),[2,1,128]),output:new Tensor('float32',[0],[1])});
  let runtimeDone,fetchDone,bufferDone,modelDone,warmDone,workletDone,nodeOptions,warmRuns=0;
  const preparedSession={async run(feeds){warmRuns++;assert.equal(feeds.input.data.length,576);assert(feeds.input.data.every(x=>x===0));await new Promise(r=>{warmDone=r;});return warmOutput();},async release(){}};
  const phaseOrt={...ort,InferenceSession:{create:()=>new Promise(r=>{modelDone=()=>r(preparedSession);})}};
  const phaseCtx={state:'running',sampleRate:48000,destination:{},
    audioWorklet:{addModule:()=>new Promise(r=>{workletDone=r;})},
    createGain:()=>({gain:{},connect(){},disconnect(){}}),
    createMediaStreamSource:()=>({connect(){},disconnect(){}})};
  const phase=new LocalVAD({...env,AudioWorkletNode:class {constructor(c,name,options){assert.equal(c,phaseCtx);assert.equal(name,"bot-call-voice-signal");nodeOptions=options;this.port={postMessage(){},close(){}};}connect(){}disconnect(){}},
    fetch:()=>new Promise(r=>{fetchDone=()=>r({ok:true,arrayBuffer:()=>new Promise(r=>{bufferDone=()=>r(new ArrayBuffer(2327524));})});})},
    ()=>new Promise(r=>{runtimeDone=()=>r(phaseOrt);}));
  phase.w.location={origin:'https://calls.example.test'};
  phase.w.performance={getEntriesByType:()=>{const t=Math.ceil(performance.now()),base=phase.w.location.origin+'/bot-call/vendor/vad-v5-ort-1.22.0-2623a295/';return [
    {name:base+'runtime.js',startTime:t,responseEnd:t+20},{name:base+'model.onnx',startTime:t+10,responseEnd:t+30},
    {name:base+'runtime.wasm',startTime:t+40,responseEnd:t+50},
    {name:base+'old.js',startTime:0,responseEnd:99999},{name:'https://elsewhere.test/private',startTime:t,responseEnd:t+99999}];}};
  const phased=phase.prepare({},phaseCtx);
  assert.deepEqual(phase.diagnostic,{stage:'runtime',reason:'preparing'});
  runtimeDone();await until(()=>fetchDone,'resource phase');assert.deepEqual(phase.diagnostic,{stage:'resource',reason:'preparing'});
  fetchDone();await until(()=>bufferDone,'resource read phase');assert.deepEqual(phase.diagnostic,{stage:'resource',reason:'preparing'});
  bufferDone();await until(()=>modelDone,'model phase');assert.deepEqual(phase.diagnostic,{stage:'model',reason:'preparing'});
  modelDone();await until(()=>warmDone,'warmup phase');assert.deepEqual(phase.diagnostic,{stage:'warmup',reason:'preparing'});
  assert.equal(phase.ready(phaseCtx),false);assert.equal(nodeOptions,undefined);warmDone();
  await until(()=>workletDone,'worklet phase');assert.deepEqual(phase.diagnostic,{stage:'worklet',reason:'preparing'});
  workletDone();assert.equal(await phased,true);assert.deepEqual(phase.diagnostic,{stage:'ready',reason:'ready'});
  assert.deepEqual(nodeOptions,{numberOfInputs:1,numberOfOutputs:1,outputChannelCount:[1],
    channelCount:1,channelCountMode:"explicit",channelInterpretation:"speakers"});
  assert.equal(await phase.prepare({},phaseCtx),true);assert.equal(warmRuns,1,"second prepare warmed the same model twice");
  assert(phase.model.state.data.every(x=>x===0));assert(phase.model.context.every(x=>x===0));
  assert.equal(phase.metrics.resourceMs,40);assert(Number.isSafeInteger(phase.metrics.sessionMs));
  await phase.close();
  // Closing during actual warmup must retain and await its one real run/session.
  let warmRelease,closedWarm=0;
  const closeWarmOrt={...ort,InferenceSession:{async create(){return {async run(){await new Promise(r=>{warmRelease=r;});return warmOutput();},async release(){closedWarm++;}};}}};
  const warmEnv={...env,AudioWorkletNode:class {},fetch:async()=>({ok:true,arrayBuffer:async()=>new ArrayBuffer(2327524)})};
  const warmingModel=new LocalVAD(warmEnv,async()=>closeWarmOrt);const warmingTask=warmingModel.prepare({},phaseCtx);
  await until(()=>warmRelease,'close warmup gate');let closeDone=false;
  const realClose=warmingModel.close().then(()=>{closeDone=true;});await Promise.resolve();
  assert.equal(closedWarm,0);assert.equal(closeDone,false);warmRelease();
  assert.equal(await warmingTask,false);await realClose;assert.equal(closedWarm,1);assert.equal(warmingModel.model,null);
  const failedWarm=new LocalVAD(warmEnv,async()=>({...ort,InferenceSession:{async create(){return {async run(){throw new Error('warmup unavailable');},async release(){}};}}}));
  assert.equal(await failedWarm.prepare({},phaseCtx),false);assert.deepEqual(failedWarm.diagnostic,{stage:'warmup',reason:'inference'});
  assert.equal(failedWarm.ready(phaseCtx),false);await failedWarm.close();
  // A settled false after each original await is a fixed failure, never still preparing.
  for(const stage of ['resource','model','worklet']) {
    let resume,closed=0;
    const gate=()=>new Promise(r=>{resume=r;});
    const inactive={...phaseCtx,state:'running',audioWorklet:{async addModule(){if(stage==='worklet')await gate();}}};
    const resourceOrt={...ort,InferenceSession:{async create(){if(stage==='model')await gate();return {async run(){return warmOutput();},async release(){closed++;}};}}};
    const d=new LocalVAD({...env,AudioWorkletNode:class {},fetch:async()=>({ok:true,async arrayBuffer(){if(stage==='resource')await gate();return new ArrayBuffer(2327524);}})},async()=>resourceOrt);
    const pending=d.prepare({},inactive);await until(()=>resume,'inactive '+stage+' gate');
    assert.deepEqual(d.diagnostic,{stage,reason:'preparing'});inactive.state='suspended';resume();
    assert.equal(await pending,false);assert.deepEqual(d.diagnostic,{stage:stage==='worklet'?'worklet':'prepare',reason:'context_inactive'});
    assert.equal(closed,0,'context loss must retain the independent model');await d.close();assert.equal(closed,1);
  }
  // Late runtime loading and unsupported worklet cannot create/reacquire a stream.
  let finishLoad,loads=0;
  const late=new LocalVAD({...env,AudioWorkletNode:class {}},()=>{loads++;return new Promise(r=>{finishLoad=r;});});
  const ctx={state:'running',sampleRate:48000,audioWorklet:{addModule(){throw new Error('late model must not reach graph');}}};
  const warming=late.prepare({},ctx),closing=late.close();late.note("quality","clock");finishLoad(ort);
  assert.equal(await warming,false);await closing;assert.equal(loads,1);assert.equal(late.model,null);
  assert.deepEqual(late.diagnostic,{stage:"quality",reason:"clock"},"old initialization overwrote newer diagnostic");
  const unsupported=new LocalVAD(env,()=>{throw new Error('unsupported must not load');});
  assert.equal(await unsupported.prepare({},ctx),false);
  assert.deepEqual(unsupported.diagnostic,{stage:"worklet",reason:"worklet_missing"});
  let createEntered=false,releaseCreate,lateReleased=0;
  const delayedOrt={...ort,InferenceSession:{async create(){createEntered=true;await new Promise(r=>{releaseCreate=r;});return {async release(){lateReleased++;}};}}};
  const delayedEnv={...env,AudioWorkletNode:class {constructor(){throw new Error('late session must not attach graph');}},
    fetch:async()=>({ok:true,arrayBuffer:async()=>new ArrayBuffer(2327524)})};
  const lateSession=new LocalVAD(delayedEnv,async()=>delayedOrt);
  const loadingSession=lateSession.prepare({},ctx);
  await until(()=>createEntered,'session preparation gate');
  const closeSession=lateSession.close();assert.equal(lateReleased,0);
  releaseCreate();assert.equal(await loadingSession,false);await closeSession;assert.equal(lateReleased,1);
  const noSIMD=new LocalVAD(delayedEnv,async()=>({...ort,InferenceSession:{async create(){throw new Error('synthetic unsupported WASM');}}}));
  assert.equal(await noSIMD.prepare({},ctx),false);assert.equal(noSIMD.model,undefined);
  assert.deepEqual(noSIMD.diagnostic,{stage:"model",reason:"initialization_failed"});
  for(const reason of ["channels","clock","sample","overflow","tail",new String("PRIVATE")]) {
    const d=new LocalVAD(env);d.active={id:1,valid:true,event(){}};
    d.message({id:1,type:"fault",reason});
    assert.equal(d.active.valid,false);
    assert.equal(d.diagnostic.reason,typeof reason==="string"?reason:"unknown");
    assert(!JSON.stringify(d.diagnostic).includes("PRIVATE"));
  }
  const mismatched=new LocalVAD(env);mismatched.context=context;
  mismatched.active={id:1,valid:true,event(){},beginResolve(){}};
  mismatched.message({type:'begun',id:1,start:0,rate:48000,maxFrames:17});
  assert.equal(mismatched.active.valid,false);assert.deepEqual(mismatched.diagnostic,{stage:'queue',reason:'capacity'});
  mismatched.message({type:'begun',id:1,start:0,rate:48000,maxFrames:16});
  assert.deepEqual(mismatched.diagnostic,{stage:'queue',reason:'capacity'},'late begun erased first failure');
  assert.equal(mismatched.active.metrics?.overflowFrames,undefined);
  const badCapacity=new Processor();messages.length=0;
  badCapacity.port.onmessage({data:{type:'begin',id:6,maxFrames:17}});badCapacity.process([[new Float32Array(128)]],[[out]]);
  assert.equal(messages.find(m=>m.type==='fault').reason,'capacity');assert.equal(messages.find(m=>m.type==='fault').unacked,0);
  assert.equal(messages.filter(m=>m.type==='fault'&&m.reason==='overflow').length,0);
  const armClock=new Clock(),arm=new LocalVAD({setTimeout:armClock.setTimeout.bind(armClock),clearTimeout:armClock.clearTimeout.bind(armClock)});
  arm.model={reset(){}};arm.node={port:{postMessage(){}}};
  const arming=arm.begin(options,()=>{});armClock.advance(300);
  assert.equal((await arming).valid,false);assert.deepEqual(arm.diagnostic,{stage:"arm",reason:"arm_unconfirmed"});

  // Existing ASGI static routes: typed immutable vendor only, API/page no-store.
  for(const [name,mime] of [['ort.wasm.min.js','javascript'],['ort-wasm-simd-threaded.mjs','javascript'],['ort-wasm-simd-threaded.wasm','application/wasm'],['silero_vad_v5.onnx','octet-stream']]) {
    const r=await globalThis.requestASGI({path:'/bot-call/vendor/vad-v5-ort-1.22.0-2623a295/'+name,method:'GET',headers:{host:'calls.example.test'},body:''});
    assert.equal(r.status,200);assert.match(r.headers.find(([k])=>k==='content-type')[1],new RegExp(mime));
    assert.match(r.headers.find(([k])=>k==='cache-control')[1],/immutable/);
    const csp=r.headers.find(([k])=>k==='content-security-policy')[1];
    assert(csp.includes("'wasm-unsafe-eval'"));assert(!csp.includes("'unsafe-eval'"));
  }
  const entry=await globalThis.requestASGI({path:'/bot-call/',method:'GET',headers:{host:'calls.example.test'},body:''});
  assert.equal(entry.status,200);assert.equal(entry.headers.find(([k])=>k==='cache-control')[1],'no-store');
}

async function recoveryDiagnosticFacets() {
  const base = () => ({audio:[],replies:[],turns:[],complete:false,audio_available:true,speech_available:true});
  const response = (value,status=200) => ({ok:status>=200&&status<300,status,json:async()=>value});
  const poison = new Proxy({}, {get(){throw new Error("PRIVATE GETTER MUST NOT RUN");}});
  const notice = b => b.nodes.notice.textContent;
  // Heartbeat recovery: real page/API ownership, entirely synthetic fetch.
  // Keep the original state/ACK/media/VAD assertions below unchanged.
  const liveLimits = () => ({entered_at:0,deadline_at:3600,server_now:20,remaining_seconds:3580,
    reconnect_remaining_seconds:100,warning:false,close_reason:null,closed_at:null,enforced:true,error:null});
  const liveState = () => ({...base(),ended:false,close_requested:false,limits:liveLimits()});
  function heartbeatPage() {
    const b=browser(null),p=b.page;
    p.reference="synthetic-reference";p.started=true;p.server=liveState();
    return {b,p};
  }
  for(const mode of ["network","timeout","503","state-failure","closed","locked","missing-limits",
      "expired","missing-active","confirm-failure","confirm-limits"]) {
    const {b,p}=heartbeatPage(),traffic=[];
    const clip={id:"retained",mime:"audio/webm",blob:new Blob(["complete local bytes"])};p.clip=clip;
    let heartbeats=0;
    p.e.fetch=async(url,init)=>{
      const route=url.split("/").at(-1);traffic.push(route);
      if(route==="heartbeat") {
        if(++heartbeats===1) {
          if(mode==="timeout")return await new Promise((_,reject)=>init.signal.addEventListener("abort",()=>reject(poison)));
          if(mode==="503")return response(null,503);
          throw poison;
        }
        if(mode==="confirm-failure")throw poison;
        return response({limits:mode==="confirm-limits"?{enforced:true}:liveLimits()});
      }
      assert.equal(route,"state","recovery entered a media/write route");
      if(mode==="state-failure")throw poison;
      if(mode==="locked")return response(null,401);
      const value=liveState();
      if(mode==="closed")Object.assign(value,{ended:true,close_requested:true,complete:true});
      if(mode==="missing-limits")delete value.limits.server_now;
      if(mode==="expired")value.limits.reconnect_remaining_seconds=0;
      if(mode==="missing-active")delete value.ended;
      return response(value);
    };
    const pending=p.heartbeat();
    if(mode==="timeout")b.clock.advance(p.config.requestMs);
    await pending;
    assert.equal(p.clip,clip);assert.equal(await clip.blob.text(),"complete local bytes");
    assert.equal(b.permissions,0);assert.equal(b.utterances.length,0);assert.equal(p.heartbeatTask,null);
    assert.equal(p.heartbeatTimer,null);assert.equal(p.poll,null);
    if(["network","timeout","503"].includes(mode)) {
      assert.match(p.message,/连接已核实/);assert.equal(p.suspended,true);assert.equal(p.apiDiagnostic,"");
      assert.deepEqual(traffic,["heartbeat","state","heartbeat"]);
      assert.equal(Array.isArray(p.server.turns),true,"limits receipt replaced full state");
    } else if(mode==="closed") {assert.equal(p.ending,true);assert.match(p.message,/通话已结束/);}
    else if(mode==="locked") {assert.equal(p.locked,true);assert.match(p.message,/会话校验未通过/);}
    else if(["missing-limits","expired","missing-active"].includes(mode)) {
      assert.match(p.message,/状态已读取/);assert.equal(heartbeats,1);
    } else {assert.match(p.message,/连接尚未确认/);assert.equal(p.suspended,true);}
    const count=traffic.length;b.clock.advance(90000);await new Promise(resolve=>setImmediate(resolve));
    assert.equal(traffic.length,count,"paused recovery kept renewing in background");
    assert.equal(b.clock.tasks.size,0,"recovery leaked a timer");
  }
  // A real recorder's final stop bytes stay local through the confirmation.
  {
    const {b,p}=heartbeatPage();let heartbeats=0;const traffic=[];
    p.e.fetch=async(url)=>{
      const route=url.split("/").at(-1);traffic.push(route);
      if(route==="heartbeat"&&++heartbeats===1)throw poison;
      return response(route==="heartbeat"?{limits:liveLimits()}:liveState());
    };
    await p.start();b.recorders[0].data("local recording");
    await p.heartbeat();
    assert.equal(p.recording,null);assert.equal(p.suspended,true);assert.equal(b.permissions,1);
    assert.match(await p.clip.blob.text(),/local recording-tail/);assert.equal(b.recorders.length,1);
    assert(b.streams.every(stream=>stream.getTracks().every(track=>track.stopped)));
    assert(traffic.every(route=>["state","heartbeat","metrics"].includes(route)));
    assert.match(p.message,/连接已核实/);assert.equal(p.heartbeatTimer,null);
  }
  // Explicit Continue restores the original foreground cadence; no automatic mic.
  {
    const {b,p}=heartbeatPage();p.muted=true;let heartbeats=0;
    p.e.fetch=async(url)=>{
      if(url.endsWith("/heartbeat")) {if(++heartbeats===1)throw poison;return response({limits:liveLimits()});}
      assert(url.endsWith("/state"));return response(liveState());
    };
    await p.heartbeat();assert.equal(heartbeats,2);assert.equal(p.heartbeatTimer,null);
    await p.start();assert.equal(p.suspended,false);assert(p.heartbeatTimer!==null);
    b.clock.advance(p.config.heartbeatMs);await until(()=>!p.heartbeatTask,"continued heartbeat did not settle");
    assert.equal(heartbeats,3);assert.equal(b.permissions,0);await p.suspend();
    assert.equal(p.heartbeatTimer,null);
  }
  // Each await edge loses authority to a newer control. Include late auth
  // failures before/after body parse, not just a rejected outer promise.
  const actions=["start","retry","stopSpeaking","toggleMute","suspend","hangup","epoch","round","reference","locked","hidden"];
  for(const phase of ["initial","stop","state","confirm"]) {
    const selected=phase==="initial"?actions:phase==="stop"?["toggleMute","hangup"]:
      phase==="state"?["stopSpeaking","suspend","reference","round"]:["start","toggleMute","hangup","hidden"];
    for(const [index,action] of selected.entries()) {
      const {b,p}=heartbeatPage();p.muted=action!=="toggleMute";
      let release,reject,entered=false,heartbeats=0,states=0,closing=false,held=false;
      const gate=new Promise((yes,no)=>{release=yes;reject=no;});
      const traffic=[];
      const original=p.finishSegment.bind(p);
      if(phase==="stop")p.finishSegment=(...args)=>{
        if(!held){held=true;entered=true;return gate;}return original(...args);
      };
      p.e.fetch=async(url)=>{
        const route=url.split("/").at(-1);traffic.push(route);
        if(route==="end"){closing=true;return response({ended:true});}
        if(route==="heartbeat") {
          heartbeats++;
          if((phase==="initial"&&heartbeats===1)||(phase==="confirm"&&heartbeats===2)) {entered=true;return gate;}
          if(heartbeats===1&&phase!=="initial")throw poison;
          return response({limits:liveLimits()});
        }
        assert.equal(route,"state");states++;
        if(phase==="state"&&states===1){entered=true;return gate;}
        return response(closing?{...liveState(),ended:true,close_requested:true,complete:true}:liveState());
      };
      const pending=p.heartbeat();await until(()=>entered,"heartbeat owner gate not reached "+phase);
      let control=Promise.resolve();
      if(["start","retry","stopSpeaking","toggleMute","suspend","hangup"].includes(action))control=p[action]();
      else {
        if(action==="epoch")p.epoch++;
        if(action==="round")p.round={id:"new-round"};
        if(action==="reference")p.reference="new-reference";
        if(action==="locked")p.locked=true;
        if(action==="hidden")b.doc.hidden=true;
        p.show("new control owns page");
      }
      if(phase==="stop")release(null);
      else if(index%3===0)reject(poison);
      else release(response(phase==="state"?liveState():{limits:liveLimits()},index%3===1?403:200));
      await Promise.all([pending,control]);
      assert.equal(p.locked,action==="locked","obsolete 403 locked new owner "+phase+"/"+action);
      assert(!p.message.includes("连接已核实")&&!p.message.includes("连接尚未确认"),"obsolete recovery overwrote control "+phase+"/"+action);
      assert.equal(b.permissions,0);assert.equal(b.utterances.length,0);
      assert(traffic.every(route=>["state","heartbeat","end","metrics"].includes(route)));
      p.stopHeartbeat();b.clock.clearTimeout(p.poll);
    }
  }
  for(const phase of ["initial","state","confirm"]) {
    const {b,p}=heartbeatPage();let release,entered=false,heartbeats=0;
    const gate=new Promise(yes=>{release=yes;});
    p.e.fetch=async(url)=>{
      const heartbeat=url.endsWith("/heartbeat");if(heartbeat)heartbeats++;
      if(heartbeat&&heartbeats===1&&phase!=="initial")throw poison;
      if((phase==="state"&&!heartbeat)||(phase==="initial"&&heartbeat&&heartbeats===1)||
          (phase==="confirm"&&heartbeat&&heartbeats===2))
        return {ok:true,status:200,json:async()=>{entered=true;return await gate;}};
      return response(heartbeat?{limits:liveLimits()}:liveState());
    };
    const pending=p.heartbeat();await until(()=>entered,"response body owner gate");
    await p.stopSpeaking();p.show("new body owner");
    const server=p.server;release(phase==="state"?liveState():{limits:liveLimits()});await pending;
    assert.equal(p.server,server);assert.equal(p.message,"new body owner");assert.equal(p.locked,false);
    assert.equal(b.permissions,0);p.stopHeartbeat();b.clock.clearTimeout(p.poll);
  }
  for(const action of ["start","hangup"]) {
    const {b,p}=heartbeatPage();p.muted=true;let release,entered=false,states=0;
    p.e.fetch=async(url)=>{
      if(url.endsWith("/end"))return response({ended:true});
      assert(url.endsWith("/state"));states++;
      if(states===1)return await new Promise(yes=>{entered=true;release=yes;});
      return response(liveState());
    };
    const first=p[action]();await until(()=>entered,"coalesced control read");
    const generation=p.heartbeatControl;assert.equal(p[action](),first);
    assert.equal(p.heartbeatControl,generation,"double click invalidated its own owner");
    release(response(action==="hangup"?{...liveState(),ended:true,close_requested:true,complete:true}:liveState()));
    await first;assert.equal(states,1);assert.equal(b.permissions,0);
    if(action==="start")assert.equal(p.suspended,false);
    else assert.equal(p.ending,true);
    p.stopHeartbeat();b.clock.clearTimeout(p.poll);
  }
  for(const fault of ["reject","timeout"]) {
    const {b,p}=heartbeatPage();let reject,entered=false;
    p.e.fetch=()=>new Promise((_,no)=>{entered=true;reject=no;});
    const old=p.refresh(false).catch(error=>p.report(error));await until(()=>entered,"stale GET rejection");
    await p.stopSpeaking();p.show("new GET owner");
    if(fault==="timeout")b.clock.advance(p.config.requestMs);else reject(poison);
    await old;assert.equal(p.message,"new GET owner");assert.equal(p.apiDiagnostic,"");
    p.stopHeartbeat();b.clock.clearTimeout(p.poll);
  }
  // A detached old transport cannot own the new heartbeat slot indefinitely.
  for(const late of ["success","error","auth"]) {
    const {b,p}=heartbeatPage();let release,reject,calls=0;
    p.e.fetch=url=>{
      assert(url.endsWith("/heartbeat"));
      return ++calls===1?new Promise((yes,no)=>{release=yes;reject=no;}):Promise.resolve(response({limits:liveLimits()}));
    };
    const old=p.heartbeat();await p.stopSpeaking();const current=p.heartbeat();
    assert.notEqual(current,old);await current;const timer=p.heartbeatTimer;
    if(late==="error")reject(poison);else release(response({limits:liveLimits()},late==="auth"?403:200));
    await old;assert.equal(calls,2);assert.equal(p.locked,false);assert.equal(p.heartbeatTimer,timer);
    assert.equal(p.suspended,false);p.stopHeartbeat();
  }
  // Actual API + original single timer. All bytes/credentials here are synthetic.
  for (const fault of ["network","timeout","http","json","shape"]) {
    const b=browser(null),p=b.page,round={id:"owned"};
    p.reference="synthetic-reference";p.server=base();p.round=round;
    let calls=0,active=0,peak=0;
    p.e.fetch=async(url,init)=>{calls++;active++;peak=Math.max(peak,active);try {
      if(calls===1) {
        if(fault==="network")throw poison;
        if(fault==="timeout")return await new Promise((_,reject)=>init.signal.addEventListener("abort",()=>reject(poison)));
        if(fault==="http")return response(null,503);
        if(fault==="json")return {ok:true,status:200,json:async()=>{throw poison;}};
        return response({audio:"PRIVATE",complete:true});
      }
      return response({...base(),audio:[{utterance_id:"owned",state:"succeeded"}],
        replies:[{utterance_id:"owned",state:"running"}]});
    }finally {active--;}};
    p.schedulePoll();b.clock.advance(p.config.pollMs);
    if(fault==="timeout") {await new Promise(resolve=>setImmediate(resolve));b.clock.advance(p.config.requestMs);}
    await until(()=>!p.refreshTask,"failed state did not settle "+fault);
    assert.equal(p.round,round);assert(p.poll!==null,"one state failure stranded the poll");
    const reason=fault==="http"?"http":fault==="timeout"?"timeout":fault==="network"?"network":"invalid-response";
    assert(notice(b).includes("state/"));assert(notice(b).includes("/"+reason));
    assert(!notice(b).includes("PRIVATE"));assert.equal(b.permissions,0);
    // The successful subsequent state follows the existing reconcile gate.
    b.clock.advance(p.config.pollMs);await until(()=>!p.refreshTask,"recovered state");
    assert.equal(p.round,round);assert.match(b.nodes.status.textContent,/等待回复/);
    assert.equal(p.apiDiagnostic,"");assert.equal(calls,2);
    assert.equal(peak,1,"read recovery overlapped a transport");
    b.clock.clearTimeout(p.poll);
  }
  for(const status of [401,403]) {
    const b=browser(null),p=b.page;p.reference="synthetic-reference";p.server=base();p.round={id:"auth"};
    let calls=0;p.e.fetch=async()=>{calls++;return response(null,status);};
    p.schedulePoll();b.clock.advance(p.config.pollMs);await until(()=>!p.refreshTask,"auth state");
    assert.equal(p.locked,true);assert.equal(p.poll,null);assert(notice(b).includes("HTTP="+status));
    b.clock.advance(60000);assert.equal(calls,1);assert.equal(b.permissions,0);
  }
  for(const change of ["epoch","round","suspend","closing"]) {
    const b=browser(null),p=b.page;p.reference="synthetic-reference";p.server=base();p.round={id:"old"};
    let reject;p.e.fetch=()=>new Promise((_,r)=>{reject=r;});
    p.schedulePoll();b.clock.advance(p.config.pollMs);
    await until(()=>!!reject,"old poll entry");
    if(change==="epoch")p.epoch++;
    if(change==="round")p.round={id:"new"};
    if(change==="suspend"){p.epoch++;p.suspended=true;}
    if(change==="closing"){p.epoch++;p.ending=true;p.server={...base(),close_requested:true};}
    p.show("new owner notice");p.schedulePoll();const timer=p.poll,round=p.round;
    reject(poison);await until(()=>!p.refreshTask,"old poll rejection");
    assert.equal(p.message,"new owner notice");assert.equal(p.poll,timer);assert.equal(p.round,round);
    b.clock.clearTimeout(p.poll);assert.equal(b.permissions,0);
  }
  // Existing close/review projection still recovers and reaches completion.
  {
    const b=browser(null),p=b.page;p.reference="synthetic-reference";p.ending=true;
    p.server={...base(),close_requested:true};let calls=0;
    p.e.fetch=async()=>++calls===1?response(null,502):response({...base(),close_requested:true,ended:true,complete:true});
    p.schedulePoll();b.clock.advance(p.config.pollMs);await until(()=>!p.refreshTask,"closing failure");
    assert(p.poll!==null);b.clock.advance(p.config.pollMs);await until(()=>!p.refreshTask,"closing complete");
    assert.equal(p.poll,null);assert.equal(p.server.complete,true);assert.equal(b.permissions,0);
  }
  // POST confirmed -> first refresh failed is also a recoverable read-only path.
  {
    const b=browser(null),p=b.page;p.reference="synthetic-reference";p.server=base();
    p.clip={id:"clip-owned",mime:"audio/webm;codecs=opus",blob:new Blob(["synthetic complete container"],{type:"audio/webm;codecs=opus"})};
    let states=0;const traffic=[];
    p.e.fetch=async(url,init)=>{
      const route=url.slice("/bot-call/".length);traffic.push(route);
      if(route==="state") {
        states++;
        if(states===1)throw poison; // the first submission has no precheck: this is the state read after the ACK
        return response({...base(),audio:[{utterance_id:"clip-owned",state:"succeeded"}],
          replies:[{utterance_id:"clip-owned",state:"succeeded",generation_id:"reply:owned",bot_sequence:2}],
          turns:[{sequence:2,role:"assistant",material_id:"reply:owned",body:"synthetic reply",playback_revision:0}]});
      }
      if(route==="audio")return response({audio_received:true,utterance_id:"clip-owned"});
      if(route==="speech")return response({generation_id:"reply:owned",state:"succeeded"});
      if(route==="speech-audio")return {ok:true,status:200,blob:async()=>new Blob(["synthetic WAV"],{type:"audio/wav"})};
      if(route==="playback")return response({recorded:true,...JSON.parse(init.body)});
      throw new Error("unexpected recovery route");
    };
    assert.equal(await p.sendClip(),false);assert.equal(p.clip,null);const round=p.round;
    assert(round);assert(p.poll!==null);assert(notice(b).includes("state/fetch/network"));
    b.clock.advance(p.config.pollMs);await until(()=>b.utterances.length===1,"ACK recovery did not find same reply");
    assert.equal(p.round,round);assert.equal(traffic.filter(x=>x==="audio").length,1);
    assert.equal(traffic.filter(x=>x==="speech").length,1);assert.equal(b.permissions,0);
    b.utterances[0].onplaying();b.utterances[0].onended();await settleSpeech(b);
    assert.equal(p.round,null);assert.equal(traffic.filter(x=>x==="audio").length,1);assert.equal(b.permissions,0);
    b.clock.clearTimeout(p.poll);
  }
  // Unconfirmed local clip is retained; only its explicit operation can resend.
  {
    const b=browser(null),p=b.page;p.reference="synthetic-reference";p.server=base();
    const clip={id:"unsent",mime:"audio/webm;codecs=opus",blob:new Blob(["retain"])};p.clip=clip;
    let calls=0;p.e.fetch=async()=>{calls++;throw poison;};
    // First submission: straight to the POST, then a read-only check of whether it arrived (both fail here).
    assert.equal(await p.sendClip(),false);assert.equal(p.clip,clip);assert.equal(p.round,null);
    assert.equal(p.poll,null);assert.equal(calls,2);assert(notice(b).includes("state/audio_readback.fetch/network"));assert.equal(clip.postStarted,true);
    // Retry: read the state first and never post blindly again.
    assert.equal(await p.sendClip(),false);assert.equal(calls,3);assert(notice(b).includes("state/clip_precheck.fetch/network"));
    delete clip.sha;
    p.e.crypto={subtle:{digest:async()=>{throw poison;}}};
    assert.equal(await p.sendClip(),false);assert(notice(b).includes("local/clip_digest/local_unknown"));
    assert.equal(calls,3);assert.equal(p.clip,clip);
  }
  // Each route uses only branded, fixed scalar metadata; hostile exceptions and
  // objects copying a branded error cannot acquire diagnostic authority.
  {
    const b=browser(null),p=b.page;p.reference="synthetic-reference";let branded;
    for(const route of ["state","audio","heartbeat","playback","speech","speech-audio"]) {
      p.e.fetch=async()=>response(null,503);
      try {await p.api(route,route==="state"?undefined:{});} catch(error){branded=error;p.report(error);}
      assert(notice(b).includes(route+"/http/http"));assert(!notice(b).includes("PRIVATE"));
    }
    for(const error of [poison,Object.create(branded),new String("PRIVATE"),{route:"speech",status:503,reason:"PRIVATE"},null]) {
      p.report(error);assert(notice(b).includes("local/local/local_unknown"));assert(!notice(b).includes("PRIVATE"));
    }
    // afterSpeech's ordinary notice must not erase the speech API failure.
    p.e.fetch=async(url,init)=>url.endsWith("/playback")?response({recorded:true,...JSON.parse(init.body)}):response(null,503);
    p.round={id:"speech-failed"};p.speak("reply:failed",{body:"synthetic",playback_revision:0});
    await until(()=>!p.speech&&!p.flushTask,"failed speech did not settle");
    assert(notice(b).includes("speech/http/http"));
  }
  // False completion with stale/missing progress becomes unknown, real failures stay precise.
  for(const diagnostic of [null,{stage:"model",reason:"preparing"},{stage:"quality",reason:"clock"},{stage:"arm",reason:"arm_unconfirmed"}]) {
    const d={diagnostic,ready:()=>false,prepare:async()=>false,close:async()=>{}};
    const b=browser(null,{createVAD:()=>d}),p=b.page;p.reference="synthetic-reference";p.api=async()=>base();b.silence();
    await p.start();await until(()=>!p.recording.vadPreparation.pending,'false preparation projection');
    assert(!notice(b).includes("正在准备"));assert.equal(p.recording.vad,null);
    assert(notice(b).includes(diagnostic&&diagnostic.reason!=="preparing" ? diagnostic.stage+"/"+diagnostic.reason : (diagnostic?.stage||"prepare")+"/unknown"));
    p.releaseMic();await p.finishSegment();
  }
  // Only safe, bounded numeric metadata reaches a current failed owner's notice.
  {
    let signal,event;
    const detector={ready:()=>true,async begin(options,callback){event=callback;return signal={valid:true};},async finish(){return {valid:false,candidate:true};},close:async()=>{}};
    const b=browser(null,{createVAD:()=>detector}),p=b.page;p.reference="synthetic-reference";p.api=async()=>base();b.silence();
    await p.start();const r=p.recording;signal.valid=false;signal.diagnostic={stage:'queue',reason:'overflow'};
    signal.metrics={warmupMs:7,awaitPeakMs:null,queuePeak:15,overflowFrames:16};event('unavailable');
    assert(notice(b).includes('overflowFrames=16'));assert(!notice(b).includes('awaitPeakMs='));
    signal.metrics={...signal.metrics,awaitPeakMs:185};b.clock.advance(100);
    assert(notice(b).includes('awaitPeakMs=185'));assert.equal(p.recording,r);
    let renders=0;const view=p.view;p.view=v=>{renders++;view(v);};b.clock.advance(100);assert.equal(renders,0);
    for(const value of [NaN,Infinity,-1,1.5,new Number(2),"PRIVATE",600001]) {
      signal.metrics={warmupMs:value,awaitPeakMs:value,queuePeak:99,overflowFrames:17};event('unavailable');
      assert(!notice(b).includes('本地检测'));assert(!notice(b).includes('PRIVATE'));
    }
    let getters=0;const gettersOnly={};for(const key of ['warmupMs','awaitPeakMs','queuePeak','overflowFrames'])Object.defineProperty(gettersOnly,key,{get(){getters++;throw new Error('PRIVATE getter');}});
    signal.metrics=gettersOnly;event('unavailable');assert.equal(getters,0);
    signal.metrics=new Proxy({}, {get(){throw new Error('PRIVATE free get');},getOwnPropertyDescriptor(){throw new Error('PRIVATE descriptor');}});event('unavailable');
    Object.defineProperty(signal,'metrics',{configurable:true,get(){getters++;throw new Error('PRIVATE metrics getter');}});event('unavailable');assert.equal(getters,0);
    delete signal.metrics;signal.metrics={warmupMs:1,awaitPeakMs:200,queuePeak:3,overflowFrames:16};
    p.view=()=>{throw new Error('synthetic render failed');};assert.doesNotThrow(()=>p.updateVADFailure(r));p.view=view;
    p.epoch++;p.suspended=true;p.show('new control');const saved=notice(b);signal.metrics.awaitPeakMs=300;event('unavailable');p.updateVADFailure(r);assert.equal(notice(b),saved);
    assert.equal(b.permissions,1);assert.equal(b.recorders.length,1);p.releaseMic();await p.finishSegment();
  }
  // Loading can update only its own notice, never adopt late VAD coverage.
  {
    let resolve;const pending=new Promise(r=>{resolve=r;});
    const detector={diagnostic:{stage:"runtime",reason:"preparing"},ready:()=>false,prepare:()=>pending,close:async()=>{}};
    const b=browser(null,{createVAD:()=>detector}),p=b.page;p.reference="synthetic-reference";b.silence();
    p.api=async()=>base();let renders=0;const view=p.view;p.view=value=>{renders++;view(value);};
    await p.start();const r=p.recording;
    assert(notice(b).includes("runtime/preparing"));assert(notice(b).includes("本段使用音量停顿"));
    for(const stage of ["resource","model","warmup","worklet"]) {
      detector.diagnostic={stage,reason:"preparing"};
      if(stage==="warmup") {
        const prior=p.view;p.view=()=>{throw new Error('synthetic preparation render failed');};
        assert.doesNotThrow(()=>b.clock.advance(100));assert.equal(p.recording,r);assert(b.clock.tasks.has(r.timer));
        p.view=prior;p.render();
      } else b.clock.advance(100);
      assert(notice(b).includes(stage+"/preparing"));assert(!notice(b).includes("检测不可用"));
    }
    const stable=renders;b.clock.advance(100);assert.equal(renders,stable,"unchanged preparation rendered again");
    detector.diagnostic={stage:"ready",reason:"ready"};resolve(true);
    await until(()=>notice(b).includes("ready/ready_next"),"VAD ready notice");assert.equal(r.vad,null);assert.equal(p.recording,r);
    b.clock.advance(600);assert.equal(r.quality.valid,false);assert.equal(r.vad,null);
    assert(notice(b).includes("下一段启用人声断句"));assert(!notice(b).includes("检测不可用"));
    // A real fixed failure takes precedence over the successful preparation result.
    detector.diagnostic={stage:"quality",reason:"clock"};b.clock.advance(100);
    assert(notice(b).includes("quality/clock"));assert(!notice(b).includes("ready/ready_next"));
    p.releaseMic();await p.finishSegment();p.clip=null;
    let release;const late=new Promise(r=>{release=r;});
    detector.prepare=()=>late;await p.listen();const owned=p.recording;
    p.epoch++;p.suspended=true;p.show("current control");release(false);
    await new Promise(r=>setImmediate(r));assert.equal(p.message,"current control");assert.equal(p.recording,owned);
    p.releaseMic();await p.finishSegment();
    // Auxiliary analysis can degrade during preparation without labelling the model failed.
    let done;const pendingAgain=new Promise(r=>{done=r;});
    const loading={diagnostic:{stage:"model",reason:"preparing"},ready:()=>false,prepare:()=>pendingAgain,close:async()=>{}};
    const c=browser(null,{createVAD:()=>loading});c.page.reference="synthetic-reference";c.page.api=async()=>base();c.silence();
    await c.page.start();c.clock.advance(600);
    assert(!notice(c).includes("检测不可用"));assert(notice(c).includes("model/preparing"));
    const old=c.page.recording;const frozenNotice=notice(c);
    c.page.ending=true;c.page.epoch++;done(true);c.clock.advance(100);
    await new Promise(r=>setImmediate(r));assert.equal(notice(c),frozenNotice);assert.equal(old.vad,null);
    c.page.releaseMic();await c.page.finishSegment();
  }
}

async function uploadAnalysisFacets() {
  const base=()=>({audio:[],replies:[],turns:[],complete:false,audio_available:true,speech_available:true});
  const response=value=>({ok:true,status:200,json:async()=>value});
  const poison=new Proxy({}, {get(){throw new Error("PRIVATE EXCEPTION GETTER");}});
  for(const audioRequestMs of [0,-1,120001,1.5,NaN,Infinity,Number.MAX_SAFE_INTEGER,null,true,"60000",new Number(60000)])
    assert.throws(()=>browser(null,{audioRequestMs}),/invalid_configuration/);
  const configured=browser(null,{audioRequestMs:120000});assert.equal(configured.page.config.audioRequestMs,120000);
  assert.equal(configured.page.config.requestMs,15000);assert.equal(configured.page.config.segmentMs,30000);assert.equal(configured.page.config.maxAudioBytes,2097152);
  // A single upload may outlive 15s. Existing user controls join its owned task.
  for(const action of ["success","suspend","hangup","mute","new-owner"]) {
    const b=browser(null),p=b.page;p.reference="synthetic-reference";p.server=base();p.started=true;
    const clip={id:"upload-owned",mime:"audio/webm;codecs=opus",blob:new Blob(["complete container"],{type:"audio/webm;codecs=opus"})};p.clip=clip;
    let release,aborted=false,closed=false;const held=new Promise(resolve=>{release=resolve;}),traffic=[];
    p.e.fetch=async(url,init)=>{
      const route=url.slice("/bot-call/".length);traffic.push(route);
      if(route==="audio") {init.signal.addEventListener("abort",()=>{aborted=true;});await held;return response({audio_received:true,utterance_id:clip.id});}
      if(route==="end"){closed=true;return response({ended:true});}
      if(route==="heartbeat")return response({limits:null});
      if(route==="state")return response({...base(),audio:traffic.includes("audio")?[{utterance_id:clip.id,state:"pending"}]:[],
        ...(closed?{ended:true,close_requested:true,complete:true}:{})});
      throw new Error("unexpected owned-upload route");
    };
    const first=p.sendClip(),second=p.sendClip();assert.equal(first,second);
    await until(()=>traffic.includes("audio"),"upload entry");
    assert.match(b.nodes.status.textContent,/正在上传/);
    b.clock.advance(15001);await new Promise(r=>setImmediate(r));
    assert.equal(aborted,false);assert.equal(p.clip,clip);assert(p.sendTask);
    await p.api("heartbeat",{});assert.equal(traffic.filter(x=>x==="audio").length,1);
    let control=Promise.resolve();
    if(action==="suspend")control=p.suspend();
    if(action==="hangup")control=p.hangup();
    if(action==="mute")control=p.toggleMute();
    if(action==="new-owner"){p.epoch++;p.round={id:"new-round"};p.clip={id:"new-clip"};p.show("new owner status");}
    const ownerClip=p.clip,ownerRound=p.round;
    b.clock.advance(14999);release();
    const accepted=await first;await control;
    assert.equal(aborted,false);assert.equal(traffic.filter(x=>x==="audio").length,1);
    assert.equal(traffic.filter(x=>x==="speech").length,0);assert.equal(b.permissions,0);
    if(action==="new-owner") {
      assert.equal(accepted,false);assert.equal(p.clip,ownerClip);assert.equal(p.round,ownerRound);assert.equal(p.message,"new owner status");
    } else {
      assert.equal(accepted,true);assert.equal(p.clip,null);assert.equal(p.round.id,clip.id);
      if(action==="hangup"){assert.equal(traffic.filter(x=>x==="end").length,1);assert(traffic.indexOf("end")>traffic.indexOf("audio"));assert.equal(p.server.complete,true);}
      if(action==="suspend")assert.equal(p.suspended,true);
      if(action==="mute")assert.equal(p.muted,true);
    }
    b.clock.clearTimeout(p.poll);b.clock.clearTimeout(p.heartbeatTimer);
  }
  // At 60s there is only a readback. Exact readback may confirm the same bytes;
  // otherwise keep the original clip/hash and never automatically send it again.
  for(const saved of [false,true]) {
    const b=browser(null),p=b.page;p.reference="synthetic-reference";p.server=base();
    const clip={id:"timeout-owned",mime:"audio/webm;codecs=opus",blob:new Blob(["retain bytes"],{type:"audio/webm;codecs=opus"})};p.clip=clip;
    const traffic=[];let aborted=false;
    p.e.fetch=async(url,init)=>{
      const route=url.slice("/bot-call/".length);traffic.push(route);
      if(route==="audio")return await new Promise((_,reject)=>init.signal.addEventListener("abort",()=>{aborted=true;reject(poison);}));
      if(route==="state")return response({...base(),audio:saved&&aborted?[{
        utterance_id:clip.id,sha256:clip.sha,byte_count:clip.blob.size,mime:clip.mime,state:"pending"}]:[]});
      throw new Error("unexpected timeout route");
    };
    const sending=p.sendClip();await until(()=>traffic.includes("audio"),"timeout upload entry");
    b.clock.advance(59999);await new Promise(r=>setImmediate(r));assert.equal(aborted,false);assert.equal(p.clip,clip);
    b.clock.advance(1);assert.equal(await sending,saved);assert.equal(aborted,true);
    assert.equal(traffic.filter(x=>x==="audio").length,1);
    if(saved){assert.equal(p.clip,null);assert.equal(p.round.id,clip.id);}
    else {
      assert.equal(p.clip,clip);assert.equal(p.round,null);assert.equal(traffic.filter(x=>x==="state").length,1,"no precheck on the first submission: only the readback");
      assert.match(b.nodes.notice.textContent,/audio\/audio_submit.fetch\/timeout/);
      assert.match(b.nodes.notice.textContent,/超时不代表未收到/);assert.match(clip.sha,/^[0-9a-f]{64}$/);
      b.clock.advance(120000);await new Promise(r=>setImmediate(r));assert.equal(traffic.filter(x=>x==="audio").length,1);
    }
    assert.equal(b.permissions,0);b.clock.clearTimeout(p.poll);
  }
  // State reads have a short deadline; other API budgets remain 15s.
  for(const route of ["state","heartbeat","playback","speech","speech-audio"]) {
    const b=browser(null,{audioRequestMs:120000}),p=b.page;p.reference="synthetic-reference";
    let aborted=false;p.e.fetch=async(url,init)=>await new Promise((_,reject)=>init.signal.addEventListener("abort",()=>{aborted=true;reject(poison);}));
    const pending=p.api(route,route==="state"?undefined:{}, {},route==="speech-audio").then(()=>assert.fail("pending API succeeded"),error=>p.report(error));
    b.clock.advance(route==="state"?2499:14999);await new Promise(r=>setImmediate(r));assert.equal(aborted,false);
    b.clock.advance(1);await pending;assert.equal(aborted,true);assert(b.nodes.notice.textContent.includes(route+"/fetch/timeout"));
  }
  // Actual sampleRecording with an independently owned signal substitute.
  // Auxiliary gaps/reader/sample failures cannot invalidate that healthy signal.
  for(const mode of ["gap","missing","read","sample","stalled","context","rate","track","disabled","identity","negative-gap","old-epoch"]) {
    let signal,event,failed=0;
    const detector={ready:()=>true,async begin(options,callback){event=callback;return signal={valid:true,candidate:false};},
      fail(s,stage,reason){if(!s.valid)return;failed++;s.diagnostic={stage,reason};s.valid=false;event("unavailable");},
      async finish(s){assert.equal(b.streams[0].getTracks()[0].enabled,false);return {valid:s.valid,candidate:s.candidate};},close:async()=>{}};
    const b=browser(null,{createVAD:()=>detector}),p=b.page;p.reference="synthetic-reference";p.config.segmentMs=500;b.silence();
    const state=base(),traffic=[];
    p.api=async(route,body,headers)=>{
      traffic.push(route);
      if(route==="audio"){state.audio.push({utterance_id:headers["X-Bot-Utterance-ID"],state:"failed",error_code:"not_applied",
        sha256:[...new Uint8Array(await webcrypto.subtle.digest("SHA-256",await body.arrayBuffer()))].map(x=>x.toString(16).padStart(2,"0")).join(""),
        byte_count:body.size,mime:body.type});return {audio_received:true,utterance_id:headers["X-Bot-Utterance-ID"]};}
      if(route==="state")return {...state};
      throw new Error("unexpected analysis route");
    };
    await p.start();const r=p.recording;
    if(mode==="gap"||mode==="negative-gap")b.clock.advance(300);
    if(mode==="missing")p.analyser=null;
    if(mode==="read")p.analyser.getFloatTimeDomainData=()=>{throw poison;};
    if(mode==="sample")b.level(NaN);
    if(mode==="stalled"){b.stall();b.clock.advance(100);}
    if(mode==="context")p.audioContext.state="suspended";
    if(mode==="rate")p.audioContext.sampleRate=NaN;
    if(mode==="track")r.stream.getTracks()[0].muted=true;
    if(mode==="disabled")r.stream.getTracks()[0].enabled=false;
    if(mode==="identity")p.audioContext=new b.win.AudioContext();
    if(mode==="old-epoch"){p.epoch++;p.show("current owner");}
    p.sampleRecording(r);assert.equal(r.quality.valid,false);
    const fatal=["context","rate","track","disabled","identity","old-epoch"].includes(mode);
    assert.equal(failed,fatal?1:0);assert.equal(signal.valid,!fatal);
    assert(!b.nodes.notice.textContent.includes("PRIVATE"));
    if(mode==="old-epoch") {
      assert.equal(p.message,"current owner");await p.finishSegment();p.releaseMic();continue;
    }
    if(!fatal)assert(!b.nodes.notice.textContent.includes("停顿检测不可用"),"auxiliary failure misreported the independent VAD");
    if(mode==="negative-gap") {
      b.clock.advance(200);await until(()=>!p.sendTask&&!!p.round,"unknown idle segment was not retained");
      assert.equal(signal.valid,true);assert.equal(signal.candidate,false);
    } else if(fatal) {
      event("endpoint");assert.equal(traffic.filter(x=>x==="audio").length,0,"unsafe capture used endpoint");
      await p.submitSegment();
    } else {
      signal.candidate=true;event("candidate");event("endpoint");event("endpoint");
      await until(()=>!p.sendTask&&!!p.round,"healthy VAD endpoint was lost");
    }
    assert.equal(traffic.filter(x=>x==="audio").length,1);assert.equal(b.recorders.length,1);assert.equal(b.permissions,1);
    p.releaseMic();b.clock.clearTimeout(p.poll);
  }
}

async function stateWaitFacets() {
  const state=()=>({audio:[{utterance_id:"owned",state:"running"}],replies:[],turns:[],complete:false,
    audio_available:true,speech_available:true});
  const response=(value,status=200)=>({ok:status>=200&&status<300,status,json:async()=>value});
  function setup() {
    const b=browser(null),p=b.page,reads=[];
    p.reference="synthetic-reference";p.server=state();p.round={id:"owned"};p.started=true;
    let late;
    p.e.fetch=async(url,init)=>{
      if(url.endsWith("/state")) {
        reads.push(init);
        if(reads.length===1)return await new Promise(resolve=>{late=resolve;});
        return response(state());
      }
      if(url.endsWith("/end"))return response({ended:true});
      throw new Error("unexpected synthetic route");
    };
    return {b,p,reads,late:(value,status=200)=>late(response(value,status))};
  }
  // Timer poll only; two explicit reads replace one long wrapper and merge
  // into one immediate GET, even if the fetch ignores AbortSignal entirely.
  {
    const {b,p,reads,late}=setup();
    p.schedulePoll();b.clock.advance(p.config.pollMs);
    await until(()=>reads.length===1,"long state poll not started");
    assert.equal(reads[0].headers["X-Bot-Call-Wait-Ms"],"8000");
    assert.equal(reads[0].headers["X-Bot-Call-Utterance"],"owned");
    const old=p.refreshTask;
    assert.equal(p.refresh(true,true),old,"same owned poll did not merge");
    const immediate=p.refresh(false),shared=p.refresh(false);
    await Promise.all([old,immediate,shared]);
    assert.equal(reads.length,2,"immediate refresh split into duplicate GETs");
    assert.equal(reads[1].headers["X-Bot-Call-Wait-Ms"],undefined);
    assert.equal(p.apiDiagnostic,"");assert.equal(b.permissions,0);
    const saved=p.server,message=p.message;
    late({audio:[],replies:[],turns:[],complete:true},403);
    await new Promise(resolve=>setImmediate(resolve));
    assert.equal(p.server,saved);assert.equal(p.message,message);assert.equal(p.locked,false);
    assert.equal(p.apiDiagnostic,"");assert.equal(reads[0].signal.aborted,true);
  }
  // An immediate read can also lose its owner while an awaited control/ACK
  // flow is active. Its stale success/rejection must not reject that caller or
  // overwrite the new read, even when the obsolete transport returns 403.
  for (const outcome of ["success","reject","auth"]) {
    const {b,p,reads,late}=setup();
    let reject;
    const fetch=p.e.fetch;
    p.e.fetch=(url,init)=>reads.length===0
      ? Promise.race([fetch(url,init),new Promise((_,no)=>{reject=no;})])
      : fetch(url,init);
    const old=p.refresh(false);
    await until(()=>reads.length===1,"immediate owner gate");
    await p.stopSpeaking();
    p.round={id:"new-owned-round"}; // A newly confirmed upload replaces the source.
    const current=p.refresh(false);
    if(outcome==="reject")reject(new Error("synthetic obsolete failure"));
    else late({...state(),complete:true},outcome==="auth"?403:200);
    await Promise.all([old,current]);
    assert.equal(reads.length,2);
    assert.equal(p.server.complete,false);assert.equal(p.locked,false);
    assert.equal(p.apiDiagnostic,"");assert.equal(b.permissions,0);
    p.stopHeartbeat();b.clock.clearTimeout(p.poll);
  }
  // Control paths settle before an unresponsive long backend wrapper. Their
  // existing immediate reads/ACK gates decide whether anything can resume.
  for(const action of ["start","retry","stopSpeaking","toggleMute","suspend","hangup"]) {
    const {b,p,reads,late}=setup();
    p.schedulePoll();b.clock.advance(p.config.pollMs);
    await until(()=>reads.length===1,"control wait not started");
    await p[action]();
    assert.equal(reads[0].signal.aborted,true,action+" did not cancel read wrapper");
    assert.equal(b.permissions,0,action+" bypassed pending round");
    assert.equal(p.apiDiagnostic,"",action+" reported internal abort");
    assert(reads.slice(1).every(init=>init.headers["X-Bot-Call-Wait-Ms"]===undefined));
    late(state());await new Promise(resolve=>setImmediate(resolve));
    assert.equal(b.permissions,0);assert.equal(p.apiDiagnostic,"");
  }
  // A bounded backend return still pending simply arms the original timer;
  // a real fetch timeout keeps the original safe read-only recovery diagnosis.
  for(const timeout of [false,true]) {
    const {b,p,reads,late}=setup();
    p.schedulePoll();b.clock.advance(p.config.pollMs);
    await until(()=>reads.length===1,"pending wait not started");
    if(timeout)b.clock.advance(p.config.requestMs);else late(state());
    await until(()=>!p.refreshTask,"bounded state wrapper did not settle");
    assert(p.poll!==null);assert.equal(b.permissions,0);
    if(timeout)assert(p.apiDiagnostic.includes("state/fetch/timeout"));
    else assert.equal(p.apiDiagnostic,"");
    b.clock.advance(p.config.pollMs);
    await until(()=>reads.length===2&&!p.refreshTask,"state wait did not resume original poll");
    assert.equal(p.apiDiagnostic,"");
    if(timeout)late(state());
  }
  // Closing/review and explicitly requested reads retain immediate wire shape.
  {
    const {p,reads,late}=setup();p.server.close_requested=true;p.ending=true;
    p.schedulePoll(); // No extra timer chain is introduced.
    const task=p.refresh(false);await until(()=>reads.length===1,"closing immediate read");
    assert.equal(reads[0].headers["X-Bot-Call-Wait-Ms"],undefined);
    late({...state(),close_requested:true});await task;
  }
}

async function normalRoundStopAndFinalClip() {
  await phoneUIFacets();
  await contextActionFacet();
  await transitionFacet();
  await uploadStepsFacet();
  await chunkUploadFacet();
  await endedFacet();
  await glowMotionFacet();
  await conversationScrollFacet();
  await captionPreferenceFacet();
  await transientContinueFacet();
  await preparedVADFacets();
  await localVADFacets();
  await recoveryDiagnosticFacets();
  await uploadAnalysisFacets();
  await silentGateFacets();
  await stateWaitFacets();
  // Local deterministic capture facets merged into the first existing flow.
  // No real ASGI traffic or resources; downstream original flow still uses IPC.
  async function capture(mode) {
    let signal,event,detectorCloses=0;
    const fake={ready:()=>mode!=='vad-loading',prepare:async()=>false,async begin(options,callback){event=callback;return signal={valid:true};},
      fail(s){s.valid=false;event?.('unavailable');},async finish(s,end){assert.equal(b.streams[0].getTracks()[0].enabled,false,"flush extended capture permission");if(mode==='vad-roundoff'){assert.equal(s.recordStart,260736);assert.equal(end,260736);}return {valid:s.valid&&mode!=='vad-tail',candidate:mode.startsWith('vad-short'),
        speechMs:mode.startsWith('vad-short') || b.page.recording.voiced?192:0,speechBeforeControlMs:0};},close(){detectorCloses++;return Promise.resolve();}};
    const b=browser(null,{noAnalysis:mode==="no-analysis",rejectBitrate:mode==="bitrate-fallback",createVAD:mode.startsWith('vad-') || ['idle','quantized-clock'].includes(mode)?()=>fake:undefined});b.silence();
    b.page.reference="synthetic-reference";b.page.config.segmentMs=500;
    const traffic=[], state={audio:[],replies:[],turns:[],audio_available:true,speech_available:true,complete:false};
    b.page.api=async(route,body,headers)=>{
      traffic.push(route);
      if(route==="audio") {
        const sha=[...new Uint8Array(await webcrypto.subtle.digest("SHA-256",await body.arrayBuffer()))].map(n=>n.toString(16).padStart(2,"0")).join("");
        state.audio.push({utterance_id:headers["X-Bot-Utterance-ID"],sha256:sha,byte_count:body.size,mime:body.type,
          state:"failed",error_code:["no-text","hangup-race"].includes(mode)?"empty_transcription":"not_applied"});
        return {audio_received:true,utterance_id:headers["X-Bot-Utterance-ID"]};
      }
      if(route==="end") {state.close_requested=true;state.ended=true;state.close_state=state.audio.every(a=>a.error_code==="empty_transcription")?"closed_no_text":"audio_blocked";return state;}
      if(route==="state") return {...state};
      throw new Error("unexpected capture route");
    };
    if(mode==="low" || (mode.startsWith('vad-')&&!['vad-short-zero','vad-failure-zero'].includes(mode))) b.level(0.00001);
    // These races verify delivery of voiced bytes when a control wins an
    // auto-limit stop; confirmed silence now has its own discard facets.
    if(["manual","bitrate-fallback","no-text","manual-race","hangup-race","vad-manual-race","vad-roundoff"].includes(mode))b.voice();
    if(mode==="NaN") b.level(NaN);
    if(mode==="gap-after-voice") {b.voice();b.page.config.silenceMs=200;b.page.config.segmentMs=1000;}
    if(mode==='quantized-clock')Object.defineProperty(b.win.AudioContext.prototype,'currentTime',{get:()=>Math.floor(b.clock.time/10)/100});
    if(mode==='vad-roundoff')Object.defineProperty(b.win.AudioContext.prototype,'currentTime',{get:()=>260736/48000});
    await b.page.start();
    assert.deepEqual(b.captureRequests[0],{audio:{echoCancellation:{ideal:true},noiseSuppression:{ideal:true},autoGainControl:{ideal:true},channelCount:{ideal:1}}});
    assert.equal(b.recorders[0].options.audioBitsPerSecond,mode==="bitrate-fallback"?undefined:24000);
    assert.deepEqual(b.page.captureSettings,{echoCancellation:true,noiseSuppression:false,autoGainControl:true});
    if(mode==='quantized-clock') {
      // Two valid wall-clock observations can share one audio quantum at
      // start and stop. They must not turn proven idle into an audio upload.
      b.clock.advance(2);b.page.sampleRecording(b.page.recording);
      b.recorders[0].beforeStop=()=>b.clock.advance(2);
    }
    if(mode.startsWith('vad-short')) {event('candidate');event('endpoint');event('endpoint');}
    if(mode.startsWith('vad-failure')) fake.fail(signal);
    if(mode==="stalled") b.stall();
    if(mode==="track") b.streams[0].getTracks()[0].muted=true;
    if(mode==="final-tail") b.recorders[0].beforeStop=()=>b.level(0.00001);
    if(mode==="manual" || mode==="bitrate-fallback" || mode==="no-text" || mode==="vad-roundoff") await b.page.submitSegment();
    else if(mode==="gap-after-voice") {
      b.silence();b.clock.advance(600);await new Promise(resolve=>setImmediate(resolve));
      assert.equal(b.page.recording?.recorder,b.recorders[0],"analysis gap became silence stop");
      assert(!b.nodes.notice.textContent.includes("停顿检测不可用"),"small scheduling jitter must remain usable");
      b.clock.advance(100);b.clock.advance(100);
    } else {
      for(let i=0;i<4;i++){b.clock.advance(100);await new Promise(resolve=>setImmediate(resolve));}
      b.clock.advance(100);
      if(mode==="manual-race" || mode==="vad-manual-race") void b.page.finishSegment("manual");
      if(mode==="hangup-race") void b.page.hangup();
    }
    if(mode==="hangup-race") await b.page.endTask;
    await until(()=>!b.page.sendTask && (b.recorders.length>1 || !!b.page.round || b.page.ending),"capture facet did not settle: "+mode);
    if(["idle","vad-idle","quantized-clock","low","final-tail","vad-loading","vad-failure","vad-tail","vad-failure-zero"].includes(mode)) {
      assert.equal(traffic.filter(x=>x==="audio").length,0,"reliable idle uploaded bytes");
      assert.equal(b.recorders.length,2);assert.equal(b.permissions,1,"idle reacquired permission");
      assert.equal(b.recorders[1].stream,b.recorders[0].stream);
      const owned=b.page.recording;b.recorders[0].onstop();
      assert.equal(b.page.recording,owned,"old onstop cleared new recorder");
      // A simultaneous final control retains the currently owned voiced tail.
      b.voice();await b.page.hangup();assert.equal(traffic.filter(x=>x==="audio").length,1);
    } else if(mode==="no-text") {
      assert.equal(traffic.filter(x=>x==="audio").length,1);
      assert.equal(b.recorders.length,2);assert.equal(b.permissions,1);
      await Promise.all([b.page.refresh(),b.page.start(),b.page.listen()]);
      assert.equal(b.recorders.length,2,"confirmed no-text duplicated capture");
      b.page.releaseMic();
    } else {
      assert.equal(traffic.filter(x=>x==="audio").length,1,"fallback lost complete bytes: "+mode);
      assert.equal(b.recorders.length,1);
    }
    if(mode==="hangup-race") {
      await until(()=>!b.page.endTask,"hangup did not finish");
      assert.match(b.nodes.status.textContent,/已结束，没有可整理文字/);
      assert.equal(b.page.poll,null);assert.equal(b.page.server.complete,false);
    }
  }
  for(const mode of ["idle","quantized-clock","low","manual","bitrate-fallback","no-analysis","stalled","track","NaN","final-tail","manual-race","hangup-race","gap-after-voice","no-text","vad-idle","vad-short","vad-loading","vad-failure","vad-tail","vad-manual-race","vad-short-zero","vad-failure-zero","vad-roundoff"]) await capture(mode);

  // Concentrated browser-only race facets within this existing flow. Backend
  // ownership/timeout/expiry/limits are exercised by the real TTS safety flow.
  for (const action of ["stop", "hangup", "suspend", "running"]) {
    const held=browser(null);held.page.reference="synthetic-reference";
    const round={id:"synthetic-round"};held.page.round=round;
    held.page.server={audio:[],replies:[],turns:[],complete:false};
    const traffic=[];let resolveSpeech, finished=false;
    const pending=new Promise(resolve=>{resolveSpeech=resolve;});
    held.page.api=async(route,body)=>{
      traffic.push({route,body});
      if(route==="speech") return finished?{generation_id:"reply:wait",state:"succeeded"}:pending;
      if(route==="speech-audio") return new Blob(["synthetic wav"],{type:"audio/wav"});
      if(route==="playback") return {recorded:true,...body};
      if(route==="end") return {ended:true};
      if(route==="state") return {audio:[],replies:[],turns:[],ended:true,complete:true};
      throw new Error("unexpected browser wait-facet route");
    };
    held.page.speak("reply:wait",{playback_revision:0,body:"synthetic"});
    const speech=held.page.speech;
    assert.equal(speech.fetching,true);assert.deepEqual(traffic[0].body,{generation_id:"reply:wait",wait_ms:8000});
    if(action==="running") {
      resolveSpeech({generation_id:"reply:wait",state:"running"});
      await until(()=>!speech.fetching,"running wait response did not settle");
      assert.equal(speech.terminal,false);assert.equal(held.page.playbackPaused,false);
      assert.equal(held.utterances.length,0);assert.equal(held.page.round,round);
      finished=true;await held.page.loadSpeech(speech);
      assert.equal(held.utterances.length,1);
      await held.page.stopSpeaking(false);
    } else {
      await (action==="stop"?held.page.stopSpeaking(false):action==="hangup"?held.page.hangup():held.page.suspend());
      resolveSpeech({generation_id:"reply:wait",state:"succeeded"});
      await until(()=>!speech.fetching,"late speech response did not settle");
      assert.equal(held.utterances.length,0);assert.equal(held.urls.size,0);
      assert.equal(traffic.filter(r=>r.route==="speech-audio").length,0);
      assert.equal(traffic.filter(r=>r.route==="speech").length,1);
      assert.equal(held.permissions,0);
    }
  }
  // Bare/expired pages can test a fixed sentence with no call or microphone.
  const noTraffic=requests.length;
  const missing=browser("",{missingMedia:true});
  assert.match(missing.nodes["speech-diagnostic"].textContent,/api-unavailable/);
  missing.nodes["test-voice"].click();assert.equal(missing.utterances.length,0);
  const sample=browser("");sample.blockNextPlay();sample.nodes["test-voice"].click();
  assert.equal(sample.utterances.length,1);assert.equal(sample.utterances[0].clicked,true);
  assert.equal(sample.utterances[0].src,"/bot-call/fixed-sample.wav");
  await until(()=>sample.nodes["resume-voice"].disabled===false,"permission recovery was not offered");
  assert.equal(sample.nodes["main-action"].hidden,false);assert.equal(sample.nodes["main-label"].textContent,"声音被浏览器拦住了，点击恢复声音");
  sample.nodes["main-action"].click();assert.equal(sample.utterances.length,1);assert.equal(sample.utterances[0].plays,2);
  assert.equal(sample.utterances[0].clicked,true);sample.utterances[0].onplaying();sample.utterances[0].onended();
  const empty=browser("");empty.nodes["test-voice"].click();empty.clock.advance(10000);
  assert.ok(empty.page.speech,"timeout released unknown playback");
  assert.match(empty.nodes["speech-diagnostic"].textContent,/尚未确认/);
  empty.utterances[0].pause=()=>{empty.utterances[0].paused=false;};
  await empty.page.stopSpeaking();await empty.page.start();empty.page.testVoice();
  assert.equal(empty.page.speechBlocked,true);assert.equal(empty.permissions,0);
  assert.equal(empty.utterances.length,1,"unconfirmed stop allowed another sample");
  assert.ok(empty.page.speech?.media,"unconfirmed stop lost player ownership");
  assert.equal(empty.nodes['main-action'].disabled,false);
  await empty.page.hangup();assert.ok(empty.page.speech?.media,"hangup dropped the unconfirmed player");
  empty.utterances[0].pause=()=>{empty.utterances[0].paused=true;};
  await empty.page.stopSpeaking();assert.equal(empty.page.speech,null);assert.equal(empty.page.speechBlocked,false);
  assert.equal(empty.permissions,0);
  assert.equal(requests.length,noTraffic,"fixed sample accessed a paid/call endpoint");
  for (const local of [missing,sample,empty]) {
    assert.equal(local.permissions,0);assert.equal(local.recorders.length,0);
    assert.equal(local.page.observations.length,0);assert.equal(local.page.seen.size,0);
  }
  const b=browser(fixture.tokens.A);
  assert.equal(b.win.speechSynthesis,undefined);assert.equal(b.win.SpeechSynthesisUtterance,undefined);
  const initial=requests.length;
  assert.equal(b.permissions,0);assert.equal(requests.length,initial);assert.equal(b.utterances.length,0);
  await b.page.retry();await b.page.toggleMute();await b.page.toggleMute();
  assert.equal(b.permissions,0,"retry/unmute bypassed first Start");
  assert.equal(requests.length,initial,"pre-start controls entered a call");
  assert.equal(b.recorders.length,0);
  assert.deepEqual(b.replaced,["/bot-call/"]);assert.equal(b.win.location.hash,"");
  const html=await fetchASGI("/bot-call/");assert.equal(html.status,200);
  assert.match(await html.text(),/id="start"/);
  const traversal=await fetchASGI("/bot-call/..%2fAGENTS.md");assert.equal(traversal.status,404);
  b.nodes.start.click();await until(()=>/听取|等待声音/.test(b.nodes.status.textContent),"did not enter listening");
  assert.equal(b.permissions,1);assert.equal(b.recorders.length,1);
  b.page.testVoice();assert.equal(b.utterances.length,0,"sample raced recording");
  b.recorders[0].data("first");
  assert.equal(requests.filter(r=>r.path.endsWith("/audio")).length,0,"timeslice was uploaded independently");
  const firstWait=b.holdNextSpeech();
  b.blockNextPlay();for(let i=0;i<4;i++)b.clock.advance(100);b.silence();for(let i=0;i<13;i++)b.clock.advance(100);
  await until(async()=>{await b.page.refresh();return firstWait.entered;},"first speech wait did not enter");
  const waitTraffic=requests.filter(r=>r.path.endsWith("/speech")).length;
  await b.page.toggleMute();await b.page.toggleMute();await b.page.refresh();
  assert.equal(b.recorders.length,1);assert.equal(b.utterances.length,0);
  assert.equal(requests.filter(r=>r.path.endsWith("/speech")).length,waitTraffic,"mute/poll duplicated speech wait");
  firstWait.release();
  await until(async()=>{await b.page.refresh();return b.utterances.length===1;},"new reply not prepared");
  await until(()=>!b.nodes["resume-voice"].disabled,"actual audio permission failure was not recoverable");
  const permissionTraffic=requests.length;
  b.nodes["resume-voice"].click();
  assert.equal(b.utterances.length,1);assert.equal(b.utterances[0].clicked,true);
  assert.equal(requests.length,permissionTraffic,"permission resume resynthesized or fetched audio");
  await b.page.toggleMute();assert.equal(b.nodes.mute.attributes["aria-pressed"],"true");
  assert.ok(b.streams[0].getTracks()[0].stopped);
  // Unmute while TTS is owned cannot start capture over the browser's voice.
  await b.page.toggleMute();assert.equal(b.recorders.length,1);

  assert.equal(b.recorders.length,1);assert.equal(b.streams[0].getTracks()[0].enabled,false);
  b.page.testVoice();assert.equal(b.utterances.length,1,"sample raced a reply");
  const first=b.utterances[0];assert.match(first.src,/^blob:/);
  assert.ok(b.nodes.captions.children.some(row=>row.children[1].textContent.includes("<img")),"reply was not rendered as literal text");
  first.onplaying();await settleSpeech(b);
  assert.match(b.nodes.status.textContent,/正在播放/);
  losePlaybackAck=1;
  b.nodes['main-action'].click(); // fake cancel synchronously emits end; must remain stopped.
  await until(()=>b.recorders.length===2,"did not resume after actual stop");
  first.onended?.();first.onerror?.();await settleSpeech(b);
  const state=await b.page.api("state");
  const bot=state.turns.find(t=>t.role==="assistant");
  assert.equal(bot.playback_state,"stopped");assert.equal(bot.playback_revision,2);
  const receipts=requests.filter(r=>r.path.endsWith("/playback")).map(r=>JSON.parse(Buffer.from(r.body,"base64")));
  assert.ok(receipts.some(r=>r.state==="started" && Number.isFinite(r.first_sound_ms) && r.first_sound_ms>=0),"missing first-sound observation");
  assert.ok(receipts.filter(r=>r.state!=="started").every(r=>r.first_sound_ms===undefined));

  assert.deepEqual(receipts.map(r=>r.state),["started","stopped"]);
  // A browser TTS failure is a persisted failed observation, not a fake end.
  // A stopped player resumes automatically once its failed receipt is confirmed.
  b.voice();b.recorders[1].data("tts-failure");await b.page.submitSegment();
  await until(async()=>{await b.page.refresh();return b.utterances.length===2;},"second reply not prepared");
  b.setPlaybackOffline(true);b.page.pushReady=true; // WS health cannot confirm a lost HTTP receipt.
  const failedGate=b.holdNextPlayback(), failedRoundOwner=b.page.round, failed=b.utterances[1];
  failed.error={code:3};failed.onerror();await until(()=>failedGate.entered,"failed receipt was not held");
  const failureChecks=responses.filter(r=>r.path.endsWith("/state")).length;
  const pendingContinue=b.page.start();
  await until(()=>responses.filter(r=>r.path.endsWith("/state")).length>failureChecks,"Continue did not join failed playback");
  failedGate.release();await pendingContinue;
  await until(()=>!b.page.flushTask && b.page.round?.playbackFinished,"unconfirmed failure did not retain its round");
  assert.equal(b.page.speech,null);assert.equal(b.page.round,failedRoundOwner);
  const failedGeneration=b.page.server.replies.at(-1).generation_id;
  const beforeContinue=requests.filter(r=>r.path.endsWith("/audio")).length;
  assert.equal((await b.page.api("state")).turns.find(t=>t.material_id===failedGeneration).playback_state,"unknown");
  assert.equal(b.page.round,failedRoundOwner);assert.equal(b.page.playbackPaused,false);
  assert.equal(b.recorders.length,2,"Continue captured before the receipt was confirmed");
  assert.equal(b.recorders.length,2,"TTS failure automatically resumed capture");
  await b.page.retry();assert.equal(b.recorders.length,2,"Retry bypassed explicit Continue");
  failed.onended?.();failed.onplaying?.();await settleSpeech(b);await b.page.refresh().catch(()=>{});
  assert.match(b.nodes["speech-diagnostic"].textContent,/decode/);
  assert.match(b.nodes["speech-diagnostic"].textContent,/decode/);
  b.setPlaybackOffline(false);
  b.clock.advance(b.page.config.pollMs);
  await until(()=>b.recorders.length===3,"online WS suppressed automatic receipt recovery");
  b.page.pushReady=false;
  assert.equal(requests.filter(r=>r.path.endsWith("/audio")).length,beforeContinue,"Continue re-uploaded an old segment");
  const failedState=await b.page.api("state");
  assert.equal(failedState.turns.find(t=>t.material_id===failedGeneration).playback_state,"failed");
  assert.match(b.nodes["speech-diagnostic"].textContent,/decode/,"listening lost the error code");
  // The third reply is held by an injected fake. Neither Start nor Retry
  // can race it, re-upload its audio, or create another model task.
  b.recorders[2].data("reply-failure");await b.page.submitSegment();
  await until(async()=>{await b.page.refresh();return b.page.server.replies.at(-1)?.state==="running";},"reply did not enter running");
  const failedRound=b.page.round.id;
  const beforeFailureAudio=requests.filter(r=>r.path.endsWith("/audio")).length;
  await Promise.all([b.page.start(),b.page.retry(),b.page.refresh()]);
  assert.equal(b.recorders.length,3);assert.equal(b.page.round.id,failedRound);
  assert.equal(requests.filter(r=>r.path.endsWith("/audio")).length,beforeFailureAudio);
  await globalThis.requestASGI({control:"release_reply"});
  await until(async()=>{await b.page.refresh();return b.page.round?.terminal;},"failed reply did not allow explicit continue");
  assert.equal(b.recorders.length,3,"failed reply auto-started another round");
  assert.match(b.nodes.status.textContent,/文字已保存/);
  assert.match(b.nodes.notice.textContent,/不会重发上一轮/);
  assert.ok(b.page.server.turns.some(t=>t.role==="user" && t.body.includes("reply-failure")));
  assert.equal(b.page.server.replies.filter(r=>r.utterance_id===failedRound).length,1);
  b.nodes["main-action"].click();await until(()=>b.recorders.length===4,"Continue did not start a new round");
  assert.notEqual(b.page.recording.id,failedRound);
  b.recorders[3].data("recovered");await b.page.submitSegment();
  await until(async()=>{await b.page.refresh();return b.utterances.length===3;},"next round did not produce a reply");
  const recovered=b.utterances[2];recovered.onplaying();await settleSpeech(b);
  const receiptGate=b.holdNextPlayback();recovered.onended();
  await until(()=>receiptGate.entered,"playback receipt was not held");
  const pendingChecks=responses.filter(r=>r.path.endsWith("/state")).length;
  let joined=false,checked=false;
  const joining=b.page.start().then(()=>{joined=true;}), checking=b.page.retry().then(()=>{checked=true;});
  await until(()=>responses.filter(r=>r.path.endsWith("/state")).length>=pendingChecks+1,"Continue/Retry did not share pending state read");
  assert.equal(responses.filter(r=>r.path.endsWith("/state")).length,pendingChecks+1,"same-owner refresh duplicated GET");
  assert.equal(joined,false);assert.equal(checked,false,"shared GET bypassed pending playback ACK");
  assert.equal(b.recorders.length,4,"unconfirmed old playback released a new recorder");
  receiptGate.release();await Promise.all([joining,checking]);
  await until(()=>b.recorders.length===5,"successful recovery did not resume automatic listening");
  assert.equal(b.permissions,3,"playback completion and Continue duplicated microphone ownership");
  await settleSpeech(b);
  // Final dataavailable is appended after stop, then ACK loss is recovered by ID.
  b.recorders[4].data("last");loseAudioAck=1;
  const before=requests.length, mediaGate=b.holdNextSpeech();
  await b.page.submitSegment();
  await until(async()=>{await b.page.refresh();return mediaGate.entered;},"final audio response did not enter its gate");
  const lateSpeech=b.page.speech;
  b.nodes.end.click();
  await until(async()=>{await b.page.refresh();return b.page.server.complete;},"final review did not complete");
  assert.equal(b.page.server.submission_status,"submitted");
  assert.equal(b.page.server.delivery_status,"unverified");
  assert.ok(b.page.server.page_states.every(s=>s==="submitted"));
  assert.match(b.nodes.status.textContent,/回顾已提交/);
  assert.match(b.nodes.notice.textContent,/来源聊天确认/);
  const closing=requests.slice(before);
  const audioIndex=closing.findIndex(r=>r.path.endsWith("/audio"));
  const endIndex=closing.findIndex(r=>r.path.endsWith("/end"));
  assert.ok(audioIndex>=0 && endIndex>audioIndex,"end arrived before final clip receipt");
  const audios=requests.filter(r=>r.path.endsWith("/audio"));
  assert.deepEqual(audios.map(r=>Buffer.from(r.body,"base64").toString()),["first-tail🙂","tts-failure-tail🙂","reply-failure-tail🙂","recovered-tail🙂","last-tail🙂"]);
  mediaGate.release();await until(()=>!lateSpeech.fetching,"late audio fetch did not settle");
  assert.equal(b.utterances.length,3,"hangup played a late audio result");
  assert.equal(b.urls.size,0,"late audio allocated an object URL after hangup");
  assert.ok(requests.filter(r=>r.path.endsWith("/speech")).every(r=>JSON.parse(Buffer.from(r.body,"base64")).wait_ms===8000));
  await until(()=>!b.page.endTask,"hangup did not settle before sample");
  const playbackCount=requests.filter(r=>r.path.endsWith("/playback")).length;
  b.nodes["test-voice"].click();assert.equal(b.utterances.length,4,"ended call prevented local sample");
  b.utterances[3].onended();
  assert.equal(requests.filter(r=>r.path.endsWith("/playback")).length,playbackCount,"sample overwrote old failed generation");
  assert.equal((await b.page.api("state")).turns.find(t=>t.material_id===failedGeneration).playback_state,"failed");
  assert.equal(b.nodes.start.disabled,true);assert.ok(b.streams.every(s=>s.getTracks().every(t=>t.stopped)));assert.equal(b.urls.size,0,"object URLs survived hangup");
  assert.ok([...b.storage.values()].every(value=>/^[0-9a-f]{64}$/.test(value)),"secret persisted in tab storage");
  const reloaded=browser("",{storage:b.storage});await reloaded.page.connect();
  assert.equal(reloaded.permissions,0);assert.equal(reloaded.utterances.length,0,"refresh spoke history");
  return b;
}

async function ackIsolationPermissionAndInterruption() {
  await realBackendChunkFacet(); // first: the server admits one open call at a time, and the previous scenario has hung up
  const c=browser(fixture.tokens.C);await c.page.start();
  c.recorders[0].data("retained");failBeforeAudio=2;
  await c.page.submitSegment();
  assert.ok(c.page.clip);const id=c.page.clip.id;
  await c.page.hangup();
  assert.ok(c.page.clip);assert.equal(c.page.clip.id,id);
  assert.equal(requests.filter(r=>r.path.endsWith("/end") && r.headers["x-bot-call-reference"]===c.page.reference).length,0,"unconfirmed last segment was called complete");
  await c.page.retry();await until(async()=>{await c.page.refresh();return c.page.server.complete;},"explicit retry did not finish retained clip");
  const sent=requests.filter(r=>r.path.endsWith("/audio") && r.headers["x-bot-call-reference"]===c.page.reference);
  assert.equal(new Set(sent.map(r=>r.headers["x-bot-utterance-id"])).size,1);
  assert.equal(new Set(sent.map(r=>r.body)).size,1);
  // B enters and overwrites shared cookie. Old C reference must never adopt B.
  // Capability is a synthetic state projection here, simulating an endpoint
  // with audio disabled. Even explicit Start must not request permission.
  const late=browser(fixture.tokens.B,{pendingPermission:true,audioAvailable:false});
  await late.page.start();assert.equal(late.permissions,0);assert.equal(late.recorders.length,0);
  assert.match(late.nodes.status.textContent,/语音暂不可用/);
  late.clock.advance(15000);await new Promise(resolve=>setImmediate(resolve));
  assert.equal(requests.filter(r=>r.path.endsWith("/heartbeat") && r.headers["x-bot-call-reference"]===late.page.reference).length,0,"audio unavailable kept itself online");
  late.enableAudio();
  const starting=late.page.start();await until(()=>late.permissions===1,"permission was not requested");
  const beforeChecks=responses.filter(r=>r.path.endsWith("/state") && r.headers["x-bot-call-reference"]===late.page.reference).length;
  const checking=late.page.retry(), continuing=late.page.start();
  await until(()=>responses.filter(r=>r.path.endsWith("/state") && r.headers["x-bot-call-reference"]===late.page.reference).length>=beforeChecks+2,"retry did not reach pending listen");
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(late.permissions,1,"pending permission was requested twice");
  const hbBefore=responses.filter(r=>r.path.endsWith("/heartbeat") && r.headers["x-bot-call-reference"]===late.page.reference).length;
  // Frequent state polling (e.g. a long model/STT wait) must not postpone HB.
  for (let i=0;i<3;i++) {late.clock.advance(5000);await late.page.refresh(false);}
  await until(()=>responses.filter(r=>r.path.endsWith("/heartbeat") && r.headers["x-bot-call-reference"]===late.page.reference).length>hbBefore,"foreground pending permission did not heartbeat");
  assert.equal(late.permissions,1,"heartbeat opened another microphone");
  await c.page.retry();assert.equal(c.page.locked,true);
  assert.equal((await late.page.api("state")).turns.length,0,"old tab polluted B");
  await until(()=>!late.page.heartbeatTask,"heartbeat did not settle");
  await late.page.suspend();await late.page.hangup();assert.equal(late.recorders.length,0);
  const hbStopped=responses.filter(r=>r.path.endsWith("/heartbeat") && r.headers["x-bot-call-reference"]===late.page.reference).length;
  late.clock.advance(30000);await new Promise(resolve=>setImmediate(resolve));
  assert.equal(responses.filter(r=>r.path.endsWith("/heartbeat") && r.headers["x-bot-call-reference"]===late.page.reference).length,hbStopped,"background/hangup continued heartbeats");
  late.allowPermission();await Promise.all([starting,checking,continuing]);
  assert.equal(late.permissions,1);assert.equal(late.streams.length,1);
  assert.ok(late.streams.every(s=>s.getTracks().every(t=>t.stopped)),"late permission reopened mic");
  assert.equal(late.recorders.length,0);
  const otherCookie=cookie;
  // Background stops a full recording and submits it; unknown STT is not replayed.
  const d=browser(fixture.tokens.D);await d.page.start();d.recorders[0].data("unknown");
  // Simulate browser/device interruption before pagehide. Complete stop still
  // retains its final event; the page must not keep claiming it is listening.
  d.recorders[0].stop();await until(()=>!!d.page.clip,"interrupted recording was not retained");
  assert.match(d.nodes.status.textContent,/录音已中断/);
  d.doc.hidden=true;d.doc.listeners.visibilitychange();
  await until(()=>d.page.clip===null && !!d.page.round,"background clip was not accepted");
  assert.ok(d.streams.every(s=>s.getTracks().every(t=>t.stopped)));
  await d.page.refresh();
  const count=requests.filter(r=>r.path.endsWith("/audio") && r.headers["x-bot-call-reference"]===d.page.reference).length;
  d.doc.hidden=false;await d.page.start();await d.page.retry();
  assert.equal(d.recorders.length,1);assert.equal(d.utterances.length,0);
  assert.match(d.nodes.notice.textContent,/当前不能继续转写/);
  assert.equal(requests.filter(r=>r.path.endsWith("/audio") && r.headers["x-bot-call-reference"]===d.page.reference).length,count);
  await d.page.hangup();await d.page.refresh();
  assert.equal(d.page.server.close_state,"audio_blocked");assert.equal(d.page.server.ended,false);assert.equal(d.page.server.complete,false);
  const orphan=d.page.round, materials=JSON.stringify(d.page.server);
  assert.ok(orphan);cookie=otherCookie;
  await d.page.retry();assert.equal(d.page.locked,true);
  const localTraffic=requests.length, localPermissions=d.permissions, localRecorders=d.recorders.length;
  assert.equal(d.nodes.start.disabled,true);assert.equal(d.nodes["test-voice"].disabled,false);
  d.nodes["test-voice"].click();
  assert.equal(d.utterances.length,1);assert.equal(d.utterances[0].clicked,true);
  assert.equal(d.utterances[0].src,"/bot-call/fixed-sample.wav");d.utterances[0].onended();
  await d.page.start();await d.page.retry();
  assert.equal(requests.length,localTraffic,"locked sample reconnected or wrote playback");
  assert.equal(d.permissions,localPermissions);assert.equal(d.recorders.length,localRecorders);
  assert.equal(d.page.round,orphan);assert.equal(JSON.stringify(d.page.server),materials);
  assert.equal(d.page.locked,true);assert.equal(d.nodes.start.disabled,true);
  // Future policy: explicit decision confirmed by original job, without STT
  // replay, and a delayed/lost old ACK must never clear a newer round.
  const f=browser(fixture.tokens.F);await f.page.start();f.recorders[0].data("missing-first");
  await f.page.submitSegment();
  await until(async()=>{await f.page.refresh();return f.page.round?.retainAvailable;},"missing action was not offered");
  const missingRound=f.page.round;
  assert.equal(f.nodes["main-label"].textContent,"这段没听清，点击保留本段并继续");
  const fUploads=()=>requests.filter(r=>r.path.endsWith("/audio") && r.headers["x-bot-call-reference"]===f.page.reference).length;
  f.setResolutionOffline(true);await f.page.start();
  assert.equal(f.page.round,missingRound);assert.equal(f.recorders.length,1);assert.equal(fUploads(),1);
  f.setResolutionOffline(false);
  const resolutionGate=f.holdResolution(true);
  const firstContinue=f.page.start(),doubleContinue=f.page.start();
  await until(()=>resolutionGate.entered,"resolution did not reach accepted gate");
  await f.page.refresh();await until(()=>f.recorders.length===2,"confirmed resolution did not resume once");
  assert.equal(f.permissions,1);
  f.recorders[1].data("recognized-after-gap");await f.page.submitSegment();
  const newRound=f.page.round;assert.notEqual(newRound.id,missingRound.id);
  resolutionGate.release();await Promise.all([firstContinue,doubleContinue]);
  assert.equal(f.page.round,newRound,"late old resolution ACK cleared the new round");
  assert.equal(f.recorders.length,2);assert.equal(fUploads(),2);
  await f.page.hangup();
  await until(async()=>{await f.page.refresh();return f.page.server.complete;},"partial review did not complete");
  assert.match(f.page.server.review_body,/^材料说明：本次仅根据已识别文字整理，有1段未识别片段/);
  assert.equal(f.page.server.audio[0].state,"uncertain");
  assert.equal(f.page.server.audio[0].resolution.decision,"retained_missing");
  assert.equal(f.page.poll,null);
  const g=browser(fixture.tokens.G);await g.page.start();g.recorders[0].data("all-missing");await g.page.submitSegment();
  await until(async()=>{await g.page.refresh();return g.page.round?.retainAvailable;},"all-missing result did not settle");
  await g.page.hangup();
  await until(async()=>{await g.page.refresh();return g.page.server.close_state==="closed_no_text";},"all-missing did not close");
  assert.equal(g.page.server.complete,false);assert.equal(g.page.server.review_body,null);assert.equal(g.page.poll,null);
  assert.match(g.nodes.notice.textContent,/未识别音频仍保留/);
  // A new real authorized page stops on size overflow and retains the
  // complete clip; no truncated POST or false final ACK.
  const isolated=browser(fixture.tokens.E,{maxAudioBytes:2});await isolated.page.start();
  isolated.recorders[0].data("oversized");
  await until(()=>!!isolated.page.clip,"oversize did not stop capture");
  assert.equal(await isolated.page.sendClip(),false);assert.ok(isolated.page.clip.blob.size>2);
  assert.match(isolated.nodes.status.textContent,/尚未提交/);
  assert.equal(requests.filter(r=>r.path.endsWith("/audio") && r.headers["x-bot-call-reference"]===isolated.page.reference).length,0);
  await isolated.page.suspend();
}
async function rejectedRecordingFacets() {
  for (const kind of ['overflow','recorder_error','stop_threw','empty']) {
    const b=browser(''),p=b.page,n=b.nodes,requests=[],discarded=[];
    p.started=true;p.reference='local-fixture';p.server={audio_available:true,speech_available:true};
    p.api=async(route,body)=>{requests.push({route,body});return {recorded:true};};
    p.refresh=async()=>{};p.connect=async()=>{};
    const blob=new Blob(kind==='empty'?[]:['kept-bytes'],{type:'audio/webm'});
    const clip={id:'rejected',blob,durationMs:32000,mime:blob.type,
      overflow:kind==='overflow',error:['recorder_error','stop_threw'].includes(kind),stopThrew:kind==='stop_threw',
      uploader:{release(){},abandon(){discarded.push(true);}}};p.clip=clip;
    assert.equal(await p.sendClip(),false);await clip.rejectionTask;
    assert.equal(p.clip,clip);assert.equal(p.round,null);assert.equal(p.presentation().phase,'error');
    p.series={id:'rejected',count:0,queue:[],limit:6};
    assert.equal(await p.submitSeriesClip(clip),false);assert.equal(p.series,null,'a never-received first clip must keep local discard recovery');
    assert.equal(n['main-action'].disabled,false);assert.equal(n['main-label'].textContent,'这段没录好，点击继续对话');
    assert.equal(requests.length,1);assert.equal(requests[0].route,'metrics');
    assert.deepEqual(requests[0].body,{generation_id:'capture:rejected',values:{client_clip_reject:{overflow:1,recorder_error:2,stop_threw:3,empty:4}[kind],client_clip_bytes:blob.size,client_clip_ms:32000}});
    assert.equal(await p.sendClip(),false);assert.equal(requests.length,1,'do not repeat diagnostics after ACK');
    let continued=0;p.start=async()=>{continued++;};n['main-action'].click();
    await until(()=>continued===1,'discard click did not continue');assert.equal(p.clip,null);assert.equal(discarded.length,1);
    assert.equal(requests.length,1,'discard never replays with a new id');
    // A rejected final clip still permits an explicit discard followed by the normal end request.
    p.clip={...clip,id:'final',rejectionSent:false,rejectionTask:null};await p.hangup();
    assert.equal(requests.some(r=>r.route==='end'),false);assert.equal(p.ending,true);
    assert.equal(n['main-label'].textContent,'这段没录好，点击丢弃并挂断');assert.equal(n['main-action'].disabled,false);
    n['main-action'].click();await until(()=>requests.some(r=>r.route==='end')&&!p.endTask,'discard did not finish hangup');
    assert.equal(p.clip,null);assert.equal(continued,1);assert(!requests.some(r=>['audio','audio-commit'].includes(r.route)));
  }
  // Error event with inactive recorder: final data/stop are still queued.
  const b=browser(''),p=b.page;p.started=true;p.server={audio_available:true,speech_available:true};
  p.refresh=async()=>{};await p.listen();const r=p.recording,rec=r.recorder;
  rec.data('body');rec.state='inactive';let stops=0;rec.stop=()=>{stops++;throw Error('must wait for queued stop');};
  rec.onerror();assert.equal(stops,0);assert.equal(p.clip,null);
  rec.data('-final');await rec.onstop();await r.done;
  assert.equal(p.clip.blob.size,new Blob(['body-final']).size);assert.equal(p.clip.error,true);
  assert.equal(p.clip.stopThrew,undefined);assert(p.clip.durationMs>=0);p.releaseMic();
  // An inactive recorder that never emits its final stop also has a bounded exit.
  const c=browser(''),q=c.page;q.started=true;q.server={audio_available:true,speech_available:true};
  q.connect=async()=>{};q.refresh=async()=>{};q.reference='local-only';q.api=async()=>({recorded:true});
  await q.listen();const stuck=q.recording;stuck.recorder.data('prefix');stuck.recorder.state='inactive';
  const pending=q.submitSegment();c.clock.advance(5000);await pending;
  assert.equal(q.recording,null);assert.equal(q.clip.error,true);assert.equal(q.clip.stopThrew,false);
  assert.equal(q.presentation().action,'discard');assert.equal(q.clip.blob.size,6);
  c.nodes['main-action'].click();await until(()=>!!q.recording&&!q.startTask,'explicit discard must reopen the actual microphone');
  assert.notEqual(q.recording.id,stuck.id);assert.equal(q.clip,null);assert.equal(c.permissions,2);
  q.releaseMic();
}

async function realBackendChunkFacet() {
  // The real server: chunks are staged, the commit verifies length and SHA-256, then the normal STT/reply flow runs.
  cookie="";  // a fresh browser: the shared cookie jar still holds the previous page's session
  const b=browser(fixture.tokens.I,{chunk:true}),p=b.page,before=requests.length;
  await p.start();assert.ok(p.recording,"no recording: "+p.apiDiagnostic+" / "+p.message+" / "+p.notice+" / "+JSON.stringify(p.server&&{a:p.server.audio_available,s:p.server.speech_available,l:p.server.limits}));const clipId=p.recording.id;
  b.voice();b.recorders[0].data("part-one");
  for(let i=0;i<4;i++){b.clock.advance(100);await new Promise(resolve=>setImmediate(resolve));}
  await until(()=>requests.slice(before).some(r=>r.path.endsWith("/audio-chunk")),"no chunk reached the real server");
  b.recorders[0].data("part-two");
  await p.submitSegment();await until(()=>!!p.round && !p.sendTask,"the real server did not confirm the commit");
  const mine=requests.slice(before).filter(r=>r.headers["x-bot-call-reference"]===p.reference),count=suffix=>mine.filter(r=>r.path.endsWith(suffix)).length;
  assert.ok(count("/audio-chunk")>=1,"chunks while speaking; final flush travels with commit");assert.equal(count("/audio-commit"),1);
  assert.equal(count("/audio"),0,"no whole-clip upload against a server that has the routes");
  assert.equal(p.round.id,clipId);assert.equal(p.apiFailure,null);
  await until(async()=>{await p.refresh();return p.server.turns.some(t=>t.role==="user" && t.body.includes("part-onepart-two-tail🙂"));},"the committed audio was not transcribed from the exact bytes");
  assert.ok(p.round.metrics.client_upload_chunks>=1);assert.ok(Number.isSafeInteger(p.round.metrics.client_upload_tail));
  await p.hangup();await until(()=>!p.endTask,"hangup did not settle");
}


async function pushRecoveryFacets() {
  // First sentence is already ready, but an older long poll has no new phase
  // change to wake it. Disconnect must cancel that read and pull immediately.
  {
    const b=browser(""),p=b.page,reads=[];let late;
    p.reference="d".repeat(64);p.e.location.origin="https://calls.example.test";
    p.round={id:"late"};p.server={audio:[{utterance_id:"late",state:"succeeded"}],
      replies:[{utterance_id:"late",state:"running"}],turns:[],complete:false};
    p.e.fetch=async(url,init)=>{
      reads.push(init);
      if(reads.length===1)return new Promise(resolve=>{late=resolve;});
      return {ok:true,status:200,json:async()=>({...p.server,revision:7})};
    };
    p.e.WebSocket=class {close(){this.onclose?.();}};
    const old=p.refresh(true,true);
    await until(()=>reads.length===1,"long recovery read missing");
    assert.equal(reads[0].headers["X-Bot-Call-Wait-Ms"],"8000");
    p.ensurePush();p.socket.close();
    await old;await until(()=>p.server.revision===7&&!p.refreshTask,"disconnect waited for old long poll");
    assert.equal(reads.length,2);assert.equal(p.server.revision,7);
    assert.equal(reads[1].headers["X-Bot-Call-Wait-Ms"],undefined);
    assert.equal(reads[0].signal.aborted,true);
    late({ok:true,status:200,json:async()=>({complete:true})});
    p.stopPush();p.e.clearTimeout(p.poll);
  }

  for(const idle of [false,true]) {
    const b=browser(""),p=b.page,sockets=[],reads=[];
    p.reference="c".repeat(64);p.e.location.origin="https://calls.example.test";
    p.server={audio:[],replies:[],turns:[],complete:false};
    if(!idle)p.round={id:"recover"};
    p.e.fetch=async(url,init)=>{
      assert(url.endsWith("/state"));
      reads.push({at:b.clock.time,headers:init.headers});
      return {ok:true,status:200,json:async()=>p.server};
    };
    p.e.WebSocket=class {
      constructor(){this.readyState=0;sockets.push(this);}
      close(){this.readyState=3;this.onclose?.();}
    };
    p.ensurePush();
    // No native onclose arrives while the broken handshake is closing.
    sockets[0].close=()=>{sockets[0].readyState=2;};
    b.clock.advance(5000);assert.equal(p.socket,sockets[0],"handshake grace lost");
    b.clock.advance(1000);assert.equal(p.socket,null,"watchdog waited for native close");
    await until(()=>!p.refreshTask,"failure read");
    assert.equal(reads.length,1,"first failure must read immediately, including idle");
    assert.equal(reads[0].at,6000);
    assert.equal(reads[0].headers["X-Bot-Call-Wait-Ms"],undefined);
    assert.equal(!!p.poll,!idle,"idle failure started polling");
    for(let i=0;i<8;i++) {
      const timer=b.clock.tasks.get(p.pushRetry),delay=timer.at-b.clock.time;
      assert(delay>=150&&delay<=5000,"unbounded retry");
      b.clock.advance(delay);
      assert.equal(sockets.length,i+2,"parallel/missing reconnect");
      // A successful TCP/WS open without sync must still pull immediately.
      sockets.at(-1).readyState=1;sockets.at(-1).onopen();
      await until(()=>!p.refreshTask,"open read");
      assert.equal(reads.at(-1).headers["X-Bot-Call-Wait-Ms"],undefined);
      sockets.at(-1).close();
      await until(()=>!p.refreshTask,"close read");
      assert.equal(!!p.poll,!idle);
    }
    if(!idle) {
      const before=reads.length;
      b.clock.advance(500);await until(()=>!p.refreshTask,"short poll");
      assert.equal(reads.length,before+1);
      assert(reads.every(r=>r.headers["X-Bot-Call-Wait-Ms"]===undefined),"recovery used long poll");
    } else assert.equal(reads.length,9,"idle performed periodic state reads");
    p.stopPush();p.e.clearTimeout(p.poll);
    const attempts=sockets.length;b.clock.advance(6000);
    assert.equal(sockets.length,attempts,"stop left a retry");
  }
}

async function pushFallbackAndMetricsFacets() {
  const b=browser(""),p=b.page,sockets=[];let reads=0,latest=1;
  p.reference="b".repeat(64);p.e.location.origin="https://calls.example.test";
  p.round={id:"push"};p.server={complete:false};
  p.refresh=async()=>{reads++;p.server={complete:false,revision:latest};return p.server;};
  p.e.WebSocket=class {
    constructor(url,protocol) {this.url=url;this.protocol=protocol;sockets.push(this);}
    close(){this.onclose?.();}
  };
  p.ensurePush();assert.equal(sockets.length,1);assert.match(sockets[0].url,/^wss:/);
  assert.equal(sockets[0].protocol,"bot-call."+p.reference);
  sockets[0].onopen();sockets[0].onmessage({data:JSON.stringify({state:"sync"})});
  await until(()=>p.pushReady,"push initial full state missing");assert.equal(reads,1);assert.equal(p.poll,null);
  assert.equal(p.round.transportMask,2,"a successful push synchronization must mark WS use");
  sockets[0].close();assert.equal(p.pushReady,false);assert.ok(p.poll);
  await until(()=>reads===2,"first failure did not immediately read state");
  latest=7;b.clock.advance(250);await until(()=>sockets.length===2,"push reconnect missing");
  sockets[1].onopen();sockets[1].onmessage({data:JSON.stringify({state:"sync"})});
  await until(()=>p.pushReady,"reconnect did not synchronize");assert.equal(p.server.revision,7);assert.ok(reads>=2);
  assert.equal(p.round.transportMask,3,"poll fallback followed by WS must remain mixed");
  assert.equal(p.wsDisconnects,1);assert.equal(p.wsReconnects,1);
  // A watchdog queued during a blocked main thread gives queued input a turn.
  const active=sockets[1];
  b.clock.advance(20000);assert.equal(p.socket,active,'watchdog preempted queued input');
  active.onmessage({data:JSON.stringify({state:'alive',pcm:{}})});
  b.clock.advance(1000);assert.equal(p.socket,active,'PCM did not renew watchdog');
  for(let i=0;i<40;i++){b.clock.advance(1000);active.onmessage({data:JSON.stringify({state:'alive'})});}
  assert.equal(p.socket,active,'continuous activity closed socket');
  b.clock.advance(20000);assert.equal(p.socket,active);
  b.clock.advance(1000);assert.equal(p.socket,null,'genuinely idle socket never closed');
  p.stopPush();p.wsDisconnects=1;p.saveTransport();
  const reports=[];
  p.server={replies:[{generation_id:"reply:push"}]};p.round={id:"push",endedAt:0,uploadAt:100,transcriptAt:300,readyAt:700,downloadAt:800,playAt:850};
  p.api=async(route,body)=>{assert.equal(route,"metrics");reports.push(body);return {recorded:true};};
  p.sendMetrics(p.round);await until(()=>!p.round.metricsTask,"numeric observations missing");
  assert.deepEqual(reports[0].values,{client_upload:100,client_transcript:200,client_audio_ready:400,client_download:100,client_play:50,first_sound:850,client_ws_disconnects:1,client_ws_reconnects:1,client_ws_close_code:0,client_hidden_ms:0});
  p.sendMetrics(p.round);assert.equal(reports.length,1);
  const priorRound=p.round;
  p.round={id:"hidden-metrics",endedAt:p.e.now(),uploadAt:p.e.now()};
  p.hiddenAt=p.e.now();p.server={replies:[{generation_id:"reply:hidden-metrics"}]};
  p.sendMetrics(p.round);await until(()=>!p.round.metricsTask,"hidden metric receipt");
  const hiddenReports=reports.length;b.clock.advance(20);
  p.sendMetrics(p.round);await new Promise(resolve=>setImmediate(resolve));
  assert.equal(reports.length,hiddenReports,"hidden clock caused a self-resending metrics loop");
  p.hiddenAt=undefined;p.round=priorRound;p.server={replies:[{generation_id:"reply:push"}]};
  p.markTransport("poll");await until(()=>!p.round.metricsTask,"poll mode not reported");
  assert.equal(reports.at(-1).values.client_transport,0);
  p.markTransport("ws");await until(()=>!p.round.metricsTask,"mixed mode not reported");
  assert.equal(reports.at(-1).values.client_transport,2);
  const restored=browser("",{storage:b.storage}).page;restored.reference=p.reference;restored.initTransport();
  assert.equal(restored.wsDisconnects,1);assert.equal(restored.wsReconnects,1);

  p.server.failures=[{service:"stt",code:"balance",generation_id:"reply:push"},{service:"summary",code:"timeout",generation_id:"reply:old"}];
  assert.equal(p.serviceFailure("stt"),"语音识别服务余额不足，请联系管理员充值");
  assert.match(p.serviceFailure("summary"),/前情摘要服务暂时没准备好/);
  for(const [code,word] of [["auth","鉴权失败"],["timeout","响应超时"],["unavailable","网络不稳"],["other","这次没接上"],["config","配置不可用"]]) {
    p.server.failures=[{service:"chat",code,generation_id:"reply:push"}];
    assert.ok(p.serviceFailure("chat").includes(word));
    if(["timeout","unavailable","other"].includes(code)) {
      assert(!p.serviceFailure("chat").includes("管理员"));assert(p.serviceFailure("chat").includes("继续通话"));
    } else assert(p.serviceFailure("chat").includes("管理员"));
  }
  p.server.failures=[{service:"stt",code:"<img src=private>",generation_id:"reply:push"}];assert.equal(p.serviceFailure("stt"),"");

}

async function pushedFirstSentenceFacets() {
  const b=browser(""),p=b.page,sockets=[],downloads=[],metrics=[];
  p.reference="c".repeat(64);p.e.location.origin="https://calls.example.test";p.started=true;
  p.round={id:"inline",uploadAt:10,transcriptAt:20};
  const rows=[{generation_id:"reply:inline",seq:0,state:"complete"},
    ...[1,2].map(seq=>({generation_id:"reply:inline",seq,state:"succeeded",revision:0,playback_state:"unknown"}))];
  const full={audio:[{utterance_id:"inline",state:"succeeded"}],replies:[{utterance_id:"inline",generation_id:"reply:inline",state:"running"}],turns:[],segments:rows};
  p.server={...full,segments:[]};
  p.api=async(route,body)=>{
    if(route==="segment-audio"){downloads.push(body.seq);return new Blob(["synthetic next"],{type:"audio/mpeg"});}
    if(route==="metrics"){metrics.push(body);return {recorded:true};}
    if(route==="segment-playback")return {...body,recorded:true};
    throw new Error("unexpected inline route "+route);
  };
  let hold=false,release;
  p.refresh=async()=>{if(hold)await new Promise(r=>release=r);return p.server;};
  p.e.WebSocket=class {constructor(){sockets.push(this);}close(){this.onclose?.();}};
  p.ensurePush();sockets[0].onmessage({data:JSON.stringify({state:"sync"})});
  await until(()=>p.pushReady,"inline socket not synchronized");
  hold=true;
  const raw=Buffer.concat([Buffer.from("fffb10c4","hex"),Buffer.alloc(100)]);
  const packet={state:"changed",closed:false,segments:rows,first_audio:{generation_id:"reply:inline",seq:1,mime:"audio/mpeg",byte_count:raw.length,data:raw.toString("base64")}};
  sockets[0].onmessage({data:JSON.stringify(packet)});
  await until(()=>b.utterances.length===1,"inline player waited for full-state HTTP");
  assert.ok(release,"state refresh should remain pending while inline audio plays");
  assert.ok(!downloads.includes(1),"inline first sentence must not fetch HTTP audio");
  assert.ok(downloads.includes(2),"later sentence must still prefetch");
  assert.equal(p.round.firstAudioPath,"push");
  await until(()=>!p.round.metricsTask,"inline path metric missing");
  assert.equal(metrics.at(-1).values.client_first_audio_push,1);
  sockets[0].onmessage({data:JSON.stringify(packet)});
  assert.equal(b.utterances.length,1,"duplicate inline packet replayed audio");
  b.utterances[0].onplaying();await p.stopSpeaking(false);
  sockets[0].onmessage({data:JSON.stringify(packet)});
  assert.equal(b.utterances.length,1,"stopped generation replayed");
  hold=false;release();await until(()=>!p.pushTask,"inline resync not released");
  // Wrong generation, oversized payload and hangup cannot create a player.
  p.round={id:"new"};p.server=full;
  sockets[0].onmessage({data:JSON.stringify(packet)});
  p.round={id:"inline"};p.server={...full,segments:[]};
  p.acceptPushedAudio({...packet,first_audio:{...packet.first_audio,byte_count:49153}});
  assert.equal(b.utterances.length,1);
  p.ending=true;p.acceptPushedAudio(packet);assert.equal(b.utterances.length,1);
  p.stopPush();

  // Inline audio can win an already pending HTTP download; a late HTTP
  // failure must not tear down the player that is already using pushed bytes.
  const c=browser(""),q=c.page;
  q.reference="d".repeat(64);q.started=true;q.round={id:"inline"};q.server=full;
  let rejectDownload;
  q.api=async(route,body)=>{
    if(route==="segment-audio") {
      if(body.seq===1)return new Promise((resolve,reject)=>rejectDownload=reject);
      return new Blob(["next"],{type:"audio/mpeg"});
    }
    return {...body,recorded:true};
  };
  await q.reconcileRound();await until(()=>!!rejectDownload,"HTTP race missing");
  q.acceptPushedAudio(packet);await until(()=>c.utterances.length===1,"push did not replace pending HTTP wait");
  rejectDownload(new Error("synthetic late download error"));await new Promise(r=>setTimeout(r,1));
  assert.equal(q.speech.terminal,false);assert.equal(c.utterances[0].paused,false);
  assert.equal(q.round.firstAudioPath,"push");
  await q.stopSpeaking(false);
  // Exercise the real upload-ACK handoff, including the download timing.
  const x=browser(""),v=x.page;v.reference="e".repeat(64);v.started=true;v.server=full;
  v.clip={id:"inline",mime:"audio/webm;codecs=opus",blob:new Blob(["synthetic"]),metrics:{},endedAt:0};
  v.api=async(route,body)=>{
    if(route==="audio"){v.acceptPushedAudio({...packet,tts_pcm_selection:true});return {audio_received:true,utterance_id:"inline"};}
    if(route==="state")return full;
    if(route==="segment-audio"){assert.notEqual(body.seq,1,"cached ACK payload downloaded again");return new Blob(["next"],{type:"audio/mpeg"});}
    return {...body,recorded:true};
  };
  await v.sendClip();await until(()=>x.utterances.length===1,"early inline not handed off on upload ACK");
  assert.equal(v.round.firstAudioPath,"push");
  assert.equal(v.round.downloadAt-v.round.readyAt,0,"early inline download timing missing");
  assert.equal(v.clip,null);await v.stopSpeaking(false);
}

async function stoppedSegmentReceiptFacets() {
  const b=browser(fixture.tokens.H),p=b.page;
  await p.start();assert.equal(b.recorders.length,1,p.message+" "+p.notice+" "+p.apiDiagnostic);b.recorders[0].data("segmented-stop");await p.submitSegment();
  await until(async()=>{await p.refresh();return p.server.segments?.some(r=>r.seq===3 && r.state==="running") && b.utterances.length===1;},"three sentence queue not ready");
  b.utterances[0].onplaying();b.utterances[0].onended();
  await until(()=>b.utterances.length===2,"second player not prepared");
  await settleSpeech(b);
  const gid=p.speech.id,lateEnd=b.utterances[1].onended;
  await p.stopSpeaking(false);lateEnd();await settleSpeech(b);
  let state=await p.api("state");
  assert.deepEqual(state.segments.filter(r=>r.seq>0).map(r=>[r.seq,r.playback_state,r.revision]),[[1,"ended",2],[2,"stopped",1],[3,"stopped",0]]);
  assert.equal(state.turns.find(t=>t.material_id===gid).playback_state,"stopped");
  // A terminal server receipt wins over a late higher-revision observation.
  // Before this fix this successful /segment-playback ACK generated the local
  // playback/validate/invalid-response HTTP=409 diagnostic.
  const ack=await p.api("segment-playback",{generation_id:gid,seq:2,revision:2,state:"ended"});
  assert.equal(ack.state,"stopped");assert.equal(ack.revision,1);assert.equal(ack.superseded,true);
  p.observations.push({generation_id:gid,seq:2,revision:2,state:"ended"});await p.flushObservations();
  p.observations.push({generation_id:gid,seq:3,revision:1,state:"started"});await p.flushObservations();
  assert.equal(p.observations.length,0);assert.doesNotMatch(p.apiDiagnostic,/invalid-response/);
  await globalThis.requestASGI({control:"release_segments"});
  // Also stop while sentence 1 is actually playing, with 2 and 3 queued.
  await p.start();b.recorders.at(-1).data("first-sentence-stop");await p.submitSegment();
  await until(async()=>{await p.refresh();return p.speech?.seq===1 && !!p.speech.media;},"first sentence missing");
  p.speech.media.onplaying();await p.stopSpeaking(false);await settleSpeech(b);
  state=await p.api("state");
  const latest=state.replies.at(-1).generation_id;
  assert.equal(state.turns.find(t=>t.material_id===latest).playback_state,"stopped");
  assert.ok(state.segments.filter(r=>r.generation_id===latest && r.seq>1).every(r=>r.playback_state==="stopped"));
  await p.hangup();
  await until(async()=>{await p.refresh();return p.server.complete;},"stopped stream review missing");
  assert.equal(b.utterances.length,3);assert.doesNotMatch(p.apiDiagnostic,/invalid-response/);
}

async function streamedSentenceFacets() {
  const b=browser(""), p=b.page;
  const downloads=[];
  p.reference="a".repeat(64); p.started=true;
  p.round={id:"stream",segmentSeq:0};
  const rows=[{generation_id:"reply:stream",seq:0,state:"complete"},
    ...[1,2].map(seq=>({generation_id:"reply:stream",seq,state:"succeeded",revision:0,playback_state:"unknown"}))];
  p.server={audio:[{utterance_id:"stream",state:"succeeded"}],replies:[{utterance_id:"stream",generation_id:"reply:stream",state:"running"}],turns:[],segments:rows};
  p.api=async(route,body)=>{
    if(route==="segment-audio") {downloads.push(body.seq);return new Blob(["synthetic"],{type:body.seq===1?"audio/mpeg":"audio/wav"});}
    if(route==="segment-playback") return {...body,recorded:true};
    throw new Error("unexpected stream fixture route "+route);
  };
  await p.reconcileRound();
  await until(()=>b.utterances.length===1,"first streamed sentence not played");
  await until(()=>downloads.includes(2),"next sentence not prefetched while first plays");
  b.utterances[0].onplaying(); b.utterances[0].onended();
  await until(()=>b.utterances.length===2,"second sentence did not follow");
  assert.deepEqual(downloads,[1,2]);
  assert.equal(p.round.firstAudioPath,"download");
  const late=b.utterances[1].onended;
  await p.stopSpeaking(false); late();
  assert.equal(p.speech,null); assert.equal(p.round,null); assert.equal(b.utterances.length,2);
  assert.equal(b.utterances[1].paused,true);
  // A download that resolves after stop cannot create an obsolete player.
  p.round={id:"late"}; p.server.audio=[{utterance_id:"late",state:"succeeded"}];
  p.server.replies=[{utterance_id:"late",generation_id:"reply:late",state:"running"}];
  p.server.segments=rows.map(r=>({...r,generation_id:"reply:late"}));
  let release; p.api=async(route,body)=>route==="segment-audio" ? new Promise(r=>release=()=>r(new Blob(["late"],{type:"audio/wav"}))) : {...body,recorded:true};
  await p.reconcileRound(); await until(()=>!!release,"held download missing");
  await p.stopSpeaking(false); release(); await new Promise(r=>setTimeout(r,1));
  assert.equal(b.utterances.length,2); assert.equal(p.speech,null);
  // PCM failures also reach the real page listen()/recorder path, without clicks.
  for(const kind of ['disconnect','no-end']) {
    const f=browser(''),q=f.page;f.silence();q.started=true;q.reference='a'.repeat(64);q.round={id:kind};
    q.server={audio_available:true,speech_available:true,audio:[{utterance_id:kind,state:'succeeded'}],
      replies:[{utterance_id:kind,generation_id:'reply:'+kind,state:'succeeded'}],turns:[],
      segments:rows.map(r=>({...r,generation_id:'reply:'+kind}))};
    const receipts=[];
    q.api=async(route,body)=>{
      if(route==='segment-audio')return new Blob(['following'],{type:'audio/mpeg'});
      if(route==='segment-playback'){receipts.push({...body});return {...body,recorded:true};}
      if(route==='metrics')return {recorded:true};
      throw Error('unexpected PCM fixture '+route);
    };
    q.pcmContext={state:'running',get currentTime(){return f.clock.time/1000;},destination:{},
      createBuffer(_,size,rate){return {duration:size/rate,getChannelData:()=>new Float32Array(size)};},
      createBufferSource(){return {connect(){},disconnect(){},start(){},stop(){}};}};
    q.e.location.origin='https://calls.example.test';
    q.e.WebSocket=class {constructor(){this.readyState=1;}send(){}close(){this.onclose?.();}};
    q.ensurePush();
    q.acceptPCM({generation_id:'reply:'+kind,seq:1,index:0,rate:24000,format:'s16le',data:btoa('\0'.repeat(1920))});
    const speech=q.speech;f.clock.advance(140);assert(speech.started);
    if(kind==='disconnect')q.socket.close();else f.clock.advance(1520);
    await until(()=>f.utterances.length===1,'PCM failure discarded second sentence');
    assert.equal(speech.pcmFailure,9);assert.equal(speech.round.metrics.client_pcm_failure,9);
    f.utterances[0].onplaying();f.utterances[0].onended();
    await until(()=>!!q.recording,'PCM failure did not return to actual recording');
    assert.deepEqual(receipts.filter(r=>r.seq===1).map(r=>r.state),['started','failed']);
    assert.equal(q.round,null);assert.equal(q.playbackPaused,false);assert.equal(f.utterances.length,1);
    await q.finishSegment('control');q.releaseMic();q.stopPush();
  }
  // An MP3 first-sentence decoder failure follows the same rule as PCM.
  const f=browser(""),q=f.page;f.silence();q.reference="synthetic";q.started=true;q.round={id:"failed-first"};
  q.server={audio_available:true,speech_available:true,audio:[{utterance_id:"failed-first",state:"succeeded"}],
    replies:[{utterance_id:"failed-first",generation_id:"reply:failed-first",state:"succeeded"}],turns:[],
    segments:rows.map(r=>({...r,generation_id:"reply:failed-first"}))};
  const receipts=[];
  q.api=async(route,body)=>{
    if(route==="segment-audio")return new Blob(["synthetic"],{type:"audio/mpeg"});
    if(route==="segment-playback"){receipts.push({...body});return {...body,recorded:true};}
    throw Error("unexpected "+route);
  };
  await q.reconcileRound();await until(()=>f.utterances.length===1,"MP3 first missing");
  f.utterances[0].onplaying();f.utterances[0].error={code:3};f.utterances[0].onerror();
  await until(()=>f.utterances.length===2,"MP3 failure discarded following sentence");
  f.utterances[1].onplaying();f.utterances[1].onended();
  await until(()=>!!q.recording,"MP3 failure did not return to listening");
  assert.deepEqual(receipts.filter(r=>r.seq===1).map(r=>r.state),["started","failed"]);
  assert.equal(q.playbackPaused,false);assert.equal(q.round,null);assert.match(q.lastSpeechFailure,/decode/);
  await q.finishSegment("control");q.releaseMic();
}


async function seriesEndpointFacets() {
  const detector={ready:()=>true,async begin(){return {valid:true};},async finish(){return {valid:true,candidate:true};},close:async()=>{}};
  const b=browser(null,{createVAD:()=>detector}),p=b.page;
  p.started=true;p.reference='synthetic';p.server={audio_available:true,speech_available:true};
  p.config.silenceMs=p.config.vadRedemptionMs=1400;
  b.clock.advance(30000);b.silence();await p.listen();
  const r=p.recording;
  // New recorder, currently silent, but real model voice only 300ms ago.
  p.series={id:'root',count:1,limit:20,queue:[],lastVoiceAt:27000};
  Object.assign(r.vad,{rate:48000,recordStart:1440000,lastCandidate:1425600,lastFrame:1440000,candidate:false});
  let reason;p.submitSegment=async value=>{reason=value;r.lastEnergyVoice=b.clock.time;await p.finishSegment(value);};
  for(let i=0;i<10;i++){r.vad.lastFrame+=4800;b.clock.advance(100);}
  assert.equal(reason,undefined,'carry the model voice time across a quiet handoff');
  r.vad.lastFrame+=4800;b.clock.advance(100);
  await until(()=>p.clip,'boundary quiet interval never ended');assert.equal(reason,'auto_silence');
  assert.equal(p.clip.metrics.client_segment_vad_endpoint,1);assert.equal(p.clip.metrics.client_endpoint_quiet_ms,1400);p.releaseMic();

  // A young detector's first frame cannot prove an old quiet span. Soft energy
  // still advances voice time even if a valid model misses that passage.
  const e=browser(null),q=e.page;e.silence();q.started=true;q.reference='synthetic';q.server={audio_available:true,speech_available:true};e.clock.advance(30000);await q.listen();
  const t=q.recording;q.series={id:'root',count:1,limit:20,queue:[],lastVoiceAt:27000};
  t.vad={valid:true,candidate:false,rate:48000,recordStart:1440000};let ended;
  q.submitSegment=async value=>{ended=value;await q.finishSegment(value);};
  e.level(.0001);e.clock.advance(100);e.level(.004);for(let i=0;i<6;i++){t.vad.lastFrame=1440000+(i+1)*4800;e.clock.advance(100);}
  assert.equal(ended,undefined);assert(t.energyVoiced);assert(e.nodes['voice-level'].value>.1);await q.finishSegment('control');q.releaseMic();
}


async function seamlessSeriesFacets() {
  const b=browser(fixture.tokens.J),p=b.page;
  await p.start();assert.equal(p.seamlessMax,10,[p.message,p.notice,p.apiDiagnostic].join(" | "));
  let release;const original=p.api.bind(p);let first=true;
  p.api=async(route,body,headers,...rest)=>{
    if(route==='audio-series' && first) {first=false;await new Promise(r=>release=r);}
    return original(route,body,headers,...rest);
  };
  const phases=[],gateReports=[];const view=p.view;p.view=value=>{phases.push(value.ui.phase);view(value);};
  const report=p.api.bind(p);p.api=async(route,body,...rest)=>{
    const answer=await report(route,body,...rest);
    if(route==='metrics' && body.generation_id?.startsWith('capture:') && answer.recorded===true)gateReports.push(body);
    return answer;
  };
  b.recorders[0].data('连续第一部分');
  const a=p.submitSegment('auto_limit');
  await until(()=>b.recorders.length===2,'next recorder waited for first upload');
  assert.equal(p.round,null);assert.equal(p.presentation().phase,'listening');
  b.recorders[1].data('连续第二部分');
  const c=p.submitSegment('auto_limit');
  await until(()=>b.recorders.length===3,'third recorder waited for upload');
  assert(!phases.includes('thinking'),'auto_limit flashed thinking');
  const rootClip=p.series.queue[0],secondClip=p.series.queue[1];
  release();await Promise.all([a,c]);
  assert.equal(rootClip.blob,null);assert.equal(secondClip.blob,null);assert.equal(p.series.rootClip.blob,undefined);
  assert(p.series.rootClip.sha);
  let state=await p.api('state');assert.equal(state.audio.length,2);assert.equal(state.replies.length,0);
  b.recorders[2].data('连续第三部分');await p.submitSegment();
  await until(async()=>{await p.refresh();return p.server.replies.length===1 && p.server.replies[0].state==='succeeded';},'series did not produce one final reply');
  state=p.server;assert.equal(state.audio.length,3);
  await until(()=>gateReports.length===2,"intermediate gate reports were not accepted by real ASGI");
  assert(gateReports.every(x=>Number.isSafeInteger(x.values.client_gate_unknown_reason)));
  assert.equal(state.turns.filter(t=>t.role==='user').length,1);
  for(const part of ['第一部分','第二部分','第三部分'])assert(state.turns[0].body.includes(part));
  assert.equal(state.replies[0].utterance_id,state.audio[0].utterance_id);
  await p.hangup();

  // Explicit regression: a normal single segment still uses exactly one final POST.
  const single=browser(fixture.tokens.K,{chunk:true}),q=single.page;await q.start();
  const since=requests.length;single.recorders[0].data('普通一句话');await q.submitSegment();
  const posts=requests.slice(since).filter(r=>r.method==='POST' && /\/(audio|audio-commit|audio-series|audio-series-commit|audio-series-control)$/.test(r.path));
  assert.equal(posts.length,1);assert(posts[0].path.endsWith('/audio-commit') || posts[0].path.endsWith('/audio'));
  await q.hangup();

  const old=browser(fixture.tokens.L),o=old.page;await o.start();old.recorders[0].data('刷新前尚未说完');await o.submitSegment('auto_limit');
  o.recording.recorder.stop();o.releaseMic();
  const fresh=browser(null,{storage:old.storage}),f=fresh.page;await f.start();
  assert.equal(f.series.failed,'reopened');assert.equal(f.recording,null);
  await f.start();assert(f.recording);state=await f.api('state');
  assert.equal(state.series[0].disposition,'abandoned_page_reopened');assert.equal(state.replies.length,0);
  await f.hangup();

  const missing=browser(fixture.tokens.M),m=missing.page;await m.start();
  missing.recorders[0].data('第一段');await m.submitSegment('auto_limit');
  missing.recorders[1].data('缺失的第二段');await m.submitSegment('auto_limit');
  await until(async()=>{await m.refresh();return m.series?.failed==='stt';},'missing segment did not stop capture');
  assert.equal(m.recording,null);assert.equal(m.server.replies.length,0);
  await m.start();assert(m.recording);state=await m.api('state');
  assert.equal(state.series[0].disposition,'abandoned_stt_missing');assert.equal(state.replies.length,0);
  await m.hangup();

  const quiet=browser(fixture.tokens.N),n=quiet.page;await n.start();quiet.recorders[0].data('只有前一段有人声');quiet.silence();await n.submitSegment('auto_limit');
  const silence=await n.finishSegment();silence.vad={valid:true,candidate:false,speechMs:0,speechBeforeControlMs:0};silence.gate={energyMs:0,energyRunMs:0};silence.quality={valid:true,zero:true,frames:2};
  const native=n.api.bind(n);let seals=0;n.api=async(route,...args)=>{if(route==='audio-series-control' && ++seals===1)throw Error('synthetic seal not sent');return native(route,...args);};
  assert.equal(await n.submitSeriesClip(silence),false);assert.equal(n.series.failed,'upload');
  await n.retry();await until(async()=>{await n.refresh();return n.server.replies.length===1;},'quiet seal retry did not finish');
  assert.equal(n.server.audio.length,1);assert.equal(seals,2);await n.hangup();
}

async function failedBargeRecoveryFacets() {
  for(const mode of ["background","ended","lost","lost-cancelled","hangup","hangup-pending","silent","restart","conflict","received","pending"]) {
    const f=browser(""),p=f.page,traffic=[];
    const round={id:"old"},gid="reply:old",media=new f.win.Audio();
    const group={generation_id:gid,seq:0,state:"complete"};
    const sentence={generation_id:gid,seq:1,playback_state:"started"};
    const state={audio:[],replies:[],turns:[],segments:[group,sentence],barge_in:[],complete:false,
      close_requested:false,ended:false,audio_available:true,speech_available:true};
    const response=(value,status=200)=>({status,ok:status===200,json:async()=>structuredClone(value)});
    const mediaSpeech=p.speech={id:gid,seq:1,round,media,playing:true,started:true};
    media.currentTime=.2;media.duration=1;media.paused=false;
    p.started=true;p.reference="b".repeat(64);p.server=state;
    p.config.bargeIn=p.config.captureMode="auto";p.config.requestMs=1000;
    p.audioContext=new f.win.AudioContext();
    p.capture={closed:false,recorder:()=>null,close(){this.closed=true;}};
    p.prepareCapture=async()=>{p.capture=null;}; // Use the existing fake microphone and MediaRecorder.
    const clipText="kept speech-tail🙂";
    let resumeBegin;
    p.e.fetch=async(url,init)=>{
      const route=url.split("/").at(-1);
      if(route==="metrics")return response({recorded:true});
      if(route==="barge-in") {
        const body=JSON.parse(init.body),action=body.action;
        assert.equal(body.generation_id,gid);
        traffic.push(action);
        if(action==="begin") {
          if(traffic.filter(value=>value==="begin").length===1) {
            const row={...body,decision:"pending",closed_reason:null};
            if(["lost","lost-cancelled","silent","received","pending"].includes(mode))state.barge_in.push(row);
            return new Promise((resolve,reject)=>{
              resumeBegin=()=>resolve(response(row));
              init.signal.addEventListener("abort",()=>reject(Error("synthetic lost response")),{once:true});
            });
          }
          if(mode==="conflict") {group.state="stopped";return response({error:"conflict"},409);}
          const row={...body,decision:"pending",closed_reason:null};
          state.barge_in.push(row);return response(row);
        }
        const row=state.barge_in.find(item=>item.utterance_id===body.utterance_id);
        assert(row,"control sent for an absent candidate");
        assert(["timeout","cancel"].includes(action),action);
        row.closed_reason=action;group.state="stopped";
        if(action==="cancel")row.decision="cancelled";
        return response(row);
      }
      traffic.push(route);
      if(route==="state")return response(state);
      if(route==="audio") {
        const id=init.headers["X-Bot-Utterance-ID"];
        assert.equal(id,candidate.id,"retry changed the utterance ID");
        assert.equal(await init.body.text(),clipText,"retry lost or changed captured audio");
        return response({audio_received:true,utterance_id:id});
      }
      if(route==="end") {state.close_requested=true;return response(state);}
      assert.fail("unexpected request "+route);
    };
    const starting=p.beginBarge(mediaSpeech,{onset:0,decided:0,referenceKnown:1});
    const candidate=p.barge;
    f.clock.advance(25);await starting;
    const recording=p.recording;
    assert(recording && resumeBegin,"begin must overlap microphone capture");
    if(mode==="silent")Object.assign(recording.recorder,{samples:0,idleFrames:2,prefixSamples:0});
    else recording.recorder.data("kept speech");
    if(mode==="pending") {
      const sending=p.submitSegment("manual");
      await until(()=>!!p.sendTask,"upload gate did not start");
      assert.deepEqual(traffic,["begin"],"audio overtook pending begin");
      for(const [key,phase] of [["muted","paused"],["suspended","paused"],["ending","ended"],["locked","error"]]) {
        p[key]=true;assert.equal(p.presentation().phase,phase);p[key]=false;
      }
      resumeBegin();assert.equal(await sending,true);
      assert.deepEqual(traffic,["begin","audio","state"]);
      process.stdout.write(JSON.stringify({type:"facet",name:"barge recovery: "+mode})+"\n");
      continue;
    }
    if(mode==="hangup-pending") {
      const closing=p.hangup();
      await until(()=>!p.recording,"hangup did not stop capture while begin was pending");
      assert.deepEqual(traffic,["begin"]);
      f.clock.advance(p.config.requestMs);
      await closing;
      assert.deepEqual(traffic,["begin","state","audio","end","state"]);
      assert.equal(candidate.registerFailed,false);
      process.stdout.write(JSON.stringify({type:"facet",name:"barge recovery: "+mode})+"\n");
      continue;
    }
    f.clock.advance(p.config.requestMs);
    await until(()=>candidate.registerFailed && !p.recording,"timeout did not stop the recorder");
    if(mode==="silent")await until(()=>!p.clip,"silent barge_error clip was retained");
    else assert.equal(p.clip.barge,candidate);
    assert.equal(p.presentation().phase,"error");
    assert.equal(p.presentation().action,"retry");
    assert.equal(f.nodes["main-action"].disabled,false);
    assert.deepEqual(traffic,["begin"]);
    if(mode==="background" || mode==="lost-cancelled") {
      await p.hidden();
      assert(candidate.cancelled);assert.equal(p.barge,null);
      assert.equal(p.clip.barge,candidate);await candidate.control;
      assert.deepEqual(traffic,["begin"],"background sent cancel without confirming begin");
      await p.foreground();
      assert.equal(f.nodes["main-action"].disabled,false);
    }
    if(mode==="ended")sentence.playback_state="ended";
    if(mode==="received") {
      const clip=p.clip;
      clip.postStarted=true;
      state.audio.push({utterance_id:clip.id,sha256:createHash("sha256").update(clipText).digest("hex"),
        byte_count:clip.blob.size,mime:clip.mime,state:"pending"});
    }
    const before=traffic.length;
    const clip=p.clip;
    if(mode==="hangup" || mode==="lost-cancelled")await p.hangup();
    else await p.retry();
    assert.equal(traffic[before],"state","recovery did not start from authoritative state");
    assert.equal(candidate.registerFailed,false);
    await candidate.register;assert.equal(candidate.control,null);
    assert.equal(p.clip,null);
    assert.equal(traffic.filter(route=>route==="audio").length,["silent","received"].includes(mode)?0:1);
    const expected={
      background:[],ended:[],lost:["timeout"],"lost-cancelled":["cancel"],hangup:[],silent:["cancel"],
      restart:["begin","timeout"],conflict:["begin"],received:["timeout"]
    };
    assert.deepEqual(traffic.slice(before).filter(route=>["begin","timeout","cancel"].includes(route)),expected[mode]);
    if(mode==="conflict")assert.deepEqual(traffic.slice(before,before+3),["state","begin","state"]);
    if(["background","ended","hangup","conflict"].includes(mode)) {
      assert.equal(clip.barge,null);assert.equal(p.barge,null);
    }
    if(["lost","restart","received"].includes(mode))assert.equal(clip.barge,candidate);
    if(mode==="hangup" || mode==="lost-cancelled")assert(traffic.indexOf("end")>traffic.indexOf("audio"));
    if(mode==="silent") {
      assert.equal(p.barge,null);
      assert(p.recording,"silent failed candidate blocked listening after recovery");
      await p.finishSegment("fixture-cleanup");p.releaseMic();
    }
    process.stdout.write(JSON.stringify({type:"facet",name:"barge recovery: "+mode})+"\n");
  }
}

async function closePushFacets() {
  for(const local of [true,false]) {
    const f=browser(""),p=f.page;
    const state={audio:[],replies:[],turns:[],complete:false,ended:false,close_requested:false,
      audio_available:true,speech_available:true};
    p.reference="b".repeat(64);p.started=true;p.server=state;
    p.e.location.origin="https://calls.example.test";
    p.e.WebSocket=class {close(){this.onclose?.();}};
    let reads=0,ends=0;
    p.e.fetch=async(url,init)=>{
      const route=url.split("/").at(-1);
      if(route==="end"){ends++;state.close_requested=true;}
      else if(route==="state"){
        assert.equal(init.method,"GET");
        if(++reads>8)throw Error("closed state read loop");
      } else if(route==="playback") {
        return {status:200,ok:true,json:async()=>({...JSON.parse(init.body),recorded:true})};
      } else if(route==="audio") {
        return {status:200,ok:true,json:async()=>({audio_received:true,utterance_id:init.headers["X-Bot-Utterance-ID"]})};
      } else assert.equal(route,"metrics");
      return {status:200,ok:true,json:async()=>structuredClone(state)};
    };
    p.ensurePush();
    const changed=()=>p.socket.onmessage({data:JSON.stringify({state:"changed"})});
    await p.listen();
    const track=p.stream.getTracks()[0],media=new f.win.Audio();
    media.paused=false;
    p.speech={id:"reply:old",revision:0,playing:true,media};
    const heartbeat=p.heartbeatTimer=f.clock.setTimeout(()=>assert.fail("closing heartbeat fired"),1000);
    const epoch=p.epoch;
    if(local) {
      await p.hangup();
      assert.equal(ends,1);assert.equal(reads,1);
    } else {
      state.close_requested=true;state.limits={close_detail:"admin_forced"};
      changed();await p.pushTask;
      assert.equal(ends,0);assert.equal(reads,2,"server closure may read again for its new state owner");
    }
    assert.equal(p.epoch,epoch+1);
    assert(p.ending && track.stopped && media.paused && !p.speech && !p.recording);
    assert.equal(p.heartbeatTimer,null);assert(!f.clock.tasks.has(heartbeat));
    const before=reads;
    changed();await p.pushTask;
    assert.equal(reads,before+1,"one changed push must produce one state read while closing");
    assert.equal(p.epoch,epoch+1);
    assert(p.pushReady);
    p.stopPush();
    process.stdout.write(JSON.stringify({type:"facet",name:"closing push: "+(local?"local":"server")})+"\n");
  }
}

try {
  await playbackDiagnosticFacets();
  await capabilityFacets();
  await rejectedRecordingFacets();
  await pushRecoveryFacets();
  await failedBargeRecoveryFacets();
  await closePushFacets();
  await pushFallbackAndMetricsFacets();
  await pushedFirstSentenceFacets();
  await streamedSentenceFacets();
  await mutePlaybackFacets();
  await normalRoundStopAndFinalClip();
  process.stdout.write(JSON.stringify({type:"scenario",name:"normal_stop_final_clip",passed:true})+"\n");
  await stoppedSegmentReceiptFacets();
  await seriesEndpointFacets();
  await seamlessSeriesFacets();
  await ackIsolationPermissionAndInterruption();
  process.stdout.write(JSON.stringify({type:"scenario",name:"ack_isolation_permission_interruption",passed:true})+"\n");
  process.stdout.write(JSON.stringify({type:"done",passed:2})+"\n");
} catch(error) {
  process.stdout.write(JSON.stringify({type:"failed",message:error.message,stack:error.stack})+"\n");
  process.exitCode=1;
} finally {globalThis.closeIPC();}

async function silentGateFacets() {
  const sample=browser(null).page;
  const clip=(ms,prior=0)=>({metrics:{},vad:{valid:true,speechMs:ms,speechBeforeControlMs:prior},
    gate:{energyMs:0,energyRunMs:0,fallbackVoice:false},quality:{valid:true,frames:2}});
  assert(sample.silentClip(clip(95)));
  assert(!sample.silentClip(clip(96)),"immediate short speech must survive the control tail window");
  const short=clip(56);short.vad.highConfidenceFrames=1;
  assert(!sample.silentClip(short));assert.equal(short.metrics.client_gate_decision,4);
  short.vad.highConfidenceFrames=0;assert(sample.silentClip(short),"a partial high-probability tail cannot qualify");
  const retry=clip(0);retry.postStarted=true;assert(!sample.silentClip(retry));
  const missing=clip(0);delete missing.vad.speechMs;assert(!sample.silentClip(missing));
  const gap=clip(0);gap.vad.coverageGap=true;assert(sample.silentClip(gap));
  assert.equal(gap.metrics.client_gate_unknown_reason,16);
  const aux=clip(0);aux.quality.valid=false;assert(sample.silentClip(aux));
  assert.equal(aux.metrics.client_gate_unknown_reason,32);
  const both=clip(0);both.vad.coverageGap=true;both.quality.valid=false;assert(!sample.silentClip(both));
  assert.equal(both.metrics.client_gate_unknown_reason,48);
  const cold=clip(0);cold.vad=null;assert(sample.silentClip(cold));
  cold.gate.fallbackVoice=true;assert(!sample.silentClip(cold));
  assert.equal(cold.metrics.client_gate_decision,2);
  for(const [mutate,flag] of [
    [c=>{c.postStarted=true;},1],[c=>{c.error=true;},2],[c=>{c.overflow=true;},4],
    [c=>{c.vad.valid=false;},8],[c=>{c.quality.frames=1;},64],
    [c=>{delete c.vad.speechMs;},128],[c=>{delete c.vad.speechBeforeControlMs;},256],
    [c=>{delete c.gate;},512]
  ]) {
    const c=clip(0);mutate(c);sample.silentClip(c);assert.equal(c.metrics.client_gate_unknown_reason,flag);
  }
  // Exercise recorder stop with retained PCM, not just the gate formula:
  // missing online evidence rechecks, settled voice/strong energy does not.
  for(const mode of ["complete-voice","complete-high","aux-voice","complete-silence",
      "valid-gap-noise","valid-gap-speech","counter-gap","aux-gap","frames-gap",
      "strong-gap","weak-gap","online-voice-gap","online-high-gap","online-partial-gap",
      "both-weak-gap","offline-high-gap","online-high-failed-gap","recheck-unavailable","recheck-throw"]) {
    const online={valid:true,speechMs:0,speechBeforeControlMs:0,highConfidenceFrames:0};
    if(["complete-voice","aux-voice","online-voice-gap"].includes(mode))online.speechMs=96;
    if(["online-high-gap","online-high-failed-gap"].includes(mode))online.highConfidenceFrames=1;
    if(mode==="online-partial-gap"){online.peakProbability=.868;online.highConfidenceMs=27;}
    if(mode==="both-weak-gap")online.speechMs=64;
    if(mode==="complete-high")online.highConfidenceFrames=1;
    if(mode.includes("gap") && !["counter-gap","aux-gap","frames-gap"].includes(mode) || mode.startsWith("recheck-"))online.coverageGap=true;
    const offline={valid:true,speechMs:mode==="valid-gap-speech"?96:mode==="both-weak-gap"?64:0,
      speechBeforeControlMs:0,highConfidenceFrames:mode==="offline-high-gap"?1:0};
    let calls=0,pcm;
    const detector={ready:()=>true,prepare:async()=>false,
      begin:async()=>({valid:true,candidate:false,metrics:{gapCount:mode==="counter-gap"?1:0}}),
      finish:async()=>online,close:async()=>{},
      classifyPCM:async(frames,config,budget,coverTail)=>{
        calls++;assert.equal(frames,pcm);assert.equal(budget,2500);assert.equal(coverTail,true);
        if(mode==="recheck-throw")throw Error("synthetic model failure");
        return ["recheck-unavailable","online-high-failed-gap"].includes(mode)?{valid:false}:offline;
      }};
    const b=browser(null,{createVAD:()=>detector}),p=b.page;b.silence();
    p.reference="synthetic";p.started=true;p.server={audio_available:true,speech_available:true};p.api=async()=>({});
    await p.listen();const r=p.recording;
    for(let i=0;i<10;i++)b.clock.advance(100);
    pcm=[new Float32Array(48000).fill(.01)];
    Object.assign(r.recorder,{samples:48000,prefixSamples:0,peak:1,pcm});
    r.recorder.beforeStop=()=>{
      r.quality={valid:!["aux-gap","aux-voice"].includes(mode),frames:mode==="frames-gap"?1:10};
      r.fallbackVoice=!!online.coverageGap;
      r.retentionEnergy=mode==="strong-gap"?[[r.start+100,r.start+400]]:mode==="weak-gap"?[[r.start+100,r.start+399]]:[];
    };
    const c=await p.finishSegment("auto_silence");
    const recheck=!["complete-voice","complete-high","aux-voice","complete-silence","strong-gap"].includes(mode);
    assert.equal(calls,recheck?1:0,"recheck selection "+mode);
    if(recheck)assert.equal(c.metrics.client_gate_recheck,(mode.startsWith("recheck-") || mode==="online-high-failed-gap")?3:2,mode);
    else assert.equal(c.metrics.client_gate_recheck,undefined,mode);
    const keep=["complete-voice","complete-high","aux-voice","strong-gap","valid-gap-speech",
      "online-voice-gap","online-high-gap","online-high-failed-gap","offline-high-gap"].includes(mode)||mode.startsWith("recheck-");
    assert.equal(!p.silentClip(c),keep,"recheck final disposition "+mode);
    assert(pcm[0].every(v=>v===0),"retained PCM not wiped "+mode);
    assert.equal(r.recorder.pcm.length,0);p.releaseMic();
  }
  // The actual offline classifier must confirm a real, complete tail window.
  // A partial padded high frame alone still cannot retain a cold-start clip.
  for(const mode of ["cold","tail-high","tail-negative","overlap","short"]) {
    const pcm=Float32Array.from({length:mode==="short"?400:912},(_,i)=>(i+1)/1000);
    const buffers=[],windows=[];let pass=0;
    class Offline {
      createBuffer(channels,count,rate) {
        assert.equal(count,pcm.length*3);assert.equal(rate,48000);
        const data=new Float32Array(count);buffers.push(data);return {getChannelData:()=>data};
      }
      createBufferSource(){return {connect(){},start(){}};}
      async startRendering(){const data=pcm.slice();buffers.push(data);return {getChannelData:()=>data};}
    }
    const d=new (globalThis.vadModule.LocalVAD)({OfflineAudioContext:Offline,setTimeout,clearTimeout});
    d.cachedModel={reset(){pass++;},async run(frame){
      windows.push({pass,start:Math.round(frame[0]*1000),end:Math.round(frame[511]*1000)});
      if(pass===1)return frame[511]===0?.868:mode==="overlap"?.7:0;
      return mode==="tail-negative"?0:mode==="overlap"?.7:.9;
    }};
    const result=await d.classifyPCM([new Float32Array(pcm.length*3)],{vadPositive:.65},2500,mode!=="cold");
    assert(result?.valid,mode);
    assert.equal(result.highConfidenceFrames,mode==="tail-high"?1:0,mode);
    if(mode==="tail-high")assert.deepEqual(windows.at(-1),{pass:2,start:401,end:912});
    if(mode==="cold" || mode==="short")assert(windows.every(w=>w.pass===1),mode);
    if(mode==="overlap")assert.equal(result.speechMs,57,"overlapping passes must not add up to 96 ms");
    assert(buffers.every(b=>b.every(v=>v===0)),"offline buffers retained PCM "+mode);
    assert.equal(d.offlineTask,null,mode);
  }
  for(const mode of ["control","manual","suspend","hangup","auto_limit","auto_silence","series","series-hangup","short","energy-short","energy-long","click-manual","vad-gap","loading","unavailable","gap","both-gap","loading-short","loading-immediate","tail","error","retry"]) {
    const candidate=mode==="short";
    let b,signal,onVad,closed=0;
    const detector={ready:()=>!mode.startsWith("loading"),prepare:async()=>false,
      async begin(options,event){onVad=event;return signal={valid:true,candidate:false};},
      async finish(s) {
        assert.equal(closed,0,"control released detector before tail settled");
        assert.equal(b.streams[0].getTracks()[0].enabled,false);
        return {valid:mode!=="tail",candidate,speechMs:candidate?192:0,speechBeforeControlMs:candidate?192:0,coverageGap:mode==="vad-gap" || mode==="both-gap"};
      },
      close(){closed++;return Promise.resolve();}
    };
    b=browser(null,{createVAD:mode==="unavailable"?undefined:()=>detector,chunk:true});b.silence();
    const p=b.page,traffic=[];
    p.reference="synthetic";p.started=true;p.server={audio_available:true,speech_available:true};
    p.refresh=async()=>{};
    p.api=async(route,body,headers={})=>{
      traffic.push({route,body});
      if(route==="metrics" || route==="end" || route==="audio-series-control")return {};
      if(route==="state")return {audio:[]};
      assert(["audio","audio-commit"].includes(route),"unexpected gate route "+route);
      return {audio_received:true,utterance_id:headers["X-Bot-Utterance-ID"]};
    };
    await p.listen();const r=p.recording;
    for(let i=0;i<4;i++)b.clock.advance(100);
    if(mode==="short") {signal.candidate=true;b.level(.0005);b.clock.advance(100);b.silence();}
    if(mode==="energy-short" || mode==="click-manual") {b.voice();b.clock.advance(100);b.silence();}
    if(mode==="energy-long") {
      b.level(.0002);for(let i=0;i<4;i++)b.clock.advance(100);
      b.voice();for(let i=0;i<4;i++)b.clock.advance(100);
      b.silence();for(let i=0;i<4;i++)b.clock.advance(100);
    }
    if(mode==="gap" || mode==="both-gap")b.clock.advance(600);
    if(mode==="loading-short" || mode==="loading-immediate") {
      // Very soft short speech, including the final sample before control.
      b.level(.002);b.clock.advance(100);
    }
    if(mode==="error")r.error=true;
    if(mode.startsWith("series"))p.series={id:"root",count:1,limit:10,queue:[],rootClip:{id:"root"},lastVoiceAt:0};
    if(mode==="manual" || mode==="click-manual")await p.submitSegment();
    else if(mode==="suspend")await p.suspend();
    else if(mode==="hangup" || mode==="series-hangup")await p.hangup();
    else if(mode==="auto_limit" || mode==="auto_silence")await p.submitSegment(mode);
    else if(mode==="series")await p.toggleMute();
    else if(mode==="retry") {
      const clip=await p.finishSegment("control");clip.postStarted=true;
      await p.sendClip();
    } else await p.toggleMute();
    const kept=["short","energy-long","both-gap","loading-short","loading-immediate","retry"].includes(mode);
    const uploads=traffic.filter(x=>["audio","audio-commit","audio-series","audio-series-commit"].includes(x.route));
    assert.equal(uploads.length,kept?1:0,"gate disposition "+mode);
    if(mode==="error") {assert.equal(p.clip,r.clip);assert(p.clip.error);}
    else if(kept) {
      assert(p.round,"voice/unknown/retry lost "+mode);
      assert(!traffic.some(x=>x.body?.values?.client_silent_drop));
      assert(Number.isSafeInteger(r.clip.metrics.client_gate_unknown_reason));
      p.server.replies=[{generation_id:"reply:"+p.round.id}];p.sendMetrics(p.round);
      await until(()=>traffic.some(x=>x.body?.generation_id==="reply:"+r.id &&
        x.body?.values?.client_gate_unknown_reason===r.clip.metrics.client_gate_unknown_reason),"submitted gate diagnostic "+mode);
      if(mode==="retry")assert.equal(traffic.filter(x=>x.route==="state").length,1);
      else assert.equal(traffic.filter(x=>x.route==="state").length,0,"ordinary voiced upload added a read");
    } else {
      assert.equal(p.clip,null);assert(r.uploader.done,"silent staging still owns lane");
      const drops=traffic.filter(x=>x.body?.values?.client_silent_drop);
      assert.equal(drops.length,1);assert.equal(drops[0].body.generation_id,"capture:"+r.id);
      assert(Number.isSafeInteger(drops[0].body.values.client_gate_unknown_reason));
      assert.equal(drops[0].body.values.client_silent_reason,["manual","click-manual"].includes(mode)?2:mode==="auto_limit"?3:mode==="auto_silence"?4:1);
      if(mode==="manual" || mode==="click-manual") {
        await until(()=>p.recording && p.recording!==r,"manual silent capture did not resume");
        assert.match(p.presentation().hint,/没有听到声音/);
        assert.match(b.nodes["call-state"].textContent,/没有听到声音，请再说一次/);
        for(let i=0;i<29;i++)b.clock.advance(100);
        assert.match(b.nodes["call-state"].textContent,/没有听到声音/);
        if(mode==="manual") {
          p.recording.vad.positiveMs=192;onVad("candidate");
        } else b.clock.advance(100);
        assert(!b.nodes["call-state"].textContent.includes("没有听到声音"));
      }
      if(mode==="series") {
        assert.deepEqual(traffic.filter(x=>x.route==="audio-series-control").map(x=>x.body.action),["seal"]);
        assert.equal(p.series.count,1,"silent tail consumed a series index");
      }
      if(mode==="series-hangup") {
        assert.equal(p.series.count,1);
        assert.equal(traffic.filter(x=>x.route==="end").length,1);
      }
    }
    if(p.recording)await p.finishSegment("control");
    p.releaseMic();
  }
}

async function capabilityFacets() {
  const {CapabilityProbe}=globalThis.pageModule;
  for(const mode of ['missing','throw','timeout','encode-error','encode-timeout','success']) {
    const clock=new Clock(),owned={closed:0,terminated:0};
    const w={setTimeout:clock.setTimeout.bind(clock),clearTimeout:clock.clearTimeout.bind(clock),performance:{now:()=>clock.time},isSecureContext:true,
      navigator:{userAgent:'Android Chrome/113.0.0.0 QQ/9.0 private-device-detail'},MediaRecorder:{isTypeSupported:()=>true}};
    if(mode!=='missing') {
      w.AudioEncoder=class {
        static isConfigSupported(){if(mode==='throw')throw Error('private');if(mode==='timeout')return new Promise(()=>{});return Promise.resolve({supported:true});}
        constructor(callbacks){this.callbacks=callbacks;}
        configure(){}
        encode(){if(mode==='encode-error')throw Error('private');this.callbacks.output({byteLength:10});}
        flush(){return mode==='encode-timeout'?new Promise(()=>{}):Promise.resolve();}
        close(){owned.closed++;}
      };
      w.AudioData=class {close(){}};
      w.Worker=class {
        constructor(url){this.url=url;if(url.includes('capability-worker'))queueMicrotask(()=>this.onmessage?.({data:31}));}
        postMessage(){
          if(mode==='encode-timeout')return;
          queueMicrotask(()=>this.onmessage?.({data:mode==='encode-error'?{type:'error',code:5,metrics:{client_capture_mux_reason:3}}:{type:'ready',data:new ArrayBuffer(1),metrics:{client_capture_mux_reason:0}}}));
        }
        terminate(){owned.terminated++;}
      };
    }
    w.OfflineAudioContext=class {
      constructor(channels,length,rate){assert.deepEqual([channels,length,rate],[1,48000,48000]);owned.offline=true;}
      async decodeAudioData(){return {sampleRate:48000,length:48000};}
    };
    const probe=new CapabilityProbe(w,()=>{},true);
    probe.decodeContext={decodeAudioData:()=>{throw Error('live decoder must not run');}};probe.run();
    let settled=false;probe.task.then(()=>{settled=true;});
    for(let i=0;i<100&&!settled;i++){await new Promise(r=>setImmediate(r));clock.advance(100);}
    assert(settled,'probe bounded '+mode);
    const v=probe.values;
    assert.equal(v.client_cap_schema,1);assert.equal(v.client_cap_platform,1);assert.equal(v.client_cap_qq,1);
    assert.equal(v.client_cap_encode,mode==='success'?1:mode==='encode-error'?2:mode==='encode-timeout'?3:0);
    assert(!JSON.stringify(v).includes('private'));
    assert(Object.values(v).every(Number.isSafeInteger));
    assert.equal(v.client_cap_chromium,113);
    if(mode==='success')assert(owned.offline,'independent decode context');
    if(mode!=='missing')assert.equal(owned.terminated,mode.startsWith('encode')||mode==='success'?2:1);
    probe.start({state:'running',sampleRate:48000},{node:{},active:{gapCount:1,lostMs:20,metrics:{overflowFrames:2}},captureProbe:{frames:6,first_ms:32,gap_peak_ms:50}});
    clock.advance(2000);assert(probe.done);assert.equal(v.client_cap_frames,6);
    assert.equal(v.client_cap_gaps,1);assert.equal(v.client_cap_rate,48000);
    assert(!('client_cap_device_id' in v));
  }
  // Report only once, even when callbacks, stop and reconnect all request it.
  const b=browser(''),p=b.page,traffic=[];
  p.reference='synthetic';p.api=async(route,body)=>{traffic.push({route,body});return {};};
  assert(!p.capability.running);p.capability.run();await p.capability.task;p.capability.start(null,null);b.clock.advance(2000);
  await until(()=>traffic.some(t=>t.body?.generation_id?.startsWith('page:cap:')),'cap report');
  p.sendCapability();p.capability.finish();p.sendCapability();
  assert.equal(traffic.filter(t=>t.body?.generation_id?.startsWith('page:cap:')).length,1);
  assert.equal(b.streams.length,0);
  const failure=browser(''),f=failure.page;
  f.reference='synthetic';f.e.fetch=async()=>({status:403,ok:false});
  f.capability.run();await f.capability.task;f.capability.finish();
  await new Promise(r=>setImmediate(r));await new Promise(r=>setImmediate(r));
  assert(f.capability.sent);assert.equal(f.locked,false);assert.equal(f.epoch,0);
  assert.equal(f.apiFailure,undefined);
}

async function playbackDiagnosticFacets() {
  const f=browser(null),p=f.page,packets=[];
  const c={state:"running",baseLatency:.01,outputLatency:.02,createBuffer(){},createBufferSource(){},resume:async()=>{}};
  p.audioContext=c;p.playbackContextCreatedAfterInput=1;p.preparePCM();p.api=async(route,body)=>{assert.equal(route,"metrics");packets.push(body);return {recorded:true};};
  f.win.navigator={audioSession:{type:"play-and-record",state:"active"}};p.deviceChanges=2;
  const round={},first={id:"reply:diag",seq:1,round};p.speech=first;
  p.playbackDiagnostics(first,1);f.clock.advance(100);c.outputLatency=.17;f.clock.advance(100);
  c.state="interrupted";c.onstatechange();c.state="running";c.onstatechange();
  first.playbackDiag.finish();
  f.clock.advance(700);const next={id:"reply:diag",seq:2,round,echoPath:"mp3"};p.speech=next;
  p.playbackDiagnostics(next,2);next.playbackDiag.finish();
  assert.deepEqual(packets.map(x=>x.values.client_playback_sentence),[1,1,2,2]);
  assert.equal(packets[0].values.client_playback_context_created_after_input,1);
  assert.equal(packets[0].values.client_playback_output_us,20000);
  assert.equal(packets[0].values.client_playback_session_type,6);
  assert.equal(packets[0].values.client_playback_devicechanges,2);
  assert.equal(packets[1].values.client_playback_statechanges,2);
  assert.equal(packets[1].values.client_playback_interrupts,1);
  assert.equal(packets[1].values.client_playback_latency_jump_ms,150);
  assert.equal(packets[2].values.client_playback_pcm_mp3_ms,900);
  assert.equal(packets[2].values.client_playback_path,2);
  assert(packets.every(x=>Object.values(x.values).every(Number.isSafeInteger)));
  Object.defineProperty(c,"baseLatency",{get(){throw Error("fixture.latency");}});
  const partial={id:"reply:diag",seq:3,round,echoPath:"mp3"};p.speech=partial;
  p.playbackDiagnostics(partial,2);partial.playbackDiag.finish();
  assert(!("client_playback_base_us" in packets.at(-2).values),"unsupported latency must not break playback");
  p.config.captureMode="legacy";p.playbackContextCreatedAfterInput=0;
  const legacy={id:"reply:legacy",seq:1,round:{}};p.speech=legacy;
  p.playbackDiagnostics(legacy,1);legacy.playbackDiag.finish();
  assert.equal(packets.at(-2).values.client_playback_context_created_after_input,0);
  assert.equal(packets.at(-2).values.client_playback_lead_ms,0);
  p.pcmContext=null;p.audioContext=null;p.playbackContextCreatedAfterInput=1;
  const missing={id:"reply:no-context",seq:1,round:{}};p.speech=missing;
  p.playbackDiagnostics(missing,2);missing.playbackDiag.finish();
  assert.equal(packets.at(-2).values.client_playback_context_created_after_input,0);
  assert(!("client_playback_output_us" in packets.at(-2).values));
  p.speech=null;p.pcmContext=null;p.audioContext=null;
}


async function mutePlaybackFacets() {
  for(const mode of ['pcm','mp3','waiting-pcm','inline','whole']) {
    const f=browser(''),p=f.page;f.silence();p.started=true;p.reference='a'.repeat(64);
    p.round={id:mode,metrics:{}};const gid='reply:'+mode,receipts=[],sources=[];
    const rows=[{generation_id:gid,seq:0,state:'complete'},
      ...[1,2,3].map(seq=>({generation_id:gid,seq,state:'succeeded',revision:0,playback_state:'unknown'}))];
    const state=p.server={audio_available:true,speech_available:true,audio:[{utterance_id:mode,state:'succeeded'}],
      replies:[{utterance_id:mode,generation_id:gid,state:'succeeded',bot_sequence:1}],
      turns:[{sequence:1,material_id:gid,role:'assistant',body:'reply',playback_revision:0}],segments:mode==='whole'?[]:rows};
    p.ensurePush=()=>{};p.connect=async()=>{};
    p.api=async(route,body)=>{
      if(route==='state')return structuredClone(state);
      if(route==='segment-audio')return new Blob(['file'],{type:'audio/mpeg'});
      if(route==='speech')return {generation_id:gid,state:'succeeded'};
      if(route==='speech-audio')return new Blob(['file'],{type:'audio/wav'});
      if(['segment-playback','playback'].includes(route)){receipts.push({...body});return {...body,recorded:true};}
      if(route==='metrics')return {recorded:true};
      throw Error('unexpected mute fixture '+route);
    };
    p.pcmContext={state:'running',get currentTime(){return f.clock.time/1000;},destination:{},resume:async()=>{},close:async()=>{},
      createBuffer(_,size,rate){return {duration:size/rate,getChannelData:()=>new Float32Array(size)};},
      createBufferSource(){const src={connect(){},disconnect(){},start(){},stop(){}};sources.push(src);return src;}};
    const pcm=index=>p.acceptPCM({generation_id:gid,seq:1,index,rate:24000,format:'s16le',data:btoa('\0'.repeat(1920))});
    if(mode==='pcm'){pcm(0);f.clock.advance(300);}
    if(mode==='mp3'){await p.reconcileRound();await until(()=>f.utterances.length===1,'initial MP3 missing');f.utterances[0].onplaying();}
    if(['waiting-pcm','inline','whole'].includes(mode))state.audio[0].state='pending';
    if(mode==='mp3')rows.find(r=>r.seq===2).state='running';
    const owned=p.speech,epoch=p.epoch;
    await p.toggleMute();if(p.refreshTask)await p.refreshTask;
    assert(p.muted);assert(p.epoch>epoch);assert.equal(p.recording,null);
    assert.equal(p.bargeEnabled(),false);
    assert.equal(f.nodes.mute.attributes['aria-pressed'],'true');
    if(owned)assert.equal(p.speech,owned,'mute replaced the active player');
    for(let i=0;i<8;i++)p.bargeFrame({valid:512,vad:1,start:i*512,end:(i+1)*512,rate:16000});
    assert(!p.barge,'muted frames triggered a candidate');
    state.audio[0].state='succeeded';
    if(mode!=='mp3')p.server.audio[0].state='succeeded';
    if(mode==='mp3'){
      rows.find(r=>r.seq===2).state='succeeded';await p.toggleMute();if(p.refreshTask)await p.refreshTask;
      assert(!p.muted);assert.equal(p.speech,owned,'unmute replaced current sentence');
    }
    if(mode==='waiting-pcm'){assert(p.canReceivePCM());pcm(0);f.clock.advance(300);}
    if(mode==='pcm'||mode==='waiting-pcm'){
      assert(p.speech?.pcm,'muted PCM was declined');pcm(1);
      p.acceptPCM({generation_id:gid,seq:1,index:2,rate:24000,format:'s16le',end:true});
      for(const source of sources)source.onended?.();
    }
    if(mode==='inline'){
      const raw=Buffer.from('inline audio bytes for fixture');
      p.acceptPushedAudio({segments:rows,first_audio:{generation_id:gid,seq:1,mime:'audio/mpeg',byte_count:raw.length,data:raw.toString('base64')}});
      assert(p.round.pushedFirstBlob,'muted inline payload was rejected');
    }
    if(mode==='whole')await p.reconcileRound();
    let index=0;
    while(p.round){
      await until(()=>f.utterances.length>index||!p.round,'muted next sentence never started');
      if(!p.round)break;
      const media=f.utterances[index++];media.onplaying();media.onended();
      await settleSpeech(f);
      if(mode==='mp3'&&index===1){
        await until(()=>f.utterances.length===2,'unmute lost a queued reply update without WS');
        await p.toggleMute();if(p.refreshTask)await p.refreshTask;
      }
    }
    assert(p.muted);assert.equal(p.recording,null,'muted round restarted capture');
    assert.deepEqual(receipts.filter(r=>r.state==='started').map(r=>r.seq||0),mode==='whole'?[0]:[1,2,3]);
    assert.deepEqual(receipts.filter(r=>r.state==='ended').map(r=>r.seq||0),mode==='whole'?[0]:[1,2,3]);
    const played=f.utterances.length;
    await p.toggleMute();assert(!p.muted);assert(p.recording,'unmute did not resume actual listening');
    assert.equal(f.utterances.length,played,'unmute replayed audio');
    const clip=await p.finishSegment('fixture-cleanup');clip?.uploader?.abandon();p.clip=null;p.releaseMic();p.closePCM();
  }

  // P2 explicit mute cancels a soft pause. A delayed resume and its old timer
  // must never restart that resource, even after the mic is unmuted later.
  const f=browser(''),p=f.page;let revived=false;
  p.started=true;p.reference='a'.repeat(64);p.round={id:'paused'};
  p.server={audio:[],replies:[],turns:[],segments:[]};
  p.api=async(route,body)=>route==='state'?p.server:{...body,recorded:true};
  const media=new f.win.Audio(),round=p.round;
  const speech=p.speech={id:'reply:paused',seq:1,round,revision:1,media,playing:false,softPaused:true};
  const candidate=p.barge={id:'candidate',speech,round,epoch:p.epoch,reference:p.reference,
    pausedAt:0,positionMs:100,started:1,register:Promise.resolve(),pauseReady:Promise.resolve()};
  candidate.timer=f.clock.setTimeout(()=>{revived=true;},6000);
  await p.toggleMute();await candidate.control;if(p.refreshTask)await p.refreshTask;
  assert(candidate.cancelled&&speech.terminal);assert(!p.barge);assert.equal(candidate.timer,null);
  await p.settleBarge({utterance_id:candidate.id,decision:'resume',dispatch:'started'});
  f.clock.advance(6001);
  assert(!revived);assert(media.paused);assert.equal(media.plays,undefined,'soft pause mute revived old media');
  assert(p.closedBargeGenerations.has(speech.id));assert.equal(speech.media,null);
  p.speech=null;p.round=null;p.e.clearTimeout(p.poll);p.stopHeartbeat();
}

// The exchange errors keep diagnostics fixed and only the busy page offers a connection retry.
{
  const invite = "a".repeat(43), reference = "b".repeat(64);
  const state = {audio:[],replies:[],turns:[],ended:false,close_requested:false,complete:false,audio_available:true,speech_available:true};
  const reply = (body,status=200) => ({status,ok:status<300,json:async()=>body});
  const setup = (status, code) => {
    const b = browser(invite, {noAnalysis:true});
    let attempts = 0;
    b.page.e.fetch = async (url, init) => {
      if (url.endsWith("/exchange")) {
        assert.equal(JSON.parse(init.body).invitation, invite);
        return ++attempts === 1 ? reply({error:code},status) : reply({reference});
      }
      return reply(state);
    };
    return b;
  };

  const busy = setup(409,"busy");
  await busy.page.start();
  assert.match(busy.nodes.status.textContent,/正在占线/);
  assert.match(busy.nodes["call-hint"].textContent,/同一时间仅支持一通通话.*评测与诊断.*强制挂断/);
  assert.equal(busy.page.invitation,invite);
  assert.equal(busy.page.locked,false);
  assert.equal(busy.nodes["main-label"].textContent,"点击重试接通");
  busy.nodes["main-action"].click();
  await until(()=>!busy.page.startTask,"busy retry did not finish");
  assert.equal(busy.page.reference,reference);
  assert.equal(busy.page.invitation,"");
  assert.equal(busy.permissions,1);

  for (const [status,code,expected] of [[409,"ended",/邀请已结束/],[401,"denied",/链接无效或已过期/]]) {
    const b=setup(status,code);
    await b.page.start();
    assert.match(b.nodes.status.textContent,expected);
    assert.match(b.nodes.notice.textContent,/\/botcall/);
    assert.equal(b.nodes["main-action"].disabled,true,"no retry action for "+code);
  }
  const unknown=setup(409,"PRIVATE_UNRECOGNIZED_ERROR");
  await unknown.page.start();
  assert.doesNotMatch(unknown.nodes.notice.textContent+unknown.nodes.status.textContent,/PRIVATE_UNRECOGNIZED_ERROR/);
  assert.match(unknown.nodes.notice.textContent,/exchange\/http\/http，HTTP=409/);

  for (const [detail,reason,expected] of [
    ["idle","duration",/长时间未发言.*自动挂断/],
    ["admin_forced","manual",/管理员已结束本次通话/],
  ]) {
    const b=browser("");
    b.page.reference=reference;b.page.started=true;
    b.page.e.fetch=async()=>reply({...state,ended:true,close_requested:true,close_state:"frozen",
      limits:{close_reason:reason,close_detail:detail}});
    await b.page.refresh();
    assert.match(b.nodes.status.textContent,expected);
    assert.match(b.nodes["call-hint"].textContent,expected);
    assert.doesNotMatch(b.nodes.notice.textContent,/时长上限|连接中断/);
  }
}
