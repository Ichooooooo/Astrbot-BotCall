// Discardable scalar observation; no recorder, permission acquisition or storage.
export const dbCode=r=>Math.round(Math.max(0,Math.min(1200,(20*Math.log10(Math.max(r,1e-6))+120)*10)));
const quantile=(a,p)=>a.slice().sort((x,y)=>x-y)[Math.ceil((a.length-1)*p)];
export class EchoWindow {
  constructor(limit=250){this.limit=limit;this.rows=[];this.rejected=0;this.gaps=0;}
  push({mic,ref,vad,tick}) {
    if(this.rows.length>=this.limit)return false;
    if(!Number.isSafeInteger(tick)||tick<0||!Number.isFinite(mic)||mic<0||mic>1) {this.rejected++;return false;}
    if(this.last!==undefined&&tick!==this.last+1)this.gaps++;
    this.last=tick;
    this.rows.push({mic,ref:Number.isFinite(ref)&&ref>=0&&ref<=1?ref:undefined,
      vad:Number.isFinite(vad)&&vad>=0&&vad<=1?vad:undefined});return true;
  }
  summary() {
    const rows=this.rows,out={frames:rows.length,ms:rows.length*32,rejected:Math.min(65535,this.rejected),gaps:Math.min(65535,this.gaps)};
    if(!rows.length)return out;
    for(const [key,p] of [['mic_p50',.5],['mic_p90',.9]])out[key]=dbCode(quantile(rows.map(r=>r.mic),p));
    if(rows.every(r=>r.ref!==undefined))out.ref_p90=dbCode(quantile(rows.map(r=>r.ref),.9));
    const checked=rows.filter(r=>r.vad!==undefined);
    if(checked.length) {
      out.vad_ms=checked.length*32;out.vad_30_ms=checked.filter(r=>r.vad>=.3).length*32;
      out.vad_65_ms=checked.filter(r=>r.vad>=.65).length*32;
      // Partial VAD coverage cannot establish a longest continuous positive run.
      if(!this.gaps&&!this.rejected&&checked.length===rows.length) {
        let run=0,max=0,count=0;
        for(const r of rows){run=r.vad>=.65?run+32:0;max=Math.max(max,run);if(run===256)count++;}
        out.run_65_ms=max;out.candidates=count;
      }
    }
    if(this.gaps||this.rejected||out.ref_p90===undefined)return out;
    let best=-1,bestLag;
    for(let lag=0;lag<=16;lag++) {
      const pairs=rows.slice(lag).map((r,i)=>[20*Math.log10(Math.max(r.mic,1e-6)),20*Math.log10(Math.max(rows[i].ref,1e-6))]);
      if(pairs.length<64)continue;
      const mx=pairs.reduce((n,p)=>n+p[0],0)/pairs.length,my=pairs.reduce((n,p)=>n+p[1],0)/pairs.length;
      let xx=0,yy=0,xy=0;for(const [x,y] of pairs){xx+=(x-mx)**2;yy+=(y-my)**2;xy+=(x-mx)*(y-my);}
      if(xx/pairs.length<1||yy/pairs.length<1)continue;
      const r=xy/Math.sqrt(xx*yy);if(r>best){best=r;bestLag=lag*32;}
    }
    if(bestLag!==undefined){out.corr_q=Math.round(Math.max(0,Math.min(1,best))*1000);out.lag_ms=bestLag;}
    return out;
  }
}
// One page, baseline <=1984ms, first PCM/MP3 playback <=8s each, wall <=60s.
// Windows are UNLABELLED mixed observations; never infer quiet/double-talk.
export class EchoProbe {
  constructor(page,recurrent=false) {
    this.recurrent=recurrent;this.pageId=page.e.crypto.randomUUID();
    this.page=page;this.w=page.e.probeWindow;this.context=page.audioContext;this.stream=page.stream;
    this.values={client_echo_schema:1,client_echo_flags:3,client_echo_route:0};
    this.windows={};this.base=new EchoWindow(62);this.refs=new Map();this.task=Promise.resolve();this.pending=0;
    this.deadline=page.e.now()+60000;
    if(!recurrent)this.timer=page.e.setTimeout(()=>this.stop(),60000);
    try {
      const keys=['echoCancellation','noiseSuppression','autoGainControl'];
      const supported=page.e.mediaDevices.getSupportedConstraints?.(),settings=this.stream.getAudioTracks()[0]?.getSettings?.();
      let checked=0,bits=0,known=0,enabled=0;
      keys.forEach((k,i)=>{
        if(typeof supported?.[k]==='boolean'){checked|=1<<i;if(supported[k])bits|=1<<i;}
        if(typeof settings?.[k]==='boolean'){known|=1<<i;if(settings[k])enabled|=1<<i;}
      });
      Object.assign(this.values,{client_echo_checked:checked,client_echo_supported:bits,
        client_echo_settings_known:known,client_echo_settings_enabled:enabled,
        client_echo_aec_mode:({false:1,true:2,all:3,'remote-only':4})[settings?.echoCancellation]||0});
    }catch(_){}
    this.listeners=[];
    for(const track of this.stream.getTracks())for(const name of ['mute','ended']){
      const fn=()=>{this.values.client_echo_flags|=256;this.stop(true);};
      track.addEventListener(name,fn);this.listeners.push([track,name,fn]);
    }
    const changed=()=>{if(this.context.state!=='running'){this.values.client_echo_flags|=256;this.stop(true);}};
    this.context.addEventListener('statechange',changed);this.listeners.push([this.context,'statechange',changed]);
    this.ready=this.open().catch(()=>{this.values.client_echo_error=1;this.stop();});
  }
  async open() {
    const c=this.context;
    await c.audioWorklet.addModule('/bot-call/vad-worklet.js');
    if(this.closed)return;
    this.source=c.createMediaStreamSource(this.stream);
    this.node=new this.w.AudioWorkletNode(c,'bot-call-voice-signal',{numberOfInputs:1,numberOfOutputs:1,
      outputChannelCount:[1],channelCount:1,channelCountMode:'explicit',channelInterpretation:'speakers'});
    this.gain=c.createGain();this.gain.gain.value=0;
    this.source.connect(this.node);this.node.connect(this.gain);this.gain.connect(c.destination);
    const model=this.page.detector;
    if(this.recurrent && model?.prepareModel)await model.prepareModel();
    if(this.closed)return;
    // Borrow only the already warm, idle model. No resource download or second session.
    const available=this.recurrent?(model?.cachedModel||model?.model):(model?.ready(c)?model.model:null);
    if(available&&!model.active&&!model.offlineTask&&!available.running){this.model=available;this.model.reset();}
    this.node.port.onmessage=({data:m})=>{try{this.message(m);}catch(_){this.values.client_echo_error=2;this.stop();}};
    this.node.onprocessorerror=()=>{this.values.client_echo_error=2;this.stop();};
    this.node.port.postMessage({type:'begin',id:1,maxFrames:16,settle:true});
  }
  path(speech,path,start=this.context.currentTime) {
    if(this.closed||speech.local||(!this.recurrent&&(this.windows[path]||this.page.e.now()>=this.deadline)))return;
    if(this.current) {
      if(this.current.speech===speech&&this.current.path===path)return;
      this.end(this.current.speech);
    }
    if(this.recurrent) {
      if(this.round!==speech.round){this.round=speech.round;this.turn=this.page.echoTurns=Math.min(4096,(this.page.echoTurns||0)+1);}
      this.values.client_echo_turn=this.turn||1;
      this.values.client_echo_sentence=Math.min(4096,Math.max(0,speech.seq||0));
      this.values.client_echo_window=Math.min(4096,(this.windowIndex||0)+1);this.windowIndex=this.values.client_echo_window;
    }
    this.windows[path]=new EchoWindow();this.current={speech,path,window:this.windows[path],
      start,end:start+8};
    this.current.generation=speech.round?.id;
    this.windowTimer=this.page.e.setTimeout(()=>this.end(speech),8000);
  }
  end(speech) {
    if(this.current?.speech!==speech)return;
    this.page.e.clearTimeout(this.windowTimer);this.generation=this.current.generation;this.current=null;
    if(this.recurrent){void this.reportWindow();return;}
    if(this.windows.pcm&&this.windows.mp3)this.stop();
  }
  async reportWindow() {
    const windows=this.windows;this.windows={};const values={...this.values};
    const generation=/^[a-f0-9-]{36}$/.test(this.generation||"")?this.generation:this.pageId;
    const gid="page:echo:"+this.pageId+":"+generation+":"+values.client_echo_sentence+":"+values.client_echo_window;
    const base=this.base.summary();
    if(base.frames){values.client_echo_base_ms=base.ms;values.client_echo_base_mic_p90=base.mic_p90;}
    const task=this.task;
    let timer;
    await Promise.race([task,new Promise(resolve=>{timer=this.page.e.setTimeout(resolve,100);})]);
    this.page.e.clearTimeout(timer);
    let complete=true;
    for(const [path,window] of Object.entries(windows)) {
      if(this.pending)window.gaps++;
      const summary=window.summary();
      if(summary.gaps||summary.rejected||!summary.frames)complete=false;
      if(summary.ref_p90!==undefined)values.client_echo_flags|=path==='pcm'?8:16;
      if(summary.vad_ms)values.client_echo_flags|=32;
      for(const [key,value] of Object.entries(summary))values['client_echo_'+path+'_'+key]=value;
      window.rows.length=0;
    }
    if(!Object.keys(windows).length)return;
    if(complete)values.client_echo_flags|=4;
    values.client_echo_rate=Math.min(192000,this.context.sampleRate);
    values.client_echo_context=({running:1,suspended:2,closed:3,interrupted:4})[this.context.state]||0;
    this.latest=values;
    if(this.page.reference&&!this.page.locked)
      void this.page.api('metrics',{generation_id:gid,values}).catch(()=>{});
  }
  scheduled(samples,start,rate) {
    if(this.closed||!this.current||this.current.path!=='pcm')return;
    if(this.origin===undefined){this.referenceLost=true;return;}
    // Only scalar energy bins, on the SAME 32ms native input clock. No PCM copies.
    for(let i=0;i<samples.length;i++){
      const t=start+i/rate;
      if(t<this.current.start||t>=this.current.end)continue;
      const tick=Math.floor((t-this.origin/this.rate)/.032);
      if(this.refs.size>=270&&!this.refs.has(tick)){this.values.client_echo_flags|=512;this.referenceLost=true;continue;}
      const bin=this.refs.get(tick)||[0,0];bin[0]+=samples[i]**2;bin[1]++;this.refs.set(tick,bin);
    }
  }
  message(m) {
    if(this.closed)return;
    if(m.type==='begun'){this.origin=m.start;this.rate=m.rate;this.edge=m.start;this.gapCount=0;this.lostMs=0;return;}
    if(m.type==='gap'||m.type==='fault') {
      if(m.type==='gap') {
        this.gapSerial=(this.gapSerial||0)+1;
        const ms=(m.start-this.edge)/this.rate*1000;
        const known=m.reason==='clock'&&Number.isSafeInteger(m.start)&&ms>=0&&
          m.count===this.gapCount+1&&Number.isSafeInteger(m.lostMs)&&m.lostMs>=this.lostMs&&
          m.lostMs-this.lostMs>=ms-1;
        // Deliver gap evidence with its following frame, in inference order.
        // A discarded partial frame is missing time too, never a voice frame.
        this.gapMs=known?(this.gapMs||0)+Math.max(ms,m.lostMs-this.lostMs):Infinity;
        this.gapCount=m.count;this.lostMs=m.lostMs;this.edge=m.start;
      }
      if(this.current)this.current.window.gaps++;
      this.values.client_echo_flags|=256;
      if(m.reason==='overflow')this.values.client_echo_flags|=512;
      if(m.type==='fault'){this.values.client_echo_error=3;this.stop();}return;
    }
    if(m.type!=='frame')return;
    const node=this.node;
    const ack=()=>{try{node?.port.postMessage({type:'ack',id:1,seq:m.seq});}catch(_){}};
    const gapSerial=this.gapSerial||0,gap=this.gapMs!==undefined||m.start!==this.edge;
    const gapMs=m.start===this.edge?this.gapMs:Infinity;this.gapMs=undefined;this.edge=m.end;
    if(this.recurrent && !this.current && this.page.bargeEnabled?.() && this.page.speech?.playing)
      this.path(this.page.speech,this.page.speech.pcm?'pcm':'mp3',this.context.currentTime);
    const window=this.current,base=!window&&this.base.rows.length<62&&!this.windows.pcm&&!this.windows.mp3;
    const monitor=this.recurrent&&this.page.bargeEnabled?.()&&!this.page.recording&&!this.page.barge;
    if(!window&&!base&&!monitor){ack();return;}
    const target=base?this.base:window?.window;
    const samples=new Float32Array(m.samples),start=m.start/this.rate,end=m.end/this.rate;
    const tick=Math.round((m.start-this.origin)/this.rate/.032);
    const lag=Math.max(0,(this.context.currentTime-end)*1000);
    this.values.client_echo_lag_peak_ms=Math.min(1000,Math.max(this.values.client_echo_lag_peak_ms||0,Math.ceil(lag)));
    if(gap&&target)target.gaps++;
    if(m.valid!==512||samples.length!==512||samples.some(v=>!Number.isFinite(v)||Math.abs(v)>1)) {if(target)target.rejected++;ack();return;}
    const inWindow=!window||start>=window.start&&end<=window.end;
    if(!inWindow&&!monitor){ack();return;}
    if(this.pending>=15){if(target)target.rejected++;this.page.bargeRun=null;this.values.client_echo_flags|=512;ack();return;}
    const mic=Math.sqrt(samples.reduce((n,v)=>n+v*v,0)/512),bin=this.refs.get(tick);
    // Empty scheduled bins are digital silence; packet arrival never sets playback time.
    const ref=window?.path==='pcm'&&!this.referenceLost?Math.sqrt((bin?.[0]||0)/(24000*.032)):undefined;
    for(const key of this.refs.keys())if(key<tick-1)this.refs.delete(key);
    this.pending++;
    this.task=this.task.then(async()=>{
      if(this.closed)return;
      let vad;
      try {
        if(this.recurrent&&!this.model&&this.values.client_echo_error!==4) {
          const detector=this.page.detector,model=detector?.cachedModel||detector?.model;
          if(model&&!detector.active&&!detector.offlineTask&&!model.running){this.model=model;model.reset();}
        }
        if(this.model)vad=await this.model.run(samples);
      }catch(_){this.model=null;this.values.client_echo_error=4;}
      if(this.closed)return;
      if(inWindow)target?.push({mic,ref,vad,tick});
      if(monitor && this.page.echo===this)
        this.page.bargeFrame?.({vad,start:m.start,end:m.end,rate:this.rate,valid:m.valid,gap,gapMs,gapSerial,
          referenceKnown:window?.path==='pcm'&&Number.isFinite(ref)&&!this.referenceLost});
      if(vad!==undefined)this.values.client_echo_flags|=32;
    }).catch(()=>{if(target)target.rejected++;this.page.bargeRun=null;}).finally(()=>{samples.fill(0);this.pending--;ack();});
  }
  settle() {
    if(!this.pending)return Promise.resolve();
    let timer;
    return Promise.race([this.task,new Promise(resolve=>{timer=this.page.e.setTimeout(resolve,100);})])
      .finally(()=>this.page.e.clearTimeout(timer));
  }
  stop(control=false) {
    if(this.closed)return this.settle();
    if(control)this.values.client_echo_flags|=2048;
    if(this.recurrent && this.current){this.page.e.clearTimeout(this.windowTimer);this.generation=this.current.generation;this.current=null;void this.reportWindow();}
    if(this.page.echo===this)this.page.bargeRun=null;
    this.closed=true;this.page.e.clearTimeout(this.timer);this.page.e.clearTimeout(this.windowTimer);
    if(control)this.values.client_echo_flags|=2048;
    try{this.node?.port.postMessage({type:'shutdown'});}catch(_){}
    for(const node of [this.source,this.node,this.gain])try{node?.disconnect();}catch(_){}
    try{this.node?.port.close();}catch(_){}
    this.refs.clear();this.current=null;
    for(const [track,name,fn] of this.listeners)track.removeEventListener(name,fn);
    this.listeners=[];
    if(this.pending)for(const window of Object.values(this.windows))window.gaps++;
    // Stop observation track use synchronously; ordinary recording owns its own enable.
    if(!this.page.recording&&!this.page.capture)for(const track of this.stream.getTracks())track.enabled=false;
    this.values.client_echo_context=({running:1,suspended:2,closed:3,interrupted:4})[this.context.state]||0;
    this.values.client_echo_rate=Math.min(192000,this.context.sampleRate);
    const base=this.base.summary();
    if(base.frames){this.values.client_echo_base_ms=base.ms;this.values.client_echo_base_mic_p90=base.mic_p90;}
    let continuous=true;
    for(const [path,window] of Object.entries(this.windows)){
      const values=window.summary();
      if(values.gaps||values.rejected||!values.frames)continuous=false;
      if(values.ref_p90!==undefined)this.values.client_echo_flags|=path==='pcm'?8:16;
      for(const [key,value] of Object.entries(values))this.values['client_echo_'+path+'_'+key]=value;
      window.rows.length=0;
    }
    if(continuous&&Object.keys(this.windows).length)this.values.client_echo_flags|=4;
    this.base.rows.length=0;
    this.page.render();
    if(!this.recurrent&&!this.sent&&this.page.reference&&!this.page.locked){
      this.sent=true;
      void this.page.api('metrics',{generation_id:'page:echo:'+this.page.e.crypto.randomUUID(),values:{...this.values}}).catch(()=>{});
    }
    return this.settle();
  }
  summary() {
    const v=this.latest||this.values;
    const path=p=>{
      const ms=v['client_echo_'+p+'_ms'],vad=v['client_echo_'+p+'_vad_ms'],high=v['client_echo_'+p+'_vad_65_ms'];
      return p.toUpperCase()+': '+(ms===undefined?'未观测':ms+'ms，VAD≥0.65 '+(vad?Math.round(high/vad*100)+'%':'未知')+
        '，能量基线差 '+(v.client_echo_base_mic_p90===undefined||v['client_echo_'+p+'_mic_p90']===undefined?'未知':((v['client_echo_'+p+'_mic_p90']-v.client_echo_base_mic_p90)/10)+'dB')+
        '，最长阳性 '+(v['client_echo_'+p+'_run_65_ms']??'未知')+'ms，相关/滞后 '+
        (v['client_echo_'+p+'_corr_q']??'未知')+'/'+(v['client_echo_'+p+'_lag_ms']??'未知')+
        'ms，缺帧 '+(v['client_echo_'+p+'_gaps']??'未知'));
    };
    return '回声探测 v1（混合观察，路由未知）：'+(this.closed?[path('pcm'),path('mp3')].join('；'):(this.recurrent?'逐句观测中，麦克风保持开启':'限时观测中，麦克风保持开启'))+'。不自动打断；缺覆盖/参考不能判定回声。';
  }
}
