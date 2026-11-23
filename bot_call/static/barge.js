import {EchoProbe} from "./echo.js";
// Semantic classification is server-owned; all audio control is page/epoch owned.
export const BARGE=Object.freeze({probability:.65,frames:5,maxGapMs:50,maxGapTotalMs:200,rampMs:25,tailMs:160,longSpeechMs:1200,maxPauseMs:6000,maxAddedBytes:1048576});
export const BargeMethods={
  async readBargeAudio(response,round,gid) {
    const reader=response.body?.getReader?.();
    if(!reader) {
      // A browser without streamed response bodies cannot bound a new Blob
      // during pause. Never claim lossless buffering in that case.
      if(this.barge?.round===round){this.closeBarge("unavailable");throw Error("barge.download");}
      return response.blob();
    }
    const parts=[];let size=0,copyBudgetOwner=null,copyBudgetBytes=0;
    try {
      while(true) {
        const {done,value}=await reader.read();
        if(done)break;
        if(this.closedBargeGenerations?.has(gid)){this.rejectBargeBlock();throw Error("barge.stale");}
        size+=value.byteLength;
        if(size>8388608)throw Error("barge.download");
        const paused=this.barge?.round===round&&!this.barge.localClosed?this.barge:null;
        if(paused) {
          if(copyBudgetOwner!==paused){copyBudgetOwner=paused;copyBudgetBytes=0;}
          if(!this.reserveBargeCache(value.byteLength*2+256)) {
            this.closeBarge("capacity");throw Error("barge.capacity");
          }
          copyBudgetBytes+=value.byteLength;
        }
        parts.push(value);
      }
      if(this.closedBargeGenerations?.has(gid)){this.rejectBargeBlock();throw Error("barge.stale");}
      // Materializing a Blob during pause also copies chunks received BEFORE
      // pause. Reserve that copy now; the per-chunk budget already covers new ones.
      const paused=this.barge?.round===round&&!this.barge.localClosed?this.barge:null;
      if(paused&&!this.reserveBargeCache(size-(copyBudgetOwner===paused?copyBudgetBytes:0))) {
        this.closeBarge("capacity");throw Error("barge.capacity");
      }
      return new this.e.Blob(parts,{type:response.headers.get("Content-Type")?.split(";")[0]||""});
    } finally {try{await reader.cancel();}catch(_){}reader.releaseLock();}
  },
  reserveBargeCache(bytes,following=false) {
    const b=this.barge;if(!b)return false;
    if((b.addedBytes||0)+bytes>BARGE.maxAddedBytes)return false;
    b.addedBytes=(b.addedBytes||0)+bytes;
    if(following)b.newCacheBytes=(b.newCacheBytes||0)+bytes;
    return true;
  },
  rejectBargeBlock() {
    this.bargeLate=Math.min(65535,(this.bargeLate||0)+1);
    if(!this.bargeLateTimer)this.bargeLateTimer=this.e.setTimeout(()=>{
      this.bargeLateTimer=null;
      if(this.lastBarge)this.bargeMetrics(this.lastBarge,{client_barge_late_blocks:this.bargeLate});
    },500);
  },
  bargeEnabled() {
    return this.config.bargeIn==="auto" && this.config.captureMode==="auto" && !this.captureDisabled &&
      !!this.capture && !this.capture.closed && !this.muted && !this.suspended && !this.ending && !this.locked;
  },
  playbackOutput() {
    if(!this.bargeEnabled())return null;
    const c=this.pcmContext||this.audioContext;
    if(!c?.createGain)return null;
    if(this.playbackGain && this.playbackGain.context!==c){try{this.playbackGain.disconnect();}catch(_){}this.playbackGain=null;}
    if(!this.playbackGain){this.playbackGain=c.createGain();this.playbackGain.connect(c.destination);}
    return this.playbackGain;
  },
  outputGain(value,ms=0) {
    const c=this.pcmContext||this.audioContext,g=this.playbackGain?.gain;
    if(!g||!c)return false;
    try{g.cancelScheduledValues(c.currentTime);g.setValueAtTime(g.value,c.currentTime);g.linearRampToValueAtTime(value,c.currentTime+ms/1000);return true;}catch(_){g.value=value;return true;}
  },
  maintainBargeInput(speech=this.speech) {
    if(this.config.bargeIn!=="auto" || this.config.captureMode!=="auto" || !this.started ||
       !speech?.playing || speech.terminal || this.recording || this.barge || this.captureDisabled ||
       this.muted || this.suspended || this.ending || this.locked || this.e.isHidden?.() ||
       !this.stream || (this.pcmContext||this.audioContext)?.state!=="running")return;
    const epoch=this.epoch;
    if((!this.capture || this.capture.closed) && !this.bargeCaptureRepair && (this.bargeInputRepairs||0)<3) {
      this.bargeInputRepairs=(this.bargeInputRepairs||0)+1;
      for(const track of this.stream.getTracks())if(track.readyState==="live")track.enabled=true;
      this.bargeCaptureRepair=this.prepareCapture(false,epoch).catch(()=>{}).finally(()=>{this.bargeCaptureRepair=null;});
    }
    if((!this.echo || this.echo.closed) && !this.echo?.pending && !this.detector?.active &&
       !this.bargeEchoRepair && (this.bargeEchoRepairs||0)<3) {
      this.bargeEchoRepairs=(this.bargeEchoRepairs||0)+1;
      const echo=this.echo=new EchoProbe(this,true);
      this.bargeEchoRepair=echo.ready.then(()=>{
        if(this.epoch===epoch && this.speech===speech && speech.playing && !this.muted && !this.suspended && !this.ending)
          echo.path(speech,speech.pcm?"pcm":"mp3");
        else echo.stop(true);
      }).catch(()=>echo.stop()).finally(()=>{this.bargeEchoRepair=null;});
    }
  },
  bargeFrame(frame) {
    this.maintainBargeInput();
    // Observe across reply preparation and sentence gaps. Ordinary capture owns
    // its own turn and model; it must never become a second candidate/STT.
    if(!this.bargeEnabled()||this.barge||this.recording||this.clip){this.bargeRun=null;return;}
    const valid=frame.valid===512 && Number.isFinite(frame.vad) && frame.end>frame.start;
    let run=this.bargeRun;
    if(!valid||frame.vad<BARGE.probability){this.bargeRun=null;return;}
    const missing=run?(frame.start-run.end)/frame.rate*1000:Infinity;
    const bridge=run&&run.rate===frame.rate&&frame.gap&&Number.isFinite(frame.gapMs)&&
      frame.gapMs>=0&&frame.gapMs<=BARGE.maxGapMs&&missing>=0&&missing<=frame.gapMs+.001&&
      (run.gapMs||0)+frame.gapMs<=BARGE.maxGapTotalMs;
    if(!run||run.rate!==frame.rate||run.end!==frame.start||frame.gap) {
      if(bridge)run.gapMs=(run.gapMs||0)+frame.gapMs;
      else run={count:0,start:frame.start,rate:frame.rate,gapMs:0};
    }
    // Only valid observed speech increments the count. Bounded clock loss
    // freezes it; negative frames, unknown/large loss and rate changes reset it.
    run.count=Math.min(BARGE.frames,run.count+1);run.end=frame.end;
    run.gapSerial=frame.gapSerial||0;run.referenceKnown=Number(frame.referenceKnown===true);this.bargeRun=run;
    this.bargePlayback();
  },
  bargePlayback() {
    const speech=this.speech,run=this.bargeRun,c=this.pcmContext||this.audioContext;
    if(!this.bargeEnabled()||this.barge||this.recording||this.clip||!speech?.playing||speech.terminal||
       speech.local||!speech.seq||!this.playbackGain||!speech.pcm&&!speech.outputNode||
       !run||run.count<BARGE.frames||(run.gapSerial||0)!==(this.echo?.gapSerial||0)||!c||c.currentTime-run.end/run.rate>.096)return;
    // A gap already received fences older queued inference until a frame
    // after that gap has applied its bounded-continuity decision.
    this.bargeRun=null;
    const decided=this.e.now(),onset=decided-Math.max(0,(c.currentTime-run.start/run.rate)*1000);
    void this.beginBarge(speech,{onset,decided,referenceKnown:run.referenceKnown}).catch(()=>{this.closeBarge("error");});
  },
  bargeSilenceMs(vadValid) {
    return vadValid?this.config.vadRedemptionMs:this.config.silenceMs;
  },
  promoteLongBarge(r) {
    const b=this.barge;
    if(!b||b!==r.barge||b.longConfirmed||b.localClosed||!r.vad?.valid)return;
    if(b.voiceRecordingId!==r.id){b.voiceBefore=b.voiceMs||0;b.voiceRecordingId=r.id;}
    if(r.vad.longBreak!==undefined)b.voiceBefore=0;
    const start=Math.max(r.vad.recordStart,r.vad.longBreak??-Infinity);
    b.voiceMs=b.voiceBefore+(r.vad.highSpans||[]).reduce((ms,[a,z,valid])=>
      ms+(valid===512?Math.max(0,z-Math.max(a,start))/r.vad.rate*1000:0),0);
    if(b.voiceMs<BARGE.longSpeechMs)return;
    b.longConfirmed=true;
    // Release old playback while the same recording continues to its endpoint.
    this.closeBarge("long_speech");
    this.show("插话已确认，正在听取完整表达");
  },
  bargeRequest(b,action) {
    return this.api("barge-in",{utterance_id:b.id,generation_id:b.speech.id,seq:b.speech.seq,
      position_ms:b.positionMs,started:b.started,remaining_ms:b.remainingMs??null,action});
  },
  bargeMetrics(b,extra={}) {
    if(!this.reference||this.locked)return;
    void this.api("metrics",{generation_id:"capture:"+b.id,values:extra}).catch(()=>{});
  },
  async beginBarge(speech,evidence) {
    if(this.barge||this.recording||this.clip||this.speech!==speech||!speech.playing||!this.bargeEnabled())return;
    const command=this.e.now(),epoch=this.epoch;
    const b=this.barge={speech,round:speech.round,epoch,id:this.e.crypto.randomUUID(),pausedAt:command,reference:this.reference,started:Number(!!speech.started)};
    this.lastBarge=b;
    speech.softPaused=true; // onpause must preserve continuous input and the ring.
    this.outputGain(0,BARGE.rampMs);
    b.cursor=speech.pcm?.softPause(BARGE.rampMs);
    if(speech.pcm && b.cursor===null)throw Error("barge.pause");
    if(speech.media)b.mediaTime=speech.media.currentTime;
    b.positionMs=Math.min(3600000,Math.max(0,Math.round(speech.pcm?b.cursor/24:b.mediaTime*1000)));
    // Remaining duration is known only after PCM end, or media metadata.
    const durationMs=speech.pcm?(speech.pcm.ended?speech.pcm.receivedSamples/24:NaN):speech.media?.duration*1000;
    b.remainingMs=Number.isFinite(durationMs)?Math.min(3600000,Math.max(0,Math.round(durationMs-b.positionMs))):null;
    speech.playing=false;this.speech=null;this.round=null;
    this.bargeCandidates=Math.min(4096,Math.max(this.bargeCandidates||0,this.server?.barge_in?.length||0)+1);
    this.bargeFalse=Math.min(4096,Math.max(this.bargeFalse||0,this.server?.barge_in?.filter(r=>r.decision==="resume").length||0));
    const context=this.pcmContext||this.audioContext;
    // Graph endpoint, not acoustic speaker latency. No cross-clock subtraction.
    b.zeroAt=context.currentTime+BARGE.rampMs/1000;
    b.pauseReady=speech.media?new Promise(resolve=>{
      b.pauseTimer=this.e.setTimeout(()=>{
        b.pauseTimer=null;
        if(speech.media&&!speech.terminal){speech.media.pause();b.mediaTime=speech.media.currentTime;b.positionMs=Math.min(3600000,Math.max(0,Math.round(b.mediaTime*1000)));b.remainingMs=Number.isFinite(speech.media.duration)?Math.min(3600000,Math.max(0,Math.round(speech.media.duration*1000-b.positionMs))):null;}
        resolve();
      },BARGE.rampMs);
    }):Promise.resolve();
    b.register=b.pauseReady.then(()=>this.bargeRequest(b,"begin"));
    // Registration fences upload/dispatch, but never delays the local mute command.
    b.register.catch(error=>{
      b.registerFailed=true;
      if(this.barge===b)this.closeBarge("error");
      if(!this.locked)this.report(error);
    });
    this.bargeMetrics(b,{client_barge_stop_ms:Math.min(60000,Math.max(0,Math.round(command-evidence.onset+BARGE.rampMs))),
      client_barge_command_ms:Math.min(60000,Math.max(0,Math.round(command-evidence.decided))),
      client_barge_reference:evidence.referenceKnown,client_barge_tail_ms:BARGE.tailMs,client_barge_candidates:this.bargeCandidates,
      client_barge_input_repairs:this.bargeInputRepairs||0,client_barge_vad_repairs:this.bargeEchoRepairs||0});
    b.timer=this.e.setTimeout(()=>{if(this.barge===b)this.closeBarge("timeout");},BARGE.maxPauseMs);
    this.show("正在听，已暂时暂停朗读");
    await b.pauseReady;
    if(this.barge!==b||b.cancelled||b.registerFailed)return;
    await this.listen();
    if(this.barge===b && !b.registerFailed && !this.recording && !this.clip){this.closeBarge("error");this.show("插话录音未准备好，旧朗读已结束，请点击继续。");}
  },
  closeBarge(reason="cancel") {
    const b=this.barge;if(!b)return;
    this.e.clearTimeout(b.timer);b.timer=null;
    const revoking=!["timeout","capacity","unavailable","error","long_speech"].includes(reason);
    // Explicit controls must revoke dispatch even after a lost close ACK.
    // If begin itself failed, defer cancel until a state read confirms the row.
    if(b.localClosed&&revoking&&!b.cancelled) {
      const previous=b.control;
      b.control=b.register?.then(async()=>{
        await previous?.catch(()=>{});
        return this.bargeRequest(b,"cancel");
      },()=>{});
      b.control?.catch(()=>{});
    }
    if(!b.localClosed) {
      b.localClosed=reason;b.pauseEndedAt??=this.e.now();b.speech.terminal=true;b.round.firstAudioClosed=true;
      this.seen.add(b.speech.id);this.closedBargeGenerations ||= new Set();this.closedBargeGenerations.add(b.speech.id);
      this.releasePlayback(b.speech);b.round.downloads?.clear();b.addedBytes=b.newCacheBytes||0;
      b.control=b.register?.then(()=>this.bargeRequest(b,["timeout","capacity","unavailable","long_speech"].includes(reason)?reason:"cancel"),()=>{});
      b.control?.catch(()=>{});
      this.bargeMetrics(b,{client_barge_pause_ms:Math.min(60000,Math.round((b.pauseEndedAt??this.e.now())-b.pausedAt)),
        client_barge_result:reason==="long_speech"?1:reason==="timeout"?3:["error","capacity","unavailable"].includes(reason)?5:4,client_barge_resumed:0});
    }
    // A timeout frees old audio but continues the same user utterance. Controls
    // revoke this candidate; no timer or delayed resume can restore old sound.
    if(reason==="error" && this.recording)void this.finishSegment("barge_error").then(clip=>{
      if(b.registerFailed && clip && this.clip===clip && !this.series?.count)this.dropSilentClip(clip);
    }).catch(error=>this.report(error));
    if(revoking){
      b.cancelled=true;this.barge=null;
      if(!this.ending){this.epoch++;this.cancelStateRead();}
    }
    this.outputGain(1);
  },
  async settleBarge(row) {
    const b=this.barge;
    if(!b||b.registerFailed||row.utterance_id!==b.id||row.decision==="pending")return;
    if(row.decision==="interrupt") {
      this.e.clearTimeout(b.timer);b.timer=null;
      if(!b.localClosed) {
        b.speech.terminal=true;b.round.firstAudioClosed=true;
        this.seen.add(b.speech.id);this.closedBargeGenerations ||= new Set();this.closedBargeGenerations.add(b.speech.id);
        this.releasePlayback(b.speech);b.round.downloads?.clear();b.addedBytes=b.newCacheBytes||0;b.localClosed="interrupt";b.pauseEndedAt??=this.e.now();
        this.bargeMetrics(b,{client_barge_pause_ms:Math.min(60000,Math.round(b.pauseEndedAt-b.pausedAt)),client_barge_result:1,client_barge_resumed:0});
      }
      if(row.dispatch==="speech_only"){
        if(this.recording||this.clip||this.series?.id===b.id&&this.series.queue.length)return;
        this.resolvedBarges ||= new Set();this.resolvedBarges.add(b.id);
        this.barge=null;this.bargeRun=null;this.round=null;
        if(this.series?.id===b.id)this.series=null;
        this.outputGain(1);
        this.playbackNotice="已停止朗读，有需要再说。";
        this.show("已停止朗读，有需要再说", "已回到普通倾听，说完后会自动发送。");
        if(!this.muted&&!this.suspended&&!this.ending)void this.listen();
        return;
      }
      if(row.dispatch!=="started"){this.show("后台处理中，已停止旧朗读，正在等待工具安全收尾");return;}
    }
    if(this.recording||this.clip)return;
    await b.pauseReady;
    if(this.barge!==b)return;
    this.resolvedBarges ||= new Set();this.resolvedBarges.add(b.id);
    this.e.clearTimeout(b.timer);this.barge=null;
    const group=this.server?.segments?.find(r=>r.generation_id===b.speech.id&&r.seq===0);
    const safe=b.epoch===this.epoch && b.reference===this.reference && !this.ending && !this.locked && !this.suspended && !this.muted && !b.cancelled;
    let resumed=false,error;
    if(row.decision==="resume"&&safe&&!b.localClosed&&!row.closed_reason&&!b.speech.terminal&&["open","complete"].includes(group?.state)) {
      try {
        if(b.speech.pcm)resumed=b.speech.pcm.resume();
        else if(b.speech.media && b.speech.media.src===b.speech.url) {
          b.speech.media.currentTime=b.mediaTime;
          error=Math.abs(b.speech.media.currentTime-b.mediaTime)*1000;
          resumed=true;
        }
        if(resumed) {
          this.round=b.round;this.speech=b.speech;b.speech.softPaused=false;
          this.outputGain(1,BARGE.rampMs);
          if(b.speech.media) {
            const media=b.speech.media;
            resumed=await new Promise(resolve=>{
              let timer;
              const done=value=>{this.e.clearTimeout(timer);media.removeEventListener("playing",ok);media.removeEventListener("error",bad);resolve(value);};
              const ok=()=>done(true),bad=()=>done(false);
              media.addEventListener("playing",ok,{once:true});media.addEventListener("error",bad,{once:true});
              timer=this.e.setTimeout(bad,Math.max(1,Math.min(1000,BARGE.maxPauseMs-(this.e.now()-b.pausedAt))));
              this.playMedia(b.speech);
            });
          } else {b.speech.playing=true;error=Math.abs(b.speech.pcm.positionSamples()-b.cursor)/24;}
          if(b.epoch!==this.epoch||this.speech!==b.speech||b.speech.terminal) {
            this.bargeMetrics(b,{client_barge_result:4,client_barge_resumed:0});return;
          }
          // Recorder stop already installed the normal echo consumer. Reuse
          // that graph, avoiding another input graph swap for every false alarm.
          if(safe && this.stream && this.e.probeWindow?.AudioWorkletNode) {
            const echo=this.echo&&!this.echo.closed?this.echo:(this.echo=new EchoProbe(this,true));
            void echo.ready.then(()=>{
              if(this.speech===b.speech && b.epoch===this.epoch && !this.muted && !this.suspended && !this.ending)
                echo.path(b.speech,b.speech.pcm?"pcm":"mp3");
              else echo.stop(true);
            });
          }
        }
      }catch(_){resumed=false;}
    }
    if(resumed)void this.bargeRequest(b,"resumed").catch(()=>{
      if(this.speech===b.speech){void this.stopSpeaking().catch(()=>{});this.playbackNotice="续播状态未确认，旧朗读已结束。";}
    });
    if(row.decision==="resume")this.bargeFalse=Math.min(4096,(this.bargeFalse||0)+1);
    const values={client_barge_pause_ms:Math.min(60000,Math.round((b.pauseEndedAt??this.e.now())-b.pausedAt)),
      client_barge_result:row.decision==="interrupt"?1:row.decision==="resume"?2:5,
      client_barge_resumed:Number(resumed),client_barge_candidates:this.bargeCandidates||0,
      client_barge_false:this.bargeFalse||0,client_barge_late_blocks:this.bargeLate||0};
    if(resumed)values.client_barge_offset_error_us=Math.min(1000000,Math.round((error||0)*1000));
    this.bargeMetrics(b,values);
    if(row.decision==="interrupt") {
      this.outputGain(1);
      if(b.newSocket===this.socket)for(const packet of b.newFrames||[])this.acceptPCM(packet);
      if(b.newPushed)this.acceptPushedAudio(b.newPushed);
      b.newFrames=[];b.newPushed=null;b.newCacheBytes=b.addedBytes=0;
      this.show("插话已确认，正在回复");return;
    }
    this.round=resumed?b.round:null;this.series=null;
    if(!resumed) {
      if(this.speech===b.speech)this.speech=null;
      b.speech.terminal=true;this.releasePlayback(b.speech);this.outputGain(1);
      this.playbackNotice="旧朗读已结束，无法从原停点恢复；文字仍保留。";
      if(safe)await this.listen();
    }else this.show(row.reason==="backchannel"?"听到了，正在从停点继续朗读":"误触已确认，正在从停点继续朗读");
  },
  async reconcileBarge(state) {
    // Restore per-call totals after refresh from durable candidate decisions.
    if(this.bargeCandidates==null)this.bargeCandidates=Math.min(4096,state.barge_in?.length||0);
    if(this.bargeFalse==null)this.bargeFalse=Math.min(4096,state.barge_in?.filter(r=>r.decision==="resume").length||0);
    const b=this.barge;
    if(b) {
      const row=state.barge_in?.find(r=>r.utterance_id===b.id);
      const group=state.segments?.find(r=>r.generation_id===b.speech.id&&r.seq===0);
      if(!b.localClosed&&group?.state==="stopped"&&row?.decision!=="interrupt")this.closeBarge("unavailable");
      if(row)await this.settleBarge(row);
      return;
    }
    // A refreshed page has no owned audio buffers/cursor. Seal every orphaned
    // candidate once; never claim it as a lossless resume or rerun its STT.
    this.closedReopenedBarges ||= new Set();
    const current=state.barge_in?.find(r=>r.utterance_id===this.round?.id);
    if(current && ["resume","cancelled","error"].includes(current.decision) && current.closed_reason) {
      this.round=null;this.series=null;this.playbackNotice="旧朗读已结束，本段材料保留；请重新说完整句。";
      if(!this.sendTask&&!this.clip&&!this.recording&&!this.muted&&!this.suspended&&!this.ending)void this.listen();
    }
    for(const row of state.barge_in||[]) {
      if(this.lastBarge?.registerFailed && row.utterance_id===this.lastBarge.id)continue;
      if(!["pending","resume"].includes(row.decision)||row.closed_reason==="resumed"||this.resolvedBarges?.has(row.utterance_id)||this.closedReopenedBarges.has(row.utterance_id))continue;
      this.closedReopenedBarges.add(row.utterance_id);
      await this.api("barge-in",{utterance_id:row.utterance_id,generation_id:row.generation_id,seq:row.seq,
        position_ms:row.position_ms,started:row.started,action:"refresh"});
    }
  }
};
