"""M6 local Chrome: real authenticated ASGI WS/receipts, fake upstream, no mic."""

import argparse
import asyncio
from dataclasses import replace
from functools import partial
import json
import os
import signal
from pathlib import Path
import sys
import time
from tempfile import TemporaryDirectory
from unittest.mock import patch

from aiohttp import web
import check_local_vad_browser as smoke
from node_runtime import node_runtime
from tools.dev_paths import BROWSER
from test_backend import NativeCase, WAV, ORIGIN
from test_segments import StreamingProvider
from test_streaming_tts import fake_dashscope
from bot_call.astrbot_tts import convert_segment_audio
from bot_call.streaming_tts import stream_sentence

ROOT = Path(__file__).resolve().parents[1]
JS = r"""
import {CallPage} from '/bot-call/call.js';
const wait=ms=>new Promise(r=>setTimeout(r,ms)),check=(v,m)=>{if(!v)throw Error(m);};
let page,result={ok:false,stage:'prepare',cases:[]};
async function until(fn,ms=9000){const end=performance.now()+ms;while(!fn()&&performance.now()<end)await wait(10);check(fn(),'deadline:'+result.stage);}
const saved=sessionStorage.getItem('m6-reload');
try {
  const fixture=await (await fetch('/fixture')).json();
  const env={storage:{getItem:()=>fixture.reference,setItem(){}},location:{hash:'',pathname:'/bot-call/',origin:location.origin},history,
    now:()=>performance.now(),fetch:fetch.bind(window),Audio,URL,Blob,crypto,AbortController,AudioContext,atob:atob.bind(window),probeWindow:window,
    WebSocket:function(url,protocols){return new WebSocket(url.replace('wss:','ws:'),protocols);},
    setTimeout:setTimeout.bind(window),clearTimeout:clearTimeout.bind(window)};
  let listens=0;const timeline=[],mark=event=>timeline.push({event,ms:Math.round(performance.now())});
  page=new CallPage(env);page.started=true;
  page.e.mediaDevices={getUserMedia:async()=>{mark('next-mic-request');throw Error('synthetic no-mic');}};
  page.listen=async()=>{listens++;if(!page.capability.running)await page.listenOnce();};
  const structure=page.capability.structure.bind(page.capability);
  page.capability.structure=async()=>{mark('probe-start');await structure();mark('probe-end');};
  let capReports=0;const api=page.api.bind(page);
  page.api=(route,body,...rest)=>{if(body?.generation_id?.startsWith('page:cap:'))capReports++;return api(route,body,...rest);};
  const acceptTimeline=page.acceptPCM.bind(page),observeTimeline=page.observe.bind(page);
  page.acceptPCM=packet=>{if(packet.generation_id==='reply:normal'&&packet.index===0)mark('first-pcm');return acceptTimeline(packet);};
  page.observe=(speech,state)=>{if(speech.id==='reply:normal')mark('sentence-'+speech.seq+'-'+state);return observeTimeline(speech,state);};
  page.preparePCM();await until(()=>page.pcmContext?.state==='running');
  await page.connect();await until(()=>page.pushReady);
  async function trigger(id,mode='slow') {
    page.round={id,endedAt:performance.now(),uploadAt:performance.now(),metrics:{client_endpoint_quiet_ms:1400}};
    await fetch('/trigger',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id,mode})});
  }
  if(saved) {
    result=JSON.parse(saved);result.stage='reload';
    page.round ||= {id:'refresh'};await page.refresh();await wait(1000);
    await until(()=>!page.round,13000);
    await page.flushObservations();page.server=await page.api('state');
    const replayed=page.server.segments.find(r=>r.generation_id==='reply:refresh'&&r.seq===1);
    check(replayed.playback_state!=='ended','refresh replayed first sentence');
    const following=page.server.segments.find(r=>r.generation_id==='reply:refresh'&&r.seq===2);
    check(following.playback_state==='ended','refresh discarded following sentence');
    const row=page.server.segments.find(r=>r.generation_id==='reply:refresh'&&r.seq===1);
    check(row.revision>=1 && row.playback_state!=='ended','lost delivery marker');
    result.cases.push({kind:'refresh',replayed:false,playback:row.playback_state,state:row.state});
    sessionStorage.removeItem('m6-reload');result.ok=true;result.stage='completed';
  } else {
    page.capability.start(page.pcmContext,null);page.capability.finish();
    result.stage='normal';await trigger('normal');
    await until(()=>page.speech?.pcm?.playing);
    const speech=page.speech,round=page.round;
    const stats=await (await fetch('/stats')).json();
    const latency=round.playAt-round.pcmFirstAt;
    check(latency>=90 && latency<300,'jitter buffer');
    const before=await page.api('state');
    check(before.segments.find(r=>r.generation_id==='reply:normal'&&r.seq===1).state==='running','waited for full sentence');
    const frameIndex=speech.pcm.index;
    await page.toggleMute();check(page.muted,'mute button');await wait(150);
    check(page.speech?.pcm===speech.pcm && speech.pcm.index>frameIndex,'mute truncated PCM');
    check(page.muted,'must remain muted through the following MP3');
    let secondAt;const observe=page.observe.bind(page);
    page.observe=(s,state)=>{if(s.id==='reply:normal'&&s.seq===2&&state==='started')secondAt=performance.now();return observe(s,state);};
    await until(()=>!page.round,13000);
    check(secondAt && secondAt>=round.playAt+1000,'overlapping sentences');
    const mutedState=await page.api('state');
    check(page.muted&&mutedState.segments.filter(r=>r.generation_id==='reply:normal'&&r.seq>0).every(r=>r.playback_state==='ended'),'mute stopped queued sentences');
    await page.toggleMute();check(!page.muted,'unmute button');
    result.cases.push({kind:'normal',firstToPlayingMs:Math.round(latency),firstSoundMs:Math.round(round.playAt-round.endedAt),secondDelayMs:Math.round(secondAt-round.playAt),archiveBeforeFirst:false,muteDidNotStopPlayback:true});
    await until(()=>timeline.some(t=>t.event==='next-mic-request'));
    const index=event=>timeline.findIndex(t=>t.event===event);
    check(index('probe-start')>index('sentence-2-ended')&&index('sentence-2-ended')>index('first-pcm'),'probe overlapped first reply');
    await until(()=>index('probe-end')>=0&&capReports===1);
    check(index('next-mic-request')>index('probe-start')&&capReports===1,'background probe/report missing');
    page.sendCapability();check(capReports===1,'duplicate capability report');
    result.cases.push({kind:'capability-idle',timeline,reports:capReports});
    // Keep the microphone muted until the whole reply ends, including when
    // its first PCM arrives after mute and when the file player owns sentence 1.
    for(const kind of ['mute-waiting','mute-mp3']) {
      result.stage=kind;const starts=[];
      const prior=page.observe.bind(page),receive=page.canReceivePCM;
      page.observe=(s,v)=>{if(s.id==='reply:'+kind&&v==='started')starts.push(s.seq);return prior(s,v);};
      if(kind==='mute-mp3'){page.canReceivePCM=()=>false;page.syncPCMCapability();}
      if(kind==='mute-waiting'){const pending=trigger(kind);await page.toggleMute();await pending;}
      else await trigger(kind);
      if(kind==='mute-waiting')await until(()=>page.speech?.pcm?.playing);
      else {await until(()=>page.speech?.media&&page.speech.playing,13000);await page.toggleMute();}
      const mutedRound=page.round;
      await until(()=>!page.round,16000);await page.flushObservations();
      const state=await page.api('state');
      check(page.muted&&starts.join(',')==='1,2','muted reply skipped or replayed a sentence');
      check(state.segments.filter(r=>r.generation_id==='reply:'+kind&&r.seq>0).every(r=>r.playback_state==='ended'),'muted reply not fully played');
      if(kind==='mute-waiting')check(mutedRound.pcmFirstAt!==undefined,'muted reply declined PCM');
      page.canReceivePCM=receive;page.syncPCMCapability();await page.toggleMute();
      check(!page.muted,'unmute did not restore input control');await wait(100);
      check(starts.join(',')==='1,2','unmute replayed old audio');
      result.cases.push({kind,starts,allSentencesEnded:true});
    }
    // Reorder two real streamed packets after ownership, including a duplicate.
    result.stage='reorder';
    const acceptOrdered=page.acceptPCM.bind(page);let held=null,reordered=false,starts=0;
    const priorObserveOrder=page.observe.bind(page);
    page.observe=(s,v)=>{if(s.id==='reply:reorder'&&s.seq===1&&v==='started')starts++;return priorObserveOrder(s,v);};
    page.acceptPCM=packet=>{
      if(packet.generation_id==='reply:reorder'&&!reordered) {
        if(packet.index===5){held=packet;return;}
        if(held){acceptOrdered(packet);acceptOrdered(packet);acceptOrdered(held);held=null;reordered=true;return;}
      }
      return acceptOrdered(packet);
    };
    await trigger('reorder');const reorderedRound=page.round;
    await until(()=>!page.round,13000);page.acceptPCM=acceptOrdered;
    await page.flushObservations();
    const reorderedState=await page.api('state');
    check(reordered&&starts===1&&!reorderedRound.metrics.client_pcm_failure,'reorder failed or replayed');
    check(reorderedState.segments.filter(r=>r.generation_id==='reply:reorder'&&r.seq>0).every(r=>r.playback_state==='ended'),'reorder skipped a sentence');
    result.cases.push({kind:'reorder',duplicateIgnored:true,firstSentencePlays:starts,allSentencesEnded:true});
    // A native suspend and a synthetic interrupted state keep ordered PCM,
    // the original sources and one playing receipt. No first-sentence replay.
    for(const state of ['suspended','interrupted']) {
      result.stage=state;await trigger(state);await until(()=>page.speech?.pcm?.playing);
      const speech=page.speech,player=speech.pcm,context=page.pcmContext,nativeResume=context.resume.bind(context);
      const firstIndex=player.index;let paused=true,starts=0;
      const prior=page.observe.bind(page);
      page.observe=(s,v)=>{if(s===speech&&v==='started')starts++;return prior(s,v);};
      if(state==='interrupted')Object.defineProperty(context,'state',{configurable:true,get:()=>paused?'interrupted':'running'});
      context.resume=()=>paused?Promise.resolve():nativeResume();
      if(state==='suspended')await context.suspend();
      player.checkContext();await wait(120);
      check(!speech.terminal&&player.index>firstIndex&&player.pending.length>0,'temporary context lost PCM');
      paused=false;delete context.state;context.resume=nativeResume;await nativeResume();player.checkContext();
      await until(()=>!page.round,13000);
      check(!speech.pcmFailure&&starts===0,'context recovery failed/replayed');
      result.cases.push({kind:state,pauseMs:120,failure:false,replayed:false});
    }
    for(const reason of ['sequence','format','schedule','closed','suspended-timeout','interrupted-timeout','permission']) {
      result.stage=reason;await trigger(reason);await until(()=>page.speech?.pcm?.playing);
      const speech=page.speech,context=page.pcmContext,player=speech.pcm;
      const priorAccept=page.acceptPCM.bind(page),priorResume=context.resume.bind(context),priorCreate=context.createBufferSource.bind(context);
      const listensBefore=listens;let missingAt,missingIndex;
      if(reason==='sequence'||reason==='format') {
        let injected=false;
        page.acceptPCM=packet=>{
          // Keep future blocks within the reorder window to test timeout,
          // rather than the independent immediate window-overflow guard.
          if(reason==='sequence'&&injected&&packet.generation_id===speech.id&&packet.index>missingIndex+3)return;
          if(!injected&&packet.generation_id===speech.id&&!packet.end) {
            injected=true;if(reason==='sequence'){missingAt=performance.now();missingIndex=packet.index;}packet={...packet,...(reason==='sequence'?{index:packet.index+1}:{data:btoa('x')})};
          }
          return priorAccept(packet);
        };
      } else if(reason==='schedule')context.createBufferSource=()=>{throw Error('private fixture');};
      else {
        Object.defineProperty(context,'state',{configurable:true,get:()=>reason==='closed'?'closed':reason.startsWith('interrupted')?'interrupted':'suspended'});
        context.resume=()=>reason==='permission'?Promise.reject({name:'NotAllowedError'}):Promise.resolve();
        player.checkContext();
      }
      await until(()=>speech.terminal,4000);
      if(reason==='sequence')check(performance.now()-missingAt>=250,'missing sequence failed before deadline');
      delete context.state;context.resume=priorResume;context.createBufferSource=priorCreate;page.acceptPCM=priorAccept;
      await until(()=>!page.round,13000);await page.flushObservations();
      const state=await page.api('state'),rows=state.segments.filter(r=>r.generation_id===speech.id);
      check(rows.find(r=>r.seq===0).state!=='stopped','failure stopped whole group');
      check(rows.find(r=>r.seq===1).playback_state==='failed','failure hidden');
      check(rows.find(r=>r.seq===2).playback_state==='ended'&&listens>listensBefore,'following sentence/listening lost');
      const code=({sequence:1,format:2,closed:3,'suspended-timeout':4,'interrupted-timeout':5,schedule:6,permission:8})[reason];
      check(speech.pcmFailure===code&&speech.round.metrics.client_pcm_failure===code,'failure reason missing');
      check(!page.playbackPaused&&!page.speech,'failure stuck');
      result.cases.push({kind:reason,reason:code,followingPlayed:true,automaticListen:true,firstSentenceReplayed:false});
    }
    // Real authenticated socket close, and live socket with no more PCM/end.
    for(const kind of ['ws-close','no-end']) {
      result.stage=kind;await until(()=>page.pushReady&&page.socket.pcmEnabled);
      const before=await (await fetch('/stats')).json(),listensBefore=listens;
      let starts=0;const priorObserve=page.observe.bind(page),priorAccept=page.acceptPCM.bind(page);
      page.observe=(speech,state)=>{if(speech.id==='reply:'+kind&&speech.seq===1&&state==='started')starts++;return priorObserve(speech,state);};
      await trigger(kind);await until(()=>page.speech?.pcm?.playing);
      const speech=page.speech,player=speech.pcm,closedAt=performance.now();
      const drainAt=closedAt+Math.max(0,player.next-page.pcmContext.currentTime)*1000;
      if(kind==='ws-close')page.socket.close();
      else page.acceptPCM=packet=>{if(packet.generation_id!==speech.id)return priorAccept(packet);};
      await until(()=>speech.terminal,2300);
      const failedAt=performance.now();
      check(speech.pcmFailure===9&&speech.round.metrics.client_pcm_failure===9,'stream failure reason missing');
      check(kind!=='no-end'||failedAt>=drainAt+1500-20,'stream failed before audio drain grace');
      check(failedAt<=drainAt+1900,'stream failure not bounded');
      page.acceptPCM=priorAccept;
      await until(()=>!page.round,13000);await page.flushObservations();
      const rows=(await page.api('state')).segments.filter(r=>r.generation_id===speech.id);
      check(rows.find(r=>r.seq===1).playback_state==='failed'&&rows.find(r=>r.seq===2).playback_state==='ended','stream failure lost following sentence');
      await until(()=>page.pushReady);await page.refresh();await wait(100);
      const after=await (await fetch('/stats')).json();
      check(after.stream_calls-before.stream_calls===1&&after.native_calls-before.native_calls===1,'disconnect resynthesized a sentence');
      check(starts===1&&listens>listensBefore&&!page.playbackPaused&&!page.speech,'disconnect replayed/stuck');
      result.cases.push({kind,reason:9,failureMs:Math.round(failedAt-closedAt),afterDrainMs:Math.round(failedAt-drainAt),followingPlayed:true,automaticListen:true,firstSentencePlays:starts,extraSynthesis:0});
    }

    // The loopback server closes the actual socket and refuses reconnects for
    // 2.5s. Only fallback GETs can discover an archived first sentence.
    for(const kind of ['ready-disconnect','stream-disconnect','idle-disconnect']) {
      result.stage=kind;
      if(kind==='ready-disconnect')page.closePCM();
      if(kind==='stream-disconnect')page.preparePCM();
      await until(()=>page.pushReady&&page.socket.pcmEnabled===(kind!=='ready-disconnect')&&!page.pushTask&&!page.refreshTask);
      let closedAt,seenAt,openedAt,starts=0,stateReads=0,longReads=0;
      const socket=page.socket,priorClose=socket.onclose,priorApi=page.api.bind(page),priorObserve=page.observe.bind(page);
      socket.onclose=event=>{closedAt=Date.now();priorClose(event);};
      page.api=(route,body,headers,...rest)=>{
        if(route==='state'){stateReads++;if(headers?.['X-Bot-Call-Wait-Ms'])longReads++;}
        return priorApi(route,body,headers,...rest).then(value=>{
          if(route==='state'&&!seenAt&&value.segments?.some(r=>r.generation_id==='reply:'+kind&&r.seq===1&&r.state==='succeeded'))seenAt=Date.now();
          return value;
        });
      };
      page.observe=(speech,state)=>{if(speech.id==='reply:'+kind&&speech.seq===1&&state==='started')starts++;return priorObserve(speech,state);};
      const priorAccept=page.acceptPCM.bind(page);
      page.acceptPCM=packet=>{
        if(packet.generation_id==='reply:'+kind&&packet.index===0&&!seenAt)seenAt=Date.now();
        return priorAccept(packet);
      };
      const before=await (await fetch('/stats')).json();
      await fetch('/fault',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({kind})});
      let round,speech;
      if(kind!=='idle-disconnect') {
        await trigger(kind);round=page.round;
        if(kind==='stream-disconnect')await until(()=>page.speech?.pcm?.playing);
        speech=page.speech;
      }
      await until(()=>closedAt);
      await wait(50);
      if(kind==='idle-disconnect') {
        const readsAtClose=stateReads;await wait(1300);
        check(stateReads===readsAtClose&&!page.poll,'idle failure started periodic polling');
      } else if(kind==='ready-disconnect') {
        await until(()=>seenAt,1500);
        const stats=await (await fetch('/stats')).json();
        check(seenAt-stats.fault_ready_at<=1500,'ready sentence discovered late');
        check(!page.pushReady,'test did not hold push disconnected');
      }
      await until(()=>page.pushReady,8000);openedAt=Date.now();
      if(kind!=='idle-disconnect') {
        await until(()=>!page.round,13000);await page.flushObservations();
        const rows=(await page.api('state')).segments.filter(r=>r.generation_id==='reply:'+kind);
        check(rows.find(r=>r.seq===1).playback_state===(kind==='stream-disconnect'?'failed':'ended'),'first-sentence receipt changed');
        check(rows.find(r=>r.seq===2).playback_state==='ended'&&starts===1,'recovery replayed/lost next sentence');
      }
      const after=await (await fetch('/stats')).json();
      check(longReads===0,'disconnected reply used long poll');
      check(kind==='idle-disconnect'||after.stream_calls-before.stream_calls===(kind==='stream-disconnect'?1:0),'extra stream synthesis');
      check(kind==='idle-disconnect'||after.native_calls-before.native_calls===(kind==='stream-disconnect'?1:2),'extra native synthesis');
      result.cases.push({kind,closedToSyncedMs:openedAt-closedAt,readyToSeenMs:seenAt?seenAt-after.fault_ready_at:null,
        closedToSeenMs:seenAt?seenAt-closedAt:null,firstSentencePlays:starts,stateReads,longReads,
        firstSentenceReplayed:false,idlePeriodicPolling:false});
      page.api=priorApi;page.observe=priorObserve;page.acceptPCM=priorAccept;
    }
    page.preparePCM();await until(()=>page.pushReady&&page.socket.pcmEnabled);

    // Return to the foreground with an already archived MP3 reply. Keep
    // microphone pause ownership; the actual media player must start itself.
    result.stage='foreground-ready';
    let hidden=true,foregroundPlays=0;
    const originalHidden=page.e.isHidden;
    page.e.isHidden=()=>hidden;await page.hidden();page.stopPush();
    await trigger('foreground-ready','fast');
    await wait(30);
    let archived;
    for(let n=0;n<200;n++) {
      archived=await page.api('state');
      if(archived.segments?.some(r=>r.generation_id==='reply:foreground-ready'&&r.seq===1&&r.state==='succeeded'))break;
      await wait(20);
    }
    check(archived.segments?.some(r=>r.generation_id==='reply:foreground-ready'&&r.seq===1&&r.state==='succeeded'),'background result not ready');
    const priorForegroundObserve=page.observe.bind(page);
    page.observe=(speech,state)=>{if(speech.id==='reply:foreground-ready'&&state==='started')foregroundPlays++;return priorForegroundObserve(speech,state);};
    hidden=false;const visibleAt=performance.now();await page.foreground();
    await until(()=>foregroundPlays>0,1000);
    const foregroundPlayMs=performance.now()-visibleAt;
    check(page.captureResumeRequired,'foreground released microphone pause');
    await until(()=>!page.round,13000);await page.flushObservations();
    result.cases.push({kind:'foreground-ready',visibleToPlayingMs:foregroundPlayMs,automaticPlayback:true,microphonePaused:true});
    page.observe=priorForegroundObserve;page.e.isHidden=originalHidden;page.captureResumeRequired=false;

    // Regressions: muted replies keep playing; genuinely unavailable output
    // remains downloadable without another synthesis.
    for(const gate of ['muted','context','paused','race']) {
      result.stage=gate;
      let starts=0;const prior=page.observe.bind(page);
      page.observe=(s,state)=>{if(s.id==='reply:'+gate&&s.seq===1&&state==='started')starts++;return prior(s,state);};
      if(gate==='muted')await page.toggleMute();
      if(gate==='context')await page.pcmContext.suspend();
      if(gate==='paused'){page.playbackPaused=true;page.render();}
      if(!['muted','race'].includes(gate)) {
        await until(()=>page.pushReady && !page.socket.pcmEnabled);
        let watchers=1;
        for(let i=0;i<100&&watchers;i++){watchers=(await (await fetch('/stats')).json()).pcm_watchers;if(watchers)await wait(10);}
        check(!watchers,'server still advertises PCM');
      }
      const callsBefore=await (await fetch('/stats')).json();
      await trigger(gate);
      if(gate==='race')await page.toggleMute(); // Selected stream, before first chunk.
      if(gate==='muted'||gate==='race') {
        await until(()=>page.speech?.playing);
        const r=page.round;
        check(page.muted,'mute gate unexpectedly opened microphone');
        if(gate==='race')check(r.pcmFirstAt!==undefined,'mute race declined selected PCM');
        await until(()=>!page.round,13000);await page.flushObservations();
        const state=await page.api('state'),sentences=state.segments.filter(s=>s.generation_id==='reply:'+gate&&s.seq>0);
        check(sentences.length===2&&sentences.every(s=>s.playback_state==='ended'),'muted gate lost a sentence');
        check(starts===1,'muted first sentence played more than once');
        const callsAfter=await (await fetch('/stats')).json();
        check(callsAfter.native_calls-callsBefore.native_calls+callsAfter.stream_calls-callsBefore.stream_calls===2,'mute caused extra synthesis');
        result.cases.push({kind:gate,firstSentencePlays:starts,path:r.firstAudioPath,allSentencesEnded:true});
        await page.toggleMute();page.preparePCM();await until(()=>page.pushReady&&page.socket.pcmEnabled);
        continue;
      }
      await until(()=>page.server?.segments?.some(r=>r.generation_id==='reply:'+gate&&r.seq===1&&r.state==='succeeded'));
      const row=page.server.segments.find(r=>r.generation_id==='reply:'+gate&&r.seq===1);
      if(gate!=='context')check(row.revision===0&&!page.speech&&!page.round.terminal,'unavailable sentence lost');
      if(gate==='context')await page.pcmContext.resume();
      if(gate==='paused'){page.playbackPaused=false;page.render();}
      await page.refresh();
      await until(()=>page.speech?.playing);
      check(page.speech.media && page.round.pcmFirstAt===undefined,'did not use complete audio');
      const r=page.round;
      await until(()=>!page.round,13000);
      check(starts===1,'first sentence played more than once');
      check(['download','push'].includes(r.firstAudioPath),'missing archived first sentence');
      const callsAfter=await (await fetch('/stats')).json();
      check(callsAfter.native_calls-callsBefore.native_calls+callsAfter.stream_calls-callsBefore.stream_calls===2,'extra synthesis');
      result.cases.push({kind:gate,firstSentencePlays:starts,path:r.firstAudioPath,revisionBefore:row.revision});
      // This playback-only fixture rejects every microphone request. Explicitly
      // restore its renderer after foreground/mute; Start now waits for input.
      page.preparePCM();
      await until(()=>page.pushReady&&page.socket.pcmEnabled);
    }
    // An in-flight first block may arrive before a control/render callback.
    // Force that ordering while leaving the real authenticated WS intact.
    result.stage='late-decline';
    const accept=page.acceptPCM.bind(page);let declined=false,declinedStarts=0;
    const priorObserve=page.observe.bind(page);
    page.observe=(s,state)=>{if(s.id==='reply:late-decline'&&s.seq===1&&state==='started')declinedStarts++;return priorObserve(s,state);};
    const declinedCallsBefore=await (await fetch('/stats')).json();
    page.acceptPCM=packet=>{
      if(!declined&&packet.index===0){declined=true;page.playbackPaused=true;}
      return accept(packet);
    };
    await trigger('late-decline');
    await until(()=>page.server?.segments?.some(r=>r.generation_id==='reply:late-decline'&&r.seq===1&&r.state==='succeeded'&&r.revision===0));
    check(!page.speech&&!page.round.terminal,'first-block rejection lost');
    page.acceptPCM=accept;page.playbackPaused=false;page.render();await page.refresh();
    await until(()=>page.speech?.playing);check(page.speech.media,'rejected first block not archived');
    await until(()=>!page.round,13000);
    const declinedCallsAfter=await (await fetch('/stats')).json();
    check(declinedStarts===1,'rejected sentence replayed');
    check(declinedCallsAfter.native_calls-declinedCallsBefore.native_calls===1&&declinedCallsAfter.stream_calls-declinedCallsBefore.stream_calls===1,'rejection resynthesized');
    result.cases.push({kind:'late-decline',path:'download',firstSentencePlays:declinedStarts,replayedPCM:false});
    await until(()=>page.pushReady&&page.socket.pcmEnabled);
    // First packet races a transport switch: no negative receipt is needed.
    result.stage='reconnect-first';
    let switched=false,reconnectStarts=0;
    const observeReconnect=page.observe.bind(page);
    page.observe=(s,state)=>{if(s.id==='reply:reconnect-first'&&s.seq===1&&state==='started')reconnectStarts++;return observeReconnect(s,state);};
    page.acceptPCM=packet=>{
      if(!switched&&packet.index===0){switched=true;page.stopPush();page.ensurePush();return;}
      return accept(packet);
    };
    await trigger('reconnect-first');const reconnectRound=page.round;
    await until(()=>!page.round,13000);
    check(switched&&reconnectStarts===1&&reconnectRound.pcmFirstAt===undefined,'connection switch lost/repeated first sentence');
    await page.flushObservations();page.server=await page.api('state');
    check(page.server.segments.find(r=>r.generation_id==='reply:reconnect-first'&&r.seq===2).playback_state==='ended','connection switch lost following sentence');
    page.acceptPCM=accept;
    result.cases.push({kind:'reconnect-first',firstSentencePlays:reconnectStarts,path:reconnectRound.firstAudioPath,followingPlayed:true});
    // Normal recording transitions and multiple rounds keep the same socket.
    const stableSocket=page.socket,stableReconnects=page.wsReconnects;
    for(let i=0;i<3;i++){page.recording={};page.render();page.recording=null;page.render();check(page.socket===stableSocket,'recording reconnected');}
    result.stage='receipt-lost';
    const send=page.socket.send.bind(page.socket);let dropped=0,lostStarts=0;
    page.socket.send=data=>{if(JSON.parse(data).type==='pcm-accepted'){dropped++;return;}return send(data);};
    const observeLost=page.observe.bind(page);
    page.observe=(s,state)=>{if(s.id==='reply:receipt-lost'&&s.seq===1&&state==='started')lostStarts++;return observeLost(s,state);};
    await trigger('receipt-lost');await until(()=>!page.round,13000);
    check(dropped===1&&lostStarts===1,'lost receipt replayed on same page');
    check(page.socket===stableSocket&&page.wsReconnects===stableReconnects,'normal round reconnected');
    page.socket.send=send;
    result.cases.push({kind:'receipt-lost',firstSentencePlays:lostStarts,normalRoundReconnectIncrease:page.wsReconnects-stableReconnects});
    // If EVERY confirmation is lost, discarding page memory permits replay.
    // Keep the full archive; no provider call is repeated.
    result.stage='all-receipts-lost';
    const originalApi=page.api.bind(page),originalSend=page.socket.send.bind(page.socket);
    page.socket.send=data=>{if(JSON.parse(data).type!=='pcm-accepted')return originalSend(data);};
    page.api=(route,body,...rest)=>{
      if(route==='segment-playback'&&body.generation_id==='reply:all-receipts-lost')
        return Promise.resolve({recorded:true,generation_id:body.generation_id,seq:body.seq,revision:body.revision,state:body.state});
      return originalApi(route,body,...rest);
    };
    let allLostStarts=0;const observeAllLost=page.observe.bind(page);
    page.observe=(s,state)=>{if(s.id==='reply:all-receipts-lost'&&s.seq===1&&state==='started')allLostStarts++;return observeAllLost(s,state);};
    await trigger('all-receipts-lost');await until(()=>!page.round,13000);
    const unconfirmed=await originalApi('state');
    check(unconfirmed.segments.find(r=>r.generation_id==='reply:all-receipts-lost'&&r.seq===1).revision===0,'lost confirmations unexpectedly persisted');
    page.api=originalApi;page.socket.send=originalSend;
    const callsBeforeReplay=await (await fetch('/stats')).json();
    // This discards the same per-round memory lost on a real page refresh.
    page.round={id:'all-receipts-lost'};await page.refresh();await until(()=>!page.round,13000);
    const callsAfterReplay=await (await fetch('/stats')).json();
    check(allLostStarts===2,'documented refresh replay boundary changed');
    check(page.socket===stableSocket&&page.wsReconnects===stableReconnects,'multiple normal rounds reconnected');
    check(callsAfterReplay.native_calls===callsBeforeReplay.native_calls&&callsAfterReplay.stream_calls===callsBeforeReplay.stream_calls,'refresh resynthesized');
    result.cases.push({kind:'all-receipts-lost',samePagePlays:1,afterRoundMemoryLossTotalPlays:allLostStarts,extraSynthesis:0});
    result.stage='stop';await trigger('stop');await until(()=>page.speech?.pcm?.playing);
    const player=page.speech.pcm,t0=performance.now(),stopping=page.stopSpeaking(false);
    check(player.closed && player.sources.size===0,'stop not immediate');await stopping;
    const stoppedAt=performance.now();
    page.acceptPCM({generation_id:'reply:stop',seq:1,index:999,rate:24000,format:'s16le',data:btoa('xx'),end:false});
    await wait(200);check(!page.speech && player.sources.size===0,'late audio replayed');
    result.cases.push({kind:'stop',localStopMs:Math.round(stoppedAt-t0),lateReplay:false});
    result.stage='early-inline';
    const earlyId='early-inline',earlyAccept=page.acceptPushedAudio.bind(page),earlyApi=page.api.bind(page);
    let buffered=false,earlyDownloads=0;
    page.playbackPaused=true;page.render();await until(()=>!page.socket.pcmEnabled);await wait(50);
    page.round=null;page.clip={id:earlyId};
    page.api=(route,...rest)=>{if(route==='segment-audio'&&rest[0].generation_id==='reply:'+earlyId&&rest[0].seq===1){earlyDownloads++;return new Promise(()=>{});}return earlyApi(route,...rest);};
    page.acceptPushedAudio=value=>{earlyAccept(value);if(page.clip?.pendingFirstAudio)buffered=true;};
    await fetch('/trigger',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id:earlyId,mode:'slow'})});
    await until(()=>buffered);
    const early=page.clip.pendingFirstAudio;
    page.round={id:earlyId,endedAt:performance.now(),readyAt:performance.now()};page.clip=null;
    page.playbackPaused=false;const ready=performance.now();earlyAccept(early);
    await until(()=>page.speech?.playing);
    const downloadMs=Math.round(page.round.downloadAt-page.round.readyAt);
    check(earlyDownloads===0&&downloadMs<100,'early inline added HTTP wait');
    result.cases.push({kind:'early-inline',downloadMs,readyToPlayingMs:Math.round(performance.now()-ready),httpDownloads:earlyDownloads});
    await until(()=>!page.round,13000);page.api=earlyApi;page.acceptPushedAudio=earlyAccept;
    await until(()=>page.socket.pcmEnabled);
    result.stage='fallback';await trigger('fallback','preclose');await until(()=>page.speech?.playing);
    check(page.round.pcmFirstAt===undefined && page.speech.media,'fallback failed');
    result.cases.push({kind:'fallback',path:page.round.firstAudioPath,downloadMs:Math.round(page.round.downloadAt-page.round.readyAt)});await page.stopSpeaking(false);
    result.stage='unsupported';page.closePCM();page.stopPush();await page.connect();await until(()=>page.pushReady);
    await trigger('unsupported');await until(()=>page.speech?.playing);
    check(page.speech.media && page.round.pcmFirstAt===undefined,'WebAudio fallback');
    result.cases.push({kind:'unsupported',path:page.round.firstAudioPath});await page.stopSpeaking(false);
    result.stage='refresh';page.preparePCM();await until(()=>page.pcmContext?.state==='running'&&page.pushReady);
    await trigger('refresh');await until(()=>page.speech?.pcm?.playing);
    sessionStorage.setItem('m6-reload',JSON.stringify(result));location.reload();
    await new Promise(()=>{});
  }
} catch(e){result.failure=e.message;result.diagnostic=page?.speechDiagnostic;result.round=page?.round?{id:page.round.id,segmentSeq:page.round.segmentSeq,terminal:page.round.terminal,pcmFirstAt:page.round.pcmFirstAt}:null;result.ui=page?.message;}
finally {
  page?.stopPush();if(page?.speech)page.releasePlayback(page.speech);
  const context=page?.pcmContext;page?.closePCM();await wait(100);
  result.contextClosed=!context || context.state==='closed';window.smokeResult=result;
}
"""

DRIVER = (
    smoke.DRIVER.replace("90000*repeats", "240000*repeats")
    .replace("i<600", "i<2300")
    .replace(
        "const external=urls.filter(u=>!u.startsWith(origin+'/'));",
        "const external=urls.filter(u=>!u.startsWith(origin+'/')&&!u.startsWith(origin.replace('http:','ws:')+'/')&&!u.startsWith('blob:'+origin+'/'));",
    )
    .replace("result.peakRuns!==1||", "")
    .replace("wasmRequests!==1||modelRequests!==1", "wasmRequests!==0||modelRequests!==0")
)


async def main(browser):
    case = NativeCase()
    await case.asyncSetUp()
    backend = None
    runner = None
    try:
        event, provider = case.environment(provider_class=StreamingProvider)
        tts = case.context.tts[event.unified_msg_origin]
        tts.model = "cosyvoice-v3-flash"
        tts.provider_config.update(
            type="dashscope_tts", api_key="synthetic-key", dashscope_tts_voice="synthetic"
        )

        async def native(text):
            return str(ROOT / "bot_call/static/fixed-sample.wav")

        tts.synthesizer = native
        original_tools = case.tools
        case.tools = lambda: replace(original_tools(), segment_converter=convert_segment_audio)
        original_settings = case.settings
        case.settings = lambda: dict(original_settings(), call_tts_provider_id=tts.meta().id)
        backend, _ = await case.backend()
        _, session = await case.enter(backend, event)
        upstream = dict(mode="slow")
        sockets = set()
        fault = dict(kind=None, ready_at=None, deny_until=0)

        async with fake_dashscope("slow", delay=0.41) as slow, fake_dashscope("preclose") as failed:

            async def synth(text, settings, emit):
                return await stream_sentence(
                    text,
                    settings,
                    emit,
                    url=(failed if upstream["mode"] == "preclose" else slow).url,
                )

            async def handle(request):
                if request.path == "/smoke/":
                    return web.Response(
                        text='<script type="module" src="/smoke.js"></script>',
                        content_type="text/html",
                    )
                if request.path == "/smoke.js":
                    return web.Response(text=JS, content_type="text/javascript")
                if request.path == "/fixture":
                    return web.json_response(dict(reference=session.reference))
                if request.path == "/stats":
                    return web.json_response(
                        dict(
                            fault_ready_at=fault["ready_at"],
                            native_calls=len(tts.texts),
                            stream_calls=len(slow.seen) // 3,
                            pcm_watchers=sum(
                                q.pcm
                                for watchers in backend._runtime._watchers.values()
                                for q in watchers
                            ),
                        )
                    )
                if request.path == "/fault":
                    body = await request.json()
                    fault.update(kind=body["kind"], ready_at=None)
                    if body["kind"] == "idle-disconnect":
                        fault["deny_until"] = time.monotonic() + 2.5
                        await asyncio.gather(*(ws.close() for ws in tuple(sockets)))
                    return web.json_response(dict(ok=True))
                if request.path == "/trigger":
                    body = await request.json()
                    upstream["mode"] = body["mode"]
                    if body["id"] == "ready-disconnect":
                        # Keep generation running after first audio is ready.
                        # An 8s state wait begun now can hide the existing audio.
                        provider.gate = asyncio.Event()
                        asyncio.get_running_loop().call_later(8, provider.gate.set)
                    else:
                        provider.gate = None
                    await backend.web_audio(
                        session.token, session.reference, body["id"], WAV, "audio/wav"
                    )
                    return web.json_response(dict(ok=True))
                headers = [
                    (k.lower().encode(), v.encode())
                    for k, v in request.headers.items()
                    if k.lower() not in {"host", "origin", "cookie", "x-bot-call-reference"}
                ]
                headers += [
                    (b"host", b"calls.example.test"),
                    (b"origin", ORIGIN.encode()),
                    (b"cookie", ("__Secure-bot_call=" + session.token).encode()),
                ]
                if request.headers.get("X-Bot-Call-Reference"):
                    headers.append(
                        (b"x-bot-call-reference", request.headers["X-Bot-Call-Reference"].encode())
                    )
                scope = dict(
                    type="http",
                    scheme="https",
                    method=request.method,
                    path=request.path,
                    query_string=b"",
                    headers=headers,
                )
                if request.path == "/bot-call/ws":
                    if time.monotonic() < fault["deny_until"]:
                        return web.Response(status=503)
                    protocols = [
                        p.strip() for p in request.headers["Sec-WebSocket-Protocol"].split(",")
                    ]
                    ws = web.WebSocketResponse(protocols=protocols)
                    connected = False
                    scope.update(type="websocket", scheme="wss", subprotocols=protocols)

                    async def receive():
                        nonlocal connected
                        if not connected:
                            connected = True
                            return dict(type="websocket.connect")
                        message = await ws.receive()
                        if message.type == web.WSMsgType.TEXT:
                            return dict(type="websocket.receive", text=message.data)
                        return dict(type="websocket.disconnect")

                    async def send(message):
                        if message["type"] == "websocket.accept":
                            await ws.prepare(request)
                            sockets.add(ws)
                        elif message["type"] == "websocket.send":
                            value = json.loads(message["text"])
                            frame = value.get("pcm", {})
                            if (
                                fault["kind"] == "stream-disconnect"
                                and frame.get("generation_id") == "reply:stream-disconnect"
                                and frame.get("index") == 0
                            ):
                                fault["ready_at"] = round(time.time() * 1000)
                            ready = (
                                fault["kind"] == "ready-disconnect"
                                and value.get("first_audio", {}).get("generation_id")
                                == "reply:ready-disconnect"
                            )
                            streaming = (
                                fault["kind"] == "stream-disconnect"
                                and frame.get("generation_id") == "reply:stream-disconnect"
                                and frame.get("index") == 15
                            )
                            if ready or streaming:
                                if ready:
                                    fault["ready_at"] = round(time.time() * 1000)
                                fault["deny_until"] = time.monotonic() + 2.5
                                fault["kind"] = None
                                await ws.close()
                                return
                            if fault["kind"] == "ready-disconnect":
                                return  # Lose notifications until ready, then actively close.
                            await ws.send_str(message["text"])
                        elif message["type"] == "websocket.close":
                            await ws.close()

                    try:
                        await backend.app(scope, receive, send)
                    finally:
                        sockets.discard(ws)
                    return ws
                body, messages = await request.read(), []

                async def receive():
                    return dict(type="http.request", body=body, more_body=False)

                async def send(message):
                    messages.append(message)

                await backend.app(scope, receive, send)
                return web.Response(
                    status=messages[0]["status"],
                    body=messages[1]["body"],
                    headers={k.decode(): v.decode() for k, v in messages[0]["headers"]},
                )

            app = web.Application()
            app.router.add_route("*", "/{path:.*}", handle)
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, "127.0.0.1", 0)
            await site.start()
            origin = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
            with (
                patch("bot_call.streaming_tts.stream_sentence", synth),
                TemporaryDirectory(prefix="bot-call-m6-browser-") as profile,
            ):
                proc = await asyncio.create_subprocess_exec(
                    node_runtime(),
                    "-e",
                    DRIVER,
                    origin,
                    str(browser),
                    "1",
                    profile,
                    "warm",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    start_new_session=True,
                )
                try:
                    stdout, stderr = await asyncio.wait_for(proc.communicate(), 235)
                finally:
                    try:
                        os.killpg(proc.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    if proc.returncode is None:
                        await proc.wait()
                print(stdout.decode(), end="")
                if proc.returncode:
                    print(stderr.decode(), file=sys.stderr)
                    raise RuntimeError("m6.browser_failed")
                await backend.drain()
                state = await backend.web_state(session.token, session.reference)
                normal = [
                    r
                    for r in state.segments
                    if r["generation_id"] == "reply:normal" and r["seq"] > 0
                ]
                assert all(r["playback_state"] == "ended" for r in normal)
                # Real ffmpeg MP3 archives remain accessible, without synthesizing again.
                archive = await backend.web_segment(
                    session.token, session.reference, "reply:normal", 1
                )
                assert archive[:3] == b"ID3" or archive[:2] == b"\xff\xfb"
                timings = await backend._runtime.diagnostic_timings()
                assert any(r["stage"] == "client_pcm_play" for r in timings["stages"])
                assert any(r["stage"] == "speech_end_first_sound" for r in timings["stages"])
                print(
                    json.dumps(
                        dict(
                            ok=True,
                            archive_bytes=len(archive),
                            native_calls=len(tts.texts),
                            schema=21,
                        )
                    )
                )
    finally:
        if runner:
            await runner.cleanup()
        await case.asyncTearDown()
        case.doCleanups()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--browser", type=smoke.browser_executable, default=str(BROWSER))
    asyncio.run(main(parser.parse_args().browser))
