import assert from "node:assert/strict";
import {PCMPlayer,PCM_FAILURE} from "PCM_MODULE";
function fixture() {
  let now=0,seq=0;const timers=new Map(),played=[],failures=[];let ended=0,started=0;
  const e={now:()=>now,atob:s=>Buffer.from(s,"base64").toString("binary"),
    setTimeout(fn,ms){const id=++seq;timers.set(id,{at:now+ms,fn});return id;},
    clearTimeout:id=>timers.delete(id)};
  const context={state:"running",currentTime:0,sampleRate:48000,destination:{},
    createBuffer(_,size,rate){const data=new Float32Array(size);return {duration:size/rate,getChannelData:()=>data};},
    createBufferSource(){return {connect(){},disconnect(){},stop(){},start(at,offset=0){played.push({source:this,at,offset});}};}};
  const player=new PCMPlayer(context,e,{playing:()=>started++,ended:()=>ended++,failed:r=>failures.push(r)});
  const packet=(index,end=false)=>({index,end,rate:24000,format:"s16le",data:Buffer.alloc(1920,index).toString("base64")});
  const advance=(ms,runTimers=true)=>{now+=ms;context.currentTime=now/1000;if(runTimers)for(const [id,t] of [...timers])if(t.at<=now&&timers.delete(id))t.fn();};
  return {player,packet,played,failures,timers,advance,get ended(){return ended;},get started(){return started;}};
}
{
  const f=fixture(),{player:p,packet}=f;
  p.feed(packet(0));p.feed(packet(2));p.feed(packet(2));f.advance(100);
  assert.deepEqual(f.failures,[],"small reorder failed before deadline");
  p.feed(packet(1));p.feed(packet(1));p.feed(packet(4,true));
  assert.equal(f.ended,0,"early end skipped missing audio");
  p.feed(packet(3));
  assert.equal(p.index,5);assert.equal(f.played.length,4,"duplicate played twice");
  assert.deepEqual(f.played.map(v=>v.source.buffer.getChannelData(0)[0]*32768),[0,257,514,771],"samples played out of order");
  assert.deepEqual(f.played.map(v=>Math.round(v.at*1000)),[120,160,200,240]);
  f.advance(140);for(const {source} of [...f.played])source.onended?.();
  assert.equal(f.started,1);assert.equal(f.ended,1);assert.deepEqual(f.failures,[]);
  assert.equal(f.timers.size,0,"completed player retained timer");
}
{
  const f=fixture();f.player.feed(f.packet(0));f.player.feed(f.packet(2));
  f.advance(200);assert.deepEqual(f.failures,[]);
  f.player.feed(f.packet(3));f.advance(51);
  assert.deepEqual(f.failures,[PCM_FAILURE.sequence],"later packets extended missing deadline");
  assert.equal(f.timers.size,0);
}
{
  const f=fixture();f.player.feed(f.packet(0));f.player.feed(f.packet(2));
  f.advance(251,false);f.player.feed(f.packet(1));
  assert.deepEqual(f.failures,[PCM_FAILURE.sequence],"late repair evaded deadline when timer was delayed");
}
{
  const f=fixture();f.player.feed(f.packet(0));
  for(let index=2;index<=10;index++)f.player.feed(f.packet(index));
  assert.deepEqual(f.failures,[PCM_FAILURE.sequence],"reorder window was not bounded");
}
{
  const f=fixture();f.player.feed(f.packet(0));f.player.feed(f.packet(2,true));f.player.stop();
  f.advance(1000);assert.deepEqual(f.failures,[]);assert.equal(f.timers.size,0);
}
console.log("PCM reorder, duplicate, end, missing deadline, bounds and cleanup passed");

// No end and no later index: this used to wait forever even with a live WS.
{
  const f=fixture();f.player.feed(f.packet(0));f.advance(160);
  f.played[0].source.onended();f.advance(1499);
  assert.deepEqual(f.failures,[],"failed before drained-audio grace");
  f.advance(11);assert.deepEqual(f.failures,[9],"unterminated stream waited forever");
  assert.equal(f.timers.size,0);
}

{
  const f=fixture();f.player.feed(f.packet(0));f.advance(140);f.player.interrupt();
  assert.deepEqual(f.failures,[PCM_FAILURE.stream]);assert.equal(f.timers.size,0);
  f.player.interrupt();f.player.feed(f.packet(1));assert.equal(f.failures.length,1);
}
{
  const f=fixture();f.player.feed(f.packet(0));f.player.feed(f.packet(1,true));
  f.player.interrupt();f.advance(160);f.played[0].source.onended();
  assert.equal(f.ended,1);assert.deepEqual(f.failures,[],"complete stream cut off by disconnect");
}
{
  const f=fixture();f.player.feed(f.packet(0));f.advance(1400);
  f.player.feed(f.packet(1));f.advance(1500);
  assert.deepEqual(f.failures,[],"new chunk did not renew drain deadline");
  f.player.feed(f.packet(1));f.advance(51);
  assert.deepEqual(f.failures,[PCM_FAILURE.stream],"duplicate extended drain deadline");
}

// Auto capture primes the hardware with actual silence without changing order
// or replay/failure semantics, and still exports the actual voice schedule.
{
  const f=fixture(),scheduled=[];f.player.callbacks.leadMs=180;
  f.player.callbacks.scheduled=(samples,at)=>scheduled.push({samples,at});
  f.player.feed(f.packet(0));f.player.feed(f.packet(2));f.player.feed(f.packet(1));
  f.player.feed(f.packet(3,true));
  assert.equal(f.played.length,4);
  assert(f.played[0].source.buffer.getChannelData(0).every(v=>v===0));
  assert.equal(Math.round(f.played[0].source.buffer.duration*1000),180);
  assert.deepEqual(f.played.slice(1).map(v=>Math.round(v.at*1000)),[185,225,265]);
  assert.deepEqual(scheduled.map(v=>Math.round(v.at*1000)),[185,225,265]);
  f.advance(305);for(const {source} of [...f.played])source.onended?.();
  assert.equal(f.started,1);assert.equal(f.ended,1);assert.equal(f.timers.size,0);
}

// Every resumed PCM sample equals the original suffix, including arrivals while
// paused. Native context/capture is still running and sources are recreated.
{
  const f=fixture(),p=f.player;
  const samples=Int16Array.from({length:5*960},(_,i)=>i-2000);
  const packet=i=>({index:i,end:false,rate:24000,format:"s16le",data:Buffer.from(samples.buffer,i*1920,1920).toString("base64")});
  for(let i=0;i<4;i++)p.feed(packet(i));
  f.advance(145);
  const cursor=p.softPause(25),before=f.played.length;
  assert.equal(cursor,1200);assert.equal(p.context.state,"running");
  p.feed(packet(4));p.feed({index:5,end:true,rate:24000,format:"s16le"});
  f.advance(500);assert.equal(f.played.length,before,"soft pause scheduled new audio");
  assert(p.resume());
  const recovered=f.played.slice(before).flatMap(({source,offset})=>Array.from(source.buffer.getChannelData(0).subarray(Math.round(offset*24000)),v=>Math.round(v*32768)));
  assert.deepEqual(recovered,Array.from(samples.subarray(cursor)),"PCM suffix differs by sample");
  assert(f.played.slice(before).every(({source})=>!f.played.slice(0,before).some(p=>p.source===source)));
  p.stop();assert.equal(p.resume(),false);assert.equal(f.timers.size,0);
}


// A stop during the gain ramp must stop/disconnect the old fading sources too.
{
 const f=fixture(),p=f.player;let stopped=0,disconnected=0;
 p.feed(f.packet(0));const source=f.played[0].source;
 source.stop=()=>stopped++;source.disconnect=()=>disconnected++;
 f.advance(130);p.softPause(25);assert.equal(p.fading.size,1);
 p.stop();assert.equal(p.fading.size,0);assert.equal(f.timers.size,0);
 assert(stopped>=2&&disconnected===1);assert.equal(p.resume(),false);
}
