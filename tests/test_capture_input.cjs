// Real worklet source in a deterministic render harness; no audio clock mocks in production.
const fs=require('fs'),vm=require('vm'),assert=require('assert/strict');
function fixture(rate=48000,ack=false) {
  let Processor;const messages=[],delivered=[];
  const env={sampleRate:rate,currentFrame:0,Float32Array,Math,Number,Set,
    AudioWorkletProcessor:class {constructor(){this.port={postMessage:m=>messages.push(structuredClone(m))};}},
    registerProcessor:(name,c)=>{Processor=c;}};
  vm.runInNewContext(fs.readFileSync('bot_call/static/capture-worklet.js','utf8'),env);
  const p=new Processor();
  const step=(n=1,values=new Float32Array(128),channels=1)=>{
    for(let i=0;i<n;i++){p.process([Array(channels).fill(values)],[[new Float32Array(128)]]);env.currentFrame+=values.length;if(ack)for(const m of messages.splice(0)) {delivered.push(m);if(m.type==='pcm')p.port.onmessage({data:{type:'ack',seq:m.seq}});}}
  };
  return {p,env,messages,delivered,step,send:m=>p.port.onmessage({data:m}),fault:()=>[...delivered,...messages].find(m=>m.type==='fault')};
}
let f=fixture();f.step(48000*5/128);
assert.equal(f.fault(),undefined);assert.equal(f.p.ring.length,25);assert.equal(f.p.credits.size,0);
assert.equal(f.messages.filter(m=>m.type==='pcm').length,0);
f.send({type:'record'});assert.equal(f.messages.at(-1).frames.length,25);
f.step(480);assert.equal(f.p.credits.size,64);assert.equal(f.fault(),undefined);
f.step(8);assert.equal(f.fault().reason,2);assert.equal(f.fault().credits,64);
f=fixture();f.step(30);f.env.currentFrame+=2640;f.step();
assert.equal(f.fault().reason,1);assert.equal(f.fault().skip,55);
for(const [reason,rate,channels,values] of [[3,48000,1,new Float32Array(128).fill(NaN)],[4,48000,2],[5,8000,1]]) {
  f=fixture(rate);f.step(1,values,channels);assert.equal(f.fault().reason,reason);
}
// FIFO prefix + active PCM + flush tail preserve every sample at the boundary.
f=fixture();f.step(400);f.send({type:'record'});f.step(40);f.send({type:'flush'});f.step(1000);
const prefix=f.messages.find(m=>m.type==='prefix').frames.reduce((n,a)=>n+a.samples.length,0);
const live=f.messages.filter(m=>m.type==='pcm').reduce((n,m)=>n+m.samples.byteLength/4,0);
const tail=f.messages.find(m=>m.type==='tail').samples.byteLength/4;
assert.equal(prefix,24000);assert.equal(live+tail,40*128+400*128%960);
assert.equal(f.fault(),undefined);assert.equal(f.p.ring.length,25);
console.log('capture input worklet: idle 5s without ACK; bounded 64 active credits; faults; exact prefix/tail passed');

// Forward gaps up to 50ms become exactly the missing duration at every test rate,
// through the same resampling phase, including a gap spanning PCM frames.
for(const rate of [44100,48000,96000]) {
  const runs=[];
  for(const ms of [0,20,21,45,50]) {
    const skip=Math.floor(rate*ms/1000);
    const t=fixture(rate,true),input=new Float32Array(128).fill(.25);
    t.step(400,input);t.send({type:'record'});t.env.currentFrame+=skip;t.step(40,input);t.send({type:'flush'});
    const all=[...t.delivered,...t.messages],parts=[];
    for(const m of all) {
      if(m.type==='prefix')parts.push(...m.frames);
      if(m.type==='pcm'||m.type==='tail')parts.push({samples:new Float32Array(m.samples),gaps:m.gaps});
    }
    let zeros=0;
    for(const part of parts)for(const g of part.gaps) {
      zeros+=g.samples;
      assert(part.samples.subarray(g.offset,g.offset+g.samples).every(v=>v===0),'nonzero padding');
    }
    assert.equal(t.fault(),undefined);
    runs.push({samples:parts.reduce((n,p)=>n+p.samples.length,0),zeros});
    // Non-integral native durations round to the resampler grid (<= 1 sample).
    assert(Math.abs(zeros-skip*48000/rate)<=1);
    assert.equal(runs.at(-1).samples-runs[0].samples,zeros);
  }
}
f=fixture();f.step(400);f.env.currentFrame+=960;f.step(8);f.send({type:'record'});
assert.equal(f.fault(),undefined);
const idle=f.messages.find(m=>m.type==='prefix');assert.equal(idle.gap.count,1);assert.equal(idle.gap.samples,960);
f=fixture();f.step(400);f.env.currentFrame-=1200;f.step();assert.equal(f.fault().reason,1);assert.equal(f.fault().skip,25);assert.equal(f.fault().forward,false);

// Exercise actual main-thread budget/accounting and the M5 slice helper.
const env={Float32Array,Set,Math,Number,performance};
vm.runInNewContext(fs.readFileSync('bot_call/static/capture.js','utf8').replace(/\bexport /g,'')+
  '\nglobalThis.Capture=ContinuousCapture;',env);
const page={e:{now:()=>0},config:{},recording:null};
const c=new env.Capture(page,{},{});c.input=(reason,details)=>{c.failure={reason,details};};
function recording(id,limit=100000) {
  const r={id,limit,state:'recording',begun:true,pcm:[],queue:[],pending:0,samples:0,failed:4};
  c.initGaps(r);c.active=r;return r;
}
let r=recording('budget');
for(let i=0;i<10;i++) {
  const v=new Float32Array(960);v.captureStart=i*960;v.gaps=[{id:i,offset:0,samples:960,max:960}];c.push(r,v);
  assert.equal(c.failure,undefined);
}
assert.equal(r.gapMetrics.client_capture_gap_ms,200);assert.equal(r.gapMetrics.client_capture_gap_count,10);
const over=new Float32Array(960);over.captureStart=9600;over.gaps=[{id:10,offset:0,samples:960,max:960}];c.push(r,over);
assert.equal(c.failure.reason,1);assert.equal(r.gapMetrics.client_capture_gap_ms,220);
c.failure=undefined;c.carry=[];
r=recording('first',640);
const cross=new Float32Array(960).fill(.25);cross.fill(0,400,880);cross.captureStart=0;cross.gaps=[{id:20,offset:400,samples:480,max:480}];
c.push(r,cross);assert.equal(r.gapMetrics.client_capture_gap_ms,5);assert.equal(r.lastSample,640);
const following=recording('next');c.push(following,c.carry.shift());
assert.equal(following.gapMetrics.client_capture_gap_ms,5);assert.equal(following.firstSample,640);
assert.equal(following.pcm[0].filter(v=>v===0).length,240);
console.log('capture forward gaps: 20/21/45/50ms padded at 44.1/48/96k; 55ms rejected; 220ms cumulative rejected; split handoff passed');
// Main thread enforces the same single-gap limit even for malformed metadata.
c.failure=undefined;r=recording('single-gap-limit');
const tooWide=new Float32Array(960);tooWide.gaps=[{id:30,offset:0,samples:960,max:2640}];
c.push(r,tooWide);assert.equal(c.failure.reason,1);assert.equal(r.gapMetrics.client_capture_gap_max_ms,55);
// Prefix counts only once; the retained prefix shares the 200ms budget.
c.failure=undefined;c.prefixGaps={count:10,samples:9600,max:960};c.prefixGapIds=Array.from({length:10},(_,i)=>i);
r=recording('prefix-budget');
for(let i=0;i<10;i++) {
  const v=new Float32Array(960);v.captureStart=i*960;v.gaps=[{id:i,offset:0,samples:960,max:960}];c.push(r,v);
}
assert.equal(r.gapBudget,9600);assert.equal(r.gapMetrics.client_capture_gap_ms,200);assert.equal(r.gapMetrics.client_capture_gap_count,10);
const prefixOver=new Float32Array(960);prefixOver.captureStart=9600;prefixOver.gaps=[{id:11,offset:0,samples:960,max:960}];
c.push(r,prefixOver);assert.equal(c.failure.reason,1);assert.equal(r.gapMetrics.client_capture_gap_ms,220);
// Earlier idle padding that rolled out of the ring remains observable,
// without consuming the new segment's live/prefix budget.
c.failure=undefined;c.prefixGaps={count:20,samples:19200,max:960};c.prefixGapIds=[19];
r=recording('idle-history');
const kept=new Float32Array(960);kept.captureStart=0;kept.gaps=[{id:19,offset:0,samples:960,max:960}];c.push(r,kept);
assert.equal(r.gapBudget,960);assert.equal(r.gapMetrics.client_capture_gap_ms,400);assert.equal(c.failure,undefined);
console.log('capture gap prefix/idle accounting: no double count, prefix budget enforced, rolled idle history retained');


// Small reverse clock reports carry NEW input: no sample inserted or removed,
// and no resampler/filter reset. Compare every output value against baseline.
for(const rate of [44100,48000,96000]) {
  let baseline;
  for(const ms of [0,3,20]) {
    const t=fixture(rate,true);
    t.step(400,new Float32Array(128).fill(.25));t.send({type:'record'});
    t.env.currentFrame-=Math.floor(rate*ms/1000);
    t.step(40,new Float32Array(128).fill(.5));t.send({type:'flush'});
    assert.equal(t.fault(),undefined);assert.equal(t.p.expected,t.env.currentFrame);
    const parts=[];
    for(const m of [...t.delivered,...t.messages]) {
      if(m.type==='prefix')parts.push(...m.frames);
      if(m.type==='pcm'||m.type==='tail')parts.push({samples:new Float32Array(m.samples),backs:m.backs});
    }
    const samples=parts.flatMap(p=>Array.from(p.samples));
    if(ms===0)baseline=samples;else assert.deepEqual(samples,baseline);
    const backs=parts.flatMap(p=>p.backs);assert.equal(backs.length,ms===0?0:1);
    if(ms)assert.equal(Math.round(backs[0].ms),ms);
  }
}
f=fixture();f.step(400);f.env.currentFrame-=144;f.step(8);f.send({type:'record'});
const backPrefix=f.messages.find(m=>m.type==='prefix');
assert.equal(backPrefix.back.count,1);assert.equal(Math.round(backPrefix.back.max),3);assert.equal(f.fault(),undefined);
// Idle statistics seed the next recording only once; events at an M5 sample
// boundary belong to the following segment, with no extra frame/time.
c.prefixBack={count:1,max:3};c.prefixBackIds=[41];r=recording('back-prefix');
let backFrame=new Float32Array(960);backFrame.captureStart=0;backFrame.backs=[{id:41,offset:100,ms:3}];
c.push(r,backFrame);assert.equal(r.gapMetrics.client_capture_clock_back_count,1);
assert.equal(r.gapMetrics.client_capture_clock_back_max_ms,3);assert.equal(r.gapMetrics.client_capture_gap_count,0);
c.carry=[];r=recording('back-split',640);
backFrame=new Float32Array(960).fill(.25);backFrame.captureStart=0;backFrame.backs=[{id:42,offset:640,ms:3}];
c.push(r,backFrame);assert.equal(r.gapMetrics.client_capture_clock_back_count,0);
const backNext=recording('back-next');c.push(backNext,c.carry.shift());
assert.equal(backNext.gapMetrics.client_capture_clock_back_count,1);assert.equal(backNext.samples,320);
assert.equal(backNext.pcm[0].length,320);
console.log('capture reverse clock: 3/20ms accepted unchanged at 44.1/48/96k; 25ms rejected; expected rebased; idle/prefix/M5 diagnostics passed');

// Timeout stages retain their identity without exposing exception text.
for(const stage of [1,2,3]) {
  assert.equal(c.startupTimeout(stage),8);
  assert.equal(page.captureStartupDiagnostics.client_capture_timeout_stage,stage);
}
