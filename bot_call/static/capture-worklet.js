// Continuous mono PCM, independent of inference/encoding. Idle ring stays here; active transport has 64-message credit.
// Fractional linear interpolation to 48 kHz; no per-quantum phase resets.
const MAX_FORWARD_GAP_MS=50,MAX_BACKWARD_MS=20;
class CaptureProcessor extends AudioWorkletProcessor {
  constructor() {
    super();this.frame=new Float32Array(960);this.pos=0;this.index=0;this.next=0;this.previous=0;this.credits=new Set();this.ring=[];this.gaps=[];this.gapSeq=0;this.idleGaps={count:0,samples:0,max:0};this.backs=[];this.backSeq=0;this.idleBack={count:0,max:0};
    if(sampleRate>48000) {
      const cutoff=.45*48000/sampleRate,n=32,taps=[];
      for(let i=0;i<n;i++) {
        const x=i-(n-1)/2;
        taps.push((Math.sin(2*Math.PI*cutoff*x)/(Math.PI*x))*(.54-.46*Math.cos(2*Math.PI*i/(n-1))));
      }
      const sum=taps.reduce((a,b)=>a+b,0);this.taps=taps.map(v=>v/sum);
      this.history=new Float32Array(n);this.filterIndex=0;
    }
    this.port.onmessage=({data:m})=>{
      if(m.type==='ack')this.credits.delete(m.seq);
      if(m.type==='record' && !this.bad) {
        this.recording=true;
        const frames=this.ring.splice(0);
        const ids=[...new Set([...frames.flatMap(f=>f.gaps),...this.gaps].map(g=>g.id))];
        const backIds=[...frames.flatMap(f=>f.backs),...this.backs].map(b=>b.id);
        this.port.postMessage({type:'prefix',frames,gap:this.idleGaps,ids,back:this.idleBack,backIds},frames.map(f=>f.samples.buffer));
        this.idleGaps={count:0,samples:0,max:0};this.idleBack={count:0,max:0};
      }
      if(m.type==='flush') {
        this.recording=false;
        const samples=this.frame.slice(0,this.pos);this.pos=0;
        this.port.postMessage({type:'tail',samples:samples.buffer,gaps:this.gaps,backs:this.backs},[samples.buffer]);this.gaps=[];this.backs=[];
      }
      if(m.type==='close'){this.closed=true;this.frame.fill(0);for(const a of this.ring)a.samples.fill(0);this.ring=[];}
    };
  }
  fault(reason,skip=0) {if(!this.bad){this.bad=true;this.port.postMessage({type:'fault',reason,forward:skip>0,
    credits:Math.min(64,this.credits.size),skip:Math.min(60000,Math.round(Math.abs(skip)/sampleRate*1000))});}}
  process(inputs,outputs) {
    for(const bus of outputs)for(const channel of bus)channel.fill(0);
    if(this.closed)return false;
    if(this.bad)return true;
    const channels=inputs[0],values=channels?.[0];
    if(!values?.length)return true;
    if(channels.length!==1){this.fault(4);return true;}
    if(sampleRate<16000||sampleRate>192000){this.fault(5);return true;}
    if(this.expected!==undefined&&this.expected!==currentFrame) {
      const skip=currentFrame-this.expected;
      if(skip<0 && -skip*1000<=sampleRate*MAX_BACKWARD_MS) {
        // Chromium can repeat a global clock value while delivering NEW input.
        // Keep every input sample and resampler phase; rebase expected below.
        const back={id:++this.backSeq,offset:this.pos,ms:-skip/sampleRate*1000};
        this.backs.push(back);
        if(!this.recording){this.idleBack.count=Math.min(1000,this.idleBack.count+1);this.idleBack.max=Math.max(this.idleBack.max,back.ms);}
      } else if(skip>0 && skip*1000<=sampleRate*MAX_FORWARD_GAP_MS) {
        // Fill the missing native time through the SAME rational resampler.
        // Reset filter history so the inserted samples are exactly zero.
        const samples=Math.max(0,Math.floor(((this.index+skip-1)*48000-this.next)/sampleRate)+1);
        const gap={id:++this.gapSeq,samples};
        if(!this.recording) {
          this.idleGaps.count=Math.min(1000,this.idleGaps.count+1);
          this.idleGaps.samples=Math.min(2880000,this.idleGaps.samples+samples);
          this.idleGaps.max=Math.max(this.idleGaps.max,samples);
        }
        this.previous=0;this.history?.fill(0);
        for(let i=0;i<skip;i++)if(!this.sample(0,gap))return true;
      } else if(this.stable){this.fault(1,skip);return true;}
      else {this.armStart=currentFrame;this.pos=0;this.index=0;this.next=0;this.gaps=[];this.backs=[];}
    }
    this.armStart ??= currentFrame;
    if(!this.stable && currentFrame+values.length-this.armStart>=sampleRate*.064) {
      this.stable=true;this.port.postMessage({type:'ready'});
    }
    this.expected=currentFrame+values.length;
    for(const raw of values) {
      let v=raw;
      if(!Number.isFinite(v)||Math.abs(v)>1)this.fault(3);
      if(this.bad)break;
      if(this.taps) {
        this.history[this.filterIndex]=v;let filtered=0;
        for(let j=0;j<this.taps.length;j++)filtered+=this.taps[j]*this.history[(this.filterIndex-j+this.taps.length)%this.taps.length];
        this.filterIndex=(this.filterIndex+1)%this.taps.length;v=filtered;
      }
      if(!this.sample(v))break;
    }
    return true;
  }
  sample(v,gap) {
    while(this.next<=this.index*48000) {
      const f=this.index-this.next/48000;
      this.frame[this.pos++]=gap?0:this.index===0?v:this.previous*f+v*(1-f);
      if(gap) {
        const last=this.gaps.at(-1);
        if(last?.id===gap.id)last.samples++;
        else this.gaps.push({id:gap.id,offset:this.pos-1,samples:1,max:gap.samples});
      }
      this.next+=sampleRate;
      if(this.pos===960) {
        if(this.recording) {
          if(this.credits.size>=64){this.fault(2);return false;}
          const seq=(this.seq||0)+1;this.seq=seq;this.credits.add(seq);
          this.port.postMessage({type:'pcm',seq,samples:this.frame.buffer,gaps:this.gaps,backs:this.backs},[this.frame.buffer]);
        } else {
          this.ring.push({samples:this.frame,gaps:this.gaps,backs:this.backs});
          if(this.ring.length>25)this.ring.shift().samples.fill(0);
        }
        this.frame=new Float32Array(960);this.pos=0;this.gaps=[];this.backs=[];
      }
    }
    this.previous=v;this.index++;return true;
  }
}
registerProcessor('bot-call-capture',CaptureProcessor);
