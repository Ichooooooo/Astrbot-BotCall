// Bounded mono PCM16 scheduler. Soft pause retains samples, never the input clock.
export const PCM_FAILURE = Object.freeze({sequence:1,format:2,closed:3,suspended:4,interrupted:5,schedule:6,capacity:7,permission:8,stream:9});
export class PCMPlayer {
  constructor(context, env, callbacks) {
    this.context=context;this.e=env;this.callbacks=callbacks;
    this.sources=new Set();this.fading=new Map();this.chunks=new Map();this.pending=[];this.reorder=new Map();this.index=0;this.bytes=0;this.next=0;
    this.receivedSamples=0;this.playedSamples=0;
  }
  fail(reason) {if(!this.closed){this.stop();this.callbacks.failed(reason);}}
  interrupt() {if(!this.ended)this.fail(PCM_FAILURE.stream);}
  positionSamples(at=this.context.currentTime) {
    let pos=this.playedSamples;
    for(const chunk of this.chunks.values())if(at>=chunk.start)
      pos=Math.max(pos,chunk.base+Math.min(chunk.length,chunk.offset+Math.max(0,Math.round((at-chunk.start)*24000))));
    return pos;
  }
  softPause(rampMs=25) {
    if(this.closed||this.softPaused)return null;
    this.softPaused=true;this.softBytes=this.bytes;this.pauseBytes=0;this.streamDeadline=null;
    const at=this.context.currentTime+rampMs/1000;
    this.cursor=this.positionSamples(at);
    const retained=[];
    for(const [source,chunk] of this.chunks) {
      source.onended=null;
      const offset=Math.min(chunk.length,chunk.offset+Math.max(0,Math.round((at-chunk.start)*24000)));
      if(offset<chunk.length)retained.push({...chunk,offset});
      try{source.stop(at);}catch(_){} // Remain connected for the output gain ramp.
      const timer=this.e.setTimeout(()=>{try{source.disconnect();}catch(_){}this.fading.delete(source);},rampMs+5);this.fading.set(source,timer);
    }
    for(const source of this.sources)if(!this.chunks.has(source)){source.onended=null;try{source.stop(at);}catch(_){}}
    this.sources.clear();this.chunks.clear();
    this.pending=retained.concat(this.pending);this.next=0;
    this.e.clearTimeout(this.timer);this.timer=null;
    return this.cursor;
  }
  resume() {
    if(this.closed||!this.softPaused||this.context.state!=="running")return false;
    this.softPaused=false;this.firstStart=undefined;this.streamDeadline=null;
    this.playedSamples=this.cursor;this.checkContext();return !this.closed;
  }
  feed(packet) {
    if(this.closed)return;
    if(this.gapAt!=null && this.e.now()-this.gapAt>=250)return this.fail(PCM_FAILURE.sequence);
    if(!this.softPaused&&!this.ended&&this.context.state==="running"&&this.streamDeadline!=null&&this.e.now()>=this.streamDeadline)return this.fail(PCM_FAILURE.stream);
    if(!Number.isSafeInteger(packet.index)||packet.index<0)return this.fail(PCM_FAILURE.sequence);
    if(packet.index<this.index||this.reorder.has(packet.index))return;
    if(this.ended||packet.index-this.index>8)return this.fail(PCM_FAILURE.sequence);
    if(packet.rate!==24000||packet.format!=="s16le")return this.fail(PCM_FAILURE.format);
    let raw=null;
    if(!packet.end) {
      try{raw=this.e.atob(packet.data);}catch(_){return this.fail(PCM_FAILURE.format);}
      if(!raw.length||raw.length%2||raw.length>1920)return this.fail(PCM_FAILURE.format);
      if(this.softPaused) {
        const cost=raw.length*2+256; // Conservative UTF-16 storage plus chunk metadata.
        if(this.callbacks.reserve? !this.callbacks.reserve(cost):this.pauseBytes+cost>1048576)
          return this.fail(PCM_FAILURE.capacity);
        this.pauseBytes+=cost;
      }
      this.bytes+=raw.length;
      if(this.bytes>8388608||this.context.state!=='running'&&this.pending.length+this.reorder.size>=128)return this.fail(PCM_FAILURE.capacity);
    }
    this.streamDeadline=null;this.reorder.set(packet.index,raw);
    while(this.reorder.has(this.index)) {
      const data=this.reorder.get(this.index);this.reorder.delete(this.index++);
      if(data===null){if(this.reorder.size)return this.fail(PCM_FAILURE.sequence);this.ended=true;break;}
      const length=data.length/2;
      this.pending.push({data,base:this.receivedSamples,offset:0,length});
      this.receivedSamples+=length;
    }
    if(this.reorder.size)this.gapAt??=this.e.now();else this.gapAt=null;
    this.checkContext();
  }
  checkContext() {
    if(this.closed)return;
    this.e.clearTimeout(this.timer);this.timer=null;
    if(this.gapAt!=null&&this.e.now()-this.gapAt>=250)return this.fail(PCM_FAILURE.sequence);
    if(this.softPaused)return;
    const context=this.context,state=context.state;
    if(state!=="running") {
      if(state!=="suspended"&&state!=="interrupted")return this.fail(PCM_FAILURE.closed);
      this.streamDeadline=null;this.pauseAt??=this.e.now();
      if(this.e.now()-this.pauseAt>=1500)return this.fail(PCM_FAILURE[state]);
      if(!this.resuming) {
        this.resuming=true;
        try{Promise.resolve(context.resume()).catch(error=>{if(!this.closed&&error?.name==="NotAllowedError")this.fail(PCM_FAILURE.permission);}).finally(()=>{this.resuming=false;});}catch(_){this.resuming=false;}
      }
    } else {
      this.pauseAt=null;
      try {
        // At most 300 ms of sources ahead; unplayed chunks remain ordered.
        while(this.pending.length&&(!this.next||this.next<=context.currentTime+(this.callbacks.lookahead??Infinity))) {
          const chunk=this.pending.shift();
          if(!chunk.buffer) {
            const samples=(chunk.buffer=context.createBuffer(1,chunk.length,24000)).getChannelData(0);
            for(let i=0;i<samples.length;i++){const v=chunk.data.charCodeAt(i*2)|(chunk.data.charCodeAt(i*2+1)<<8);samples[i]=(v>=32768?v-65536:v)/32768;}
            chunk.data=null;
          }
          const source=context.createBufferSource();this.sources.add(source);
          source.buffer=chunk.buffer;source.connect(this.callbacks.output||context.destination);
          if(!this.next&&this.firstStart===undefined&&this.cursor===undefined&&this.callbacks.leadMs) {
            const silent=context.createBufferSource();this.sources.add(silent);
            silent.buffer=context.createBuffer(1,Math.round(context.sampleRate*this.callbacks.leadMs/1000),context.sampleRate);
            silent.connect(this.callbacks.output||context.destination);const at=context.currentTime+.005;
            silent.onended=()=>{this.sources.delete(silent);silent.disconnect();this.checkContext();};
            silent.start(at);this.next=at+silent.buffer.duration;
          }
          const start=this.next?Math.max(this.next,context.currentTime+.005):context.currentTime+(this.cursor===undefined?.12:.005);
          this.firstStart??=start;this.next=start+(chunk.length-chunk.offset)/24000;chunk.start=start;this.chunks.set(source,chunk);
          source.onended=()=>{this.playedSamples=Math.max(this.playedSamples,chunk.base+chunk.length);this.chunks.delete(source);this.sources.delete(source);source.disconnect();this.checkContext();};
          source.start(start,chunk.offset/24000);
          try{this.callbacks.scheduled?.(chunk.buffer.getChannelData(0).subarray(chunk.offset),start,24000);}catch(_){}
        }
        if(!this.playing&&this.firstStart!==undefined&&context.currentTime>=this.firstStart){this.playing=true;this.callbacks.playing();}
        if(!this.ended&&this.firstStart!==undefined) {
          this.streamDeadline??=this.e.now()+Math.max(0,this.next-context.currentTime)*1000+1500;
          if(this.e.now()>=this.streamDeadline)return this.fail(PCM_FAILURE.stream);
        }
        if(this.ended&&!this.sources.size&&!this.pending.length){this.stop();this.callbacks.ended();return;}
      }catch(_){this.fail(PCM_FAILURE.schedule);return;}
    }
    if(!this.closed)this.timer=this.e.setTimeout(()=>this.checkContext(),10);
  }
  stop() {
    this.closed=true;this.e.clearTimeout(this.timer);this.pending=[];this.reorder.clear();this.gapAt=null;
    for(const source of this.sources){source.onended=null;try{source.stop();}catch(_){}try{source.disconnect();}catch(_){}}
    this.sources.clear();this.chunks.clear();
    for(const [source,timer] of this.fading){this.e.clearTimeout(timer);try{source.stop();source.disconnect();}catch(_){}}
    this.fading.clear();
  }
}
