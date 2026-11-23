// Original append-only WebM/Opus writer; framing follows RFC 6716 section 3.
// Packet samples, not WebCodecs duration/timestamps, own the audio clock.
const join=(...xs)=>{const out=new Uint8Array(xs.reduce((n,x)=>n+x.length,0));let p=0;for(const x of xs){out.set(x,p);p+=x.length;}return out;};
const hex=s=>Uint8Array.from(s.match(/../g).map(x=>parseInt(x,16)));
const uint=(n,bytes=0)=>{let v=BigInt(n);if(!bytes){bytes=1;while(v>=1n<<BigInt(bytes*8))bytes++;}const a=new Uint8Array(bytes);for(let i=bytes-1;i>=0;i--){a[i]=Number(v&255n);v>>=8n;}return a;};
const size=n=>{for(let b=1;b<=8;b++)if(BigInt(n)<(1n<<BigInt(b*7))-1n)return uint(BigInt(n)|(1n<<BigInt(b*7)),b);throw Error('size');};
const el=(id,...data)=>{const v=join(...data);return join(hex(id),size(v.length),v);};
const num=(id,n)=>el(id,uint(n)),str=(id,s)=>el(id,new TextEncoder().encode(s));
const float=n=>{const a=new Uint8Array(8);new DataView(a.buffer).setFloat64(0,n);return a;};
const config={codec:'opus',sampleRate:48000,numberOfChannels:1,bitrate:24000,opus:{frameDuration:20000}};
// reason: 1 head, 2 skip, 3 packet framing/TOC, 4 duration (reserved),
// 5 timestamp (reserved), 6 missing audio/padding, 7 exception, 8 no packets.
const bound=(v,max)=>Math.max(0,Math.min(max,Math.round(v)));
const signed=v=>bound((Number.isFinite(v)?v:0)+2000000,4000000);
let muxParts=[],probeParts=[],chain=Promise.resolve();
let encoder,owner,head,skip=0,packets=0,samples=0,coded=0,tail=[],failed=false,checking=false,clock={};
function diagnostics(reason=0,padding=coded-skip-samples) {
  return {client_capture_worker_ms:bound(clock.encodeAt===undefined?0:performance.now()-clock.encodeAt,60000),
    client_capture_mux_reason:reason,client_capture_packet:bound(packets,10000),
    client_capture_duration_us:clock.duration==null||!Number.isFinite(clock.duration)?0:bound(clock.duration+1,240001),
    client_capture_duration_min_us:clock.durationMin===undefined?0:bound(clock.durationMin+1,240001),
    client_capture_duration_max_us:clock.durationMax===undefined?0:bound(clock.durationMax+1,240001),
    client_capture_ts_delta_us:signed(clock.delta),client_capture_ts_offset_us:signed(clock.offset),
    client_capture_skip:bound(skip,5760),client_capture_packets:bound(packets,10000),
    client_capture_samples:bound(samples,2000000),client_capture_coded_samples:bound(coded,2000000),
    client_capture_padding:signed(padding),client_capture_dropped:bound(clock.dropped||0,10000),
    client_capture_clock_flags:clock.flags||0,client_capture_packet_min:clock.min||0,client_capture_packet_max:clock.max||0};
}
function fail(code=4,reason=0) {
  if(failed)return;failed=true;try{encoder?.close();}catch(_){}
  postMessage({type:'error',id:owner,code,clock:{...clock,packet:packets,skip,samples,coded},metrics:diagnostics(reason)});
}
function send(data,final=false) {
  if(checking){probeParts.push(data);return;}
  muxParts.push(data);
  if(muxParts.length>=12||final) {
    const bytes=join(...muxParts);muxParts=[];
    postMessage({type:'data',id:owner,data:bytes.buffer},[bytes.buffer]);
  }
}
function header() {
  return join(el('1a45dfa3',num('4286',1),num('42f7',1),num('42f2',4),num('42f3',8),str('4282','webm'),num('4287',4),num('4285',2)),
    hex('1853806701ffffffffffffff'),
    el('1549a966',num('2ad7b1',1000),str('4d80','Bot-Call'),str('5741','Bot-Call')),
    el('1654ae6b',el('ae',num('d7',1),num('73c5',1),num('83',2),str('86','A_OPUS'),el('63a2',head),
      num('56aa',Math.round(skip/48000*1e9)),num('56bb',80000000),
      el('e1',el('b5',float(48000)),num('9f',1)))));
}
function block(packet,discard=0) {
  const payload=join(hex('81000000'),packet.data);
  const group=el('a0',el('a1',payload),num('9b',Math.round(packet.samples/48000*1e6)),
    ...(discard?[el('75a2',uint(Math.round(discard/48000*1e9),8))]:[]));
  return el('1f43b675',num('e7',Math.round(packet.start/48000*1e6)),group);
}
function packetSamples(a) {
  if(!a.length)throw 3;
  const cfg=a[0]>>3,code=a[0]&3;
  const frame=cfg>=16?[120,240,480,960][cfg&3]:cfg>=12?[480,960][cfg&1]:[480,960,1920,2880][cfg&3];
  let pos=1,end=a.length,count=code===0?1:2,lengths=[];
  const length=()=>{if(pos>=end)throw 3;const n=a[pos++];if(n<252)return n;if(pos>=end)throw 3;return n+4*a[pos++];};
  if(code===3) {
    if(pos>=end)throw 3;
    const control=a[pos++];count=control&63;if(!count||count*frame>5760)throw 3;
    if(control&64){let n;do{if(pos>=end)throw 3;n=a[pos++];end-=n===255?254:n;}while(n===255);if(pos>end)throw 3;}
    if(control&128)for(let i=1;i<count;i++)lengths.push(length());
  } else if(code===2)lengths.push(length());
  let remaining=end-pos;
  if(lengths.length){for(const n of lengths){remaining-=n;if(n>1275||remaining<0)throw 3;}if(remaining>1275)throw 3;}
  else if(remaining<0||remaining%count||remaining/count>1275)throw 3;
  if(frame*count>5760)throw 3;
  return frame*count;
}
function drain() {
  // Do not publish any packet crossing the current known PCM end. This also
  // leaves arbitrarily many flush-only padding packets removable at finish.
  while(tail.length>1&&tail[0].start+tail[0].samples<samples+skip)send(block(tail.shift()));
}
function output(chunk,meta) {
  if(failed)return;
  let reason=7;
  try {
    clock.duration=chunk.duration;
    if(Number.isFinite(chunk.duration)){clock.durationMin=Math.min(clock.durationMin??chunk.duration,chunk.duration);clock.durationMax=Math.max(clock.durationMax??chunk.duration,chunk.duration);}
    if(!head) {
      reason=1;const description=meta?.decoderConfig?.description;
      head=ArrayBuffer.isView(description)?new Uint8Array(description.buffer,description.byteOffset,description.byteLength):new Uint8Array(description||0);
      if(head.length<19||new TextDecoder().decode(head.slice(0,8))!=='OpusHead'||head[8]>15||head[9]!==1||head[18]!==0)throw reason;
      reason=2;skip=new DataView(head.buffer,head.byteOffset).getUint16(10,true);
      if(skip>5760)throw reason;
      send(header());
    }
    reason=3;const data=new Uint8Array(chunk.byteLength);chunk.copyTo(data);
    const n=packetSamples(data);
    const expected=coded/48000*1e6;
    if(Number.isFinite(chunk.timestamp)) {
      clock.offset ??= chunk.timestamp;clock.delta=chunk.timestamp-clock.offset-expected;
      if(Math.abs(clock.delta)>2)clock.flags|=8;
    }else clock.flags|=4;
    if(chunk.duration==null||chunk.duration===0)clock.flags|=1;
    else if(Math.abs(chunk.duration-n/48000*1e6)>0)clock.flags|=2;
    clock.min=Math.min(clock.min??n,n);clock.max=Math.max(clock.max??n,n);
    tail.push({data,samples:n,start:coded});coded+=n;packets++;drain();
  }catch(e){fail(5,Number.isInteger(e)?e:7);}
}
async function open(id,probe=false) {
  try{encoder?.close();}catch(_){}
  muxParts=[];probeParts=[];chain=Promise.resolve();owner=id;checking=probe;failed=false;
  head=null;skip=0;packets=0;samples=0;coded=0;tail=[];clock={flags:0};
  if(typeof AudioEncoder!=='function'||typeof AudioData!=='function'){fail(1);return;}
  if(!(await AudioEncoder.isConfigSupported(config)).supported){fail(2);return;}
  encoder=new AudioEncoder({output,error:()=>fail(4)});encoder.configure(config);
}
async function encode(values) {
  if(failed)return;clock.encodeAt ??= performance.now();
  while(!failed&&encoder.encodeQueueSize>=8)await new Promise(resolve=>encoder.addEventListener('dequeue',resolve,{once:true}));
  if(failed)return;
  const frame=new AudioData({format:'f32',sampleRate:48000,numberOfFrames:values.length,numberOfChannels:1,
    timestamp:Math.round(samples/48000*1e6),data:values});
  try{encoder.encode(frame);samples+=values.length;drain();}finally{frame.close();}
}
async function finish() {
  await chain;if(failed)return;await encoder.flush();if(failed)return;
  let padding=coded-skip-samples;
  if(!head||!tail.length){fail(5,8);return;}
  if(padding<0){fail(5,6);return;}
  while(tail.length&&padding>=tail.at(-1).samples) {
    const p=tail.pop();padding-=p.samples;clock.dropped=(clock.dropped||0)+1;
  }
  if(!tail.length){fail(5,6);return;}
  while(tail.length>1)send(block(tail.shift()));
  send(block(tail.pop(),padding),true);encoder.close();
  const result={samples,skip,padding,packets,metrics:diagnostics(0,padding)};
  if(checking) {
    const data=join(...probeParts);probeParts=[];
    postMessage({type:'ready',...result,data:data.buffer},[data.buffer]);
  }else postMessage({type:'done',id:owner,...result});
}
self.onmessage=async({data:m})=>{
  try {
    if(m.type==='check'){await open('probe',true);if(!failed){encoder.close();postMessage({type:'ready'});}return;}
    if(m.type==='probe') {
      await open('probe',true);if(failed)return;
      for(let i=0;i<50&&!failed;i++) {
        const pcm=Float32Array.from({length:960},(_,j)=>.1*Math.sin(2*Math.PI*440*(i*960+j)/48000));
        await encode(pcm);pcm.fill(0);
      }
      await finish();return;
    }
    if(m.type==='recover') {
      await open(m.id);if(failed)return;
      for(const data of m.frames) {const values=new Float32Array(data);await encode(values);values.fill(0);if(failed)return;}
      await finish();return;
    }
    if(m.type==='start'){await open(m.id);if(!failed)postMessage({type:'started',id:owner});return;}
    if(m.id!==owner||failed)return;
    if(m.type==='pcm'){
      const values=new Float32Array(m.samples);
      chain=chain.then(async()=>{await encode(values);values.fill(0);postMessage({type:'ack',id:owner});}).catch(()=>fail(4));
    }
    if(m.type==='finish')await finish();
  }catch(_){fail(checking?2:5,7);}
};
