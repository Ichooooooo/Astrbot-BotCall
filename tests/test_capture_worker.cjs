const fs=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict');
const source=fs.readFileSync('bot_call/static/capture-worker.js','utf8');
const spec=[
  ['normal',20,20000,0,0],['duration-jitter',20,20000,0,0],['duration-zero',20,0,0,0],['duration-null',20,null,0,0],
  ['offset-negative',20,20000,-6500,0],['offset-positive',20,20000,6500,0],
  ['timestamp-drift',20,20000,6500,0],['flush-extra',20,20000,0,2],
  ['10ms',10,0,-2500,1],['40ms',40,40000,20000,1],['60ms',60,null,0,2],
  ['2.5ms',2.5,2500,0,1],['invalid',20,20000,0,0],
  ['invalid-cbr',20,20000,0,0],['invalid-vbr',20,20000,0,0],['invalid-padding',20,20000,0,0],['invalid-frames',20,20000,0,0],['invalid-empty',20,20000,0,0],
  ['missing-audio',20,20000,0,0],['head',20,20000,0,0],['skip',20,20000,0,0],
  ['exception',20,20000,0,0]
];
function packet(ms) {
  if(ms===20)return Uint8Array.of(0xf8,0xff,0xfe);
  if(ms===10)return Uint8Array.of(0xf0,0xff,0xfe);
  if(ms===2.5)return Uint8Array.of(0xe0,0xff,0xfe);
  if(ms===40)return Uint8Array.of(0xf9,0xff,0xfe,0xff,0xfe);
  return Uint8Array.of(0xfb,3,0xff,0xfe,0xff,0xfe,0xff,0xfe);
}
(async()=>{
for(const [mode,ms,duration,offset,extra] of spec) {
  const messages=[];let total=0,encoder;
  const context={Uint8Array,Float32Array,BigInt,DataView,TextEncoder,TextDecoder,Number,Math,Promise,
    self:{},performance,postMessage:m=>messages.push(m),AudioData:class{constructor(o){Object.assign(this,o);}close(){}},
    AudioEncoder:class {
      static async isConfigSupported(){return {supported:true};}
      constructor(cb){this.cb=cb;this.encodeQueueSize=0;encoder=this;}
      configure(){}
      encode(a){total+=a.numberOfFrames;}
      async flush() {
        const head=new Uint8Array(19);head.set(new TextEncoder().encode('OpusHead'));head[8]=1;head[9]=1;
        head[10]=0x38;head[11]=1;
        if(mode==='head')head[0]=0;
        if(mode==='skip'){head[10]=255;head[11]=255;}
        const count=mode==='missing-audio'?1:Math.ceil((total+312)/(ms*48))+extra;
        for(let i=0;i<count;i++) {
          const bad={'invalid':[0xfb,0],'invalid-cbr':[0xf9,0],'invalid-vbr':[0xfa,252],
            'invalid-padding':[0xfb,65,255],'invalid-frames':[0xfb,7],'invalid-empty':[]};
          const a=bad[mode]&&i===2?Uint8Array.from(bad[mode]):packet(ms);
          this.cb.output({byteLength:a.length,duration:mode==='duration-jitter'?20000+(i%2?1:-1):duration,timestamp:offset+i*ms*1000+(mode==='timestamp-drift'&&i>1?123:0),
            copyTo(b){if(mode==='exception')throw Error('private');b.set(a);}},
            i===0?{decoderConfig:{description:head}}:{});
        }
      }
      close(){}
    }};
  vm.createContext(context);vm.runInContext(source,context);
  await context.self.onmessage({data:{type:'start',id:'s'}});
  for(let i=0;i<50;i++)await context.self.onmessage({data:{type:'pcm',id:'s',samples:new Float32Array(960).buffer}});
  await context.self.onmessage({data:{type:'finish',id:'s'}});
  const error=messages.find(m=>m.type==='error'),done=messages.find(m=>m.type==='done');
  const expected=mode.startsWith('invalid')?3:{invalid:3,'missing-audio':6,head:1,skip:2,exception:7}[mode];
  if(expected) {
    assert(error,mode);assert.equal(error.code,5);assert.equal(error.metrics.client_capture_mux_reason,expected,mode);
    assert(!done);if(mode.startsWith('invalid'))assert.equal(error.metrics.client_capture_packet,2);
  }else {
    assert(!error,mode);assert.equal(done.samples,48000,mode);
    assert(done.padding>=0&&done.padding<ms*48,mode);
    assert.equal(done.metrics.client_capture_dropped,extra,mode);
    assert.equal(done.metrics.client_capture_ts_offset_us,2000000+offset);
    assert.equal(done.metrics.client_capture_ts_delta_us,2000000+(mode==='timestamp-drift'?123:0));
    if(duration===0||duration===null)assert(done.metrics.client_capture_clock_flags&1);
    if(mode==='duration-jitter') {
      assert.equal(done.metrics.client_capture_duration_min_us,20000);
      assert.equal(done.metrics.client_capture_duration_max_us,20002);
      assert(done.metrics.client_capture_clock_flags&2);
    }
    const data=Buffer.concat(messages.filter(m=>m.type==='data').map(m=>Buffer.from(m.data)));
    assert(data.length>100,mode);
    if(process.env.BOT_CALL_CODEC_EVIDENCE) {
      fs.mkdirSync(process.env.BOT_CALL_CODEC_EVIDENCE,{recursive:true});
      fs.writeFileSync(process.env.BOT_CALL_CODEC_EVIDENCE+'/'+mode+'.webm',data);
    }
  }
  console.log(JSON.stringify({mode,reason:error?.metrics.client_capture_mux_reason||0,...(done||error).metrics}));
}
})();
