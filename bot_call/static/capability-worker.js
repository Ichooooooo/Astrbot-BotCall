// Dedicated Worker/CSP path check, synthetic configuration only; no audio.
(async()=>{
  let bits=0;
  try {
    if(typeof AudioEncoder==='function')bits|=1;
    if(typeof AudioData==='function')bits|=2;
    if(bits&1)for(const [i,rate] of [16000,24000,48000].entries()) {
      try{if((await AudioEncoder.isConfigSupported({codec:'opus',sampleRate:rate,numberOfChannels:1,bitrate:24000})).supported)bits|=4<<i;}catch(_){}
    }
    postMessage(bits);
  }catch(_){postMessage(0);}
})();
