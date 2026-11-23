// Bot-Call local signal capture. No microphone acquisition or audible output.
// Box-average resampling adapted from @ricky0123/vad-web 0.0.31 resampler.js
// (ISC); continuous fractional phase, bounded port credit and explicit tail added.
// See vendor/vad-v5-ort-1.22.0-2623a295/VAD-SILERO-LICENSE.txt.
const MAX_FRAMES = 16; // Must match the fixed begin/begun capability, never negotiated upward.
class VoiceSignalProcessor extends AudioWorkletProcessor {
  constructor() {
    super(); this.active = null; this.closed = false;
    this.port.onmessage = ({data:m}) => {
      if (m.type === "shutdown") {this.closed = true; this.active = null; return;}
      if (m.type === "begin" && !this.active) {
        this.active = {id:m.id, seq:0, credits:new Set(), samples:new Float32Array(512),
          pos:0, phase:0, sum:0, count:0, gaps:0, lost:0, bad:false, started:false, settle:m.settle===true, armGaps:0, armSkipped:0, flush:false};
        if (m.maxFrames !== MAX_FRAMES) this.fault(this.active,"capacity");
      } else if (this.active?.id === m.id) {
        if (m.type === "ack") this.active.credits.delete(m.seq);
        if (m.type === "flush") this.finish(this.active,m.end);
      }
    };
  }
  fault(a,reason) {
    if (!a.bad) {a.bad = true; this.port.postMessage({type:"fault",id:a.id,reason,channels:a.inputChannels,samples:a.inputSamples,unacked:a.credits.size});}
  }
  realign(a,start,reason,dropped=0) {
    const missing=start-a.frameStart;
    if (!Number.isSafeInteger(missing) || missing<0) {this.fault(a,"clock");return;}
    a.gaps++;a.lost+=missing+dropped;
    this.port.postMessage({type:"gap",id:a.id,start,count:a.gaps,lostMs:Math.ceil(a.lost/sampleRate*1000),reason});
    if(missing/sampleRate>0.5 || a.lost/sampleRate>2 || a.gaps>64)this.fault(a,reason);
    a.samples=new Float32Array(512);a.pos=0;a.phase=0;a.sum=0;a.count=0;a.frameStart=a.expected=start;
  }
  frame(a,end) {
    if (a.credits.size >= MAX_FRAMES) {this.realign(a,end,"overflow");return;}
    if (a.bad) {a.pos = 0; return;}
    const seq = ++a.seq, samples = a.samples, valid = a.pos;
    a.credits.add(seq);
    this.port.postMessage({type:"frame",id:a.id,seq,start:a.frameStart,end,valid,
      samples:samples.buffer},[samples.buffer]);
    a.samples = new Float32Array(512); a.pos = 0; a.frameStart = end;
  }
  process(inputs,outputs) {
    // Explicitly silent even when connected to destination to keep graph alive.
    for (const output of outputs) for (const channel of output) channel.fill(0);
    if (this.closed) return false;
    const a = this.active; if (!a) return true;
    const channels = inputs[0], values = channels?.[0];
    a.inputChannels = channels?.length || 0; a.inputSamples = values?.length || 0;
    // Arm before MediaRecorder.start: an initially empty graph is not recorded
    // coverage. Wait for an actual input quantum; main's bounded arm deadline
    // falls back if none arrives. Once begun, any missing input is a fault.
    if (!a.started && !values?.length && !a.flush) return true;
    if (!a.started) {
      // Settle only BEFORE recorder start. A new input graph can jump from
      // its first quantum to the live capture clock. Never count that startup
      // interval as recorded loss; demand 64ms of contiguous native input.
      if(a.settle) {
        if(channels?.length!==1 || !values?.length) {this.fault(a,"channels");return true;}
        if(a.armExpected!==currentFrame) {
          if(a.armExpected!==undefined) {
            a.armGaps++;a.armSkipped+=Math.max(0,currentFrame-a.armStart);
            this.port.postMessage({type:"startup-gap",id:a.id,count:Math.min(65,a.armGaps),
              skippedMs:Math.min(60000,Math.ceil(a.armSkipped/sampleRate*1000))});
          }
          a.armStart=currentFrame;
        }
        a.armExpected=currentFrame+values.length;
        if(a.armExpected-a.armStart<sampleRate*.064)return true;
      }
      a.started = true; a.start = a.frameStart = a.expected = currentFrame;
      this.port.postMessage({type:"begun",id:a.id,start:a.start,rate:sampleRate,maxFrames:MAX_FRAMES});
    }
    if (channels?.length !== 1 || !values?.length) this.fault(a,"channels");
    if (sampleRate < 16000 || sampleRate > 192000) this.fault(a,"clock");
    let skip=0;
    if (currentFrame !== a.expected && !a.bad) {
      if(currentFrame<a.expected) {
        // Only the analysis path discards overlapping samples. Keep its edge
        // monotonic; the independent capture worklet retains the original PCM.
        const back=a.expected-currentFrame;
        if(!Number.isSafeInteger(back) || back>sampleRate*.020)this.fault(a,"clock");
        else {
          skip=Math.min(values?.length || 0,back);
          this.realign(a,a.expected,"clock",skip);
        }
      }else this.realign(a,currentFrame,"clock");
    }
    if (values) {
      for (let i=skip;i<values.length;i++) {
        const v = values[i];
        if (!Number.isFinite(v) || Math.abs(v)>1) this.fault(a,"sample");
        if (a.bad) continue;
        a.sum += v; a.count++; a.phase += 16000;
        if (a.phase >= sampleRate) {
          a.samples[a.pos++] = a.sum/a.count;
          a.phase -= sampleRate; a.sum = 0; a.count = 0;
          if (a.pos === 512) this.frame(a,currentFrame+i+1);
        }
      }
      a.expected = Math.max(a.expected,currentFrame+values.length);
    }
    return true;
  }
  finish(a,requiredEnd) {
    // Tracks are already disabled at recorder onstop. Flush ONLY captured bins;
    // never invent tail coverage from a later disabled-track zero quantum.
    if (!a.started || !Number.isSafeInteger(requiredEnd) || a.expected < requiredEnd) this.fault(a,"tail");
    if (a.count && !a.bad) {a.samples[a.pos++] = a.sum/a.count;a.count = 0;}
    if (a.pos) this.frame(a,a.expected);
    this.port.postMessage({type:"flushed",id:a.id,lastSeq:a.seq,start:a.start,
      end:a.expected,bad:a.bad});
    this.active = null;
  }
}
registerProcessor("bot-call-voice-signal",VoiceSignalProcessor);
