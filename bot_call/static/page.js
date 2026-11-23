import {mountCallPage} from "./call.js";
import {LocalVAD} from "./vad.js";
mountCallPage(window,document,{createVAD:()=>new LocalVAD(window)});
