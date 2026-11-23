# 随包第三方声明

Bot-Call 自有代码的 MIT 授权不覆盖下列第三方的独立许可。此清单基于随包 sources.json；原版权、完整许可与 ONNX Runtime 第三方声明均随包保留。未重新授权第三方文件。

## Silero VAD、vad-web 与 ONNX Runtime Web

- Silero VAD v5 模型：MIT，Copyright (c) 2020-present Silero Team；经 @ricky0123/vad-web 0.0.31 分发。
- vad-web 0.0.31 适配：ISC，Copyright (c) 2022-present ricky0123。Bot-Call 的 static/vad.js 使用其模型帧、上下文与 state 适配方式，static/vad-worklet.js 的 box-average 重采样改编自其 resampler.js；不是随包分发完整 vad-web npm 库。
- onnxruntime-web 1.22.0（JS/MJS/WASM）：MIT，Copyright (c) Microsoft Corporation；其内部第三方组件详见 ORT-ThirdPartyNotices.txt。

下列文件位于 `bot_call/static/vendor/vad-v5-ort-1.22.0-2623a295/`；许可全文分别为 VAD-SILERO-LICENSE.txt、ORT-LICENSE.txt、ORT-ThirdPartyNotices.txt。来源与摘要如下（SHA256 可用于核对文件，不代表安全背书）：

| 文件 | 来源 | SHA256 |
| --- | --- | --- |
| silero_vad_v5.onnx | https://cdn.jsdelivr.net/npm/@ricky0123/vad-web@0.0.31/dist/silero_vad_v5.onnx | `2623a2953f6ff3d2c1e61740c6cdb7168133479b267dfef114a4a3cc5bdd788f` |
| ort.wasm.min.js | https://cdn.jsdelivr.net/npm/onnxruntime-web@1.22.0/dist/ort.wasm.min.js | `65e09376df69107e881b5c34d2d37aed333a366b6d941073ec518168e269b87d` |
| ort-wasm-simd-threaded.mjs | https://cdn.jsdelivr.net/npm/onnxruntime-web@1.22.0/dist/ort-wasm-simd-threaded.mjs | `30dd851d9c00622940500f71ddd2ff8820c5cb65270816080175b958705385a8` |
| ort-wasm-simd-threaded.wasm | https://cdn.jsdelivr.net/npm/onnxruntime-web@1.22.0/dist/ort-wasm-simd-threaded.wasm | `71aef04959c5c1b6de461b6538e2058e306610034a85aad2742d0c7fd4533fe4` |
| VAD-SILERO-LICENSE.txt | https://raw.githubusercontent.com/ricky0123/vad/de9b3ff83fd44cd7a2b3e07e9454e6b6f0bc249d/LICENSE | `239ebca4207803d3507124517c0dd6324c0d5cf73e1c486ade16bac1788f4a79` |
| ORT-LICENSE.txt | https://raw.githubusercontent.com/microsoft/onnxruntime/v1.22.0/LICENSE | `2f07c72751aed99790b8a4869cf2311df85a860b22ded05fa22803587a48922c` |
| ORT-ThirdPartyNotices.txt | https://raw.githubusercontent.com/microsoft/onnxruntime/v1.22.0/ThirdPartyNotices.txt | `e9e90971a8e75a9a8ac0c6412e29c1202d079998389915aa485f46c816c3b4cc` |

## 内置固定录音

`bot_call/static/fixed-sample.wav` 用于声音检查、手动服务测试及浏览器测试。内容为中文短句“你好，这是 Bot-Call 的声音测试。”，由项目作者于 2026-11-17 使用自己的阿里云百炼账号调用语音合成服务（模型 cosyvoice-v3-flash，系统音色 longyingxun_v3）生成，24 kHz 单声道 16 位 PCM WAV，约 2.8 秒，未经其他编辑；不含真人录音。随本项目按 MIT 许可分发，使用时请同时遵守阿里云百炼服务条款中关于合成内容的约定。SHA256：`2e8730ba1e49312f13f5a8c2675a83bf8a42cca60ab2e180b381331403cb1e2c`。

## 运行依赖（不随本插件 ZIP 分发）

- AstrBot：AGPL-3.0，用户自行部署的宿主框架；官方来源 https://github.com/AstrBotDevs/AstrBot 。使用、修改与对外提供服务需遵守其许可，Bot-Call 的 MIT 不改变框架义务。
- ffmpeg：用户自行安装，基础为 LGPL-2.1-or-later；启用 GPL/其他可选组件的构建可能适用 GPL 或额外限制，应检查实际二进制的构建选项和许可。来源 https://ffmpeg.org/legal.html 。
- cloudflared：Apache-2.0，临时隧道模式使用用户已有二进制，插件不下载或附带。来源 https://github.com/cloudflare/cloudflared 。
- nginx/Caddy、Python、AstrBot 的 Python 依赖等由运行环境提供，不包含在此 ZIP。实际采用版本的许可证及第三方声明由安装者核对。
- DeepSeek、硅基流动、百炼与 Cloudflare 为可选外部服务，服务条款、数据政策和计费规则独立于本代码许可证。
