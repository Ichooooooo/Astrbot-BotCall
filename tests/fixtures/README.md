# 语音测试样本

测试专用，不随插件静态资源发布。由魏高（Wei Gao）、Vion Nicolas 为 The Shtooka Project 录制，男声，北京普通话，2006-10-29。以下原文件经 Wikimedia Commons 下载，许可均为 [CC BY 2.0 France](https://creativecommons.org/licenses/by/2.0/fr/)。测试只去除前导低幅静音、截取200～400毫秒并降低增益；不改变原始语速或音高。原文件保持不变。

| 文件 | 原页面 | 下载地址 | SHA256 |
| --- | --- | --- | --- |
| zh-ng.ogg（嗯） | https://commons.wikimedia.org/wiki/File:Zh-ng.ogg | https://upload.wikimedia.org/wikipedia/commons/e/ef/Zh-ng.ogg | 6c3653f3352750c224ac30b24a2791ff76d730a21a8fc73ed92fc41c6e4109e5 |
| zh-dui.ogg（对） | https://commons.wikimedia.org/wiki/File:Zh-du%C3%AC.ogg | https://upload.wikimedia.org/wikipedia/commons/d/d1/Zh-du%C3%AC.ogg | 769ee2325737f98fde430caf86dd1b0b601208bd132f1dc744e9f7af2181d536 |
| zh-hao.ogg（好） | https://commons.wikimedia.org/wiki/File:Zh-h%C7%8Eo.ogg | https://upload.wikimedia.org/wikipedia/commons/2/22/Zh-h%C7%8Eo.ogg | d490fdc48559f2f374622cf1caecfccae38d5968ace53391db5866d589a4fcc3 |

`check_noise_gate_browser.py` 用固定种子 12345 的 LCG、滤波、正弦与衰减包络生成点击、轻拍、风扇、街道、键盘类合成声；代码是来源，无第三方录音。浏览器记录每个输入 Float32 PCM 的 SHA256、样本数与实际判定。它们是代表性合成波形，不是手机触屏或实地环境录音。

运行：在 `tools/run_tests.sh` 对应的 Python 环境下执行 `tests/check_noise_gate_browser.py`（在保留历史 tag 的仓库中加 `--baseline` 可对照旧版脚本）。仅使用无头 Chromium、真实 MediaRecorder 与 Silero、本地 API 桩，不访问外部服务。

测试套件在相同音源与驱动上同时覆盖冷加载、真实 VAD 队列溢出缺口和仅辅助采样缺口；冷加载回归通过暂时阻止检测器就绪来覆盖无模型录音路径，额外真实限速冷加载使用 `check_local_vad_browser.py --cold-start`。
