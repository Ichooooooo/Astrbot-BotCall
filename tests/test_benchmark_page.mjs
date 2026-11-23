// Evaluation and diagnostics page scenarios against a mock plugin-page bridge: no network, browser or dependencies.
// The fixtures include failed reviews and the explicit retry flow.
import assert from "node:assert/strict";
import vm from "node:vm";
const source = globalThis.benchmarkPageSource, html = globalThis.benchmarkPageHtml, css = globalThis.benchmarkPageCss;

class Element {
  constructor(tag) { this.tagName = tag.toUpperCase(); this.children = []; this.attributes = {}; this.listeners = {}; this.className = "";
    this.hidden = false; this.disabled = false; this.checked = false; this.style = {}; this.value = ""; }
  get textContent() { return this.children.map(child => child.textContent).join(""); }
  set textContent(value) { this.children = String(value) === "" ? [] : [{ textContent: String(value), children: [] }]; }
  append(...nodes) { for (const node of nodes) this.children.push(typeof node === "string" ? { textContent: node, children: [] } : node); }
  appendChild(node) { this.children.push(node); return node; }
  replaceChildren(...nodes) { this.children = []; this.append(...nodes); }
  setAttribute(name, value) { this.attributes[name] = String(value); }
  removeAttribute(name) { delete this.attributes[name]; }
  addEventListener(name, handler) { (this.listeners[name] ||= []).push(handler); }
  fire(name, extra = {}) { for (const handler of this.listeners[name] || []) handler({ target: this, preventDefault() {}, ...extra }); }
  click() { if (!this.disabled) this.fire("click"); }
  focus() { this.focused = true; Element.active = this; }
  select() { this.selected = true; }
  setSelectionRange(from, to) { this.range = [from, to]; }
  set innerHTML(_) { throw new Error("unsafe HTML sink used"); }
  scrollIntoView() { this.scrolled = true; }
}
const walk = (node, visit) => { visit(node); for (const child of node.children || []) walk(child, visit); };
const all = (node, test) => { const out = []; walk(node, item => { if (test(item)) out.push(item); }); return out; };
const byClass = (node, name) => all(node, item => (" " + (item.className || "") + " ").includes(" " + name + " "));
const clone = value => JSON.parse(JSON.stringify(value));

// ---- fake data (contract shapes) ------------------------------------------------------------------------------
const stageDefs = [
  { key: "endpoint", label: "判停等待", desc: "说完后等待静音确认的时间，主要由“判停时长”决定" },
  { key: "upload", label: "上传", desc: "录音从手机传到服务器" },
  { key: "stt", label: "语音识别", desc: "排队、转码与 STT 服务调用" },
  { key: "llm", label: "对话模型", desc: "模型生成第一句回复" },
  { key: "tts", label: "语音合成", desc: "第一句合成出首段音频" },
  { key: "delivery", label: "下发与起播", desc: "音频送达手机并开始播放" },
  { key: "total", label: "说完到首声", desc: "从你说完到听到第一声的总时长" },
];
const pair = (median_ms, p90_ms) => ({ median_ms, p90_ms });
const BASE_LLM = { label: "DeepSeek（关闭思考）", model: "deepseek-v4-flash", thinking: "off" };
const BASE_STT = { label: "硅基流动 Qwen3-ASR-1.7B", model: "Qwen/Qwen3-ASR-1.7B" };
const BASE_TTS = { label: "百炼 cosyvoice-v3-flash", model: "cosyvoice-v3-flash", streaming: true };
const fixture = {
  schema: 1,
  stage_defs: stageDefs,
  baseline: {
    label: "标准配置", measured_at: "2026-11-17", environment: "阿里云华东 2 核 1.6 GB，固定 HTTPS，Android QQ 内置浏览器",
    config: { llm: BASE_LLM, stt: BASE_STT, tts: BASE_TTS, access: "固定 HTTPS", silence_ms: 1400 },
    stages: { endpoint: pair(1400, 1450), upload: pair(130, 280), stt: pair(520, 800), llm: pair(900, 1300), tts: pair(600, 850), delivery: pair(250, 400), total: pair(3700, 4300) },
  },
  // Same llm model id with another display name and upper-case letters: still the same model. stt differs.
  current_config: { llm: { label: "我的模型 DeepSeek", model: "DeepSeek-V4-Flash", thinking: "off" }, stt: { label: "OpenAI Whisper", model: "whisper-1" }, tts: { ...BASE_TTS },
    access: "临时隧道", silence_ms: 1800, capture: "auto", barge_in: "auto" },
  summary: { calls: 12, stages: { endpoint: pair(1800, 1900), upload: pair(410, 900), stt: pair(540, 700), llm: pair(2100, 3000), delivery: pair(260, 420), total: pair(5400, 6100) } },
  calls: [
    { ref: "c-new", started_at: 1794895224, duration_s: 420, turns: 7,
      stages: { endpoint: { median_ms: 1800, max_ms: 1900, n: 7 }, upload: { median_ms: 128, max_ms: 284, n: 7 }, llm: { median_ms: 2300, max_ms: 3100, n: 6 }, total: { median_ms: 3700, max_ms: 5000, n: 7 } },
      flags: { access: "fixed_https", capture: "auto", barge_candidates: 2, ws_disconnects: 1, model_fetch_ms: 220 },
      hints: [{ stage: "llm", level: "warn", text: "<b>对话模型</b>比标准慢约 2.6 倍，可以换用更快的模型。" }] },
    { ref: "c-mid", started_at: 1794800000, duration_s: 45, turns: 2, stages: { upload: { median_ms: 600, max_ms: 800, n: 2 } }, flags: { access: "quick_tunnel", capture: "mixed" }, hints: [] },
    { ref: "c-old", started_at: 1794700000, duration_s: 0, turns: 0, stages: {}, flags: {}, hints: [] },
  ],
  hints: [
    { stage: "upload", level: "warn", text: "上传比标准慢约 3 倍，你在用临时隧道，建议改用固定 HTTPS。" },
    { stage: "endpoint", level: "info", text: "判停时长比标准长 0.4 秒，可以在控制面板里调短。" },
  ],
  status: { version: "0.14.0", plugin_ready: true, https_ready: true, page_url: "https://bot.example.com/bot-call/", access: "固定 HTTPS",
    call_model: { selection: "dedicated", model: "deepseek-v4-flash", available: true } },
  reviews: { pending_count: 3, reasons: { work_failed: 2, unresolved: 1 }, reason_labels: { work_failed: "生成失败", unresolved: "等待处理" },
    calls: [
      { ref: "rv-1", state: "pending", reason: "work_failed", work_state: "failed", at: 1794895224 },
      { ref: "rv-2", state: "pending", reason: "work_failed", work_state: "failed", at: 1794800000 },
      { ref: "rv-3", state: "pending", reason: "unresolved", work_state: null, at: 1794700000 },
    ], truncated: false },
  last_run: null,
};
const run = (extra = {}) => ({
  at: 1794895300, ok: false, duration_ms: 5200,
  tests: [
    { key: "stt", label: "语音识别", state: "ok", ms: 320, detail: "识别结果与预期一致" },
    { key: "llm", label: "对话模型", state: "ok", ms: 900, detail: "首句 900 ms" },
    { key: "tts", label: "语音合成", state: "fail", ms: null, detail: "服务返回余额不足" },
  ],
  selfcheck: [
    { key: "host", label: "通话服务", state: "ok", detail: "已就绪" },
    { key: "ffmpeg", label: "ffmpeg", state: "warn", detail: "版本较旧" },
    { key: "vad", label: "VAD 资源", state: "skip", detail: "未检查" },
  ],
  ...extra,
});
// Everything agrees with the baseline: no marks, nothing slower.
const calmData = () => {
  const calm = clone(fixture);
  calm.current_config = { ...clone(calm.baseline.config), access: "固定 HTTPS", capture: "auto", barge_in: "auto" };
  calm.summary.stages = clone(calm.baseline.stages); calm.hints = [];
  calm.reviews = { pending_count: 0, reasons: {}, reason_labels: {}, calls: [], truncated: false };
  return calm;
};

function boot(initial = fixture, { bridge: withBridge = true, clipboard = "ok", execCommand } = {}) {
  const ids = new Map();
  for (const match of html.matchAll(/<(\w+)([^>]*)\sid="([^"]+)"([^>]*)>/g)) {
    const element = new Element(match[1]); const attrs = match[2] + match[4];
    element.hidden = /\shidden(\s|$|>)/.test(attrs + ">"); element.disabled = /\sdisabled(\s|$|>)/.test(attrs + ">");
    ids.set(match[3], element);
  }
  Element.active = null;
  const document = { documentElement: { dataset: {}, lang: "" }, createElement: tag => new Element(tag), get activeElement() { return Element.active; },
    getElementById: id => { assert.ok(ids.has(id), "page uses an element id that index.html does not define: " + id); return ids.get(id); } };
  if (execCommand) document.execCommand = execCommand;
  const state = { data: clone(initial), gets: 0, getError: null, posts: [], postHandler: null, onContext: null, timers: [], copied: [], clipboardError: null };
  const bridge = {
    ready: () => Promise.resolve(), getContext: () => ({ isDark: false, locale: "zh-CN" }), onContext: callback => { state.onContext = callback; },
    apiGet: async (...args) => {
      assert.deepEqual(args, ["benchmark"], "GET must be namespace-relative and take no extra arguments");
      state.gets++; if (state.getError) throw state.getError; return clone(state.data);
    },
    apiPost: async (...args) => {
      assert.equal(args.length, 2); state.posts.push(clone(args));
      if (!state.postHandler) throw new Error("no handler");
      return state.postHandler(...args);
    },
  };
  const window = withBridge ? { AstrBotPluginPage: bridge } : {};
  const sandbox = { window, document, URL, setTimeout: callback => { state.timers.push(callback); return state.timers.length; } };
  if (clipboard === "ok") sandbox.navigator = { clipboard: { writeText: async value => { if (state.clipboardError) throw state.clipboardError; state.copied.push(value); } } };
  else if (clipboard === "none") sandbox.navigator = {};
  vm.runInNewContext(source, sandbox, { timeout: 1000 });
  const page = {
    document, state, el: id => document.getElementById(id),
    text: id => document.getElementById(id).textContent,
    async settle() { for (let i = 0; i < 6; i++) await new Promise(resolve => setImmediate(resolve)); },
    calls: () => byClass(document.getElementById("calls"), "call"),
    stage: key => all(document.getElementById("stages"), node => node.attributes && node.attributes["data-stage"] === key)[0],
    rows: () => byClass(document.getElementById("config"), "cfg-row").filter(row => !row.className.includes("head")),
    row: label => byClass(document.getElementById("config"), "cfg-row").find(row => row.children[0] && row.children[0].textContent === label),
  };
  return page;
}
// Boot with a changed copy of the fixture and let it settle.
async function bootWith(change, options) { const copy = clone(fixture); change(copy); const page = boot(copy, options); await page.settle(); return page; }
const marks = row => all(row, node => /(^| )tag( |$)/.test(node.className || "") && /^与标准不同/.test(node.textContent)).map(node => node.textContent);

// ---- source hygiene ------------------------------------------------------------------------------------------
assert.ok(!/innerHTML|outerHTML|insertAdjacentHTML|document\.write|eval\(|new Function|localStorage|sessionStorage|XMLHttpRequest|fetch\(|\.href\s*=|window\.open|confirm\(|alert\(|prompt\(|setInterval/.test(source),
  "page must render by text only, not store or fetch by itself, and not use modal dialogs (sandboxed iframe)");
assert.ok(!/<script[^>]*>[^<]/.test(html) && !/\sstyle=/.test(html) && !/https?:\/\//.test(html), "no inline script/style or external resource");
assert.ok(!/https?:\/\/|@import|url\(/.test(css + source), "no external resource in CSS or script");
assert.ok(/prefers-reduced-motion/.test(css) && /max-width:\s*640px/.test(css), "phone layout and reduced motion are handled");
assert.ok(/html\[data-theme="dark"\]/.test(css), "dark theme");
assert.ok(/\.rv\.failed/.test(css) && /\.rv-retry/.test(css) && /\.check/.test(css), "failed review and retry dialog styles exist (theme variables only)");
for (const id of new Set([...source.matchAll(/\$\("([^"]+)"\)/g)].map(match => match[1]))) assert.ok(html.includes('id="' + id + '"'), "index.html lacks id " + id);
assert.match(html, /<title>Bot-Call 评测与诊断<\/title>/); assert.match(html, /<h1>评测与诊断<\/h1>/);
assert.ok(!/诊断页|延迟评测页/.test(html), "the page is no longer described as two pages");

// ---- populated page ------------------------------------------------------------------------------------------
{
  const page = boot(); await page.settle();
  assert.equal(page.state.gets, 1);
  assert.equal(page.el("loading").hidden, true); assert.equal(page.el("content").hidden, false); assert.equal(page.el("notice").hidden, true);
  assert.equal(page.el("copy").disabled, false);
  assert.match(page.text("updated"), /^更新于 \d\d:\d\d:\d\d$/);
  // baseline card
  assert.equal(page.el("placeholder").hidden, true);
  assert.match(page.text("baseline-meta"), /标准配置.*2026-11-17.*阿里云华东 2 核 1\.6 GB/);
  assert.match(page.text("baseline-total"), /说完到首声（中位数）：3\.7 秒/);
  // configuration comparison by model id: the renamed/upper-case llm is the same model; stt, access and silence differ
  const diff = page.rows().filter(row => row.className.includes("diff"));
  assert.deepEqual(diff.map(row => row.children[0].textContent), ["语音识别", "接入方式", "判停时长"]);
  assert.match(diff[0].textContent, /硅基流动 Qwen3-ASR-1\.7B.*模型：Qwen\/Qwen3-ASR-1\.7B.*OpenAI Whisper.*模型：whisper-1.*与标准不同：模型不同/);
  assert.match(diff[1].textContent, /固定 HTTPS.*临时隧道.*与标准不同/);
  assert.match(diff[2].textContent, /1\.4 秒（1400 ms）.*1\.8 秒（1800 ms）/);
  assert.equal(marks(page.row("对话模型")).length, 0); assert.equal(marks(page.row("语音合成")).length, 0);
  assert.match(page.row("对话模型").textContent, /我的模型 DeepSeek.*模型：DeepSeek-V4-Flash.*思考状态：关闭/, "label, model id and thinking are shown");
  assert.match(page.row("对话模型").textContent, /DeepSeek（关闭思考）.*模型：deepseek-v4-flash.*思考状态：关闭/);
  assert.match(page.row("语音合成").textContent, /百炼 cosyvoice-v3-flash.*模型：cosyvoice-v3-flash.*首句流式：是/);
  assert.equal(all(page.el("config"), node => node.className === "tag warn").length, 3);
  assert.match(page.text("diff-summary"), /有 3 项和标准配置不同/);
  const extra = page.rows().filter(row => /录音方式|插话功能/.test(row.textContent));
  assert.equal(extra.length, 2); assert.ok(extra.every(row => !row.className.includes("diff") && /标准未设定/.test(row.textContent)));
  // stage comparison in stage_defs order; total is last and is not a sum
  const blocks = byClass(page.el("stages"), "stage");
  assert.deepEqual(blocks.map(block => block.attributes["data-stage"]), stageDefs.map(def => def.key));
  assert.deepEqual(blocks.filter(block => block.className.includes("over")).map(block => block.attributes["data-stage"]), ["upload", "llm"], "only clearly slower stages are highlighted");
  assert.match(page.stage("upload").textContent, /明显偏慢：约为标准的 3\.2 倍/);
  assert.match(page.stage("llm").textContent, /约为标准的 2\.3 倍/);
  assert.doesNotMatch(page.stage("endpoint").textContent, /偏慢/, "1.29x and +400 ms is not flagged");
  assert.match(page.stage("upload").textContent, /标准.*中位 130 ms · p90 280 ms.*你的.*中位 410 ms · p90 900 ms/);
  assert.match(page.stage("endpoint").textContent, /中位 1\.4 秒 · p90 1\.5 秒/);
  assert.match(page.stage("tts").textContent, /中位 600 ms.*暂无数据/, "a missing stage is shown as no data, not zero");
  assert.match(page.stage("total").textContent, /说完到首声.*不是上面各段相加.*中位 3\.7 秒.*中位 5\.4 秒/);
  assert.ok(!/5\.1 秒|5110/.test(page.text("stages")), "the page never adds stages up");
  assert.equal(all(page.stage("upload"), node => node.attributes && node.attributes.role === "img").length, 2);
  assert.match(page.text("stages-summary"), /最近 12 通.*有 2 段明显比标准慢/);
  assert.ok(byClass(page.stage("upload"), "med").every(node => /%$/.test(node.style.width)));
  assert.equal(byClass(page.stage("tts"), "track").filter(node => node.className.includes("empty")).length, 1);
  // backend hints are shown verbatim, in order
  const hints = byClass(page.el("hints"), "hint");
  assert.equal(hints.length, 2);
  assert.match(hints[0].textContent, /需要注意.*上传.*上传比标准慢约 3 倍，你在用临时隧道，建议改用固定 HTTPS。/);
  assert.match(hints[1].textContent, /提示.*判停等待.*判停时长比标准长 0\.4 秒/);
  assert.ok(hints[0].className.includes("warn") && hints[1].className.includes("info"));
  assert.equal(page.el("hints-empty").hidden, true);
  // calls: newest first, compact stage chips, flags, hints, expand/collapse
  const calls = page.calls();
  assert.equal(calls.length, 3); assert.equal(page.el("calls-empty").hidden, true);
  assert.match(page.text("calls-count"), /最近 3 通/);
  const first = calls[0];
  assert.match(first.textContent, /\d+月\d+日 \d\d:\d\d.*7 轮 · 通话约 7 分钟.*说完到首声 3\.7 秒/);
  const chips = byClass(first, "chip").map(chip => chip.textContent);
  assert.deepEqual(chips.filter(chip => !/接入|录音|插话|推送|模型下载/.test(chip)), ["判停等待 1.8 秒", "上传 128 ms", "对话模型 2.3 秒"]);
  assert.ok(chips.includes("接入：固定 HTTPS") && chips.includes("录音：新录音方式") && chips.includes("插话候选 2 次") && chips.includes("推送断开 1 次") && chips.includes("模型下载 220 ms"));
  assert.ok(byClass(first, "chip").find(chip => chip.textContent === "对话模型 2.3 秒").className.includes("over"), "slow stage chip is flagged");
  assert.equal(byClass(first, "hint").length, 1);
  assert.equal(byClass(first, "hint-text")[0].textContent, "<b>对话模型</b>比标准慢约 2.6 倍，可以换用更快的模型。", "hint text is rendered as text");
  assert.ok(first.className.includes("warn"));
  assert.match(calls[1].textContent, /录音：新旧方式混用/); assert.match(calls[1].textContent, /接入：临时隧道/); assert.match(calls[1].textContent, /无首声数据/);
  assert.match(calls[2].textContent, /无首声数据/); assert.equal(byClass(calls[2], "chip").length, 0);
  const detail = byClass(first, "call-detail")[0], button = first.children[0];
  assert.equal(detail.hidden, true); assert.equal(button.attributes["aria-expanded"], "false");
  button.click();
  assert.equal(detail.hidden, false); assert.equal(button.attributes["aria-expanded"], "true");
  assert.match(detail.textContent, /通话约 7 分钟，共 7 轮.*判停等待.*1\.8 秒.*1\.9 秒.*7.*说完到首声.*3\.7 秒.*5\.0 秒/);
  assert.ok(!/语音合成/.test(detail.textContent), "stages without data are left out of the detail");
  button.click(); assert.equal(detail.hidden, true); assert.equal(button.attributes["aria-expanded"], "false");
  // expanded state survives a refresh
  button.click(); page.el("refresh").click(); await page.settle();
  assert.equal(page.state.gets, 2);
  assert.equal(page.calls()[0].children[0].attributes["aria-expanded"], "true");
  assert.equal(page.calls()[1].children[0].attributes["aria-expanded"], "false");
  assert.equal(page.el("refresh").disabled, false); assert.equal(page.el("refresh").textContent, "刷新");
  // no last run yet
  assert.equal(page.el("run-result").hidden, true); assert.equal(page.text("run"), "开始一键测试");
  assert.match(html, /会真实调用一次识别、对话和合成服务，产生少量费用/);
  // the one-click test sits above the call list; status and reviews close the page
  const order = [...html.matchAll(/<section id="([^"]+)"/g)].map(match => match[1]);
  assert.deepEqual(order, ["health", "overview", "stages-card", "hints-card", "test-card", "calls-card", "status-card", "reviews-card"]);
}

// ---- configuration comparison: thinking, streaming, unconfigured, old string shape ----------------------------------------
{
  // thinking switched on although the standard is off
  const on = await bootWith(copy => { copy.current_config.llm.thinking = "on"; });
  assert.deepEqual(marks(on.row("对话模型")), ["与标准不同：开启了思考"]);
  assert.ok(on.row("对话模型").className.includes("diff")); assert.match(on.row("对话模型").textContent, /思考状态：开启/);
  assert.match(on.text("diff-summary"), /有 4 项和标准配置不同/);
  // thinking unknown: gentle hint, not counted as different
  const unknown = await bootWith(copy => { copy.current_config.llm.thinking = "unknown"; });
  assert.equal(marks(unknown.row("对话模型")).length, 0); assert.ok(!unknown.row("对话模型").className.includes("diff"));
  assert.match(unknown.row("对话模型").textContent, /思考状态：无法确认.*无法确认思考状态/);
  assert.match(unknown.text("diff-summary"), /有 3 项和标准配置不同.*思考状态无法确认，没有计入/);
  // different llm model id, with thinking and a missing thinking field
  const other = await bootWith(copy => { copy.current_config.llm = { label: "Qwen 3 Max", model: "qwen3-max", thinking: "on" }; });
  assert.deepEqual(marks(other.row("对话模型")), ["与标准不同：模型不同", "与标准不同：开启了思考"]);
  const noThinking = await bootWith(copy => { copy.current_config.llm = { label: "某模型", model: "other-model" }; });
  assert.deepEqual(marks(noThinking.row("对话模型")), ["与标准不同：模型不同"]); assert.doesNotMatch(noThinking.row("对话模型").children[2].textContent, /思考状态/);
  // streaming differs, either way
  const slow = await bootWith(copy => { copy.current_config.tts.streaming = false; });
  assert.deepEqual(marks(slow.row("语音合成")), ["与标准不同：没有使用首句流式"]); assert.match(slow.row("语音合成").textContent, /首句流式：否/);
  const fast = await bootWith(copy => { copy.baseline.config.tts.streaming = false; });
  assert.deepEqual(marks(fast.row("语音合成")), ["与标准不同：使用了首句流式"]);
  // model ids decide: another label for the same id is not different, a missing streaming flag is not compared
  const same = await bootWith(copy => { copy.current_config.tts = { label: "别名", model: "COSYVOICE-V3-FLASH" }; });
  assert.equal(marks(same.row("语音合成")).length, 0);
  // not configured (null) is shown as such and counts as different
  const nothing = await bootWith(copy => { copy.current_config.stt = null; });
  assert.match(nothing.row("语音识别").textContent, /硅基流动.*未配置.*与标准不同：未配置/); assert.ok(nothing.row("语音识别").className.includes("diff"));
  assert.match(nothing.text("diff-summary"), /有 3 项和标准配置不同/);
  const gone = await bootWith(copy => { delete copy.current_config.tts; });
  assert.match(gone.row("语音合成").textContent, /未配置/); assert.ok(gone.row("语音合成").className.includes("diff"));
  // standard not set but configured now: shown, not different
  const noBase = await bootWith(copy => { delete copy.baseline.config.stt; });
  assert.match(noBase.row("语音识别").textContent, /标准未设定.*OpenAI Whisper/); assert.ok(!noBase.row("语音识别").className.includes("diff"));
  // both missing: the row is left out
  const neither = await bootWith(copy => { delete copy.baseline.config.stt; copy.current_config.stt = null; });
  assert.equal(neither.row("语音识别"), undefined);
  const alias = await bootWith(copy => {
    copy.current_config.llm = { label: "Flash", model: "deepseek-flash", compare_model: "deepseek-v4-flash", thinking: "off" };
    copy.status.plugin_ready = false;
    copy.status.error = "临时隧道不可用：未找到 cloudflared";
  });
  assert.doesNotMatch(alias.row("对话模型").textContent, /与标准不同/);
  assert.match(alias.text("st-plugin"), /临时隧道不可用：未找到 cloudflared/);

  // old backend: plain strings still render and compare by text
  const legacy = await bootWith(copy => {
    copy.baseline.config = { llm: "DeepSeek（关闭思考）", stt: "硅基流动 Qwen3-ASR-1.7B", tts: "百炼 cosyvoice-v3-flash", access: "固定 HTTPS", silence_ms: 1400 };
    copy.current_config = { llm: "DeepSeek（关闭思考）", stt: "OpenAI Whisper", tts: "百炼 cosyvoice-v3-flash", access: "固定 HTTPS", silence_ms: 1400 };
  });
  assert.equal(legacy.el("content").hidden, false);
  assert.deepEqual(legacy.rows().filter(row => row.className.includes("diff")).map(row => row.children[0].textContent), ["语音识别"]);
  assert.match(legacy.row("语音识别").textContent, /硅基流动 Qwen3-ASR-1\.7B.*OpenAI Whisper.*与标准不同：模型不同/);
  assert.match(legacy.text("diff-summary"), /有 1 项和标准配置不同/);
  // old string on one side, object on the other: compared by display text
  const mixed = await bootWith(copy => { copy.baseline.config.llm = "DeepSeek（关闭思考）"; copy.current_config.llm = { ...BASE_LLM }; });
  assert.equal(marks(mixed.row("对话模型")).length, 0);
  // odd shapes never crash the page
  const odd = await bootWith(copy => { copy.current_config.llm = ["x"]; copy.current_config.tts = { streaming: "yes" }; copy.baseline.config.stt = 5; });
  assert.equal(odd.el("content").hidden, false);
}

// ---- runtime status ------------------------------------------------------------------------------------------
{
  const page = boot(); await page.settle();
  assert.equal(page.el("status-empty").hidden, true); assert.equal(page.el("status").hidden, false);
  assert.equal(page.text("st-version"), "0.14.0");
  assert.match(page.text("st-plugin"), /^就绪$/); assert.match(page.text("st-https"), /已就绪.*未验证外网可达/);
  assert.match(page.text("st-page"), /https:\/\/bot\.example\.com\/bot-call\//);
  const anchor = all(page.el("st-page"), node => node.tagName === "A")[0];
  assert.equal(anchor.attributes.href, "https://bot.example.com/bot-call/"); assert.equal(anchor.attributes.target, "_blank"); assert.equal(anchor.attributes.rel, "noopener noreferrer");
  assert.equal(page.text("st-access"), "固定 HTTPS");
  assert.match(page.text("st-model"), /可用.*deepseek-v4-flash.*专门给电话设置的模型/);
  // not ready, unavailable model, source selection, odd or unsafe address
  const bad = await bootWith(copy => { Object.assign(copy.status, { plugin_ready: false, https_ready: false, access: "临时隧道", page_url: "javascript:alert(1)",
    call_model: { selection: "source", model: "src-model", available: false } }); });
  assert.match(bad.text("st-plugin"), /^未就绪$/); assert.match(bad.text("st-https"), /未就绪.*固定 HTTPS/); assert.equal(bad.text("st-access"), "临时隧道");
  assert.equal(all(bad.el("st-page"), node => node.tagName === "A").length, 0); assert.match(bad.text("st-page"), /javascript:alert\(1\)/, "shown as plain text, never a link");
  assert.match(bad.text("st-model"), /不可用.*src-model.*沿用来源会话的模型.*还没有可信的来源会话/);
  for (const url of ["http://bot.example.com/bot-call/", "https://user:pw@bot.example.com/bot-call/", "https://bot.example.com/bot-call/?k=1"]) {
    const unsafe = await bootWith(copy => { copy.status.page_url = url; });
    assert.equal(all(unsafe.el("st-page"), node => node.tagName === "A").length, 0, url);
  }
  const sparse = await bootWith(copy => { copy.status = { version: "0.14.0" }; });
  assert.match(sparse.text("st-plugin"), /未知/); assert.match(sparse.text("st-page"), /未配置/); assert.match(sparse.text("st-model"), /未知/);
  // an old plugin without status and reviews: the page still works
  const old = await bootWith(copy => { delete copy.status; delete copy.reviews; });
  assert.equal(old.el("content").hidden, false); assert.equal(old.el("status-empty").hidden, false); assert.equal(old.el("status").hidden, true);
  assert.match(old.text("reviews-summary"), /没有提供通话回顾状态/); assert.equal(old.el("reviews-tag").hidden, true);
}

// ---- call reviews --------------------------------------------------------------------------------------------
{
  const page = boot(); await page.settle();
  assert.equal(page.text("reviews-tag"), "未完成 3 通"); assert.ok(page.el("reviews-tag").className.includes("warn"));
  assert.match(page.text("reviews-summary"), /有 3 通通话的回顾还没有完成/);
  assert.deepEqual(byClass(page.el("review-reasons"), "chip").map(chip => chip.textContent), ["生成失败 2 通", "等待处理 1 通"], "reasons use the Chinese labels, most frequent first");
  const rows = byClass(page.el("reviews"), "rv");
  assert.equal(rows.length, 3); assert.equal(page.el("reviews-empty").hidden, true); assert.equal(page.el("reviews-more").hidden, true);
  assert.match(rows[0].textContent, /\d+月\d+日 \d\d:\d\d.*pending.*生成失败.*处理状态：failed/);
  assert.doesNotMatch(rows[2].textContent, /处理状态/);
  assert.ok(!/rv-[123]/.test(page.text("reviews")), "the public reference is not shown as a call id");
  // none pending
  const none = boot(calmData()); await none.settle();
  assert.equal(none.el("reviews-empty").hidden, false); assert.match(html, /id="reviews-empty"[^]*?没有未完成的回顾/);
  assert.equal(none.text("reviews-tag"), "全部完成"); assert.ok(none.el("reviews-tag").className.includes("ok"));
  assert.equal(byClass(none.el("reviews"), "rv").length, 0); assert.equal(none.text("reviews-summary"), "");
  // 20 shown out of more
  const many = await bootWith(copy => {
    copy.reviews = { pending_count: 27, reasons: { a: 20, b: 7 }, reason_labels: { a: "原因甲" }, truncated: true, calls: [] };
    for (let i = 0; i < 25; i++) copy.reviews.calls.push({ ref: "r" + i, state: "pending", reason: i % 2 ? "a" : "b", work_state: "running", at: 1794000000 + i * 100 });
  });
  assert.equal(byClass(many.el("reviews"), "rv").length, 20); assert.equal(many.el("reviews-more").hidden, false); assert.match(many.text("reviews-more"), /只列出最近 20 条/);
  assert.equal(many.text("reviews-tag"), "未完成 27 通");
  assert.deepEqual(byClass(many.el("review-reasons"), "chip").map(chip => chip.textContent), ["原因甲 20 通", "b 7 通"], "an unlabelled reason falls back to its key");
  const exactly = await bootWith(copy => {
    copy.reviews = { pending_count: 20, reasons: { a: 20 }, reason_labels: { a: "原因甲" }, truncated: false, calls: [] };
    for (let i = 0; i < 20; i++) copy.reviews.calls.push({ ref: "r" + i, state: "pending", reason: "a", at: 1794000000 + i });
  });
  assert.equal(byClass(exactly.el("reviews"), "rv").length, 20); assert.equal(exactly.el("reviews-more").hidden, true);
  // text is never interpreted as markup
  const markup = await bootWith(copy => { copy.reviews.reason_labels.work_failed = "<img src=x onerror=1>"; });
  assert.ok(markup.text("review-reasons").includes("<img src=x onerror=1>"));
  // a pending count without a list still renders
  const bare = await bootWith(copy => { copy.reviews = { pending_count: 2 }; });
  assert.equal(bare.text("reviews-tag"), "未完成 2 通"); assert.equal(byClass(bare.el("reviews"), "rv").length, 0); assert.equal(bare.el("reviews-empty").hidden, true);
}

// ---- overall health ------------------------------------------------------------------------------------------
{
  const warn = boot(); await warn.settle();
  assert.equal(warn.text("health-tag"), "需要注意"); assert.equal(warn.el("health").className, "card health warn");
  const items = warn.el("health-items").children.map(item => item.textContent);
  assert.ok(items.includes("有 3 通通话回顾没有完成。") && items.includes("有 1 条延迟提示需要注意。") && items.includes("有 3 项配置和标准配置不同。"));
  assert.match(warn.text("health-text"), /服务可用，但有几处需要留意/);
  const calm = boot(calmData()); await calm.settle();
  assert.equal(calm.text("health-tag"), "一切就绪"); assert.equal(calm.el("health").className, "card health ok"); assert.equal(calm.el("health-items").children.length, 0);
  const down = await bootWith(copy => { copy.status.plugin_ready = false; copy.status.call_model.available = false; });
  assert.equal(down.text("health-tag"), "有问题"); assert.equal(down.el("health").className, "card health bad");
  assert.match(down.text("health-items"), /插件还没有就绪.*电话对话模型暂不可用/);
  const noHttps = await bootWith(copy => { copy.status.https_ready = false; });
  assert.equal(noHttps.text("health-tag"), "需要注意"); assert.match(noHttps.text("health-items"), /HTTPS 入口还没有就绪/);
  const old = boot((() => { const copy = calmData(); delete copy.status; delete copy.reviews; return copy; })()); await old.settle();
  assert.equal(old.text("health-tag"), "状态未知"); assert.match(old.text("health-text"), /没有读到运行状态/);
  // a failed one-click test lowers the summary, a passing one does not
  const failed = boot(Object.assign(calmData(), { last_run: run() })); await failed.settle();
  assert.equal(failed.text("health-tag"), "需要注意"); assert.match(failed.text("health-items"), /上一次一键测试有项目没通过/);
  const passed = boot(Object.assign(calmData(), { last_run: run({ ok: true }) })); await passed.settle();
  assert.equal(passed.text("health-tag"), "一切就绪");
  // running a test updates the summary
  const live = boot(calmData()); await live.settle();
  live.state.postHandler = async () => ({ status: "ok", data: run() }); live.el("run").click(); await live.settle();
  assert.equal(live.text("health-tag"), "需要注意"); assert.match(live.text("health-items"), /上一次一键测试有项目没通过/);
  assert.match(html, /不代表手机和公网已经实测通过/);
}

// ---- placeholder baseline, theme ------------------------------------------------------------------------------
{
  const withPlaceholder = clone(fixture); withPlaceholder.baseline.placeholder = true;
  const page = boot(withPlaceholder); await page.settle();
  assert.equal(page.el("placeholder").hidden, false); assert.match(html, /id="placeholder"[^>]*>基线数据待更新</);
  assert.equal(page.document.documentElement.dataset.theme, "light");
  page.state.onContext({ isDark: true, locale: "zh-CN" }); assert.equal(page.document.documentElement.dataset.theme, "dark");
  page.state.onContext({ isDark: false, locale: "en-US" }); assert.equal(page.document.documentElement.dataset.theme, "light");
  assert.equal(page.document.documentElement.lang, "en-US");
}

// ---- same configuration, nothing slower: no marks ---------------------------------------------------------------
{
  const page = boot(calmData()); await page.settle();
  assert.equal(byClass(page.el("config"), "diff").length, 0); assert.match(page.text("diff-summary"), /和标准配置一致/);
  assert.equal(byClass(page.el("stages"), "over").length, 0); assert.match(page.text("stages-summary"), /没有哪一段明显比标准慢/);
  assert.equal(page.el("hints-empty").hidden, false); assert.match(page.text("hints-empty"), /没有需要特别注意/);
}

// ---- no calls: friendly empty state -----------------------------------------------------------------------------
{
  const empty = clone(fixture); empty.calls = []; empty.summary = { calls: 0, stages: {} }; empty.hints = [];
  const page = boot(empty); await page.settle();
  assert.equal(page.el("calls-empty").hidden, false); assert.match(html, /id="calls-empty"[^]*?先打一通电话[^]*?一键测试/);
  assert.equal(page.calls().length, 0); assert.equal(page.text("calls-count"), "");
  assert.match(page.text("stages-summary"), /还没有通话数据/);
  assert.match(page.stage("upload").textContent, /中位 130 ms.*暂无数据/);
  assert.equal(page.el("hints-empty").hidden, false); assert.match(page.text("hints-empty"), /有了通话数据后/);
  page.el("goto-test").click(); assert.equal(page.el("test-card").scrolled, true);
  // missing baseline pieces never crash the page
  const bare = clone(empty); delete bare.baseline; delete bare.current_config; delete bare.summary; delete bare.status; delete bare.reviews;
  const page2 = boot(bare); await page2.settle();
  assert.equal(page2.el("content").hidden, false); assert.match(page2.text("baseline-meta"), /暂无基线信息/); assert.match(page2.text("diff-summary"), /没有读到你的当前配置/);
  assert.equal(page2.el("baseline-total").hidden, true);
}

// ---- at most 30 calls, newest first -----------------------------------------------------------------------------
{
  const many = clone(fixture); many.calls = [];
  for (let i = 0; i < 35; i++) many.calls.push({ ref: "r" + i, started_at: 1794000000 + i * 100, duration_s: 60, turns: i + 1, stages: { total: { median_ms: 3000 + i, max_ms: 4000, n: 1 } }, flags: {}, hints: [] });
  const page = boot(many); await page.settle();
  const calls = page.calls();
  assert.equal(calls.length, 30); assert.match(calls[0].textContent, /35 轮/); assert.match(calls[29].textContent, /6 轮/);
  assert.match(page.text("calls-count"), /最近 30 通/);
}

// ---- last_run shown on load ------------------------------------------------------------------------------------
{
  const withRun = clone(fixture); withRun.last_run = run();
  const page = boot(withRun); await page.settle();
  assert.equal(page.el("run-result").hidden, false);
  const text = page.text("run-result");
  assert.match(text, /上次测试.*\d+月\d+日 \d\d:\d\d.*用时 5\.2 秒.*有项目没通过/);
  assert.match(text, /正常.*语音识别.*320 ms.*识别结果与预期一致/); assert.match(text, /失败.*语音合成.*—.*服务返回余额不足/);
  assert.match(text, /安装与环境自检.*正常.*通话服务.*已就绪.*注意.*ffmpeg.*版本较旧.*跳过.*VAD 资源/);
  assert.match(text, /重载后会清空/);
  assert.equal(page.text("run"), "重新测试");
  const rows = byClass(page.el("run-result"), "row");
  assert.equal(rows.length, 6); assert.deepEqual(rows.filter(row => row.className.includes("bad")).map(row => row.children[1].textContent), ["语音合成"]);
  assert.deepEqual(rows.filter(row => row.className.includes("warn")).map(row => row.children[1].textContent), ["ffmpeg"]);
  const passed = clone(withRun); passed.last_run.ok = true; passed.last_run.tests[2] = { key: "tts", label: "语音合成", state: "ok", ms: 700, detail: "首段音频 700 ms" };
  const page2 = boot(passed); await page2.settle(); assert.match(page2.text("run-result"), /全部通过/);
}

// ---- running a test ---------------------------------------------------------------------------------------------
{
  const page = boot(); await page.settle();
  let finish; page.state.postHandler = () => new Promise(resolve => { finish = resolve; });
  page.el("run").click(); await page.settle();
  assert.deepEqual(page.state.posts, [["benchmark", { action: "run" }]]);
  assert.equal(page.el("run").disabled, true); assert.equal(page.text("run"), "测试中…");
  assert.equal(page.el("run-progress").hidden, false); assert.equal(page.el("run-status").hidden, false);
  assert.match(page.text("run-status"), /正在测试，已用 \d+ 秒.*5～30 秒/);
  assert.equal(page.state.timers.length, 1, "elapsed counter ticks");
  page.el("run").fire("click"); await page.settle(); assert.equal(page.state.posts.length, 1, "no second request while running");
  page.state.timers.shift()(); assert.equal(page.state.timers.length, 1, "keeps ticking while running");
  finish({ status: "ok", data: run() }); await page.settle();
  assert.equal(page.el("run").disabled, false); assert.equal(page.text("run"), "重新测试");
  assert.equal(page.el("run-progress").hidden, true); assert.equal(page.el("run-status").hidden, true);
  assert.match(page.text("run-result"), /本次测试.*语音合成.*服务返回余额不足/); assert.doesNotMatch(page.text("run-result"), /重载后会清空/);
  assert.equal(page.el("run-notice").className, "notice warn"); assert.match(page.text("run-notice"), /有项目没通过/);
  page.state.timers.shift()(); assert.equal(page.state.timers.length, 0, "the counter stops after the test");
  // a later page refresh that returns an older server-side result must not replace the fresh one
  page.state.data.last_run = run({ at: 1700000000, ok: true }); page.el("refresh").click(); await page.settle();
  assert.match(page.text("run-result"), /本次测试/); assert.equal(page.el("notice").hidden, true);
  // an unwrapped result (bridge already resolves data) works too, and a passing run says so
  page.state.postHandler = async () => run({ ok: true, at: 1794895999 });
  page.el("run").click(); await page.settle();
  assert.equal(page.el("run-notice").className, "notice ok"); assert.match(page.text("run-notice"), /全部通过/);
}

// ---- error codes -------------------------------------------------------------------------------------------------
{
  const page = boot(); await page.settle();
  const attempt = async fail => { page.state.postHandler = fail; page.el("run").click(); await page.settle(); };
  const withData = code => () => { const error = new Error("中文原因"); error.data = { code }; throw error; };
  await attempt(withData("busy"));
  assert.equal(page.el("run-notice").className, "notice warn"); assert.match(page.text("run-notice"), /通话进行中.*不能测试/);
  assert.equal(page.el("run").disabled, false); assert.equal(page.el("run-progress").hidden, true); assert.equal(page.el("run-result").hidden, true);
  await attempt(async () => ({ status: "error", message: "限流", data: { code: "rate_limited" } }));
  assert.equal(page.el("run-notice").className, "notice warn"); assert.match(page.text("run-notice"), /至少要隔 30 秒/);
  await attempt(withData("disabled"));
  assert.equal(page.el("run-notice").className, "notice error"); assert.match(page.text("run-notice"), /不可用.*没有启用/);
  const before = page.state.gets;
  await attempt(() => { throw new Error("bot_call.benchmark_running"); });
  assert.match(page.text("run-notice"), /上一次测试还在运行/);
  const link = all(page.el("run-notice"), node => node.tagName === "BUTTON")[0];
  assert.equal(link.textContent, "刷新看结果"); link.click(); await page.settle(); assert.equal(page.state.gets, before + 1);
  await attempt(() => { throw new Error("rate_limited"); }); assert.match(page.text("run-notice"), /至少要隔 30 秒/);
  // unknown failures: Chinese server text is shown, anything else is not leaked
  await attempt(() => { throw new Error("服务暂时不可用：超时"); });
  assert.equal(page.el("run-notice").className, "notice error"); assert.match(page.text("run-notice"), /测试没有完成：服务暂时不可用：超时/);
  await attempt(() => { throw new Error("SYNTHETIC_SECRET_TOKEN exploded"); });
  assert.match(page.text("run-notice"), /测试没有完成：没能连上插件或管理端/); assert.ok(!page.text("run-notice").includes("SYNTHETIC_SECRET"));
  await attempt(async () => ({ status: "ok", data: { ok: true } }));
  assert.match(page.text("run-notice"), /测试没有完成/); assert.equal(page.el("run").disabled, false);
}

// ---- copy data ---------------------------------------------------------------------------------------------------
const DATA_MARK = "===== 数据（JSON）=====\n";
const split = copied => { const at = copied.indexOf(DATA_MARK); assert.ok(at > 0, "preface comes before the data"); return { preface: copied.slice(0, at), json: copied.slice(at + DATA_MARK.length) }; };
{
  // success: preface + indented JSON of the GET data, nothing from the page itself
  const page = boot(); assert.equal(page.el("copy").disabled, true, "nothing to copy before the data is loaded"); await page.settle();
  assert.equal(page.el("copy").disabled, false); assert.match(html, /id="copy"[^>]*>复制数据</);
  page.calls()[0].children[0].click();                       // page state: an expanded call ...
  page.state.postHandler = async () => { throw new Error("服务暂时不可用"); }; page.el("run").click(); await page.settle(); // ... and a notice
  page.el("copy").click(); await page.settle();
  assert.equal(page.state.copied.length, 1); const { preface, json } = split(page.state.copied[0]);
  assert.match(preface, /^【Bot-Call 评测与诊断数据】/);
  assert.match(preface, /Bot-Call（AstrBot 的网页语音通话插件）.*延迟评测与运行诊断数据/);
  assert.match(preface, /不含通话内容、用户原文和密钥/);
  assert.match(preface, /stage_defs.*毫秒.*不是各段之和/); assert.match(preface, /model.*不看显示名/); assert.match(preface, /thinking.*streaming|streaming.*thinking/);
  assert.match(preface, /status[^]*reviews[^]*last_run/); assert.match(preface, /请你帮我：[^]*延迟[^]*原因[^]*改进建议/); assert.match(preface, /导出时间（本机时间）：\d{4}-\d\d-\d\d \d\d:\d\d/);
  assert.deepEqual(JSON.parse(json), page.state.data, "exactly the data the backend sent");
  assert.ok(json.startsWith("{\n  \"schema\": 1,"), "indented JSON");
  assert.doesNotMatch(json, /expanded|aria-|notice|updated|c-new\W*true/); assert.ok(!json.includes("测试没有完成"), "no page notices in the copy");
  assert.equal(page.el("copy-notice").className, "notice ok"); assert.match(page.text("copy-notice"), /已复制（共 \d+ 字）.*粘贴给任何 AI/);
  assert.equal(page.el("copy-dialog").hidden, true);
  // the newest one-click result of this page replaces last_run; the rest is untouched
  page.state.postHandler = async () => ({ status: "ok", data: run({ ok: true, at: 1794895999 }) }); page.el("run").click(); await page.settle();
  page.el("copy").click(); await page.settle();
  const second = JSON.parse(split(page.state.copied[1]).json);
  assert.deepEqual(second.last_run, run({ ok: true, at: 1794895999 })); delete second.last_run; const expected = clone(page.state.data); delete expected.last_run; assert.deepEqual(second, expected);
  // refreshing clears the confirmation
  page.el("refresh").click(); await page.settle(); assert.equal(page.el("copy-notice").hidden, true);
}
{
  // clipboard refuses: a read-only text box with everything selected opens instead
  const page = boot(fixture, { execCommand: () => false }); await page.settle(); page.state.clipboardError = new Error("NotAllowedError");
  page.el("copy").click(); await page.settle();
  assert.equal(page.state.copied.length, 0); assert.equal(page.el("copy-dialog").hidden, false); assert.equal(page.el("copy-notice").hidden, true);
  const area = page.el("copy-text"); assert.match(html, /<textarea id="copy-text" readonly/);
  assert.match(area.value, /^【Bot-Call 评测与诊断数据】/); assert.deepEqual(JSON.parse(split(area.value).json), page.state.data);
  assert.equal(area.focused, true); assert.equal(area.selected, true); assert.deepEqual(area.range, [0, area.value.length]);
  assert.match(html, /id="copy-dialog"[^>]*role="dialog"[^>]*aria-modal="true"/);
  // “全选并复制”: reports what happened
  area.selected = false; page.el("copy-select").click(); assert.equal(area.selected, true); assert.match(page.text("copy-dialog-note"), /已全选，请按 Ctrl\+C/);
  const works = boot(fixture, { clipboard: "none", execCommand: command => command === "copy" }); await works.settle();
  works.el("copy").click(); await works.settle(); assert.equal(works.el("copy-dialog").hidden, false, "no clipboard API at all falls back to the box too");
  works.el("copy-select").click(); assert.match(works.text("copy-dialog-note"), /已复制，现在可以粘贴给 AI/);
  const throws = boot(fixture, { clipboard: "none", execCommand: () => { throw new Error("blocked"); } }); await throws.settle();
  throws.el("copy").click(); await throws.settle(); throws.el("copy-select").click(); assert.match(throws.text("copy-dialog-note"), /已全选/);
  // closing: button, Escape, click on the backdrop; the text is dropped and focus returns to the button
  page.el("copy-close").click(); assert.equal(page.el("copy-dialog").hidden, true); assert.equal(area.value, ""); assert.equal(page.el("copy").focused, true);
  page.el("copy").click(); await page.settle(); assert.equal(page.el("copy-dialog").hidden, false);
  page.el("copy-dialog").fire("keydown", { key: "a" }); assert.equal(page.el("copy-dialog").hidden, false);
  page.el("copy-dialog").fire("keydown", { key: "Escape" }); assert.equal(page.el("copy-dialog").hidden, true);
  page.el("copy").click(); await page.settle();
  page.el("copy-text").fire("click"); assert.equal(page.el("copy-dialog").hidden, false);
  page.el("copy-dialog").click(); assert.equal(page.el("copy-dialog").hidden, true);
  // a later successful copy closes an open box
  page.state.clipboardError = null; page.el("copy").click(); await page.settle(); assert.equal(page.el("copy-dialog").hidden, true); assert.equal(page.state.copied.length, 1);
  // after a failed refresh the data already on the page can still be copied; before any data the button stays disabled
  const failed = boot(); failed.state.getError = new Error("down"); await failed.settle(); assert.equal(failed.el("copy").disabled, true);
  failed.el("copy").fire("click"); await failed.settle(); assert.equal(failed.state.copied.length, 0); assert.equal(failed.el("copy-dialog").hidden, true);
  const later = boot(); await later.settle(); later.state.getError = new Error("down"); later.el("refresh").click(); await later.settle();
  assert.equal(later.el("copy").disabled, false); later.el("copy").click(); await later.settle(); assert.equal(later.state.copied.length, 1);
}

// ---- loading and read errors ---------------------------------------------------------------------------------------
{
  const page = boot(); page.state.getError = new Error("SYNTHETIC_SECRET_EXCEPTION");
  assert.equal(page.el("loading").hidden, false); assert.equal(page.el("content").hidden, true);
  await page.settle();
  assert.equal(page.el("loading").hidden, true); assert.equal(page.el("content").hidden, true);
  assert.equal(page.el("notice").className, "notice error"); assert.match(page.text("notice"), /读取评测与诊断数据失败/); assert.ok(!page.text("notice").includes("SYNTHETIC_SECRET"));
  assert.equal(page.el("refresh").disabled, false);
  page.state.getError = null; page.el("refresh").click(); await page.settle();
  assert.equal(page.el("content").hidden, false); assert.equal(page.el("notice").hidden, true);
  // a later failure keeps the data that is already on the page
  page.state.getError = new Error("down"); page.el("refresh").click(); await page.settle();
  assert.equal(page.el("content").hidden, false); assert.equal(page.calls().length, 3); assert.match(page.text("notice"), /刷新失败.*上一次读到的数据/);
  // malformed payloads are rejected rather than rendered
  const broken = boot({ schema: 1, stage_defs: [] }); await broken.settle();
  assert.equal(broken.el("content").hidden, true); assert.equal(broken.el("notice").className, "notice error");
  // a newer schema is rendered with a warning
  const newer = clone(fixture); newer.schema = 2; const page2 = boot(newer); await page2.settle();
  assert.equal(page2.el("content").hidden, false); assert.equal(page2.el("notice").className, "notice warn"); assert.match(page2.text("notice"), /比插件旧/);
}

// ---- not inside the AstrBot page ---------------------------------------------------------------------------------
{
  const page = boot(fixture, { bridge: false }); await page.settle();
  assert.equal(page.el("refresh").disabled, true); assert.equal(page.el("run").disabled, true); assert.equal(page.el("copy").disabled, true); assert.equal(page.el("loading").hidden, true);
  assert.match(page.text("notice"), /AstrBot 插件页面/); assert.equal(page.state.gets, 0);
}
// ---- failed reviews: rendering (contract 修订 3) ----------------------------------------------------------------------
const deferred = () => { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return { promise, resolve, reject }; };
const FAIL_DETAIL = "回顾模型调用失败：额度不足";
const retryReviews = () => ({
  pending_count: 4, reasons: { model_failed: 2, outcome_uncertain: 1, scope_required: 1 },
  reason_labels: { model_failed: "回顾模型调用失败", outcome_uncertain: "结果未知，不自动重放", scope_required: "历史保留（未自动处理）" },
  calls: [
    { ref: "rv-fail", state: "failed", reason: "model_failed", detail: FAIL_DETAIL, model: "deepseek-v4-flash", retryable: true, at: 1794895224 },
    { ref: "rv-unc", state: "running", reason: "outcome_uncertain", detail: "请求发出后进程中断，无法确认是否已有结果", model: "deepseek-v4-flash", retryable: true, at: 1794800000 },
    { ref: "rv-hard", state: "failed", reason: "model_failed", detail: "模型不存在，请检查设置", model: "gone-model", retryable: false, at: 1794750000 },
    { ref: "rv-old", state: "pending", reason: "scope_required", retryable: false, at: 1794700000 },
  ], truncated: false,
});
const PLAN = { ref: "rv-fail", model: "deepseek-v4-flash", llm_calls_min: 1, llm_calls_max: 2, may_rebill: true, context_only: true, notes: ["只写入原会话的聊天记忆。", "已成功投递的部分不会重复投递。"] };
const retryPage = (change = () => {}, options) => bootWith(copy => { copy.reviews = retryReviews(); change(copy); }, options);
const retryBtns = page => byClass(page.el("reviews"), "rv-retry");
const previewPost = ref => ["benchmark", { action: "retry_review", ref, preview: true }];
const runPost = (ref, send) => ["benchmark", { action: "retry_review", ref, preview: false, send }];
const okPlan = (plan = PLAN) => async () => ({ status: "ok", data: plan });
const thrower = (code, style) => style === "data" ? () => { const error = new Error("中文原因"); error.data = { code }; throw error; }
  : style === "reply" ? async () => ({ status: "error", message: "中文原因（" + code + "）", data: { code } }) : () => { throw new Error("中文原因（" + code + "）"); };
// Opens the dialog for the first retryable row and waits for the preview.
async function openReady(page, plan = PLAN, index = 0) {
  page.state.postHandler = okPlan(plan); retryBtns(page)[index].click(); await page.settle(); return retryBtns(page)[index];
}
{
  const page = await retryPage();
  const rows = byClass(page.el("reviews"), "rv");
  assert.deepEqual(rows.map(row => row.className), ["rv failed", "rv", "rv failed", "rv"], "only failed rows get the failed style");
  // status, Chinese reason (detail first), the review model
  assert.match(rows[0].textContent, /\d+月\d+日 \d\d:\d\d.*失败.*回顾模型调用失败：额度不足.*回顾所用模型：deepseek-v4-flash.*重试/);
  assert.match(rows[1].textContent, /进行中.*请求发出后进程中断，无法确认是否已有结果.*回顾所用模型：deepseek-v4-flash.*重试/);
  assert.match(rows[2].textContent, /失败.*模型不存在，请检查设置.*回顾所用模型：gone-model/);
  assert.match(rows[3].textContent, /待处理.*历史保留（未自动处理）/, "without detail the reason label is used");
  assert.doesNotMatch(rows[3].textContent, /回顾所用模型/, "no model, no model line");
  assert.deepEqual(rows.map(row => byClass(row, "tag")[0].className), ["tag bad", "tag info", "tag bad", "tag warn"]);
  assert.ok(!/rv-fail|rv-unc|rv-hard|rv-old/.test(page.text("reviews")), "the public reference is not shown");
  // the retry button only on retryable rows
  const buttons = retryBtns(page);
  assert.equal(buttons.length, 2); assert.deepEqual(rows.map(row => byClass(row, "rv-retry").length), [1, 1, 0, 0]);
  assert.ok(buttons.every(button => button.tagName === "BUTTON" && button.type === "button" && button.textContent === "重试" && !button.disabled));
  assert.match(buttons[0].attributes["aria-label"], /^重试 \d+月\d+日 \d\d:\d\d 的通话回顾$/);
  // summary: failed (retryable / not), unsure outcome (retryable) and history are told apart
  assert.equal(page.text("reviews-summary"), "有 4 通通话的回顾还没有完成。失败 1 通（可重试）；结果不确定 1 通（可重试）；失败 1 通（暂不能重试）；历史保留 1 通。");
  assert.equal(page.text("reviews-tag"), "未完成 4 通"); assert.equal(page.el("reviews-empty").hidden, true);
  assert.deepEqual(byClass(page.el("review-reasons"), "chip").map(chip => chip.textContent), ["回顾模型调用失败 2 通", "结果未知，不自动重放 1 通", "历史保留（未自动处理） 1 通"]);
  // overall health: every retryable unfinished row (failed or unsure) is called out, the generic unfinished line stays
  assert.equal(page.text("health-tag"), "需要注意");
  const items = page.el("health-items").children.map(item => item.textContent);
  assert.ok(items.includes("需要注意：有 2 通回顾未完成，可在下方重试。") && items.includes("有 4 通通话回顾没有完成。"), items.join("|"));
  // detail and model are text, never markup
  const markup = await retryPage(copy => { copy.reviews.calls[0].detail = "<img src=x onerror=1>"; copy.reviews.calls[0].model = "<b>m</b>"; });
  assert.ok(byClass(markup.el("reviews"), "rv")[0].textContent.includes("<img src=x onerror=1>") && markup.text("reviews").includes("<b>m</b>"));
  // 2 retryable failures, none retryable
  const two = await retryPage(copy => { copy.reviews.calls[2].retryable = true; });
  assert.match(two.text("reviews-summary"), /失败 2 通（可重试）/); assert.doesNotMatch(two.text("reviews-summary"), /暂不能重试/);
  assert.ok(two.el("health-items").children.some(item => item.textContent === "需要注意：有 3 通回顾未完成，可在下方重试。"));
  const none = await retryPage(copy => { for (const call of copy.reviews.calls) call.retryable = false; });
  assert.equal(retryBtns(none).length, 0); assert.match(none.text("reviews-summary"), /失败 2 通（暂不能重试）/);
  assert.ok(!none.el("health-items").children.some(item => /可在下方重试/.test(item.textContent)));
  // a failed review that is not counted as pending still shows (tag, summary, no “empty” message, health)
  const lone = await bootWith(copy => { copy.reviews = { pending_count: 0, reasons: { model_failed: 1 }, reason_labels: { model_failed: "回顾模型调用失败" }, calls: [retryReviews().calls[0]], truncated: false }; });
  assert.equal(lone.text("reviews-tag"), "失败 1 通"); assert.ok(lone.el("reviews-tag").className.includes("warn")); assert.equal(lone.el("reviews-empty").hidden, true);
  assert.equal(lone.text("reviews-summary"), "有 1 通通话的回顾失败了。失败 1 通（可重试）。");
  assert.equal(lone.text("health-tag"), "需要注意"); assert.ok(lone.el("health-items").children.some(item => /可在下方重试/.test(item.textContent)));
  assert.ok(!lone.el("health-items").children.some(item => /回顾没有完成/.test(item.textContent)));
  // only history rows: counted as history, no failure wording, no button
  const history = await bootWith(copy => { copy.reviews = { pending_count: 2, reasons: { scope_required: 2 }, reason_labels: { scope_required: "历史保留（未自动处理）" },
    calls: [{ ref: "h1", state: "pending", reason: "scope_required", at: 1794700000 }, { ref: "h2", state: "pending", reason: "scope_required", at: 1794600000 }], truncated: false }; });
  assert.equal(history.text("reviews-summary"), "有 2 通通话的回顾还没有完成。历史保留 2 通。"); assert.doesNotMatch(history.text("reviews-summary"), /失败/);
  assert.ok(!history.el("health-items").children.some(item => /可在下方重试/.test(item.textContent)));
}

// ---- unsure outcomes (retryable, not failed): counted, worded apart, truncation noted -----------------------------------
{
  const reviewsOf = (calls, extra = {}) => ({ pending_count: calls.length, reasons: {}, reason_labels: { outcome_uncertain: "结果未知，不自动重放" }, calls, truncated: false, ...extra });
  const unsure = ref => ({ ref, state: "running", reason: "outcome_uncertain", retryable: true, at: 1794800000 });
  const page = await bootWith(copy => { copy.reviews = reviewsOf([{ ...retryReviews().calls[0] }, unsure("u1"), unsure("u2"), { ref: "h", state: "pending", reason: "scope_required", at: 1794700000 }]); });
  assert.equal(page.text("reviews-summary"), "有 4 通通话的回顾还没有完成。失败 1 通（可重试）；结果不确定 2 通（可重试）；历史保留 1 通。");
  assert.ok(page.el("health-items").children.some(item => item.textContent === "需要注意：有 3 通回顾未完成，可在下方重试。"));
  assert.equal(retryBtns(page).length, 3);
  // a row that succeeded is not counted even if the backend still marks it retryable
  const done = await bootWith(copy => { copy.reviews = reviewsOf([{ ...unsure("d"), state: "succeeded" }]); });
  assert.doesNotMatch(done.text("reviews-summary"), /结果不确定/); assert.ok(!done.el("health-items").children.some(item => /可在下方重试/.test(item.textContent)));
  // only unsure rows and nothing pending: still visible, not “全部完成”
  const lone = await bootWith(copy => { copy.reviews = reviewsOf([unsure("u")], { pending_count: 0 }); });
  assert.equal(lone.text("reviews-tag"), "结果不确定 1 通"); assert.ok(lone.el("reviews-tag").className.includes("warn")); assert.equal(lone.el("reviews-empty").hidden, true);
  assert.equal(lone.text("reviews-summary"), "有 1 通通话的回顾结果不确定。结果不确定 1 通（可重试）。");
  // wording of the reason: backend detail, then the label, then a plain fallback
  const text = async (row, labels) => (await bootWith(copy => { copy.reviews = reviewsOf([row], { reason_labels: labels }); })).text("reviews");
  assert.match(await text({ ...unsure("a"), detail: "请求发出后进程中断" }, {}), /请求发出后进程中断/);
  assert.match(await text(unsure("b"), { outcome_uncertain: "结果未知，不自动重放" }), /结果未知，不自动重放/);
  const fallback = await text(unsure("c"), {}); assert.match(fallback, /上次请求结果不确定/); assert.ok(!fallback.includes("outcome_uncertain"));
  assert.match(await text({ ref: "e", state: "pending", reason: "other_key", at: 1794700000 }, {}), /other_key/, "other unknown reasons still show their key");
  // truncated: say that only the listed rows were counted
  const cut = await bootWith(copy => { copy.reviews = reviewsOf([unsure("u")], { pending_count: 30, truncated: true }); });
  assert.equal(cut.text("reviews-summary"), "有 30 通通话的回顾还没有完成。结果不确定 1 通（可重试）。（仅统计最近 20 条）");
  const cut2 = await bootWith(copy => { copy.reviews = reviewsOf([{ ref: "x", state: "pending", reason: "other", at: 1 }], { truncated: true, pending_count: 1 }); });
  assert.equal(cut2.text("reviews-summary"), "有 1 通通话的回顾还没有完成。（仅统计最近 20 条）");
  const full = await bootWith(copy => { copy.reviews = reviewsOf([unsure("u")], { truncated: false }); }); assert.doesNotMatch(full.text("reviews-summary"), /仅统计/);
}

// ---- old backend shape: old look, no button -----------------------------------------------------------------------
{
  const page = boot(); await page.settle();                      // default fixture: no detail / retryable / model
  assert.equal(retryBtns(page).length, 0); assert.equal(all(page.el("reviews"), node => node.tagName === "BUTTON").length, 0);
  const rows = byClass(page.el("reviews"), "rv");
  assert.deepEqual(rows.map(row => row.className), ["rv", "rv", "rv"]);
  assert.match(rows[0].textContent, /pending.*生成失败.*处理状态：failed/, "raw state and reason label as before");
  assert.equal(page.text("reviews-summary"), "有 3 通通话的回顾还没有完成。");
  assert.ok(!page.el("health-items").children.some(item => /可在下方重试/.test(item.textContent)));
  // detail only, or retryable that is not exactly true / has no reference: still no button
  const partial = await retryPage(copy => {
    copy.reviews.calls = [{ ref: "a", state: "failed", reason: "model_failed", detail: "仅有详情", at: 1794895224 }, { ref: "b", state: "failed", reason: "model_failed", retryable: "yes", at: 1794895000 },
      { state: "failed", reason: "model_failed", retryable: true, at: 1794894000 }, { ref: "", state: "failed", reason: "model_failed", retryable: true, at: 1794893000 }, { ref: "d", state: "failed", retryable: false, at: 1794892000 }];
  });
  assert.equal(retryBtns(partial).length, 0); assert.match(partial.text("reviews"), /仅有详情/);
  // a new-shape row with a legacy reason still renders when detail is missing
  const bare = await retryPage(copy => { copy.reviews.calls = [{ ref: "x", state: "weird", reason: "unknown_key", retryable: true, at: 1794895224 }]; });
  assert.match(bare.text("reviews"), /weird.*unknown_key/); assert.equal(retryBtns(bare).length, 1);
}

// ---- retry: preview, confirm, run -------------------------------------------------------------------------------------
{
  const page = await retryPage();
  const [button, other] = retryBtns(page);
  const preview = deferred(); page.state.postHandler = () => preview.promise;
  assert.equal(page.el("retry-dialog").hidden, true);
  button.click(); await page.settle();
  // step 1: only a preview request, nothing else is sent
  assert.deepEqual(page.state.posts, [previewPost("rv-fail")]);
  assert.equal(page.el("retry-dialog").hidden, false); assert.match(page.text("retry-target"), /\d+月\d+日 \d\d:\d\d.*回顾模型调用失败：额度不足/);
  assert.equal(page.el("retry-loading").hidden, false); assert.match(html, /id="retry-loading"[^>]*>[^<]*还没有调用模型/);
  assert.equal(page.el("retry-body").hidden, true); assert.equal(page.el("retry-confirm").disabled, true); assert.equal(page.el("retry-error").hidden, true);
  assert.equal(page.document.activeElement, page.el("retry-cancel"), "focus moves into the dialog");
  assert.ok(retryBtns(page).every(item => item.disabled), "no second flow while one is open"); other.click(); await page.settle(); assert.equal(page.state.posts.length, 1);
  preview.resolve({ status: "ok", data: PLAN }); await page.settle();
  // step 2: the confirmation box
  assert.equal(page.el("retry-loading").hidden, true); assert.equal(page.el("retry-body").hidden, false);
  assert.equal(page.text("retry-model"), "deepseek-v4-flash"); assert.equal(page.text("retry-calls"), "1～2 次");
  assert.equal(page.el("retry-rebill").hidden, false); assert.match(html, /id="retry-rebill"[^>]*>上次请求可能已计费，重试可能重复计费。<\/div>/);
  assert.deepEqual(page.el("retry-notes").children.map(item => item.textContent), PLAN.notes, "notes are listed verbatim");
  assert.equal(page.el("retry-send").checked, false, "QQ delivery is off by default"); assert.equal(page.el("retry-send").disabled, false);
  assert.match(html, /默认只写入聊天记忆，不在 QQ 发消息/); assert.match(html, /<span>同时发到 QQ<\/span>/); assert.ok(!/id="retry-send"[^>]*checked/.test(html));
  assert.equal(page.el("retry-confirm").disabled, false); assert.equal(page.el("retry-confirm").hidden, false); assert.equal(page.document.activeElement, page.el("retry-confirm"));
  assert.equal(page.el("retry-cancel").textContent, "取消");
  // step 3: confirm runs once, with send=false by default
  const exec = deferred(); page.state.postHandler = () => exec.promise; const gets = page.state.gets;
  page.el("retry-confirm").click(); await page.settle();
  assert.deepEqual(page.state.posts, [previewPost("rv-fail"), runPost("rv-fail", false)]);
  assert.equal(page.el("retry-confirm").disabled, true); assert.equal(page.text("retry-confirm"), "重试中…"); assert.equal(page.el("retry-send").disabled, true);
  assert.equal(page.el("retry-progress").hidden, false); assert.equal(page.el("retry-status").hidden, false); assert.match(html, /id="retry-status"[^>]*>[^<]*请不要重复点击/);
  page.el("retry-confirm").fire("click"); await page.settle(); assert.equal(page.state.posts.length, 2, "no second execute request while running");
  assert.equal(retryBtns(page)[0].textContent, "重试中…"); assert.ok(retryBtns(page).every(item => item.disabled));
  assert.equal(page.el("retry-cancel").textContent, "先关闭（仍在后台重试）");
  exec.resolve({ status: "ok", data: { ref: "rv-fail", state: "succeeded", detail: "回顾已生成" } }); await page.settle();
  assert.equal(page.el("retry-dialog").hidden, true); assert.equal(page.el("review-notice").className, "notice ok");
  assert.equal(page.text("review-notice"), "回顾重试完成，已写入聊天记忆（没有在 QQ 发消息）。（回顾已生成）");
  assert.equal(page.state.gets, gets + 1, "data is refreshed after the run");
  assert.ok(retryBtns(page).every(item => !item.disabled && item.textContent === "重试"));
  assert.equal(page.document.activeElement, retryBtns(page)[0], "focus returns to the row button");
  assert.equal(page.state.posts.length, 2);
  // refresh clears the result notice
  page.el("refresh").click(); await page.settle(); assert.equal(page.el("review-notice").hidden, true);
}
{
  // ticking “同时发到 QQ” sends send=true; the box starts unchecked again next time; wrapped and unwrapped replies both work
  const page = await retryPage();
  page.state.postHandler = async (_, body) => body.preview ? { ...PLAN, ref: body.ref, may_rebill: false, llm_calls_min: 2, llm_calls_max: 2 } : { ref: body.ref, state: "succeeded", detail: "" };
  retryBtns(page)[1].click(); await page.settle();
  assert.equal(page.el("retry-rebill").hidden, true, "no billing warning unless may_rebill is true"); assert.equal(page.text("retry-calls"), "2 次");
  page.el("retry-send").checked = true; page.el("retry-confirm").click(); await page.settle();
  assert.deepEqual(page.state.posts[1], runPost("rv-unc", true));
  assert.equal(page.text("review-notice"), "回顾重试完成，已写入聊天记忆，并发到了 QQ。");
  retryBtns(page)[0].click(); await page.settle(); assert.equal(page.el("retry-send").checked, false);
  // incomplete plans
  page.el("retry-cancel").click();
  for (const [plan, calls] of [[{ model: "m", llm_calls_min: 3 }, "至少 3 次"], [{ model: "m", llm_calls_max: 4 }, "最多 4 次"], [{ model: "m" }, "（插件没有提供）"]]) {
    await openReady(page, plan); assert.equal(page.text("retry-calls"), calls); assert.equal(page.el("retry-rebill").hidden, true); assert.equal(page.el("retry-notes").children.length, 0);
    page.el("retry-cancel").click();
  }
  await openReady(page, { llm_calls_min: 1, notes: [5, "", "有效说明", null] }); assert.equal(page.text("retry-model"), "（插件没有提供）");
  assert.deepEqual(page.el("retry-notes").children.map(item => item.textContent), ["有效说明"]);
}
{
  // other outcomes of the run
  for (const [state, detail, kind, pattern] of [["failed", "额度仍然不足", "warn", /重试后这通回顾仍然失败.*（额度仍然不足）/], ["running", "", "warn", /还在后台处理中.*先不要重复点/]]) {
    const page = await retryPage(); await openReady(page);
    page.state.postHandler = async () => ({ status: "ok", data: { ref: "rv-fail", state, detail } }); page.el("retry-confirm").click(); await page.settle();
    assert.equal(page.el("retry-dialog").hidden, true); assert.equal(page.el("review-notice").className, "notice " + kind); assert.match(page.text("review-notice"), pattern);
    assert.ok(retryBtns(page).every(item => !item.disabled));
  }
}

// ---- retry: cancel never runs anything ---------------------------------------------------------------------------------
{
  const page = await retryPage();
  const button = await openReady(page);
  page.el("retry-cancel").click();
  assert.equal(page.el("retry-dialog").hidden, true); assert.deepEqual(page.state.posts, [previewPost("rv-fail")], "cancel sends no execute request");
  assert.ok(retryBtns(page).every(item => !item.disabled)); assert.equal(page.document.activeElement, button, "focus goes back to the button that opened the box");
  assert.equal(page.el("review-notice").hidden, true);
  // Escape (and not other keys), click on the backdrop (and not inside)
  retryBtns(page)[0].click(); await page.settle(); assert.equal(page.el("retry-dialog").hidden, false);
  page.el("retry-dialog").fire("keydown", { key: "a" }); assert.equal(page.el("retry-dialog").hidden, false);
  page.el("retry-dialog").fire("click", { target: page.el("retry-confirm") }); assert.equal(page.el("retry-dialog").hidden, false);
  page.el("retry-dialog").fire("keydown", { key: "Escape" }); assert.equal(page.el("retry-dialog").hidden, true);
  retryBtns(page)[1].click(); await page.settle(); page.el("retry-dialog").click(); assert.equal(page.el("retry-dialog").hidden, true);
  assert.ok(page.state.posts.every(post => post[1].preview === true), "only previews were ever sent");
  // cancelling while the preview is still loading: the late answer is ignored and a new flow works
  const late = deferred(); page.state.postHandler = () => late.promise; retryBtns(page)[0].click(); await page.settle();
  page.el("retry-cancel").click(); late.resolve({ status: "ok", data: PLAN }); await page.settle();
  assert.equal(page.el("retry-dialog").hidden, true); assert.equal(page.el("retry-body").hidden, true); assert.ok(retryBtns(page).every(item => !item.disabled));
  await openReady(page, { ...PLAN, model: "second-model" }); assert.equal(page.text("retry-model"), "second-model");
  // a refresh while the box is open keeps the buttons locked until it is closed
  const gets = page.state.gets; page.el("refresh").click(); await page.settle();
  assert.equal(page.state.gets, gets + 1); assert.ok(retryBtns(page).every(item => item.disabled)); page.el("retry-cancel").click(); assert.ok(retryBtns(page).every(item => !item.disabled));
  // after Esc-closing a box whose row vanished after a refresh, focus falls back to the review heading
  retryBtns(page)[0].click(); await page.settle(); page.state.data.reviews.calls = []; page.el("refresh").click(); await page.settle();
  page.el("retry-dialog").fire("keydown", { key: "Escape" }); assert.equal(page.document.activeElement, page.el("reviews-title"));
}

// ---- retry: keyboard focus stays inside the box -----------------------------------------------------------------------
{
  const page = await retryPage(); const dialog = page.el("retry-dialog");
  assert.match(html, /id="retry-dialog"[^>]*role="dialog"[^>]*aria-modal="true"[^>]*aria-labelledby="retry-title"/); assert.match(html, /id="retry-title"/);
  await openReady(page);
  const press = (shift, active) => { let prevented = 0; Element.active = active; dialog.fire("keydown", { key: "Tab", shiftKey: shift, preventDefault() { prevented++; } }); return prevented; };
  assert.equal(press(false, page.el("retry-cancel")), 1); assert.equal(page.document.activeElement, page.el("retry-send"), "Tab from the last control wraps to the first");
  assert.equal(press(true, page.el("retry-send")), 1); assert.equal(page.document.activeElement, page.el("retry-cancel"), "Shift+Tab from the first wraps to the last");
  assert.equal(press(false, page.el("retry-confirm")), 0, "inside the box the browser moves the focus");
  assert.equal(press(false, null), 1); assert.equal(page.document.activeElement, page.el("retry-send"), "focus that escaped is pulled back");
  assert.equal(press(true, null), 1); assert.equal(page.document.activeElement, page.el("retry-cancel"));
  // while running only the close button can take focus
  page.state.postHandler = () => deferred().promise; page.el("retry-confirm").click(); await page.settle();
  assert.equal(press(false, page.el("retry-cancel")), 1); assert.equal(page.document.activeElement, page.el("retry-cancel"));
  assert.equal(press(true, page.el("retry-cancel")), 1);
}

// ---- retry: error codes ---------------------------------------------------------------------------------------------------
{
  const codes = [
    ["busy", /现在有通话进行中.*暂时不能重试回顾/, "notice warn", false],
    ["running", /正在重试中.*不要重复点/, "notice warn", true],
    ["not_retryable", /现在不能重试.*已为你刷新列表/, "notice warn", true],
    ["not_found", /找不到这通回顾.*已为你刷新列表/, "notice warn", true],
    ["disabled", /回顾重试目前不可用.*没有启用/, "notice error", false],
  ];
  for (const [code, pattern, kind, reloads] of codes) {
    for (const style of ["data", "reply", "message"]) {            // error.data.code / {status:"error"} reply / “（code）” in the message
      const page = await retryPage(); const gets = page.state.gets, label = code + "/" + style;
      page.state.postHandler = thrower(code, style); retryBtns(page)[0].click(); await page.settle();
      assert.equal(page.el("retry-error").hidden, false, label); assert.equal(page.el("retry-error").className, kind, label); assert.match(page.text("retry-error"), pattern, label);
      assert.equal(page.el("retry-body").hidden, true, label); assert.equal(page.el("retry-loading").hidden, true, label); assert.equal(page.el("retry-confirm").hidden, true, label);
      assert.equal(page.text("retry-cancel"), "关闭", label); assert.equal(page.document.activeElement, page.el("retry-cancel"), label);
      assert.equal(page.state.gets, gets + (reloads ? 1 : 0), label + ": the list is refreshed only when the row's state may have changed");
      assert.equal(page.state.posts.length, 1, label);
      assert.ok(retryBtns(page).every(item => item.disabled), label + ": locked while the box is open");
      page.el("retry-cancel").click(); assert.equal(page.el("retry-dialog").hidden, true, label); assert.ok(retryBtns(page).every(item => !item.disabled), label);
    }
  }
  // the same codes when running (box open): shown in the box, nothing sent twice
  for (const [code, pattern, kind] of codes) {
    const page = await retryPage(); await openReady(page);
    page.state.postHandler = thrower(code, "reply"); page.el("retry-confirm").click(); await page.settle();
    assert.equal(page.el("retry-dialog").hidden, false, code); assert.equal(page.el("retry-error").className, kind, code); assert.match(page.text("retry-error"), pattern, code);
    assert.equal(page.state.posts.length, 2, code); assert.equal(page.el("review-notice").hidden, true, code);
    page.el("retry-cancel").click(); assert.ok(retryBtns(page).every(item => !item.disabled && item.textContent === "重试"), code);
  }
  // closed while running: the outcome lands in the notice above the list, and the buttons come back
  {
    const page = await retryPage(); await openReady(page);
    const exec = deferred(); page.state.postHandler = () => exec.promise; page.el("retry-confirm").click(); await page.settle();
    page.el("retry-cancel").click(); assert.equal(page.el("retry-dialog").hidden, true);
    assert.ok(retryBtns(page).every(item => item.disabled), "still locked while the request is in flight"); assert.equal(retryBtns(page)[0].textContent, "重试中…");
    page.el("retry-dialog").fire("keydown", { key: "Escape" }); assert.equal(page.state.posts.length, 2);
    exec.resolve({ status: "error", message: "有通话进行中（busy）", data: { code: "busy" } }); await page.settle();
    assert.equal(page.el("retry-dialog").hidden, true); assert.equal(page.el("review-notice").className, "notice warn"); assert.match(page.text("review-notice"), /暂时不能重试回顾/);
    assert.ok(retryBtns(page).every(item => !item.disabled && item.textContent === "重试"));
    const ok = await retryPage(); await openReady(ok); const run2 = deferred(); ok.state.postHandler = () => run2.promise; ok.el("retry-confirm").click(); await ok.settle();
    ok.el("retry-cancel").click(); const gets = ok.state.gets; run2.resolve({ status: "ok", data: { ref: "rv-fail", state: "succeeded", detail: "" } }); await ok.settle();
    assert.equal(ok.el("review-notice").className, "notice ok"); assert.equal(ok.state.gets, gets + 1); assert.equal(ok.el("retry-dialog").hidden, true);
  }
  // unknown failures: Chinese server text is shown, anything else is not leaked
  const odd = await retryPage(); odd.state.postHandler = () => { throw new Error("SYNTHETIC_SECRET_TOKEN exploded"); }; retryBtns(odd)[0].click(); await odd.settle();
  assert.equal(odd.el("retry-error").className, "notice error"); assert.match(odd.text("retry-error"), /回顾重试没有完成：没能连上插件或管理端。 请稍后再试。/); assert.ok(!odd.text("retry-error").includes("SYNTHETIC_SECRET"));
  odd.el("retry-cancel").click(); odd.state.postHandler = () => { throw new Error("服务暂时不可用：超时"); }; retryBtns(odd)[0].click(); await odd.settle();
  assert.match(odd.text("retry-error"), /回顾重试没有完成：服务暂时不可用：超时/);
  odd.el("retry-cancel").click();
  // malformed replies are not shown as a plan, and a malformed result refreshes the list
  for (const reply of [{ status: "ok" }, { status: "ok", data: null }, null, { status: "ok", data: {} }]) {
    const page = await retryPage(); page.state.postHandler = async () => reply; retryBtns(page)[0].click(); await page.settle();
    assert.equal(page.el("retry-body").hidden, true); assert.equal(page.el("retry-confirm").hidden, true); assert.match(page.text("retry-error"), /无法识别的结果/);
  }
  const weird = await retryPage(); await openReady(weird); const before = weird.state.gets;
  weird.state.postHandler = async () => ({ status: "ok", data: { ref: "rv-fail", state: "weird" } }); weird.el("retry-confirm").click(); await weird.settle();
  assert.match(weird.text("retry-error"), /无法识别的结果/); assert.equal(weird.state.gets, before + 1);
}
console.log("PASS evaluation page: populated/empty/placeholder, config diff by model id (thinking/streaming/null/old strings), status, reviews (0/20/cap), health, stage highlight, hints, 30-call cap, run + error codes, last_run, copy data (clipboard + fallback), read errors, theme, failed reviews (render/summary/health/old shape), retry (preview→confirm→run, send flag, rebill, cancel/Esc, focus, double submit, error codes)");

// The line view exports only display metadata and asks before posting a hang-up.
{
  const ref = "abcdef01".repeat(8);
  const call = {ref,state:"open",created_at:1000,entered_at:1010,elapsed_seconds:125,idle_seconds:null,
    remaining_seconds:3475,idle_remaining_seconds:null,turn_count:3,tool_processing:true,can_force_hangup:true,
    call_id:"private-call-id",body:"private-transcript",token:"private-token"};
  const empty = await bootWith(data => { data.active_calls = []; });
  assert.match(empty.text("active-call"),/当前线路空闲/);
  assert.match(empty.text("health-items"),/当前线路空闲/);

  const page = await bootWith(data => { data.active_calls = [call]; });
  const button = () => all(page.el("active-call"), node => node.tagName === "BUTTON")[0];
  assert.match(page.text("health-items"),/占线/);
  assert.match(page.text("active-call"),/通话中.*abcdef01.*2 分 5 秒.*工具处理中.*57 分 55 秒.*3 轮/);
  assert.doesNotMatch(page.text("active-call"),/private-|abcdef01abcdef01/);
  page.el("copy").click();
  await page.settle();
  const exported = split(page.state.copied[0]).json;
  assert.doesNotMatch(exported,/private-call-id|private-transcript|private-token/);
  assert.equal(JSON.parse(exported).active_calls[0].ref,ref);

  button().click();
  assert.equal(page.el("hangup-dialog").hidden,false);
  assert.equal(page.document.activeElement,page.el("hangup-cancel"));
  page.el("hangup-cancel").click();
  assert.equal(page.el("hangup-dialog").hidden,true);
  assert.equal(page.document.activeElement,button());
  button().click();
  page.el("hangup-dialog").fire("keydown",{key:"Escape"});
  assert.equal(page.el("hangup-dialog").hidden,true);
  assert.deepEqual(page.state.posts,[]);

  button().click();
  page.state.postHandler=()=>({status:"error",data:{code:"already_closing"}});
  page.el("hangup-submit").click();
  await page.settle();
  assert.match(page.text("hangup-error"),/已经在收尾/);
  assert.equal(page.el("hangup-dialog").hidden,false);

  let release;
  page.state.postHandler=()=>new Promise(resolve => { release = resolve; });
  page.el("hangup-submit").click();
  assert.equal(page.el("hangup-submit").disabled,true);
  assert.equal(page.el("hangup-cancel").disabled,true);
  page.el("hangup-submit").fire("click");
  assert.deepEqual(page.state.posts,Array.from({length:2},()=>["benchmark",{action:"force_hangup",ref}]));
  page.state.data.active_calls=[];
  release({status:"ok",data:{ref,state:"ended",review_expected:true}});
  await page.settle();
  assert.equal(page.el("hangup-dialog").hidden,true);
  assert.equal(page.state.gets,2);
  assert.match(page.text("hangup-notice"),/线路已释放/);
  assert.match(page.text("active-call"),/当前线路空闲/);
  assert.equal(page.document.activeElement,page.el("active-title"));

  const closing = await bootWith(data => { data.active_calls = [{...call,state:"closing",can_force_hangup:false}]; });
  assert.match(closing.text("active-call"),/正在收尾/);
  assert.equal(all(closing.el("active-call"), node => node.tagName === "BUTTON")[0].disabled,true);
}
