/* Evaluation and diagnostics page for the Bot-Call plugin. Auth stays in the native dashboard parent: this page only
   calls bridge.apiGet/apiPost with namespace-relative endpoints and never sees tokens, keys or raw HTTP.
   Everything is rendered as text (no HTML sinks) and nothing is loaded from outside the plugin. The page shows
   what the backend reports; hints come from the backend and are displayed verbatim. */
(() => {
  "use strict";
  const $ = id => document.getElementById(id);
  const bridge = window.AstrBotPluginPage;
  const MAX_CALLS = 30, MAX_REVIEWS = 20;
  // A stage is highlighted as clearly slower than the baseline only when it is both relatively and absolutely slower.
  const OVER_RATIO = 2, OVER_MS = 100; // Same ratio as backend hints.
  const ACCESS = {fixed_https: "固定 HTTPS", quick_tunnel: "临时隧道"};
  const CAPTURE = {auto: "新录音方式", legacy: "旧录音方式", mixed: "新旧方式混用"};
  const BARGE = {auto: "开启（自动）", off: "关闭"};
  const THINKING = {off: "关闭", on: "开启", unknown: "无法确认"};
  const SELECTION = {dedicated: "专门给电话设置的模型", source: "沿用来源会话的模型", inherited: "沿用来源会话的模型"};
  const MODEL_KEYS = ["llm", "stt", "tts"];
  const CONFIG_ROWS = [["llm", "对话模型"], ["stt", "语音识别"], ["tts", "语音合成"], ["access", "接入方式"], ["silence_ms", "判停时长"]];
  const EXTRA_ROWS = [["capture", "录音方式"], ["barge_in", "插话功能"]];
  const RUN_STATES = {ok: ["正常", "ok"], fail: ["失败", "bad"], skip: ["跳过", "skip"]};
  const CHECK_STATES = {ok: ["正常", "ok"], warn: ["注意", "warn"], fail: ["失败", "bad"], skip: ["跳过", "skip"]};
  const LEVELS = {warn: ["需要注意", "warn"], info: ["提示", "info"]};
  const RUN_ERRORS = {
    busy: ["warn", "现在有通话进行中，为了不影响通话，暂时不能测试。等通话结束后再试。"],
    running: ["warn", "上一次测试还在运行，请过几十秒再来看结果。"],
    rate_limited: ["warn", "测试太频繁了：两次测试之间至少要隔 30 秒，请稍后再试。"],
    disabled: ["error", "一键测试目前不可用（插件或这个功能没有启用）。"],
  };
  const RUN_CODES = Object.keys(RUN_ERRORS);
  const RETRY_ERRORS = {
    not_retryable: ["warn", "这通回顾现在不能重试（它的状态已经变了，或者已经处理完）。已为你刷新列表。"],
    not_found: ["warn", "找不到这通回顾（可能已经处理完或被清理）。已为你刷新列表。"],
    disabled: ["error", "回顾重试目前不可用（插件或这个功能没有启用）。"],
    busy: ["warn", "现在有通话进行中，为了不影响通话，暂时不能重试回顾。等通话结束后再试。"],
    running: ["warn", "这通回顾正在重试中（可能刚刚有人点过），请过一会儿刷新看结果，不要重复点。"],
  };
  const RETRY_CODES = Object.keys(RETRY_ERRORS), RETRY_RELOAD = ["not_retryable", "not_found", "running"];
  const RETRY_RESULTS = ["succeeded", "failed", "running"];
  const REVIEW_STATES = {failed: ["失败", "bad"], running: ["进行中", "info"], pending: ["待处理", "warn"], succeeded: ["已完成", "ok"]};
  const REASON_HISTORY = "scope_required", REASON_MODEL_FAILED = "model_failed", REASON_UNSURE = "outcome_uncertain";
  // Fixed Chinese preface for “复制数据”: it tells any AI what the pasted block is and what to analyse.
  const COPY_INTRO = [
    "【Bot-Call 评测与诊断数据】",
    "",
    "这是什么：Bot-Call（AstrBot 的网页语音通话插件）在管理页“评测与诊断”里导出的延迟评测与运行诊断数据，下面“数据”一节是原样的 JSON。插件设计上只含状态和耗时数字，不含通话内容、用户原文和密钥。",
    "",
    "字段说明：",
    "- stage_defs：延迟分段的定义（key、中文名、含义），顺序即一通电话的处理顺序。所有耗时的单位是毫秒（ms）；median_ms 是中位数，p90_ms 是 90 分位，max_ms 是最大值，n 是样本轮数。各段之间有重叠并含手机端计时，total（说完到首声）是整体测得的时长，不是各段之和。某段缺数据时该键被省略，不是 0。",
    "- baseline：随插件发布的“标准配置”实测基线（含配置、测量环境和各段耗时）；current_config：这台机器当前的配置；summary：最近若干通通话各段耗时的汇总。",
    "- 配置对照按模型编号（model）比较，不看显示名（label）：llm.thinking 表示思考状态（off 关闭 / on 开启 / unknown 无法确认），tts.streaming 表示是否使用首句流式合成；某项为 null 表示未配置；access 是接入方式，silence_ms 是判停时长。",
    "- calls：最近通话的逐通数据（stages 为各段耗时，flags 为接入/录音方式、插话候选、推送断开、模型下载耗时等，hints 为该通的提示）。",
    "- hints：插件按固定规则生成的整体提示。",
    "- status：运行状态（版本、插件和 HTTPS 是否就绪、接入方式、电话对话模型的选择方式与是否可用）。",
    "- reviews：通话回顾的未完成情况（数量、按原因汇总及最多 20 条列表，reason_labels 是原因的中文说明；列表项里 state 是状态，detail 是中文原因，model 是回顾所用的模型，retryable 表示管理员能否在页面上重试）。",
    "- last_run：最近一次手动“一键测试”的结果（tests 是识别、对话、合成各自耗时，selfcheck 是安装与环境自检）；为 null 表示还没有测过。",
    "",
    "请你帮我：",
    "1. 先用几句话概括整体情况：服务是否就绪，“说完到首声”的延迟中位数与标准基线相比如何；",
    "2. 找出相对基线最慢的环节（看各段 median_ms 与 p90_ms），结合配置对照（模型、思考状态、流式合成、接入方式、判停时长）和 hints，分析最可能的原因；",
    "3. 检查 status、reviews、last_run 里有没有异常（例如未就绪、回顾卡住、测试失败）；",
    "4. 按优先级给出具体、可操作的改进建议，并说明哪些结论是推测、还需要补充什么数据才能确认。",
    "请只依据下面的数据作答，不要编造数据里没有的数字。",
  ].join("\n");

  let data = null, lastRun = null, lastRunFresh = false, running = false, loadTask = null, runStarted = 0;
  let hangupTarget = null, hangingUp = false, activeButton = null;
  const expanded = new Set();
  // One retry flow at a time: {ref, phase: preview | ready | running | error, open, plan}. Buttons are disabled while it exists.
  let flow = null, wantFocus = null;
  const retryButtons = new Map();

  const make = (tag, className, text) => {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  };
  const num = value => typeof value === "number" && Number.isFinite(value) ? value : null;
  const pad2 = value => String(value).padStart(2, "0");
  const plain = value => value !== null && typeof value === "object" && !Array.isArray(value);
  const ms = value => {
    const n = num(value);
    if (n === null) return "—";
    return n < 1000 ? Math.round(n) + " ms" : (Math.round(n / 100) / 10).toFixed(1) + " 秒";
  };
  const seconds = value => { const n = num(value); return n === null ? "" : (Math.round(n / 100) / 10).toFixed(1) + " 秒"; };
  function clock(sec) {
    const n = num(sec), date = new Date((n === null ? NaN : n) * 1000);
    return Number.isNaN(date.getTime()) ? "" : pad2(date.getHours()) + ":" + pad2(date.getMinutes());
  }
  function stamp(sec) {
    const n = num(sec), date = new Date((n === null ? NaN : n) * 1000);
    return Number.isNaN(date.getTime()) ? "时间未知" : (date.getMonth() + 1) + "月" + date.getDate() + "日 " + clock(sec);
  }
  function duration(sec) {
    const n = num(sec);
    if (n === null || n <= 0) return "";
    return n < 60 ? "通话约 " + Math.round(n) + " 秒" : "通话约 " + Math.round(n / 60) + " 分钟";
  }
  const stat = (group, key) => {
    const item = group && group.stages && typeof group.stages === "object" ? group.stages[key] : null;
    return item && num(item.median_ms) !== null ? item : null;
  };
  const text = value => value === undefined || value === null ? "" : String(value);
  const count = value => Number.isSafeInteger(value) && value >= 0 ? value : null;

  // ---- notices ---------------------------------------------------------------------------------------------
  function notice(kind, message, actions = [], id = "notice") {
    const box = $(id);
    box.className = "notice " + kind;
    box.replaceChildren(make("span", "", message));
    for (const [label, handler] of actions) {
      const button = make("button", "link", label); button.type = "button"; button.addEventListener("click", handler); box.append(button);
    }
    box.hidden = false;
  }
  const clearNotice = (id = "notice") => { $(id).hidden = true; $(id).replaceChildren(); };

  // ---- configuration comparison ------------------------------------------------------------------------------
  function configText(key, value) {
    if (value === undefined || value === null || value === "") return null;
    if (key === "silence_ms" && num(value) !== null) return (value / 1000).toFixed(1).replace(/\.0$/, "") + " 秒（" + value + " ms）";
    if (key === "access") return ACCESS[value] || String(value);
    if (key === "capture") return CAPTURE[value] || String(value);
    if (key === "barge_in") return BARGE[value] || String(value);
    return String(value);
  }
  // llm/stt/tts are objects {label, model, thinking?, streaming?}; older backends send a plain display string.
  function modelInfo(value) {
    if (value === undefined || value === null || value === "") return null;
    if (!plain(value)) return {label: String(value), model: "", thinking: "", streaming: null};
    const label = text(value.label), model = text(value.model);
    if (!label && !model) return null;
    return {label: label || model, model, compare_model: text(value.compare_model), thinking: Object.hasOwn(THINKING, value.thinking) ? value.thinking : "", streaming: typeof value.streaming === "boolean" ? value.streaming : null};
  }
  function modelCell(kind, info) {
    const cell = [make("span", "cfg-main", info.label)];
    if (info.model && info.model !== info.label) cell.push(make("span", "cfg-sub", "模型：" + info.model));
    if (kind === "llm" && info.thinking) cell.push(make("span", "cfg-sub", "思考状态：" + THINKING[info.thinking]));
    if (kind === "tts" && info.streaming !== null) cell.push(make("span", "cfg-sub", "首句流式：" + (info.streaming ? "是" : "否")));
    return cell;
  }
  // Models are compared by model id (case-insensitive); the display label never decides. Without ids (old string shape) the label is used.
  function compareModel(kind, base, mine) {
    const marks = [], soft = [];
    const same = base.model && mine.model ? base.model.toLowerCase() === (mine.compare_model || mine.model).toLowerCase() : base.label === mine.label;
    if (!same) marks.push("与标准不同：模型不同");
    if (kind === "llm") {
      if (mine.thinking === "on" && base.thinking === "off") marks.push("与标准不同：开启了思考");
      else if (mine.thinking === "off" && base.thinking === "on") marks.push("与标准不同：关闭了思考");
      else if (mine.thinking === "unknown") soft.push("无法确认思考状态");
    }
    if (kind === "tts" && base.streaming !== null && mine.streaming !== null && base.streaming !== mine.streaming) {
      marks.push(mine.streaming ? "与标准不同：使用了首句流式" : "与标准不同：没有使用首句流式");
    }
    return {marks, soft};
  }
  function compareConfig() {
    const baseline = plain(data.baseline) ? data.baseline : null;
    const current = plain(data.current_config) ? data.current_config : null;
    const rows = []; let differing = 0, compared = 0, softCount = 0;
    for (const [key, label] of CONFIG_ROWS.concat(EXTRA_ROWS)) {
      const baseValue = baseline && plain(baseline.config) ? baseline.config[key] : null;
      const mineValue = current ? current[key] : null;
      const row = {key, label, base: [], mine: [], baseNone: "（标准未设定）", mineNone: "未读取到", marks: [], soft: [], diff: false};
      if (MODEL_KEYS.includes(key)) {
        const base = modelInfo(baseValue), mine = modelInfo(mineValue);
        if (!base && !mine) continue;
        if (base) row.base = modelCell(key, base);
        if (mine) row.mine = modelCell(key, mine);
        if (base && mine) { compared++; Object.assign(row, compareModel(key, base, mine)); }
        else if (base && current) { row.mineNone = "未配置"; row.marks.push("与标准不同：未配置"); compared++; }
      } else {
        const base = configText(key, baseValue), mine = configText(key, mineValue);
        if (base === null && mine === null) continue;
        if (base !== null) row.base = [make("span", "cfg-main", base)];
        if (mine !== null) row.mine = [make("span", "cfg-main", mine)];
        if (base !== null && mine !== null) { compared++; if (base !== mine) row.marks.push("与标准不同"); }
      }
      row.diff = row.marks.length > 0;
      if (row.diff) differing++;
      softCount += row.soft.length;
      rows.push(row);
    }
    return {rows, differing, compared, soft: softCount, hasCurrent: !!current};
  }
  function cell(who, nodes, none, marks, soft) {
    const box = make("div", "cfg-cell" + (nodes.length ? "" : " none"), nodes.length ? undefined : none);
    box.setAttribute("data-who", who);
    if (nodes.length) box.append(...nodes);
    if (marks.length || soft.length) {
      const tags = make("div", "cfg-tags");
      for (const mark of marks) tags.append(make("span", "tag warn", mark));
      for (const note of soft) tags.append(make("span", "tag info", note));
      box.append(tags);
    }
    return box;
  }
  function renderOverview(cfg) {
    const baseline = plain(data.baseline) ? data.baseline : null;
    $("placeholder").hidden = !(baseline && baseline.placeholder === true);
    const meta = [];
    if (baseline) {
      if (baseline.label) meta.push(text(baseline.label));
      if (baseline.measured_at) meta.push("测量日期 " + text(baseline.measured_at));
      if (baseline.environment) meta.push("测量环境：" + text(baseline.environment));
    }
    $("baseline-meta").textContent = meta.length ? meta.join(" · ") : "暂无基线信息";
    const total = stat(baseline, "total");
    const line = $("baseline-total");
    line.replaceChildren();
    if (total) line.append("说完到首声（中位数）：", make("b", "", ms(total.median_ms)), total.p90_ms == null ? "" : "　p90 " + ms(total.p90_ms));
    line.hidden = !total;

    const rows = cfg.rows.map(item => {
      const row = make("div", "cfg-row" + (item.diff ? " diff" : ""));
      row.setAttribute("role", "row");
      row.append(make("div", "cfg-label", item.label), cell("标准配置", item.base, item.baseNone, [], []), cell("你的当前配置", item.mine, item.mineNone, item.marks, item.soft));
      return row;
    });
    const head = make("div", "cfg-row head");
    head.append(make("div", "", "项目"), make("div", "", "标准配置"), make("div", "", "你的当前配置"));
    $("config").replaceChildren(head, ...rows);
    const softNote = cfg.soft ? "思考状态无法确认，没有计入“不同”。" : "";
    $("diff-summary").textContent = !cfg.hasCurrent ? "没有读到你的当前配置。"
      : cfg.differing ? "有 " + cfg.differing + " 项和标准配置不同（已标出）。延迟和标准有差距时，先看这几项。" + softNote
      : cfg.compared ? "你的配置和标准配置一致。" + softNote : softNote;
  }

  // ---- stage comparison -------------------------------------------------------------------------------------
  const pct = (value, scale) => Math.max(1, Math.min(100, value / scale * 100)).toFixed(1) + "%";
  const isOver = (mine, base) => !!mine && !!base && mine.median_ms > base.median_ms * OVER_RATIO && mine.median_ms - base.median_ms >= OVER_MS;
  function barRow(who, item, scale, kind) {
    const row = make("div", "bar-row");
    const track = make("div", "track"), value = make("span", "val");
    if (item && num(item.median_ms) !== null) {
      const p90 = num(item.p90_ms);
      if (p90 !== null && p90 > item.median_ms) { const tail = make("i", "p90 " + kind); tail.style.width = pct(p90, scale); track.append(tail); }
      const fill = make("i", "med " + kind); fill.style.width = pct(item.median_ms, scale); track.append(fill);
      const label = "中位 " + ms(item.median_ms) + (p90 === null ? "" : " · p90 " + ms(p90));
      value.textContent = label;
      track.setAttribute("role", "img"); track.setAttribute("aria-label", who + " " + label);
    } else {
      track.className = "track empty"; value.className = "val none"; value.textContent = "暂无数据";
    }
    row.append(make("span", "who", who), track, value);
    return row;
  }
  function stageBlock(def, base, mine, scale) {
    const over = isOver(mine, base);
    const block = make("div", "stage" + (def.key === "total" ? " total" : "") + (over ? " over" : ""));
    block.setAttribute("data-stage", def.key);
    const head = make("div", "stage-head");
    head.append(make("span", "stage-label", text(def.label || def.key)));
    if (over) head.append(make("span", "tag warn", "明显偏慢：约为标准的 " + (mine.median_ms / base.median_ms).toFixed(1) + " 倍"));
    block.append(head);
    const desc = text(def.desc) + (def.key === "total" ? "（整体测得，不是上面各段相加）" : "");
    if (desc) block.append(make("p", "stage-desc", desc));
    block.append(barRow("标准", base, scale, "base"), barRow("你的", mine, scale, over ? "over" : "mine"));
    return block;
  }
  const stageDefs = () => Array.isArray(data.stage_defs) ? data.stage_defs.filter(def => def && typeof def.key === "string") : [];
  function renderStages() {
    const defs = stageDefs();
    const baseline = data.baseline, summary = plain(data.summary) ? data.summary : null;
    const calls = summary && Number.isSafeInteger(summary.calls) ? summary.calls : 0;
    const peak = list => list.reduce((best, item) => item ? Math.max(best, num(item.p90_ms) || 0, num(item.median_ms) || 0) : best, 0) || 1;
    const parts = defs.filter(def => def.key !== "total");
    const scale = peak(parts.flatMap(def => [stat(baseline, def.key), stat(summary, def.key)]));
    const blocks = []; let overCount = 0;
    for (const def of defs) {
      const base = stat(baseline, def.key), mine = stat(summary, def.key);
      if (def.key !== "total" && isOver(mine, base)) overCount++;
      const own = def.key === "total" ? peak([base, mine]) : scale;
      blocks.push(stageBlock(def, base, mine, own));
    }
    $("stages").replaceChildren(...blocks);
    $("stages-summary").textContent = !calls ? "还没有通话数据，“你的”一栏暂时是空的。"
      : "“你的”一栏是最近 " + calls + " 通通话的汇总。" + (overCount ? "有 " + overCount + " 段明显比标准慢（已标出）。" : "没有哪一段明显比标准慢。");
  }

  // ---- hints ------------------------------------------------------------------------------------------------
  function hintItem(hint, defs) {
    const level = LEVELS[hint.level] || LEVELS.info;
    const item = make("li", "hint " + (hint.level === "warn" ? "warn" : "info"));
    item.append(make("span", "tag " + level[1], level[0]));
    const def = defs.find(entry => entry && entry.key === hint.stage);
    if (def && def.key !== "total") item.append(make("span", "tag", text(def.label)));
    item.append(make("span", "hint-text", text(hint.text)));
    return item;
  }
  const validHints = list => (Array.isArray(list) ? list : []).filter(hint => hint && typeof hint.text === "string" && hint.text);
  function renderHints() {
    const defs = Array.isArray(data.stage_defs) ? data.stage_defs : [];
    const hints = validHints(data.hints);
    $("hints").replaceChildren(...hints.map(hint => hintItem(hint, defs)));
    const calls = plain(data.summary) && Number.isSafeInteger(data.summary.calls) ? data.summary.calls : 0;
    $("hints-empty").hidden = hints.length > 0;
    $("hints-empty").textContent = calls ? "目前没有需要特别注意的地方。" : "有了通话数据后，这里会告诉你哪里慢、可以调整什么。";
  }

  // ---- recent calls -----------------------------------------------------------------------------------------
  function flagChips(flags) {
    const out = [];
    if (!plain(flags)) return out;
    if (flags.access) out.push("接入：" + (ACCESS[flags.access] || text(flags.access)));
    if (flags.capture) out.push("录音：" + (CAPTURE[flags.capture] || text(flags.capture)));
    if (num(flags.barge_candidates) !== null) out.push("插话候选 " + flags.barge_candidates + " 次");
    if (num(flags.ws_disconnects) !== null) out.push("推送断开 " + flags.ws_disconnects + " 次");
    if (num(flags.model_fetch_ms) !== null) out.push("模型下载 " + ms(flags.model_fetch_ms));
    return out;
  }
  function callDetail(call, defs) {
    const box = make("div", "call-detail");
    const lengthText = duration(call.duration_s);
    if (lengthText) box.append(make("p", "sub", lengthText + (Number.isSafeInteger(call.turns) ? "，共 " + call.turns + " 轮" : "")));
    const head = make("div", "dstage head");
    head.append(make("span", "", "阶段"), make("span", "", "中位"), make("span", "", "最大"), make("span", "", "轮数"));
    const rows = [head];
    for (const def of defs) {
      const item = call.stages && call.stages[def.key];
      if (!item || num(item.median_ms) === null) continue;
      const row = make("div", "dstage");
      row.append(make("span", "", text(def.label)), make("span", "", ms(item.median_ms)), make("span", "", ms(item.max_ms)), make("span", "", num(item.n) === null ? "—" : String(item.n)));
      rows.push(row);
    }
    if (rows.length === 1) box.append(make("p", "sub", "这通电话没有分段数据。"));
    else box.append(...rows);
    return box;
  }
  function callCard(call, index, defs) {
    const key = text(call.ref || "#" + index);
    const hints = validHints(call.hints);
    const open = expanded.has(key);
    const card = make("article", "call" + (hints.some(hint => hint.level === "warn") ? " warn" : ""));
    const head = make("button", "call-head"); head.type = "button";
    head.setAttribute("aria-expanded", open ? "true" : "false");
    const when = make("span", "call-time", stamp(call.started_at));
    const turns = Number.isSafeInteger(call.turns) ? call.turns + " 轮" : "";
    when.append(make("span", "call-meta", [turns, duration(call.duration_s)].filter(Boolean).join(" · ")));
    const total = call.stages && call.stages.total && num(call.stages.total.median_ms) !== null ? call.stages.total : null;
    const totalNode = make("span", "call-total");
    if (total) totalNode.append("说完到首声 ", make("b", "", ms(total.median_ms)));
    else totalNode.textContent = "无首声数据";
    const caret = make("span", "caret", open ? "▾" : "▸");
    head.append(when, totalNode, caret);
    card.append(head);
    const stageChips = defs.filter(def => def.key !== "total" && call.stages && call.stages[def.key] && num(call.stages[def.key].median_ms) !== null);
    const chips = make("div", "chips");
    for (const def of stageChips) {
      const mine = call.stages[def.key], base = stat(data.baseline, def.key);
      chips.append(make("span", "chip" + (isOver(mine, base) ? " over" : ""), def.label + " " + ms(mine.median_ms)));
    }
    for (const label of flagChips(call.flags)) chips.append(make("span", "chip flag", label));
    if (chips.children.length) card.append(chips);
    if (hints.length) { const list = make("ul", "hint-list"); list.append(...hints.map(hint => hintItem(hint, defs))); card.append(list); }
    const detail = callDetail(call, defs); detail.hidden = !open;
    card.append(detail);
    head.addEventListener("click", () => {
      const now = detail.hidden;
      detail.hidden = !now; head.setAttribute("aria-expanded", now ? "true" : "false"); caret.textContent = now ? "▾" : "▸";
      if (now) expanded.add(key); else expanded.delete(key);
    });
    return card;
  }
  function renderCalls() {
    const defs = stageDefs();
    const calls = (Array.isArray(data.calls) ? data.calls.filter(call => call && typeof call === "object") : [])
      .map((call, index) => ({call, index}))
      .sort((a, b) => (num(b.call.started_at) || 0) - (num(a.call.started_at) || 0) || a.index - b.index)
      .slice(0, MAX_CALLS);
    $("calls-empty").hidden = calls.length > 0;
    $("calls-count").textContent = calls.length ? "最近 " + calls.length + " 通，最新的在前（最多显示 " + MAX_CALLS + " 通）" : "";
    $("calls").replaceChildren(...calls.map(({call, index}) => callCard(call, index, defs)));
  }

  // ---- runtime status ----------------------------------------------------------------------------------------
  function safeLink(value) {
    try {
      const url = new URL(String(value));
      return url.protocol === "https:" && !url.username && !url.password && !url.search && !url.hash ? url.href : "";
    } catch (_) { return ""; }
  }
  const flag = (value, yes, no) => value === true ? make("span", "tag ok", yes) : value === false ? make("span", "tag bad", no) : make("span", "tag", "未知");
  function renderStatus() {
    const status = plain(data.status) ? data.status : null;
    $("status-empty").hidden = !!status; $("status").hidden = !status;
    if (!status) return;
    $("st-version").textContent = text(status.version) || "未知";
    $("st-plugin").replaceChildren(flag(status.plugin_ready, "就绪", text(status.error) || "未就绪"));
    $("st-https").replaceChildren(flag(status.https_ready, "已就绪", "未就绪"),
      status.https_ready === true ? make("span", "sub", "入口已配置且宿主就绪（未验证外网可达）") : make("span", "sub", "手机通话需要 HTTPS 入口，配置方法见 README 的“固定 HTTPS”一节"));
    const page = text(status.page_url), link = safeLink(page);
    if (link) {
      const anchor = make("a", "", link);
      anchor.setAttribute("href", link); anchor.setAttribute("target", "_blank"); anchor.setAttribute("rel", "noopener noreferrer");
      $("st-page").replaceChildren(anchor, make("span", "sub", "不含邀请；打开后可以试听网页播放，不验证当前语音合成配置。"));
    } else $("st-page").replaceChildren(make("span", page ? "" : "sub", page || "未配置"));
    $("st-access").textContent = ACCESS[status.access] || text(status.access) || "未知";
    const model = plain(status.call_model) ? status.call_model : null;
    if (!model) $("st-model").replaceChildren(make("span", "tag", "未知"));
    else $("st-model").replaceChildren(
      flag(model.available, "可用", "不可用"),
      make("span", "", text(model.model) || "（尚未确定）"),
      make("span", "sub", (SELECTION[model.selection] || "选择方式未知") + (model.available === false ? "；不可用或还没有可信的来源会话" : "")));
  }

  // ---- call reviews ------------------------------------------------------------------------------------------
  const reasonName = (reviews, key) => plain(reviews.reason_labels) && Object.hasOwn(reviews.reason_labels, key) && reviews.reason_labels[key] ? text(reviews.reason_labels[key]) : text(key);
  // Rows from a backend without detail/retryable/model keep the old look (raw state, reason label) and never get a button.
  const isEnriched = item => Object.hasOwn(item, "detail") || Object.hasOwn(item, "retryable") || Object.hasOwn(item, "model");
  const isFailed = item => item.state === "failed" || item.reason === REASON_MODEL_FAILED;
  const canRetry = item => item.retryable === true && typeof item.ref === "string" && item.ref !== "";
  const reviewItems = reviews => (Array.isArray(reviews.calls) ? reviews.calls.filter(item => plain(item)) : []).slice(0, MAX_REVIEWS);
  // Every row that can be retried and is not succeeded needs attention: failed ones (retryable) and the others (unsure outcome).
  const isUnsure = item => !isFailed(item) && canRetry(item) && item.state !== "succeeded";
  function reviewCounts(reviews) {
    const items = reviewItems(reviews); let retryable = 0, stuck = 0, unsure = 0, history = 0;
    for (const item of items) {
      if (isFailed(item)) { if (canRetry(item)) retryable++; else stuck++; }
      else if (isUnsure(item)) unsure++;
      else if (item.reason === REASON_HISTORY) history++;
    }
    return {items, retryable, stuck, unsure, history, failed: retryable + stuck, actionable: retryable + unsure};
  }
  function reviewSummary(pending, counts, truncated) {
    const lead = pending ? "有 " + pending + " 通通话的回顾还没有完成。" : counts.failed ? "有 " + counts.failed + " 通通话的回顾失败了。"
      : counts.unsure ? "有 " + counts.unsure + " 通通话的回顾结果不确定。" : "";
    const parts = [];
    if (counts.retryable) parts.push("失败 " + counts.retryable + " 通（可重试）");
    if (counts.unsure) parts.push("结果不确定 " + counts.unsure + " 通（可重试）");
    if (counts.stuck) parts.push("失败 " + counts.stuck + " 通（暂不能重试）");
    if (counts.history) parts.push("历史保留 " + counts.history + " 通");
    const text = lead + (parts.length ? parts.join("；") + "。" : "");
    return text && truncated ? text + "（仅统计最近 " + MAX_REVIEWS + " 条）" : text;
  }
  // Backend detail first, then the reason label; an unsure outcome without either still gets a plain sentence.
  function reviewLabel(reviews, item) {
    if (!item.reason) return "未记录原因";
    const named = reasonName(reviews, item.reason);
    return named === text(item.reason) && item.reason === REASON_UNSURE ? "上次请求结果不确定" : named;
  }
  function reviewRow(reviews, item) {
    const enriched = isEnriched(item), failed = isFailed(item);
    const row = make("li", "rv" + (failed ? " failed" : ""));
    const known = enriched ? REVIEW_STATES[item.state] : null;
    const tag = known ? make("span", "tag " + known[1], known[0]) : make("span", "tag", text(item.state) || "未知");
    const label = reviewLabel(reviews, item);
    let reason;
    if (enriched) {
      reason = make("span", "rv-reason");
      reason.append(make("span", "rv-main", text(item.detail) || label));
      if (text(item.model)) reason.append(make("span", "rv-sub", "回顾所用模型：" + text(item.model)));
      if (item.work_state) reason.append(make("span", "rv-sub", "处理状态：" + text(item.work_state)));
    } else {
      reason = make("span", "rv-reason", label);
      if (item.work_state) reason.append(make("span", "rv-work", "处理状态：" + text(item.work_state)));
    }
    row.append(make("span", "rv-time", stamp(item.at)), tag, reason);
    if (canRetry(item)) {
      const button = make("button", "rv-retry", "重试"); button.type = "button";
      button.setAttribute("aria-label", "重试 " + stamp(item.at) + " 的通话回顾");
      button.disabled = !!flow;
      button.addEventListener("click", () => openRetry(item, button));
      retryButtons.set(item.ref, button); row.append(button);
    }
    return row;
  }
  function renderReviews() {
    const reviews = plain(data.reviews) && count(data.reviews.pending_count) !== null ? data.reviews : null;
    const tag = $("reviews-tag");
    retryButtons.clear();
    $("review-reasons").replaceChildren(); $("reviews").replaceChildren(); $("reviews-more").hidden = true; $("reviews-empty").hidden = true;
    if (!reviews) { tag.hidden = true; $("reviews-summary").textContent = "当前插件没有提供通话回顾状态，可能是版本较旧，请升级插件后刷新。"; return; }
    const pending = reviews.pending_count, counts = reviewCounts(reviews), items = counts.items;
    tag.hidden = false; tag.className = "tag " + (pending || counts.failed || counts.unsure ? "warn" : "ok");
    tag.textContent = pending ? "未完成 " + pending + " 通" : counts.failed ? "失败 " + counts.failed + " 通" : counts.unsure ? "结果不确定 " + counts.unsure + " 通" : "全部完成";
    $("reviews-summary").textContent = reviewSummary(pending, counts, reviews.truncated === true);
    $("reviews-empty").hidden = pending > 0 || items.length > 0;
    const reasons = plain(reviews.reasons) ? Object.entries(reviews.reasons).filter(([, n]) => count(n)).sort((a, b) => b[1] - a[1]) : [];
    $("review-reasons").replaceChildren(...reasons.map(([key, n]) => make("span", "chip flag", reasonName(reviews, key) + " " + n + " 通")));
    $("reviews").replaceChildren(...items.map(item => reviewRow(reviews, item)));
    if (reviews.truncated === true || pending > items.length && items.length) {
      $("reviews-more").hidden = false; $("reviews-more").textContent = "只列出最近 " + items.length + " 条，还有更多没有列出。";
    }
  }

  // ---- overall health ------------------------------------------------------------------------------------------
  const CALL_STATES = {open: ["通话中", "warn"], invited: ["等待接通", "info"], closing: ["正在收尾", "info"]};
  const HANGUP_ERRORS = {
    disabled: "插件尚未就绪，暂时无法挂断。",
    not_found: "找不到这通电话，可能已经结束。请刷新查看当前通话。",
    already_closing: "这通电话已经在收尾。请刷新查看当前通话。",
    running: "已有管理员正在挂断这通电话，请稍后刷新。",
  };
  function activeCalls() {
    if (!Array.isArray(data.active_calls)) return [];
    return data.active_calls.filter(item => plain(item) && typeof item.ref === "string" &&
      /^[0-9a-f]{64}$/.test(item.ref) && Object.hasOwn(CALL_STATES, item.state)).slice(0, 1).map(item => ({
        ref: item.ref, state: item.state, created_at: num(item.created_at), entered_at: num(item.entered_at),
        elapsed_seconds: num(item.elapsed_seconds), idle_seconds: num(item.idle_seconds),
        remaining_seconds: num(item.remaining_seconds), idle_remaining_seconds: num(item.idle_remaining_seconds),
        turn_count: count(item.turn_count), tool_processing: item.tool_processing === true,
        can_force_hangup: item.can_force_hangup === true && item.state !== "closing",
      }));
  }

  function renderActiveCall() {
    const box = $("active-call"), [call] = activeCalls();
    activeButton = null;
    if (!call) {
      box.replaceChildren(make("p", "sub", Array.isArray(data.active_calls)
        ? "当前线路空闲，没有正在进行的通话。" : "当前版本未提供线路状态。"));
      return;
    }
    const head = make("div", "active-call-head"), facts = make("dl", "facts");
    const time = value => value === null ? "—" : Math.floor(value / 60) + " 分 " + Math.floor(value % 60) + " 秒";
    activeButton = make("button", "", "强制挂断");
    activeButton.type = "button"; activeButton.disabled = !call.can_force_hangup;
    activeButton.addEventListener("click", () => openHangup(call.ref));
    head.append(tagFor(CALL_STATES, call.state), make("span", "sub", "编号 " + call.ref.slice(0, 8)), activeButton);
    const rows = [
      ["已持续", time(call.elapsed_seconds)],
      ["无发言时长", call.tool_processing ? "工具处理中" : time(call.idle_seconds)],
      ["剩余通话时长", time(call.remaining_seconds)],
      ["距空闲挂断", time(call.idle_remaining_seconds)],
      ["已说轮数", call.turn_count === null ? "—" : call.turn_count + " 轮"],
    ];
    for (const [name, value] of rows) facts.append(make("dt", "", name), make("dd", "", value));
    box.replaceChildren(head, facts);
  }

  function openHangup(ref) {
    if (hangupTarget || hangingUp) return;
    hangupTarget = ref;
    clearNotice("hangup-error");
    clearNotice("hangup-notice");
    $("hangup-target").textContent = "通话编号 " + ref.slice(0, 8);
    $("hangup-dialog").hidden = false;
    $("hangup-cancel").focus();
  }

  function closeHangup(restoreFocus = true) {
    if (hangingUp) return;
    hangupTarget = null;
    $("hangup-dialog").hidden = true;
    if (restoreFocus) (activeButton && !activeButton.disabled ? activeButton : $("active-title")).focus();
  }

  function hangupKeys(event) {
    if (event.key === "Escape") { closeHangup(); return; }
    if (event.key !== "Tab") return;
    const first = $("hangup-submit"), last = $("hangup-cancel"), current = document.activeElement;
    if (hangingUp || (event.shiftKey ? current !== last : current !== first)) {
      event.preventDefault();
      (hangingUp ? $("hangup-dialog") : event.shiftKey ? last : first).focus();
    }
  }

  async function submitHangup() {
    if (!hangupTarget || hangingUp) return;
    const ref = hangupTarget;
    hangingUp = true;
    clearNotice("hangup-error");
    $("hangup-submit").disabled = $("hangup-cancel").disabled = true;
    $("hangup-progress").hidden = false; $("hangup-dialog").focus();
    try {
      const reply = await bridge.apiPost("benchmark", {action: "force_hangup", ref});
      if (reply?.status === "error") throw reply;
      const result = reply?.status === "ok" ? reply.data : reply;
      if (result?.ref !== ref || result.state !== "ended") throw new Error("bad_result");
    } catch (error) {
      const code = errorCode(error, Object.keys(HANGUP_ERRORS));
      notice("error", HANGUP_ERRORS[code] || "挂断结果尚未确认，请关闭此窗口并刷新通话状态。", [], "hangup-error");
      return;
    } finally {
      hangingUp = false;
      $("hangup-submit").disabled = $("hangup-cancel").disabled = false;
      $("hangup-progress").hidden = true; $("hangup-cancel").focus();
    }
    closeHangup(false);
    $("active-title").focus();
    notice("ok", "通话已挂断，线路已释放。", [], "hangup-notice");
    if (loadTask) await loadTask;
    await load();
  }

  function renderHealth(cfg) {
    const problems = [], notes = [];
    const status = plain(data.status) ? data.status : null, reviews = plain(data.reviews) ? data.reviews : null;
    if (status) {
      if (status.plugin_ready !== true) problems.push("插件还没有就绪。");
      if (plain(status.call_model) && status.call_model.available === false) problems.push("电话对话模型暂不可用（还没有可信的来源会话，或模型选择有问题）。");
      if (status.https_ready !== true) notes.push("HTTPS 入口还没有就绪（手机通话需要它）。");
    }
    const pending = reviews ? count(reviews.pending_count) : null;
    const retryable = reviews ? reviewCounts(reviews).actionable : 0;
    if (retryable) notes.push("需要注意：有 " + retryable + " 通回顾未完成，可在下方重试。");
    if (pending) notes.push("有 " + pending + " 通通话回顾没有完成。");
    const warns = validHints(data.hints).filter(hint => hint.level === "warn").length;
    if (warns) notes.push("有 " + warns + " 条延迟提示需要注意。");
    if (cfg.differing) notes.push("有 " + cfg.differing + " 项配置和标准配置不同。");
    if (lastRun && lastRun.ok === false) notes.push("上一次一键测试有项目没通过。");
    const level = problems.length ? "bad" : notes.length ? "warn" : status ? "ok" : "info";
    const tag = $("health-tag");
    tag.className = "tag " + level; tag.textContent = {bad: "有问题", warn: "需要注意", ok: "一切就绪", info: "状态未知"}[level];
    $("health").className = "card health " + level;
    $("health-text").textContent = problems.length ? "有需要先处理的问题。" : notes.length ? "服务可用，但有几处需要留意。"
      : status ? "插件就绪，没有发现需要处理的地方。" : "没有读到运行状态，可能是插件版本较旧，请升级插件后刷新。";
    const [call] = activeCalls();
    const line = call?.state === "open" ? "当前线路占线。" : call?.state === "invited"
      ? "有通话正在等待接通。" : call?.state === "closing" ? "上一通电话正在收尾，线路可用。" : "当前线路空闲。";
    const items = problems.concat(notes);
    if (Array.isArray(data.active_calls)) items.push(line);
    $("health-items").replaceChildren(...items.map(line => make("li", "", line)));
  }

  // ---- one-click test ----------------------------------------------------------------------------------------
  function tagFor(table, state) { const hit = table[state]; return hit ? make("span", "tag " + hit[1], hit[0]) : make("span", "tag", "未知"); }
  function resultList(items, table, withMs) {
    const list = make("ul", "rows");
    for (const item of Array.isArray(items) ? items : []) {
      if (!item || typeof item !== "object") continue;
      const bad = item.state === "fail" ? " bad" : item.state === "warn" ? " warn" : "";
      const row = make("li", "row" + bad);
      row.append(tagFor(table, item.state), make("span", "row-label", text(item.label || item.key)),
        make("span", "row-ms", withMs ? (num(item.ms) === null ? "—" : ms(item.ms)) : ""), make("span", "row-detail", text(item.detail)));
      list.append(row);
    }
    return list;
  }
  function renderRun() {
    const button = $("run");
    button.disabled = running || !bridge;
    button.textContent = running ? "测试中…" : lastRun ? "重新测试" : "开始一键测试";
    $("run-progress").hidden = !running;
    $("run-status").hidden = !running;
    if (running) elapsed();
    const box = $("run-result");
    if (!lastRun) { box.hidden = true; box.replaceChildren(); return; }
    const head = make("div", "run-head");
    head.append(make("b", "", lastRunFresh ? "本次测试" : "上次测试"));
    const when = [stamp(lastRun.at)];
    if (num(lastRun.duration_ms) !== null) when.push("用时 " + seconds(lastRun.duration_ms));
    head.append(make("span", "sub", when.join(" · ")), make("span", "tag " + (lastRun.ok === true ? "ok" : "bad"), lastRun.ok === true ? "全部通过" : "有项目没通过"));
    const parts = [head, make("h3", "", "服务实测"), resultList(lastRun.tests, RUN_STATES, true)];
    if (Array.isArray(lastRun.selfcheck) && lastRun.selfcheck.length) parts.push(make("h3", "", "安装与环境自检"), resultList(lastRun.selfcheck, CHECK_STATES, false));
    if (!lastRunFresh) parts.push(make("p", "foot", "这是上一次的结果，插件重载后会清空。"));
    box.replaceChildren(...parts); box.hidden = false;
  }
  const elapsed = () => { $("run-status").textContent = "正在测试，已用 " + Math.floor((Date.now() - runStarted) / 1000) + " 秒（通常需要 5～30 秒，请不要关闭页面）。"; };
  function tick() {
    if (!running) return;
    elapsed();
    setTimeout(tick, 1000);
  }
  function errorCode(error, codes = RUN_CODES) {
    const direct = error && (error.code || error.data && error.data.code || error.response && error.response.data && error.response.data.code);
    if (codes.includes(direct)) return direct;
    const message = String(error && error.message || "");
    for (const code of codes) if (new RegExp("(^|[^a-z])" + code + "([^a-z]|$)").test(message)) return code;
    return "";
  }
  const hasCjk = value => /[㐀-鿿]/.test(value);
  function runFailed(error) {
    const code = errorCode(error);
    if (code) {
      const [kind, message] = RUN_ERRORS[code];
      notice(kind, message, code === "running" ? [["刷新看结果", () => load()]] : [], "run-notice");
      return;
    }
    const message = String(error && error.message || "");
    notice("error", "测试没有完成" + (hasCjk(message) ? "：" + message : "：没能连上插件或管理端。") + " 请稍后再试。", [], "run-notice");
  }
  const pickRun = (a, b) => !a ? b : !b ? a : (num(b.at) || 0) > (num(a.at) || 0) ? b : a;
  async function run() {
    if (running || !bridge) return;
    running = true; runStarted = Date.now(); clearNotice("run-notice"); renderRun(); setTimeout(tick, 1000);
    try {
      const reply = await bridge.apiPost("benchmark", {action: "run"});
      if (reply && reply.status === "error") { runFailed({data: reply.data, message: reply.message}); return; }
      const result = reply && reply.status === "ok" && reply.data ? reply.data : reply;
      if (!result || !Array.isArray(result.tests)) throw new Error("bad_result");
      lastRun = result; lastRunFresh = true;
      notice(result.ok === true ? "ok" : "warn", result.ok === true ? "测试完成，全部通过。" : "测试完成，有项目没通过，请看下面的说明。", [], "run-notice");
    } catch (error) { runFailed(error); }
    finally { running = false; renderRun(); if (data) renderHealth(compareConfig()); }
  }

  // ---- retry a failed review: preview, confirm, run -------------------------------------------------------------
  function syncRetryButtons() {
    for (const [ref, button] of retryButtons) {
      button.disabled = !!flow;
      button.textContent = flow && flow.phase === "running" && flow.ref === ref ? "重试中…" : "重试";
    }
  }
  function consumeFocus() {
    if (!wantFocus) return;
    const target = retryButtons.get(wantFocus); wantFocus = null;
    if (target && !target.disabled) target.focus(); else $("reviews-title").focus();
  }
  const retryOrder = () => ["retry-send", "retry-confirm", "retry-cancel"].map($).filter(el => !el.hidden && !el.disabled && (el.id !== "retry-send" || !$("retry-body").hidden));
  function renderRetry() {
    const phase = flow ? flow.phase : "";
    $("retry-loading").hidden = phase !== "preview";
    $("retry-body").hidden = phase !== "ready" && phase !== "running";
    $("retry-error").hidden = phase !== "error";
    $("retry-progress").hidden = phase !== "running"; $("retry-status").hidden = phase !== "running";
    $("retry-confirm").hidden = phase === "error"; $("retry-confirm").disabled = phase !== "ready";
    $("retry-confirm").textContent = phase === "running" ? "重试中…" : "确认重试";
    $("retry-send").disabled = phase !== "ready";
    $("retry-cancel").textContent = phase === "running" ? "先关闭（仍在后台重试）" : phase === "error" ? "关闭" : "取消";
  }
  function fillPlan(plan) {
    $("retry-model").textContent = text(plan.model) || "（插件没有提供）";
    const low = count(plan.llm_calls_min), high = count(plan.llm_calls_max);
    $("retry-calls").textContent = low !== null && high !== null ? (low === high ? low + " 次" : low + "～" + high + " 次")
      : low !== null ? "至少 " + low + " 次" : high !== null ? "最多 " + high + " 次" : "（插件没有提供）";
    $("retry-rebill").hidden = plan.may_rebill !== true;
    $("retry-notes").replaceChildren(...(Array.isArray(plan.notes) ? plan.notes : []).filter(note => typeof note === "string" && note).map(note => make("li", "", note)));
  }
  function retryProblem(error) {
    const code = errorCode(error, RETRY_CODES);
    if (code) { const [kind, message] = RETRY_ERRORS[code]; return {code, kind, message}; }
    const message = String(error && error.message || "");
    if (message === "bad_result") return {code: "bad_result", kind: "warn", message: "插件返回了无法识别的结果，请刷新页面确认这通回顾的状态后再试。"};
    return {code: "", kind: "error", message: "回顾重试没有完成" + (hasCjk(message) ? "：" + message : "：没能连上插件或管理端。") + " 请稍后再试。"};
  }
  function openRetry(item, opener) {
    if (flow || !bridge || !canRetry(item)) return;
    const mine = flow = {ref: item.ref, phase: "preview", open: true, plan: null};
    $("retry-send").checked = false; $("retry-error").replaceChildren(); $("retry-notes").replaceChildren(); $("retry-rebill").hidden = true;
    $("retry-model").textContent = ""; $("retry-calls").textContent = "";
    $("retry-target").textContent = "要重试的回顾：" + stamp(item.at) + (text(item.detail) ? "（" + text(item.detail) + "）" : "");
    syncRetryButtons(); renderRetry();
    $("retry-dialog").hidden = false; $("retry-cancel").focus();
    previewRetry(mine);
  }
  async function previewRetry(mine) {
    try {
      const reply = await bridge.apiPost("benchmark", {action: "retry_review", ref: mine.ref, preview: true});
      if (flow !== mine) return;
      if (reply && reply.status === "error") { retryFailed(mine, {data: reply.data, message: reply.message}); return; }
      const plan = reply && reply.status === "ok" && plain(reply.data) ? reply.data : reply;
      if (!plain(plan) || !text(plan.model) && count(plan.llm_calls_min) === null && count(plan.llm_calls_max) === null) throw new Error("bad_result");
      mine.plan = plan; fillPlan(plan); mine.phase = "ready"; renderRetry(); $("retry-confirm").focus();
    } catch (error) { if (flow === mine) retryFailed(mine, error); }
  }
  async function submitRetry() {
    const mine = flow;
    if (!mine || mine.phase !== "ready") return;
    const send = $("retry-send").checked === true;
    mine.phase = "running"; renderRetry(); syncRetryButtons();
    try {
      const reply = await bridge.apiPost("benchmark", {action: "retry_review", ref: mine.ref, preview: false, send});
      if (reply && reply.status === "error") { retryFailed(mine, {data: reply.data, message: reply.message}); return; }
      const result = reply && reply.status === "ok" && plain(reply.data) ? reply.data : reply;
      if (!plain(result) || !RETRY_RESULTS.includes(result.state)) throw new Error("bad_result");
      retryDone(mine, result, send);
    } catch (error) { retryFailed(mine, error); }
  }
  function endRetry(mine) {
    if (flow === mine) flow = null;
    if (mine.open) { mine.open = false; $("retry-dialog").hidden = true; }
    wantFocus = mine.ref; syncRetryButtons();
  }
  function retryDone(mine, result, send) {
    const detail = text(result.detail);
    const done = result.state === "succeeded", live = result.state === "running";
    const line = done ? "回顾重试完成，" + (send ? "已写入聊天记忆，并发到了 QQ。" : "已写入聊天记忆（没有在 QQ 发消息）。")
      : live ? "这通回顾还在后台处理中，请稍后刷新看结果，先不要重复点“重试”。" : "重试后这通回顾仍然失败，已保留原状，可以稍后再试。";
    endRetry(mine);
    notice(done ? "ok" : "warn", line + (detail ? "（" + detail + "）" : ""), [], "review-notice");
    load().then(consumeFocus);
  }
  function retryFailed(mine, error) {
    const problem = retryProblem(error);
    if (RETRY_RELOAD.includes(problem.code) || problem.code === "bad_result") load();
    if (mine.open) {
      mine.phase = "error"; $("retry-error").className = "notice " + problem.kind; $("retry-error").replaceChildren(make("span", "", problem.message));
      renderRetry(); syncRetryButtons(); $("retry-cancel").focus();
    } else { endRetry(mine); notice(problem.kind, problem.message, [], "review-notice"); }
  }
  function closeRetry() {
    const mine = flow;
    if (!mine || !mine.open) return;
    mine.open = false; $("retry-dialog").hidden = true;
    if (mine.phase !== "running") { flow = null; syncRetryButtons(); }
    wantFocus = mine.ref; consumeFocus();
  }
  function retryKeys(event) {
    if (!event) return;
    if (event.key === "Escape") { closeRetry(); return; }
    if (event.key !== "Tab") return;
    const order = retryOrder();
    if (!order.length) { event.preventDefault(); return; }
    const now = document.activeElement, outside = !order.includes(now);
    if (event.shiftKey ? now === order[0] || outside : now === order[order.length - 1] || outside) {
      event.preventDefault(); (event.shiftKey ? order[order.length - 1] : order[0]).focus();
    }
  }

  // ---- copy data ----------------------------------------------------------------------------------------------
  // The copied block is the data the backend sent (GET `data`), with only the newest one-click result swapped in as
  // last_run when this page ran a test; no page state (expanded rows, notices, scroll, theme) is added.
  function copyPayload() {
    const body = JSON.parse(JSON.stringify(data));
    if (Array.isArray(body.active_calls)) body.active_calls = activeCalls();
    if (lastRun) body.last_run = JSON.parse(JSON.stringify(lastRun));
    const now = new Date();
    const when = now.getFullYear() + "-" + pad2(now.getMonth() + 1) + "-" + pad2(now.getDate()) + " " + pad2(now.getHours()) + ":" + pad2(now.getMinutes());
    return COPY_INTRO + "\n\n导出时间（本机时间）：" + when + "\n\n===== 数据（JSON）=====\n" + JSON.stringify(body, null, 2) + "\n";
  }
  function selectAll(area) {
    area.focus(); area.select(); area.scrollTop = 0;
    if (typeof area.setSelectionRange === "function") area.setSelectionRange(0, area.value.length);
  }
  function openCopyDialog(payload) {
    const area = $("copy-text");
    area.value = payload; $("copy-dialog-note").textContent = "";
    $("copy-dialog").hidden = false; selectAll(area);
  }
  function closeCopyDialog() {
    if ($("copy-dialog").hidden) return;
    $("copy-dialog").hidden = true; $("copy-text").value = ""; $("copy").focus();
  }
  async function copyData() {
    if (!data) return;
    const payload = copyPayload();
    clearNotice("copy-notice");
    try {
      if (typeof navigator === "undefined" || !navigator.clipboard || typeof navigator.clipboard.writeText !== "function") throw new Error("no_clipboard");
      await navigator.clipboard.writeText(payload);
      closeCopyDialog();
      notice("ok", "已复制（共 " + payload.length + " 字）。现在可以粘贴给任何 AI 助手，让它帮你分析。", [], "copy-notice");
    } catch (_) {
      openCopyDialog(payload);
    }
  }
  function selectAndCopy() {
    selectAll($("copy-text"));
    let done = false;
    try { done = typeof document.execCommand === "function" && document.execCommand("copy") === true; } catch (_) { done = false; }
    $("copy-dialog-note").textContent = done ? "已复制，现在可以粘贴给 AI。" : "已全选，请按 Ctrl+C 复制（手机上长按选中的文字，选“复制”）。";
  }

  // ---- loading -----------------------------------------------------------------------------------------------
  function render() {
    const cfg = compareConfig();
    renderHealth(cfg); renderActiveCall(); renderOverview(cfg); renderStages(); renderHints(); renderRun(); renderCalls(); renderStatus(); renderReviews();
  }
  function load() {
    if (!loadTask) loadTask = readData().finally(() => { loadTask = null; });
    return loadTask;
  }
  async function readData() {
    $("refresh").disabled = true; $("refresh").textContent = "刷新中…";
    try {
      const next = await bridge.apiGet("benchmark");
      if (!next || typeof next !== "object" || !Array.isArray(next.stage_defs) || !next.stage_defs.length) throw new Error("bad_data");
      data = next;
      if (next.last_run && Array.isArray(next.last_run.tests)) {
        const merged = pickRun(lastRun, next.last_run);
        if (merged !== lastRun) { lastRun = merged; lastRunFresh = false; }
      }
      $("loading").hidden = true; $("content").hidden = false; $("copy").disabled = false; render();
      $("updated").textContent = "更新于 " + (() => { const now = new Date(); return pad2(now.getHours()) + ":" + pad2(now.getMinutes()) + ":" + pad2(now.getSeconds()); })();
      if (Number.isSafeInteger(next.schema) && next.schema > 1) notice("warn", "这个页面比插件旧，部分内容可能显示不全，建议升级插件。");
      else if (!running && $("notice").className.includes("error")) clearNotice();
      return true;
    } catch (_) {
      $("loading").hidden = true;
      notice("error", data ? "刷新失败，下面显示的是上一次读到的数据。" : "读取评测与诊断数据失败：请确认已登录管理端、插件已启用，然后点“刷新”。");
      return false;
    } finally { $("refresh").disabled = false; $("refresh").textContent = "刷新"; }
  }

  function context(value) {
    document.documentElement.dataset.theme = value && value.isDark ? "dark" : "light";
    document.documentElement.lang = value && typeof value.locale === "string" ? value.locale : "zh-CN";
  }

  $("refresh").addEventListener("click", () => { clearNotice(); clearNotice("copy-notice"); clearNotice("review-notice"); clearNotice("hangup-notice"); load(); });
  $("copy").addEventListener("click", copyData);
  $("copy-select").addEventListener("click", selectAndCopy);
  $("copy-close").addEventListener("click", closeCopyDialog);
  $("copy-dialog").addEventListener("click", event => { if (event.target === $("copy-dialog")) closeCopyDialog(); });
  $("copy-dialog").addEventListener("keydown", event => { if (event && event.key === "Escape") closeCopyDialog(); });
  $("hangup-submit").addEventListener("click", submitHangup);
  $("hangup-cancel").addEventListener("click", closeHangup);
  $("hangup-dialog").addEventListener("click", event => { if (event.target === $("hangup-dialog")) closeHangup(); });
  $("hangup-dialog").addEventListener("keydown", hangupKeys);
  $("retry-confirm").addEventListener("click", submitRetry);
  $("retry-cancel").addEventListener("click", closeRetry);
  $("retry-dialog").addEventListener("click", event => { if (event.target === $("retry-dialog")) closeRetry(); });
  $("retry-dialog").addEventListener("keydown", retryKeys);
  $("run").addEventListener("click", run);
  $("goto-test").addEventListener("click", () => { const card = $("test-card"); if (card.scrollIntoView) card.scrollIntoView({behavior: "smooth", block: "start"}); });
  if (!bridge) { $("loading").hidden = true; $("run").disabled = true; notice("error", "请从 AstrBot 插件页面打开此页。"); $("refresh").disabled = true; return; }
  bridge.ready().then(() => { context(bridge.getContext()); bridge.onContext(context); return load(); })
    .catch(() => { $("loading").hidden = true; notice("error", "无法连接 AstrBot 管理页面。"); });
})();
