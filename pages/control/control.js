/* Control panel for the Bot-Call plugin config. Auth stays in the native dashboard parent: this page only
   calls bridge.apiGet/apiPost with namespace-relative endpoints and never sees tokens, keys or raw HTTP.
   The page runs in a sandboxed iframe (no modals, popups or top navigation), so every prompt is in-page. */
(() => {
  "use strict";
  const $ = id => document.getElementById(id);
  const bridge = window.AstrBotPluginPage;
  const APPLIES = {next_call: "下一通电话生效", reload: "重载后生效"};
  const RELOAD_HINT_DEFAULT = "请在 AstrBot 插件管理里重载 Bot-Call。";
  const entryWide = new Map();
  let data = null, fields = [], baseline = {}, current = {}, raw = {}, serverErrors = {}, controls = new Map();
  let saving = false, reloading = false, loadedOnce = false, categories = [], selected = null;
  const ICONS = {
    plug: "M9 3v5M15 3v5M7 8h10v3a5 5 0 0 1-10 0V8zM12 16v5",
    chat: "M4 5h16v11H9l-5 4V5z",
    mic: "M12 3a3 3 0 0 1 3 3v5a3 3 0 0 1-6 0V6a3 3 0 0 1 3-3zM6 11a6 6 0 0 0 12 0M12 17v4M9 21h6",
    note: "M6 3h9l4 4v14H6V3zM14 3v5h5M9 13h7M9 17h7",
  };
  function icon(name) {
    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg"), path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    svg.setAttribute("viewBox", "0 0 24 24"); svg.setAttribute("aria-hidden", "true"); svg.setAttribute("class", "icon");
    path.setAttribute("d", ICONS[name] || ICONS.note); svg.append(path); return svg;
  }
  // Provider counts vary with AstrBot configuration, but their controls stay compact.
  const PROVIDER_SELECT_KEYS = ["call_chat_provider_id", "call_tts_provider_id", "call_stt_provider_id"];
  const isRadioChoice = field => field.type === "choice" && field.choices.length <= 4 &&
    !PROVIDER_SELECT_KEYS.includes(field.key);
  const isWide = field => field.type === "text" || field.type === "list" || field.type === "string" ||
    isRadioChoice(field);

  const make = (tag, className, text) => {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  };
  const same = (a, b) => Array.isArray(a) || Array.isArray(b) ? JSON.stringify(a) === JSON.stringify(b) : a === b;
  const lines = value => Array.isArray(value) ? value.join("\n") : "";
  const fromLines = text => String(text).split(/\r?\n/).map(line => line.trim()).filter(Boolean);

  function describe(field, value) {
    if (field.type === "bool") return value ? "开" : "关";
    if (field.type === "list") return Array.isArray(value) && value.length ? value.join("、") : "（空）";
    if (field.type === "choice") {
      const hit = (field.choices || []).find(choice => same(choice.value, value));
      return hit ? String(hit.label) : String(value === "" || value == null ? "（空）" : value);
    }
    const text = value === "" || value == null ? "（空）" : String(value);
    return text.length > 60 ? text.slice(0, 60) + "…" : text;
  }

  function clientError(field, value) {
    if (field.type === "int") {
      const text = String(raw[field.key] ?? "").trim();
      if (!/^-?\d+$/.test(text)) return "请输入整数。";
      const number = Number(text), lo = field.min, hi = field.max;
      if (Number.isFinite(lo) && number < lo || Number.isFinite(hi) && number > hi) {
        return Number.isFinite(lo) && Number.isFinite(hi) ? "请输入 " + lo + "–" + hi + " 之间的整数。"
          : Number.isFinite(lo) ? "不能小于 " + lo + "。" : "不能大于 " + hi + "。";
      }
    }
    if (field.type === "choice" && !(field.choices || []).some(choice => same(choice.value, value))) return "请选择列表里的选项。";
    return "";
  }

  const isDirty = field => !same(current[field.key], baseline[field.key]) || (field.type === "int" && !!clientError(field, current[field.key]));
  const dirtyFields = () => fields.filter(isDirty);
  const writable = () => !!data && data.writable === true;

  // ---- notices ---------------------------------------------------------------------------------------------
  function notice(kind, text, actions = []) {
    const box = $("notice");
    box.className = "notice " + kind;
    box.replaceChildren(make("span", "", text));
    for (const [label, handler] of actions) {
      const button = make("button", "link", label); button.type = "button"; button.addEventListener("click", handler); box.append(button);
    }
    box.hidden = false;
  }
  const clearNotice = () => { $("notice").hidden = true; $("notice").replaceChildren(); };

  // ---- rendering -------------------------------------------------------------------------------------------
  function fieldControl(field) {
    const id = "f-" + field.key, inputs = [];
    let node;
    const onChange = () => update(field);
    if (field.type === "bool") {
      const label = make("label", "switch");
      const input = make("input"); input.type = "checkbox"; input.id = id; input.setAttribute("role", "switch");
      input.checked = current[field.key] === true;
      const text = make("span", "switch-text", ""); label.append(input, make("span", "track"), text);
      input.addEventListener("change", () => { current[field.key] = input.checked; text.textContent = input.checked ? "开" : "关"; onChange(); });
      text.textContent = input.checked ? "开" : "关"; inputs.push(input); node = label;
      controls.set(field.key, {sync: () => { input.checked = current[field.key] === true; text.textContent = input.checked ? "开" : "关"; }});
    } else if (isRadioChoice(field)) {
      node = make("div", "radios"); node.setAttribute("role", "radiogroup");
      node.setAttribute("aria-labelledby", id + "-label");
      field.choices.forEach((choice, index) => {
        const option = make("label", "radio"), input = make("input");
        input.type = "radio"; input.name = id; input.id = id + "-" + index; input.value = String(choice.value);
        input.checked = same(current[field.key], choice.value);
        input.addEventListener("change", () => { if (input.checked) { current[field.key] = choice.value; onChange(); } });
        const body = make("span", "radio-body"); body.append(make("span", "radio-label", String(choice.label)));
        for (const extra of [choice.description, choice.example]) if (typeof extra === "string" && extra) body.append(make("span", extra === choice.example ? "radio-example" : "radio-desc", extra));
        option.append(input, body); node.append(option); inputs.push(input);
      });
      controls.set(field.key, {sync: () => field.choices.forEach((choice, index) => { inputs[index].checked = same(current[field.key], choice.value); })});
    } else if (field.type === "choice") {
      node = make("select"); node.id = id;
      for (const choice of field.choices) { const option = make("option", "", String(choice.label)); option.value = String(choice.value); node.append(option); }
      const pick = () => (field.choices.find(choice => String(choice.value) === node.value) || {}).value;
      node.value = String(current[field.key]);
      node.addEventListener("change", () => { current[field.key] = pick(); onChange(); });
      inputs.push(node); controls.set(field.key, {sync: () => { node.value = String(current[field.key]); }});
    } else if (field.type === "text" || field.type === "list") {
      node = make("textarea"); node.id = id; node.rows = field.type === "text" ? 5 : 3;
      if (field.type === "list") node.setAttribute("placeholder", "每行一项");
      node.value = field.type === "list" ? lines(current[field.key]) : String(current[field.key] ?? "");
      node.addEventListener("input", () => { current[field.key] = field.type === "list" ? fromLines(node.value) : node.value; onChange(); });
      inputs.push(node);
      controls.set(field.key, {sync: () => { node.value = field.type === "list" ? lines(current[field.key]) : String(current[field.key] ?? ""); }});
    } else {
      node = make("input"); node.id = id; node.type = field.type === "int" ? "number" : "text";
      if (field.type === "int") { node.setAttribute("inputmode", "numeric"); node.step = "1"; if (Number.isFinite(field.min)) node.min = String(field.min); if (Number.isFinite(field.max)) node.max = String(field.max); }
      node.value = String(raw[field.key] ?? current[field.key] ?? "");
      node.addEventListener("input", () => {
        raw[field.key] = node.value;
        current[field.key] = field.type === "int" ? (/^-?\d+$/.test(node.value.trim()) ? Number(node.value.trim()) : NaN) : node.value;
        onChange();
      });
      inputs.push(node);
      controls.set(field.key, {sync: () => { node.value = String(raw[field.key] ?? current[field.key] ?? ""); }});
    }
    const entry = controls.get(field.key); entry.inputs = inputs; entry.field = field;
    return node;
  }

  function renderField(field) {
    const row = make("div", "field"); row.setAttribute("data-key", field.key);
    const wide = isWide(field); entryWide.set(field.key, wide);
    const info = make("div", "info"), head = make("div", "field-head");
    const label = make("label", "field-label", String(field.label)); label.id = "f-" + field.key + "-label";
    if (!isRadioChoice(field)) label.setAttribute("for", "f-" + field.key);
    const state = make("span", "state", ""); state.hidden = true; head.append(label, state);
    const control = fieldControl(field), holder = make("div", "control"); holder.append(control);
    const entry = controls.get(field.key); entry.row = row; entry.state = state;
    const help = make("p", "help", String(field.help || ""));
    const meta = make("div", "meta"); meta.append(make("span", "default", "默认：" + describe(field, field.default)));
    if (APPLIES[field.applies]) meta.append(make("span", "badge " + field.applies, APPLIES[field.applies]));
    const restore = make("button", "link restore", "恢复默认"); restore.type = "button";
    restore.addEventListener("click", () => setValue(field, field.default));
    meta.append(restore);
    const error = make("p", "error", ""); error.hidden = true; error.setAttribute("role", "alert");
    entry.restore = restore; entry.error = error;
    info.append(head, help, meta);
    row.className = "field" + (wide ? " wide" : "");
    row.append(info, holder, error);
    return row;
  }

  const rowClass = (field, message) => "field" + (entryWide.get(field.key) ? " wide" : "") + (message ? " invalid" : "") + (isDirty(field) ? " dirty" : "");
  function setValue(field, value) {
    current[field.key] = Array.isArray(value) ? value.slice() : value;
    raw[field.key] = String(value ?? "");
    controls.get(field.key).sync();
    update(field);
  }

  function update(field) {
    const entry = controls.get(field.key); if (!entry) return;
    delete serverErrors[field.key];
    const value = current[field.key], message = clientError(field, value) || serverErrors[field.key] || "";
    entry.error.textContent = message; entry.error.hidden = !message;
    entry.row.className = rowClass(field, message);
    entry.state.textContent = isDirty(field) ? "未保存" : ""; entry.state.hidden = !isDirty(field);
    entry.restore.disabled = !writable() || same(value, field.default);
    for (const input of entry.inputs) { input.disabled = !writable(); if (input.setAttribute) input.setAttribute("aria-invalid", message ? "true" : "false"); }
    summary();
  }

  function showErrors() {
    for (const field of fields) {
      const entry = controls.get(field.key), message = clientError(field, current[field.key]) || serverErrors[field.key] || "";
      entry.error.textContent = message; entry.error.hidden = !message;
      entry.row.className = rowClass(field, message);
      entry.state.textContent = isDirty(field) ? "未保存" : ""; entry.state.hidden = !isDirty(field);
      entry.restore.disabled = !writable() || same(current[field.key], field.default);
      for (const input of entry.inputs) { input.disabled = !writable() || saving; if (input.setAttribute) input.setAttribute("aria-invalid", message ? "true" : "false"); }
    }
    summary();
  }

  function summary() {
    const dirty = dirtyFields(), invalid = fields.some(field => clientError(field, current[field.key]) || serverErrors[field.key]);
    $("dirty").textContent = dirty.length ? "有 " + dirty.length + " 项未保存" : "没有未保存的修改"; $("dirty").className = dirty.length ? "chip warn" : "chip";
    $("save").disabled = !writable() || saving || !dirty.length || fields.some(field => clientError(field, current[field.key]));
    $("discard").disabled = !writable() || saving || !dirty.length;
    $("actions").hidden = !data || !writable();
    $("save").textContent = saving ? "正在保存…" : "保存修改";
    if (!dirty.length && $("refresh").confirming) { $("refresh").confirming = false; $("refresh").textContent = "刷新"; }
    if (invalid && dirty.length) $("save").title = "请先修正标红的项目"; else $("save").title = "";
    markNav();
  }

  function select(id) {
    const category = categories.find(item => item.id === id) || categories[0]; if (!category) return;
    selected = category.id;
    for (const item of categories) {
      item.node.hidden = item !== category; item.button.setAttribute("aria-current", item === category ? "page" : "false");
      item.button.className = "nav-item" + (item === category ? " active" : "");
    }
    $("category-title").textContent = category.title; if ($("content")) $("content").scrollTop = 0;
  }
  function markNav() {
    for (const item of categories) {
      const keys = item.keys.map(key => fields.find(field => field.key === key)).filter(Boolean);
      const bad = keys.some(field => clientError(field, current[field.key]) || serverErrors[field.key]), dirty = keys.some(isDirty);
      item.dot.hidden = !bad && !dirty; item.dot.className = "dot" + (bad ? " bad" : "");
      item.button.setAttribute("aria-label", item.title + (bad ? "（有错误）" : dirty ? "（有未保存修改）" : ""));
    }
  }

  function render() {
    controls = new Map(); fields = []; baseline = {}; current = {}; raw = {}; serverErrors = {}; categories = []; entryWide.clear();
    const body = $("groups"), nav = $("nav"); body.replaceChildren(); nav.replaceChildren();
    for (const group of data.groups) {
      const category = make("div", "category"); category.id = "g-" + group.id; category.hidden = true;
      // Older responses have no cards: the whole category is one card.
      const cards = Array.isArray(group.sections) && group.sections.length ? group.sections : [{id: group.id, title: group.title, fields: group.fields || []}];
      const keys = [];
      for (const cardData of cards) {
        const card = make("section", "card"); card.id = "s-" + group.id + "-" + cardData.id;
        card.append(make("h3", "", String(cardData.title)));
        for (const field of cardData.fields) {
          fields.push(field); keys.push(field.key);
          baseline[field.key] = Array.isArray(field.value) ? field.value.slice() : field.value;
          current[field.key] = Array.isArray(field.value) ? field.value.slice() : field.value;
          raw[field.key] = String(field.value ?? "");
          card.append(renderField(field));
        }
        category.append(card);
      }
      body.append(category);
      const button = make("button", "nav-item"); button.type = "button"; button.id = "nav-" + group.id;
      const dot = make("span", "dot"); dot.hidden = true;
      button.append(icon(group.icon), make("span", "nav-text", String(group.title)), dot);
      button.addEventListener("click", () => select(group.id)); nav.append(button);
      categories.push({id: group.id, title: String(group.title), node: category, button, dot, keys});
    }
    $("version").textContent = typeof data.version === "string" ? "v" + data.version : "";
    $("loading").hidden = true; select(selected); showErrors(); banner();
  }

  function banner() {
    const readonly = !writable();
    $("readonly").hidden = !readonly;
    $("readonly").textContent = readonly ? "当前只能查看，不能在这里保存：" + (typeof data.readonly_reason === "string" && data.readonly_reason ? data.readonly_reason : "管理端暂不支持写入。请在 AstrBot 的插件配置页修改。") : "";
    const rejection = $("access-rejection");
    const last = data.last_access_rejection;
    rejection.hidden = !last;
    rejection.textContent = last ? "最近一次接入拒绝（刷新更新，重载清空）：" +
      new Date(last.at * 1000).toLocaleString() + "；原因 " + last.reason +
      "；peer " + last.peer + "；X-Forwarded-Proto 数量 " + last.forwarded_proto_count +
      "，恰好为 https：" + last.forwarded_proto_https +
      "；Host " + last.host + "，期望 " + last.expected_host +
      "；Origin 存在：" + last.origin_present + "，匹配：" + last.origin_matches : "";
    const bar = $("reload-bar"), button = $("reload"), hint = $("reload-hint");
    const needs = data.reload_required === true, calls = Number.isInteger(data.active_calls) ? data.active_calls : 0;
    const byHint = typeof data.reload_hint === "string" && data.reload_hint.length > 0;
    bar.hidden = !needs && !byHint;
    button.hidden = byHint || !needs; button.disabled = reloading || calls > 0;
    hint.textContent = byHint ? data.reload_hint : !needs ? "" : calls > 0
      ? "已保存的设置要重载后才生效；现在有 " + calls + " 通通话进行中，等结束后再重载。"
      : "已保存的设置要重载后才生效。重载会让插件短暂离线几秒，进行中没有通话时才能重载。";
    $("calls").textContent = Number.isInteger(data.active_calls) ? "进行中的通话：" + data.active_calls : "";
  }

  // ---- talking to the plugin -------------------------------------------------------------------------------
  const codeOf = error => {
    const message = String(error && error.message || "");
    for (const code of ["settings_invalid", "settings_conflict", "reload_busy"]) if (message.includes(code)) return {code, message};
    return {code: "", message};
  };
  function errorMap(source) {
    if (!source || typeof source !== "object") return null;
    const map = source.errors && typeof source.errors === "object" ? source.errors : source;
    const out = {};
    for (const [key, value] of Object.entries(map)) if (typeof value === "string" && fields.some(field => field.key === key)) out[key] = value;
    return Object.keys(out).length ? out : null;
  }
  function errorsFromMessage(message) {
    const start = message.indexOf("{"); if (start < 0) return null;
    try { return errorMap(JSON.parse(message.slice(start))); } catch (_) { return null; }
  }

  async function load(keep) {
    $("loading").hidden = false;
    try {
      const next = await bridge.apiGet("settings");
      if (!next || !Array.isArray(next.groups) || typeof next.revision !== "string") throw new Error("shape");
      const mine = keep ? dirtyFields().map(field => [field, current[field.key], raw[field.key]]) : [];
      data = next; render(); loadedOnce = true;
      for (const [field, value, text] of mine) {
        if (!fields.some(item => item.key === field.key)) continue;
        current[field.key] = value; raw[field.key] = text; controls.get(field.key).sync();
      }
      showErrors(); return true;
    } catch (_) {
      $("loading").hidden = true;
      if (!loadedOnce) { $("groups").replaceChildren(); $("actions").hidden = true; }
      notice("error", "读取设置失败：请确认已登录管理端、插件已启用，然后点“刷新”。", []);
      return false;
    }
  }

  async function save() {
    if ($("save").disabled) return;
    const dirty = dirtyFields(), values = {};
    for (const field of dirty) values[field.key] = Array.isArray(current[field.key]) ? current[field.key].slice() : current[field.key];
    saving = true; serverErrors = {}; showErrors(); notice("info", "正在保存…");
    try {
      const result = await bridge.apiPost("settings", {revision: data.revision, values});
      const rejected = errorMap(result && !Array.isArray(result.saved) ? result : result && result.errors ? result : null);
      if (rejected) { serverErrors = rejected; invalidNotice(); return; }
      const saved = result && Array.isArray(result.saved) ? result.saved : dirty.map(field => field.key);
      const needsReload = saved.some(key => (fields.find(field => field.key === key) || {}).applies === "reload");
      const pending = needsReload || result && result.reload_required === true;
      for (const key of saved) { baseline[key] = Array.isArray(current[key]) ? current[key].slice() : current[key]; }
      if (result && typeof result.revision === "string") data.revision = result.revision;
      const text = "已保存 " + saved.length + " 项。" + (needsReload ? "其中有设置需要重载后才生效。" : "新设置从下一通电话开始生效。");
      await load(true); // Show what the plugin really holds now (it may normalise values).
      if (pending) data.reload_required = true;
      banner(); notice("ok", text);
    } catch (error) {
      const {code, message} = codeOf(error);
      if (code === "settings_invalid") {
        serverErrors = errorsFromMessage(message) || {}; invalidNotice();
      } else if (code === "settings_conflict") {
        notice("warn", "这份配置刚在别处被修改过（可能是 AstrBot 的插件配置页）。你的修改还没有保存。", [["载入最新配置（保留我的修改）", () => load(true).then(ok => ok && notice("info", "已载入最新配置，你的未保存修改仍在，请核对后再保存。"))]]);
      } else {
        notice("error", "保存失败：没能连上插件或管理端。你填的内容还在，稍后可再点保存。");
      }
    } finally { saving = false; showErrors(); }
  }
  function invalidNotice() {
    const count = Object.keys(serverErrors).length;
    notice("error", count ? "有 " + count + " 项没通过校验，没有保存任何内容。请看标红的项目。" : "有设置没通过校验，没有保存任何内容。服务器没有说明具体是哪一项，请核对数值范围后再试。");
    showErrors();
    const first = fields.find(field => serverErrors[field.key]);
    if (first) {
      const home = categories.find(item => item.keys.includes(first.key)); if (home) select(home.id);
      if (controls.get(first.key).row.scrollIntoView) controls.get(first.key).row.scrollIntoView();
    }
  }

  async function reload() {
    if ($("reload").disabled) return;
    reloading = true; banner(); notice("info", "已请求重载，正在确认…");
    try {
      await bridge.apiPost("reload", {});
      for (let attempt = 0; attempt < 6; attempt++) {
        await new Promise(resolve => setTimeout(resolve, 1500));
        try { const next = await bridge.apiGet("settings"); if (next && next.reload_required === false) { reloading = false; await load(true); notice("ok", "已重载，新设置已生效。"); return; } } catch (_) { /* The plugin is restarting. */ }
      }
      notice("warn", "已请求重载，但暂时没能确认是否完成。请稍后点“刷新”查看。", [["刷新", () => load(true)]]);
    } catch (error) {
      const {code} = codeOf(error);
      notice(code === "reload_busy" ? "warn" : "error", code === "reload_busy" ? "现在有通话进行中，不能重载。等通话结束后再试。" : "重载没有成功。" + RELOAD_HINT_DEFAULT);
    } finally { reloading = false; if (data) banner(); }
  }

  function context(value) {
    document.documentElement.dataset.theme = value && value.isDark ? "dark" : "light";
    document.documentElement.lang = value && typeof value.locale === "string" ? value.locale : "zh-CN";
  }

  $("save").addEventListener("click", save);
  $("reload").addEventListener("click", reload);
  $("refresh").addEventListener("click", () => {
    if (dirtyFields().length && !$("refresh").confirming) { $("refresh").confirming = true; $("refresh").textContent = "放弃未保存修改并刷新？再点一次确认"; return; }
    $("refresh").confirming = false; $("refresh").textContent = "刷新";
    load(false).then(ok => { if (ok) clearNotice(); });
  });
  $("discard").addEventListener("click", () => { for (const field of fields) setValue(field, baseline[field.key]); serverErrors = {}; showErrors(); clearNotice(); });
  if (window.addEventListener) window.addEventListener("beforeunload", event => {
    if (!dirtyFields().length) return undefined;
    event.preventDefault(); event.returnValue = "还有未保存的修改。"; return event.returnValue;
  });
  if (!bridge) { $("loading").hidden = true; notice("error", "请从 AstrBot 插件页面打开此页。"); $("refresh").disabled = true; return; }
  bridge.ready().then(() => { context(bridge.getContext()); bridge.onContext(context); return load(false); })
    .catch(() => { $("loading").hidden = true; notice("error", "无法连接 AstrBot 管理页面。"); });
})();
