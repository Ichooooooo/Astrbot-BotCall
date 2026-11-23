// Control panel scenarios against a mock plugin-page bridge: no network, browser or dependencies.
import assert from "node:assert/strict";
import vm from "node:vm";
const source = globalThis.controlPageSource, html = globalThis.controlPageHtml, css = globalThis.controlPageCss, fixture = JSON.parse(globalThis.controlPageFixture);

class Element {
  constructor(tag) { this.tagName = tag.toUpperCase(); this.children = []; this.attributes = {}; this.listeners = {}; this.className = "";
    this._text = ""; this.hidden = false; this.disabled = false; this.value = ""; this.checked = false; this.dataset = {}; }
  get textContent() { return this.children.length ? this.children.map(child => child.textContent).join("") : this._text; }
  set textContent(value) { this.children = []; this._text = String(value); }
  append(...nodes) { for (const node of nodes) this.children.push(node); }
  appendChild(node) { this.children.push(node); return node; }
  replaceChildren(...nodes) { this.children = []; this._text = ""; this.append(...nodes); }
  setAttribute(name, value) { this.attributes[name] = String(value); }
  removeAttribute(name) { delete this.attributes[name]; }
  addEventListener(name, handler) { (this.listeners[name] ||= []).push(handler); }
  fire(name, extra = {}) { for (const handler of this.listeners[name] || []) handler({ target: this, preventDefault() {}, ...extra }); }
  click() { if (!this.disabled) this.fire("click"); }
  set innerHTML(_) { throw new Error("unsafe HTML sink used"); }
  scrollIntoView() { this.scrolled = true; }
}
const walk = (node, visit) => { visit(node); for (const child of node.children) walk(child, visit); };
const all = (node, test) => { const out = []; walk(node, item => { if (test(item)) out.push(item); }); return out; };
const clone = value => JSON.parse(JSON.stringify(value));
// Revision 2 carries every field twice: inside its card and in the flat per-category list. Keep both in step.
const eachField = (settings, visit) => { for (const group of settings.groups) { for (const field of group.fields || []) visit(field); for (const card of group.sections || []) for (const field of card.fields) visit(field); } };
const editField = (settings, key, change) => eachField(settings, field => { if (field.key === key) change(field); });

function boot(settings = fixture, {bridge: withBridge = true} = {}) {
  const ids = new Map();
  for (const match of html.matchAll(/<(\w+)([^>]*)\sid="([^"]+)"([^>]*)>/g)) {
    const element = new Element(match[1]); const attrs = match[2] + match[4];
    element.hidden = /\shidden(\s|$|>)/.test(attrs + ">"); element.disabled = /\sdisabled(\s|$|>)/.test(attrs + ">");
    ids.set(match[3], element);
  }
  const document = { documentElement: { dataset: {}, lang: "" }, createElement: tag => new Element(tag), createElementNS: (ns, tag) => { assert.equal(ns, "http://www.w3.org/2000/svg"); return new Element(tag); },
    getElementById: id => { assert.ok(ids.has(id), "page uses an element id that index.html does not define: " + id); return ids.get(id); } };
  const state = { settings: clone(settings), posts: [], gets: 0, getError: null, postHandler: null, onContext: null };
  const timers = [];
  const bridge = {
    ready: () => Promise.resolve(), getContext: () => ({ isDark: false, locale: "zh-CN" }), onContext: callback => { state.onContext = callback; },
    apiGet: async (...args) => {
      assert.deepEqual(args, ["settings"], "GET must be namespace-relative and take no extra arguments");
      state.gets++; if (state.getError) throw state.getError; return clone(state.settings);
    },
    apiPost: async (...args) => {
      assert.equal(args.length, 2); state.posts.push(clone(args));
      const [endpoint, body] = args;
      if (state.postHandler) return state.postHandler(endpoint, body);
      throw new Error("no handler");
    },
  };
  const listeners = {};
  const window = withBridge ? { AstrBotPluginPage: bridge, addEventListener: (name, handler) => { listeners[name] = handler; } } : {};
  vm.runInNewContext(source, { window, document, setTimeout: callback => { timers.push(callback); return timers.length; } }, { timeout: 1000 });
  const page = {
    document, state, listeners, el: id => document.getElementById(id), timers,
    row: key => all(document.getElementById("groups"), node => node.attributes["data-key"] === key)[0],
    input: key => all(page.row(key), node => ["INPUT", "TEXTAREA", "SELECT"].includes(node.tagName) && node.type !== "radio")[0],
    radios: key => all(page.row(key), node => node.type === "radio"),
    error: key => all(page.row(key), node => node.className === "error")[0],
    restore: key => all(page.row(key), node => node.className.includes("restore"))[0],
    async settle() { for (let i = 0; i < 6; i++) await new Promise(resolve => setImmediate(resolve)); },
    async pump() { for (let i = 0; i < 30; i++) { while (page.timers.length) page.timers.shift()(); await page.settle(); } },
    type(key, value) { const input = page.input(key); input.value = value; input.fire("input"); },
    notice: () => page.el("notice"),
  };
  return page;
}
const okPost = (page, extra = {}) => { page.state.postHandler = (endpoint, body) => {
  assert.equal(endpoint, "settings");
  eachField(page.state.settings, field => { if (Object.hasOwn(body.values, field.key)) field.value = body.values[field.key]; });
  page.state.settings.revision = "ffff0000ffff0000";
  return { saved: Object.keys(body.values), revision: "ffff0000ffff0000", reload_required: false, ...extra }; }; };

// ---- source hygiene --------------------------------------------------------------------------------------
assert.ok(!/innerHTML|outerHTML|insertAdjacentHTML|document\.write|eval\(|new Function|localStorage|sessionStorage|XMLHttpRequest|fetch\(|\.href\s*=|window\.open|confirm\(|alert\(|prompt\(/.test(source),
  "page must render by text only, not store or fetch by itself, and not use modal dialogs (sandboxed iframe)");
assert.ok(!/<script[^>]*>[^<]/.test(html) && !/\sstyle=/.test(html) && !/https?:\/\//.test(html), "no inline script/style or external resource");
for (const id of new Set([...source.matchAll(/\$\("([^"]+)"\)/g)].map(match => match[1]))) assert.ok(html.includes('id="' + id + '"'), "index.html lacks id " + id);
assert.ok(/“评测与诊断”页/.test(html) && !/诊断页或延迟评测页/.test(html), "the control page points to the merged evaluation and diagnostics page");

// Latest access rejection uses the existing notice style and text-only rendering.
{
  const withRejection = clone(fixture);
  withRejection.last_access_rejection = {
    at: 1794880000, reason: "proxy_rejected", peer: "192.0.2.1",
    forwarded_proto_count: 2, forwarded_proto_https: false,
    host: "wrong.example", expected_host: "calls.example.test",
    origin_present: true, origin_matches: false,
  };
  const page = boot(withRejection); await page.settle();
  assert.equal(page.el("access-rejection").hidden, false);
  assert.match(page.el("access-rejection").textContent, /192.0.2.1/);
  assert.match(page.el("access-rejection").textContent, /数量 2/);
  assert.match(page.el("access-rejection").textContent, /calls.example.test/);
}
// ---- rendering of every field type ------------------------------------------------------------------------
{
  const page = boot(); await page.settle();
  assert.equal(page.el("groups").children.length, 4);
  assert.deepEqual(page.el("nav").children.map(button => button.children[1].textContent), ["接入", "对话", "语音与断句", "回顾"]);
  assert.deepEqual(page.el("groups").children.map(category => category.children.map(card => card.children[0].textContent)),
    [["入口", "临时隧道"], ["通话模型与回复", "上下文"], ["合成与自动断句", "录音限制", "音频留存"], ["回顾写法", "篇幅"]]);
  assert.equal(all(page.el("groups"), node => node.attributes["data-key"]).length, 25);
  assert.equal(page.el("version").textContent, "v0.9.0");
  assert.equal(page.el("loading").hidden, true); assert.equal(page.el("readonly").hidden, true); assert.equal(page.el("reload-bar").hidden, true);
  assert.equal(page.el("actions").hidden, false); assert.equal(page.el("save").disabled, true); assert.equal(page.el("dirty").textContent, "没有未保存的修改");
  const enabled = page.input("enabled"); assert.equal(enabled.type, "checkbox"); assert.equal(enabled.attributes.role, "switch"); assert.equal(enabled.checked, true);
  assert.equal(page.input("call_history_summary").checked, true);
  const timeout = page.input("call_vad_silence_ms"); assert.equal(timeout.type, "number"); assert.equal(timeout.value, "1200"); assert.equal(timeout.min, "300"); assert.equal(timeout.max, "3000");
  const retention = page.input("audio_retention_days"); assert.equal(retention.type, "number"); assert.equal(retention.value, "7"); assert.equal(retention.min, "0"); assert.equal(retention.max, "3650");
  assert.match(page.row("audio_retention_days").textContent, /音频材料保留天数.*保留文字记录与回顾.*填 0 表示不自动清理/);
  assert.equal(page.input("public_origin").type, "text");
  assert.equal(page.input("call_reply_style").tagName, "TEXTAREA"); assert.equal(page.input("call_skip_plugin_hooks").tagName, "TEXTAREA");
  const select = page.input("call_chat_provider_id"); assert.equal(select.tagName, "SELECT"); assert.equal(select.children.length, 6); assert.equal(select.value, "example/fast-chat");
  const styles = page.radios("review_style"); assert.equal(styles.length, 4); assert.equal(styles[0].checked, true);
  assert.match(page.row("review_style").textContent, /要点＋待办/); assert.match(page.row("review_style").textContent, /约客户确认时间/, "radio choices show their example");
  assert.equal(page.radios("review_system_notes").length, 3); assert.equal(page.radios("access_mode").length, 2);
  assert.match(page.row("call_vad_silence_ms").textContent, /停顿时长（毫秒）/); assert.match(page.row("call_vad_silence_ms").textContent, /下一通电话生效/);
  assert.match(page.row("listen_port").textContent, /重载后生效/); assert.match(page.row("call_vad_silence_ms").textContent, /默认：1200/);
  assert.match(page.row("call_history_summary").textContent, /默认：开/); assert.match(page.row("review_style").textContent, /默认：标准回顾/);
  assert.match(page.row("call_chat_provider_id").textContent, /默认：跟随来源会话/);
  assert.match(page.el("groups").textContent, /较早对话先做摘要/);
  assert.ok(all(page.el("groups"), node => node.type === "password").length === 0, "no secret input");
  assert.equal(page.restore("call_vad_silence_ms").disabled, true, "already at default");
  page.state.onContext({ isDark: true, locale: "zh-CN" }); assert.equal(page.document.documentElement.dataset.theme, "dark");
}

// ---- immediate validation, dirty marks, restore default ----------------------------------------------------
{
  const page = boot(); await page.settle();
  page.type("call_vad_silence_ms", "100");
  assert.equal(page.error("call_vad_silence_ms").textContent, "请输入 300–3000 之间的整数。"); assert.equal(page.error("call_vad_silence_ms").hidden, false);
  assert.match(page.row("call_vad_silence_ms").className, /invalid/); assert.match(page.row("call_vad_silence_ms").className, /dirty/);
  assert.equal(page.el("save").disabled, true, "an invalid value blocks saving"); assert.equal(page.el("dirty").textContent, "有 1 项未保存");
  page.type("call_vad_silence_ms", "abc"); assert.equal(page.error("call_vad_silence_ms").textContent, "请输入整数。");
  page.type("call_vad_silence_ms", "5000"); assert.match(page.error("call_vad_silence_ms").textContent, /300–3000/);
  assert.equal(page.input("call_tts_provider_id").value, "");
  assert.match(page.row("call_tts_provider_id").textContent, /电话合成服务/);
  page.type("call_vad_silence_ms", ""); assert.equal(page.error("call_vad_silence_ms").textContent, "请输入整数。");
  page.type("call_vad_silence_ms", "1500"); assert.equal(page.error("call_vad_silence_ms").hidden, true); assert.equal(page.el("save").disabled, false);
  assert.equal(page.restore("call_vad_silence_ms").disabled, false);
  page.restore("call_vad_silence_ms").click();
  assert.equal(page.input("call_vad_silence_ms").value, "1200"); assert.equal(page.el("save").disabled, true); assert.equal(page.el("dirty").textContent, "没有未保存的修改");
  assert.doesNotMatch(page.row("call_vad_silence_ms").className, /dirty/);
  // Restore default for a choice, a bool and a list.
  page.radios("review_style")[2].checked = true; page.radios("review_style")[2].fire("change"); assert.equal(page.el("dirty").textContent, "有 1 项未保存");
  page.restore("review_style").click(); assert.equal(page.radios("review_style")[0].checked, true); assert.equal(page.radios("review_style")[2].checked, false);
  page.input("call_history_summary").checked = false; page.input("call_history_summary").fire("change"); assert.equal(page.el("dirty").textContent, "有 1 项未保存");
  page.restore("call_history_summary").click(); assert.equal(page.input("call_history_summary").checked, true); assert.equal(page.el("dirty").textContent, "没有未保存的修改");
  page.type("listen_port", "80"); assert.match(page.error("listen_port").textContent, /1024–65535/);
  page.type("tunnel_start_timeout", "10"); assert.equal(page.error("tunnel_start_timeout").hidden, true);
}

// ---- saving: only changed keys, then the page shows what the plugin now holds -------------------------------
{
  const page = boot(); await page.settle(); okPost(page);
  page.type("call_vad_silence_ms", "1500");
  page.input("call_history_summary").checked = false; page.input("call_history_summary").fire("change");
  page.radios("review_style")[1].checked = true; page.radios("review_style")[1].fire("change");
  page.type("call_skip_plugin_hooks", "alpha\n\n  beta  \n"); page.type("call_reply_style", "多用短句。\n不要列表。");
  const select = page.input("call_chat_provider_id"); select.value = "openrouter/v4.1-flash"; select.fire("change");
  assert.equal(page.el("dirty").textContent, "有 6 项未保存");
  page.el("save").click(); await page.settle();
  assert.equal(page.state.posts.length, 1);
  assert.deepEqual(page.state.posts[0], ["settings", { revision: "a1b2c3d4e5f60718", values: {
    call_vad_silence_ms: 1500, call_history_summary: false, review_style: "bullets", call_skip_plugin_hooks: ["alpha", "beta"],
    call_reply_style: "多用短句。\n不要列表。", call_chat_provider_id: "openrouter/v4.1-flash" } }]);
  assert.equal(page.notice().className, "notice ok"); assert.match(page.notice().textContent, /已保存 6 项/); assert.match(page.notice().textContent, /下一通电话/);
  assert.equal(page.el("reload-bar").hidden, true); assert.equal(page.el("dirty").textContent, "没有未保存的修改"); assert.equal(page.el("save").disabled, true);
  assert.equal(page.state.gets, 2, "re-read after saving"); assert.equal(page.input("call_vad_silence_ms").value, "1500");
  assert.equal(page.radios("review_style")[1].checked, true); assert.equal(page.input("call_skip_plugin_hooks").value, "alpha\nbeta");
  // The next save carries the new revision and nothing unchanged.
  page.type("call_vad_silence_ms", "1000"); page.el("save").click(); await page.settle();
  assert.deepEqual(page.state.posts[1], ["settings", { revision: "ffff0000ffff0000", values: { call_vad_silence_ms: 1000 } }]);
  // Nothing changed: no request at all.
  const before = page.state.posts.length; page.el("save").click(); await page.settle(); assert.equal(page.state.posts.length, before);
}

// ---- settings that need a reload, and the reload button ---------------------------------------------------------
{
  const page = boot(); await page.settle();
  page.state.postHandler = (endpoint, body) => {
    if (endpoint === "reload") { assert.deepEqual(clone(body), {}); page.state.settings.reload_required = false; return { reloading: true }; }
    editField(page.state.settings, "listen_port", field => { field.value = body.values.listen_port; });
    page.state.settings.reload_required = true; page.state.settings.revision = "1111222233334444";
    return { saved: ["listen_port"], revision: "1111222233334444", reload_required: true };
  };
  page.type("listen_port", "6200"); page.el("save").click(); await page.settle();
  assert.match(page.notice().textContent, /已保存 1 项/); assert.match(page.notice().textContent, /重载后才生效/);
  assert.equal(page.el("reload-bar").hidden, false); assert.equal(page.el("reload").hidden, false); assert.equal(page.el("reload").disabled, false);
  assert.match(page.el("reload-hint").textContent, /重载后才生效/);
  page.el("reload").click(); await page.pump();
  assert.deepEqual(page.state.posts.at(-1), ["reload", {}]); assert.equal(page.notice().className, "notice ok"); assert.match(page.notice().textContent, /已重载/);
  assert.equal(page.el("reload-bar").hidden, true);
}
{
  const busy = clone(fixture); busy.reload_required = true; busy.active_calls = 2;
  const page = boot(busy); await page.settle();
  assert.equal(page.el("reload").disabled, true); assert.match(page.el("reload-hint").textContent, /2 通通话进行中/); assert.match(page.el("calls").textContent, /进行中的通话：2/);
  page.state.settings.active_calls = 0; page.el("refresh").click(); await page.settle();
  assert.equal(page.el("reload").disabled, false);
  page.state.postHandler = () => { throw new Error("bot_call.reload_busy"); };
  page.el("reload").click(); await page.pump(); assert.equal(page.notice().className, "notice warn"); assert.match(page.notice().textContent, /通话进行中/);
  page.state.postHandler = () => { throw new Error("Request failed with status code 500 SECRET"); };
  page.el("reload").click(); await page.pump(); assert.equal(page.notice().className, "notice error"); assert.match(page.notice().textContent, /AstrBot 插件管理里重载/); assert.ok(!page.notice().textContent.includes("SECRET"));
  assert.equal(page.el("reload").disabled, false, "reload can be tried again");
}
{
  const hinted = clone(fixture); hinted.reload_required = true; hinted.reload_hint = "请在 AstrBot 插件管理里重载 Bot-Call";
  const page = boot(hinted); await page.settle();
  assert.equal(page.el("reload-bar").hidden, false); assert.equal(page.el("reload").hidden, true); assert.equal(page.el("reload-hint").textContent, "请在 AstrBot 插件管理里重载 Bot-Call");
  const quiet = clone(fixture); quiet.reload_hint = "请在 AstrBot 插件管理里重载 Bot-Call"; const page2 = boot(quiet); await page2.settle();
  assert.equal(page2.el("reload-bar").hidden, false); assert.equal(page2.el("reload").hidden, true);
}

// ---- server-side validation failure keeps everything the user typed -----------------------------------------------
{
  const page = boot(); await page.settle();
  page.type("public_origin", "ftp://call.example.test"); page.type("call_reply_style", "不会丢");
  page.state.postHandler = () => { throw new Error('bot_call.settings_invalid:{"errors":{"public_origin":"必须以 https:// 开头","cloudflared_path":"路径不存在","not_a_field":"x"}}'); };
  page.el("save").click(); await page.settle();
  assert.equal(page.error("public_origin").textContent, "必须以 https:// 开头"); assert.equal(page.error("public_origin").hidden, false);
  assert.equal(page.error("cloudflared_path").textContent, "路径不存在", "an unchanged field may be named by the server too");
  assert.equal(page.input("public_origin").value, "ftp://call.example.test"); assert.equal(page.input("call_reply_style").value, "不会丢");
  assert.equal(page.notice().className, "notice error"); assert.match(page.notice().textContent, /2 项没通过校验，没有保存任何内容/);
  assert.equal(page.el("dirty").textContent, "有 2 项未保存"); assert.equal(page.el("save").disabled, false, "the server's verdict is not final: the user may retry after fixing");
  page.type("public_origin", "https://call.example.test"); assert.equal(page.error("public_origin").hidden, true); assert.equal(page.error("cloudflared_path").hidden, false);
  page.type("cloudflared_path", "/usr/local/bin/cloudflared"); assert.equal(page.el("save").disabled, false);
  // No field list available: say so plainly instead of guessing.
  page.state.postHandler = () => { throw new Error("bot_call.settings_invalid"); };
  page.el("save").click(); await page.settle();
  assert.match(page.notice().textContent, /没有保存任何内容.*没有说明具体是哪一项/); assert.equal(page.input("call_reply_style").value, "不会丢");
  // A bridge that hands the errors back as a normal result is understood as well.
  page.state.postHandler = () => ({ saved: [], errors: { call_reply_style: "太长了" } });
  page.el("save").click(); await page.settle(); assert.equal(page.error("call_reply_style").textContent, "太长了"); assert.equal(page.input("call_reply_style").value, "不会丢");
}

// ---- conflicts ----------------------------------------------------------------------------------------------------------
{
  const page = boot(); await page.settle();
  page.type("call_vad_silence_ms", "1500"); page.type("call_vad_sensitivity", "55");
  page.state.postHandler = () => { throw new Error("bot_call.settings_conflict"); };
  page.el("save").click(); await page.settle();
  assert.equal(page.notice().className, "notice warn"); assert.match(page.notice().textContent, /别处被修改过/); assert.match(page.notice().textContent, /还没有保存/);
  assert.equal(page.input("call_vad_silence_ms").value, "1500"); assert.equal(page.el("dirty").textContent, "有 2 项未保存");
  const button = all(page.notice(), node => node.tagName === "BUTTON")[0]; assert.match(button.textContent, /载入最新配置/);
  page.state.settings.revision = "9999888877776666"; editField(page.state.settings, "call_vad_sensitivity", field => { field.value = 80; }); editField(page.state.settings, "call_history_turns", field => { field.value = 12; });
  button.click(); await page.settle();
  assert.equal(page.input("call_history_turns").value, "12", "someone else's change is shown");
  assert.equal(page.input("call_vad_silence_ms").value, "1500", "my edit is kept"); assert.equal(page.input("call_vad_sensitivity").value, "55", "my edit wins on the same key");
  assert.equal(page.el("dirty").textContent, "有 2 项未保存"); assert.match(page.notice().textContent, /请核对后再保存/);
  okPost(page); page.el("save").click(); await page.settle();
  assert.deepEqual(page.state.posts.at(-1), ["settings", { revision: "9999888877776666", values: { call_vad_silence_ms: 1500, call_vad_sensitivity: 55 } }]);
}

// ---- network / unknown failures never leak raw text and never drop edits --------------------------------------------------------
{
  const page = boot(); await page.settle(); page.type("call_vad_silence_ms", "1500");
  page.state.postHandler = () => { throw new Error("Network Error: https://secret.example.test/api?token=SYNTHETIC"); };
  page.el("save").click(); await page.settle();
  assert.equal(page.notice().className, "notice error"); assert.match(page.notice().textContent, /保存失败.*还在/);
  assert.ok(!page.notice().textContent.includes("SYNTHETIC") && !page.notice().textContent.includes("secret.example"));
  assert.equal(page.input("call_vad_silence_ms").value, "1500"); assert.equal(page.el("save").disabled, false, "can try again");
  assert.equal(page.input("call_vad_silence_ms").disabled, false);
}

// ---- read-only mode -----------------------------------------------------------------------------------------------------------------
{
  const locked = clone(fixture); locked.writable = false; locked.readonly_reason = "当前 AstrBot 版本不支持从插件页保存配置。";
  const page = boot(locked); await page.settle();
  assert.equal(page.el("readonly").hidden, false); assert.match(page.el("readonly").textContent, /只能查看.*当前 AstrBot 版本不支持/);
  assert.equal(page.el("actions").hidden, true);
  const controls = all(page.el("groups"), node => ["INPUT", "TEXTAREA", "SELECT"].includes(node.tagName));
  assert.ok(controls.length > 20 && controls.every(node => node.disabled), "every control is read-only");
  assert.ok(all(page.el("groups"), node => node.className.includes("restore")).every(node => node.disabled));
  assert.equal(page.input("call_vad_silence_ms").value, "1200");
  const bare = clone(fixture); bare.writable = false; const page2 = boot(bare); await page2.settle();
  assert.match(page2.el("readonly").textContent, /AstrBot 的插件配置页修改/);
}

// ---- leaving with unsaved changes, refresh and discard ------------------------------------------------------------------------------------
{
  const page = boot(); await page.settle();
  const prevented = []; const leave = () => { const event = { preventDefault() { prevented.push(1); }, returnValue: undefined }; const result = page.listeners.beforeunload(event); return { result, event }; };
  assert.equal(leave().result, undefined); assert.equal(prevented.length, 0, "clean page leaves silently");
  page.type("call_vad_silence_ms", "1500");
  const { result, event } = leave(); assert.equal(prevented.length, 1); assert.match(String(result), /未保存/); assert.match(String(event.returnValue), /未保存/);
  // Refresh asks once before throwing edits away.
  const reads = page.state.gets; page.el("refresh").click(); assert.equal(page.state.gets, reads); assert.match(page.el("refresh").textContent, /放弃未保存修改并刷新/);
  page.el("refresh").click(); await page.settle(); assert.equal(page.state.gets, reads + 1); assert.equal(page.input("call_vad_silence_ms").value, "1200"); assert.equal(page.el("refresh").textContent, "刷新");
  page.type("call_vad_silence_ms", "1500"); page.type("listen_port", "7000"); page.el("discard").click();
  assert.equal(page.input("call_vad_silence_ms").value, "1200"); assert.equal(page.input("listen_port").value, "6191"); assert.equal(page.el("dirty").textContent, "没有未保存的修改");
  assert.equal(leave().result, undefined);
  page.type("call_vad_silence_ms", "1500"); page.el("refresh").click(); page.type("call_vad_silence_ms", "1200"); assert.equal(page.el("refresh").textContent, "刷新", "the confirmation resets when nothing is unsaved");
}

// ---- hostile text, unexpected data, no bridge ------------------------------------------------------------------------------------------------
{
  const odd = clone(fixture); odd.version = "<img src=x onerror=alert(1)>"; editField(odd, "enabled", field => { field.label = "<b>启用</b>"; field.help = "<script>alert(1)</script>"; });
  editField(odd, "review_style", field => { field.choices[1].label = "<i>要点</i>"; });
  const page = boot(odd); await page.settle();
  assert.equal(page.el("version").textContent, "v<img src=x onerror=alert(1)>"); assert.match(page.row("enabled").textContent, /<b>启用<\/b>/); assert.match(page.row("enabled").textContent, /<script>/);
  assert.match(page.row("review_style").textContent, /<i>要点<\/i>/);
}
{
  const failing = boot(); failing.state.getError = new Error("SYNTHETIC_SECRET");
  await failing.settle(); assert.equal(failing.notice().className, "notice error"); assert.match(failing.notice().textContent, /读取设置失败/); assert.ok(!failing.notice().textContent.includes("SYNTHETIC"));
  assert.equal(failing.el("groups").children.length, 0); assert.equal(failing.el("actions").hidden, true); assert.equal(failing.el("loading").hidden, true);
  failing.state.getError = null; failing.el("refresh").click(); await failing.settle(); assert.equal(failing.el("groups").children.length, 4); assert.equal(failing.notice().hidden, true);
  const shapeless = boot(); shapeless.state.settings = { version: "x" }; shapeless.el("refresh").click(); await shapeless.settle();
  assert.equal(shapeless.notice().className, "notice error");
}
{
  const page = boot(fixture, { bridge: false }); await page.settle();
  assert.equal(page.el("refresh").disabled, true); assert.match(page.notice().textContent, /AstrBot 插件页面/); assert.equal(page.el("loading").hidden, true);
}
// ---- sidebar of categories, one card stack at a time -------------------------------------------------------------------------------
{
  const page = boot(); await page.settle();
  const nav = page.el("nav").children, cats = page.el("groups").children, title = () => page.el("category-title").textContent;
  assert.equal(nav.length, 4); assert.deepEqual(nav.map(button => button.id), ["nav-access", "nav-conversation", "nav-audio", "nav-review"]);
  assert.ok(nav.every(button => button.tagName === "BUTTON" && button.type === "button" && button.children[0].tagName.toLowerCase() === "svg" && button.children[0].children[0].attributes.d), "every entry has an inline svg icon");
  assert.equal(new Set(nav.map(button => button.children[0].children[0].attributes.d)).size, 4, "plug/chat/mic/note are four different icons");
  assert.equal(title(), "接入"); assert.deepEqual(cats.map(category => category.hidden), [false, true, true, true]);
  assert.match(nav[0].className, /active/); assert.equal(nav[0].attributes["aria-current"], "page"); assert.equal(nav[1].attributes["aria-current"], "false");
  page.el("content").scrollTop = 500; nav[2].click();
  assert.equal(title(), "语音与断句"); assert.deepEqual(cats.map(category => category.hidden), [true, true, false, true]); assert.match(nav[2].className, /active/); assert.doesNotMatch(nav[0].className, /active/);
  assert.equal(page.el("content").scrollTop, 0, "switching starts at the top of the new category");
  // The choice survives a refresh, and every category stays rendered so edits in hidden ones are never lost.
  page.type("call_history_turns", "10"); assert.equal(page.el("dirty").textContent, "有 1 项未保存");
  nav[3].click(); page.el("refresh").click(); page.el("refresh").click(); await page.settle();
  assert.equal(title(), "回顾"); assert.deepEqual(page.el("groups").children.map(category => category.hidden), [true, true, true, false]);
  page.type("call_history_turns", "10"); nav[3].click(); assert.equal(page.input("call_history_turns").value, "10");
  // Markers on the entries: unsaved edits and errors are visible from any category.
  const dot = index => page.el("nav").children[index].children[2];
  assert.equal(dot(1).hidden, false); assert.equal(dot(1).className, "dot"); assert.equal(dot(0).hidden, true); assert.match(page.el("nav").children[1].attributes["aria-label"], /有未保存修改/);
  page.type("call_history_turns", "999"); assert.equal(dot(1).className, "dot bad"); assert.match(page.el("nav").children[1].attributes["aria-label"], /有错误/);
  page.type("call_history_turns", "8"); assert.equal(dot(1).hidden, true);
  // A server error in another category takes the user there.
  page.type("public_origin", "ftp://x"); nav[3].click(); assert.equal(title(), "回顾");
  page.state.postHandler = () => { throw new Error('bot_call.settings_invalid:{"errors":{"public_origin":"必须以 https:// 开头"}}'); };
  page.el("save").click(); await page.settle();
  assert.equal(title(), "接入"); assert.equal(page.el("groups").children[0].hidden, false); assert.equal(page.row("public_origin").scrolled, true); assert.equal(dot(0).className, "dot bad");
}

// ---- STT uses a compact select even with only two choices -----------------------------------------------
{
  const page = boot(); await page.settle(); okPost(page);
  const key = "call_stt_provider_id";
  const select = page.input(key);
  assert.ok(select, "STT renders a select with few choices");
  assert.equal(select.tagName, "SELECT");
  assert.equal(select.id, "f-call_stt_provider_id");
  assert.deepEqual(select.children.map(option => option.value), ["", "your-stt-provider-id"]);
  assert.equal(select.value, "");
  const label = all(page.row(key), node => node.tagName === "LABEL" && node.className === "field-label")[0];
  assert.equal(label.attributes.for, "f-call_stt_provider_id");

  select.value = "your-stt-provider-id"; select.fire("change");
  assert.equal(page.el("dirty").textContent, "有 1 项未保存");
  page.el("save").click(); await page.settle();
  assert.deepEqual(page.state.posts, [
    ["settings", { revision: "a1b2c3d4e5f60718", values: { [key]: "your-stt-provider-id" } }],
  ]);
  assert.equal(page.input(key).value, "your-stt-provider-id");
  assert.equal(page.el("save").disabled, true);

  assert.equal(page.restore(key).disabled, false);
  page.restore(key).click();
  assert.equal(page.input(key).value, "");
  assert.equal(page.el("dirty").textContent, "有 1 项未保存");
  page.el("save").click(); await page.settle();
  assert.deepEqual(page.state.posts[1], ["settings", { revision: "ffff0000ffff0000", values: { [key]: "" } }]);
  assert.equal(page.input(key).value, "");
  assert.equal(page.restore(key).disabled, true);
  assert.equal(page.el("dirty").textContent, "没有未保存的修改");
}

// ---- rows: name and note on the left, control on the right; long controls underneath ------------------------------------------------------
{
  const page = boot(); await page.settle();
  const wide = key => /\bwide\b/.test(page.row(key).className);
  for (const key of ["call_reply_style", "call_skip_plugin_hooks", "public_origin", "cloudflared_path", "review_style", "review_voice", "access_mode", "review_instructions"]) assert.equal(wide(key), true, key + " takes a whole row");
  for (const key of ["enabled", "call_history_summary", "call_vad_silence_ms", "listen_port", "call_chat_provider_id", "call_stt_provider_id", "call_tts_provider_id", "max_audio_bytes"]) assert.equal(wide(key), false, key + " sits to the right");
  // Default, timing, restore and the unsaved mark are small print inside the row, after the note.
  const row = page.row("call_vad_silence_ms"), info = row.children[0];
  assert.equal(row.children[1].className, "control"); assert.deepEqual(info.children.map(node => node.className), ["field-head", "help", "meta"]);
  assert.match(info.children[2].textContent, /默认：1200.*下一通电话生效.*恢复默认/);
  page.type("call_vad_silence_ms", "100"); assert.match(page.row("call_vad_silence_ms").className, /^field invalid dirty$/);
  assert.equal(page.row("call_reply_style").className, "field wide"); page.type("call_reply_style", "x"); assert.equal(page.row("call_reply_style").className, "field wide dirty", "layout class survives state changes");
}

// ---- older responses without cards are still shown (one card per category) ------------------------------------------------------------------------
{
  const legacy = clone(fixture); for (const group of legacy.groups) { delete group.sections; delete group.icon; }
  const page = boot(legacy); await page.settle();
  assert.equal(page.el("groups").children.length, 4); assert.deepEqual(page.el("groups").children.map(category => category.children.length), [1, 1, 1, 1]);
  assert.equal(page.el("groups").children[0].children[0].children[0].textContent, "接入"); assert.equal(all(page.el("groups"), node => node.attributes["data-key"]).length, 25);
  assert.ok(page.el("nav").children.every(button => button.children[0].children[0].attributes.d), "a missing icon falls back to a default one");
  okPost(page); page.type("call_vad_silence_ms", "1500"); page.el("save").click(); await page.settle();
  assert.deepEqual(page.state.posts[0], ["settings", { revision: "a1b2c3d4e5f60718", values: { call_vad_silence_ms: 1500 } }]);
  // Categories that only have the flat list work as well, and an unknown icon name is harmless.
  const flat = clone(fixture); flat.groups.forEach((group, index) => { delete group.sections; group.icon = index ? "no-such-icon" : "plug"; });
  const page2 = boot(flat); await page2.settle(); assert.equal(all(page2.el("groups"), node => node.attributes["data-key"]).length, 25);
}

// ---- layout rules: fixed sidebar, scrolling content, top strip on narrow screens -----------------------------------------------------------------
{
  const rule = (selector, media = "") => { const scope = media ? css.slice(css.indexOf(media)) : css.slice(0, css.indexOf("@media")); const at = scope.indexOf(selector + " {"); assert.ok(at >= 0, "css rule missing: " + selector); return scope.slice(at, scope.indexOf("}", at)); };
  assert.match(rule("html, body"), /height: 100%/); assert.match(css, /body \{ margin: 0; overflow: hidden; \}/);
  assert.match(rule(".shell"), /grid-template-columns: 200px/); assert.match(rule(".shell"), /height: 100vh/);
  assert.match(rule(".side"), /overflow-y: auto/); assert.match(rule(".content"), /overflow-y: auto/); assert.match(rule(".actions"), /position: sticky/); assert.match(rule(".actions"), /bottom: 0/);
  assert.match(rule(".nav-item.active::before"), /width: 3px/);
  const narrow = "@media (max-width: 640px)"; assert.ok(css.includes(narrow));
  assert.match(rule(".shell", narrow), /grid-template-rows: auto/); assert.match(rule(".side", narrow), /overflow-x: auto/); assert.match(rule("#nav", narrow).length ? css.slice(css.indexOf(narrow)) : "", /#nav \{ display: flex/);
  assert.match(rule(".nav-item", narrow), /white-space: nowrap/);
  assert.ok(!/https?:\/\//.test(css) && !/url\(/.test(css), "no external or remote resource");
  assert.match(css, /html\[data-theme="dark"\]/);
}
console.log("PASS control panel: all field types, validation, changed-keys-only save, invalid/conflict/network errors, read-only, reload, unsaved guard, safe rendering");
