// shell.js — the console's chrome: theme, toasts, copy, confirm dialog, command
// palette, keyboard shortcuts, the message inspector, the docked composer and live
// polling. Chrome elements live outside #workspace, so they survive every
// content swap; anything inside it is looked up when it is needed, never held.
(function () {
  "use strict";
  var root = document.documentElement;
  var PREFIX = (document.querySelector('link[rel="icon"]') || {}).href
    ? new URL(document.querySelector('link[rel="icon"]').href).pathname.replace(/\/static\/favicon\.svg$/, "")
    : "";
  function stored(key) { try { return localStorage.getItem(key); } catch (e) { return null; } }
  function store(key, value) {
    try { if (value == null) localStorage.removeItem(key); else localStorage.setItem(key, value); } catch (e) {}
  }

  // ---------- Alpine and editors after a swap ----------
  document.addEventListener("htmx:afterSwap", function () {
    // For an outerHTML swap e.detail.target is the old, detached node, so both
    // upgrades run document-wide; both skip what is already set up.
    if (window.Alpine) window.Alpine.initTree(document.body);
    if (window.dozeEditor) window.dozeEditor.upgradeAll();
    resetListCursor();
  });

  // ---------- theme: light / dark / high contrast / system ----------
  // The stylesheet knows three themes. "System" is this script choosing between
  // them from the OS settings (dark, or a request for more contrast), and
  // following them when they change; it is stored as the absence of a choice.
  // (The same resolution runs inline in <head>, before first paint.)
  var themeSeg = document.getElementById("theme-seg");
  var darkMQ = window.matchMedia ? window.matchMedia("(prefers-color-scheme: dark)") : null;
  var contrastMQ = window.matchMedia ? window.matchMedia("(prefers-contrast: more)") : null;
  function themeMode() {
    var t = stored("theme");
    return t === "light" || t === "dark" || t === "contrast" ? t : "system";
  }
  function applyTheme(mode) {
    var system = contrastMQ && contrastMQ.matches ? "contrast" : (darkMQ && darkMQ.matches ? "dark" : "light");
    root.setAttribute("data-theme", mode === "system" ? system : mode);
    store("theme", mode === "system" ? null : mode);
    if (themeSeg) themeSeg.querySelectorAll("button").forEach(function (b) {
      var on = b.getAttribute("data-mode") === mode;
      b.classList.toggle("on", on);
      b.setAttribute("aria-pressed", on ? "true" : "false");
    });
  }
  applyTheme(themeMode());
  if (themeSeg) themeSeg.addEventListener("click", function (e) {
    var b = e.target.closest("button[data-mode]");
    if (b) applyTheme(b.getAttribute("data-mode"));
  });
  [darkMQ, contrastMQ].forEach(function (mq) {
    if (mq && mq.addEventListener) mq.addEventListener("change", function () {
      if (themeMode() === "system") applyTheme("system");
    });
  });

  // ---------- the rail's numbers ----------
  // The counts beside Traffic, Topics, Groups and Schemas, refreshed gently. The
  // server draws them with the page; this keeps them current. An item that has no
  // number (the broker did not answer) keeps whatever it was showing.
  function refreshCounts() {
    if (document.hidden) return;
    fetch(PREFIX + "/api/counts").then(function (r) { return r.json(); }).then(function (counts) {
      document.querySelectorAll(".rail [data-ct]").forEach(function (el) {
        var n = counts[el.getAttribute("data-ct")];
        if (n == null) return;
        el.textContent = String(n) + (el.getAttribute("data-ct-unit") || "");
      });
    }).catch(function () {});
  }
  refreshCounts();
  setInterval(refreshCounts, 5000);

  // ---------- the traces you opened ----------
  // Kept in this tab only (sessionStorage): the ids of the traces followed, newest
  // first, with how many messages each had once its scan finished. The Trace page
  // lists them (Tc-Home's list).
  function traceRecents() {
    try { return JSON.parse(sessionStorage.getItem("traces") || "[]") || []; } catch (e) { return []; }
  }
  function rememberTrace() {
    var page = document.querySelector("[data-trace-id]");
    if (!page) return;
    var id = page.getAttribute("data-trace-id"), live = document.getElementById("trace-live");
    var hits = live && live.hasAttribute("data-hits") ? live.getAttribute("data-hits") : null;
    try {
      var all = traceRecents(), prev = all.filter(function (x) { return x.id === id; })[0];
      all = all.filter(function (x) { return x.id !== id; });
      all.unshift({ id: id, n: hits != null ? hits : (prev ? prev.n : null) });
      sessionStorage.setItem("traces", JSON.stringify(all.slice(0, 12)));
    } catch (e) {}
  }
  function renderTraceRecents() {
    var box = document.querySelector("[data-trace-recents]");
    if (!box) return;
    box.textContent = "";
    traceRecents().forEach(function (t) {
      var a = document.createElement("a");
      a.className = "li";
      a.href = PREFIX + "/trace?id=" + encodeURIComponent(t.id);
      var name = document.createElement("span");
      name.className = "m grow el";
      name.textContent = t.id.length > 18 ? t.id.slice(0, 8) + "\u2026" + t.id.slice(-7) : t.id;
      name.title = t.id;
      a.appendChild(name);
      if (t.n != null) { var n = document.createElement("span"); n.className = "m mut"; n.textContent = t.n; a.appendChild(n); }
      box.appendChild(a);
    });
  }
  function traceBookkeeping() { rememberTrace(); renderTraceRecents(); }
  document.addEventListener("DOMContentLoaded", traceBookkeeping);
  document.addEventListener("htmx:afterSettle", traceBookkeeping);

  // ---------- toasts ----------
  function toast(msg, kind) {
    var box = document.getElementById("toasts");
    if (!box) return;
    var el = document.createElement("div");
    el.className = "toast" + (kind === "err" ? " err" : "");
    // Errors interrupt a screen reader (alert); confirmations wait their turn (status).
    el.setAttribute("role", kind === "err" ? "alert" : "status");
    el.innerHTML = '<span class="t-ic" aria-hidden="true">' + (kind === "err" ? "⚠" : "✓") + '</span><span></span><button type="button" class="tclose" aria-label="Dismiss">✕</button>';
    el.children[1].textContent = msg;
    el.querySelector(".tclose").onclick = function () { el.remove(); };
    box.appendChild(el);
    setTimeout(function () { el.remove(); }, kind === "err" ? 6000 : 3200);
  }
  window.addEventListener("toast", function (e) { toast(e.detail.value !== undefined ? e.detail.value : e.detail, "ok"); });
  window.addEventListener("toast-error", function (e) { toast(e.detail.value !== undefined ? e.detail.value : e.detail, "err"); });
  // The text of an error response. The server HTML-escapes error bodies, so decode
  // the entities to show real quotes and brackets rather than "&#34;".
  function errorText(xhr) {
    var raw = (xhr && xhr.responseText) || "Request failed";
    var ta = document.createElement("textarea");
    ta.innerHTML = raw;
    var msg = ta.value.replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
    return msg || "Request failed";
  }
  // Is this event about a live region's own poll? A failing poll must not toast
  // every few seconds; it gets one persistent state on the live control instead.
  // The live regions a poll's path belongs to.
  function liveRegionsFor(path) {
    return Array.prototype.filter.call(document.querySelectorAll("[data-live]"), function (el) {
      var base = el.getAttribute("data-live");
      return !!base && path.indexOf(base) === 0;
    });
  }
  function isLivePoll(e) {
    var path = (e.detail && e.detail.requestConfig && e.detail.requestConfig.path) || "";
    // Any live region's own poll counts: a page can have several (the tape and
    // the wire dock), and a failing poll of any of them is "not updating", once.
    return liveRegionsFor(path).length > 0;
  }
  function pollPath(e) { return (e.detail && e.detail.requestConfig && e.detail.requestConfig.path) || ""; }
  function isInspectorRequest(e) { return !!(e.detail && e.detail.target && e.detail.target.id === "inspector"); }
  // The console when the broker is not there. One persistent state, not a toast
  // per poll: the live pill says "Not connected" with the last time anything was
  // heard, the regions whose poll failed are frozen under a muted overlay, and one
  // Retry asks again at once. Polling carries on behind it, so it ends by itself
  // (no reload) when the broker comes back.
  var lastGood = Date.now();
  function clockText(ms) {
    return window.dozeTime ? window.dozeTime.clock(ms) : new Date(ms).toLocaleTimeString();
  }
  function renderPill() {
    var pill = document.getElementById("live-pill");
    var down = document.querySelectorAll("[data-live].disconnected").length > 0;
    document.documentElement.toggleAttribute("data-connection-lost", down);
    if (!pill) return;
    pill.classList.toggle("disconnected", down);
    var st = pill.querySelector(".pill-state"), retry = document.getElementById("pill-retry");
    if (st) {
      st.textContent = down ? "Not connected · last heard " + clockText(lastGood) : "";
      st.hidden = !down;
    }
    if (retry) retry.hidden = !down;
    pill.setAttribute("aria-live", "polite");
  }
  // setDisconnected marks the regions whose poll failed (or all of them, with no
  // path), once: a failing poll repeats every few seconds and says nothing new.
  function setDisconnected(on, why, path) {
    var regions = path ? liveRegionsFor(path) : Array.prototype.slice.call(document.querySelectorAll("[data-live]"));
    regions.forEach(function (el) {
      el.classList.toggle("disconnected", on);
      var b = el.querySelector("[data-live-toggle]");
      if (b) b.title = on ? "Not updating: " + (why || "can't reach the broker") : (livePaused ? "Resume live updates" : "Pause live updates");
    });
    if (!on && regions.length) lastGood = Date.now();
    renderPill();
  }
  document.addEventListener("click", function (e) {
    if (!e.target.closest("#pill-retry") || !window.htmx) return;
    // One retry: every frozen region asks now rather than at its next tick.
    document.querySelectorAll("[data-live].disconnected").forEach(function (el) {
      var url = el.getAttribute("data-live");
      if (url && el.id) htmx.ajax("GET", url + (url.indexOf("?") >= 0 ? "&" : "?") + "h=", { target: "#" + el.id, swap: "outerHTML" }).catch(function () {});
    });
  });
  // An error response is shown where the person is looking: in the failed form's
  // error slot if it has one, else a toast. (The inspector reports its own; see below.)
  document.addEventListener("htmx:responseError", function (e) {
    var msg = errorText(e.detail.xhr);
    if (isLivePoll(e)) { setDisconnected(true, msg, pollPath(e)); return; }
    if (isInspectorRequest(e)) return;
    var elt = e.detail.elt;
    var form = elt && elt.closest ? elt.closest("form") : null;
    var slot = form && form.querySelector("[data-err-slot]");
    if (slot) { slot.textContent = msg; slot.hidden = false; return; }
    toast(msg.length > 140 ? msg.slice(0, 140) + "…" : msg, "err");
  });
  // Nothing came back at all (the console server itself is gone).
  function networkFailure(e) {
    if (isLivePoll(e)) { setDisconnected(true, "the console server did not answer", pollPath(e)); return; }
    if (isInspectorRequest(e)) return;
    toast("No answer from the console — is doze-kafka still running?", "err");
  }
  document.addEventListener("htmx:sendError", networkFailure);
  document.addEventListener("htmx:timeout", networkFailure);

  // htmx does not swap a 4xx/5xx response. A page the server rendered for a
  // navigation (not found, broker unreachable) is a page to show, and says so.
  document.addEventListener("htmx:beforeSwap", function (e) {
    var xhr = e.detail.xhr;
    if (xhr && xhr.getResponseHeader("X-Doze-Page") === "error") {
      e.detail.shouldSwap = true;
      e.detail.isError = false;
    }
  });

  // ---------- request feedback ----------
  // Controls lock while their request is in flight (hx-disabled-elt), a slow
  // request shows a thin progress bar after half a second, and a form's old error
  // clears when it is resubmitted.
  //
  // The end of a request is taken from the XMLHttpRequest itself, not from
  // htmx:afterRequest: htmx raises that on the element that asked, and when the
  // response replaces that element (the inspector's Older and Newer buttons replace
  // themselves) the event has nowhere to bubble from, and the bar stayed on.
  var slowTimer = null, inflight = 0;
  document.addEventListener("htmx:beforeRequest", function (e) {
    if (isLivePoll(e)) return;
    var elt = e.detail.elt;
    var form = elt && elt.closest ? elt.closest("form") : null;
    var slot = form && form.querySelector("[data-err-slot]");
    if (slot) { slot.hidden = true; slot.textContent = ""; }
    var bar = document.getElementById("reqbar");
    inflight++;
    if (!slowTimer && bar) slowTimer = setTimeout(function () { bar.hidden = false; }, 500);
    var xhr = e.detail.xhr;
    if (isInspectorRequest(e)) inspectorRequestStarted(xhr, elt);
    xhr.addEventListener("loadend", function () {
      inflight = Math.max(0, inflight - 1);
      if (inflight === 0) {
        clearTimeout(slowTimer); slowTimer = null;
        if (bar) bar.hidden = true;
      }
    });
  });

  // ---------- copy ----------
  // navigator.clipboard exists only on a secure origin. http://127.0.0.1 is one;
  // http://kafka.doze is not, and there the old way still works.
  function copyText(text) {
    if (navigator.clipboard && window.isSecureContext) return navigator.clipboard.writeText(text);
    return new Promise(function (resolve, reject) {
      var ta = document.createElement("textarea");
      ta.value = text;
      ta.setAttribute("readonly", "");
      ta.className = "sr-only";
      document.body.appendChild(ta);
      var back = document.activeElement;
      ta.select();
      var ok = false;
      try { ok = document.execCommand("copy"); } catch (e) {}
      ta.remove();
      if (back && back.focus) back.focus();
      if (ok) resolve(); else reject(new Error("copy refused"));
    });
  }
  document.addEventListener("click", function (e) {
    var el = e.target.closest("[data-copy]");
    if (!el) return;
    copyText(el.getAttribute("data-copy")).then(
      function () { toast("Copied"); },
      function () { toast("This browser would not copy that; select the text and copy it by hand.", "err"); });
  });

  // ---------- "Edit a copy" (inspector → composer) ----------
  // Prefills the composer from a record in the inspector: key, value, headers,
  // partition, and how each is encoded, for changing before sending. ("Send
  // again" is a different thing: the server re-produces the stored record
  // exactly, which a form cannot promise.)
  document.addEventListener("click", function (e) {
    var b = e.target.closest("#inspector-edit");
    if (!b) return;
    closeInspector(false);
    var panel = document.getElementById("compose-panel");
    if (!panel) return;
    setDock(true);
    var form = panel.querySelector("form");
    var st = window.Alpine ? Alpine.$data(form) : null;
    var key = form.querySelector('input[name="key"]');
    if (key) key.value = b.getAttribute("data-key") || "";
    var sel = form.querySelector('select[name="partition"]');
    if (sel) sel.value = b.getAttribute("data-partition");
    // A record read through the registry is edited as JSON and sent encoded again.
    var sp = form.querySelector('select[name="schema_subject"]');
    if (sp) sp.value = b.getAttribute("data-schema") || "";
    var ta = form.querySelector('textarea[name="value"]');
    var val = b.getAttribute("data-value") || "";
    if (ta && window.dozeEditor) dozeEditor.set(ta, val); else if (ta) ta.value = val;
    if (st) {
      st.keyB64 = b.getAttribute("data-key-enc") === "base64";
      st.valB64 = b.getAttribute("data-value-enc") === "base64";
      st.tomb = b.getAttribute("data-tombstone") === "1";
      try {
        st.hdrs = JSON.parse(b.getAttribute("data-headers") || "[]").map(function (r) {
          return { k: r.k, v: r.v, b64: !!r.b64 };
        });
      } catch (err) { st.hdrs = []; }
    }
    focusComposer();
  });

  // ---------- dialogs ----------
  // Three things open over the page: the confirm dialog, the command palette and
  // the key help. Only one is open at a time, and while one is, the page's
  // single-key shortcuts are off. (The inspector is not one of them: it is part of
  // the page, and its keys are the page's.)
  var FOCUSABLE = 'button:not([disabled]), a[href], input:not([disabled]):not([type="hidden"]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';
  function isOpen(id) { var el = document.getElementById(id); return !!el && !el.hidden; }
  function confirmOpen() { return isOpen("confirm"); }
  function paletteOpen() { return isOpen("palette"); }
  function inspectorIsOpen() { return isOpen("inspector"); }
  function helpOpen() { return isOpen("help"); }
  // Tab stays inside an open dialog.
  function trapTab(box, ev) {
    if (ev.key !== "Tab") return;
    var f = Array.prototype.filter.call(box.querySelectorAll(FOCUSABLE), function (el) { return el.offsetParent !== null; });
    if (!f.length) { ev.preventDefault(); return; }
    var first = f[0], last = f[f.length - 1];
    if (!box.contains(document.activeElement)) { first.focus(); ev.preventDefault(); }
    else if (ev.shiftKey && document.activeElement === first) { last.focus(); ev.preventDefault(); }
    else if (!ev.shiftKey && document.activeElement === last) { first.focus(); ev.preventDefault(); }
  }

  // ---------- styled confirm (intercepts hx-confirm) ----------
  // The title and the confirming verb come from the button that asked
  // (data-confirm-title / data-confirm-verb), so the dialog says "Delete topic",
  // not "Are you sure? / Confirm". Focus starts on Cancel: Enter on a destructive
  // dialog must not be the dangerous answer.
  // The sentence is measured when the button is pressed (data-confirm-url): the
  // one drawn into the page is as old as the page, and a topic gains records. The
  // drawn one is the fallback when the console cannot answer.
  var confirmPending = false;
  document.addEventListener("htmx:confirm", function (e) {
    var box = document.getElementById("confirm");
    var q = e.detail.question;
    if (!q || !box) return;
    e.preventDefault();
    if (confirmOpen() || confirmPending) return; // one question at a time.
    var elt = e.detail.elt || {}, data = elt.dataset || {};
    var opener = document.activeElement;
    if (data.confirmUrl) {
      confirmPending = true;
      fetch(data.confirmUrl, { headers: { Accept: "application/json" } })
        .then(function (r) { return r.ok ? r.json() : null; })
        .catch(function () { return null; })
        .then(function (fresh) {
          confirmPending = false;
          showConfirm(fresh && fresh.text ? fresh.text : q, fresh && fresh.title || data.confirmTitle, fresh && fresh.verb || data.confirmVerb);
        });
      return;
    }
    showConfirm(q, data.confirmTitle, data.confirmVerb);
    function showConfirm(text, title, verb) {
    document.getElementById("confirm-title").textContent = title || "Are you sure?";
    document.getElementById("confirm-msg").textContent = text;
    var yes = document.getElementById("confirm-yes"), no = document.getElementById("confirm-no");
    yes.textContent = verb || "Confirm";
    box.hidden = false;
    function close(restore) {
      box.hidden = true; yes.onclick = no.onclick = box.onclick = null;
      document.removeEventListener("keydown", onKey, true);
      if (restore && opener && document.contains(opener) && opener.focus) opener.focus();
    }
    function onKey(ev) {
      if (ev.key === "Escape") { close(true); ev.preventDefault(); ev.stopPropagation(); return; }
      trapTab(box, ev);
    }
    yes.onclick = function () { close(false); e.detail.issueRequest(true); };
    no.onclick = function () { close(true); };
    box.onclick = function (ev) { if (ev.target === box) close(true); };
    document.addEventListener("keydown", onKey, true);
    no.focus();
    }
  });

  // ---------- command palette (Sy-05) ----------
  // Jump or act: results are grouped under Topics, Groups, Subjects and Actions, each
  // row has what it is beside its name, the part of the name that matched is bold, and
  // the foot has Connect an app and the keys. The sections themselves are one more
  // group ("Go to"), because they are reached by name too.
  var pal = document.getElementById("palette"), palQ = document.getElementById("pal-q"), palList = document.getElementById("pal-list");
  var palItems = [], palSel = 0, palOpener = null, palShown = [];
  var PAL_GROUPS = [["topics", "Topics"], ["groups", "Groups"], ["schemas", "Subjects"], ["actions", "Actions"], ["views", "Go to"]];
  var PAL_CHIP = { topics: ["TP", "c-tp"], groups: ["GR", "c-gr"], schemas: ["SC", "c-sc"] };

  // The palette lists the last few topics and groups you opened first: the one
  // you just left costs ⌘K ⏎ instead of retyping its name.
  function trackVisit() {
    try {
      var path = location.pathname;
      if (path.indexOf(PREFIX + "/") !== 0) return;
      var rest = path.slice(PREFIX.length + 1).split("/");
      if ((rest[0] !== "topics" && rest[0] !== "groups") || !rest[1]) return;
      var rec = JSON.parse(stored("recents") || "[]").filter(function (x) { return x.u !== path; });
      rec.unshift({ s: rest[0], n: decodeURIComponent(rest[1]), u: path });
      store("recents", JSON.stringify(rec.slice(0, 6)));
    } catch (e) {}
  }
  document.addEventListener("DOMContentLoaded", trackVisit);
  document.addEventListener("htmx:pushedIntoHistory", trackVisit);

  function bucketOf(it) {
    if (it.k === "action") return "actions";
    if (it.k === "view") return "views";
    if (it.s === "schemas") return "schemas";
    return it.s;
  }
  // What can be done to the page you are on: the topic's composer, the group's reset.
  function contextActions() {
    var out = [], rest = location.pathname.indexOf(PREFIX + "/") === 0 ? location.pathname.slice(PREFIX.length + 1).split("/") : [];
    if (rest[0] === "topics" && rest[1] && document.getElementById("compose-panel")) {
      out.push({ s: "views", n: "Produce to " + decodeURIComponent(rest[1]), k: "action", a: "produce", h: "p", glyph: "+", ctx: true });
    }
    if (rest[0] === "groups" && rest[1]) {
      out.push({ s: "views", n: "Reset offsets for " + decodeURIComponent(rest[1]), k: "action", u: location.pathname + "?tab=offsets#reset", h: "asks first", glyph: "↺", ctx: true });
    }
    return out;
  }

  function openPalette() {
    if (!pal || confirmOpen()) return;
    if (helpOpen()) closeHelp();
    if (!paletteOpen()) palOpener = document.activeElement;
    pal.hidden = false; palQ.value = ""; palSel = 0;
    var here = location.pathname;
    var recents = [];
    try {
      recents = JSON.parse(stored("recents") || "[]")
        .filter(function (x) { return x.u !== here; }).slice(0, 5)
        .map(function (x) { return { s: x.s, n: x.n, u: x.u, k: "recent" }; });
    } catch (e) {}
    palItems = recents.concat(contextActions());
    renderPal();
    palQ.focus();
    // The catalogue (views, actions, every topic, group and subject) comes from the
    // server, so the palette cannot drift from the console's real routes.
    fetch(PREFIX + "/api/palette").then(function (r) { return r.json(); }).then(function (all) {
      var live = {}, fixed = [], resources = [], detail = {};
      (all || []).forEach(function (x) {
        if (x.u) live[x.u] = 1;
        if (x.d) detail[x.u] = x.d;
        (x.k === "view" || x.k === "action" ? fixed : resources).push(x);
      });
      // A recent that no longer exists (a deleted topic) would be a dead link:
      // drop it here and from storage.
      var pruned = recents.filter(function (x) { return live[x.u]; });
      if (pruned.length !== recents.length) {
        try { store("recents", JSON.stringify(JSON.parse(stored("recents") || "[]").filter(function (x) { return live[x.u]; }))); } catch (e) {}
      }
      pruned.forEach(function (x) { x.d = detail[x.u]; });
      var seen = {};
      pruned.forEach(function (x) { seen[x.u] = 1; });
      palItems = pruned.concat(resources.filter(function (r) { return !seen[r.u]; })).concat(contextActions()).concat(fixed);
      renderPal();
    }).catch(function () {}); // the console is unreachable: recents still work.
  }
  // An entry is an address, or (it.a) something the console does in the browser.
  function goTo(it) {
    if (!it.a) { location.href = it.u; return; }
    closePalette();
    var act = it.a;
    if (act.indexOf("theme:") === 0) applyTheme(act.slice(6));
    else if (act === "pause") { if (document.querySelector("[data-live-toggle]")) setPaused(!livePaused); }
    else if (act === "help") openHelp();
    else if (act === "produce") { setDock(true); focusComposer(); }
  }
  function palFiltered() {
    var q = palQ.value.toLowerCase().trim();
    var rows = palItems.filter(function (it) {
      if (it.ctx && q) return it.n.toLowerCase().indexOf(q) >= 0 || /^(produce|reset)/.test(q);
      return !q || (it.n + " " + it.k).toLowerCase().indexOf(q) >= 0;
    });
    // In groups, in the board's order; recents lead their own group.
    var out = [], per = q ? 6 : 5;
    PAL_GROUPS.forEach(function (g) {
      var inG = rows.filter(function (it) { return bucketOf(it) === g[0]; });
      inG.sort(function (a, b) { return (a.k === "recent" ? 0 : 1) - (b.k === "recent" ? 0 : 1); });
      var seen = {};
      inG.filter(function (it) { var k = it.u || it.n; if (seen[k]) return false; seen[k] = 1; return true; }).slice(0, per).forEach(function (it) { out.push(it); });
    });
    return out;
  }
  function highlight(el, text, q) {
    el.textContent = "";
    var i = q ? text.toLowerCase().indexOf(q) : -1;
    if (i < 0) { el.textContent = text; return; }
    el.appendChild(document.createTextNode(text.slice(0, i)));
    var b = document.createElement("b");
    b.textContent = text.slice(i, i + q.length);
    el.appendChild(b);
    el.appendChild(document.createTextNode(text.slice(i + q.length)));
  }
  function renderPal() {
    var items = palFiltered(), q = palQ.value.toLowerCase().trim();
    palShown = items;
    if (palSel >= items.length) palSel = Math.max(0, items.length - 1);
    palList.innerHTML = "";
    var count = document.getElementById("pal-count");
    if (count) count.textContent = items.length ? items.length + " result" + (items.length === 1 ? "" : "s") : "";
    if (!items.length) { palList.innerHTML = '<div class="pal-empty">Nothing matches</div>'; palQ.removeAttribute("aria-activedescendant"); return; }
    var lastGroup = "";
    items.forEach(function (it, i) {
      var g = bucketOf(it);
      if (g !== lastGroup) {
        lastGroup = g;
        var cap = document.createElement("div");
        cap.className = "cap pal-cap";
        cap.setAttribute("role", "presentation");
        cap.textContent = PAL_GROUPS.filter(function (x) { return x[0] === g; })[0][1];
        palList.appendChild(cap);
      }
      var a = document.createElement("a");
      a.className = "pal-item" + (i === palSel ? " sel" : "");
      a.href = it.a ? "#" : it.u;
      a.addEventListener("click", function (ev) { if (it.a) { ev.preventDefault(); goTo(it); } });
      a.id = "pal-opt-" + i; a.setAttribute("role", "option"); a.setAttribute("aria-selected", i === palSel ? "true" : "false"); a.tabIndex = -1;
      var lead = document.createElement("span");
      if (PAL_CHIP[g]) { lead.className = "sv sm " + PAL_CHIP[g][1]; lead.textContent = PAL_CHIP[g][0]; lead.setAttribute("aria-hidden", "true"); }
      else { lead.className = "m mut pal-glyph"; lead.setAttribute("aria-hidden", "true"); lead.textContent = it.glyph || (it.a ? "·" : g === "actions" ? "+" : "→"); }
      var name = document.createElement("span");
      name.className = "pal-name" + (PAL_CHIP[g] ? " m" : "");
      highlight(name, it.n, q);
      var det = document.createElement("span");
      det.className = "grow note el pal-detail";
      det.textContent = it.d || "";
      a.appendChild(lead); a.appendChild(name); a.appendChild(det);
      // An action's own key (or "asks first"), and on the selected row what Enter does.
      if (it.h) {
        var hint = document.createElement("span");
        hint.className = it.h.length > 3 ? "note" : "kbd";
        hint.textContent = it.h;
        a.appendChild(hint);
      }
      if (i === palSel) {
        var enter = document.createElement("span");
        enter.className = "kbd";
        enter.textContent = "↵";
        a.appendChild(enter);
      }
      a.onmousemove = function () { if (palSel !== i) { palSel = i; renderPal(); } };
      palList.appendChild(a);
    });
    var sel = palList.querySelector(".pal-item.sel");
    if (sel) { sel.scrollIntoView({ block: "nearest" }); palQ.setAttribute("aria-activedescendant", sel.id); }
  }
  function closePalette() {
    if (!paletteOpen()) return;
    pal.hidden = true;
    if (palOpener && document.contains(palOpener) && palOpener.focus) palOpener.focus();
  }
  var palButton = document.getElementById("palette-open");
  if (palButton) palButton.addEventListener("click", openPalette);
  if (palQ) palQ.addEventListener("input", function () { palSel = 0; renderPal(); });
  if (pal) pal.addEventListener("click", function (e) { if (e.target === pal) closePalette(); });

  // ---------- rail active state ----------
  // The rail is outside #workspace, so a boosted navigation does not repaint its
  // server-rendered `.on`. Recompute it from the URL after every navigation. The
  // console's home belongs to whichever section the server says is home (Traffic,
  // or Topics when the broker records no traffic); a create form belongs to the
  // section it creates in.
  function sectionFor(path) {
    if (path.indexOf(PREFIX) !== 0) return "";
    var parts = path.slice(PREFIX.length).replace(/^\/+/, "").split("/");
    var seg = parts[0];
    if (seg === "") return (document.querySelector(".app") || {}).getAttribute ? document.querySelector(".app").getAttribute("data-home") || "topics" : "topics";
    if (seg === "new") return parts[1] === "schema" ? "schemas" : "topics";
    return seg;
  }
  var currentSection = sectionFor(location.pathname), currentPath = location.pathname;
  function afterNavigation() {
    if (window.dozeView) window.dozeView();
    var section = sectionFor(location.pathname);
    document.querySelectorAll(".rail .nav").forEach(function (a) {
      var on = sectionFor(new URL(a.href, location.origin).pathname) === section;
      a.classList.toggle("on", on);
      if (on) a.setAttribute("aria-current", "page"); else a.removeAttribute("aria-current");
    });
    // A filter typed in one section means nothing in the next (topic names do
    // not filter groups), and used to carry over and hide everything.
    if (section !== currentSection) currentSection = section;
    // Pausing, and an open composer, belong to the page they were on. A pause used
    // to follow you to a group page (which has no pause control) and freeze it;
    // an open composer followed you to the next topic and covered its messages.
    if (location.pathname !== currentPath) {
      currentPath = location.pathname;
      setPaused(false);
      setDock(false);
    }
  }
  document.addEventListener("DOMContentLoaded", afterNavigation);
  document.addEventListener("htmx:pushedIntoHistory", afterNavigation);
  window.addEventListener("popstate", afterNavigation);

  // ---------- keyboard ----------
  // The cursor is a row, not an index: filtering changes which rows are visible,
  // and an index into the old list pointed at the wrong (or a hidden) one. j and k
  // move over the page's own rows (a table's, the home table's) and, on a page that
  // has none, over the list.
  var cursorEl = null;
  var ROW_SEL = 'main tr[data-inspector-open], main a.r[href], main [data-row]';
  function visible(el) { return el.offsetParent !== null; }
  function listRows() {
    var rows = Array.prototype.filter.call(document.querySelectorAll(ROW_SEL), visible);
    if (rows.length) return rows;
    return Array.prototype.filter.call(document.querySelectorAll(".list .li[href]"), visible);
  }
  function resetListCursor() { cursorEl = null; }
  function moveCursor(d) {
    var rows = listRows();
    if (!rows.length) return;
    var at = rows.indexOf(cursorEl);
    var next = at < 0 ? (d > 0 ? 0 : rows.length - 1) : Math.min(rows.length - 1, Math.max(0, at + d));
    document.querySelectorAll(".cursor").forEach(function (r) { r.classList.remove("cursor"); });
    cursorEl = rows[next];
    cursorEl.classList.add("cursor");
    cursorEl.scrollIntoView({ block: "nearest" });
  }
  function focusComposer() {
    var panel = document.getElementById("compose-panel");
    var ta = panel && panel.querySelector('textarea[name="value"]');
    requestAnimationFrame(function () {
      if (window.dozeEditor) dozeEditor.refresh(panel);
      if (ta && ta.__cm) ta.__cm.focus(); else if (ta) ta.focus();
    });
  }
  // g, then a letter, goes to a section (Sy-06's "Go to"). The chord is armed for a
  // moment and lapses; any other key disarms it.
  var chord = 0;
  document.addEventListener("keydown", function (e) {
    if (confirmOpen()) return; // the dialog has its own keys, and nothing else gets any.
    var mod = e.metaKey || e.ctrlKey;
    if (paletteOpen()) {
      var items = palFiltered();
      if (e.key === "Tab") { e.preventDefault(); return; } // one control; focus stays on the search box.
      if (e.key === "Escape") { closePalette(); e.preventDefault(); }
      else if (e.key === "ArrowDown") { palSel = Math.min(palSel + 1, items.length - 1); renderPal(); e.preventDefault(); }
      else if (e.key === "ArrowUp") { palSel = Math.max(palSel - 1, 0); renderPal(); e.preventDefault(); }
      else if (e.key === "Enter") { var it = items[palSel]; if (it) goTo(it); e.preventDefault(); }
      return;
    }
    if (mod && !e.altKey && !e.shiftKey && e.key === "k") { e.preventDefault(); openPalette(); return; }
    var active = document.activeElement || document.body;
    // ⌘⏎ sends from the composer, from a plain field or from inside the editor.
    // Once per press: a held key repeats, and each repeat used to send a record.
    if (mod && e.key === "Enter") {
      var form = active.closest ? active.closest("form") : null;
      if (form && form.querySelector("textarea, .cm-editor")) {
        e.preventDefault();
        if (!e.repeat && !form.classList.contains("htmx-request")) form.requestSubmit();
      }
      return;
    }
    // Single-key shortcuts are single keys. With a modifier held the key belongs
    // to the browser or the OS: ⌘C is copy, not "new topic".
    if (mod || e.altKey) return;
    var typing = /^(INPUT|TEXTAREA|SELECT)$/.test(active.tagName) || active.isContentEditable || (active.closest && active.closest(".cm-editor"));
    // Esc is the one key that works from a field: it clears the field's filter, or
    // leaves the field, and only then closes the panel.
    if (typing) {
      if (e.key === "Escape" && active.hasAttribute && active.hasAttribute("data-filter-target") && active.value) {
        active.value = ""; active.dispatchEvent(new Event("input", { bubbles: true })); e.preventDefault();
      } else if (e.key === "Escape" && !active.closest(".cm-editor")) {
        active.blur();
      }
      chord = 0;
      return;
    }
    // The key map is declared once, on the server (keys.go), and arrives as JSON:
    // what a key does is looked up there, so a key cannot be bound here and left out
    // of the help overlay.
    if (chord && Date.now() < chord) {
      chord = 0;
      var b = bindingFor("g " + e.key);
      if (b) { goToSection(b.target); e.preventDefault(); return; }
    }
    chord = 0;
    if (e.key === "g" && !e.repeat) { chord = Date.now() + 1500; return; }
    var acts = pageKeys(e.key);
    for (var i = 0; i < acts.length; i++) {
      if (pageActions[acts[i]] && pageActions[acts[i]](e, active) !== false) return;
    }
  });
  function goToSection(target) {
    // A section this console does not have (no recorder, no registry) is not a place to go.
    var link = document.querySelector('.rail a.nav[href$="' + target + '"]');
    if (link) link.click();
  }

  // ---------- the key map ----------
  var KEYMAP = [];
  try { KEYMAP = JSON.parse(document.getElementById("keymap").textContent) || []; } catch (err) {}
  function bindingFor(key) {
    for (var i = 0; i < KEYMAP.length; i++) if (KEYMAP[i].key === key) return KEYMAP[i];
    return null;
  }
  // Every action a single key stands for, in the order they are declared: "p" is
  // produce on a topic and pause on a page that tails, and the page decides.
  function pageKeys(key) {
    var out = [];
    for (var i = 0; i < KEYMAP.length; i++) {
      var b = KEYMAP[i];
      if (b.key === key && b.scope !== "In a dialog") out.push(b.action);
    }
    return out;
  }
  var pageActions = {
    filter: function (e) {
      var f = document.querySelector("main input[data-filter-target]") || document.querySelector(".list input[data-filter-target]");
      if (f && visible(f)) { f.focus(); e.preventDefault(); return; }
      openPalette();
      e.preventDefault();
    },
    next: function () { moveCursor(1); },
    prev: function () { moveCursor(-1); },
    open: function (e, active) {
      // Enter on a focused button or link is that control's own. The list cursor
      // is opened only when nothing else has the keyboard.
      if (active !== document.body && active.matches && active.matches(FOCUSABLE + ", summary")) return false;
      if (cursorEl && document.contains(cursorEl) && visible(cursorEl)) cursorEl.click();
    },
    create: function () {
      var nb = document.querySelector("[data-create]");
      if (nb) nb.click(); else return false;
    },
    // On a topic, p is produce: the composer opens and takes focus.
    produce: function (e) {
      if (!document.getElementById("compose-panel")) return false;
      e.preventDefault(); setDock(true); focusComposer();
    },
    // Anywhere else that tails, p pauses.
    pause: function () {
      if (document.getElementById("compose-panel")) return false;
      if (!document.querySelector("[data-live-toggle]")) return false;
      setPaused(!livePaused);
    },
    help: function (e) { e.preventDefault(); openHelp(); },
    // ] and [ move along the page's tabs.
    "tab-next": function () { return stepTab(1); },
    "tab-prev": function () { return stepTab(-1); },
    // Esc closes the panel; with none open it clears the page's own filter.
    close: function (e) {
      if (inspectorIsOpen()) { closeInspector(); e.preventDefault(); return; }
      var f = document.querySelector("input[data-filter-target]");
      if (f && f.value) { f.value = ""; f.dispatchEvent(new Event("input", { bubbles: true })); e.preventDefault(); return; }
      return false;
    },
    // The chord's own binding is handled where the chord is; these have no key alone.
    goto: function () { return false; },
  };
  function stepTab(d) {
    var tabs = Array.prototype.filter.call(document.querySelectorAll("main .tabs a.tab"), visible);
    if (tabs.length < 2) return false;
    var at = -1;
    tabs.forEach(function (t, i) { if (t.classList.contains("on")) at = i; });
    var to = tabs[(at + d + tabs.length) % tabs.length];
    if (to) to.click();
  }

  // ---------- help: the keys, listed from the declared map ----------
  var helpBox = document.getElementById("help"), helpOpener = null;
  function openHelp() {
    if (!helpBox || confirmOpen()) return;
    if (paletteOpen()) closePalette();
    if (!helpOpen()) helpOpener = document.activeElement;
    helpBox.hidden = false;
    var c = document.getElementById("help-close");
    if (c) c.focus();
  }
  function closeHelp() {
    if (!helpOpen()) return;
    helpBox.hidden = true;
    if (helpOpener && document.contains(helpOpener) && helpOpener.focus) helpOpener.focus();
  }
  if (helpBox) {
    helpBox.addEventListener("click", function (e) { if (e.target === helpBox) closeHelp(); });
    var hc = document.getElementById("help-close");
    if (hc) hc.addEventListener("click", closeHelp);
  }
  document.addEventListener("keydown", function (e) {
    if (!helpOpen()) return;
    if (e.key === "Escape" || e.key === "?") { closeHelp(); e.preventDefault(); e.stopPropagation(); return; }
    trapTab(helpBox, e);
  }, true);

  // ---------- the inspector ----------
  // The right-hand panel (Sy-01), in flow beside the page: nothing slides over the
  // content and there is no scrim. A selection fills #inspector; closing it puts the
  // page's own summary back (Traffic's open connections), or hides the panel. It has
  // a title and a close button in every state (loading, a record, an error), takes
  // focus when it opens, closes on Esc, and hands focus back to the row that opened
  // it. Its selection is in the URL (?open=), so a refresh or a shared link reopens it.
  var inspectorPanel = document.getElementById("inspector-panel");
  var inspector = document.getElementById("inspector");
  var inspectorOpener = null, inspectorXhr = null, inspectorRefocus = null;
  // The selection is in the address as ?open=. Opening one pushes an entry (so
  // Back returns to the previous selection, or to none); closing rewrites the
  // current entry without it. Our entries carry a marker so a popstate to one of
  // them is ours to answer and htmx (which restores only its own) leaves it alone.
  function openParamNow() { return new URLSearchParams(location.search).get("open"); }
  function setOpenParam(value, push) {
    try {
      var u = new URL(location.href);
      if (value == null) u.searchParams.delete("open"); else u.searchParams.set("open", value);
      var next = u.pathname + (u.search || "") + u.hash;
      if (next === location.pathname + location.search + location.hash) return;
      if (push) history.pushState({ dozeInspector: 1 }, "", next);
      else history.replaceState(history.state, "", next);
    } catch (e) {}
  }
  function currentInspector() {
    // The page is swapped under the script; the panel and its body are looked up when
    // they are needed, never held.
    inspectorPanel = document.getElementById("inspector-panel");
    inspector = document.getElementById("inspector");
  }
  function openInspector(from) {
    currentInspector();
    if (!inspector || confirmOpen()) return;
    if (!inspectorIsOpen()) inspectorOpener = from || document.activeElement;
    inspector.hidden = false;
    inspectorPanel.hidden = false;
    var def = document.getElementById("insp-default");
    if (def) def.hidden = true;
  }
  // closeInspector(restoreFocus, keepURL): the URL loses ?open= unless the close
  // is the answer to the URL having already lost it (Back).
  function closeInspector(restoreFocus, keepURL) {
    currentInspector();
    if (!inspectorIsOpen()) return;
    inspector.hidden = true;
    var def = document.getElementById("insp-default");
    if (def) def.hidden = false; else inspectorPanel.hidden = true;
    // A request still on its way is for an inspector that is no longer open. Left
    // alone, its answer filled the closed inspector and put ?open= back in the URL.
    if (inspectorXhr) { inspectorXhr.dozeDropped = true; inspectorXhr.abort(); inspectorXhr = null; }
    inspector.textContent = "";
    inspector.classList.remove("busy");
    inspector.removeAttribute("aria-busy");
    if (!keepURL) setOpenParam(null, false);
    var back = inspectorOpener;
    inspectorOpener = null;
    if (restoreFocus === false || !back) return;
    // A live update may have replaced the row since; the new one has the same id.
    if (!document.contains(back) && back.id) back = document.getElementById(back.id);
    if (back && back.focus) back.focus();
  }
  // message fills the inspector with a titled state that is not a selection:
  // loading, or what went wrong.
  function inspectorMessage(title, text, isError) {
    currentInspector();
    inspector.textContent = "";
    var h = document.createElement("div");
    h.className = "inspector-h";
    h.innerHTML = '<div><span class="cap">Details</span><span class="t h2" id="inspector-title"></span></div><div class="inspector-nav"><button type="button" class="icon-btn" aria-label="Close" data-inspector-close>✕</button></div>';
    h.querySelector("#inspector-title").textContent = title;
    var b = document.createElement("div");
    if (isError) { b.className = "err inspector-err"; b.setAttribute("role", "alert"); b.textContent = text; }
    else { b.className = "inspector-loading"; b.innerHTML = '<span class="spinner" aria-hidden="true"></span> '; b.appendChild(document.createTextNode(text)); }
    inspector.appendChild(h);
    inspector.appendChild(b);
    h.querySelector("[data-inspector-close]").focus();
  }
  function inspectorRequestStarted(xhr, from) {
    currentInspector();
    if (!inspector) return;
    openInspector(from);
    if (inspectorXhr) { inspectorXhr.dozeDropped = true; inspectorXhr.abort(); } // the newer click wins.
    inspectorXhr = xhr;
    var a = document.activeElement;
    inspectorRefocus = a && inspector.contains(a) ? a.getAttribute("aria-label") : null;
    // Stepping to the next record keeps the current one on screen, dimmed, until
    // the new one arrives. A first open has nothing to keep, and says so.
    if (inspector.querySelector(".inspector-b")) { inspector.classList.add("busy"); inspector.setAttribute("aria-busy", "true"); }
    else inspectorMessage("Opening", "Loading…", false);
    xhr.addEventListener("loadend", function () {
      if (xhr.dozeDropped) return;
      if (inspectorXhr === xhr) inspectorXhr = null;
      inspector.classList.remove("busy");
      inspector.removeAttribute("aria-busy");
      if (!inspectorIsOpen()) return;
      if (xhr.status === 0) inspectorMessage("Could not open it", "No answer from the console — is doze-kafka still running?", true);
      else if (xhr.status >= 400) inspectorMessage("Could not open it", errorText(xhr), true);
    });
  }
  document.addEventListener("click", function (e) {
    var row = e.target.closest("[data-inspector-open]");
    if (row) { openInspector(row); return; }
    if (e.target.closest("[data-inspector-close]")) { closeInspector(); return; }
  });
  document.addEventListener("keydown", function (e) {
    if (confirmOpen() || paletteOpen() || helpOpen()) return;
    var row = e.target.closest && e.target.closest("[data-inspector-open]");
    if (row && (e.key === "Enter" || e.key === " ")) { openInspector(row); return; } // htmx fires the request.
  }, true);
  // A selection landed in the inspector: put it in the URL and move focus in, to
  // the button that asked when it was Older or Newer, so stepping keeps its place.
  document.addEventListener("htmx:afterSwap", function (e) {
    var t = e.detail.target;
    if (!t || t.id !== "inspector") return;
    if (!inspectorIsOpen()) { t.textContent = ""; return; }
    var title = t.querySelector("#inspector-title[data-open]");
    var open = title ? title.getAttribute("data-open") : "";
    var kind = t.querySelector("[data-inspector-kind]");
    if (inspectorPanel && kind) inspectorPanel.setAttribute("data-kind", kind.getAttribute("data-inspector-kind"));
    // A record is named by an offset on the partition in view; a search across
    // partitions has no single partition, and so no address for it.
    var region = document.getElementById("tape-and-table");
    var isMessage = title && title.hasAttribute("data-offset");
    if (open && (!isMessage || (region && region.hasAttribute("data-msg-base")))) setOpenParam(open, openParamNow() !== open);
    var again = inspectorRefocus && t.querySelector('button[aria-label="' + inspectorRefocus + '"]:not([disabled])');
    var btn = again || t.querySelector("[data-inspector-close]");
    if (btn) btn.focus();
  });
  // Where a selection named by ?open= is fetched from: a bare value is a record on
  // the page's own partition; kind:key is the inspector's route.
  function inspectorFetchURL(open) {
    var m = /^([a-z]+):(.*)$/.exec(open);
    if (m) return PREFIX + "/inspect/" + m[1] + "?key=" + encodeURIComponent(m[2]);
    var region = document.getElementById("tape-and-table");
    if (!region || !region.hasAttribute("data-msg-base")) return null;
    return region.getAttribute("data-msg-base") + "&offset=" + encodeURIComponent(open);
  }
  function inspectorOpenRow(open) {
    var m = /^([a-z]+):(.*)$/.exec(open);
    return m ? null : document.getElementById("msg-" + open);
  }
  // A link or refresh with ?open= reopens that selection; Back and Forward move
  // between the selections the person made.
  function syncInspectorFromURL() {
    var open = openParamNow();
    if (open == null) { closeInspector(true, true); return; }
    var url = inspectorFetchURL(open);
    if (!url || !window.htmx) return;
    currentInspector();
    var title = inspectorIsOpen() && inspector.querySelector("#inspector-title[data-open]");
    if (title && title.getAttribute("data-open") === open) return;
    openInspector(inspectorOpenRow(open));
    htmx.ajax("GET", url, { target: "#inspector", swap: "innerHTML" }).catch(function () {});
  }
  document.addEventListener("DOMContentLoaded", syncInspectorFromURL);
  window.addEventListener("popstate", syncInspectorFromURL);
  // "Send again" closes the inspector once the broker has taken the copy; on a
  // failure it stays open, and the error is a toast.
  document.addEventListener("htmx:afterRequest", function (e) {
    var el = e.detail.elt;
    if (el && el.hasAttribute && el.hasAttribute("data-close-inspector-on-success") && e.detail.successful) closeInspector(false);
  });

  // ---------- docked composer ----------
  // Whether it is open is an attribute on <html>, not a class on the dock: the
  // dock is replaced when the partition changes or a record is sent, and the
  // composer must stay as it was through that. Moving to another topic closes it
  // (see afterNavigation).
  function dockOpen() { return root.getAttribute("data-dock") === "open"; }
  function syncDock() {
    var bar = document.getElementById("compose-head");
    if (bar) bar.setAttribute("aria-expanded", dockOpen() ? "true" : "false");
  }
  function setDock(open) {
    if (open) root.setAttribute("data-dock", "open"); else root.removeAttribute("data-dock");
    syncDock();
    var panel = document.getElementById("compose-panel");
    if (open && panel && window.dozeEditor) requestAnimationFrame(function () { dozeEditor.refresh(panel); });
  }
  document.addEventListener("click", function (e) {
    if (e.target.closest("[data-dock-toggle]")) setDock(!dockOpen());
  });
  document.addEventListener("DOMContentLoaded", syncDock);
  document.addEventListener("htmx:afterSettle", syncDock);

  // ---------- filters ----------
  // A text field with data-filter-target="<selector>" narrows the elements that match
  // the selector to those whose name (data-name, or else their text) contains what is
  // typed; a segmented control with data-seg-for="<same selector>" narrows them to
  // those that carry its value in data-seg. data-filter-none names the element that
  // says nothing matched. Both are reapplied after a swap, so a poll does not undo them.
  function applyFilters() {
    var done = {};
    document.querySelectorAll("input[data-filter-target]").forEach(function (inp) {
      var sel = inp.getAttribute("data-filter-target");
      if (!sel || done[sel]) return;
      done[sel] = true;
      runFilter(sel, inp);
    });
    document.querySelectorAll(".seg[data-seg-for]").forEach(function (seg) {
      var sel = seg.getAttribute("data-seg-for");
      if (done[sel]) return;
      done[sel] = true;
      runFilter(sel, null);
    });
  }
  function runFilter(sel, inp) {
    var q = inp ? inp.value.toLowerCase().trim() : "";
    var seg = document.querySelector('.seg[data-seg-for="' + sel + '"] .on');
    var segVal = seg ? (seg.getAttribute("data-seg-value") || "") : "";
    var rows;
    try { rows = document.querySelectorAll(sel); } catch (e) { return; }
    var shown = 0;
    rows.forEach(function (row) {
      var hay = ((row.getAttribute("data-name") || "") + " " + (row.getAttribute("data-text") || "") + " " + (row.getAttribute("data-name") ? "" : row.textContent)).toLowerCase();
      var ok = (!q || hay.indexOf(q) >= 0) && (!segVal || (row.getAttribute("data-seg") || "").split(" ").indexOf(segVal) >= 0);
      row.hidden = !ok;
      if (ok) shown++;
    });
    var noneSel = inp && inp.getAttribute("data-filter-none");
    var none = noneSel && document.querySelector(noneSel);
    if (none) none.hidden = !(rows.length && shown === 0);
  }
  document.addEventListener("input", function (e) {
    if (e.target && e.target.matches && e.target.matches("input[data-filter-target]")) applyFilters();
  });
  document.addEventListener("click", function (e) {
    var b = e.target.closest && e.target.closest(".seg[data-seg-for] [data-seg-value]");
    if (!b) return;
    var seg = b.closest(".seg");
    seg.querySelectorAll("[data-seg-value]").forEach(function (x) {
      var on = x === b;
      x.classList.toggle("on", on);
      x.setAttribute("aria-pressed", on ? "true" : "false");
    });
    applyFilters();
  });
  document.addEventListener("htmx:afterSettle", applyFilters);
  document.addEventListener("DOMContentLoaded", applyFilters);

  // ---------- the list keeps its place ----------
  // A click in the list replaces the whole page (the inspector and the dock belong to
  // it), so the list's scroll position is put back on the list that arrives.
  var listScroll = 0;
  document.addEventListener("htmx:beforeSwap", function (e) {
    var t = e.detail.target;
    if (!t || t.id !== "workspace") return;
    var sc = document.querySelector(".list .list-scroll");
    listScroll = sc ? sc.scrollTop : 0;
  });
  document.addEventListener("htmx:afterSettle", function (e) {
    var t = e.detail.target;
    if (!t || t.id !== "workspace" || !listScroll) return;
    var sc = document.querySelector(".list .list-scroll");
    if (sc) sc.scrollTop = listScroll;
    listScroll = 0;
  });

  // ---------- the page's own summary comes back ----------
  // The open connections are replaced out of band with the Traffic page's poll. While
  // a selection is open they stay hidden behind it, and are visible again when it
  // closes.
  document.addEventListener("htmx:oobAfterSwap", function () {
    var def = document.getElementById("insp-default");
    if (def && inspectorIsOpen()) def.hidden = true;
  });

  // ---------- the ··· menu ----------
  // Closes when a click lands elsewhere, and on Esc.
  function closeMenus(except) {
    document.querySelectorAll(".more > [data-more][aria-expanded=true]").forEach(function (b) {
      if (b === except) return;
      b.setAttribute("aria-expanded", "false");
      var m = b.parentNode.querySelector(".menu");
      if (m) m.hidden = true;
    });
  }
  document.addEventListener("click", function (e) {
    var btn = e.target.closest && e.target.closest(".more > [data-more]");
    if (btn) {
      var menu = btn.parentNode.querySelector(".menu"), open = btn.getAttribute("aria-expanded") === "true";
      closeMenus(btn);
      btn.setAttribute("aria-expanded", open ? "false" : "true");
      if (menu) menu.hidden = open;
      return;
    }
    // A menu action is done: the menu closes (a confirm may follow); a click elsewhere closes it too.
    closeMenus(null);
  });
  document.addEventListener("keydown", function (e) {
    if (e.key !== "Escape") return;
    var open = document.querySelector(".more > [data-more][aria-expanded=true]");
    if (open) { closeMenus(null); open.focus(); e.stopPropagation(); e.preventDefault(); }
  }, true);

  // ---------- the produce button ----------
  document.addEventListener("click", function (e) {
    var b = e.target.closest && e.target.closest("[data-compose-open]");
    if (!b || !document.getElementById("compose-panel")) return;
    e.preventDefault();
    setDock(true);
    focusComposer();
    var panel = document.getElementById("compose-panel");
    if (panel && panel.scrollIntoView) panel.scrollIntoView({ block: "nearest" });
  });

  // ---------- time ----------
  // One formatter. The server writes every moment as <time datetime="UTC" data-style>
  // with its own text in UTC (timefmt.go). Here it is shown in the browser's zone,
  // with the zone and the UTC instant on hover, and "ago" and "since" are counted in
  // the browser so they keep moving on a quiet or paused page. The styles are the
  // server's: clock, clockms, datetime, date, ago, since.
  function pad(n, w) { n = String(n); while (n.length < (w || 2)) n = "0" + n; return n; }
  function zoneName() { try { return Intl.DateTimeFormat().resolvedOptions().timeZone || ""; } catch (e) { return ""; } }
  function offsetText(d) {
    var m = -d.getTimezoneOffset(), sign = m < 0 ? "-" : "+";
    m = Math.abs(m);
    return "UTC" + sign + pad(Math.floor(m / 60)) + ":" + pad(m % 60);
  }
  function local(ms, style) {
    var d = new Date(ms);
    var date = d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate());
    var clock = pad(d.getHours()) + ":" + pad(d.getMinutes()) + ":" + pad(d.getSeconds());
    switch (style) {
      case "clockms": return clock + "." + pad(d.getMilliseconds(), 3);
      case "datetime": return date + " " + clock;
      case "date": return date;
    }
    return clock;
  }
  function spanText(ms) {
    var s = Math.max(0, Math.floor(ms / 1000));
    if (s < 2) return "just now";
    if (s < 60) return s + "s";
    if (s < 3600) return Math.floor(s / 60) + "m " + pad(s % 60) + "s";
    return Math.floor(s / 3600) + "h " + pad(Math.floor(s / 60) % 60) + "m";
  }
  function agoText(ms) {
    var s = Math.max(0, Math.round(ms / 1000));
    if (s < 60) return "just now";
    if (s < 3600) return Math.floor(s / 60) + "m ago";
    if (s < 86400) return Math.floor(s / 3600) + "h ago";
    return Math.floor(s / 86400) + "d ago";
  }
  function formatTime(el) {
    var ms = Date.parse(el.getAttribute("datetime") || "");
    if (isNaN(ms)) return;
    var style = el.getAttribute("data-style") || "clock", d = new Date(ms);
    if (style === "ago") el.textContent = agoText(Date.now() - ms);
    else if (style === "since") el.textContent = spanText(Date.now() - ms);
    else el.textContent = local(ms, style);
    // On hover: the moment in this browser's zone, then the same instant in UTC.
    var utc = new Date(ms).toISOString().replace("T", " ").replace("Z", " UTC");
    el.title = local(ms, "datetime") + "." + pad(d.getMilliseconds(), 3) + " " + (zoneName() ? zoneName() + " " : "") + "(" + offsetText(d) + ") · " + utc;
  }
  function formatTimes(root) {
    (root || document).querySelectorAll("time.when").forEach(formatTime);
  }
  window.dozeTime = {
    // clock is a moment as the time of day in this browser's zone.
    clock: function (ms) { return local(ms, "clock"); },
    format: formatTimes,
  };
  document.addEventListener("DOMContentLoaded", function () { formatTimes(); });
  document.addEventListener("htmx:afterSettle", function () { formatTimes(); });
  document.addEventListener("htmx:afterSwap", function () { formatTimes(); });
  setInterval(function () {
    document.querySelectorAll('time.when[data-style="ago"], time.when[data-style="since"]').forEach(formatTime);
  }, 5000);

  // ---------- flash banner ----------
  // The server shows a one-shot "Topic created" banner for a ?flash= redirect. Left
  // in the address bar, a refresh (or a copied link) replays it; drop the
  // parameter once it has been shown.
  function stripFlash() {
    try {
      var u = new URL(location.href);
      if (!u.searchParams.has("flash")) return;
      u.searchParams.delete("flash");
      history.replaceState(history.state, "", u.pathname + (u.search || "") + u.hash);
    } catch (e) {}
  }
  document.addEventListener("DOMContentLoaded", stripFlash);

  // ---------- jump-to-offset landing ----------
  // The row a jump lands on is marked by the server; bring it into view when the
  // page arrives, whether by a full load or by a swap (the jump form is boosted).
  function revealHL() {
    var row = document.querySelector(".tbl tr[data-hl]");
    if (row) row.scrollIntoView({ block: "center" });
  }
  document.addEventListener("DOMContentLoaded", revealHL);
  document.addEventListener("htmx:afterSettle", function (e) {
    var t = e.detail.target;
    if (t && (t.id === "workspace" || t.id === "detail")) revealHL();
  });

  // ---------- arriving content ----------
  // A different topic or group fades in over a moment instead of snapping.
  document.addEventListener("htmx:afterSwap", function (e) {
    var t = e.detail.target;
    if (!t || t.id !== "detail") return;
    var pane = document.getElementById("detail");
    if (!pane) return;
    pane.classList.add("arriving");
    setTimeout(function () { pane.classList.remove("arriving"); }, 220);
  });

  // ---------- live polling with change detection ----------
  // An element with data-live="URL" polls every data-live-ms (default 3000). The
  // server echoes a content hash; when nothing changed it answers 204 and nothing
  // moves. Changed content replaces the element (outerHTML). What a replace would
  // lose (the jump field's text, keyboard focus, the table's scroll position) is
  // saved before the swap and put back after it.
  var livePaused = false;
  function setPaused(p) {
    livePaused = p;
    document.querySelectorAll("[data-live-toggle]").forEach(function (b) {
      b.setAttribute("aria-pressed", p ? "true" : "false");
      b.classList.toggle("on", p);
      b.title = p ? "Resume live updates" : "Pause live updates";
      // The Traffic page's toggle is a button that says what it will do (Main).
      var off = b.getAttribute("data-label-off"), on = b.getAttribute("data-label-on");
      if (off && on) b.textContent = p ? on : off;
    });
    document.querySelectorAll("[data-live-state]").forEach(function (el) {
      el.classList.toggle("ok", !p);
      var l = el.querySelector("[data-live-state-l]");
      if (l) l.textContent = p ? "paused" : "live";
      var d = el.querySelector(".dot");
      if (d) d.classList.toggle("off", p);
    });
  }
  document.addEventListener("click", function (e) {
    if (e.target.closest("[data-live-toggle]")) setPaused(!livePaused);
  });
  var liveTimers = [];
  function setupLive() {
    liveTimers.forEach(clearInterval);
    liveTimers = [];
    document.querySelectorAll("[data-live]").forEach(function (el) {
      var id = el.id, busy = false;
      var every = parseInt(el.getAttribute("data-live-ms") || "3000", 10);
      liveTimers.push(setInterval(function () {
        // Looked up by id every tick: a swap replaces the element, and a held
        // reference would go stale and the poll would silently die.
        var cur = document.getElementById(id);
        if (!cur || busy || document.hidden) return;
        // Paused is the page's: the tape, the dock and whatever else polls stop
        // together (a navigation clears it, so it never follows you to a page
        // that has no control to undo it).
        if (livePaused) return;
        // A confirm dialog is asking about a button in this region. Replacing the
        // region would detach that button, and "Confirm" would then do nothing.
        // The same goes for a row whose offset is being edited (data-hold-live):
        // a refresh would throw away what is being typed.
        if (confirmOpen() || cur.querySelector("[data-hold-live]")) return;
        var url = cur.getAttribute("data-live");
        if (!url) return;
        busy = true;
        var done = function () { busy = false; };
        // A failed poll is reported by the responseError / sendError handlers (as
        // the "not updating" state); the rejection itself is not news.
        htmx.ajax("GET", url + (url.indexOf("?") >= 0 ? "&" : "?") + "h=" + encodeURIComponent(cur.getAttribute("data-hash") || ""), {
          target: "#" + id, swap: "outerHTML",
        }).then(done, done);
      }, every));
    });
  }
  document.addEventListener("DOMContentLoaded", setupLive);
  document.addEventListener("htmx:afterSettle", renderPill);
  document.addEventListener("htmx:afterSwap", function (e) {
    // Re-arm after a navigation, or after any swap that lands or contains a live region.
    var t = e.detail.target;
    if (t && (t.id === "workspace" || t.id === "detail" || (t.matches && t.matches("[data-live]")) || (t.querySelector && t.querySelector("[data-live]")))) {
      setupLive();
      setPaused(livePaused); // a replaced region comes back looking unpaused.
    }
  });

  var liveState = null;
  document.addEventListener("htmx:beforeSwap", function (e) {
    var t = e.detail.target;
    if (!t || !t.matches || !t.matches("[data-live]")) return;
    // A poll that was already on its way when a confirm dialog opened.
    if ((confirmOpen() || t.querySelector("[data-hold-live]")) && isLivePoll(e)) { e.detail.shouldSwap = false; if (e.detail.xhr) e.detail.xhr.dozeHeld = true; return; }
    // Fires for a 204 too, where nothing will be swapped. Saving state then left
    // it lying around for the next unrelated swap (opening the inspector) to
    // "restore", which threw the table back to wherever it had been.
    if (!e.detail.shouldSwap) return;
    var jump = t.querySelector("#jump-to");
    var wrap = t.querySelector(".tbl-wrap");
    // What has been typed into the region's own fields (the jump box, the search
    // box) and not yet sent.
    var typed = {};
    t.querySelectorAll("input[id]").forEach(function (i) { typed[i.id] = i.type === "checkbox" ? i.checked : i.value; });
    var active = document.activeElement, focus = null;
    if (active && t.contains(active)) {
      // By id where there is one (rows, the jump field); otherwise by position
      // among the region's focusable elements, which a refresh rarely changes.
      focus = active.id ? { id: active.id } : { at: Array.prototype.indexOf.call(t.querySelectorAll(FOCUSABLE), active), tag: active.tagName };
    }
    liveState = {
      id: t.id, focus: focus, typed: typed, poll: isLivePoll(e),
      selStart: jump ? jump.selectionStart : null, selEnd: jump ? jump.selectionEnd : null,
      scrollTop: wrap ? wrap.scrollTop : 0, scrollLeft: wrap ? wrap.scrollLeft : 0,
      atTop: !wrap || wrap.scrollTop < 4,
    };
  });
  document.addEventListener("htmx:afterSettle", function (e) {
    var st = liveState;
    if (!st) return;
    var landed = (e.detail.elt && e.detail.elt.id === st.id) || (e.detail.target && e.detail.target.id === st.id);
    if (!landed) return;
    liveState = null;
    var region = document.getElementById(st.id);
    if (!region) return;
    var jump = region.querySelector("#jump-to");
    Object.keys(st.typed).forEach(function (id) {
      var i = document.getElementById(id);
      if (!i || !region.contains(i)) return;
      if (i.type === "checkbox") i.checked = st.typed[id]; else if (st.typed[id]) i.value = st.typed[id];
    });
    // A record that arrived while you were watching lands with a small motion.
    if (st.poll) region.classList.add("landed");
    var wrap = region.querySelector(".tbl-wrap");
    // Someone reading down the table keeps their place; someone parked at the top
    // (the newest rows) just sees the new ones arrive.
    if (wrap) { wrap.scrollLeft = st.scrollLeft; if (!st.atTop) wrap.scrollTop = st.scrollTop; }
    if (st.focus && !inspectorIsOpen() && !paletteOpen()) {
      var el = st.focus.id ? document.getElementById(st.focus.id) : region.querySelectorAll(FOCUSABLE)[st.focus.at];
      if (el && region.contains(el) && (st.focus.id || el.tagName === st.focus.tag)) {
        el.focus({ preventScroll: true });
        if (el === jump) { try { jump.setSelectionRange(st.selStart, st.selEnd); } catch (err) {} }
      }
    }
  });
  // After each poll, record the hash the server sent, so the next poll asks
  // "anything since this?". Not for a response that was held back above: its
  // content was not shown, and the next poll must fetch it again.
  document.addEventListener("htmx:afterRequest", function (e) {
    var xhr = e.detail.xhr;
    if (!xhr) return;
    if (e.detail.successful && isLivePoll(e)) setDisconnected(false, "", pollPath(e));
    var h = xhr.getResponseHeader("HX-Live-Hash");
    if (!h || xhr.dozeHeld) return;
    var path = (e.detail.requestConfig && e.detail.requestConfig.path) || "";
    document.querySelectorAll("[data-live]").forEach(function (el) {
      var base = el.getAttribute("data-live");
      if (base && path.indexOf(base) === 0) el.setAttribute("data-hash", h);
    });
  });
})();
