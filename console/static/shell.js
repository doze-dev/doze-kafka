// shell.js — the console's chrome: theme, toasts, copy, confirm dialog, command
// palette, keyboard shortcuts, the message inspector, the docked composer and live
// polling. Chrome elements live outside #workspace, so they survive every
// content swap; anything inside it is looked up when it is needed, never held.
(function () {
  "use strict";
  var root = document.documentElement;
  var PREFIX = (document.querySelector('link[rel="icon"]') || {}).href
    ? new URL(document.querySelector('link[rel="icon"]').href).pathname.replace(/\/static\/favicon\.svg$/, "")
    : "/_console";
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

  // ---------- theme: light / system / dark ----------
  // The stylesheet knows two themes. "System" is this script choosing between
  // them from the OS setting, and following it when it changes; it is stored as
  // the absence of a choice. (The same resolution runs inline in <head>, before
  // first paint.)
  var themeSeg = document.getElementById("theme-seg");
  var darkMQ = window.matchMedia ? window.matchMedia("(prefers-color-scheme: dark)") : null;
  function themeMode() {
    var t = stored("theme");
    return t === "light" || t === "dark" ? t : "system";
  }
  function applyTheme(mode) {
    root.setAttribute("data-theme", mode === "system" ? (darkMQ && darkMQ.matches ? "dark" : "light") : mode);
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
  if (darkMQ && darkMQ.addEventListener) darkMQ.addEventListener("change", function () {
    if (themeMode() === "system") applyTheme("system");
  });

  // ---------- collapsible rail ----------
  function railSlim() { return root.getAttribute("data-rail") === "slim"; }
  // The rail folds to icons on narrow windows unless you have chosen "wide".
  var narrowMQ = window.matchMedia ? window.matchMedia("(max-width:1180px)") : null;
  function applyRailAuto() {
    var pref = stored("rail");
    var slim = pref === "slim" || (pref !== "wide" && !!narrowMQ && narrowMQ.matches);
    if (slim) root.setAttribute("data-rail", "slim"); else root.removeAttribute("data-rail");
  }
  if (narrowMQ && narrowMQ.addEventListener) narrowMQ.addEventListener("change", applyRailAuto);
  function setRail(slim) {
    if (slim) root.setAttribute("data-rail", "slim"); else root.removeAttribute("data-rail");
    store("rail", slim ? "slim" : "wide");
  }
  var railToggle = document.getElementById("rail-toggle");
  if (railToggle) railToggle.addEventListener("click", function () { setRail(!railSlim()); });
  // Flyout labels in slim mode: positioned on hover so the rail's own overflow
  // can never clip them.
  document.addEventListener("mouseover", function (e) {
    if (!railSlim()) return;
    var ri = e.target.closest(".rail .ri");
    var fly = ri && ri.querySelector(".ri-fly");
    if (!fly) return;
    var r = ri.getBoundingClientRect();
    fly.style.left = (r.right + 8) + "px";
    fly.style.top = (r.top + r.height / 2) + "px";
  });
  // The counts beside Topics and Consumer groups, refreshed gently.
  function refreshCounts() {
    if (document.hidden) return;
    fetch(PREFIX + "/api/counts").then(function (r) { return r.json(); }).then(function (counts) {
      document.querySelectorAll(".rail [data-ct]").forEach(function (el) {
        var n = counts[el.getAttribute("data-ct")];
        el.textContent = n == null ? "" : String(n);
      });
    }).catch(function () {});
  }
  refreshCounts();
  setInterval(refreshCounts, 5000);

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
  // the message inspector. Only one is open at a time, and while one is, the page's
  // single-key shortcuts are off.
  var FOCUSABLE = 'button:not([disabled]), a[href], input:not([disabled]):not([type="hidden"]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';
  function isOpen(id) { var el = document.getElementById(id); return !!el && !el.hidden; }
  function confirmOpen() { return isOpen("confirm"); }
  function paletteOpen() { return isOpen("palette"); }
  function inspectorIsOpen() { return isOpen("inspector-overlay"); }
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
  document.addEventListener("htmx:confirm", function (e) {
    var box = document.getElementById("confirm");
    var q = e.detail.question;
    if (!q || !box) return;
    e.preventDefault();
    if (confirmOpen()) return; // one question at a time.
    var elt = e.detail.elt || {}, data = elt.dataset || {};
    var opener = document.activeElement;
    document.getElementById("confirm-title").textContent = data.confirmTitle || "Are you sure?";
    document.getElementById("confirm-msg").textContent = q;
    var yes = document.getElementById("confirm-yes"), no = document.getElementById("confirm-no");
    yes.textContent = data.confirmVerb || "Confirm";
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
  });

  // ---------- command palette ----------
  var pal = document.getElementById("palette"), palQ = document.getElementById("pal-q"), palList = document.getElementById("pal-list");
  var palItems = [], palSel = 0, palOpener = null;

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

  function openPalette() {
    if (!pal || confirmOpen()) return;
    if (inspectorIsOpen()) closeInspector(false);
    if (!paletteOpen()) palOpener = document.activeElement;
    pal.hidden = false; palQ.value = ""; palSel = 0;
    var here = location.pathname;
    var recents = [];
    try {
      recents = JSON.parse(stored("recents") || "[]")
        .filter(function (x) { return x.u !== here; }).slice(0, 5)
        .map(function (x) { return { s: x.s, n: x.n, u: x.u, k: "recent" }; });
    } catch (e) {}
    palItems = recents;
    renderPal();
    palQ.focus();
    // The catalogue (views, actions, every topic and group) comes from the server,
    // so the palette cannot drift from the console's real routes.
    fetch(PREFIX + "/api/palette").then(function (r) { return r.json(); }).then(function (all) {
      var live = {}, fixed = [], resources = [];
      (all || []).forEach(function (x) {
        live[x.u] = 1;
        (x.k === "view" || x.k === "action" ? fixed : resources).push(x);
      });
      // A recent that no longer exists (a deleted topic) would be a dead link:
      // drop it here and from storage.
      var pruned = recents.filter(function (x) { return live[x.u]; });
      if (pruned.length !== recents.length) {
        try { store("recents", JSON.stringify(JSON.parse(stored("recents") || "[]").filter(function (x) { return live[x.u]; }))); } catch (e) {}
      }
      var seen = {};
      pruned.forEach(function (x) { seen[x.u] = 1; });
      palItems = pruned.concat(resources.filter(function (r) { return !seen[r.u]; })).concat(fixed);
      renderPal();
    }).catch(function () {}); // the console is unreachable: recents still work.
  }
  function palFiltered() {
    var q = palQ.value.toLowerCase().trim();
    if (!q) return palItems.slice(0, 14);
    return palItems.filter(function (it) { return (it.n + " " + it.k).toLowerCase().indexOf(q) >= 0; }).slice(0, 14);
  }
  function renderPal() {
    var items = palFiltered();
    if (palSel >= items.length) palSel = Math.max(0, items.length - 1);
    palList.innerHTML = "";
    if (!items.length) { palList.innerHTML = '<div class="pal-empty">Nothing matches</div>'; palQ.removeAttribute("aria-activedescendant"); return; }
    items.forEach(function (it, i) {
      var a = document.createElement("a");
      a.className = "pal-item" + (i === palSel ? " sel" : "");
      a.href = it.u;
      a.id = "pal-opt-" + i; a.setAttribute("role", "option"); a.setAttribute("aria-selected", i === palSel ? "true" : "false"); a.tabIndex = -1;
      a.innerHTML = '<span class="pal-dot"></span><span class="pal-name"></span><span class="pal-kind"></span>';
      // The dot is what the row is (topic, group, view), not how it got into the list.
      a.querySelector(".pal-dot").classList.add(it.s === "topics" ? "d-topic" : it.s === "groups" ? "d-group" : "d-view");
      a.querySelector(".pal-name").textContent = it.n;
      a.querySelector(".pal-kind").textContent = it.k;
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
  // console's home and the create-topic form belong to Topics.
  function sectionFor(path) {
    if (path.indexOf(PREFIX) !== 0) return "";
    var seg = path.slice(PREFIX.length).replace(/^\/+/, "").split("/")[0];
    if (seg === "" || seg === "topics" || seg === "new") return "topics";
    return seg;
  }
  var currentSection = sectionFor(location.pathname), currentPath = location.pathname;
  function afterNavigation() {
    if (window.dozeView) window.dozeView();
    var section = sectionFor(location.pathname);
    document.querySelectorAll(".rail .ri").forEach(function (a) {
      var on = sectionFor(new URL(a.href, location.origin).pathname) === section;
      a.classList.toggle("on", on);
      if (on) a.setAttribute("aria-current", "page"); else a.removeAttribute("aria-current");
    });
    // A filter typed in one section means nothing in the next (topic names do
    // not filter groups), and used to carry over and hide everything.
    if (section !== currentSection) {
      currentSection = section;
      if (window.Alpine && Alpine.store("filter")) Alpine.store("filter").q = "";
    }
    // A click in the list replaces only the detail pane, so the list itself (its
    // scroll position, the row under the pointer) stays put; its highlight is
    // moved here.
    document.querySelectorAll(".listpane .li[href]").forEach(function (a) {
      a.classList.toggle("on", new URL(a.href, location.origin).pathname === location.pathname);
    });
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
  // and an index into the old list pointed at the wrong (or a hidden) one.
  var cursorEl = null;
  function listRows() {
    return Array.prototype.filter.call(document.querySelectorAll(".listpane .li[href]"), function (r) { return r.offsetParent !== null; });
  }
  function resetListCursor() { cursorEl = null; }
  function moveCursor(d) {
    var rows = listRows();
    if (!rows.length) return;
    var at = rows.indexOf(cursorEl);
    var next = at < 0 ? (d > 0 ? 0 : rows.length - 1) : Math.min(rows.length - 1, Math.max(0, at + d));
    document.querySelectorAll(".listpane .li.cursor").forEach(function (r) { r.classList.remove("cursor"); });
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
  document.addEventListener("keydown", function (e) {
    if (confirmOpen()) return; // the dialog has its own keys, and nothing else gets any.
    var mod = e.metaKey || e.ctrlKey;
    if (paletteOpen()) {
      var items = palFiltered();
      if (e.key === "Tab") { e.preventDefault(); return; } // one control; focus stays on the search box.
      if (e.key === "Escape") { closePalette(); e.preventDefault(); }
      else if (e.key === "ArrowDown") { palSel = Math.min(palSel + 1, items.length - 1); renderPal(); e.preventDefault(); }
      else if (e.key === "ArrowUp") { palSel = Math.max(palSel - 1, 0); renderPal(); e.preventDefault(); }
      else if (e.key === "Enter") { var it = items[palSel]; if (it) location.href = it.u; e.preventDefault(); }
      return;
    }
    if (mod && !e.altKey && !e.shiftKey && e.key === "k") { e.preventDefault(); openPalette(); return; }
    if (inspectorIsOpen()) return; // the inspector's keys are handled where the inspector is.
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
    if (/^(INPUT|TEXTAREA|SELECT)$/.test(active.tagName) || active.isContentEditable || (active.closest && active.closest(".cm-editor"))) return;
    if (e.key === "/") {
      var f = document.querySelector(".listpane .filter input");
      if (f && f.offsetParent !== null) { f.focus(); e.preventDefault(); } else { openPalette(); e.preventDefault(); }
    } else if (e.key === "j") moveCursor(1);
    else if (e.key === "k") moveCursor(-1);
    else if (e.key === "Enter") {
      // Enter on a focused button or link is that control's own. The list cursor
      // is opened only when nothing else has the keyboard.
      if (active !== document.body && active.matches && active.matches(FOCUSABLE + ", summary")) return;
      if (cursorEl && document.contains(cursorEl) && cursorEl.offsetParent !== null) cursorEl.click();
    }
    else if (e.key === "c") { var nb = document.querySelector(".listpane .new-link"); if (nb) nb.click(); }
    else if (e.key === "m") {
      if (document.getElementById("compose-panel")) { e.preventDefault(); setDock(true); focusComposer(); }
    }
    else if (e.key === "[") setRail(!railSlim());
  });

  // ---------- the message inspector ----------
  // A real dialog in every state (loading, a record, an error): it has a title
  // and a close button, takes focus when it opens, keeps Tab inside, closes on
  // Esc or a click outside, and hands focus back to the row that opened it. Its
  // record is in the URL (?open=OFFSET), so a refresh or a shared link reopens it.
  var inspectorOverlay = document.getElementById("inspector-overlay");
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
  function openInspector(from) {
    if (!inspectorOverlay || confirmOpen()) return;
    if (!inspectorIsOpen()) inspectorOpener = from || document.activeElement;
    inspectorOverlay.hidden = false;
  }
  // closeInspector(restoreFocus, keepURL): the URL loses ?open= unless the close
  // is the answer to the URL having already lost it (Back).
  function closeInspector(restoreFocus, keepURL) {
    if (!inspectorIsOpen()) return;
    inspectorOverlay.hidden = true;
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
    inspector.textContent = "";
    var h = document.createElement("div");
    h.className = "inspector-h";
    h.innerHTML = '<div><span class="t" id="inspector-title"></span></div><div class="inspector-nav"><button type="button" class="icon-btn" aria-label="Close" data-inspector-close>✕</button></div>';
    h.querySelector("#inspector-title").textContent = title;
    var b = document.createElement("div");
    if (isError) { b.className = "err inspector-err"; b.setAttribute("role", "alert"); b.textContent = text; }
    else { b.className = "inspector-loading"; b.innerHTML = '<span class="spinner" aria-hidden="true"></span> '; b.appendChild(document.createTextNode(text)); }
    inspector.appendChild(h);
    inspector.appendChild(b);
    h.querySelector("[data-inspector-close]").focus();
  }
  function inspectorRequestStarted(xhr, from) {
    if (!inspector) return;
    // htmx hears the click on a row before the document does, so the request can
    // start while the inspector is still hidden, and a hidden dialog cannot take focus.
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
    if (e.target === inspectorOverlay) closeInspector();
  });
  document.addEventListener("keydown", function (e) {
    if (confirmOpen() || paletteOpen() || helpOpen()) return;
    var row = e.target.closest && e.target.closest("[data-inspector-open]");
    if (row && (e.key === "Enter" || e.key === " ")) { openInspector(row); return; } // htmx fires the request.
    if (!inspectorIsOpen()) return;
    if (e.key === "Escape") { closeInspector(); e.preventDefault(); e.stopPropagation(); return; }
    trapTab(inspectorOverlay, e);
  }, true);
  // A selection landed in the inspector: put it in the URL and move focus in, to
  // the button that asked when it was Older or Newer, so stepping keeps its place.
  document.addEventListener("htmx:afterSwap", function (e) {
    var t = e.detail.target;
    if (!t || t.id !== "inspector") return;
    if (!inspectorIsOpen()) { t.textContent = ""; return; }
    var title = t.querySelector("#inspector-title[data-open]");
    var open = title ? title.getAttribute("data-open") : "";
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

  // ---------- relative times ----------
  // The server renders "3m ago" once; on a quiet or paused view it would freeze
  // there while the clock moved on. Tick them in the browser.
  function agoText(ms) {
    var s = Math.max(0, Math.round((Date.now() - ms) / 1000));
    if (s < 60) return "just now";
    if (s < 3600) return Math.floor(s / 60) + "m ago";
    if (s < 86400) return Math.floor(s / 3600) + "h ago";
    return Math.floor(s / 86400) + "d ago";
  }
  setInterval(function () {
    document.querySelectorAll(".ago[data-ts]").forEach(function (el) {
      el.textContent = "· " + agoText(parseInt(el.getAttribute("data-ts"), 10));
    });
  }, 20000);

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
