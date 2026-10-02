// shell.js — workbench chrome: theme, toasts, copy, styled confirms, command
// palette, keyboard navigation, and live-region polling helpers. Everything
// here lives OUTSIDE #workspace so it survives every content swap.
(function () {
  "use strict";
  var PREFIX = (document.querySelector('link[rel="icon"]') || {}).href
    ? new URL(document.querySelector('link[rel="icon"]').href).pathname.replace(/\/static\/favicon\.svg$/, "")
    : "/_console";

  // ---------- Alpine glue ----------
  document.addEventListener("htmx:afterSwap", function () {
    // e.detail.target is the OLD detached node for outerHTML swaps; upgrade
    // document-wide instead (both calls are idempotent). Alpine also inits new
    // nodes via its own MutationObserver, but initTree on the live tree is safe.
    if (window.Alpine) window.Alpine.initTree(document.body);
    if (window.dozeEditor) window.dozeEditor.upgradeAll();
    resetListCursor();
  });

  // ---------- theme: light / system / dark ----------
  // "system" is the absence of an override: no data-theme attribute, no
  // localStorage entry — the prefers-color-scheme media query does the rest,
  // and tracks OS changes live for free.
  var themeSeg = document.getElementById("theme-seg");
  function themeMode() {
    try { return localStorage.getItem("theme") || "system"; } catch (e) { return "system"; }
  }
  function applyTheme(mode) {
    if (mode === "system") {
      document.documentElement.removeAttribute("data-theme");
      try { localStorage.removeItem("theme"); } catch (e) {}
    } else {
      document.documentElement.setAttribute("data-theme", mode);
      try { localStorage.setItem("theme", mode); } catch (e) {}
    }
    if (themeSeg) themeSeg.querySelectorAll("button").forEach(function (b) {
      b.classList.toggle("on", b.getAttribute("data-mode") === mode);
    });
  }
  if (themeSeg) {
    applyTheme(themeMode()); // paint the active segment on load
    themeSeg.addEventListener("click", function (e) {
      var b = e.target.closest("button[data-mode]");
      if (b) applyTheme(b.getAttribute("data-mode"));
    });
  }

  // ---------- collapsible rail ----------
  function railSlim() { return document.documentElement.getAttribute("data-rail") === "slim"; }
  function setRail(slim) {
    if (slim) document.documentElement.setAttribute("data-rail", "slim");
    else document.documentElement.removeAttribute("data-rail");
    try { localStorage.setItem("rail", slim ? "slim" : "wide"); } catch (e) {}
  }
  var railToggle = document.getElementById("rail-toggle");
  if (railToggle) railToggle.addEventListener("click", function () { setRail(!railSlim()); });
  // Flyout labels in slim mode: position fixed on hover so the rail's own
  // scroll/overflow can never clip them.
  document.addEventListener("mouseover", function (e) {
    if (!railSlim()) return;
    var ri = e.target.closest(".rail .ri");
    if (!ri) return;
    var fly = ri.querySelector(".ri-fly");
    if (!fly) return;
    var r = ri.getBoundingClientRect();
    fly.style.left = (r.right + 8) + "px";
    fly.style.top = (r.top + r.height / 2) + "px";
  });
  // Live per-service counts, refreshed gently. textContent writes are no-ops
  // visually when the number hasn't changed, so there's nothing to de-jitter.
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
  var seq = 0;
  function toast(msg, kind) {
    var box = document.getElementById("toasts");
    if (!box) return;
    var el = document.createElement("div");
    el.className = "toast anim" + (kind === "err" ? " err" : "");
    el.innerHTML = '<span class="t-ic">' + (kind === "err" ? "⚠" : "✓") + '</span><span></span><span class="tclose">✕</span>';
    el.children[1].textContent = msg;
    el.querySelector(".tclose").onclick = function () { el.remove(); };
    box.appendChild(el);
    var id = ++seq;
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
  // Is this event about the live tail's own poll? A failing poll must not toast
  // every few seconds; it gets one persistent state on the live control instead.
  function isLivePoll(e) {
    var path = (e.detail && e.detail.requestConfig && e.detail.requestConfig.path) || "";
    var el = document.querySelector("[data-live]");
    var base = el && el.getAttribute("data-live");
    return !!base && path.indexOf(base) === 0;
  }
  function setDisconnected(on, why) {
    document.querySelectorAll("[data-live]").forEach(function (el) {
      el.classList.toggle("disconnected", on);
      var b = el.querySelector("[data-live-toggle]");
      if (b) b.title = on ? "Not updating: " + (why || "can't reach the broker") : (livePaused ? "Resume live updates" : "Pause live updates");
    });
  }
  // An error response is shown where the person is looking: in the failed form's
  // error slot if it has one, in the drawer if the drawer asked, else a toast.
  document.addEventListener("htmx:responseError", function (e) {
    var msg = errorText(e.detail.xhr);
    if (isLivePoll(e)) { setDisconnected(true, msg); return; }
    var target = e.detail.target;
    if (target && target.id === "drawer") {
      target.textContent = "";
      var box = document.createElement("div");
      box.className = "drawer-err";
      box.setAttribute("role", "alert");
      box.textContent = msg;
      target.appendChild(box);
      return;
    }
    var elt = e.detail.elt;
    var form = elt && elt.closest ? elt.closest("form") : null;
    var slot = form && form.querySelector("[data-err-slot]");
    if (slot) { slot.textContent = msg; slot.hidden = false; return; }
    toast(msg.length > 140 ? msg.slice(0, 140) + "…" : msg, "err");
  });
  // Nothing came back at all (the console server itself is gone).
  function networkFailure(e) {
    if (isLivePoll(e)) { setDisconnected(true, "the console server did not answer"); return; }
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
  // request shows a thin progress bar after half a second, a drawer opens on a
  // loading state rather than yesterday's message, and a form's old error clears
  // when it is resubmitted.
  var slowTimer = null, inflight = 0;
  var bar = document.getElementById("reqbar");
  document.addEventListener("htmx:beforeRequest", function (e) {
    if (isLivePoll(e)) return;
    var elt = e.detail.elt;
    var form = elt && elt.closest ? elt.closest("form") : null;
    var slot = form && form.querySelector("[data-err-slot]");
    if (slot) { slot.hidden = true; slot.textContent = ""; }
    var t = e.detail.target;
    if (t && t.id === "drawer") {
      t.textContent = "";
      var l = document.createElement("div");
      l.className = "drawer-loading";
      l.innerHTML = '<span class="spinner" aria-hidden="true"></span> Loading…';
      t.appendChild(l);
    }
    inflight++;
    if (!slowTimer && bar) slowTimer = setTimeout(function () { bar.hidden = false; }, 500);
  });
  function requestDone(e) {
    if (isLivePoll(e)) return;
    inflight = Math.max(0, inflight - 1);
    if (inflight === 0) {
      clearTimeout(slowTimer); slowTimer = null;
      if (bar) bar.hidden = true;
    }
  }
  document.addEventListener("htmx:afterRequest", requestDone);

  // ---------- copy ----------
  document.addEventListener("click", function (e) {
    var el = e.target.closest("[data-copy]");
    if (!el) return;
    navigator.clipboard.writeText(el.getAttribute("data-copy")).then(function () { toast("Copied"); });
  });

  // ---------- "Send again" (drawer → compose) ----------
  // Prefills the compose panel from a message in the drawer. Delegated,
  // because the drawer's content is swapped in per-message by htmx.
  document.addEventListener("click", function (e) {
    var b = e.target.closest("#drawer-resend");
    if (!b) return;
    var overlay = document.getElementById("drawer-overlay");
    if (overlay) overlay.hidden = true;
    var panel = document.getElementById("compose-panel");
    if (!panel) return;
    panel.classList.add("open");
    var form = panel.querySelector("form");
    var key = form.querySelector('input[name="key"]');
    if (key) key.value = b.getAttribute("data-key") || "";
    var sel = form.querySelector('select[name="partition"]');
    if (sel) sel.value = b.getAttribute("data-partition");
    var ta = form.querySelector('textarea[name="value"]');
    if (ta && window.dozeEditor) {
      dozeEditor.set(ta, b.getAttribute("data-value") || "");
      requestAnimationFrame(function () { dozeEditor.refresh(panel); });
    } else if (ta) {
      ta.value = b.getAttribute("data-value") || "";
    }
    panel.scrollIntoView({ behavior: "smooth", block: "center" });
  });

  // ---------- styled confirm (intercepts hx-confirm) ----------
  var confirmBox = document.getElementById("confirm");
  document.addEventListener("htmx:confirm", function (e) {
    var q = e.detail.question;
    if (!q || !confirmBox) return;
    e.preventDefault();
    document.getElementById("confirm-msg").textContent = q;
    confirmBox.hidden = false;
    var yes = document.getElementById("confirm-yes"), no = document.getElementById("confirm-no");
    function close() { confirmBox.hidden = true; yes.onclick = no.onclick = confirmBox.onclick = null; document.removeEventListener("keydown", onKey); }
    function onKey(ev) { if (ev.key === "Escape") close(); }
    yes.onclick = function () { close(); e.detail.issueRequest(true); };
    no.onclick = close;
    confirmBox.onclick = function (ev) { if (ev.target === confirmBox) close(); };
    document.addEventListener("keydown", onKey);
    yes.focus();
  });

  // ---------- command palette ----------
  var pal = document.getElementById("palette"), palQ = document.getElementById("pal-q"), palList = document.getElementById("pal-list");
  var palItems = [], palSel = 0;
  var NAV = [["topics","Topics","/topics"],["groups","Consumer groups","/groups"],["cluster","Cluster","/cluster"]];
  var ACTS = [["topics","Create topic","/new/topic"]];
  var KIND = { topics:"topic", groups:"group" };

  // ---------- recently visited resources ----------
  // The palette pins your last few resource pages on top: the queue you just
  // left costs ⌘K ⏎ instead of retyping its name.
  var SVCSET = { topics:1, groups:1 };
  function trackVisit() {
    try {
      var path = location.pathname;
      if (path.indexOf(PREFIX + "/") !== 0) return;
      var rest = path.slice(PREFIX.length + 1).split("/");
      var svc = rest[0];
      if (!SVCSET[svc]) return;
      var name = "";
      var u = path;
      if (rest.length >= 2 && rest[1]) {
        name = decodeURIComponent(rest[1]);
      }
      if (!name) return;
      var rec = JSON.parse(localStorage.getItem("recents") || "[]").filter(function (x) { return x.u !== u; });
      rec.unshift({ s: svc, n: name, u: u });
      localStorage.setItem("recents", JSON.stringify(rec.slice(0, 6)));
    } catch (e) {}
  }
  document.addEventListener("DOMContentLoaded", trackVisit);
  document.addEventListener("htmx:pushedIntoHistory", trackVisit);

  // ---------- rail active state ----------
  // The rail lives outside #workspace, so boosted navigation swaps the page
  // without repainting the server-rendered `.on` class. Recompute the active
  // item from the URL after every navigation (and on back/forward).
  // The section a URL belongs to: the console's home (and the create-topic form)
  // is the Topics section, which the old "starts with the rail link's path" test
  // missed — it left no rail item lit on the landing page.
  function sectionFor(path) {
    if (path.indexOf(PREFIX) !== 0) return "";
    var seg = path.slice(PREFIX.length).replace(/^\/+/, "").split("/")[0];
    if (seg === "" || seg === "topics" || seg === "new") return "topics";
    return seg;
  }
  var currentSection = sectionFor(location.pathname);
  function syncRail() {
    var section = sectionFor(location.pathname);
    document.querySelectorAll(".rail .ri").forEach(function (a) {
      var ip = new URL(a.href, location.origin).pathname;
      a.classList.toggle("on", sectionFor(ip) === section);
    });
    // A filter typed in one section means nothing in the next (topic names do
    // not filter groups), and used to carry over and hide everything.
    if (section !== currentSection) {
      currentSection = section;
      if (window.Alpine && Alpine.store("filter")) Alpine.store("filter").q = "";
    }
  }
  document.addEventListener("DOMContentLoaded", syncRail);
  document.addEventListener("htmx:pushedIntoHistory", syncRail);
  window.addEventListener("popstate", syncRail);

  function openPalette() {
    if (!pal) return;
    pal.hidden = false; palQ.value = ""; palSel = 0;
    var here = location.pathname + location.search;
    var recents = [];
    try {
      recents = JSON.parse(localStorage.getItem("recents") || "[]")
        .filter(function (x) { return x.u !== here; }).slice(0, 5)
        .map(function (x) { return { s: x.s, n: x.n, u: x.u, k: "recent" }; });
    } catch (e) {}
    var fixed = NAV.map(function (n) { return { s:n[0], n:n[1], u:PREFIX+n[2], k:"view" }; })
      .concat(ACTS.map(function (a) { return { s:a[0], n:a[1], u:PREFIX+a[2], k:"action" }; }));
    palItems = recents.concat(fixed);
    renderPal();
    palQ.focus();
    fetch(PREFIX + "/api/resources").then(function (r) { return r.json(); }).then(function (rs) {
      // Recents that no longer exist on the broker (deleted topics, dead
      // groups) would be dead links — drop them from the list AND from
      // localStorage so they stop resurfacing.
      var live = {};
      (rs || []).forEach(function (r) { live[r.u] = 1; });
      var pruned = recents.filter(function (x) { return live[x.u]; });
      if (pruned.length !== recents.length) {
        try {
          var stored = JSON.parse(localStorage.getItem("recents") || "[]")
            .filter(function (x) { return live[x.u]; });
          localStorage.setItem("recents", JSON.stringify(stored));
        } catch (e) {}
      }
      var seen = {};
      pruned.forEach(function (x) { seen[x.u] = 1; });
      var fresh = (rs || []).filter(function (r) { return !seen[r.u]; })
        .map(function (r) { r.k = KIND[r.s] || r.s; return r; });
      palItems = pruned.concat(fresh).concat(fixed);
      renderPal();
    }).catch(function () {}); // offline: the fixed entries are still there.
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
    if (!items.length) { palList.innerHTML = '<div class="pal-empty">Nothing matches</div>'; return; }
    items.forEach(function (it, i) {
      var a = document.createElement("a");
      a.className = "pal-item" + (i === palSel ? " sel" : "");
      a.href = it.u;
      a.innerHTML = '<span class="pal-dot"></span><span class="pal-name"></span><span class="pal-kind"></span>';
      // The dot encodes what the row IS (topic/group/view), not how it got
      // into the list — a "recent" topic still gets the topic hue.
      a.querySelector(".pal-dot").classList.add(
        it.s === "topics" ? "d-topic" : it.s === "groups" ? "d-group" : "d-view");
      a.querySelector(".pal-name").textContent = it.n;
      a.querySelector(".pal-kind").textContent = it.k;
      a.onmouseenter = function () { if (palSel !== i) { palSel = i; renderPal(); } };
      palList.appendChild(a);
    });
    // Keyboard selection can walk past the fold — keep it in view.
    var sel = palList.querySelector(".pal-item.sel");
    if (sel) sel.scrollIntoView({ block: "nearest" });
  }
  function closePalette() { if (pal) pal.hidden = true; }
  var opener = document.getElementById("palette-open");
  if (opener) opener.addEventListener("click", openPalette);
  if (palQ) palQ.addEventListener("input", function () { palSel = 0; renderPal(); });
  if (pal) pal.addEventListener("click", function (e) { if (e.target === pal) closePalette(); });

  // ---------- keyboard ----------
  var cursor = -1;
  function listRows() { return Array.prototype.slice.call(document.querySelectorAll(".listpane .li[href]")); }
  function resetListCursor() { cursor = -1; }
  function moveCursor(d) {
    var rows = listRows();
    if (!rows.length) return;
    cursor = Math.min(rows.length - 1, Math.max(0, cursor + d));
    rows.forEach(function (r, i) { r.classList.toggle("cursor", i === cursor); });
    rows[cursor].scrollIntoView({ block: "nearest" });
  }
  document.addEventListener("keydown", function (e) {
    var inPal = !pal.hidden;
    if (inPal) {
      var items = palFiltered();
      if (e.key === "Escape") { closePalette(); e.preventDefault(); }
      else if (e.key === "ArrowDown") { palSel = Math.min(palSel + 1, items.length - 1); renderPal(); e.preventDefault(); }
      else if (e.key === "ArrowUp") { palSel = Math.max(palSel - 1, 0); renderPal(); e.preventDefault(); }
      else if (e.key === "Enter") { var it = items[palSel]; if (it) location.href = it.u; e.preventDefault(); }
      return;
    }
    if ((e.metaKey || e.ctrlKey) && e.key === "k") { e.preventDefault(); openPalette(); return; }
    // ⌘⏎ submits the composer you're typing in — send, publish, invoke, put.
    // Works from plain textareas and from inside CodeMirror editors alike.
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
      var form = document.activeElement.closest ? document.activeElement.closest("form") : null;
      if (form && form.querySelector("textarea, .CodeMirror")) { e.preventDefault(); form.requestSubmit(); return; }
    }
    var tag = document.activeElement.tagName;
    if (/(INPUT|TEXTAREA|SELECT)/.test(tag) || document.activeElement.closest(".CodeMirror")) return;
    if (e.key === "/") {
      var f = document.querySelector(".listpane .filter input");
      if (f) { f.focus(); e.preventDefault(); } else openPalette();
    } else if (e.key === "j") moveCursor(1);
    else if (e.key === "k") moveCursor(-1);
    else if (e.key === "Enter" && cursor >= 0) { var r = listRows()[cursor]; if (r) r.click(); }
    else if (e.key === "c") { var nb = document.querySelector(".listpane .new-link"); if (nb) nb.click(); }
    else if (e.key === "m") {
      // Open the docked composer and put the caret in the value editor.
      var dk = document.getElementById("compose-panel");
      if (dk) {
        e.preventDefault();
        if (!dk.classList.contains("open")) {
          var bar = document.getElementById("compose-head");
          if (bar) bar.click(); else dk.classList.add("open");
        }
        var ta = dk.querySelector('textarea[name="value"]');
        requestAnimationFrame(function () { if (ta && ta.__cm) ta.__cm.focus(); else if (ta) ta.focus(); });
      }
    }
    else if (e.key === "[") { setRail(!railSlim()); }
    else if (e.key === "Escape") { var back = document.querySelector("[data-esc-back]"); if (back) history.back(); }
  });

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
  // The ?hl= row gets .hl server-side; scroll it into view once the page
  // (or a swap that carries it) lands.
  function revealHL() {
    var row = document.querySelector(".tbl tr[data-hl]");
    if (row) row.scrollIntoView({ block: "center" });
  }
  document.addEventListener("DOMContentLoaded", revealHL);

  // ---------- live polling with change detection ----------
  // Elements with data-live="URL" poll every data-live-ms (default 3000); the
  // server echoes a content hash, and when nothing changed it replies 204 and
  // nothing moves. Changed content REPLACES the element (outerHTML).
  //
  // Not a morph, deliberately. This used to say morph:outerHTML, but no element
  // carried hx-ext="morph", so htmx fell back to its default innerHTML swap and
  // inserted the response (itself a #tape-and-table) INSIDE the existing one on
  // the first change: two regions, two pollers, double the requests. Fixing it
  // by enabling morph on <body> broke unrelated swaps; a plain replace is the
  // boring thing that works. What a replace costs — the jump field's text and
  // focus, the table's scroll position — is saved and restored around the swap
  // (see preserveLiveState).
  var livePaused = false;
  function setPaused(p) {
    livePaused = p;
    document.querySelectorAll("[data-live-toggle]").forEach(function (b) {
      b.setAttribute("aria-pressed", p ? "true" : "false");
      b.classList.toggle("on", p);
      b.title = p ? "Resume live updates" : "Pause live updates";
    });
    document.querySelectorAll("[data-live]").forEach(function (el) { el.classList.toggle("paused", p); });
  }
  document.addEventListener("click", function (e) {
    if (e.target.closest("[data-live-toggle]")) setPaused(!livePaused);
  });
  var liveTimers = [];
  function setupLive() {
    liveTimers.forEach(clearInterval);
    liveTimers = [];
    document.querySelectorAll("[data-live]").forEach(function (el) {
      var id = el.id;
      var every = parseInt(el.getAttribute("data-live-ms") || "3000", 10);
      var t = setInterval(function () {
        // Re-query by id every tick: a plain outerHTML swap (composer send,
        // purge, redrive, delete) REPLACES the element, so a captured reference
        // would go stale and the poll would silently die. The morph poll itself
        // re-includes data-live, so a morphed element keeps the same id too.
        var cur = document.getElementById(id);
        if (document.hidden || livePaused || !cur) return;
        var url = cur.getAttribute("data-live");
        if (!url) return;
        var swap = cur.getAttribute("data-live-swap") || "outerHTML";
        var hash = cur.getAttribute("data-hash") || "";
        // htmx.ajax returns a promise that rejects when the request fails. A failed
        // poll is reported by the htmx:responseError / sendError handlers (as the
        // "not updating" state); left uncaught it is also an unhandled rejection.
        var p = htmx.ajax("GET", url + (url.indexOf("?") >= 0 ? "&" : "?") + "h=" + hash, {
          target: "#" + id, swap: swap,
        });
        if (p && p.catch) p.catch(function () {});
      }, every);
      liveTimers.push(t);
    });
  }
  document.addEventListener("DOMContentLoaded", setupLive);
  document.addEventListener("htmx:afterSwap", function (e) {
    // Re-arm after a navigation OR after a swap that lands (or contains) a live
    // region — including one whose target IS the data-live element itself, which
    // a descendant-only querySelector check would miss.
    var t = e.detail.target;
    if (t && (t.id === "workspace" ||
              (t.matches && t.matches("[data-live]")) ||
              (t.querySelector && t.querySelector("[data-live]")))) {
      setupLive();
      setPaused(livePaused); // a replaced region comes back unpaused-looking.
    }
  });

  // What a replace would otherwise destroy: the "go to offset" text and focus, and
  // the message table's scroll position. Saved just before a live swap, put back
  // just after it.
  var liveState = null;
  document.addEventListener("htmx:beforeSwap", function (e) {
    var t = e.detail.target;
    if (!t || !t.matches || !t.matches("[data-live]")) return;
    var jump = t.querySelector(".jump input");
    var wrap = t.querySelector(".tbl-wrap");
    liveState = {
      jumpValue: jump ? jump.value : "",
      jumpFocused: !!jump && document.activeElement === jump,
      selStart: jump ? jump.selectionStart : null,
      selEnd: jump ? jump.selectionEnd : null,
      scrollTop: wrap ? wrap.scrollTop : 0,
      atTop: !wrap || wrap.scrollTop < 4,
    };
  });
  document.addEventListener("htmx:afterSettle", function (e) {
    var st = liveState;
    liveState = null;
    var t = e.detail.target;
    if (!st) return;
    var region = document.querySelector("[data-live]");
    if (!region) return;
    var jump = region.querySelector(".jump input");
    if (jump && st.jumpValue) {
      jump.value = st.jumpValue;
      if (st.jumpFocused) {
        jump.focus();
        try { jump.setSelectionRange(st.selStart, st.selEnd); } catch (err) {}
      }
    }
    var wrap = region.querySelector(".tbl-wrap");
    // Someone reading down the table keeps their place; someone parked at the top
    // (the newest rows) just sees the new ones arrive.
    if (wrap && !st.atTop) wrap.scrollTop = st.scrollTop;
  });
  // After each poll, stamp the element's data-hash from the response header — a
  // morph swap doesn't reliably update the root's attributes, so without this the
  // poll would keep re-fetching the same change every tick.
  document.addEventListener("htmx:afterRequest", function (e) {
    var xhr = e.detail.xhr;
    if (!xhr) return;
    if (e.detail.successful && isLivePoll(e)) setDisconnected(false);
    var h = xhr.getResponseHeader("HX-Live-Hash");
    if (!h) return;
    var path = (e.detail.requestConfig && e.detail.requestConfig.path) || "";
    document.querySelectorAll("[data-live]").forEach(function (el) {
      var base = el.getAttribute("data-live");
      if (base && path.indexOf(base) === 0) el.setAttribute("data-hash", h);
    });
  });

  window.dozeShell = { toast: toast, openPalette: openPalette };
})();
