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
  document.addEventListener("htmx:responseError", function (e) {
    var raw = (e.detail.xhr.responseText || "Request failed");
    // The server HTML-escapes error bodies; decode entities first so the message
    // regex matches and the toast shows real quotes/brackets, not "&#34;".
    var ta = document.createElement("textarea");
    ta.innerHTML = raw;
    raw = ta.value;
    var m = raw.match(/<Message>([^<]+)|"message"\s*:\s*"([^"]+)"/i);
    var msg = (m && (m[1] || m[2])) || raw.replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
    if (msg.length > 140) msg = msg.slice(0, 140) + "…";
    toast(msg, "err");
  });

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
  function syncRail() {
    var here = location.pathname;
    document.querySelectorAll(".rail .ri").forEach(function (a) {
      var ip = new URL(a.href, location.origin).pathname;
      var on = ip.charAt(ip.length - 1) === "/"
        ? (here === ip || here === ip.slice(0, -1))       // Flows (root)
        : (here === ip || here.indexOf(ip + "/") === 0);  // a service section
      a.classList.toggle("on", on);
    });
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
    });
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

  // ---------- jump-to-offset landing ----------
  // The ?hl= row gets .hl server-side; scroll it into view once the page
  // (or a swap that carries it) lands.
  function revealHL() {
    var row = document.querySelector(".tbl tr[data-hl]");
    if (row) row.scrollIntoView({ block: "center" });
  }
  document.addEventListener("DOMContentLoaded", revealHL);

  // ---------- live polling with change detection ----------
  // Elements with data-live="URL" poll every data-live-ms (default 3000);
  // the server echoes a content hash — when unchanged it replies 204 and
  // nothing moves. Changed content is morph-swapped so selection survives.
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
        if (document.hidden || !cur) return;
        var url = cur.getAttribute("data-live");
        if (!url) return;
        // Most live regions morph so selection/scroll survive; a small self-
        // contained element can opt into a plain outerHTML swap via
        // data-live-swap to avoid idiomorph nesting its replacement.
        var swap = cur.getAttribute("data-live-swap") || "morph:outerHTML";
        var hash = cur.getAttribute("data-hash") || "";
        htmx.ajax("GET", url + (url.indexOf("?") >= 0 ? "&" : "?") + "h=" + hash, {
          target: "#" + id, swap: swap,
        });
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
              (t.querySelector && t.querySelector("[data-live]")))) setupLive();
  });
  // After each poll, stamp the element's data-hash from the response header — a
  // morph swap doesn't reliably update the root's attributes, so without this the
  // poll would keep re-fetching the same change every tick.
  document.addEventListener("htmx:afterRequest", function (e) {
    var xhr = e.detail.xhr;
    if (!xhr) return;
    var h = xhr.getResponseHeader("HX-Live-Hash");
    if (!h) return;
    var path = (e.detail.requestConfig && e.detail.requestConfig.path) || "";
    document.querySelectorAll("[data-live]").forEach(function (el) {
      var base = el.getAttribute("data-live");
      if (base && path.indexOf(base) === 0) el.setAttribute("data-hash", h);
    });
  });

  // ---------- sleep countdowns ----------
  // A .rt-cd inside an element carrying data-sleep-left="<seconds>" counts down
  // every second. The server sends seconds-remaining (not an absolute time), and
  // each fresh element re-bases it against the browser's own clock — so a
  // browser/server clock skew can't drift the countdown. The 3s live poll swaps
  // in a fresh element (with a new _deadline) only on a reset; the going-cold
  // flip is server-authoritative, arriving as a swapped-in grey badge.
  function fmtLeft(s) {
    return s >= 60 ? Math.floor(s / 60) + "m " + String(s % 60).padStart(2, "0") + "s" : s + "s";
  }
  setInterval(function () {
    document.querySelectorAll(".rt-cd").forEach(function (cd) {
      var host = cd.closest("[data-sleep-left]");
      if (!host) return;
      if (host._deadline == null) {
        host._deadline = Date.now() + parseInt(host.getAttribute("data-sleep-left") || "0", 10) * 1000;
      }
      var left = Math.round((host._deadline - Date.now()) / 1000);
      cd.textContent = left > 0 ? fmtLeft(left) : "any moment";
    });
  }, 1000);

  window.dozeShell = { toast: toast, openPalette: openPalette };
})();
