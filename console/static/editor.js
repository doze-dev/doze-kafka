// editor.js — upgrades every textarea[data-editor] into a CodeMirror editor
// with JSON/YAML highlighting. Mode auto-detects from content ('{' or '[' →
// JSON, otherwise YAML) unless data-editor names one explicitly. Values sync
// back to the textarea before any htmx request or native form submit.
//
// data-editor="json" additionally gets a first-class JSON experience: a live
// validity pill, a Format button, and an inline error marker at the exact line
// where parsing fails.
(function () {
  "use strict";

  function detectMode(text, hint) {
    if (hint === "sql") return "text/x-sql";
    if (hint === "json" || hint === "yaml") return hint === "json" ? { name: "javascript", json: true } : "yaml";
    var t = (text || "").trimStart();
    if (t.startsWith("{") || t.startsWith("[")) return { name: "javascript", json: true };
    return "yaml";
  }

  // jsonError parses text and, on failure, returns {message, line}. The
  // character offset in V8's "...at position N" message is mapped to a line;
  // engines without it still surface the message (line -1 = no marker).
  function jsonError(text) {
    if (!text.trim()) return null;
    try { JSON.parse(text); return null; }
    catch (e) {
      var msg = String(e.message || "invalid JSON").replace(/^JSON\.parse:\s*/, "");
      var m = /position (\d+)/.exec(msg);
      var line = -1;
      if (m) {
        var pos = parseInt(m[1], 10), l = 0;
        for (var i = 0; i < pos && i < text.length; i++) if (text.charCodeAt(i) === 10) l++;
        line = l;
        msg = msg.replace(/\s*in JSON at position \d+/, "").replace(/\s*at position \d+/, "");
      } else {
        var lm = /line (\d+)/.exec(msg);
        if (lm) line = parseInt(lm[1], 10) - 1;
      }
      return { message: msg, line: line };
    }
  }

  var GUTTER = "cm-json-gutter";

  // attachJsonChrome adds the validity toolbar and wires live linting.
  function attachJsonChrome(cm) {
    var wrap = cm.getWrapperElement();
    var bar = document.createElement("div");
    bar.className = "cm-jsonbar";
    bar.innerHTML = '<span class="cm-valid" aria-live="polite"></span>' +
      '<button type="button" class="cm-fmt" title="Prettify (2-space indent)">Format</button>';
    wrap.parentNode.insertBefore(bar, wrap);
    var pill = bar.querySelector(".cm-valid");
    bar.querySelector(".cm-fmt").addEventListener("click", function () {
      try { cm.setValue(JSON.stringify(JSON.parse(cm.getValue()), null, 2)); }
      catch (e) { /* invalid — the pill already says why */ }
    });

    var errLine = -1;
    function validate() {
      cm.clearGutter(GUTTER);
      if (errLine >= 0) { cm.removeLineClass(errLine, "background", "cm-error-line"); errLine = -1; }
      var text = cm.getValue();
      var err = jsonError(text);
      if (!text.trim()) { pill.className = "cm-valid"; pill.textContent = ""; return; }
      if (!err) { pill.className = "cm-valid ok"; pill.textContent = "✓ valid JSON"; return; }
      pill.className = "cm-valid bad";
      pill.textContent = "✗ " + (err.line >= 0 ? "line " + (err.line + 1) + ": " : "") + err.message;
      if (err.line >= 0 && err.line < cm.lineCount()) {
        var mark = document.createElement("span");
        mark.className = "cm-gutter-err"; mark.textContent = "●"; mark.title = err.message;
        cm.setGutterMarker(err.line, GUTTER, mark);
        cm.addLineClass(err.line, "background", "cm-error-line");
        errLine = err.line;
      }
    }
    cm.on("change", validate);
    validate();
  }

  function upgrade(ta) {
    if (ta.__cm || typeof CodeMirror === "undefined") return;
    var hint = ta.getAttribute("data-editor");
    var isJSON = hint === "json";
    var explicit = hint === "json" || hint === "yaml" || hint === "sql";
    var opts = {
      mode: detectMode(ta.value, hint),
      lineNumbers: true,
      matchBrackets: true,
      autoCloseBrackets: true,
      viewportMargin: Infinity,
      tabSize: 2,
      indentWithTabs: false,
      placeholder: ta.placeholder || "",
    };
    if (isJSON) opts.gutters = ["CodeMirror-linenumbers", GUTTER];
    var cm = CodeMirror.fromTextArea(ta, opts);
    ta.__cm = cm;
    cm.on("change", function () {
      cm.save();
      if (!explicit) { // only auto-detect when the caller didn't name a mode
        var m = detectMode(cm.getValue(), null);
        var cur = cm.getOption("mode");
        var curIsJSON = typeof cur === "object" && cur.json;
        var nextIsJSON = typeof m === "object" && m.json;
        if (curIsJSON !== nextIsJSON) cm.setOption("mode", m);
      }
      // Re-emit as a bubbling input event so live listeners (debounced htmx
      // triggers, dirty flags) hear edits made inside the editor.
      ta.dispatchEvent(new Event("input", { bubbles: true }));
    });
    cm.setSize("100%", null);
    if (isJSON) attachJsonChrome(cm);
  }

  function upgradeAll(root) {
    (root || document).querySelectorAll("textarea[data-editor]").forEach(upgrade);
  }

  // Sync every editor back to its textarea before htmx serializes the form.
  document.addEventListener("htmx:configRequest", function () {
    document.querySelectorAll("textarea[data-editor]").forEach(function (ta) {
      if (ta.__cm) ta.__cm.save();
    });
  });
  document.addEventListener("submit", function () {
    document.querySelectorAll("textarea[data-editor]").forEach(function (ta) {
      if (ta.__cm) ta.__cm.save();
    });
  }, true);

  document.addEventListener("DOMContentLoaded", function () { upgradeAll(); });
  // For outerHTML swaps htmx reports the OLD (detached) node as e.detail.target,
  // so upgrading from it never reaches the swapped-in content. upgrade() is
  // idempotent (skips textareas already carrying a CodeMirror), so a cheap
  // document-wide pass correctly attaches editors to newly-swapped content.
  document.addEventListener("htmx:afterSwap", function () { upgradeAll(); });

  // Programmatic access (e.g. "Edit item" prefills the put-item editor).
  window.dozeEditor = {
    set: function (ta, value) {
      if (typeof ta === "string") ta = document.querySelector(ta);
      if (!ta) return;
      if (ta.__cm) { ta.__cm.setValue(value); ta.__cm.save(); ta.__cm.refresh(); }
      else ta.value = value;
    },
    refresh: function (root) {
      (root || document).querySelectorAll("textarea[data-editor]").forEach(function (ta) {
        if (ta.__cm) ta.__cm.refresh();
      });
    },
    upgradeAll: upgradeAll,
  };

  // Fetch a random password from Secrets Manager and drop it into the panel's
  // editor as a starter JSON secret. Used by the "Generate password" button.
  window.dozeGenPassword = function (btn, prefix) {
    var ta = btn.closest(".panel").querySelector("textarea[data-editor]");
    if (!ta) return;
    btn.disabled = true;
    fetch(prefix + "/sm/password").then(function (r) { return r.text(); }).then(function (pw) {
      var cur = (ta.__cm ? ta.__cm.getValue() : ta.value).trim();
      var next;
      try {
        var obj = cur ? JSON.parse(cur) : {};
        obj.password = pw;
        next = JSON.stringify(obj, null, 2);
      } catch (e) {
        next = JSON.stringify({ password: pw }, null, 2);
      }
      window.dozeEditor.set(ta, next);
    }).finally(function () { btn.disabled = false; });
  };
})();
