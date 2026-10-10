// editor.js — upgrades every textarea[data-editor] into a CodeMirror 6 editor.
// The language follows the content: text that starts with '{' or '[' is highlighted
// as JSON, anything else is plain text. Values sync back to the textarea on every
// change, so htmx and a native form submit read what is on screen.
//
// data-editor="json" additionally gets a first-class JSON experience — a live
// validity pill, a Format button, and an inline error marker at the line where
// parsing fails — but only for content that looks like JSON. A payload that is
// plain text, or base64 for binary bytes, is not an error, so it is not flagged
// as one. The textarea's data-validate="off" (with data-why) switches validation
// off and says why.
//
// CodeMirror 6 arrives as the window.CM6 global from static/cm6.min.js, which is
// built from console/cm6/ (see static/VERSIONS).
(function () {
  "use strict";

  function looksLikeJSON(text) {
    var t = (text || "").trimStart();
    return t.startsWith("{") || t.startsWith("[");
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

  // attachJsonChrome adds the validity bar above the editor and wires live linting.
  function attachJsonChrome(api, ta, host) {
    var C = window.CM6;
    var bar = document.createElement("div");
    bar.className = "cm-jsonbar";
    bar.innerHTML = '<span class="cm-valid" aria-live="polite"></span>' +
      '<button type="button" class="cm-fmt" title="Prettify (2-space indent)">Format</button>';
    host.parentNode.insertBefore(bar, host);
    var pill = bar.querySelector(".cm-valid");
    bar.querySelector(".cm-fmt").addEventListener("click", function () {
      try { api.setValue(JSON.stringify(JSON.parse(api.getValue()), null, 2)); }
      catch (e) { /* invalid — the pill already says why */ }
    });

    function parseErrorLine() {
      var st = api.view.state;
      var tree = C.ensureSyntaxTree(st, st.doc.length, 50);
      var at = -1;
      if (tree) tree.iterate({ enter: function (n) { if (at < 0 && n.type.isError) { at = n.from; return false; } } });
      return at < 0 ? -1 : st.doc.lineAt(at).number - 1;
    }
    function mark(diags) { api.view.dispatch(C.setDiagnostics(api.view.state, diags)); }
    function validate() {
      var text = api.getValue();
      if (ta.getAttribute("data-validate") === "off") {
        pill.className = "cm-valid"; pill.textContent = ta.getAttribute("data-why") || "";
        mark([]); return;
      }
      if (!text.trim()) { pill.className = "cm-valid"; pill.textContent = ""; mark([]); return; }
      if (!looksLikeJSON(text)) { pill.className = "cm-valid"; pill.textContent = "plain text"; mark([]); return; }
      var err = jsonError(text);
      // Newer engines name the token but not where it is; the editor's own parser
      // knows, so ask it for the first error it found.
      if (err && err.line < 0) err.line = parseErrorLine();
      if (!err) { pill.className = "cm-valid ok"; pill.textContent = "✓ valid JSON"; mark([]); return; }
      pill.className = "cm-valid bad";
      pill.textContent = "✗ " + (err.line >= 0 ? "line " + (err.line + 1) + ": " : "") + err.message;
      var doc = api.view.state.doc;
      if (err.line >= 0 && err.line < doc.lines) {
        var ln = doc.line(err.line + 1);
        mark([{ from: ln.from, to: Math.max(ln.to, Math.min(ln.from + 1, doc.length)), severity: "error", message: err.message }]);
      } else mark([]);
    }
    api.onChange = function () {
      // A view update cannot start another one; validate once it has finished.
      Promise.resolve().then(validate);
    };
    api.revalidate = validate;
    validate();
  }

  function upgrade(ta) {
    var C = window.CM6;
    if (ta.__cm || !C) return;
    var isJSON = ta.getAttribute("data-editor") === "json";
    var host = document.createElement("div");
    host.className = "cm-host";
    ta.parentNode.insertBefore(host, ta.nextSibling);
    ta.style.display = "none";

    var lang = new C.Compartment();
    var curJSON = looksLikeJSON(ta.value);
    var api = { view: null, onChange: null, revalidate: null };

    // Command-Enter belongs to the page (it sends the composer); the default
    // keymap would use it to insert a blank line.
    var keys = C.defaultKeymap.filter(function (k) { return k.key !== "Mod-Enter"; });
    var extensions = [
      C.lineNumbers(),
      C.history(),
      C.drawSelection(),
      C.bracketMatching(),
      C.closeBrackets(),
      C.indentUnit.of("  "),
      C.EditorState.tabSize.of(2),
      C.syntaxHighlighting(C.classHighlighter),
      C.keymap.of(C.closeBracketsKeymap.concat(keys, C.historyKeymap)),
      C.placeholder(ta.placeholder || ""),
      lang.of(curJSON ? C.json() : []),
      C.EditorView.updateListener.of(function (u) {
        if (!u.docChanged) return;
        var text = u.state.doc.toString();
        ta.value = text;
        var next = looksLikeJSON(text);
        if (next !== curJSON) {
          curJSON = next;
          Promise.resolve().then(function () {
            api.view.dispatch({ effects: lang.reconfigure(next ? C.json() : []) });
          });
        }
        if (api.onChange) api.onChange();
      }),
    ];
    if (isJSON) extensions.push(C.lintGutter());

    var view = new C.EditorView({
      state: C.EditorState.create({ doc: ta.value, extensions: extensions }),
      parent: host,
    });
    api.view = view;
    api.getValue = function () { return view.state.doc.toString(); };
    api.setValue = function (v) {
      view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: v } });
    };
    api.save = function () { ta.value = api.getValue(); };
    api.focus = function () { view.focus(); };
    api.refresh = function () { view.requestMeasure(); };
    ta.__cm = api;
    if (isJSON) attachJsonChrome(api, ta, host);
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
  // idempotent (skips textareas already carrying an editor), so a cheap
  // document-wide pass correctly attaches editors to newly-swapped content.
  document.addEventListener("htmx:afterSwap", function () { upgradeAll(); });

  // For shell.js: "Edit a copy" fills the composer's editor, and the composer
  // switches validation off for base64 and null values.
  window.dozeEditor = {
    set: function (ta, value) {
      if (typeof ta === "string") ta = document.querySelector(ta);
      if (!ta) return;
      if (ta.__cm) { ta.__cm.setValue(value); ta.__cm.save(); }
      else ta.value = value;
    },
    refresh: function (root) {
      (root || document).querySelectorAll("textarea[data-editor]").forEach(function (ta) {
        if (ta.__cm) ta.__cm.refresh();
      });
    },
    upgradeAll: upgradeAll,
    // Turn JSON validation on or off for an editor (and say why when off).
    setValidate: function (ta, on, why) {
      if (typeof ta === "string") ta = document.querySelector(ta);
      if (!ta) return;
      ta.setAttribute("data-validate", on ? "on" : "off");
      ta.setAttribute("data-why", on ? "" : (why || ""));
      if (ta.__cm && ta.__cm.revalidate) ta.__cm.revalidate();
    },
  };
})();
