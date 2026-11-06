// compose.js: the open composer (Tp-10, Tp-11, Tp-12). It is a plain script loaded before
// Alpine, which reads topicsCompose when it meets the composer's form.

// ---------- topics: the composer (Tp-10, Tp-11, Tp-12) ----------
// The open composer's state. What it sends is the form's own fields (the hidden inputs
// in the template carry the state into them), so htmx posts it as it always did; what
// "What will be sent" says is the server's answer to the same fields (compose/preview),
// asked as they change. The JSON note under the value is checked here, as it is typed.
function topicsCompose(cfg) {
  var enc = new TextEncoder();
  return {
    parts: cfg.parts, fixed: cfg.fixed, partNote: cfg.partNote, subject: cfg.subject || "",
    part: "auto", acks: "1", key: "", keyB64: false, keyEmpty: false,
    mode: "json", valB64: false, value: "", hdrs: [], hdrJSON: false, hdrText: "", hdrError: "",
    pv: {}, _t: 0, _seq: 0,
    init: function () {
      var self = this;
      ["part", "fixed", "acks", "key", "keyB64", "keyEmpty", "mode", "valB64", "value", "subject", "hdrJSON", "hdrText"].forEach(function (k) {
        self.$watch(k, function () { self.ask(); });
      });
      this.$watch("hdrs", function () { self.ask(); });
      this.ask();
    },
    ask: function () {
      var self = this;
      clearTimeout(this._t);
      this._t = setTimeout(function () {
        var seq = ++self._seq;
        var body = new URLSearchParams(new FormData(self.$el));
        fetch(cfg.preview, { method: "POST", body: body, credentials: "same-origin" })
          .then(function (r) { return r.ok ? r.json() : null; })
          .then(function (j) { if (j && seq === self._seq) self.pv = j; })
          .catch(function () {});
      }, 120);
    },
    get validity() {
      var n = enc.encode(this.value).length;
      if (this.subject) {
        var s = this.pv.schema;
        return s ? { cls: s.ok ? "note ok" : "note bad", text: s.message } : { cls: "note", text: "Write the value as JSON." };
      }
      if (this.mode === "null") return { cls: "note", text: "Null value: no value at all is sent. On a compacted topic that is a delete." };
      if (this.valB64) {
        try { return { cls: "note", text: "Base64, " + atob(this.value.replace(/-/g, "+").replace(/_/g, "/")).length + " B once decoded. Sent as raw bytes." }; }
        catch (e) { return { cls: "note bad", text: "Not valid base64. Send it as text, or fix it." }; }
      }
      if (this.mode === "text") return { cls: "note", text: "Text, " + n + " B. Sent as typed." };
      if (!this.value.trim()) return { cls: "note", text: "Empty. An empty value is a record of zero bytes; use Null for a delete." };
      try { JSON.parse(this.value); return { cls: "note ok", text: "Valid JSON, " + n + " B. Checked as you type; the broker would accept any bytes." }; }
      catch (e) { return { cls: "note bad", text: "Not valid JSON: " + e.message + ". The broker would still accept it; choose Text to send it as typed." }; }
    },
    headersWire: function () {
      if (this.hdrJSON) return this.hdrText.trim();
      return JSON.stringify(this.hdrs);
    },
    headersAs: function (json) {
      if (json === this.hdrJSON) return;
      if (json) {
        this.hdrText = this.hdrs.length ? JSON.stringify(this.hdrs.map(function (h) { var o = { k: h.k, v: h.v }; if (h.b64) o.b64 = true; return o; })) : "";
        this.hdrError = "";
        this.hdrJSON = true;
        return;
      }
      var rows = this.parseHeaderJSON();
      if (rows === null) return;
      this.hdrs = rows;
      this.hdrJSON = false;
    },
    // parseHeaderJSON reads the JSON box as rows; null, with the reason in hdrError, when
    // it is not a JSON array of {k, v}.
    parseHeaderJSON: function () {
      var t = this.hdrText.trim();
      this.hdrError = "";
      if (!t) return [];
      try {
        var a = JSON.parse(t);
        if (!Array.isArray(a)) throw new Error("it is not an array");
        return a.map(function (o) {
          if (!o || typeof o !== "object" || typeof o.k !== "string" || typeof o.v !== "string") throw new Error("every entry needs a string k and a string v");
          return { k: o.k, v: o.v, b64: !!o.b64 };
        });
      } catch (e) {
        this.hdrError = 'Headers must be a JSON array of {"k": …, "v": …}: ' + e.message;
        return null;
      }
    },
    // beforeSend stops a send the form can see is wrong, before any request: the broker
    // never sees it, so no Kafka error exists for it (Tp-12).
    beforeSend: function (e) {
      if (!this.hdrJSON || this.parseHeaderJSON() !== null) return;
      e.preventDefault();
      var slot = this.$el.querySelector("[data-err-slot]");
      slot.replaceChildren();
      var box = document.createElement("div");
      box.className = "callout warn refusal";
      box.setAttribute("role", "alert");
      var h = document.createElement("div"); h.className = "row g8 ce";
      var b = document.createElement("span"); b.className = "b"; b.textContent = "Not sent";
      var m = document.createElement("span"); m.className = "m"; m.textContent = "header JSON";
      h.append(b, m);
      var msg = document.createElement("span"); msg.className = "m"; msg.textContent = this.hdrError;
      var why = document.createElement("span"); why.className = "note"; why.textContent = "Caught in the form, before any request. No Kafka error exists for this, because the broker never saw it. Fix the JSON, or switch back to Rows.";
      box.append(h, msg, why);
      slot.append(box);
      slot.hidden = false;
    },
    reset: function () {
      this.key = ""; this.value = ""; this.hdrs = []; this.hdrText = ""; this.hdrError = "";
      this.keyEmpty = false; this.keyB64 = false; this.valB64 = false; this.mode = "json";
      var slot = this.$el.querySelector("[data-err-slot]");
      if (slot) { slot.hidden = true; slot.replaceChildren(); }
    },
    // fill is "Edit a copy": the composer takes a stored record's parts for changing.
    fill: function (r) {
      this.key = r.key; this.keyB64 = r.keyB64; this.keyEmpty = r.keyEmpty;
      this.value = r.value; this.valB64 = r.valB64; this.mode = r.tombstone ? "null" : "json";
      this.hdrs = r.hdrs; this.hdrJSON = false; this.hdrError = "";
      this.subject = r.subject || "";
      this.part = "fixed"; this.fixed = r.partition;
    },
  };
}
