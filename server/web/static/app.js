// Landing page behaviour:
//  - "Try it" panel: with a token it calls the owner API (/v1/...); without
//    one, if enabled, it uses the public demo sandbox (/v1/demo/...). The
//    token is kept only in the form field and sent only to this origin.
//  - Live stats: refreshed from the public /stats.json (aggregate counters
//    only) every 10 s while the tab is visible, and after each request.
(function () {
  "use strict";

  var STATUS_TEXT = {
    200: "OK", 204: "No Content", 400: "Bad Request", 401: "Unauthorized",
    404: "Not Found", 405: "Method Not Allowed", 413: "Payload Too Large",
    414: "URI Too Long", 429: "Too Many Requests", 500: "Internal Server Error",
    503: "Service Unavailable", 507: "Insufficient Storage",
  };

  function humanBytes(n) {
    if (n < 1024) return n + " B";
    var units = ["KiB", "MiB", "GiB"], i = -1;
    do { n /= 1024; i++; } while (n >= 1024 && i < units.length - 1);
    return n.toFixed(1) + " " + units[i];
  }

  function humanDuration(s) {
    var d = Math.floor(s / 86400), h = Math.floor(s % 86400 / 3600), m = Math.floor(s % 3600 / 60);
    if (d) return d + "d " + h + "h";
    if (h) return h + "h " + m + "m";
    if (m) return m + "m " + (s % 60) + "s";
    return s + "s";
  }

  // ---------- live stats ----------
  function stat(name) { return document.querySelector('[data-stat="' + name + '"]'); }

  function setStat(name, text) {
    var el = stat(name);
    if (!el || el.textContent === text) return;
    el.textContent = text;
    var card = el.closest(".stat");
    if (card) {
      card.classList.remove("bump");
      void card.offsetWidth; // restart the animation
      card.classList.add("bump");
    }
  }

  function setMeter(used, limit) {
    var fill = stat("mem_fill"), meter = stat("mem_meter");
    if (!fill || !meter || !limit) return;
    var pct = Math.min(100, used / limit * 100);
    fill.style.width = pct.toFixed(2) + "%";
    meter.setAttribute("aria-valuenow", String(Math.round(pct)));
  }

  async function refreshStats() {
    try {
      var resp = await fetch("/stats.json", { cache: "no-store", credentials: "omit" });
      if (!resp.ok) return;
      var s = await resp.json();
      setStat("entries", String(s.entries));
      setStat("last_seq", String(s.last_seq));
      setStat("wal_bytes", humanBytes(s.wal_bytes));
      setStat("memtable_bytes", humanBytes(s.memtable_bytes));
      setStat("sync_policy", s.sync_policy);
      var up = stat("uptime");
      if (up) up.textContent = humanDuration(s.uptime_seconds);
      setMeter(s.memtable_bytes, s.memtable_limit);
    } catch (e) { /* offline or waking up; try again next tick */ }
  }

  refreshStats();
  setInterval(function () {
    if (document.visibilityState === "visible") refreshStats();
  }, 10000);

  // ---------- try it ----------
  var form = document.getElementById("kv");
  var out = document.getElementById("result");
  var history = document.getElementById("history");
  if (!form || !out) return;
  var demoOn = form.dataset.demo === "on";

  form.querySelectorAll("[data-fill-key]").forEach(function (b) {
    b.addEventListener("click", function () {
      form.key.value = b.dataset.fillKey;
      form.value.value = b.dataset.fillValue;
      form.key.focus();
    });
  });

  var lastOp = "get";
  form.querySelectorAll("button[data-op]").forEach(function (b) {
    b.addEventListener("click", function () { lastOp = b.dataset.op; });
  });

  // Renders the response with DOM nodes (never innerHTML), so stored values
  // are always shown as text.
  function show(parts) {
    out.textContent = "";
    parts.forEach(function (p) {
      var span = document.createElement("span");
      if (p.cls) span.className = p.cls;
      span.textContent = p.text;
      out.appendChild(span);
    });
  }

  function addHistory(code, label, ms) {
    if (!history) return;
    var li = document.createElement("li");
    var c = document.createElement("span");
    c.className = code >= 200 && code < 300 ? "code-ok" : "code-err";
    c.textContent = code || "ERR";
    var l = document.createElement("span");
    l.textContent = label + (ms != null ? "  " + ms + " ms" : "");
    li.appendChild(c);
    li.appendChild(l);
    history.insertBefore(li, history.firstChild);
    while (history.children.length > 6) history.removeChild(history.lastChild);
  }

  form.addEventListener("submit", async function (ev) {
    ev.preventDefault();
    var op = (ev.submitter && ev.submitter.dataset.op) || lastOp;
    var token = form.token.value.trim();
    var key = form.key.value;
    var demo = !token;

    if (demo && !demoOn) {
      var det = form.querySelector("details");
      if (det) det.open = true;
      show([{ cls: "st-err", text: "Enter the access token first." }]);
      form.token.focus();
      return;
    }
    if (op !== "stats" && !key) {
      show([{ cls: "st-err", text: "Enter a key first" }, { text: " (or pick a quick-start example)." }]);
      form.key.focus();
      return;
    }

    var base = demo ? "/v1/demo" : "/v1";
    var url = op === "stats" ? base + "/stats" : base + "/kv/" + encodeURIComponent(key);
    var method = { get: "GET", put: "PUT", delete: "DELETE", stats: "GET" }[op];
    var init = { method: method, headers: {}, cache: "no-store", credentials: "omit" };
    if (!demo) init.headers["Authorization"] = "Bearer " + token;
    if (op === "put") {
      init.body = form.value.value;
      init.headers["Content-Type"] = "application/octet-stream";
    }

    var label = method + " " + (op === "stats" ? url : base + "/kv/" + key);
    var buttons = form.querySelectorAll("button");
    buttons.forEach(function (b) { b.disabled = true; });
    show([{ cls: "req", text: "$ " + label }, { cls: "muted", text: "\n  sending..." }]);
    var t0 = performance.now();
    try {
      var resp = await fetch(url, init);
      var body = await resp.text();
      var ms = Math.round(performance.now() - t0);
      var note = "";
      if (resp.status === 204) {
        note = op === "put"
          ? "Stored. Appended to the write-ahead log, fsynced, then inserted into the memtable."
          : "Deleted. A tombstone was written; it shadows any older version of the key.";
        body = "";
      } else if (op === "stats" && resp.ok) {
        try { body = JSON.stringify(JSON.parse(body), null, 2); } catch (e) { /* show raw */ }
      } else if (resp.status === 404 && op === "get") {
        note = "Key not found (never written, or its newest version is a tombstone).";
        body = "";
      } else if (resp.status === 429) {
        note = "Rate limit reached. Wait " + (resp.headers.get("Retry-After") || "a few") + " seconds and try again.";
        body = "";
      } else if (resp.status === 401) {
        note = "The token is not valid." + (demoOn ? " Clear it to use the public demo instead." : "");
        body = "";
      }
      var ok = resp.ok || (resp.status === 404 && op === "get");
      var parts = [
        { cls: "req", text: "$ " + label + (demo ? "   # demo sandbox" : "") + "\n" },
        { cls: ok ? "st-ok" : "st-err", text: resp.status + " " + (resp.statusText || STATUS_TEXT[resp.status] || "") },
        { cls: "muted", text: "  in " + ms + " ms\n\n" },
      ];
      if (body) parts.push({ text: body + (body.endsWith("\n") ? "" : "\n") });
      if (note) parts.push({ cls: "muted", text: note });
      show(parts);
      addHistory(resp.status, label, ms);
      if (op !== "get") refreshStats();
    } catch (e) {
      show([
        { cls: "req", text: "$ " + label + "\n" },
        { cls: "st-err", text: "Request failed: " + e.message + "\n" },
        { cls: "muted", text: "The service may be waking up from idle; try again in a few seconds." },
      ]);
      addHistory(0, label, null);
    } finally {
      buttons.forEach(function (b) { b.disabled = false; });
    }
  });
})();
