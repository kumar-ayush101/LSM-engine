// "Try it" panel. With a token it calls the owner API (/v1/...); without
// one, and if the server enables it, it uses the public demo sandbox
// (/v1/demo/...). The token is kept only in the form field (never stored)
// and sent only to this origin.
(function () {
  "use strict";
  var form = document.getElementById("kv");
  var out = document.getElementById("result");
  if (!form || !out) return;
  var demoOn = form.dataset.demo === "on";

  var lastOp = "get";
  form.querySelectorAll("button[data-op]").forEach(function (b) {
    b.addEventListener("click", function () { lastOp = b.dataset.op; });
  });

  function show(text, ok) {
    out.textContent = text;
    out.className = "result " + (ok ? "ok" : "err");
  }

  form.addEventListener("submit", async function (ev) {
    ev.preventDefault();
    var op = (ev.submitter && ev.submitter.dataset.op) || lastOp;
    var token = form.token.value.trim();
    var key = form.key.value;
    var demo = !token;

    if (demo && !demoOn) { show("Enter the access token first.", false); form.token.focus(); return; }
    if (op !== "stats" && !key) { show("Enter a key.", false); form.key.focus(); return; }

    var base = demo ? "/v1/demo" : "/v1";
    var url = op === "stats" ? base + "/stats" : base + "/kv/" + encodeURIComponent(key);
    var init = {
      method: { get: "GET", put: "PUT", delete: "DELETE", stats: "GET" }[op],
      headers: {},
      cache: "no-store",
      credentials: "omit",
    };
    if (!demo) init.headers["Authorization"] = "Bearer " + token;
    if (op === "put") {
      init.body = form.value.value;
      init.headers["Content-Type"] = "application/octet-stream";
    }

    var buttons = form.querySelectorAll("button");
    buttons.forEach(function (b) { b.disabled = true; });
    var label = init.method + " " + (op === "stats" ? url : base + "/kv/" + key) + (demo ? "  (demo sandbox)" : "");
    show("Sending " + label + " ...", true);
    var t0 = performance.now();
    try {
      var resp = await fetch(url, init);
      var body = await resp.text();
      var ms = Math.round(performance.now() - t0);
      var line = resp.status + " " + resp.statusText + " (" + ms + " ms)  " + label;
      if (resp.status === 204) {
        body = op === "put" ? "Stored. It is in the write-ahead log and the memtable." : "Deleted: a tombstone was written (or the key did not exist).";
      } else if (op === "stats" && resp.ok) {
        try { body = JSON.stringify(JSON.parse(body), null, 2); } catch (e) { /* show raw */ }
      } else if (resp.status === 404 && op === "get") {
        body = "Key not found.";
      } else if (resp.status === 429) {
        body = "Rate limit reached. Wait " + (resp.headers.get("Retry-After") || "a few") + " seconds and try again.";
      } else if (resp.status === 401) {
        body = "The token is not valid. Clear the token field to use the public demo instead.";
      }
      show(line + "\n\n" + body, resp.ok);
    } catch (e) {
      show("Request failed: " + e.message + "\nThe service may be waking up from idle; try again in a few seconds.", false);
    } finally {
      buttons.forEach(function (b) { b.disabled = false; });
    }
  });
})();
