// "Try it" panel: sends requests to this server's own API. The token is
// kept only in the form field (never stored) and sent only to this origin.
(function () {
  "use strict";
  var form = document.getElementById("kv");
  var out = document.getElementById("result");
  if (!form || !out) return;

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

    if (!token) { show("Enter the access token first.", false); form.token.focus(); return; }
    if (op !== "stats" && !key) { show("Enter a key.", false); form.key.focus(); return; }

    var url = op === "stats" ? "/v1/stats" : "/v1/kv/" + encodeURIComponent(key);
    var init = {
      method: { get: "GET", put: "PUT", delete: "DELETE", stats: "GET" }[op],
      headers: { "Authorization": "Bearer " + token },
      cache: "no-store",
      credentials: "omit",
    };
    if (op === "put") {
      init.body = form.value.value;
      init.headers["Content-Type"] = "application/octet-stream";
    }

    var buttons = form.querySelectorAll("button");
    buttons.forEach(function (b) { b.disabled = true; });
    show("Sending " + init.method + " " + (op === "stats" ? url : "/v1/kv/" + key) + " ...", true);
    var t0 = performance.now();
    try {
      var resp = await fetch(url, init);
      var body = await resp.text();
      var ms = Math.round(performance.now() - t0);
      var line = resp.status + " " + resp.statusText + " (" + ms + " ms)";
      if (resp.status === 204) {
        body = op === "put" ? "Stored." : "Deleted (or key did not exist).";
      } else if (op === "stats" && resp.ok) {
        try { body = JSON.stringify(JSON.parse(body), null, 2); } catch (e) { /* show raw */ }
      } else if (resp.status === 404 && op === "get") {
        body = "Key not found.";
      }
      show(line + "\n\n" + body, resp.ok);
    } catch (e) {
      show("Request failed: " + e.message + "\nThe service may be waking up from idle; try again in a few seconds.", false);
    } finally {
      buttons.forEach(function (b) { b.disabled = false; });
    }
  });
})();
