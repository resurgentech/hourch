// hourchd UI: re-render the live region when /ui/events reports a change,
// and stream a job's log from /ui/jobs/{id}/log/stream.
(function () {
  "use strict";
  var status = document.getElementById("stream");
  var live = document.getElementById("live");

  // ---- live region ----
  if (window.EventSource && live) {
    var timer = null;
    var editing = function () {
      var el = document.activeElement;
      return el && live.contains(el) && (el.tagName === "INPUT" || el.tagName === "SELECT" || el.tagName === "TEXTAREA");
    };
    var refresh = function () {
      timer = null;
      if (editing()) { schedule(); return; }
      fetch(location.pathname + location.search, { headers: { "X-Partial": "1" }, credentials: "same-origin" })
        .then(function (r) { return r.ok ? r.text() : null; })
        .then(function (html) { if (html !== null && !editing()) live.innerHTML = html; })
        .catch(function () {});
    };
    var schedule = function () {
      if (!timer) timer = setTimeout(refresh, 300);
    };
    var es = new EventSource("/ui/events");
    ["run", "job", "worker", "resync"].forEach(function (name) {
      es.addEventListener(name, schedule);
    });
    es.onopen = function () { status.textContent = "live"; status.className = "ok"; };
    es.onerror = function () { status.textContent = "live: reconnecting"; status.className = "bad"; };
    // Ages and the oldest-ready age move without events.
    setInterval(schedule, 15000);
  } else if (status) {
    status.textContent = "live: unsupported";
  }

  // ---- job log ----
  var log = document.getElementById("log");
  if (!log || !window.EventSource) return;
  var follow = document.getElementById("follow");
  var state = document.getElementById("logstate");
  var stick = function () {
    if (follow && follow.checked) {
      log.scrollTop = log.scrollHeight;
      window.scrollTo(0, document.body.scrollHeight);
    }
  };
  // EventSource resumes with Last-Event-ID (the byte offset) after a drop.
  var src = new EventSource(log.getAttribute("data-src"));
  src.onopen = function () { state.textContent = "streaming"; };
  src.addEventListener("log", function (e) {
    var d = JSON.parse(e.data);
    log.appendChild(document.createTextNode(d.data));
    stick();
  });
  src.addEventListener("end", function (e) {
    var d = JSON.parse(e.data);
    src.close();
    state.textContent = "ended: " + d.status + (d.exit_code !== undefined && d.exit_code !== null ? " (exit " + d.exit_code + ")" : "");
    stick();
  });
  src.onerror = function () { state.textContent = "reconnecting"; };
})();
