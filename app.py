#!/usr/bin/env python3
"""Simple web frontend for VC deals search."""

import json

from flask import Flask, request, Response
from query_deals import search_deals, ask_claude, _load_resources
from agent import query as agent_query, route_question, LogCollector

app = Flask(__name__)

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>VC Deals Search</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: #0f1117;
    color: #e0e0e0;
    min-height: 100vh;
    display: flex;
    flex-direction: row;
  }
  /* Left sidebar: logs */
  #log-sidebar {
    width: 340px;
    min-width: 340px;
    height: 100vh;
    position: sticky;
    top: 0;
    background: #0b0d14;
    border-right: 1px solid #1e2130;
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }
  #log-sidebar-header {
    padding: 1rem 1rem 0.6rem;
    border-bottom: 1px solid #1e2130;
    flex-shrink: 0;
  }
  #log-sidebar-header h2 {
    font-size: 0.85rem;
    color: #6b6f90;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }
  #log-count {
    color: #444;
    font-weight: 400;
  }
  #log-entries {
    flex: 1;
    overflow-y: auto;
    padding: 0.5rem;
  }
  #log-empty {
    color: #333;
    font-size: 0.8rem;
    text-align: center;
    padding: 2rem 1rem;
  }
  /* Right main content */
  #main {
    flex: 1;
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 3rem 1rem;
    min-width: 0;
    overflow-y: auto;
    height: 100vh;
  }
  h1 {
    font-size: 1.8rem;
    margin-bottom: 0.3rem;
    color: #fff;
  }
  .subtitle {
    color: #888;
    margin-bottom: 2rem;
    font-size: 0.95rem;
  }
  .search-box {
    display: flex;
    gap: 0.5rem;
    width: 100%;
    max-width: 700px;
    margin-bottom: 2rem;
  }
  input {
    flex: 1;
    padding: 0.75rem 1rem;
    border-radius: 8px;
    border: 1px solid #333;
    background: #1a1d27;
    color: #e0e0e0;
    font-size: 1rem;
    outline: none;
    transition: border-color 0.2s;
  }
  input:focus { border-color: #5b6ef5; }
  button {
    padding: 0.75rem 1.5rem;
    border-radius: 8px;
    border: none;
    background: #5b6ef5;
    color: #fff;
    font-size: 1rem;
    cursor: pointer;
    transition: background 0.2s;
    white-space: nowrap;
  }
  button:hover { background: #4a5cd4; }
  button:disabled { opacity: 0.5; cursor: not-allowed; }
  #route-info {
    width: 100%;
    max-width: 700px;
    margin-bottom: 0.75rem;
    padding: 0.6rem 1rem;
    background: #1e2130;
    border-radius: 8px;
    border: 1px solid #2a2d40;
    font-size: 0.85rem;
    color: #8a8fb0;
    display: none;
  }
  .route-badge {
    display: inline-block;
    padding: 0.15rem 0.5rem;
    border-radius: 4px;
    font-size: 0.75rem;
    font-weight: 600;
    text-transform: uppercase;
    margin-right: 0.5rem;
  }
  .route-quantitative { background: #1a3a2a; color: #4ade80; }
  .route-qualitative { background: #1a2a3a; color: #60a5fa; }
  .route-both { background: #3a2a1a; color: #fbbf24; }
  #result {
    width: 100%;
    max-width: 700px;
    background: #1a1d27;
    border-radius: 10px;
    padding: 1.5rem;
    min-height: 60px;
    max-height: 70vh;
    overflow-y: auto;
    line-height: 1.6;
    display: none;
    border: 1px solid #262938;
  }
  .spinner {
    display: inline-block;
    width: 18px; height: 18px;
    border: 2px solid #555;
    border-top-color: #5b6ef5;
    border-radius: 50%;
    animation: spin 0.6s linear infinite;
    vertical-align: middle;
    margin-right: 0.5rem;
  }
  @keyframes spin { to { transform: rotate(360deg); } }
  /* Markdown rendered content */
  #result h1, #result h2, #result h3, #result h4 {
    color: #fff;
    margin: 1.2em 0 0.4em 0;
    line-height: 1.3;
  }
  #result h1:first-child, #result h2:first-child, #result h3:first-child { margin-top: 0; }
  #result h1 { font-size: 1.3rem; }
  #result h2 { font-size: 1.15rem; }
  #result h3 { font-size: 1.05rem; }
  #result p { margin: 0.5em 0; }
  #result ul, #result ol { margin: 0.5em 0 0.5em 1.5em; }
  #result li { margin: 0.25em 0; }
  #result strong { color: #fff; }
  #result code {
    background: #262938;
    padding: 0.15em 0.4em;
    border-radius: 4px;
    font-size: 0.9em;
    color: #c5c8f0;
  }
  #result pre {
    background: #151720;
    border: 1px solid #262938;
    border-radius: 6px;
    padding: 0.8em 1em;
    overflow-x: auto;
    margin: 0.6em 0;
  }
  #result pre code {
    background: none;
    padding: 0;
    font-size: 0.85em;
  }
  #result table {
    border-collapse: collapse;
    width: 100%;
    margin: 0.6em 0;
    font-size: 0.9em;
  }
  #result th, #result td {
    border: 1px solid #2a2d40;
    padding: 0.4em 0.7em;
    text-align: left;
  }
  #result th { background: #1e2130; color: #b0b4d0; font-weight: 600; }
  #result tr:nth-child(even) { background: #161824; }
  #result blockquote {
    border-left: 3px solid #5b6ef5;
    padding: 0.3em 0 0.3em 1em;
    margin: 0.5em 0;
    color: #a0a4c0;
  }
  #result hr { border: none; border-top: 1px solid #2a2d40; margin: 1em 0; }
  #result a { color: #7b8ef5; text-decoration: none; }
  #result a:hover { text-decoration: underline; }
  /* Log entries */
  .log-entry {
    margin-bottom: 0.3rem;
    border-radius: 5px;
    overflow: hidden;
  }
  .log-header {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    padding: 0.3rem 0.5rem;
    background: #12141c;
    cursor: pointer;
    font-size: 0.73rem;
    color: #8a8fb0;
    user-select: none;
    transition: background 0.15s;
  }
  .log-header:hover { background: #1a1d27; }
  .log-step {
    font-weight: 600;
    font-size: 0.65rem;
    padding: 0.1rem 0.35rem;
    border-radius: 3px;
    text-transform: uppercase;
    flex-shrink: 0;
  }
  .log-step-router { background: #1a2a3a; color: #60a5fa; }
  .log-step-orchestrator { background: #2a1a3a; color: #c084fc; }
  .log-step-sql_agent { background: #1a3a2a; color: #4ade80; }
  .log-step-qual_agent { background: #3a2a1a; color: #fbbf24; }
  .log-step-synthesizer { background: #3a1a2a; color: #f472b6; }
  .log-label {
    flex: 1;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .log-timing { color: #444; font-size: 0.65rem; flex-shrink: 0; }
  .log-content {
    display: none;
    padding: 0.5rem;
    background: #0a0c12;
    font-family: 'SF Mono', 'Fira Code', 'Consolas', monospace;
    font-size: 0.68rem;
    color: #7a7e98;
    white-space: pre-wrap;
    word-break: break-all;
    max-height: 250px;
    overflow-y: auto;
    line-height: 1.45;
    border-top: 1px solid #161824;
  }
  /* Responsive: stack on narrow screens */
  @media (max-width: 900px) {
    body { flex-direction: column; }
    #log-sidebar {
      width: 100%;
      min-width: 0;
      height: auto;
      max-height: 40vh;
      position: relative;
      border-right: none;
      border-bottom: 1px solid #1e2130;
    }
    #main { height: auto; min-height: 60vh; }
  }
</style>
<script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
</head>
<body>
  <div id="log-sidebar">
    <div id="log-sidebar-header">
      <h2>Agent Logs <span id="log-count"></span></h2>
    </div>
    <div id="log-entries">
      <div id="log-empty">Logs will appear here when you run a query.</div>
    </div>
  </div>
  <div id="main">
    <h1>VC Deals Search</h1>
    <p class="subtitle">Semantic search over Axios Pro Rata deals</p>
    <div class="search-box">
      <input id="q" type="text" placeholder="e.g. What are the largest Series B rounds?" autofocus>
      <button id="btn" onclick="doQuery()">Search</button>
    </div>
    <div id="route-info"></div>
    <div id="result"></div>
  </div>
<script>
const q = document.getElementById('q');
const btn = document.getElementById('btn');
const result = document.getElementById('result');
const routeInfo = document.getElementById('route-info');
const logEntries = document.getElementById('log-entries');
const logCount = document.getElementById('log-count');
const logEmpty = document.getElementById('log-empty');

marked.setOptions({ breaks: true, gfm: true });

q.addEventListener('keydown', e => { if (e.key === 'Enter') doQuery(); });

let rawMarkdown = '';

function renderMarkdown() {
  result.innerHTML = marked.parse(rawMarkdown);
}

function escapeHtml(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

function renderLogs(entries) {
  logCount.textContent = '(' + entries.length + ')';
  if (logEmpty) logEmpty.remove();
  logEntries.innerHTML = entries.map((e, i) => {
    const stepClass = 'log-step-' + e.step;
    const timing = e.elapsed_ms ? e.elapsed_ms + 'ms' : '';
    const content = escapeHtml(e.content || '(empty)');
    return '<div class="log-entry">' +
      '<div class="log-header" onclick="toggleLogEntry(' + i + ')">' +
        '<span class="log-step ' + stepClass + '">' + escapeHtml(e.step) + '</span>' +
        '<span class="log-label">' + escapeHtml(e.label) + '</span>' +
        (timing ? '<span class="log-timing">' + timing + '</span>' : '') +
      '</div>' +
      '<div class="log-content" id="log-entry-' + i + '">' + content + '</div>' +
    '</div>';
  }).join('');
  logEntries.scrollTop = logEntries.scrollHeight;
}

function toggleLogEntry(i) {
  const el = document.getElementById('log-entry-' + i);
  el.style.display = el.style.display === 'block' ? 'none' : 'block';
}

function doQuery() {
  const question = q.value.trim();
  if (!question) return;
  btn.disabled = true;
  rawMarkdown = '';
  routeInfo.style.display = 'none';
  logEntries.innerHTML = '<div style="color:#444;font-size:0.8rem;text-align:center;padding:1rem;"><span class="spinner"></span> Waiting for agents...</div>';
  logCount.textContent = '';
  result.style.display = 'block';
  result.innerHTML = '<span class="spinner"></span> Routing question to agents...';

  fetch('/query', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({question})
  }).then(response => {
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    function read() {
      reader.read().then(({done, value}) => {
        if (done) { btn.disabled = false; return; }
        buffer += decoder.decode(value, {stream: true});
        const lines = buffer.split('\\n');
        buffer = lines.pop();
        let needsRender = false;
        for (const line of lines) {
          if (line.startsWith('data: ')) {
            const data = line.slice(6);
            if (data === '[DONE]') { btn.disabled = false; renderMarkdown(); return; }
            try {
              const parsed = JSON.parse(data);
              if (parsed.meta) {
                const r = parsed.meta.route;
                routeInfo.innerHTML = '<span class="route-badge route-' + r + '">' + r + '</span>' + escapeHtml(parsed.meta.plan);
                routeInfo.style.display = 'block';
                result.innerHTML = '<span class="spinner"></span> Running agents...';
              }
              if (parsed.logs) {
                renderLogs(parsed.logs);
              }
              if (parsed.text) {
                rawMarkdown += parsed.text;
                needsRender = true;
              }
              if (parsed.error) { result.innerHTML = '<strong>Error:</strong> ' + escapeHtml(parsed.error); btn.disabled = false; return; }
            } catch(e) {}
          }
        }
        if (needsRender) renderMarkdown();
        read();
      });
    }
    read();
  }).catch(err => {
    result.innerHTML = '<strong>Error:</strong> ' + escapeHtml(err.message);
    btn.disabled = false;
  });
}
</script>
</body>
</html>"""


@app.route("/")
def index():
    return HTML


@app.route("/query", methods=["POST"])
def handle_query():
    question = request.json.get("question", "").strip()
    if not question:
        return Response("data: " + json.dumps({"error": "Empty question"}) + "\n\n",
                        content_type="text/event-stream")

    def generate():
        try:
            logs = LogCollector()

            # Run multi-agent query (routing happens inside)
            result, logs = agent_query(question, stream=True, logs=logs)

            # Emit route info from logs
            route_entry = next((e for e in logs.entries if e["label"] == "Decision"), None)
            if route_entry:
                info = json.loads(route_entry["content"])
                yield f"data: {json.dumps({'meta': {'route': info.get('route', 'both'), 'plan': info.get('plan', '')}})}\n\n"

            # Emit all logs collected so far (before streaming starts)
            yield f"data: {json.dumps({'logs': logs.entries})}\n\n"

            # Stream the synthesis
            with result as stream:
                for text in stream.text_stream:
                    yield f"data: {json.dumps({'text': text})}\n\n"
            yield "data: [DONE]\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return Response(generate(), content_type="text/event-stream")


if __name__ == "__main__":
    print("Loading model and index...")
    _load_resources()
    print("Ready! Open http://localhost:5050")
    app.run(debug=False, port=5050)
