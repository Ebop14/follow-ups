#!/usr/bin/env python3
"""Simple web frontend for VC deals search."""

import json

from flask import Flask, request, Response
from query_deals import search_deals, ask_claude, _load_resources

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
    flex-direction: column;
    align-items: center;
    padding: 3rem 1rem;
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
  #result {
    width: 100%;
    max-width: 700px;
    background: #1a1d27;
    border-radius: 10px;
    padding: 1.5rem;
    min-height: 60px;
    white-space: pre-wrap;
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
</style>
</head>
<body>
  <h1>VC Deals Search</h1>
  <p class="subtitle">Semantic search over Axios Pro Rata deals</p>
  <div class="search-box">
    <input id="q" type="text" placeholder="e.g. What are the largest Series B rounds?" autofocus>
    <button id="btn" onclick="doQuery()">Search</button>
  </div>
  <div id="result"></div>
<script>
const q = document.getElementById('q');
const btn = document.getElementById('btn');
const result = document.getElementById('result');

q.addEventListener('keydown', e => { if (e.key === 'Enter') doQuery(); });

function doQuery() {
  const question = q.value.trim();
  if (!question) return;
  btn.disabled = true;
  result.style.display = 'block';
  result.innerHTML = '<span class="spinner"></span> Searching deals...';

  fetch('/query', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({question})
  }).then(response => {
    result.textContent = '';
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    function read() {
      reader.read().then(({done, value}) => {
        if (done) { btn.disabled = false; return; }
        buffer += decoder.decode(value, {stream: true});
        const lines = buffer.split('\\n');
        buffer = lines.pop();
        for (const line of lines) {
          if (line.startsWith('data: ')) {
            const data = line.slice(6);
            if (data === '[DONE]') { btn.disabled = false; return; }
            try {
              const parsed = JSON.parse(data);
              if (parsed.text) result.textContent += parsed.text;
              if (parsed.error) { result.textContent = 'Error: ' + parsed.error; btn.disabled = false; return; }
            } catch(e) {}
          }
        }
        read();
      });
    }
    read();
  }).catch(err => {
    result.textContent = 'Error: ' + err.message;
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
            deals = search_deals(question)
            with ask_claude(question, deals, stream=True) as stream:
                for text in stream.text_stream:
                    yield f"data: {json.dumps({'text': text})}\n\n"
            yield "data: [DONE]\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return Response(generate(), content_type="text/event-stream")


if __name__ == "__main__":
    print("Loading model and index...")
    _load_resources()
    print("Ready! Open http://localhost:5000")
    app.run(debug=False, port=5000)
