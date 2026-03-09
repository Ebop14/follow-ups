#!/usr/bin/env python3
"""Multi-agent system for querying VC deals.

Architecture:
  1. User asks a question
  2. Router agent classifies: qualitative, quantitative, or both
  3. Appropriate agent(s) run:
     - Qualitative: semantic search (FAISS + BM25) + Claude synthesis
     - Quantitative: SQL generation + execution against deal_details
  4. If both, results are merged into a final answer
"""

import json
import logging
import os
import sqlite3
import time

import anthropic
from dotenv import load_dotenv

from query_deals import search_deals

load_dotenv()

DB_FILE = "emails.db"
ROUTER_MODEL = "claude-opus-4-20250514"
AGENT_MODEL = "claude-sonnet-4-20250514"

logger = logging.getLogger("agent")


class LogCollector:
    """Collects structured log entries during a query for streaming to the frontend."""

    def __init__(self):
        self.entries: list[dict] = []

    def log(self, step: str, label: str, content: str, elapsed: float | None = None):
        entry = {"step": step, "label": label, "content": content}
        if elapsed is not None:
            entry["elapsed_ms"] = round(elapsed * 1000)
        self.entries.append(entry)
        # Also print to server console
        preview = content[:200] + "..." if len(content) > 200 else content
        timing = f" ({entry['elapsed_ms']}ms)" if elapsed is not None else ""
        logger.info(f"[{step}] {label}{timing}: {preview}")

# --- Qualitative metadata for each column ---
COLUMN_METADATA = {
    "deal_details": {
        "id": "Auto-incrementing primary key. Not useful for queries.",
        "deal_id": "Foreign key to deals.id. Links to the raw deal entry.",
        "company_name": (
            "Name of the company that raised funding. ~3% of rows are empty. "
            "Examples: 'Sierra Space', 'Neura Robotics', 'Starface'."
        ),
        "description": (
            "Brief description of what the company does or its sector. ~1.5% empty. "
            "Typically a short phrase like 'German maker of humanoid robots', "
            "'agentic AI startup focused on procurement', 'cybersecurity startup for regulated institutions'. "
            "Useful for filtering by sector/industry via LIKE patterns."
        ),
        "amount_raised": (
            "Amount of funding raised, stored as TEXT with inconsistent formatting. ~6% empty. "
            "Common patterns: '$550m', '105m' (no $ sign), '$1.2B', '€1b', 'A$30m'. "
            "Some are missing the '$' prefix. Suffixes: 'm' = millions, 'b' = billions. "
            "To compare numerically: strip currency symbols, extract the number, "
            "multiply by 1e6 for 'm' or 1e9 for 'b'. Use CASE/CAST expressions."
        ),
        "investors": (
            "Comma-separated list of all investors in the round (including lead). ~27% empty. "
            "Examples: 'General Atlantic, Coatue, Moore Strategic Ventures'. "
            "Use LIKE '%investor_name%' to search. A single investor may appear in many deals."
        ),
        "lead_investor": (
            "The investor(s) who led the round. ~17% empty. "
            "Usually one firm but sometimes multiple: 'Astó Consumer Partners and Align Ventures'. "
            "Occasionally noisy: 'Nir Zuk led by Greylock'. Use LIKE for partial matching."
        ),
        "round_type": (
            "Funding round stage. ~31% empty. NOT normalized — many variations exist. "
            "Common values: 'Series A' (234), 'seed' (158), 'Series B' (116), 'Series C' (52), "
            "'seed funding' (32), 'pre-seed' (28), 'Series D' (28). "
            "Also: 'seed and Series A', 'Series A extension funding', 'Series B extension'. "
            "Use LIKE '%seed%' or LIKE '%Series A%' for robust matching."
        ),
        "email_date": (
            "Date the deal was reported in the Axios Pro Rata newsletter. "
            "Stored as RFC 2822 text, e.g. 'Thu, 5 Mar 2026 10:03:26 -0500 (EST)'. "
            "Data spans Dec 2025 – Jan 2026 (small window so far). "
            "Use LIKE for rough date filtering, or substr() to extract components."
        ),
        "raw_text": (
            "Full original text of the deal announcement bullet from the newsletter. "
            "Contains all information in unstructured form. Useful for full-text LIKE searches "
            "when structured fields are incomplete."
        ),
    },
    "deals": {
        "id": "Auto-incrementing primary key.",
        "email_id": "Foreign key to emails.id.",
        "company_name": "Company name extracted from HTML <strong> tags. May be less clean than deal_details.",
        "raw_text": "Raw text of the deal bullet. Same content as deal_details.raw_text for matched rows.",
        "link": "URL to the original article (axios.link/...).",
        "email_date": "Same format as deal_details.email_date.",
    },
}


def describe_table(table_name: str) -> str:
    """Dynamically describe a table using PRAGMA + sample data + qualitative metadata."""
    db = sqlite3.connect(DB_FILE)
    db.row_factory = sqlite3.Row

    # Get column info from PRAGMA
    columns = db.execute(f"PRAGMA table_info({table_name})").fetchall()
    row_count = db.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    meta = COLUMN_METADATA.get(table_name, {})

    lines = [f"TABLE: {table_name} ({row_count:,} rows)", ""]
    lines.append("Columns:")
    for col in columns:
        name, col_type = col["name"], col["type"]
        pk = " PRIMARY KEY" if col["pk"] else ""
        desc = meta.get(name, "")
        lines.append(f"  {name} {col_type}{pk}")
        if desc:
            lines.append(f"    -> {desc}")

    # Sample 5 rows
    sample_rows = db.execute(
        f"SELECT * FROM {table_name} WHERE company_name != '' ORDER BY RANDOM() LIMIT 5"
    ).fetchall()
    if sample_rows:
        col_names = [col["name"] for col in columns]
        lines.append("")
        lines.append("Sample rows:")
        for row in sample_rows:
            parts = []
            for c in col_names:
                val = str(row[c] or "")
                if len(val) > 80:
                    val = val[:77] + "..."
                if val:
                    parts.append(f"  {c}: {val}")
            lines.append("  ---")
            lines.append("\n".join(parts))

    db.close()
    return "\n".join(lines)


def get_full_schema() -> str:
    """Build complete schema description with live data for the SQL agent."""
    sections = [
        describe_table("deal_details"),
        "",
        describe_table("deals"),
    ]
    return "\n\n".join(sections)

client = anthropic.Anthropic()


def route_question(question: str, logs: LogCollector | None = None) -> dict:
    """Classify a question and draft execution plans for sub-agents.

    Returns {
        "route": "qualitative"|"quantitative"|"both",
        "plan": "high-level strategy",
        "sql_plan": "specific instructions for the SQL agent (if applicable)",
        "qualitative_plan": "specific instructions for the semantic search agent (if applicable)",
        "synthesis_guidance": "how to combine results into a final answer"
    }
    """
    schema_summary = get_full_schema()

    system_prompt = (
        "You are the orchestrator for a VC deals research system. You have two sub-agents:\n\n"
        "1. **SQL Agent** — writes and executes SQLite queries against a structured deal_details table. "
        "Best for: counts, totals, averages, rankings, filtering by amount/round/investor, top-N lists.\n"
        "2. **Qualitative Agent** — runs semantic search (FAISS + BM25) over raw deal text. "
        "Best for: finding deals by topic/sector, understanding what companies do, surfacing deals "
        "related to a theme.\n\n"
        "Given the user's question, return a JSON object with these fields:\n"
        '- "route": one of "qualitative", "quantitative", or "both"\n'
        '- "plan": 1-2 sentence high-level strategy for answering the question\n'
        '- "sql_plan": (include ONLY if route is "quantitative" or "both") '
        "Detailed instructions for the SQL agent: what columns to query, what filters to apply, "
        "what aggregations to use, pitfalls to watch for (e.g. amount_raised is text, round_type "
        "is not normalized). Be specific enough that the SQL agent can write the query without "
        "re-reading the schema.\n"
        '- "qualitative_plan": (include ONLY if route is "qualitative" or "both") '
        "Detailed instructions for the semantic search agent: what search terms to use, what kinds "
        "of deals to look for, what context would be most useful for answering the question.\n"
        '- "synthesis_guidance": how the final synthesizer should combine and present the results. '
        "Should the answer be a table? A ranked list? A narrative summary? What should be emphasized?\n\n"
        "Here is the database schema so you can write informed plans:\n\n"
        f"{schema_summary}\n\n"
        "Respond with ONLY valid JSON, no other text."
    )
    if logs:
        logs.log("router", "System prompt", system_prompt)
        logs.log("router", "User question", question)
        logs.log("router", "Model", ROUTER_MODEL)

    t0 = time.time()
    response = client.messages.create(
        model=ROUTER_MODEL,
        max_tokens=800,
        system=system_prompt,
        messages=[{"role": "user", "content": question}],
    )
    elapsed = time.time() - t0
    text = response.content[0].text.strip()

    if logs:
        logs.log("router", "Raw LLM response", text, elapsed)
        tokens = response.usage
        logs.log("router", "Token usage", f"input: {tokens.input_tokens}, output: {tokens.output_tokens}")

    try:
        result = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            result = json.loads(text[start:end])
        else:
            result = {"route": "both", "plan": "Unclear classification, using both agents."}

    if logs:
        logs.log("router", "Decision", json.dumps(result, indent=2))
    return result


def run_sql_agent(question: str, sql_plan: str = "", logs: LogCollector | None = None) -> dict:
    """Generate and execute SQL to answer a quantitative question.
    Returns {"sql": "...", "results": [...], "summary": "..."}
    """
    schema = get_full_schema()
    if logs:
        logs.log("sql_agent", "Schema sent to LLM", schema)
        if sql_plan:
            logs.log("sql_agent", "Plan from router", sql_plan)

    system_prompt = (
        "You are a SQL expert. Given a user question about VC deals, write a SQLite query "
        "to answer it. You have access to the following schema:\n\n"
        f"{schema}\n\n"
        "Important notes:\n"
        "- amount_raised is TEXT. To compare amounts numerically, you'll need to parse the "
        "string. A useful pattern: extract the number and multiply by the suffix "
        "(m=1000000, b=1000000000). Use CASE expressions or CAST.\n"
        "- Not all rows have complete data. Filter out empty strings where relevant.\n"
        "- Limit results to 25 rows max unless the user asks for more.\n"
        "- Return ONLY the SQL query, no explanation, no markdown fences."
    )
    if logs:
        logs.log("sql_agent", "System prompt (without schema)", system_prompt[:200] + "... [schema omitted]")

    user_message = question
    if sql_plan:
        user_message = f"Question: {question}\n\nExecution plan from orchestrator:\n{sql_plan}"

    # Step 1: Generate SQL
    t0 = time.time()
    response = client.messages.create(
        model=AGENT_MODEL,
        max_tokens=500,
        system=system_prompt,
        messages=[{"role": "user", "content": user_message}],
    )
    elapsed = time.time() - t0
    sql = response.content[0].text.strip()

    if logs:
        logs.log("sql_agent", "Raw LLM response", sql, elapsed)
        tokens = response.usage
        logs.log("sql_agent", "Token usage", f"input: {tokens.input_tokens}, output: {tokens.output_tokens}")

    # Strip markdown fences if present
    if sql.startswith("```"):
        lines = sql.split("\n")
        sql = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

    if logs:
        logs.log("sql_agent", "Generated SQL", sql)

    # Step 2: Execute SQL
    db = sqlite3.connect(DB_FILE)
    db.row_factory = sqlite3.Row
    try:
        t0 = time.time()
        cursor = db.execute(sql)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        results = [dict(zip(columns, row)) for row in rows]
        elapsed = time.time() - t0
        if logs:
            logs.log("sql_agent", "Query executed successfully", f"{len(results)} rows returned", elapsed)
            logs.log("sql_agent", "Query results", json.dumps(results[:50], indent=2, default=str))
    except Exception as e:
        if logs:
            logs.log("sql_agent", "Query FAILED", f"{type(e).__name__}: {e}")
            logs.log("sql_agent", "Requesting SQL fix from LLM", f"Error: {e}")

        # If the first query fails, ask Claude to fix it
        t0 = time.time()
        fix_response = client.messages.create(
            model=AGENT_MODEL,
            max_tokens=500,
            system=(
                "The following SQL query failed. Fix it and return ONLY the corrected SQL.\n"
                f"Schema:\n{schema}\n"
                f"Error: {e}\n"
                "Return ONLY the SQL query, no explanation, no markdown fences."
            ),
            messages=[{"role": "user", "content": f"Original question: {question}\nFailed SQL: {sql}"}],
        )
        elapsed = time.time() - t0
        sql = fix_response.content[0].text.strip()
        if sql.startswith("```"):
            lines = sql.split("\n")
            sql = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

        if logs:
            logs.log("sql_agent", "Fixed SQL", sql, elapsed)

        try:
            t0 = time.time()
            cursor = db.execute(sql)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            results = [dict(zip(columns, row)) for row in rows]
            elapsed = time.time() - t0
            if logs:
                logs.log("sql_agent", "Retry executed successfully", f"{len(results)} rows returned", elapsed)
                logs.log("sql_agent", "Query results", json.dumps(results[:50], indent=2, default=str))
        except Exception as e2:
            if logs:
                logs.log("sql_agent", "Retry also FAILED", f"{type(e2).__name__}: {e2}")
            db.close()
            return {"sql": sql, "results": [], "summary": f"SQL execution failed: {e2}"}
    finally:
        db.close()

    return {"sql": sql, "results": results[:50]}


def run_qualitative_agent(question: str, qualitative_plan: str = "", logs: LogCollector | None = None) -> dict:
    """Run semantic search and return relevant deal texts.
    Returns {"deals": [...]}
    """
    search_query = question
    if qualitative_plan:
        if logs:
            logs.log("qual_agent", "Plan from router", qualitative_plan)
        # Use the plan to refine the search query if it suggests specific terms
        search_query = f"{question} {qualitative_plan}"

    if logs:
        logs.log("qual_agent", "Running semantic search", f"Query: {search_query}")

    t0 = time.time()
    deals = search_deals(search_query)
    elapsed = time.time() - t0

    if logs:
        logs.log("qual_agent", "Search complete", f"{len(deals)} deals retrieved", elapsed)
        logs.log("qual_agent", "Top 5 results", "\n\n".join(deals[:5]))

    return {"deals": deals[:30]}


def synthesize_answer(question: str, route_info: dict, sql_result: dict | None, qual_result: dict | None, stream: bool = False, logs: LogCollector | None = None):
    """Combine results from agents into a final answer."""
    context_parts = []

    if sql_result:
        context_parts.append(
            f"## SQL Agent Results\n"
            f"Query: {sql_result['sql']}\n\n"
            f"Results ({len(sql_result['results'])} rows):\n"
            f"{json.dumps(sql_result['results'], indent=2, default=str)}"
        )

    if qual_result and qual_result.get("deals"):
        deals_text = "\n\n".join(qual_result["deals"][:20])
        context_parts.append(
            f"## Semantic Search Results\n"
            f"Top matching deals:\n\n{deals_text}"
        )

    context = "\n\n---\n\n".join(context_parts)

    synthesis_guidance = route_info.get("synthesis_guidance", "")

    system = (
        "You are a venture capital deals analyst. You have been given results from "
        "one or more research agents that queried a database of VC deals.\n\n"
        f"Routing decision: {route_info.get('route', 'unknown')} — {route_info.get('plan', '')}\n\n"
    )
    if synthesis_guidance:
        system += f"Presentation guidance from orchestrator: {synthesis_guidance}\n\n"
    system += (
        "Synthesize the information into a clear, concise answer. "
        "If SQL results are provided, use exact numbers from the data. "
        "If semantic search results are provided, reference specific companies and deals. "
        "If data is insufficient, say so clearly."
    )

    user_message = f"Agent results:\n\n{context}\n\n---\nQuestion: {question}"

    if logs:
        logs.log("synthesizer", "System prompt", system)
        logs.log("synthesizer", "Full context sent to LLM", user_message)
        ctx_len = len(system) + len(user_message)
        logs.log("synthesizer", "Context size", f"{ctx_len:,} chars (~{ctx_len // 4:,} tokens est.)")

    kwargs = dict(
        model=AGENT_MODEL,
        max_tokens=1024,
        system=system,
        messages=[{"role": "user", "content": user_message}],
    )

    if stream:
        return client.messages.stream(**kwargs)
    response = client.messages.create(**kwargs)
    return response.content[0].text


def query(question: str, stream: bool = False, verbose: bool = False, logs: LogCollector | None = None):
    """Main entry point: route, run agents, synthesize.

    If stream=True, returns a context manager that yields text chunks.
    If stream=False, returns the final answer string.
    Returns (result, logs) tuple. Result is a string or stream context manager.
    """
    if logs is None:
        logs = LogCollector()

    logs.log("orchestrator", "Question received", question)

    # Step 1: Route
    route_info = route_question(question, logs=logs)
    route = route_info.get("route", "both")
    plan = route_info.get("plan", "")
    logs.log("orchestrator", "Routing complete", f"route={route}, plan={plan}")

    # Step 2: Run agent(s) with plans from router
    sql_result = None
    qual_result = None
    sql_plan = route_info.get("sql_plan", "")
    qualitative_plan = route_info.get("qualitative_plan", "")

    if route in ("quantitative", "both"):
        logs.log("orchestrator", "Launching SQL agent", sql_plan or "(no specific plan)")
        sql_result = run_sql_agent(question, sql_plan=sql_plan, logs=logs)

    if route in ("qualitative", "both"):
        logs.log("orchestrator", "Launching qualitative agent", qualitative_plan or "(no specific plan)")
        qual_result = run_qualitative_agent(question, qualitative_plan=qualitative_plan, logs=logs)

    # Step 3: Synthesize
    logs.log("orchestrator", "Starting synthesis", "")
    result = synthesize_answer(question, route_info, sql_result, qual_result, stream=stream, logs=logs)

    if verbose:
        print("\n--- Agent Logs ---")
        for entry in logs.entries:
            timing = f" ({entry['elapsed_ms']}ms)" if 'elapsed_ms' in entry else ""
            print(f"[{entry['step']}] {entry['label']}{timing}")
            if entry['content']:
                for line in entry['content'].split('\n')[:10]:
                    print(f"    {line}")
                if entry['content'].count('\n') > 10:
                    print(f"    ... ({entry['content'].count(chr(10)) - 10} more lines)")
        print("--- End Logs ---\n")

    return result, logs


def main():
    import sys
    logging.basicConfig(level=logging.INFO, format="%(name)s | %(message)s")

    if len(sys.argv) < 2:
        print("Usage: python agent.py 'your question here'")
        print("\nExamples:")
        print("  python agent.py 'What are the largest Series B rounds?'")
        print("  python agent.py 'How many deals were led by Sequoia?'")
        print("  python agent.py 'Which AI companies raised the most money?'")
        sys.exit(1)

    question = " ".join(sys.argv[1:])
    print(f"Question: {question}\n")
    answer, logs = query(question, verbose=True)
    print(f"\n{'='*60}\n{answer}")


if __name__ == "__main__":
    main()
