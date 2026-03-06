#!/usr/bin/env python3
"""Extract structured deal information from deals table using a local LLM (Ollama).

Calls llama3.2:3b once per row with JSON format mode to extract:
- company_name, description, amount_raised, investors, lead_investor, round_type
Stores results in a new deal_details table. Resumable — skips already-processed rows.
"""

import json
import re
import sqlite3
import requests
import sys
import time

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "llama3.2:3b"
DB_FILE = "emails.db"

SYSTEM_PROMPT = "You extract structured data from venture capital deal announcements. You respond with only JSON, no other text."

PROMPT_TEMPLATE = (
    'Extract from this deal announcement: company_name, description (what they do), '
    'amount_raised, round_type (e.g. Series A), lead_investor (who led the round - '
    'look for "X led" or "led by X"), investors (all investors comma-separated including the lead). '
    'Use empty string if info not found.\n\nText: {raw_text}'
)


def clean_lead_investor(value: str) -> str:
    """Strip trailing 'led' and clean up the lead investor field."""
    if not value:
        return ""
    value = re.sub(r'\s+led$', '', value.strip())
    value = re.sub(r'^led by\s+', '', value.strip(), flags=re.IGNORECASE)
    return value.strip()


def call_ollama(raw_text: str) -> dict:
    """Call Ollama with a single deal's raw text and parse the JSON response."""
    prompt = PROMPT_TEMPLATE.format(raw_text=raw_text)
    resp = requests.post(OLLAMA_URL, json={
        "model": MODEL,
        "system": SYSTEM_PROMPT,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {"temperature": 0, "num_predict": 300},
    }, timeout=120)
    resp.raise_for_status()
    output = resp.json()["response"].strip()

    try:
        result = json.loads(output)
    except json.JSONDecodeError:
        # Try to find JSON object in the output
        start = output.find("{")
        end = output.rfind("}") + 1
        if start >= 0 and end > start:
            try:
                result = json.loads(output[start:end])
            except json.JSONDecodeError:
                return {}
        else:
            return {}

    # Post-process: clean lead_investor
    if "lead_investor" in result:
        result["lead_investor"] = clean_lead_investor(str(result["lead_investor"]))

    # Ensure all values are strings
    for key in ("company_name", "description", "amount_raised", "investors", "lead_investor", "round_type"):
        val = result.get(key, "")
        if isinstance(val, list):
            result[key] = ", ".join(str(v) for v in val)
        else:
            result[key] = str(val) if val else ""

    return result


def main():
    db = sqlite3.connect(DB_FILE)
    db.row_factory = sqlite3.Row

    # Create deal_details table
    db.execute("""
        CREATE TABLE IF NOT EXISTS deal_details (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deal_id INTEGER,
            company_name TEXT,
            description TEXT,
            amount_raised TEXT,
            investors TEXT,
            lead_investor TEXT,
            round_type TEXT,
            email_date TEXT,
            raw_text TEXT,
            FOREIGN KEY (deal_id) REFERENCES deals(id)
        )
    """)
    db.commit()

    # Find deals not yet processed
    already_done = set()
    for row in db.execute("SELECT deal_id FROM deal_details"):
        already_done.add(row[0])

    deals = db.execute("SELECT id, company_name, raw_text, email_date FROM deals").fetchall()
    remaining = [d for d in deals if d["id"] not in already_done]

    print(f"Total deals: {len(deals)}, already processed: {len(already_done)}, remaining: {len(remaining)}")

    if not remaining:
        print("All deals already processed.")
        return

    # Verify Ollama is running
    try:
        requests.get("http://localhost:11434/api/tags", timeout=5)
    except requests.ConnectionError:
        print("Error: Ollama is not running. Start it with: ollama serve")
        sys.exit(1)

    errors = 0
    start_time = time.time()

    for i, deal in enumerate(remaining, 1):
        try:
            result = call_ollama(deal["raw_text"])
            if not result:
                errors += 1
                print(f"  [{i}/{len(remaining)}] PARSE ERROR: {deal['company_name'][:40]}", flush=True)
                continue

            db.execute(
                """INSERT INTO deal_details
                   (deal_id, company_name, description, amount_raised, investors, lead_investor, round_type, email_date, raw_text)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    deal["id"],
                    result.get("company_name", deal["company_name"]),
                    result.get("description", ""),
                    result.get("amount_raised", ""),
                    result.get("investors", ""),
                    result.get("lead_investor", ""),
                    result.get("round_type", ""),
                    deal["email_date"],
                    deal["raw_text"],
                ),
            )

            elapsed = time.time() - start_time
            rate = i / elapsed
            eta = (len(remaining) - i) / rate
            pct = 100 * i / len(remaining)
            print(
                f"  [{i}/{len(remaining)}] ({pct:.1f}%) {deal['company_name'][:35]:<37} "
                f"| {result.get('amount_raised','?'):<10} "
                f"| {rate:.1f}/s  ETA: {eta/60:.0f}m",
                flush=True,
            )

            if i % 10 == 0:
                db.commit()

        except requests.exceptions.RequestException as e:
            errors += 1
            print(f"  [{i}/{len(remaining)}] NETWORK ERROR: {e}", flush=True)
            if errors > 20:
                print("Too many errors, stopping.", flush=True)
                break
        except Exception as e:
            errors += 1
            print(f"  [{i}/{len(remaining)}] ERROR: {e}", flush=True)

    db.commit()

    total_done = db.execute("SELECT COUNT(*) FROM deal_details").fetchone()[0]
    print(f"\nDone. {total_done} deals processed total. {errors} errors this run.")

    # Show samples
    print("\nSample extracted deals:")
    for row in db.execute(
        "SELECT company_name, amount_raised, lead_investor, round_type, description FROM deal_details ORDER BY id DESC LIMIT 5"
    ):
        print(f"  {row[0]:<30} {row[1]:<12} {row[2]:<25} {row[3]:<12} {row[4][:60]}")

    db.close()


if __name__ == "__main__":
    main()
