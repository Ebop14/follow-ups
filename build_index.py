#!/usr/bin/env python3
"""Build a FAISS vector index from the deals table in emails.db."""

import argparse
import json
import os
import sqlite3

import faiss
import numpy as np
from sentence_transformers import SentenceTransformer

DB_PATH = "emails.db"
INDEX_DIR = "./faiss_index"
INDEX_FILE = os.path.join(INDEX_DIR, "deals.index")
META_FILE = os.path.join(INDEX_DIR, "deals_meta.json")
MODEL_NAME = "all-MiniLM-L6-v2"
BATCH_SIZE = 500


def load_deals():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, email_id, company_name, raw_text, link, email_date FROM deals"
    ).fetchall()
    conn.close()
    return rows


def build_index(rebuild=False):
    if os.path.exists(INDEX_FILE) and not rebuild:
        print(f"Index already exists at {INDEX_FILE}. Use --rebuild to re-index.")
        return

    os.makedirs(INDEX_DIR, exist_ok=True)

    print("Loading deals from database...")
    rows = load_deals()
    print(f"Found {len(rows)} deals.")

    print(f"Loading embedding model ({MODEL_NAME})...")
    model = SentenceTransformer(MODEL_NAME)

    texts = [row["raw_text"] for row in rows]
    metadata = [
        {
            "id": row["id"],
            "company_name": row["company_name"] or "",
            "email_date": row["email_date"] or "",
            "link": row["link"] or "",
            "email_id": row["email_id"] or "",
            "raw_text": row["raw_text"],
        }
        for row in rows
    ]

    print("Encoding deals...")
    embeddings = model.encode(texts, show_progress_bar=True, batch_size=BATCH_SIZE)
    embeddings = np.array(embeddings, dtype="float32")

    # Normalize for cosine similarity
    faiss.normalize_L2(embeddings)

    # Build index (inner product on normalized vectors = cosine similarity)
    dim = embeddings.shape[1]
    index = faiss.IndexFlatIP(dim)
    index.add(embeddings)

    faiss.write_index(index, INDEX_FILE)
    with open(META_FILE, "w") as f:
        json.dump(metadata, f)

    print(f"Done. {len(rows)} deals indexed in {INDEX_DIR}/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build vector index for VC deals")
    parser.add_argument("--rebuild", action="store_true", help="Rebuild index from scratch")
    args = parser.parse_args()
    build_index(rebuild=args.rebuild)
