#!/usr/bin/env python3
"""Query VC deals using natural language via FAISS + Claude."""

import argparse
import json
import os
import sys

import anthropic
import faiss
from dotenv import load_dotenv
import numpy as np
from sentence_transformers import SentenceTransformer

INDEX_DIR = "./faiss_index"
INDEX_FILE = os.path.join(INDEX_DIR, "deals.index")
META_FILE = os.path.join(INDEX_DIR, "deals_meta.json")
MODEL_NAME = "all-MiniLM-L6-v2"
TOP_K = 20

SYSTEM_PROMPT = (
    "You are a venture capital deals analyst. Answer the user's question based ONLY on "
    "the deal data provided below. Each deal is a bullet from the Axios Pro Rata newsletter. "
    "If the data doesn't contain enough information to answer, say so. Be concise and specific, "
    "citing company names and amounts."
)


def query(question: str):
    load_dotenv()
    if not os.path.exists(INDEX_FILE):
        print("Error: No index found. Run build_index.py first.")
        sys.exit(1)

    index = faiss.read_index(INDEX_FILE)
    with open(META_FILE) as f:
        metadata = json.load(f)

    model = SentenceTransformer(MODEL_NAME)
    embedding = model.encode([question])
    embedding = np.array(embedding, dtype="float32")
    faiss.normalize_L2(embedding)

    scores, indices = index.search(embedding, TOP_K)

    # Format retrieved deals for the prompt
    deals_text = []
    for idx in indices[0]:
        if idx < 0:
            continue
        meta = metadata[idx]
        date = meta.get("email_date", "")
        text = meta.get("raw_text", "")
        deals_text.append(f"[{date}] {text}")

    context = "\n\n".join(deals_text)

    claude = anthropic.Anthropic()
    response = claude.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": f"Deal data:\n\n{context}\n\n---\nQuestion: {question}",
            }
        ],
    )

    print(response.content[0].text)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query VC deals with natural language")
    parser.add_argument("question", help="Your question about VC deals")
    args = parser.parse_args()
    query(args.question)
