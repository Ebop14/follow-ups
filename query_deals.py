#!/usr/bin/env python3
"""Query VC deals using hybrid search (FAISS + BM25) and Claude."""

import argparse
import json
import os
import pickle
import re

import anthropic
import faiss
from dotenv import load_dotenv
import numpy as np
from sentence_transformers import SentenceTransformer

INDEX_DIR = "./faiss_index"
INDEX_FILE = os.path.join(INDEX_DIR, "deals.index")
META_FILE = os.path.join(INDEX_DIR, "deals_meta.json")
BM25_FILE = os.path.join(INDEX_DIR, "bm25.pkl")
MODEL_NAME = "all-MiniLM-L6-v2"
TOP_K = 50
RRF_K = 60  # Reciprocal rank fusion constant

SYSTEM_PROMPT = (
    "You are a venture capital deals analyst. You will be given deal data retrieved via "
    "semantic search — some deals may not be relevant to the user's question. Ignore any "
    "deals that do not fit the query criteria. Answer based ONLY on the relevant deals. "
    "If none of the provided deals are relevant, say so. Be concise and specific, "
    "citing company names and amounts."
)

# Lazy-loaded globals
_model = None
_index = None
_metadata = None
_bm25 = None


def _tokenize(text: str) -> list[str]:
    """Lowercase and split on non-alphanumeric characters."""
    return re.findall(r"[a-z0-9]+", text.lower())


def _load_resources():
    """Load FAISS index, BM25 index, and sentence transformer model once."""
    global _model, _index, _metadata, _bm25
    if _model is None:
        load_dotenv()
        if not os.path.exists(INDEX_FILE):
            raise FileNotFoundError("No index found. Run build_index.py first.")
        _index = faiss.read_index(INDEX_FILE)
        with open(META_FILE) as f:
            _metadata = json.load(f)
        if os.path.exists(BM25_FILE):
            with open(BM25_FILE, "rb") as f:
                _bm25 = pickle.load(f)
        _model = SentenceTransformer(MODEL_NAME)


def _rrf_merge(faiss_indices: list[int], bm25_indices: list[int], k: int = RRF_K) -> list[int]:
    """Merge two ranked lists using Reciprocal Rank Fusion."""
    scores = {}
    for rank, idx in enumerate(faiss_indices):
        scores[idx] = scores.get(idx, 0) + 1.0 / (k + rank)
    for rank, idx in enumerate(bm25_indices):
        scores[idx] = scores.get(idx, 0) + 1.0 / (k + rank)
    return sorted(scores, key=scores.get, reverse=True)[:TOP_K]


def search_deals(question: str) -> list[str]:
    """Hybrid search: FAISS vector search + BM25 keyword search, merged with RRF."""
    _load_resources()

    # Vector search
    embedding = _model.encode([question])
    embedding = np.array(embedding, dtype="float32")
    faiss.normalize_L2(embedding)
    _, faiss_hits = _index.search(embedding, TOP_K)
    faiss_indices = [int(i) for i in faiss_hits[0] if i >= 0]

    # BM25 keyword search
    if _bm25 is not None:
        tokens = _tokenize(question)
        bm25_scores = _bm25.get_scores(tokens)
        bm25_indices = list(np.argsort(bm25_scores)[::-1][:TOP_K])
        merged = _rrf_merge(faiss_indices, bm25_indices)
    else:
        merged = faiss_indices

    deals_text = []
    for idx in merged:
        meta = _metadata[idx]
        date = meta.get("email_date", "")
        text = meta.get("raw_text", "")
        deals_text.append(f"[{date}] {text}")

    return deals_text


def ask_claude(question: str, deals: list[str], stream: bool = False):
    """Ask Claude about the deals. Returns response text, or a stream if stream=True."""
    context = "\n\n".join(deals)
    claude = anthropic.Anthropic()
    kwargs = dict(
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
    if stream:
        return claude.messages.stream(**kwargs)
    response = claude.messages.create(**kwargs)
    return response.content[0].text


def query(question: str):
    deals = search_deals(question)
    print(ask_claude(question, deals))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query VC deals with natural language")
    parser.add_argument("question", help="Your question about VC deals")
    args = parser.parse_args()
    query(args.question)
