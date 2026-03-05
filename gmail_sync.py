#!/usr/bin/env python3
"""Fetch all emails sent to the authenticated user and store them in SQLite."""

import base64
import os
import sqlite3
import sys
from datetime import datetime, timezone

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE = "token.json"
DB_FILE = "emails.db"


def authenticate():
    """Authenticate with Gmail API via OAuth 2.0, returning a service object."""
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(CREDENTIALS_FILE):
                print(
                    f"Error: {CREDENTIALS_FILE} not found.\n"
                    "Download OAuth 2.0 credentials from Google Cloud Console\n"
                    "and place the file in the project root."
                )
                sys.exit(1)
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
    return build("gmail", "v1", credentials=creds)


def init_db():
    """Create the emails table if it doesn't exist and return a connection."""
    conn = sqlite3.connect(DB_FILE)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS emails (
            id TEXT PRIMARY KEY,
            thread_id TEXT,
            from_address TEXT,
            to_address TEXT,
            subject TEXT,
            date TEXT,
            snippet TEXT,
            body TEXT,
            labels TEXT,
            fetched_at TIMESTAMP
        )
        """
    )
    conn.commit()
    return conn


def get_header(headers, name):
    """Extract a header value by name (case-insensitive)."""
    name_lower = name.lower()
    for h in headers:
        if h["name"].lower() == name_lower:
            return h["value"]
    return ""


def extract_body(payload):
    """Extract the message body, preferring text/plain over text/html."""
    # Simple single-part message
    if "body" in payload and payload["body"].get("data"):
        return base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8", errors="replace")

    parts = payload.get("parts", [])
    plain = ""
    html = ""
    for part in parts:
        mime = part.get("mimeType", "")
        if mime == "text/plain" and part.get("body", {}).get("data"):
            plain = base64.urlsafe_b64decode(part["body"]["data"]).decode("utf-8", errors="replace")
        elif mime == "text/html" and part.get("body", {}).get("data"):
            html = base64.urlsafe_b64decode(part["body"]["data"]).decode("utf-8", errors="replace")
        # Recurse into nested multipart
        elif mime.startswith("multipart/") and "parts" in part:
            nested = extract_body(part)
            if nested:
                if not plain:
                    plain = nested
    return plain or html


def sync_emails(service, conn):
    """Fetch all emails sent to the user and upsert into SQLite."""
    print("Fetching message list...")
    message_ids = []
    page_token = None
    while True:
        kwargs = {"userId": "me", "q": "to:me", "maxResults": 500}
        if page_token:
            kwargs["pageToken"] = page_token
        result = service.users().messages().list(**kwargs).execute()
        messages = result.get("messages", [])
        message_ids.extend(m["id"] for m in messages)
        print(f"  Found {len(message_ids)} messages so far...")
        page_token = result.get("nextPageToken")
        if not page_token:
            break

    if not message_ids:
        print("No messages found.")
        return

    print(f"\nTotal messages to process: {len(message_ids)}")

    # Check which IDs already exist
    existing = set()
    cursor = conn.execute("SELECT id FROM emails")
    for row in cursor:
        existing.add(row[0])
    new_ids = [mid for mid in message_ids if mid not in existing]
    print(f"Already stored: {len(existing)}, new to fetch: {len(new_ids)}")

    for i, msg_id in enumerate(new_ids, 1):
        try:
            msg = service.users().messages().get(userId="me", id=msg_id, format="full").execute()
            headers = msg.get("payload", {}).get("headers", [])
            body = extract_body(msg.get("payload", {}))
            labels = ",".join(msg.get("labelIds", []))
            now = datetime.now(timezone.utc).isoformat()

            conn.execute(
                """
                INSERT OR IGNORE INTO emails
                    (id, thread_id, from_address, to_address, subject, date, snippet, body, labels, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    msg["id"],
                    msg.get("threadId", ""),
                    get_header(headers, "From"),
                    get_header(headers, "To"),
                    get_header(headers, "Subject"),
                    get_header(headers, "Date"),
                    msg.get("snippet", ""),
                    body,
                    labels,
                    now,
                ),
            )
            if i % 50 == 0:
                conn.commit()
            print(f"  [{i}/{len(new_ids)}] {get_header(headers, 'Subject')[:60]}")
        except Exception as e:
            print(f"  [{i}/{len(new_ids)}] Error fetching {msg_id}: {e}")

    conn.commit()
    total = conn.execute("SELECT count(*) FROM emails").fetchone()[0]
    print(f"\nDone. Total emails in database: {total}")


def main():
    service = authenticate()
    conn = init_db()
    try:
        sync_emails(service, conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
