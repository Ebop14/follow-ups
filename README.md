# follow-ups

Gmail email sync bot — fetches all emails sent to you and stores them in a local SQLite database.

## Setup

### 1. Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (or select an existing one)
3. Enable the **Gmail API**: APIs & Services → Library → search "Gmail API" → Enable
4. Create OAuth 2.0 credentials:
   - APIs & Services → Credentials → Create Credentials → OAuth client ID
   - Application type: **Desktop app**
   - Download the JSON file and save it as `credentials.json` in the project root

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run

```bash
python gmail_sync.py
```

On first run, a browser window opens for OAuth authorization. Grant read-only Gmail access. The token is saved to `token.json` for future runs.

### 4. Verify

```bash
sqlite3 emails.db "SELECT count(*) FROM emails;"
```

## How It Works

- Authenticates via OAuth 2.0 (read-only Gmail scope)
- Lists all messages matching `to:me`
- Fetches full message details and extracts headers + body
- Stores everything in `emails.db` (SQLite), skipping already-fetched messages
- On subsequent runs, only new messages are fetched
