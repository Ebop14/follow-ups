#!/usr/bin/env python3
"""Extract venture capital deals from Axios Pro Rata emails in emails.db.

Step 1: Extract raw bullet text, company name (from <strong> tags),
and link (from <a> tags) using HTML structure.
"""

import re
import sqlite3
import html


def extract_vc_html(body: str) -> str:
    """Extract the Venture Capital Deals section from raw HTML."""
    start = body.find('Venture Capital Deals')
    if start == -1:
        return ''
    section = body[start:]
    # End at next major section header — these appear in similar styled blocks
    for boundary in ['Private Equity Deals', 'Public Offerings',
                     'Liquidity Events', 'Fundraising', 'More M&amp;A',
                     'A MESSAGE FROM AXIOS', 'PE Deals', 'Exits']:
        idx = section.find(boundary, 30)
        if idx > 0:
            section = section[:idx]
            break
    return section


def extract_bullets(vc_html: str) -> list[dict]:
    """Parse each <p> bullet into {company_name, link, raw_text}."""
    bullets = []

    # Each deal is a <p> tag
    for p_match in re.finditer(r'<p\b[^>]*>(.*?)</p>', vc_html, re.DOTALL):
        p_html = p_match.group(1)

        # Extract the link — last <a> tag with axios.link or similar
        link = ''
        link_matches = re.findall(r'<a\s[^>]*href="([^"]*)"[^>]*>[^<]*</a>', p_html)
        if link_matches:
            link = link_matches[-1]

        # Extract company name — first <strong> tag (may have emoji prefix outside it)
        strong_match = re.search(r'<strong>(.*?)</strong>', p_html)
        if not strong_match:
            continue
        company_name = strong_match.group(1).strip()
        # Strip emoji prefixes from company name
        company_name = re.sub(r'^[🚀🚑🌎⚡️☕🏈💰🔫🏠🛡️🤖💊🔬📱]+\s*', '', company_name).strip()
        if not company_name:
            continue

        # Get plain text version of the full bullet
        raw_text = re.sub(r'<[^>]+>', '', p_html)
        raw_text = html.unescape(raw_text).strip()
        # Collapse whitespace
        raw_text = re.sub(r'\s+', ' ', raw_text)

        bullets.append({
            'company_name': company_name,
            'link': link,
            'raw_text': raw_text,
        })

    return bullets


def main():
    db = sqlite3.connect('emails.db')
    db.row_factory = sqlite3.Row

    # Create deals table
    db.execute('DROP TABLE IF EXISTS deals')
    db.execute('''
        CREATE TABLE deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email_id TEXT,
            company_name TEXT,
            raw_text TEXT,
            link TEXT,
            email_date TEXT
        )
    ''')

    emails = db.execute(
        "SELECT id, body, date FROM emails WHERE subject LIKE '%Pro Rata%'"
    ).fetchall()
    print(f"Found {len(emails)} Axios Pro Rata emails")

    total = 0
    for email in emails:
        vc_html = extract_vc_html(email['body'])
        if not vc_html:
            continue

        bullets = extract_bullets(vc_html)
        for b in bullets:
            db.execute(
                'INSERT INTO deals (email_id, company_name, raw_text, link, email_date) '
                'VALUES (?, ?, ?, ?, ?)',
                (email['id'], b['company_name'], b['raw_text'], b['link'], email['date'])
            )
        total += len(bullets)
        if bullets:
            print(f"  {email['date'][:30]}: {len(bullets)} bullets")

    db.commit()
    print(f"\nTotal: {total} bullets extracted")

    # Show samples
    print("\nSample entries:")
    for row in db.execute("SELECT company_name, link, raw_text FROM deals LIMIT 10"):
        name = row[0][:30]
        has_link = 'Y' if row[1] else 'N'
        text = row[2][:80]
        print(f"  [{has_link}] {name:<32} {text}")

    # Stats
    with_link = db.execute("SELECT COUNT(*) FROM deals WHERE link != ''").fetchone()[0]
    print(f"\n{with_link}/{total} bullets have a link ({100*with_link//total}%)")

    db.close()


if __name__ == '__main__':
    main()
