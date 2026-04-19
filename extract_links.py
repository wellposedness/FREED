"""
FREED — Link Extractor
Parses Claude export JSON (and plain URL lists) to build links_queue.json.

Usage:
  python3 extract_links.py                          # process Claude export
  python3 extract_links.py --list my_links.txt      # process plain URL list (one per line)
  python3 extract_links.py --stats                  # show queue stats only

Output: FREED/links_queue.json  — ready for batch_feed.py
"""

import json
import re
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, urlunparse

FREED_DIR    = Path(__file__).parent
QUEUE_FILE   = FREED_DIR / "links_queue.json"
SEEN_FILE    = FREED_DIR / "tamura_seen.json"
EXPORT_FILE  = Path.home() / "Downloads" / "claude_export" / "conversations.json"

URL_RE = re.compile(r'https?://[^\s\"\'<>\)\]\}]+')

# ── Academic source scoring ───────────────────────────────────────────────────

ACADEMIC_PATTERNS = [
    (r'arxiv\.org',                    10, 'arXiv'),
    (r'biorxiv\.org',                  10, 'bioRxiv'),
    (r'medrxiv\.org',                  10, 'medRxiv'),
    (r'doi\.org',                       9, 'DOI'),
    (r'pubmed\.ncbi\.nlm\.nih\.gov',    9, 'PubMed'),
    (r'nature\.com/articles',           9, 'Nature'),
    (r'science\.org',                   9, 'Science'),
    (r'cell\.com',                      9, 'Cell'),
    (r'pnas\.org',                      9, 'PNAS'),
    (r'journals\.plos\.org',            8, 'PLOS'),
    (r'eneuro\.org',                    8, 'eNeuro'),
    (r'quantamagazine\.org',            7, 'Quanta'),
    (r'philarchive\.org',               7, 'PhilArchive'),
    (r'plato\.stanford\.edu',           7, 'SEP'),
    (r'zenodo\.org',                    7, 'Zenodo'),
    (r'royalsocietypublishing\.org',    8, 'Royal Society'),
    (r'pubs\.acs\.org',                 8, 'ACS'),
    (r'journals\.aps\.org',             8, 'APS'),
    (r'link\.springer\.com',            8, 'Springer'),
    (r'sciencedirect\.com',             8, 'ScienceDirect'),
    (r'frontiersin\.org',               7, 'Frontiers'),
    (r'mdpi\.com',                      6, 'MDPI'),
    (r'researchgate\.net',              6, 'ResearchGate'),
    (r'semanticscholar\.org',           6, 'SemanticScholar'),
    (r'psyarxiv\.com',                  8, 'PsyArXiv'),
    (r'osf\.io',                        7, 'OSF'),
    (r'ssrn\.com',                      7, 'SSRN'),
]

def score_url(url: str) -> tuple:
    """Returns (score, source_label). Score 0 = general web."""
    for pattern, score, label in ACADEMIC_PATTERNS:
        if re.search(pattern, url, re.I):
            return score, label
    return 0, 'web'


def clean_url(url: str) -> str:
    """Strip tracking params (fbclid, utm_*, etc.) from URL."""
    parsed = urlparse(url)
    # Strip trailing punctuation that got swept up by regex
    path = parsed.path.rstrip('.,;:!?')
    # Remove common tracking query params
    if parsed.query:
        params = []
        for part in parsed.query.split('&'):
            key = part.split('=')[0].lower()
            if key not in ('fbclid', 'utm_source', 'utm_medium', 'utm_campaign',
                           'utm_content', 'utm_term', 'aem', 'aem_id'):
                params.append(part)
        query = '&'.join(params)
    else:
        query = ''
    return urlunparse((parsed.scheme, parsed.netloc, path,
                       parsed.params, query, ''))


# ── Loaders ───────────────────────────────────────────────────────────────────

def load_seen() -> set:
    if SEEN_FILE.exists():
        with open(SEEN_FILE) as f:
            return set(json.load(f))
    return set()


def load_queue() -> list:
    if QUEUE_FILE.exists():
        with open(QUEUE_FILE) as f:
            return json.load(f)
    return []


def extract_from_export(path: Path) -> list:
    """Extract URLs from Claude conversations.json export."""
    print(f"[EXTRACT] Reading {path} ...")
    with open(path, encoding='utf-8') as f:
        data = json.load(f)

    entries = []
    for conv in data:
        title = conv.get('name', 'untitled')
        created = conv.get('created_at', '')
        for msg in conv.get('chat_messages', []):
            sender = msg.get('sender', 'unknown')
            text = msg.get('text', '') or ''
            for block in msg.get('content', []):
                if isinstance(block, dict):
                    text += ' ' + (block.get('text', '') or '')
            for raw_url in URL_RE.findall(text):
                url = clean_url(raw_url)
                score, label = score_url(url)
                entries.append({
                    'url':    url,
                    'score':  score,
                    'source': label,
                    'from':   sender,
                    'conv':   title,
                    'added':  datetime.now(timezone.utc).isoformat(),
                    'status': 'queued',
                })
    return entries


def extract_from_list(path: Path) -> list:
    """Extract URLs from a plain text file, one URL per line."""
    print(f"[EXTRACT] Reading URL list {path} ...")
    entries = []
    with open(path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            url = clean_url(line)
            score, label = score_url(url)
            entries.append({
                'url':    url,
                'score':  score,
                'source': label,
                'from':   'manual',
                'conv':   'link_list',
                'added':  datetime.now(timezone.utc).isoformat(),
                'status': 'queued',
            })
    return entries


# ── Merge & deduplicate ───────────────────────────────────────────────────────

def merge(existing: list, new_entries: list, seen: set) -> tuple:
    existing_urls = {e['url'] for e in existing}
    added, skipped_seen, skipped_dup = 0, 0, 0

    for e in new_entries:
        url = e['url']
        if url in seen:
            skipped_seen += 1
            continue
        if url in existing_urls:
            skipped_dup += 1
            continue
        # Boost score for URLs you personally shared
        if e.get('from') == 'human':
            e['score'] += 2
        existing.append(e)
        existing_urls.add(url)
        added += 1

    # Sort: human-submitted first, then by score desc
    existing.sort(key=lambda x: (
        0 if x.get('from') == 'human' else 1,
        -x.get('score', 0)
    ))
    return existing, added, skipped_seen, skipped_dup


# ── Stats ─────────────────────────────────────────────────────────────────────

def print_stats(queue: list):
    total   = len(queue)
    queued  = sum(1 for e in queue if e['status'] == 'queued')
    done    = sum(1 for e in queue if e['status'] == 'done')
    failed  = sum(1 for e in queue if e['status'] == 'failed')

    by_source = {}
    for e in queue:
        if e['status'] == 'queued':
            s = e.get('source', 'web')
            by_source[s] = by_source.get(s, 0) + 1

    print(f"\n[QUEUE] {QUEUE_FILE}")
    print(f"  Total:   {total}")
    print(f"  Queued:  {queued}")
    print(f"  Done:    {done}")
    print(f"  Failed:  {failed}")
    if by_source:
        print("  Sources (queued):")
        for s, n in sorted(by_source.items(), key=lambda x: -x[1]):
            print(f"    {s:<22} {n}")

    academic = sum(1 for e in queue if e['status']=='queued' and e.get('score',0) >= 6)
    print(f"  Academic (score≥6): {academic}")
    print()


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='FREED link extractor')
    parser.add_argument('--list',  metavar='FILE', help='Plain text file of URLs')
    parser.add_argument('--stats', action='store_true', help='Show queue stats only')
    args = parser.parse_args()

    queue = load_queue()

    if args.stats:
        print_stats(queue)
        return

    seen = load_seen()

    if args.list:
        new_entries = extract_from_list(Path(args.list))
    else:
        if not EXPORT_FILE.exists():
            print(f"[EXTRACT] Export not found at {EXPORT_FILE}")
            print("  Pass --list <file> for a plain URL list, or update EXPORT_FILE path.")
            sys.exit(1)
        new_entries = extract_from_export(EXPORT_FILE)

    queue, added, skipped_seen, skipped_dup = merge(queue, new_entries, seen)

    with open(QUEUE_FILE, 'w') as f:
        json.dump(queue, f, indent=2, ensure_ascii=False)

    print(f"[EXTRACT] Added:         {added}")
    print(f"[EXTRACT] Already seen:  {skipped_seen}")
    print(f"[EXTRACT] Duplicates:    {skipped_dup}")
    print_stats(queue)


if __name__ == '__main__':
    main()
