"""
FREED — Queue Rescore
Genome-aware re-ranking of links_queue.json. Uses Haiku to score each queued
paper against current open obligations and genome symbols, then updates the
score field so the highest-value papers drain first.

Usage:
  python3 rescore_queue.py             # dry-run: show what would change
  python3 rescore_queue.py --apply     # write updated scores to links_queue.json
  python3 rescore_queue.py --batch N   # papers per Haiku call (default 8)
"""

import os
import re
import sys
import json
import time
import argparse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import requests
import anthropic
from bs4 import BeautifulSoup

FREED_DIR    = Path(__file__).parent
QUEUE_FILE   = FREED_DIR / "links_queue.json"
OBLIG_FILE   = FREED_DIR / "FREED_obligations.json"
SYMBOLS_FILE = FREED_DIR / "genome_symbols.json"

HAIKU_MODEL    = "claude-haiku-4-5-20251001"
DEFAULT_BATCH  = 8
POLITENESS     = 0.4   # seconds between API calls
FETCH_POLITENESS = 1.0  # seconds between web fetches
REQUEST_TIMEOUT  = 15

MAX_OB_CONTEXT  = 14   # open obligations included in prompt
MAX_SYM_CONTEXT = 10   # genome symbols included in prompt
BLURB_CAP       = 220  # chars of title+abstract per paper in prompt

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


# ── Context loaders ───────────────────────────────────────────────────────────

def _load_obligations():
    if not OBLIG_FILE.exists():
        return []
    obs = json.loads(OBLIG_FILE.read_text())
    active = [o for o in obs if o.get("status") in ("open", "partial")]
    def _pri(o):
        p = (o.get("priority") or "normal").lower()
        order = 0 if p in ("high", "critical") else 1 if p == "normal" else 2
        return (order, o.get("created", ""))
    active.sort(key=_pri)
    return active[:MAX_OB_CONTEXT]


def _load_symbols():
    if not SYMBOLS_FILE.exists():
        return []
    data = json.loads(SYMBOLS_FILE.read_text())
    pairs = [(k, v) for k, v in data.items() if k != "_meta" and isinstance(v, dict)]
    pairs.sort(key=lambda kv: -(kv[1].get("recurrence") or 0))
    return [k for k, _ in pairs[:MAX_SYM_CONTEXT]]


def _build_system_prompt(obligations, symbols):
    ob_lines = "\n".join(
        f"  {o['id']} [{o.get('priority','normal')}]: {(o.get('statement') or '')[:110]}"
        + (f"  closes_when: {o['closes_when'][:80]}" if o.get("closes_when") and o["closes_when"] not in ("MALFORMED", "None", None) else "")
        for o in obligations
    )
    sym_line = ", ".join(symbols)
    return (
        "You are scoring research papers for the FREED autonomous science daemon.\n"
        "Score each paper 0-12 for genome relevance based on the open obligations and\n"
        "genome symbols listed below.\n\n"
        "SCORING SCALE:\n"
        "  12   = directly resolves or closes a listed obligation\n"
        "  9-11 = advances 2+ obligations or challenges a core genome claim\n"
        "  6-8  = relevant to genome themes (criticality, thermodynamics, RSA, compression)\n"
        "  3-5  = marginal overlap, adjacent topics\n"
        "  0-2  = off-topic or already well-covered\n\n"
        f"OPEN OBLIGATIONS:\n{ob_lines}\n\n"
        f"GENOME SYMBOLS (highest recurrence first):\n  {sym_line}\n\n"
        "Return ONLY a compact JSON object mapping PAPER_N keys to integer scores.\n"
        'No explanation. Example: {"PAPER_1": 9, "PAPER_2": 3, "PAPER_3": 11}'
    )


# ── Paper blurb ───────────────────────────────────────────────────────────────

def _blurb(entry):
    title    = (entry.get("title") or "").strip()
    abstract = (entry.get("abstract") or "").strip()
    conv     = (entry.get("conv") or "").strip()

    if title and abstract and title.lower() not in abstract.lower():
        combined = f"{title} | {abstract}"
    elif title:
        combined = f"{title} | {conv}" if conv else title
    else:
        combined = conv or entry.get("url", "")

    return combined[:BLURB_CAP]


# ── Content fetching ─────────────────────────────────────────────────────────

def _needs_fetch(entry):
    """True if the entry lacks both title and abstract (nothing real to score)."""
    return not ((entry.get("title") or "").strip() or
                (entry.get("abstract") or "").strip())


def _extract_arxiv_id(url):
    m = re.search(r'arxiv\.org/(?:abs|pdf)/([0-9]+\.[0-9v]+)', url)
    return m.group(1) if m else None


def _is_search_url(url):
    return 'scholar.google.com/scholar?q=' in url or 'google.com/search?q=' in url


def _fetch_arxiv(arxiv_id):
    """Fetch title + abstract for an arXiv paper. Atom API first, HTML fallback."""
    try:
        r = requests.get(
            f"https://export.arxiv.org/api/query?id_list={arxiv_id}",
            headers=_HEADERS, timeout=REQUEST_TIMEOUT
        )
        if r.status_code == 200:
            ns = {'atom': 'http://www.w3.org/2005/Atom'}
            root = ET.fromstring(r.text)
            entry = root.find('atom:entry', ns)
            if entry is not None:
                title    = (entry.findtext('atom:title', '', ns) or '').strip().replace('\n', ' ')
                abstract = (entry.findtext('atom:summary', '', ns) or '').strip().replace('\n', ' ')
                if title and abstract:
                    return {'title': title, 'abstract': abstract}
    except Exception:
        pass
    try:
        r = requests.get(
            f"https://arxiv.org/abs/{arxiv_id}",
            headers=_HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True
        )
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            title_tag = soup.find('h1', class_='title') or soup.find('title')
            title = title_tag.get_text(strip=True).replace('Title:', '').strip() if title_tag else ''
            abs_tag = soup.find('blockquote', class_='abstract')
            abstract = abs_tag.get_text(strip=True).replace('Abstract:', '').strip() if abs_tag else ''
            if title or abstract:
                return {'title': title, 'abstract': abstract}
    except Exception:
        pass
    return {}


def _fetch_generic(url):
    """Fetch title + body text from a generic web page."""
    try:
        r = requests.get(url, headers=_HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if r.status_code != 200:
            return {}
        soup = BeautifulSoup(r.text, 'html.parser')
        title_tag = soup.find('title')
        title = title_tag.get_text(strip=True) if title_tag else ''
        for tag in soup(['nav', 'header', 'footer', 'aside', 'script', 'style']):
            tag.decompose()
        content_tag = (
            soup.find('article') or soup.find('main') or
            soup.find('div', class_=lambda c: c and any(
                x in c.lower() for x in ['content', 'article', 'post', 'entry']
            )) or soup.find('body')
        )
        text = content_tag.get_text(separator=' ', strip=True) if content_tag else ''
        text = re.sub(r'\s+', ' ', text).strip()
        return {'title': title, 'content': text[:4000]}
    except Exception:
        return {}


def _fetch_entry(entry):
    """Fetch title+abstract for an entry and update it in-place. Returns True if enriched."""
    url = entry.get("url", "")
    if not url or _is_search_url(url):
        return False
    arxiv_id = _extract_arxiv_id(url)
    if arxiv_id:
        data = _fetch_arxiv(arxiv_id)
        if data.get('title') or data.get('abstract'):
            entry['title']    = data.get('title', '')
            entry['abstract'] = data.get('abstract', '')
            return True
    data = _fetch_generic(url)
    if data.get('title') or data.get('content'):
        entry['title']    = data.get('title', '')
        entry['abstract'] = data.get('content', '')[:600]
        return True
    return False


# ── Haiku batch scorer ────────────────────────────────────────────────────────

def _score_batch(client, system_prompt, batch):
    lines = [f"PAPER_{i+1}: {_blurb(e)}" for i, e in enumerate(batch)]
    user_msg = "\n".join(lines)

    try:
        resp = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=120,
            system=system_prompt,
            messages=[{"role": "user", "content": user_msg}],
        )
        raw = resp.content[0].text.strip()
        m = re.search(r"\{[^}]+\}", raw, re.DOTALL)
        if not m:
            print(f"    [WARN] No JSON in response: {raw[:80]}")
            return {}, resp.usage
        scores = json.loads(m.group())
        result = {}
        for i in range(len(batch)):
            key = f"PAPER_{i+1}"
            if key in scores:
                try:
                    result[i] = max(0, min(12, int(scores[key])))
                except (TypeError, ValueError):
                    pass
        return result, resp.usage
    except Exception as e:
        print(f"    [WARN] Haiku call failed: {e}")
        return {}, None


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Rescore links_queue.json against genome")
    parser.add_argument("--apply", action="store_true",
                        help="Write updated scores to links_queue.json")
    parser.add_argument("--batch", type=int, default=DEFAULT_BATCH,
                        help=f"Papers per Haiku call (default {DEFAULT_BATCH})")
    parser.add_argument("--no-fetch", dest="no_fetch", action="store_true",
                        help="Skip web fetching — score on existing queue content only")
    args = parser.parse_args()

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not set. Run: source ~/.zshrc")
        sys.exit(1)

    queue = json.loads(QUEUE_FILE.read_text())

    # Pre-fetch phase: enrich entries that have no title/abstract
    all_queued = [(i, e) for i, e in enumerate(queue) if e.get("status") == "queued"]
    if not args.no_fetch:
        needs = [(i, e) for i, e in all_queued if _needs_fetch(e)]
        if needs:
            print(f"Fetch phase: {len(needs)}/{len(all_queued)} queued entries lack title/abstract — fetching...\n")
            for fi, (qi, entry) in enumerate(needs):
                url = entry.get("url", "")
                print(f"  [{fi+1:3d}/{len(needs)}] {url[:70]}", end=" ... ", flush=True)
                ok = _fetch_entry(entry)
                print("OK" if ok else "no content")
                if fi < len(needs) - 1:
                    time.sleep(FETCH_POLITENESS)
            print()

    candidates = [
        (i, e) for i, e in enumerate(queue)
        if e.get("status") == "queued" and _blurb(e).strip()
    ]

    if not candidates:
        print("No queued entries with scorable content.")
        return

    obligations = _load_obligations()
    symbols     = _load_symbols()

    print(f"Rescore queue: {len(candidates)} queued entries")
    print(f"Context: {len(obligations)} open obligations, {len(symbols)} genome symbols")
    if args.apply:
        print("Mode: APPLY — scores will be written\n")
    else:
        print("Mode: DRY RUN — pass --apply to write\n")

    client        = anthropic.Anthropic(api_key=api_key)
    system_prompt = _build_system_prompt(obligations, symbols)

    all_indices = [i for i, _ in candidates]
    all_entries = [e for _, e in candidates]
    batch_size  = args.batch

    changes          = []   # (queue_idx, old_score, new_score, blurb)
    total_in_tokens  = 0
    total_out_tokens = 0
    total_calls      = 0

    for start in range(0, len(all_entries), batch_size):
        batch_e = all_entries[start : start + batch_size]
        batch_i = all_indices[start : start + batch_size]
        end     = min(start + batch_size, len(all_entries))

        print(f"  [{start+1:3d}–{end:3d}] scoring...", end=" ", flush=True)
        scores, usage = _score_batch(client, system_prompt, batch_e)
        total_calls += 1
        if usage:
            total_in_tokens  += usage.input_tokens
            total_out_tokens += usage.output_tokens

        got = 0
        for local_i, global_i in enumerate(batch_i):
            if local_i in scores:
                new_score = scores[local_i]
                old_score = int(queue[global_i].get("score") or 0)
                blurb     = _blurb(batch_e[local_i])[:70]
                changes.append((global_i, old_score, new_score, blurb))
                got += 1

        print(f"{got}/{len(batch_e)} scored")
        if end < len(all_entries):
            time.sleep(POLITENESS)

    print(f"\n{len(changes)} entries rescored in {total_calls} Haiku call(s).")
    est_cost = total_in_tokens * 0.00000080 + total_out_tokens * 0.00000400
    print(f"Tokens: {total_in_tokens:,} in / {total_out_tokens:,} out  (~${est_cost:.4f})\n")

    # Sort by new score descending for display
    changes.sort(key=lambda x: -x[2])

    print("Top 25 after rescore:")
    print(f"  {'NEW':>3}  {'OLD':>3}  {'Δ':>3}  TITLE")
    print("  " + "-" * 72)
    for _, old, new, blurb in changes[:25]:
        delta = new - old
        arrow = f"+{delta}" if delta > 0 else (str(delta) if delta < 0 else " 0")
        print(f"  {new:3d}  {old:3d}  {arrow:>3}  {blurb}")

    promoted  = sum(1 for _, o, n, _ in changes if n > o)
    demoted   = sum(1 for _, o, n, _ in changes if n < o)
    unchanged = len(changes) - promoted - demoted
    print(f"\nPromoted: {promoted}  |  Demoted: {demoted}  |  Unchanged: {unchanged}")

    if not args.apply:
        print("\nDry run complete. Pass --apply to write changes.")
        return

    ts = datetime.now(timezone.utc).isoformat()
    for global_i, _, new_score, _ in changes:
        queue[global_i]["score"]      = new_score
        queue[global_i]["rescore_ts"] = ts

    QUEUE_FILE.write_text(json.dumps(queue, indent=2))
    print(f"\nWrote updated scores to {QUEUE_FILE.name}.")


if __name__ == "__main__":
    main()
