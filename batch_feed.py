"""
FREED — Batch Feed
Processes links_queue.json through L7, N links at a time.

Usage:
  python3 batch_feed.py             # process next 5 links
  python3 batch_feed.py --n 10      # process next 10 links
  python3 batch_feed.py --n 0       # process ALL remaining (careful with credits)
  python3 batch_feed.py --stats     # show queue status only
  python3 batch_feed.py --academic  # only process academic sources (score >= 6)

Results are written back to links_queue.json (status: done/failed).
Engrams go to the normal FREED log. Successful feeds update FREED_state.json.
"""

import os
import sys
import json
import time
import math
import hashlib
import argparse
import re as _re_module
import requests
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from collections import Counter

from bs4 import BeautifulSoup
from feed_guard import sanitize as guard_sanitize
from l7_agent import L7Agent

# ─── Paths ────────────────────────────────────────────────────────────────────
FREED_DIR   = Path(__file__).parent
QUEUE_FILE  = FREED_DIR / "links_queue.json"
STATE_FILE  = FREED_DIR / "FREED_state.json"
SEEN_FILE   = FREED_DIR / "tamura_seen.json"
LOG_DIR     = FREED_DIR / "FREED_log"
DEDUP_FILE  = FREED_DIR / "feed_dedup_index.json"
CONTENT_DEDUP_FILE = FREED_DIR / "feed_content_dedup.json"

DEFAULT_BATCH   = 5
POLITENESS_SEC  = 2      # delay between fetches
REQUEST_TIMEOUT = 15
MAX_CONTENT_CHARS = 8000  # truncate long pages before feeding

# ─── RangeEn normalization parameters ─────────────────────────────────────────
# Window length m for local-range normalization (RangeEn-style).
# Range = max - min over a sliding window of length m.
# This replaces global σ normalization for coherence/novelty scoring.
RANGEEN_WINDOW_M = 5
RANGEEN_FLOOR    = 1e-6   # avoid division by zero when range is flat

# ─── RangeEn (Sample Entropy) parameters ─────────────────────────────────────
# Embedding dimension and tolerance for the RangeEn complexity metric.
# Per the RangeEn paper, tolerance r is expressed as a fraction of the
# local range (max - min) of the time series, rather than as a fraction
# of the global standard deviation (as in classical SampEn).
RANGEEN_EMBED_DIM  = 2       # embedding dimension m for template matching
RANGEEN_TOLERANCE  = 0.3     # r as fraction of local range (0 < r < 1)
RANGEEN_MIN_TOKENS = 10      # minimum token count to attempt RangeEn


# ─── RangeEn: Range-normalized Sample Entropy ─────────────────────────────────

def _build_token_frequency_series(text):
    """
    Convert a text string into a token-frequency time series.

    Tokenizes the text into lowercased words, then for each token position i,
    records the cumulative frequency of that token up to position i.
    This produces a nonstationary time series whose complexity reflects
    the lexical diversity and structure of the input.

    Args:
        text: str — input text

    Returns:
        list of float — token-frequency time series
    """
    if not text:
        return []
    # Simple whitespace + punctuation tokenization
    tokens = _re_module.findall(r'[a-z0-9]+', text.lower())
    if not tokens:
        return []

    cumulative_counts = Counter()
    series = []
    for tok in tokens:
        cumulative_counts[tok] += 1
        series.append(float(cumulative_counts[tok]))
    return series


def _range_en(series, m=None, r=None):
    """
    Compute Range Entropy (RangeEn) of a time series.

    RangeEn is a modification of Sample Entropy (SampEn) where the tolerance
    threshold is defined relative to the local range (max - min) of each
    template pair, rather than the global standard deviation of the series.
    This makes it robust to amplitude nonstationarity — critical for
    token-frequency series that grow monotonically.

    Algorithm:
      1. Form template vectors of length m and m+1 from the series.
      2. For each pair of templates (i, j) where i != j:
         a. Compute the local range as max(concat(template_i, template_j))
            - min(concat(template_i, template_j)).
         b. Compute the Chebyshev distance (max absolute difference).
         c. Count a match if distance <= r * local_range.
      3. RangeEn = -ln(A / B) where:
         A = number of template matches at dimension m+1
         B = number of template matches at dimension m

    Args:
        series: list of float — the time series
        m: int — embedding dimension (default: RANGEEN_EMBED_DIM)
        r: float — tolerance as fraction of local range (default: RANGEEN_TOLERANCE)

    Returns:
        float — RangeEn value (higher = more complex/unpredictable)
               Returns float('inf') if A=0 (maximally complex),
               returns 0.0 if B=0 (degenerate/too short).
    """
    if m is None:
        m = RANGEEN_EMBED_DIM
    if r is None:
        r = RANGEEN_TOLERANCE

    n = len(series)
    if n < m + 2:
        return 0.0

    def _count_range_matches(dim):
        """Count template matches at a given embedding dimension using range tolerance."""
        templates = []
        for i in range(n - dim):
            templates.append(series[i:i + dim])

        count = 0
        num_templates = len(templates)
        for i in range(num_templates):
            for j in range(i + 1, num_templates):
                # Chebyshev distance
                dist = max(abs(templates[i][k] - templates[j][k]) for k in range(dim))

                # Local range: range of the union of both templates
                combined = templates[i] + templates[j]
                local_range = max(combined) - min(combined)

                if local_range < RANGEEN_FLOOR:
                    # Both templates are identical/flat — count as match
                    count += 1
                elif dist <= r * local_range:
                    count += 1

        return count

    B = _count_range_matches(m)
    A = _count_range_matches(m + 1)

    if B == 0:
        return 0.0
    if A == 0:
        return float('inf')

    return -math.log(A / B)


def compute_range_entropy(text, m=None, r=None):
    """
    Compute RangeEn complexity score for a text string.

    Converts text to a token-frequency time series, then computes
    Range Entropy. Returns a dict with the RangeEn value and metadata.

    For short texts (< RANGEEN_MIN_TOKENS tokens), returns None to signal
    insufficient data for reliable entropy estimation.

    Args:
        text: str — input text to analyze
        m: int — embedding dimension (optional, default from config)
        r: float — tolerance fraction (optional, default from config)

    Returns:
        dict with keys:
            'range_en': float — the RangeEn value
            'series_length': int — length of the token-frequency series
            'embed_dim': int — m used
            'tolerance': float — r used
        or None if text is too short.
    """
    series = _build_token_frequency_series(text)
    if len(series) < RANGEEN_MIN_TOKENS:
        return None

    # For very long texts, subsample to keep computation tractable.
    # RangeEn is O(N^2) in series length; cap at 500 tokens.
    max_len = 500
    if len(series) > max_len:
        # Take evenly spaced samples to preserve global structure
        step = len(series) / max_len
        series = [series[int(i * step)] for i in range(max_len)]

    used_m = m if m is not None else RANGEEN_EMBED_DIM
    used_r = r if r is not None else RANGEEN_TOLERANCE

    ren = _range_en(series, m=used_m, r=used_r)

    return {
        'range_en': ren,
        'series_length': len(series),
        'embed_dim': used_m,
        'tolerance': used_r,
    }


# ─── RangeEn local-range normalization ────────────────────────────────────────

def _range_normalize(scores):
    """
    RangeEn-style local sliding-window range normalization.

    Instead of normalizing a batch of scores by global standard deviation
    (which is sensitive to amplitude outliers from e.g. highly-cited papers
    or very long abstracts), we normalize each score by the local range
    (max - min) over a sliding window of length m centered on that score.

    This decouples each feed's score from batch composition, improving
    cross-session score comparability and reducing false-novelty signals
    from amplitude artifacts.

    Parameters follow the RangeEn paper: window length m, with range =
    max(window) - min(window), and a floor to avoid division by zero.

    Args:
        scores: list of float — raw yield/coherence scores for a batch

    Returns:
        list of float — range-normalized scores
    """
    if not scores:
        return scores

    n = len(scores)
    m = RANGEEN_WINDOW_M
    normalized = []

    for i in range(n):
        # Define the window: m elements centered on i (or as close as possible)
        half = m // 2
        win_start = max(0, i - half)
        win_end = min(n, i + half + 1)
        # Ensure we get at least m elements if possible
        if win_end - win_start < m:
            if win_start == 0:
                win_end = min(n, m)
            elif win_end == n:
                win_start = max(0, n - m)

        window = scores[win_start:win_end]
        local_range = max(window) - min(window)

        if local_range < RANGEEN_FLOOR:
            # Flat region — score is at baseline, normalize to 0.5
            normalized.append(0.5)
        else:
            # Normalize score within the local range to [0, 1]
            val = (scores[i] - min(window)) / local_range
            normalized.append(val)

    return normalized


# ─── Content-level deduplication (DOI / title+abstract hash) ──────────────────
#
# This layer hashes each feed input by DOI (if available) or by a SHA-256 of
# the normalized title+abstract. If the hash has been seen before, processing
# is blocked and a short "DUPLICATE — see gen N" notice is emitted instead.
#
# Rationale: the quadruple-feed of the RangeEn paper proved the pipeline wastes
# kernel cycles on already-digested material. Content-level dedup enforces γ=1
# criticality by redirecting processing budget toward novel inputs that can
# actually move obligations or generate new ones.

def _extract_doi_from_data(data):
    """
    Try to extract a DOI from fetched data.
    Checks explicit 'doi' field and scans content/abstract for DOI patterns.
    """
    # Explicit field
    doi = data.get('doi', '')
    if doi:
        return doi.strip().lower()

    # Scan title, abstract, content for DOI pattern
    for field in ('abstract', 'content', 'title'):
        text = data.get(field, '')
        if text:
            m = _re_module.search(r'(10\.\d{4,9}/[^\s]+)', text)
            if m:
                return m.group(1).rstrip('.,;)').lower()

    return None


def _extract_doi_from_url(url):
    """Try to extract a DOI from common DOI URL patterns."""
    if not url:
        return None
    # doi.org direct links
    m = _re_module.search(r'doi\.org/(10\.\d{4,9}/[^\s?#]+)', url)
    if m:
        return m.group(1).rstrip('.,;)').lower()
    return None


def _content_hash(doi, title, abstract):
    """
    Compute a content dedup hash.
    Priority: DOI if available, else SHA-256 of normalized title+abstract.
    Returns (hash_key: str, hash_type: str) or (None, None) if insufficient data.
    """
    if doi:
        # DOI is the gold standard — normalize and hash
        normalized_doi = doi.strip().lower()
        h = hashlib.sha256(('doi:' + normalized_doi).encode('utf-8')).hexdigest()[:40]
        return h, 'doi'

    # Fall back to title+abstract
    norm_title = _re_module.sub(r'\s+', ' ', (title or '').strip().lower())
    norm_abstract = _re_module.sub(r'\s+', ' ', (abstract or '').strip().lower())

    if not norm_title and not norm_abstract:
        return None, None

    combined = norm_title + '|||' + norm_abstract
    h = hashlib.sha256(combined.encode('utf-8')).hexdigest()[:40]
    return h, 'title+abstract'


def _load_content_dedup():
    """Load the content-level dedup registry from disk."""
    if CONTENT_DEDUP_FILE.exists():
        try:
            with open(CONTENT_DEDUP_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {}


def _save_content_dedup(registry):
    """Persist the content-level dedup registry."""
    with open(CONTENT_DEDUP_FILE, 'w') as f:
        json.dump(registry, f, indent=2, ensure_ascii=False)


def _content_dedup_check(doi, title, abstract):
    """
    Check if content has already been processed (by DOI or title+abstract hash).
    Returns (is_duplicate: bool, generation: int or None, hash_type: str or None).
    """
    h, htype = _content_hash(doi, title, abstract)
    if h is None:
        return False, None, None

    registry = _load_content_dedup()
    if h in registry:
        prev = registry[h]
        return True, prev.get('generation', '?'), htype

    return False, None, None


def _content_dedup_register(doi, title, abstract, url, generation):
    """Register content hash after successful processing."""
    h, htype = _content_hash(doi, title, abstract)
    if h is None:
        return

    registry = _load_content_dedup()
    registry[h] = {
        'generation': generation,
        'hash_type': htype,
        'url': url,
        'title': (title or '')[:120],
        'registered_at': datetime.now(timezone.utc).isoformat()
    }
    _save_content_dedup(registry)


def _bootstrap_content_dedup_from_logs():
    """
    On first run (no content dedup file yet), scan existing engram logs to
    populate the content dedup registry. Also scans the existing dedup index
    and queue for previously processed entries. Idempotent.
    """
    if CONTENT_DEDUP_FILE.exists():
        return  # already bootstrapped

    registry = {}
    gen_counter = 0

    # Scan engram logs
    if LOG_DIR.exists():
        for log_file in sorted(LOG_DIR.glob('freed_*.jsonl')):
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                            title = entry.get('title', '')
                            abstract = entry.get('abstract', '')
                            url = entry.get('url', '')
                            doi = entry.get('doi', '') or _extract_doi_from_url(url)

                            gen_counter += 1
                            h, htype = _content_hash(doi, title, abstract)
                            if h and h not in registry:
                                registry[h] = {
                                    'generation': gen_counter,
                                    'hash_type': htype,
                                    'url': url,
                                    'title': (title or '')[:120],
                                    'registered_at': entry.get('fed_at', entry.get('timestamp', ''))
                                }
                        except (json.JSONDecodeError, KeyError):
                            continue
            except IOError:
                continue

    # Also scan queue for already-done entries
    if QUEUE_FILE.exists():
        try:
            with open(QUEUE_FILE) as f:
                q = json.load(f)
            for entry in q:
                if entry.get('status') == 'done':
                    title = entry.get('title', '')
                    url = entry.get('url', '')
                    doi = _extract_doi_from_url(url)
                    # Queue entries usually don't have abstract, but try
                    abstract = entry.get('abstract', '')
                    h, htype = _content_hash(doi, title, abstract)
                    if h and h not in registry:
                        gen_counter += 1
                        registry[h] = {
                            'generation': gen_counter,
                            'hash_type': htype,
                            'url': url,
                            'title': (title or '')[:120],
                            'registered_at': entry.get('fed_at', '')
                        }
        except (json.JSONDecodeError, IOError):
            pass

    _save_content_dedup(registry)
    if registry:
        print(f'[CONTENT-DEDUP] Bootstrapped content dedup index: {len(registry)} hash(es) indexed.')


# ─── Deduplication ────────────────────────────────────────────────────────────

def _title_hash(title):
    """
    Compute a stable hash from a paper title.
    Normalizes whitespace and case before hashing.
    """
    import re
    if not title:
        return None
    normalized = re.sub(r'\s+', ' ', title.strip().lower())
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()[:32]


def _abstract_fingerprint(abstract):
    """
    Compute a fingerprint from abstract text.
    Uses first 500 chars after normalization to handle minor variations
    (e.g. trailing whitespace, line breaks) across different sources.
    """
    import re
    if not abstract:
        return None
    normalized = re.sub(r'\s+', ' ', abstract.strip().lower())[:500]
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()[:32]


def _load_dedup_index():
    """Load the deduplication index from disk."""
    if DEDUP_FILE.exists():
        try:
            with open(DEDUP_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {'title_hashes': {}, 'abstract_fps': {}}


def _save_dedup_index(index):
    """Persist the deduplication index."""
    with open(DEDUP_FILE, 'w') as f:
        json.dump(index, f, indent=2, ensure_ascii=False)


def _check_duplicate(title, abstract):
    """
    Check if a paper (by title hash or abstract fingerprint) has already
    been processed. Returns (is_duplicate: bool, match_info: str or None).

    Per Seed Integrity Rule 9: generate and pay debts, not collect trophies.
    Re-feeding confirmed duplicates wastes kernel cycles for diminishing returns.
    """
    index = _load_dedup_index()

    th = _title_hash(title)
    af = _abstract_fingerprint(abstract)

    # Check title hash
    if th and th in index.get('title_hashes', {}):
        prev = index['title_hashes'][th]
        return True, "title match — previously fed at {}".format(prev.get('fed_at', '?'))

    # Check abstract fingerprint
    if af and af in index.get('abstract_fps', {}):
        prev = index['abstract_fps'][af]
        return True, "abstract match — previously fed at {}".format(prev.get('fed_at', '?'))

    return False, None


def _register_in_dedup_index(title, abstract, url):
    """Register a successfully processed paper in the dedup index."""
    index = _load_dedup_index()

    now = datetime.now(timezone.utc).isoformat()
    record = {'url': url, 'fed_at': now}

    th = _title_hash(title)
    af = _abstract_fingerprint(abstract)

    if 'title_hashes' not in index:
        index['title_hashes'] = {}
    if 'abstract_fps' not in index:
        index['abstract_fps'] = {}

    if th:
        index['title_hashes'][th] = record
    if af:
        index['abstract_fps'][af] = record

    _save_dedup_index(index)


# ─── Engram log scanning for dedup bootstrap ──────────────────────────────────

def _bootstrap_dedup_from_logs():
    """
    On first run (no dedup index yet), scan existing engram logs to populate
    the dedup index with previously processed papers. Idempotent.
    """
    if DEDUP_FILE.exists():
        return  # already bootstrapped

    index = {'title_hashes': {}, 'abstract_fps': {}}
    if not LOG_DIR.exists():
        _save_dedup_index(index)
        return

    count = 0
    for log_file in sorted(LOG_DIR.glob('freed_*.jsonl')):
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        title = entry.get('title', '')
                        abstract = entry.get('abstract', '')
                        url = entry.get('url', '')
                        fed_at = entry.get('fed_at', entry.get('timestamp', ''))

                        record = {'url': url, 'fed_at': fed_at}
                        th = _title_hash(title)
                        af = _abstract_fingerprint(abstract)
                        if th:
                            index['title_hashes'][th] = record
                            count += 1
                        if af:
                            index['abstract_fps'][af] = record
                    except (json.JSONDecodeError, KeyError):
                        continue
        except IOError:
            continue

    _save_dedup_index(index)
    if count:
        print(f'[DEDUP] Bootstrapped dedup index from logs: {count} title(s) indexed.')


# ─── Fetch helpers ────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def _fetch_arxiv(arxiv_id: str) -> dict:
    """
    Fetch title + abstract for an arXiv paper.
    Primary: Atom API. Fallback: scrape the abstract HTML page.
    Returns empty dict only if both fail.
    """
    import re as _re
    import xml.etree.ElementTree as ET

    # ── Try Atom API first ────────────────────────────────────────────────────
    try:
        r = requests.get(
            f"https://export.arxiv.org/api/query?id_list={arxiv_id}",
            headers=HEADERS, timeout=REQUEST_TIMEOUT
        )
        if r.status_code == 200:
            ns = {'atom': 'http://www.w3.org/2005/Atom'}
            root = ET.fromstring(r.text)
            entry = root.find('atom:entry', ns)
            if entry is not None:
                title    = (entry.findtext('atom:title', '', ns) or '').strip().replace('\n', ' ')
                abstract = (entry.findtext('atom:summary', '', ns) or '').strip().replace('\n', ' ')
                authors  = [a.findtext('atom:name', '', ns)
                            for a in entry.findall('atom:author', ns)]
                if title and abstract:
                    return {'title': title, 'abstract': abstract, 'authors': authors}
    except Exception as e:
        print(f"  [FETCH] arXiv API error: {e}")

    # ── Fallback: scrape abstract page HTML ───────────────────────────────────
    try:
        r = requests.get(
            f"https://arxiv.org/abs/{arxiv_id}",
            headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True
        )
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')

            # Title: <h1 class="title mathjax">
            title_tag = soup.find('h1', class_='title')
            if not title_tag:
                title_tag = soup.find('title')
            title = title_tag.get_text(strip=True).replace('Title:', '').strip() if title_tag else ''

            # Abstract: <blockquote class="abstract mathjax">
            abs_tag = soup.find('blockquote', class_='abstract')
            abstract = abs_tag.get_text(strip=True).replace('Abstract:', '').strip() if abs_tag else ''

            # Authors: <div class="authors">
            auth_tag = soup.find('div', class_='authors')
            authors = []
            if auth_tag:
                authors = [a.get_text(strip=True) for a in auth_tag.find_all('a')][:5]

            if title or abstract:
                print(f"  [FETCH] arXiv API empty — used HTML fallback.")
                return {'title': title, 'abstract': abstract, 'authors': authors}
    except Exception as e:
        print(f"  [FETCH] arXiv HTML fallback error: {e}")

    return {}


def _extract_arxiv_id(url: str):
    """Extract arXiv ID from URL like arxiv.org/abs/2304.01904."""
    import re
    m = re.search(r'arxiv\.org/(?:abs|pdf)/([0-9]+\.[0-9v]+)', url)
    return m.group(1) if m else None


def _fetch_generic(url: str) -> dict:
    """Fetch title + body text from a generic web page."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT,
                         allow_redirects=True)
        if r.status_code != 200:
            return {'error': f'HTTP {r.status_code}'}
        soup = BeautifulSoup(r.text, 'html.parser')

        # Title
        title_tag = soup.find('title')
        title = title_tag.get_text(strip=True) if title_tag else ''

        # Remove nav/header/footer noise
        for tag in soup(['nav', 'header', 'footer', 'aside', 'script', 'style']):
            tag.decompose()

        # Try article/main first, fall back to body
        content_tag = (
            soup.find('article') or
            soup.find('main') or
            soup.find('div', class_=lambda c: c and any(
                x in c.lower() for x in ['content', 'article', 'post', 'entry']
            )) or
            soup.find('body')
        )
        text = content_tag.get_text(separator=' ', strip=True) if content_tag else ''
        # Collapse whitespace
        import re
        text = re.sub(r'\s+', ' ', text).strip()
        return {'title': title, 'content': text[:MAX_CONTENT_CHARS]}
    except Exception as e:
        return {'error': str(e)}


def is_search_url(url: str) -> bool:
    """True for Google Scholar / search fallback URLs — these return no content."""
    return 'scholar.google.com/scholar?q=' in url or 'google.com/search?q=' in url


def fetch_url(url: str) -> dict:
    """
    Fetch content from a URL. Returns dict with title, abstract/content.
    Handles arXiv specially for clean abstract extraction.
    """
    if is_search_url(url):
        return {'error': 'search_url — no direct paper URL available'}

    arxiv_id = _extract_arxiv_id(url)
    if arxiv_id:
        result = _fetch_arxiv(arxiv_id)
        if result.get('title') or result.get('abstract'):
            return result
        # Both methods failed — let generic try
        print(f"  [FETCH] arXiv fetch failed for {arxiv_id}, trying generic...")

    return _fetch_generic(url)


# ─── Feed prompt builder ──────────────────────────────────────────────────────

def build_feed_prompt(url: str, data: dict) -> str:
    title    = data.get('title', '(no title)')
    abstract = data.get('abstract', '')
    content  = data.get('content', '')
    authors  = data.get('authors', [])

    author_str = ', '.join(authors[:3]) if authors else ''
    body = abstract or content or '(no content retrieved)'

    parts = [f"FEED INPUT:\nURL: {url}"]
    if title:
        parts.append(f"Title: {title}")
    if author_str:
        parts.append(f"Authors: {author_str}")
    parts.append(f"\n{body[:MAX_CONTENT_CHARS]}")
    parts.append(
        "\nMap this input against the genome. "
        "Does it confirm, refute, or extend any invariant or obligation? "
        "Which obligation does it advance? What should be OBLIGATEd?"
    )
    return '\n'.join(parts)


# ─── State management ─────────────────────