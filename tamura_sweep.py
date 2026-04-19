"""
FREED — Tamura Sweep (Piece 4)
Paper ingestion pipeline. The sensory surface of the organism.

Sources:
  - Cecile G. Tamura / Lifeboat Foundation  (curated AI/science/futures)
  - arXiv biophysics RSS feeds              (nature as independent substrate)

The sweep:
  1. Fetches each source
  2. Extracts new articles/papers
  3. For arXiv: keyword pre-filters against RSA-adjacent topics (no API cost)
  4. Fetches full text for new items
  5. Returns structured inputs ready for FEED

Seen URLs are tracked in tamura_seen.json — FREED never feeds the same article twice.
"""

import json
import time
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from feed_guard import sanitize

# ─── Paths ────────────────────────────────────────────────────────────────────
FREED_DIR  = Path(__file__).parent
SEEN_FILE  = FREED_DIR / "tamura_seen.json"

# ─── Source definitions ───────────────────────────────────────────────────────
SOURCES = [
    {
        "name":     "Cecile G. Tamura — Lifeboat Foundation",
        "url":      "https://lifeboat.com/blog/author/cecile-g-tamura",
        "type":     "lifeboat_author",
        "priority": "high",
    },
    # Biophysics — nature as independent substrate for RSA invariant confirmation
    {
        "name":     "arXiv — Neurons & Cognition (q-bio.NC)",
        "url":      "http://export.arxiv.org/rss/q-bio.NC",
        "type":     "arxiv_rss",
        "priority": "normal",
    },
    {
        "name":     "arXiv — Biological Physics (physics.bio-ph)",
        "url":      "http://export.arxiv.org/rss/physics.bio-ph",
        "type":     "arxiv_rss",
        "priority": "normal",
    },
    {
        "name":     "arXiv — Molecular Networks (q-bio.MN)",
        "url":      "http://export.arxiv.org/rss/q-bio.MN",
        "type":     "arxiv_rss",
        "priority": "normal",
    },
    {
        "name":     "arXiv — Populations & Evolution (q-bio.PE)",
        "url":      "http://export.arxiv.org/rss/q-bio.PE",
        "type":     "arxiv_rss",
        "priority": "normal",
    },
]

# ─── arXiv relevance pre-filter ───────────────────────────────────────────────
# Papers must hit at least ARXIV_MIN_SCORE to enter the FEED pipeline.
# Pure text matching — no API cost. Nature is the ultimate independent substrate;
# these keywords map directly to RSA framework concepts.
ARXIV_MIN_SCORE = 2

ARXIV_KEYWORDS = [
    # Thermodynamics / entropy
    ("thermodynamic", 3), ("entropy", 3), ("dissipation", 3), ("landauer", 3),
    ("free energy", 3), ("irreversib", 2), ("heat dissipat", 2),
    # Criticality / phase transitions
    ("criticality", 3), ("critical transition", 3), ("phase transition", 2),
    ("self-organized criticality", 3), ("edge of chaos", 2), ("bifurcation", 1),
    ("power.?law", 2), ("scale.?free", 2), ("zipf", 3), ("1/f noise", 2),
    # Information / compression
    ("information theoret", 2), ("compression", 2), ("minimum description", 3),
    ("kolmogorov", 2), ("mutual information", 2), ("predictive coding", 3),
    # Autopoiesis / self-organization
    ("autopoies", 3), ("self.organiz", 2), ("self.maintain", 2),
    ("recursive", 2), ("self.referent", 2), ("fixed.?point", 2),
    # Substrate / computation
    ("substrate", 2), ("physical.?implement", 2), ("neural substrate", 2),
    ("stochastic computation", 2), ("probabilistic computation", 2),
    # Minimal / irreducible
    ("minimal cell", 3), ("minimal genome", 3), ("irreducib", 2),
    ("generating set", 2), ("basis set", 1),
    # Conservation / symmetry / invariants
    ("conservation law", 2), ("symmetry break", 2), ("invariant", 1),
    ("noether", 3),
    # Consciousness / cognition / reasoning
    ("consciousness", 2), ("cognition", 1), ("integrated information", 2),
    ("phi", 1), ("global workspace", 2),
    # Scale invariance / renormalization
    ("scale invarian", 3), ("renormalization", 3), ("universality class", 2),
    ("coarse.grain", 2),
]

# ─── HTTP config ─────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "FREED/1.0 (Freed Recursive Engine for Epistemic Dynamics; "
        "research bot; contact: RSA-Omega framework)"
    ),
}
REQUEST_TIMEOUT = 20    # seconds
POLITENESS_DELAY = 2.0  # seconds between requests — be a good citizen


class TamuraSweep:
    """
    Fetches new articles from all configured sources.
    Returns a list of input dicts ready for FREED's FEED phase.
    """

    def __init__(self, max_new_per_source: int = 3):
        """
        max_new_per_source: how many new articles to return per source per cycle.
        Keeps FEED bounded — the daemon won't choke on a busy day.
        """
        self.max_new = max_new_per_source
        self._load_seen()

    # ── Seen-URL tracking ────────────────────────────────────────────────────

    def _load_seen(self):
        if SEEN_FILE.exists():
            with open(SEEN_FILE) as f:
                self.seen = set(json.load(f))
        else:
            self.seen = set()

    def _save_seen(self):
        with open(SEEN_FILE, "w") as f:
            json.dump(sorted(self.seen), f, indent=2)

    def _mark_seen(self, url: str):
        self.seen.add(url)
        self._save_seen()

    # ── Main entry point ─────────────────────────────────────────────────────

    def sweep(self) -> list[dict]:
        """
        Run the full sweep across all sources.
        Returns a flat list of new article dicts for FEED.
        """
        all_inputs = []

        for source in SOURCES:
            print(f"[SWEEP] Fetching: {source['name']}")
            try:
                inputs = self._sweep_source(source)
                all_inputs.extend(inputs)
                if inputs:
                    print(f"[SWEEP]   → {len(inputs)} new article(s).")
                else:
                    print(f"[SWEEP]   → No new articles.")
            except Exception as e:
                print(f"[SWEEP]   → Error: {e}")

            time.sleep(POLITENESS_DELAY)

        return all_inputs

    def _sweep_source(self, source: dict) -> list[dict]:
        """Dispatch to the right parser based on source type."""
        parsers = {
            "lifeboat_author": self._parse_lifeboat_author,
            "arxiv_rss":       self._parse_arxiv_rss,
        }
        parser = parsers.get(source["type"])
        if parser is None:
            print(f"[SWEEP] No parser for type '{source['type']}' — skipping.")
            return []
        return parser(source)

    # ── Lifeboat author page parser ───────────────────────────────────────────

    def _parse_lifeboat_author(self, source: dict) -> list[dict]:
        """
        Parse an author page on lifeboat.com.
        Extracts article cards: title, URL, excerpt, date.
        """
        html = self._fetch(source["url"])
        if html is None:
            return []

        soup = BeautifulSoup(html, "html.parser")
        articles = []

        # Lifeboat blog uses article or div cards — try multiple selectors
        # Common patterns: <article>, <div class="post">, <h2><a href>
        candidates = (
            soup.find_all("article") or
            soup.find_all("div", class_=re.compile(r"post|entry|blog-item", re.I)) or
            []
        )

        # Fallback: grab all links that look like blog posts
        if not candidates:
            candidates = self._fallback_link_extraction(soup, source["url"])

        seen_urls = set()
        for card in candidates:
            item = self._extract_lifeboat_card(card, source["url"])
            if item and item["url"] not in seen_urls:
                seen_urls.add(item["url"])
                articles.append(item)

        # Filter to unseen, up to max
        new_articles = []
        for art in articles:
            if art["url"] not in self.seen:
                new_articles.append(art)
            if len(new_articles) >= self.max_new:
                break

        # Fetch full text for each new article
        for art in new_articles:
            time.sleep(POLITENESS_DELAY)
            art["content"] = self._fetch_article_text(art["url"])
            self._mark_seen(art["url"])

        return new_articles

    def _extract_lifeboat_card(self, card, base_url: str):
        """Extract title, URL, excerpt, date from one article card."""
        # Title + URL
        link_tag = card.find("a", href=True)
        h_tag    = card.find(re.compile(r"h[123456]"))
        if h_tag:
            link_tag = h_tag.find("a", href=True) or link_tag

        if not link_tag:
            return None

        title = link_tag.get_text(strip=True)
        url   = urljoin(base_url, link_tag["href"])

        # Only keep links that look like blog posts (not tag/category pages)
        if not re.search(r'/blog/', url):
            return None
        if url == base_url:
            return None

        # Excerpt
        excerpt_tag = (
            card.find("p") or
            card.find("div", class_=re.compile(r"excerpt|summary|content|entry", re.I))
        )
        excerpt = excerpt_tag.get_text(strip=True)[:500] if excerpt_tag else ""

        # Date
        date_tag = card.find(["time", "span"], class_=re.compile(r"date|time|posted", re.I))
        date_str = ""
        if date_tag:
            date_str = date_tag.get("datetime", "") or date_tag.get_text(strip=True)

        return {
            "title":    title,
            "url":      url,
            "abstract": excerpt,
            "date":     date_str,
            "source":   "Cecile G. Tamura / Lifeboat Foundation",
            "fetched":  datetime.now(timezone.utc).isoformat(),
        }

    def _fallback_link_extraction(self, soup: BeautifulSoup, base_url: str) -> list:
        """
        When article cards aren't found, extract all blog-post-like links
        and wrap them in minimal dicts so the main loop can still process them.
        """
        links = soup.find_all("a", href=re.compile(r'/blog/\d{4}|/blog/[^/]+/[^/]+'))
        seen  = set()
        result = []
        for link in links:
            url = urljoin(base_url, link["href"])
            if url not in seen and url != base_url:
                seen.add(url)
                # Create a minimal card-like object
                mock = BeautifulSoup(
                    f'<div><a href="{url}">{link.get_text(strip=True)}</a></div>',
                    "html.parser"
                )
                result.append(mock.find("div"))
        return result

    # ── arXiv RSS parser ─────────────────────────────────────────────────────

    def _parse_arxiv_rss(self, source: dict) -> list[dict]:
        """
        Parse an arXiv RSS feed.
        Extracts title, abstract, URL, authors.
        Applies keyword relevance pre-filter — no API cost.
        Only processes announce_type=new (skips revisions).
        """
        raw = self._fetch(source["url"])
        if raw is None:
            return []

        try:
            root = ET.fromstring(raw)
        except ET.ParseError as e:
            print(f"[SWEEP] RSS parse error: {e}")
            return []

        # Strip namespaces for simpler access
        ns = {
            "arxiv": "http://arxiv.org/schemas/atom",
            "dc":    "http://purl.org/dc/elements/1.1/",
        }

        channel = root.find("channel")
        if channel is None:
            return []

        new_articles = []
        for item in channel.findall("item"):
            # Only new submissions — skip replacements/cross-lists
            announce = item.find("arxiv:announce_type", ns)
            if announce is not None and announce.text.strip() != "new":
                continue

            link_el  = item.find("link")
            title_el = item.find("title")
            desc_el  = item.find("description")
            auth_el  = item.find("dc:creator", ns)

            if link_el is None or title_el is None:
                continue

            url      = (link_el.text or "").strip()
            title    = (title_el.text or "").strip()
            desc_raw = (desc_el.text or "") if desc_el is not None else ""
            authors  = (auth_el.text or "") if auth_el is not None else ""

            # Strip the "arXiv:XXXX Announce Type: new\nAbstract: " prefix
            abstract = re.sub(
                r'^arXiv:\S+\s+Announce\s+Type:\s*\w+\s*\n?Abstract:\s*',
                '', desc_raw, flags=re.IGNORECASE
            ).strip()
            abstract = abstract[:800]

            if url in self.seen:
                continue

            # Relevance pre-filter — score against RSA-adjacent keywords
            score = self._arxiv_relevance(title + " " + abstract)
            if score < ARXIV_MIN_SCORE:
                self._mark_seen(url)   # mark seen so we don't re-check
                continue

            new_articles.append({
                "title":    title,
                "url":      url,
                "abstract": abstract,
                "content":  abstract,
                "authors":  authors,
                "source":   source["name"],
                "score":    score,
                "fetched":  datetime.now(timezone.utc).isoformat(),
            })
            self._mark_seen(url)

            if len(new_articles) >= self.max_new:
                break

        # Sort by relevance score — most relevant first
        new_articles.sort(key=lambda x: x["score"], reverse=True)
        return new_articles

    def _arxiv_relevance(self, text: str) -> int:
        """
        Score text against RSA-adjacent keywords.
        Returns total score — caller decides threshold.
        No API cost — pure regex matching.
        """
        text_lower = text.lower()
        score = 0
        for pattern, weight in ARXIV_KEYWORDS:
            if re.search(pattern, text_lower):
                score += weight
        return score

    # ── Full article fetch ────────────────────────────────────────────────────

    def _fetch_article_text(self, url: str) -> str:
        """
        Fetch the full text of an article page.
        Strips navigation, ads, sidebars — returns the main prose.
        """
        html = self._fetch(url)
        if html is None:
            return ""

        soup = BeautifulSoup(html, "html.parser")

        # Remove noise
        for tag in soup.find_all(["nav", "header", "footer", "aside", "script",
                                   "style", "form", "noscript"]):
            tag.decompose()

        # Try to find the article body
        body = (
            soup.find("article") or
            soup.find("div", class_=re.compile(r"entry-content|post-content|article-body|content", re.I)) or
            soup.find("main") or
            soup.find("body")
        )

        if body is None:
            return ""

        # Extract text, collapse whitespace
        text = body.get_text(separator="\n")
        text = re.sub(r'\n{3,}', '\n\n', text).strip()

        # Cap at 4000 chars — enough for L7 to work with, not so much it blows the budget
        text = text[:4000]

        # Prompt injection defense — strip any injection attempts before L7 sees this
        result = sanitize(text, source_url=url)
        if result.dropped:
            return ""   # article is poisoned — drop entirely
        return result.clean

    # ── HTTP fetch ───────────────────────────────────────────────────────────

    def _fetch(self, url: str):
        """Fetch a URL. Returns HTML string or None on failure."""
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            print(f"[SWEEP]   Fetch error for {url}: {e}")
            return None


# ─── Wire into freed.py ───────────────────────────────────────────────────────
# Replace _tamura_sweep_placeholder() in freed.py with:
#
#   from tamura_sweep import TamuraSweep
#   self._sweep = TamuraSweep(max_new_per_source=MAX_FEEDS_PER_CYCLE)
#
# Then in _phase_sweep():
#   inputs = self._sweep.sweep()


# ─── Quick test ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Running Tamura sweep test...\n")
    sweep = TamuraSweep(max_new_per_source=2)

    inputs = sweep.sweep()

    if not inputs:
        print("\nNo new articles found (either all seen, or fetch failed).")
        print(f"Seen URLs tracked: {len(sweep.seen)}")
    else:
        print(f"\n── {len(inputs)} new article(s) found ──")
        for i, inp in enumerate(inputs, 1):
            print(f"\n[{i}] {inp['title']}")
            print(f"     URL:    {inp['url']}")
            print(f"     Source: {inp['source']}")
            print(f"     Date:   {inp.get('date', 'unknown')}")
            excerpt = (inp.get('content') or inp.get('abstract', ''))[:200]
            print(f"     Text:   {excerpt}...")
