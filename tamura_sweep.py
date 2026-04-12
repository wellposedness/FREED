"""
FREED — Tamura Sweep (Piece 4)
Paper ingestion pipeline. The sensory surface of the organism.

Primary source: Cecile G. Tamura
  lifeboat.com/blog/author/cecile-g-tamura

The sweep:
  1. Fetches the author page
  2. Extracts article listings (title, URL, excerpt, date)
  3. Filters to articles FREED hasn't seen yet
  4. Fetches full article text for new items
  5. Returns structured inputs ready for FEED

Seen URLs are tracked in tamura_seen.json — FREED never feeds the same article twice.
"""

import json
import time
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

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
    # Add more sources here later:
    # {
    #     "name":  "arXiv cs.AI new submissions",
    #     "url":   "https://arxiv.org/list/cs.AI/recent",
    #     "type":  "arxiv",
    #     "priority": "normal",
    # },
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
            "arxiv":           self._parse_arxiv,
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

    # ── arXiv parser (stub — uncomment source above to activate) ─────────────

    def _parse_arxiv(self, source: dict) -> list[dict]:
        """
        Parse arXiv listing page.
        Extracts: title, abstract, arXiv ID, URL.
        Stub — activate by adding arXiv to SOURCES above.
        """
        html = self._fetch(source["url"])
        if html is None:
            return []

        soup = BeautifulSoup(html, "html.parser")
        items = soup.find_all("div", class_="arxiv-result")
        if not items:
            items = soup.find_all("li", class_=re.compile(r"arxiv|submission"))

        new_articles = []
        for item in items:
            link  = item.find("a", href=re.compile(r"/abs/"))
            title = item.find(class_=re.compile(r"title|is-size"))
            abst  = item.find("span", class_=re.compile(r"abstract"))

            if not link or not title:
                continue

            url      = urljoin("https://arxiv.org", link["href"])
            title_s  = title.get_text(strip=True)
            abstract = abst.get_text(strip=True)[:600] if abst else ""

            if url not in self.seen:
                new_articles.append({
                    "title":    title_s,
                    "url":      url,
                    "abstract": abstract,
                    "content":  abstract,
                    "source":   "arXiv",
                    "fetched":  datetime.now(timezone.utc).isoformat(),
                })
                self._mark_seen(url)

            if len(new_articles) >= self.max_new:
                break

        return new_articles

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
        return text[:4000]

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
