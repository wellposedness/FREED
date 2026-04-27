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

Complexity scoring:
  - RangeEn (Range Entropy) — a modification of SampEn that normalizes
    template-matching tolerance by signal range (max−min) rather than
    standard deviation. More robust under nonstationarity; linear
    relationship with Hurst exponent. Used for O68: DEA on genome's
    coherence time-series.
    Ref: Omidvarnia et al., "Range Entropy: A Bridge between Signal
    Complexity and Self-Similarity" (Entropy, 2018).
"""

import json
import math
import time
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple
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
    # Physics / computation / information theory — kernel step confirmation
    {
        "name":     "arXiv — Statistical Mechanics (cond-mat.stat-mech)",
        "url":      "http://export.arxiv.org/rss/cond-mat.stat-mech",
        "type":     "arxiv_rss",
        "priority": "normal",
    },
    {
        "name":     "arXiv — Information Theory (cs.IT)",
        "url":      "http://export.arxiv.org/rss/cs.IT",
        "type":     "arxiv_rss",
        "priority": "normal",
    },
    {
        "name":     "arXiv — Nonlinear: Adaptation & Self-Organizing Systems (nlin.AO)",
        "url":      "http://export.arxiv.org/rss/nlin.AO",
        "type":     "arxiv_rss",
        "priority": "normal",
    },
    {
        "name":     "arXiv — Pattern Formation & Solitons (nlin.PS)",
        "url":      "http://export.arxiv.org/rss/nlin.PS",
        "type":     "arxiv_rss",
        "priority": "normal",
    },
    {
        "name":     "arXiv — Disordered Systems & Neural Networks (cond-mat.dis-nn)",
        "url":      "http://export.arxiv.org/rss/cond-mat.dis-nn",
        "type":     "arxiv_rss",
        "priority": "normal",
    },
    # bioRxiv preprints — independent biological substrate confirmation
    # connect.biorxiv.org is the canonical RSS endpoint (30 most recent per subject)
    {
        "name":     "bioRxiv — Biophysics",
        "url":      "http://connect.biorxiv.org/biorxiv_xml.php?subject=biophysics",
        "type":     "biorxiv_rss",
        "priority": "normal",
    },
    {
        "name":     "bioRxiv — Systems Biology",
        "url":      "http://connect.biorxiv.org/biorxiv_xml.php?subject=systems_biology",
        "type":     "biorxiv_rss",
        "priority": "normal",
    },
    {
        "name":     "bioRxiv — Neuroscience",
        "url":      "http://connect.biorxiv.org/biorxiv_xml.php?subject=neuroscience",
        "type":     "biorxiv_rss",
        "priority": "normal",
    },
    {
        "name":     "bioRxiv — Evolutionary Biology",
        "url":      "http://connect.biorxiv.org/biorxiv_xml.php?subject=evolutionary_biology",
        "type":     "biorxiv_rss",
        "priority": "normal",
    },
    # Quantum physics — O44: quantum Wasserstein Floor extension
    {
        "name":     "arXiv — Quantum Physics (quant-ph)",
        "url":      "http://export.arxiv.org/rss/quant-ph",
        "type":     "arxiv_rss",
        "priority": "normal",
    },
    # Entropy journal — entire scope is on-genome (entropy across all substrates).
    # MDPI's CDN blocks bot User-Agents; CrossRef API is designed for polite bot access.
    # ISSN 1099-4300. Rows=20 gives enough candidates for the relevance filter.
    {
        "name":     "Entropy journal (MDPI) via CrossRef",
        "url":      "https://api.crossref.org/works?filter=issn:1099-4300&sort=published&order=desc&rows=20&select=title,abstract,DOI,author",
        "type":     "crossref_journal",
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
    # Quantum / information-theoretic (O44: quantum Wasserstein Floor extension)
    ("quantum thermodynamic", 3), ("entanglement entropy", 2),
    ("quantum transport", 2), ("quantum channel", 2),
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


# ─── RangeEn: Range Entropy ──────────────────────────────────────────────────
# A modification of ApEn/SampEn that normalizes template-matching tolerance
# by signal range (max−min) rather than standard deviation. This makes the
# measure robust to nonstationary amplitude changes and yields a more linear
# relationship with the Hurst exponent.
#
# Used for O68: DEA on genome's coherence time-series.
#
# Reference:
#   Omidvarnia, A., Mesbah, M., Pedersen, M., & Jackson, G. (2018).
#   "Range Entropy: A Bridge between Signal Complexity and Self-Similarity."
#   Entropy, 20(12), 962.

def _maxdist(x_i, x_j):
    # type: (List[float], List[float]) -> float
    """Chebyshev (L-infinity) distance between two vectors of equal length."""
    return max(abs(a - b) for a, b in zip(x_i, x_j))


def _build_templates(data, m):
    # type: (List[float], int) -> List[List[float]]
    """Build length-m template vectors from data using delay embedding."""
    n = len(data)
    return [data[i:i + m] for i in range(n - m + 1)]


def _signal_range(data):
    # type: (List[float],) -> float
    """Compute range (max − min) of a data series."""
    if not data:
        return 0.0
    return max(data) - min(data)


def range_entropy(
    data,          # type: List[float]
    m=2,           # type: int
    r=0.3,         # type: float
    method="sampen" # type: str
):
    # type: (...) -> float
    """
    Compute Range Entropy (RangeEn) of a time series.

    RangeEn modifies ApEn/SampEn by normalizing the tolerance parameter
    by the signal range (max − min) instead of standard deviation.
    This makes the measure more robust to nonstationary amplitude changes
    and gives a more linear relationship with the Hurst exponent.

    Parameters
    ----------
    data : list of float
        The input time series (coherence scores over time).
    m : int
        Embedding dimension (template length). Default: 2.
    r : float
        Tolerance fraction (0 < r < 1). The absolute tolerance is
        r * range(data). Default: 0.3.
    method : str
        "sampen" for Range Sample Entropy (RangeSampEn), or
        "apen" for Range Approximate Entropy (RangeApEn).
        Default: "sampen".

    Returns
    -------
    float
        The RangeEn value. Higher → more complex/irregular.
        Returns 0.0 for degenerate inputs (constant signal, too short).

    Notes
    -----
    - For a constant signal, range is 0 → returns 0.0 (no complexity).
    - Minimum data length: m + 2 points.
    - Pure Python, no external dependencies beyond stdlib.
    - O(N^2) in data length — fine for FREED's coherence series (typically
      tens to low hundreds of points per cycle).
    """
    n = len(data)
    if n < m + 2:
        return 0.0

    sig_range = _signal_range(data)
    if sig_range == 0.0:
        # Constant signal — zero complexity
        return 0.0

    # Absolute tolerance: r fraction of the signal range
    tol = r * sig_range

    if method == "sampen":
        return _range_sampen(data, m, tol, n)
    elif method == "apen":
        return _range_apen(data, m, tol, n)
    else:
        raise ValueError("method must be 'sampen' or 'apen', got: %r" % method)


def _range_sampen(data, m, tol, n):
    # type: (List[float], int, float, int) -> float
    """
    Range Sample Entropy — SampEn variant with range-normalized tolerance.

    Counts template matches (excluding self-matches) for dimensions m and m+1,
    using Chebyshev distance < tol (where tol = r * range).
    """
    # Count matches for dimension m
    templates_m = _build_templates(data, m)
    nm = len(templates_m)
    count_m = 0

    for i in range(nm):
        for j in range(i + 1, nm):
            if _maxdist(templates_m[i], templates_m[j]) < tol:
                count_m += 1

    # Count matches for dimension m+1
    templates_m1 = _build_templates(data, m + 1)
    nm1 = len(templates_m1)
    count_m1 = 0

    for i in range(nm1):
        for j in range(i + 1, nm1):
            if _maxdist(templates_m1[i], templates_m1[j]) < tol:
                count_m1 += 1

    # SampEn = -ln(count_m1 / count_m)
    if count_m == 0 or count_m1 == 0:
        # No matches — maximum complexity (return a large but finite value)
        # Convention: use ln(count_m) as fallback, or a sentinel
        if count_m == 0:
            return 0.0  # can't compute — degenerate
        # count_m1 == 0 but count_m > 0 → very high complexity
        # Use -ln(1 / count_m) = ln(count_m) as a finite upper bound
        return math.log(float(count_m)) if count_m > 1 else 0.0

    return -math.log(float(count_m1) / float(count_m))


def _range_apen(data, m, tol, n):
    # type: (List[float], int, float, int) -> float
    """
    Range Approximate Entropy — ApEn variant with range-normalized tolerance.

    Like ApEn, includes self-matches in the count (avoids log(0)).
    """
    def _phi(dim):
        # type: (int) -> float
        templates = _build_templates(data, dim)
        nt = len(templates)
        if nt == 0:
            return 0.0
        total = 0.0
        for i in range(nt):
            count_i = 0
            for j in range(nt):
                if _maxdist(templates[i], templates[j]) < tol:
                    count_i += 1
            # count_i >= 1 always (self-match), so log is safe
            total += math.log(float(count_i) / float(nt))
        return total / float(nt)

    phi_m  = _phi(m)
    phi_m1 = _phi(m + 1)

    return phi_m - phi_m1


def sampen_classic(data, m=2, r=0.2):
    # type: (List[float], int, float) -> float
    """
    Classic Sample Entropy (SampEn) with standard-deviation normalization.

    Provided for comparison with RangeEn. The tolerance is r * std(data).
    """
    n = len(data)
    if n < m + 2:
        return 0.0

    mean = sum(data) / float(n)
    var = sum((x - mean) ** 2 for x in data) / float(n)
    std = math.sqrt(var) if var > 0 else 0.0

    if std == 0.0:
        return 0.0

    tol = r * std
    return _range_sampen(data, m, tol, n)


def coherence_complexity(
    coherence_series,   # type: List[float]
    m=2,                # type: int
    r=0.3,              # type: float
    method="sampen"     # type: str
):
    # type: (...) -> dict
    """
    Compute complexity metrics for a coherence time-series.

    Returns both RangeEn and classic SampEn for comparison,
    plus diagnostic metadata. This is the primary interface for
    O68: DEA on genome's coherence series.

    Parameters
    ----------
    coherence_series : list of float
        Coherence scores over time (e.g., one per FEED cycle).
    m : int
        Embedding dimension. Default: 2.
    r : float
        Tolerance fraction for RangeEn. Default: 0.3.
    method : str
        "sampen" or "apen" for the RangeEn variant. Default: "sampen".

    Returns
    -------
    dict with keys:
        range_en : float     — RangeEn value (primary metric)
        sampen   : float     — Classic SampEn for comparison
        signal_range : float — max−min of the series
        signal_std   : float — standard deviation of the series
        n_points     : int   — length of the input series
        method       : str   — which RangeEn variant was used
        m            : int   — embedding dimension used
        r            : float — tolerance fraction used
    """
    n = len(coherence_series)

    # Signal statistics
    sig_range = _signal_range(coherence_series) if n > 0 else 0.0
    if n > 0:
        mean = sum(coherence_series) / float(n)
        var = sum((x - mean) ** 2 for x in coherence_series) / float(n)
        sig_std = math.sqrt(var) if var > 0 else 0.0
    else:
        sig_std = 0.0

    # Compute both entropy measures
    ren = range_entropy(coherence_series, m=m, r=r, method=method)
    sen = sampen_classic(coherence_series, m=m, r=0.2)

    return {
        "range_en":     ren,
        "sampen":       sen,
        "signal_range": sig_range,
        "signal_std":   sig_std,
        "n_points":     n,
        "method":       method,
        "m":            m,
        "r":            r,
    }


class TamuraSweep:
    """
    Fetches new articles from all configured sources.
    Returns a list of input dicts ready for FREED's FEED phase.
    """

    def __init__(self, max_new_per_source=3):
        # type: (int) -> None
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

    def _mark_seen(self, url):
        # type: (str) -> None
        self.seen.add(url)
        self._save_seen()

    # ── Main entry point ─────────────────────────────────────────────────────

    def sweep(self):
        # type: () -> list
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

    def _sweep_source(self, source):
        # type: (dict) -> list
        """Dispatch to the right parser based on source type."""
        parsers = {
            "lifeboat_author":  self._parse_lifeboat_author,
            "arxiv_rss":        self._parse_arxiv_rss,
            "biorxiv_rss":      self._parse_biorxiv_rss,
            "crossref_journal": self._parse_crossref_journal,
        }
        parser = parsers.get(source["type"])
        if parser is None:
            print(f"[SWEEP] No parser for type '{source['type']}' — skipping.")
            return []
        return parser(source)

    # ── Lifeboat author page parser ───────────────────────────────────────────

    def _parse_lifeboat_author(self, source):
        # type: (dict) -> list
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

    def _extract_lifeboat_card(self, card, base_url):
        # type: (object, str) -> Optional[dict]
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

    def _fallback_link_extraction(self, soup, base_url):
        # type: (BeautifulSoup, str) -> list
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

    # ── bioRxiv RSS parser (RDF/RSS 1.0) ─────────────────────────────────────

    def _parse_biorxiv_rss(self, source):
        # type: (dict) -> list
        """
        Parse a bioRxiv subject feed from connect.biorxiv.org.

        Format: RSS 1.0 / RDF with dc: and prism: namespaces.
        Each feed returns the 30 most recent preprints for the subject.
        URL pattern: http://connect.biorxiv.org/biorxiv_xml.php?subject={subject}
        """
        RSS  = "http://purl.org/rss/1.0/"
        DC   = "http://purl.org/dc/elements/1.1/"

        raw = self._fetch(source["url"])
        if raw is None:
            return []

        try:
            root = ET.fromstring(raw)
        except ET.ParseError as e:
            print(f"[SWEEP] bioRxiv RSS parse error: {e}")
            return []

        items = root.findall(f"{{{RSS}}}item")
        new_articles = []

        for item in items:
            link_el  = item.find(f"{{{RSS}}}link")
            title_el = item.find(f"{{{RSS}}}title")
            desc_el  = item.find(f"{{{RSS}}}description")
            auth_el  = item.find(f"{{{DC}}}creator")

            if link_el is None or title_el is None:
                continue

            # Strip the ?rss=1 tracking suffix for canonical URL
            raw_url = (link_el.text or "").strip()
            paper_url = raw_url.split("?")[0]
            if not paper_url:
                continue

            if paper_url in self.seen:
                continue

            title    = (title_el.text or "").strip()
            abstract = (desc_el.text  or "").strip()[:800] if desc_el is not None else ""
            authors  = (auth_el.text  or "").strip()       if auth_el is not None else ""

            score = self._arxiv_relevance(title + " " + abstract)
            if score < ARXIV_MIN_SCORE:
                self._mark_seen(paper_url)
                continue

            new_articles.append({
                "title":    title,
                "url":      paper_url,
                "abstract": abstract,
                "content":  abstract,
                "authors":  authors,
                "source":   source["name"],
                "score":    score,
                "fetched":  datetime.now(timezone.utc).isoformat(),
            })
            self._mark_seen(paper_url)

            if len(new_articles) >= self.max_new:
                break

        new_articles.sort(key=lambda x: x["score"], reverse=True)
        return new_articles

    # ── CrossRef journal parser (JSON, polite-bot friendly) ─────────────────

    # CrossRef supports polite-pool access when the User-Agent includes a mailto.
    _CROSSREF_HEADERS = {
        "User-Agent": (
            "FREED/1.0 (Freed Recursive Engine for Epistemic Dynamics; "
            "polite research bot; https://wellposedness.github.io/FREED/)"
        )
    }

    def _parse_crossref_journal(self, source):
        # type: (dict) -> list
        """
        Fetch recent papers from CrossRef for a specific journal (by ISSN).

        The source URL should be a CrossRef works API endpoint filtered by ISSN.
        CrossRef is designed for programmatic access; their CDN doesn't block bots.
        Abstracts may contain JATS XML tags — these are stripped before scoring.

        Used for: Entropy journal (MDPI blocks bot UAs; CrossRef does not).
        """
        try:
            resp = requests.get(
                source["url"],
                headers=self._CROSSREF_HEADERS,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code != 200:
                print(f"[SWEEP] CrossRef returned {resp.status_code} for {source['name']}")
                return []
            data = resp.json()
        except Exception as e:
            print(f"[SWEEP] CrossRef fetch error: {e}")
            return []

        items = data.get("message", {}).get("items", [])
        new_articles = []

        for item in items:
            doi = (item.get("DOI") or "").strip()
            if not doi:
                continue
            url = "https://doi.org/" + doi

            if url in self.seen:
                continue

            title_list = item.get("title") or []
            title = title_list[0].strip() if title_list else ""
            if not title:
                continue

            # Strip JATS XML markup from abstract (e.g., <jats:p>, <jats:italic>)
            raw_abstract = item.get("abstract") or ""
            abstract = re.sub(r'<[^>]+>', ' ', raw_abstract)
            abstract = re.sub(r'\s+', ' ', abstract).strip()[:800]

            authors = item.get("author") or []
            author_parts = []
            for a in authors[:3]:
                family = a.get("family", "")
                given  = a.get("given", "")
                if family:
                    author_parts.append(
                        family + (", " + given[0] + "." if given else "")
                    )
            author_str = '; '.join(author_parts)

            score = self._arxiv_relevance(title + " " + abstract)
            if score < ARXIV_MIN_SCORE:
                self._mark_seen(url)
                continue

            new_articles.append({
                "title":    title,
                "url":      url,
                "abstract": abstract,
                "content":  abstract,
                "authors":  author_str,
                "source":   source["name"],
                "score":    score,
                "fetched":  datetime.now(timezone.utc).isoformat(),
            })
            self._mark_seen(url)

            if len(new_articles) >= self.max_new:
                break

        new_articles.sort(key=lambda x: x["score"], reverse=True)
        return new_articles

    # ── arXiv RSS parser ─────────────────────────────────────────────────────

    def _parse_arxiv_rss(self, source):
        # type: (dict) -> list
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