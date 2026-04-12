"""
FREED — Smart Backfill
Crawls the full Tamura archive, scores every title for relevance
with a single cheap Claude call, then processes papers in priority order.

Two relevance tiers:
  DIRECT     (score 0-3): directly touches RSA / MCPM / Freed's Law /
                          Autokinetics / Zipf / criticality / thermodynamics
                          of computation / active inference / autopoiesis
  ORTHOGONAL (score 0-3): adjacent field that could inform or modify the
                          genome's architecture — complexity, emergence,
                          information theory, category theory, consciousness,
                          topology, evolutionary dynamics, etc.

Papers are processed in order of (DIRECT*2 + ORTHOGONAL), highest first.
Full text is only fetched for papers that score above the cutoff.
Budget is enforced by the Astrocyte throughout.

Usage:
    python3 ~/FREED/backfill.py
"""

import os
import sys
import json
import time
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import anthropic

from l7_agent    import L7Agent
from astrocyte   import Astrocyte
from tamura_sweep import TamuraSweep, HEADERS, REQUEST_TIMEOUT, POLITENESS_DELAY

# ─── Config ───────────────────────────────────────────────────────────────────
FREED_DIR       = Path(__file__).parent
TAMURA_BASE     = "https://lifeboat.com/blog/author/cecile-g-tamura"

# Only process papers with combined score >= this threshold
# (DIRECT*2 + ORTHOGONAL >= SCORE_CUTOFF)
SCORE_CUTOFF    = 2      # 0 = process everything, 6 = direct hits only

# Max papers to process per run (budget backstop)
MAX_PER_RUN     = 30

# Score cache — so we don't re-score on subsequent runs
SCORE_CACHE     = FREED_DIR / "backfill_scores.json"

MODEL           = "claude-opus-4-6"


# ─── HTTP helpers ─────────────────────────────────────────────────────────────

def fetch(url: str):
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"  [fetch error] {e}")
        return None


# ─── Phase 1: Archive discovery (zero AI tokens) ─────────────────────────────

def crawl_archive(base_url: str) -> list:
    """
    Crawl all paginated pages of the author archive.
    Returns list of (title, url) tuples. No AI calls.
    """
    all_links = []
    seen_urls = set()
    page_url  = base_url
    page_num  = 1

    while page_url:
        print(f"  Page {page_num}: {page_url}")
        html = fetch(page_url)
        if html is None:
            break

        soup  = BeautifulSoup(html, "html.parser")
        found = 0

        cards = (
            soup.find_all("article") or
            soup.find_all("div", class_=re.compile(r"post|entry|blog-item", re.I)) or
            []
        )

        if cards:
            for card in cards:
                h    = card.find(re.compile(r"h[123456]"))
                a    = (h.find("a", href=True) if h else None) or card.find("a", href=True)
                if not a:
                    continue
                title = a.get_text(strip=True)
                url   = urljoin(base_url, a["href"])
                if "/blog/" in url and url not in seen_urls and len(title) > 4:
                    all_links.append((title, url))
                    seen_urls.add(url)
                    found += 1
        else:
            for a in soup.find_all("a", href=re.compile(r'/blog/')):
                url   = urljoin(base_url, a["href"])
                title = a.get_text(strip=True)
                if url not in seen_urls and url != base_url and len(title) > 4:
                    all_links.append((title, url))
                    seen_urls.add(url)
                    found += 1

        print(f"    → {found} link(s)")

        # Next page — must stay on lifeboat.com
        nxt = (
            soup.find("a", string=re.compile(r"next|older|→|»", re.I)) or
            soup.find("a", class_=re.compile(r"next|older", re.I))
        )
        if nxt and nxt.get("href"):
            candidate = urljoin(base_url, nxt["href"])
            if candidate != page_url and "lifeboat.com" in candidate:
                page_url = candidate
                page_num += 1
                time.sleep(POLITENESS_DELAY)
                continue
        break

    return all_links


# ─── Phase 2: Batch relevance scoring (one Claude call for all titles) ────────

SCORE_SYSTEM = """You are the relevance scorer for FREED — the Freed Recursive Engine for Epistemic Dynamics.

The genome is built on:
  - Freed's Law: ∃R(t) → ∃M₀ : dS(M_R,t)/dt > 0  (reasoning requires physical substrate, generates entropy)
  - RSA Kernel: Perceive → Represent → Predict → Compare → Adjust → Compress → Repeat
  - MCPM (Only Processes exist — confirmed at 6 scales)
  - Autokinetics (self-reinforcing movement/pathway dynamics)
  - γ=1 criticality (phase parameter between frozen singularity and dissipative gas)
  - Zipf's Law as output equilibrium of high-order compression
  - Wasserstein Floor (ΔS_min = W₂(P,Q)²/T·Δτ·μ)
  - Active inference / Free Energy Principle
  - Autopoiesis (R[R]=R, self-generating systems)
  - Thermodynamics of computation (Landauer)
  - 19-scale layer hierarchy from L(-3) Mathematical to L20+ Monoidal Legislation
  - Obligations: O21 (spectral γ / belief revision), O28 (entropy asymmetry ratio / intelligence), O34 (conservation law bijection)

Score each paper title on two dimensions, integers 0-3:

DIRECT (0-3): How directly does this paper touch the genome's core claims?
  3 = directly confirms, extends, or challenges a genome invariant or theorem
  2 = strong overlap with a genome concept (entropy, criticality, compression, autopoiesis, substrate, prediction)
  1 = tangential connection
  0 = no meaningful connection

ORTHOGONAL (0-3): Could this paper inform or modify the genome's architecture even if not directly about it?
  3 = opens a new formal pathway into the genome (e.g. category theory, topology, new thermodynamic result)
  2 = strong adjacent field (evolutionary dynamics, information geometry, complexity theory, consciousness)
  1 = weakly adjacent
  0 = irrelevant to genome development

Respond ONLY as a JSON array. Each entry: {"i": <index>, "d": <DIRECT>, "o": <ORTHOGONAL>}
No preamble, no explanation, no extra fields outside the JSON."""


SCORE_BATCH_SIZE = 50   # titles per scoring call


def score_batch(client, batch: list, offset: int):
    """Score one batch of (title, url) pairs. Returns (scored_list, usage)."""
    numbered = "\n".join(
        f'{offset + i}: "{title}"'
        for i, (title, _) in enumerate(batch)
    )
    message = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        system=SCORE_SYSTEM,
        messages=[{"role": "user", "content": numbered}],
    )
    raw = message.content[0].text.strip()

    # Strip markdown fences if present
    raw = re.sub(r'^```[a-z]*\n?', '', raw, flags=re.MULTILINE)
    raw = re.sub(r'```$', '', raw, flags=re.MULTILINE).strip()

    scores = []
    try:
        scores = json.loads(raw)
    except json.JSONDecodeError:
        # Try to salvage valid entries from a truncated response
        # Extract all complete {"i":...,"d":...,"o":...} objects
        matches = re.findall(r'\{"i"\s*:\s*(\d+)\s*,\s*"d"\s*:\s*(\d+)\s*,\s*"o"\s*:\s*(\d+)[^}]*\}', raw)
        for m in matches:
            scores.append({"i": int(m[0]), "d": int(m[1]), "o": int(m[2])})
        if scores:
            print(f"(salvaged {len(scores)} entries from truncated response)", end=" ")

    results = []
    for s in scores:
        idx = s.get("i", -1) - offset
        if 0 <= idx < len(batch):
            title, url = batch[idx]
            d = s.get("d", 0)
            o = s.get("o", 0)
            results.append({
                "title":      title,
                "url":        url,
                "direct":     d,
                "orthogonal": o,
                "combined":   d * 2 + o,
            })
    return results, message.usage


def score_titles(client, astrocyte, titles_with_urls: list, score_cache: dict) -> list:
    """
    Score all titles in batches of SCORE_BATCH_SIZE.
    Saves cache after every batch — safe to interrupt and resume.
    Returns sorted list of score dicts.
    """
    total   = len(titles_with_urls)
    batches = [titles_with_urls[i:i+SCORE_BATCH_SIZE]
               for i in range(0, total, SCORE_BATCH_SIZE)]
    print(f"\n[SCORE] Scoring {total} titles in {len(batches)} batch(es)...")

    all_results = []
    total_in = total_out = 0

    for b_num, batch in enumerate(batches, 1):
        # Stop gracefully if output budget is nearly gone
        if astrocyte.remaining_output < 800:
            print(f"\n[SCORE] Output budget low ({astrocyte.remaining_output} tokens) — "
                  f"stopping after batch {b_num-1}. "
                  f"Scores cached. Resume tomorrow.")
            break

        print(f"  Batch {b_num}/{len(batches)} ({len(batch)} titles)...", end=" ", flush=True)
        results, usage = score_batch(client, batch, offset=(b_num-1)*SCORE_BATCH_SIZE)
        astrocyte.record_usage(usage.input_tokens, usage.output_tokens)
        total_in  += usage.input_tokens
        total_out += usage.output_tokens
        all_results.extend(results)
        print(f"{usage.input_tokens}in/{usage.output_tokens}out tokens.")

        # Save cache after every batch — resume-safe
        for s in results:
            score_cache[s["url"]] = s
        with open(SCORE_CACHE, "w") as f:
            json.dump(score_cache, f, indent=2)

    all_results.sort(key=lambda x: x["combined"], reverse=True)
    print(f"[SCORE] Done. Total: {total_in}in / {total_out}out tokens.\n")
    return all_results


# ─── Phase 3: Fetch full text ─────────────────────────────────────────────────

def fetch_article_text(url: str) -> str:
    html = fetch(url)
    if html is None:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["nav", "header", "footer", "aside",
                               "script", "style", "form", "noscript"]):
        tag.decompose()
    body = (
        soup.find("article") or
        soup.find("div", class_=re.compile(
            r"entry-content|post-content|article-body|content", re.I)) or
        soup.find("main") or soup.find("body")
    )
    if not body:
        return ""
    text = body.get_text(separator="\n")
    text = re.sub(r'\n{3,}', '\n\n', text).strip()
    return text[:4000]


# ─── Main ─────────────────────────────────────────────────────────────────────

def run_backfill(api_key: str):
    print("\n╔══════════════════════════════════════════════════════╗")
    print("║  FREED — Smart Backfill                              ║")
    print("║  Direct relevance first. Orthogonal second.         ║")
    print("╚══════════════════════════════════════════════════════╝\n")

    client    = anthropic.Anthropic(api_key=api_key)
    astrocyte = Astrocyte()
    l7        = L7Agent(api_key=api_key)
    sweep     = TamuraSweep()

    astrocyte.print_status()

    # ── Load score cache ─────────────────────────────────────────────────────
    if SCORE_CACHE.exists():
        with open(SCORE_CACHE) as f:
            score_cache = json.load(f)
        print(f"[BACKFILL] Score cache loaded ({len(score_cache)} entries).\n")
    else:
        score_cache = {}

    # ── Phase 1: Discover archive ────────────────────────────────────────────
    print("[BACKFILL] Phase 1: Crawling Tamura archive (no AI tokens)...")
    all_links = crawl_archive(TAMURA_BASE)

    unseen = [(t, u) for t, u in all_links if u not in sweep.seen]
    print(f"\n[BACKFILL] Archive: {len(all_links)} total, "
          f"{len(unseen)} unseen, {len(all_links)-len(unseen)} already processed.\n")

    if not unseen:
        print("[BACKFILL] Nothing new. Archive fully processed.")
        return

    # ── Phase 2: Score unseen titles ─────────────────────────────────────────
    # Split into cached vs needs scoring
    needs_scoring = [(t, u) for t, u in unseen if u not in score_cache]
    cached_scores = [score_cache[u] for _, u in unseen if u in score_cache]

    new_scores = []
    if needs_scoring:
        print(f"[BACKFILL] Phase 2: Scoring {len(needs_scoring)} new titles...")

        # Budget check for scoring call (estimate)
        scoring_tokens = min(200 + len(needs_scoring) * 15, 3000)
        if not astrocyte.authorize(scoring_tokens, priority="high"):
            print("[BACKFILL] Insufficient budget even for scoring. Aborting.")
            return

        scored = score_titles(client, astrocyte, needs_scoring, score_cache)

        # Cache scores
        for s in scored:
            score_cache[s["url"]] = s
        with open(SCORE_CACHE, "w") as f:
            json.dump(score_cache, f, indent=2)

        new_scores = scored
    else:
        print("[BACKFILL] Phase 2: All titles already scored (cache hit).")

    # Merge cached + new, re-sort
    all_scores = cached_scores + new_scores
    all_scores.sort(key=lambda x: x["combined"], reverse=True)

    # Filter by cutoff
    above_cutoff = [s for s in all_scores if s["combined"] >= SCORE_CUTOFF]
    below_cutoff = [s for s in all_scores if s["combined"] < SCORE_CUTOFF]

    print(f"\n[BACKFILL] Relevance summary (cutoff={SCORE_CUTOFF}):")
    print(f"  Above cutoff: {len(above_cutoff)} papers (will process)")
    print(f"  Below cutoff: {len(below_cutoff)} papers (skipping)")
    print()

    # Show top 10 to process
    print("  Top papers queued:")
    for s in above_cutoff[:10]:
        tag = "DIRECT" if s["direct"] >= 2 else "ORTHO "
        print(f"    [{tag} d={s['direct']} o={s['orthogonal']}] {s['title'][:70]}")
    if len(above_cutoff) > 10:
        print(f"    ... and {len(above_cutoff)-10} more.")
    print()

    # ── Phase 3: Process in priority order ───────────────────────────────────
    print(f"[BACKFILL] Phase 3: Processing up to {MAX_PER_RUN} papers...\n")

    processed = 0
    skipped   = 0

    for s in above_cutoff[:MAX_PER_RUN]:

        if not astrocyte.authorize(3500, priority="high"):
            print(f"\n[BACKFILL] Budget exhausted after {processed} papers. "
                  f"Run again tomorrow — scores are cached, no re-scoring needed.")
            break

        tier = "DIRECT" if s["direct"] >= 2 else "ORTHO"
        print(f"[{processed+1}] [{tier} {s['combined']}pts] {s['title'][:70]}")

        time.sleep(POLITENESS_DELAY)
        content = fetch_article_text(s["url"])

        if not content:
            print("  [skip — no content]\n")
            sweep._mark_seen(s["url"])
            skipped += 1
            continue

        # Tailor the feed prompt based on tier
        if s["direct"] >= 2:
            feed_instruction = (
                "Map this directly against genome invariants and obligations. "
                "Does it confirm, refute, or sharpen any theorem or prediction? "
                "Flag any OBLIGATE-worthy finding."
            )
        else:
            feed_instruction = (
                "This is an orthogonal paper — it may not directly touch the genome "
                "but could inform its architecture. "
                "Identify any formal structure, method, or result that could be "
                "imported into the genome to strengthen or extend it. "
                "What new pathway does this open? Should we OBLIGATE a synthesis?"
            )

        prompt = (
            f"BACKFILL FEED [{tier} relevance — score {s['combined']}]:\n"
            f"Title: {s['title']}\n"
            f"Source: Cecile G. Tamura / Lifeboat Foundation\n"
            f"Genome relevance: direct={s['direct']}, orthogonal={s['orthogonal']}\n"
            f"Scores: direct={s['direct']}, orthogonal={s['orthogonal']}\n\n"
            f"Content:\n{content}\n\n"
            f"{feed_instruction}"
        )

        result = l7.query(prompt)
        astrocyte.record_usage(input_tokens=3500, output_tokens=500)

        print(f"  COMPRESS: {result.get('compress','')[:100]}")
        print(f"  NEXT:     {result.get('next','')[:80]}\n")

        sweep._mark_seen(s["url"])
        processed += 1

    # Mark below-cutoff as seen so they don't reappear
    for s in below_cutoff:
        sweep._mark_seen(s["url"])
    if below_cutoff:
        print(f"[BACKFILL] Marked {len(below_cutoff)} below-cutoff papers as seen.")

    remaining = len([s for s in above_cutoff if s["url"] not in sweep.seen])
    print(f"\n[BACKFILL] Run complete.")
    print(f"  Processed: {processed} | Skipped (no content): {skipped}")
    print(f"  Above-cutoff papers still unprocessed: {remaining}")
    print(f"  (Scores cached — next run resumes without re-scoring)\n")
    astrocyte.print_status()


if __name__ == "__main__":
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        api_key = input("Paste your Anthropic API key: ").strip()
    run_backfill(api_key)
