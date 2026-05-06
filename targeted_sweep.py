"""
FREED — Targeted Sweep
Active search: generates arXiv + Semantic Scholar queries from open obligations.

This is the daemon's proactive reach. Instead of waiting for Tamura to surface
relevant papers, TargetedSweep reads the open obligations, asks Claude Haiku to
generate search terms, and actively hunts for confirming or refuting evidence.

One targeted sweep per cycle, at most MAX_PER_OBLIGATION results per obligation.
Results are merged with the Tamura sweep in _phase_sweep() before FEED.

Deduplication: tamura_seen.json is shared — won't re-feed URLs already processed.
"""

import json
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import anthropic
import requests

# ─── Paths ────────────────────────────────────────────────────────────────────
FREED_DIR  = Path(__file__).parent
SEEN_FILE  = FREED_DIR / "tamura_seen.json"
LOG_DIR    = FREED_DIR / "FREED_log"

# ─── Config ───────────────────────────────────────────────────────────────────
HAIKU_MODEL       = "claude-haiku-4-5-20251001"
MAX_PER_OBLIGATION = 2      # max papers to return per open obligation
ARXIV_MAX_RESULTS  = 8      # papers to retrieve from arXiv per query
S2_MAX_RESULTS     = 5      # papers to retrieve from Semantic Scholar per query
REQUEST_TIMEOUT    = 20
POLITENESS_DELAY   = 1.5    # seconds between API calls

HEADERS = {
    "User-Agent": (
        "FREED/1.0 (Freed Recursive Engine for Epistemic Dynamics; "
        "targeted search; contact: RSA-Omega framework)"
    )
}

# ─── Relevance keywords (shared with tamura_sweep) ───────────────────────────
# Any paper scoring >= this gets passed to FEED
RELEVANCE_MIN_SCORE = 2

ARXIV_KEYWORDS = [
    ("thermodynamic", 3), ("entropy", 3), ("dissipation", 3), ("landauer", 3),
    ("free energy", 3), ("irreversib", 2), ("heat dissipat", 2),
    ("criticality", 3), ("critical transition", 3), ("phase transition", 2),
    ("self-organized criticality", 3), ("edge of chaos", 2), ("bifurcation", 1),
    ("power.?law", 2), ("scale.?free", 2), ("zipf", 3), ("1/f noise", 2),
    ("information theoret", 2), ("compression", 2), ("minimum description", 3),
    ("kolmogorov", 2), ("mutual information", 2), ("predictive coding", 3),
    ("autopoies", 3), ("self.organiz", 2), ("self.maintain", 2),
    ("recursive", 2), ("self.referent", 2), ("fixed.?point", 2),
    ("substrate", 2), ("physical.?implement", 2), ("neural substrate", 2),
    ("stochastic computation", 2), ("probabilistic computation", 2),
    ("minimal cell", 3), ("minimal genome", 3), ("irreducib", 2),
    ("generating set", 2), ("basis set", 1),
    ("conservation law", 2), ("symmetry break", 2), ("invariant", 1),
    ("noether", 3),
    ("consciousness", 2), ("cognition", 1), ("integrated information", 2),
    ("global workspace", 2),
    ("scale invarian", 3), ("renormalization", 3), ("universality class", 2),
    ("coarse.grain", 2),
    # Obligation-specific extras
    ("wasserstein", 3), ("quantum transport", 2), ("entanglement entropy", 2),
    ("spectral", 2), ("belief revision", 3), ("eeg", 1), ("neural oscillation", 2),
    ("ear", 1), ("intelligence", 1),
]

# ─── σ-decay relevance scorer (O64: long-range stat-mech × optimal transport) ─
# Papers co-occurring in BOTH vocabularies get a large bonus, surfacing the
# cross-pollination zone between power-law / fractional-RG systems and
# Wasserstein / optimal-transport geometry.

# Vocabulary A: long-range / power-law / fractional-dimensional RG
SIGMA_DECAY_LONG_RANGE = [
    (r"long.?range\s+interact", 4),
    (r"power.?law\s+(decay|interact|coupl)", 4),
    (r"sigma.?(decay|exponent|model)", 4),
    (r"fractional.?dimension", 4),
    (r"fractional\s+renormalization", 4),
    (r"anomalous\s+dimension", 3),
    (r"weak\s+long.?range", 5),
    (r"levy\s+flight", 3),
    (r"levy\s+stable", 3),
    (r"long.?range\s+(ising|heisenberg|spin)", 4),
    (r"mean.?field\s+crossover", 3),
    (r"non.?local\s+(field|action|interaction)", 3),
    (r"fractional\s+laplacian", 4),
    (r"riesz\s+(potential|kernel)", 3),
    (r"dyson\s+hierarchical", 3),
    (r"extensiv(e|ity)\s.{0,30}long.?range", 4),
    (r"1/r\^\s*\{?\s*(d\s*\+\s*)?sigma", 4),
    (r"algebraic\s+decay", 3),
    (r"power.?law\s+kernel", 3),
]

# Vocabulary B: Wasserstein / optimal transport
SIGMA_DECAY_OT = [
    (r"wasserstein", 5),
    (r"optimal\s+transport", 5),
    (r"earth\s+mover", 4),
    (r"kantorovich", 4),
    (r"monge.?amp.?re", 4),
    (r"sinkhorn", 4),
    (r"transport\s+(metric|distance|cost|plan|map)", 3),
    (r"displacement\s+convex", 3),
    (r"otto\s+calculus", 4),
    (r"gradient\s+flow.{0,20}(wasserstein|probability|measure)", 4),
    (r"wasserstein\s+(gradient|geometry|space|barycenter)", 5),
    (r"optimal\s+coupling", 3),
    (r"entropy.?regularized\s+transport", 4),
    (r"unbalanced\s+transport", 3),
    (r"gromov.?wasserstein", 5),
    (r"transport\s+inequalit", 3),
]

# Bonus when BOTH vocabularies fire — this is the cross-pollination signal
SIGMA_DECAY_COOCCUR_BONUS = 12
# Minimum hits in each vocabulary to count as co-occurrence
SIGMA_DECAY_MIN_HITS_PER_VOCAB = 1

# ─── INV_073 mimicry-control audit ───────────────────────────────────────────
# INV_073 claims critical-ridge navigation is *necessary* for cognition.
# Three independent mechanisms produce the same observables without requiring
# critical-ridge navigation:
#   (A) Off-ridge output equivalence — systems away from criticality can
#       produce identical input-output maps via parameter compensation.
#   (B) Basin-width ambiguity — wide basins in loss landscapes mimic
#       critical-point signatures (long correlation, 1/f spectra) without
#       actual phase transitions.
#   (C) Criticality mimicry via multiplicative noise / heterogeneous Poisson —
#       heavy-tailed, scale-free statistics arise from multiplicative
#       stochastic processes or superpositions of Poisson processes, not
#       from criticality per se.
#
# Any evidence offered as INV_073 confirmation must demonstrably control for
# at least one of these three; otherwise it is flagged as MIMICRY_UNCONTROLLED.

INV_073_CONFIRM_PATTERNS = [
    (r"critical.?ridge", 4),
    (r"criticality\s.{0,30}(necessary|essential|required)", 5),
    (r"edge\s+of\s+(chaos|criticality)", 3),
    (r"critical\s+(brain|neural|cortical)", 4),
    (r"neuronal?\s+avalanche", 3),
    (r"power.?law.{0,30}(neural|brain|cortex)", 3),
    (r"self.?organized\s+criticality.{0,30}(brain|neural|cognit)", 4),
    (r"critical\s+transition.{0,30}cognit", 4),
    (r"INV.?073", 6),
]
INV_073_CONFIRM_THRESHOLD = 5  # cumulative score to count as INV_073-confirming

# Alternative-explanation signatures — presence means the paper *controls*
# for (or *demonstrates*) a mimicry mechanism, which either (a) strengthens
# the confirmation if the paper rules the alternative out, or (b) weakens it
# if the paper shows the alternative suffices.
INV_073_MIMICRY_PATTERNS = {
    "multiplicative_noise": [
        (r"multiplicative\s+(noise|process)", 5),
        (r"log.?normal.{0,30}(neural|avalanche|power.?law)", 4),
        (r"multiplicative\s+stochastic", 4),
        (r"geometric\s+brownian", 3),
        (r"gibrat", 4),
        (r"yule.?simon", 3),
    ],
    "heterogeneous_poisson": [
        (r"heterogeneous\s+poisson", 5),
        (r"superposition.{0,30}poisson", 4),
        (r"mixture\s+of\s+poisson", 4),
        (r"apparent\s+(power.?law|scaling).{0,30}(mixture|heterogen)", 5),
        (r"rate\s+heterogeneity.{0,30}(power|scal)", 4),
        (r"non.?critical.{0,30}(power.?law|scal)", 4),
    ],
    "off_ridge_equivalence": [
        (r"off.?ridge", 5),
        (r"(input.?output|functional)\s+equivalen", 4),
        (r"parameter\s+compensat", 4),
        (r"(away|far)\s+from\s+critical", 4),
        (r"non.?critical\s+(regime|phase).{0,30}(same|identical|equivalent)", 5),
        (r"degeneracy.{0,30}critical", 3),
    ],
    "basin_width_ambiguity": [
        (r"basin.?width", 5),
        (r"flat\s+(direction|minimum|basin)", 4),
        (r"loss\s+landscape.{0,30}(wide|flat|broad)", 4),
        (r"(wide|broad)\s+basin.{0,30}(mimic|resemble|indistinguish)", 5),
        (r"1/f.{0,20}(non.?critical|without\s+critical)", 5),
        (r"long.?range\s+correlation.{0,30}(non.?critical|artifact)", 4),
    ],
}
INV_073_MIMICRY_DETECT_THRESHOLD = 3  # per-mechanism score to count as detected


def score_sigma_decay_relevance(text):
    """
    Score a paper for σ-decay cross-pollination relevance (O64).

    Returns a dict with:
        long_range_score: int — total weight from long-range vocabulary hits
        ot_score:         int — total weight from optimal-transport vocabulary hits
        bonus:            int — co-occurrence bonus (0 or SIGMA_DECAY_COOCCUR_BONUS)
        total:            int — sum of all three
        is_crossover:     bool — True if both vocabularies fired
    """
    text_lower = text.lower()

    lr_score = 0
    lr_hits = 0
    for pattern, weight in SIGMA_DECAY_LONG_RANGE:
        if re.search(pattern, text_lower):
            lr_score += weight
            lr_hits += 1

    ot_score = 0
    ot_hits = 0
    for pattern, weight in SIGMA_DECAY_OT:
        if re.search(pattern, text_lower):
            ot_score += weight
            ot_hits += 1

    is_crossover = (lr_hits >= SIGMA_DECAY_MIN_HITS_PER_VOCAB and
                    ot_hits >= SIGMA_DECAY_MIN_HITS_PER_VOCAB)

    bonus = SIGMA_DECAY_COOCCUR_BONUS if is_crossover else 0

    return {
        "long_range_score": lr_score,
        "ot_score":         ot_score,
        "bonus":            bonus,
        "total":            lr_score + ot_score + bonus,
        "is_crossover":     is_crossover,
    }


# ═══════════════════════════════════════════════════════════════════════════════
class TargetedSweep:
    """
    Active search: mines open obligations → generates queries → fetches papers.
    Returns structured input dicts compatible with the FEED pipeline.
    """

    def __init__(self, api_key, max_per_obligation=MAX_PER_OBLIGATION):
        # type: (str, int) -> None
        self.api_key  = api_key
        self.max_per  = max_per_obligation
        self._client  = anthropic.Anthropic(api_key=api_key)
        self._load_seen()

    # ── Seen-URL tracking ────────────────────────────────────────────────────

    def _load_seen(self):
        if SEEN_FILE.exists():
            with open(SEEN_FILE) as f:
                self.seen = set(json.load(f))
        else:
            self.seen = set()

    def _mark_seen(self, url):
        # type: (str) -> None
        self.seen.add(url)
        with open(SEEN_FILE, "w") as f:
            json.dump(sorted(self.seen), f, indent=2)

    # ── Main entry point ─────────────────────────────────────────────────────

    def sweep(self, obligations):
        # type: (list) -> list
        """
        Run targeted search against open/partial obligations.

        obligations: list of dicts with keys: id, status, statement, priority
        Returns: list of input dicts ready for FEED
        """
        self._load_seen()   # refresh before each sweep

        # Only hunt for open/partial obligations — resolved ones are done
        targets = [o for o in obligations
                   if o.get("status") in ("open", "partial")]

        if not targets:
            print("[TARGET] No open obligations to search against.")
            return []

        # Prioritize high-priority first
        targets.sort(key=lambda o: (
            0 if o.get("priority") == "high" else 1,
            o.get("id", "")
        ))

        print(f"[TARGET] {len(targets)} open obligation(s) — generating search queries...")

        all_inputs = []
        for ob in targets:
            if len(all_inputs) >= self.max_per * 3:
                break   # enough results for this cycle

            obid = ob.get("id", "?")
            stmt = ob.get("statement", "")
            prog = ob.get("progress", "")
            if not stmt:
                continue

            print(f"[TARGET] → {obid}: {stmt[:70]}...")

            # Generate search queries from this obligation via Haiku
            queries = self._generate_queries(ob)
            if not queries:
                print(f"[TARGET]   Query generation failed — skipping {obid}.")
                continue

            print(f"[TARGET]   Queries: {queries}")

            # Search both arXiv and Semantic Scholar for each query
            ob_papers = []
            for q in queries:
                if len(ob_papers) >= self.max_per:
                    break

                # arXiv search
                papers = self._search_arxiv(q)
                for p in papers:
                    if p["url"] not in self.seen and len(ob_papers) < self.max_per:
                        p["obligation"]  = obid
                        p["query_used"]  = q
                        ob_papers.append(p)

                if len(ob_papers) < self.max_per:
                    time.sleep(POLITENESS_DELAY)
                    # Semantic Scholar search
                    papers = self._search_semantic_scholar(q)
                    for p in papers:
                        if p["url"] not in self.seen and len(ob_papers) < self.max_per:
                            p["obligation"] = obid
                            p["query_used"] = q
                            ob_papers.append(p)

                time.sleep(POLITENESS_DELAY)

            for p in ob_papers:
                self._mark_seen(p["url"])

            if ob_papers:
                print(f"[TARGET]   Found {len(ob_papers)} paper(s) for {obid}.")
            else:
                print(f"[TARGET]   No new papers for {obid}.")

            all_inputs.extend(ob_papers)

        self._log(all_inputs)
        print(f"[TARGET] Total targeted inputs: {len(all_inputs)}")
        return all_inputs

    # ── Query generation (Claude Haiku) ──────────────────────────────────────

    def _generate_queries(self, obligation):
        # type: (dict) -> list
        """
        Ask Claude Haiku to generate 3 arXiv search queries for this obligation.
        Returns list of query strings. Cheap — Haiku costs ~20x less than Opus.
        """
        stmt = obligation.get("statement", "")
        prog = obligation.get("progress", "")
        obid = obligation.get("id", "")

        context = f"Obligation {obid}: {stmt}"
        if prog:
            context += f"\n\nProgress so far: {prog[:300]}"

        prompt = f"""You are generating arXiv paper search queries for a philosophy-of-science research system.

The system is built on the RSA framework (Freed's Law: to reason is to burn; autopoiesis; entropy as resource; γ=1 criticality).

Given this open obligation, generate exactly 3 short arXiv search query strings that would find papers confirming, refuting, or advancing it. Each query should be 3–6 words, use technical terms that appear in paper titles/abstracts, and target a different angle.

{context}

Reply with ONLY 3 lines — one query per line, no numbering, no explanation."""

        try:
            resp = self._client.messages.create(
                model=HAIKU_MODEL,
                max_tokens=150,
                messages=[{"role": "user", "content": prompt}]
            )
            raw = resp.content[0].text.strip()
            queries = [line.strip() for line in raw.splitlines() if line.strip()]
            # Safety: take at most 3, filter out any clearly empty lines
            return [q for q in queries[:3] if len(q) > 4]
        except Exception as e:
            print(f"[TARGET]   Haiku query-gen error: {e}")
            return []

    # ── arXiv search ─────────────────────────────────────────────────────────

    def _search_arxiv(self, query):
        # type: (str) -> list
        """
        Search arXiv using the Atom API.
        Returns list of paper dicts scored by RSA relevance.
        """
        import urllib.parse
        q_encoded = urllib.parse.quote(query)
        url = (
            f"https://export.arxiv.org/api/query"
            f"?search_query=all:{q_encoded}"
            f"&start=0&max_results={ARXIV_MAX_RESULTS}"
            f"&sortBy=relevance&sortOrder=descending"
        )

        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                return []
        except Exception as e:
            print(f"[TARGET]   arXiv fetch error: {e}")
            return []

        try:
            root = ET.fromstring(resp.text)
        except ET.ParseError:
            return []

        ns = {'atom': 'http://www.w3.org/2005/Atom'}
        papers = []

        for entry in root.findall('atom:entry', ns):
            title    = (entry.findtext('atom:title', '', ns) or '').strip().replace('\n', ' ')
            abstract = (entry.findtext('atom:summary', '', ns) or '').strip().replace('\n', ' ')
            link_el  = entry.find('atom:id', ns)
            paper_id = (link_el.text or '').strip() if link_el is not None else ''

            # Convert API URL to abs URL
            arxiv_url = paper_id.replace('http://arxiv.org/abs/', 'https://arxiv.org/abs/')
            if not arxiv_url or 'arxiv.org' not in arxiv_url:
                continue

            authors = [a.findtext('atom:name', '', ns)
                       for a in entry.findall('atom:author', ns)]

            combined_text = title + ' ' + abstract
            score = self._score_relevance(combined_text)

            # Apply σ-decay cross-pollination bonus
            sigma_result = score_sigma_decay_relevance(combined_text)
            score += sigma_result["total"]

            if score < RELEVANCE_MIN_SCORE:
                continue

            paper_dict = {
                "title":    title,
                "url":      arxiv_url,
                "abstract": abstract[:800],
                "content":  abstract[:800],
                "authors":  ', '.join(authors[:3]),
                "source":   "targeted_sweep/arxiv",
                "score":    score,
                "fetched":  datetime.now(timezone.utc).isoformat(),
            }

            # Tag σ-decay crossover papers for downstream visibility
            if sigma_result["is_crossover"]:
                paper_dict["sigma_decay_crossover"] = True
                paper_dict["sigma_decay_detail"] = sigma_result

            papers.append(paper_dict)

        # Sort by relevance
        papers.sort(key=lambda x: x["score"], reverse=True)
        return papers

    # ── Semantic Scholar search ───────────────────────────────────────────────

    def _search_semantic_scholar(self, query):
        # type: (str) -> list
        """
        Search Semantic Scholar (free, no key required).
        Returns list of paper dicts scored by RSA relevance.
        """
        import urllib.parse
        q_encoded = urllib.parse.quote(query)
        url = (
            f"https://api.semanticscholar.org/graph/v1/paper/search"
            f"?query={q_encoded}"
            f"&limit={S2_MAX_RESULTS}"
            f"&fields=title,abstract,year,authors,externalIds,openAccessPdf"
        )

        try:
            resp = requests.get(
                url,
                headers={**HEADERS, "Accept": "application/json"},
                timeout=REQUEST_TIMEOUT
            )
            if resp.status_code != 200:
                return []
            data = resp.json()
        except Exception as e:
            print(f"[TARGET]   S2 fetch error: {e}")
            return []

        papers = []
        for item in data.get("data", []):
            title    = (item.get("title") or "").strip()
            abstract = (item.get("abstract") or "").strip()
            if not title:
                continue

            # Prefer arXiv URL from externalIds, then open access PDF, then S2 URL
            ext_ids = item.get("externalIds") or {}
            arxiv_id = ext_ids.get("ArXiv")
            if arxiv_id:
                paper_url = f"https://arxiv.org/abs/{arxiv_id}"
            else:
                pdf_info = item.get("openAccessPdf")
                if pdf_info and pdf_info.get("url"):
                    paper_url = pdf_info["url"]
                else:
                    s2_id = item.get("paperId", "")
                    if not s2_id:
                        continue
                    paper_url = f"https://www.semanticscholar.org/paper/{s2_id}"

            combined_text = title + ' ' + abstract
            score = self._score_relevance(combined_text)

            # Apply σ-decay cross-pollination bonus
            sigma_result = score_sigma_decay_relevance(combined_text)
            score += sigma_result["total"]

            if score < RELEVANCE_MIN_SCORE:
                continue

            authors_list = item.get("authors") or []
            author_str   = ', '.join(a.get("name", "") for a in authors_list[:3])

            paper_dict = {
                "title":    title,
                "url":      paper_url,
                "abstract": abstract[:800],
                "content":  abstract[:800],
                "authors":  author_str,
                "source":   "targeted_sweep/semantic_scholar",
                "score":    score,
                "fetched":  datetime.now(timezone.utc).isoformat(),
            }

            # Tag σ-decay crossover papers for downstream visibility
            if sigma_result["is_crossover"]:
                paper_dict["sigma_decay_crossover"] = True
                paper_dict["sigma_decay_detail"] = sigma_result

            papers.append(paper_dict)

        papers.sort(key=lambda x: x["score"], reverse=True)
        return papers

    # ── Relevance scoring ────────────────────────────────────────────────────

    def _score_relevance(self, text):
        # type: (str) -> int
        """Score text against RSA-adjacent keywords. Pure regex — no API cost."""
        text_lower = text.lower()
        score = 0
        for pattern, weight in ARXIV_KEYWORDS:
            if re.search(pattern, text_lower):
                score += weight
        return score

    # ── Logging ─────────────────────────────────────────────────────────────

    def _log(self, inputs):
        # type: (list) -> None
        """Append targeted sweep results to a daily log file."""
        if not inputs:
            return
        LOG_DIR.mkdir(exist_ok=True)
        date_str  = datetime.utcnow().strftime('%Y-%m-%d')
        log_file  = LOG_DIR / f"targeted_sweep_{date_str}.jsonl"
        timestamp = datetime.now(timezone.utc).isoformat()
        with open(log_file, 'a', encoding='utf-8') as f:
            for inp in inputs:
                entry = {
                    "timestamp": timestamp,
                    "url":       inp.get("url"),
                    "title":     inp.get("title"),
                    "obligation": inp.get("obligation"),
                    "query":     inp.get("query_used"),
                    "score":     inp.get("score"),
                    "source":    inp.get("source"),
                }
                # Log σ-decay crossover flag when present
                if inp.get("sigma_decay_crossover"):
                    entry["sigma_decay_crossover"] = True
                    entry["sigma_decay_detail"] = inp.get("sigma_decay_detail")
                f.write(json.dumps(entry) + '\n')


# ─── Quick test ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import os
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        api_key = input("Anthropic API key: ").strip()

    # ── Test σ-decay scorer directly ─────────────────────────────────────────
    print("── σ-decay relevance scorer tests ──\n")

    # Pure long-range paper (no OT) — should get LR score but no bonus
    test_lr = (
        "Critical properties of the long-range Ising model with power-law "
        "decay sigma exponent: renormalization group and universality class"
    )
    r1 = score_sigma_decay_relevance(test_lr)
    print(f"Long-range only:  {r1}")

    # Pure OT paper (no LR) — should get OT score but no bonus
    test_ot = (
        "Wasserstein gradient flows and Sinkhorn divergences for "
        "entropy-regularized optimal transport on probability measures"
    )
    r2 = score_sigma_decay_relevance(test_ot)
    print(f"OT only:          {r2}")

    # Crossover paper — should trigger bonus
    test_cross = (
        "Wasserstein distance and optimal transport on spin systems with "
        "long-range interactions: power-law decay and fractional Laplacian "
        "renormalization group in the weak long-range regime"
    )
    r3 = score_sigma_decay_relevance(test_cross)
    print(f"Crossover (O64):  {r3}")
    assert r3["is_crossover"], "Crossover paper should be flagged!"
    assert r3["bonus"] == SIGMA_DECAY_COOCCUR_BONUS, "Bonus should fire!"

    # Irrelevant paper — should get 0
    test_nil = "Finite element methods for computational fluid dynamics"
    r4 = score_sigma_decay_relevance(test_nil)
    print(f"Irrelevant:       {r4}")
    assert r4["total"] == 0, "Irrelevant paper should score 0."

    print("\n✓ σ-decay scorer tests passed.\n")

    # ── Test full sweep ──────────────────────────────────────────────────────
    # Minimal test obligations
    test_obligs = [
        {
            "id": "O28",
            "status": "partial",
            "priority": "high",
            "statement": (
                "Entropy asymmetry ratio (EAR) predicts intelligence. "
                "Open-access EEG: osf.io/htrsg. EAR as composite predictor not yet tested."
            ),
            "progress": "Thiele et al. 2025 confirmed INV_094 prediction.",
        },
        {
            "id": "O64",
            "status": "open",
            "priority": "high",
            "statement": (
                "Cross-pollinate long-range statistical mechanics (sigma-decay, "
                "fractional-dimensional RG, weak long-range universality) with "
                "optimal transport / Wasserstein geometry to find shared structure."
            ),
            "progress": "",
        },
    ]

    ts = TargetedSweep(api_key=api_key, max_per_obligation=2)
    results = ts.sweep(test_obligs)

    print(f"\n── {len(results)} result(s) ──")
    for r in results:
        crossover_tag = " ★σ-CROSSOVER★" if r.get("sigma_decay_crossover") else ""
        print(f"\n[{r.get('obligation')}] {r['title'][:70]}{crossover_tag}")
        print(f"  URL:   {r['url']}")
        print(f"  Score: {r['score']}")
        print(f"  Query: {r.get('query_used')}")
        if r.get("sigma_decay_detail"):
            sd = r["sigma_decay_detail"]
            print(f"  σ-decay: LR={sd['long_range_score']} OT={sd['ot_score']} bonus={sd['bonus']}")