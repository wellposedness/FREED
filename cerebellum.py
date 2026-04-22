"""
Cerebellum — fast pre-scorer for FREED passive sweep candidates.

Sits between _phase_sweep() and _phase_feed() in the daemon cycle.
Targeted-sweep results bypass entirely (already obligation-driven).
Passive candidates go through two tiers:

  Tier 0  Lexical scoring — free, instant
           Symbol hits, obligation keyword overlap, RSA term density
  Tier 1  Haiku semantic — only fires when Tier 0 lands in ambiguous band

Candidates below CEREBELLUM_THRESHOLD are dropped before reaching L7.
Survivors receive 'cerebellum_score' and 'methodology_type' annotations.
Decisions logged to FREED_log/cerebellum_{date}.jsonl.
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path

CEREBELLUM_THRESHOLD = 3.0   # drop passive candidates scoring below this
HAIKU_BAND_LOW       = 2.0   # Haiku fires only when lexical is in [LOW, HIGH]
HAIKU_BAND_HIGH      = 6.0

HAIKU_MODEL = "claude-haiku-4-5-20251001"

FREED_DIR = Path(__file__).parent
LOG_DIR   = FREED_DIR / "FREED_log"

# ─── Methodology classification keywords ─────────────────────────────────────

_EXPERIMENTAL_KW = {
    "fmri", "eeg", "ecg", "emg", "in vitro", "in vivo", "trial",
    "measurement", "empirical", "laboratory", "animal model", "human subject",
    "neuroimaging", "electrophysiology", "patch clamp", "ablation", "biopsy",
}
_COMPUTATIONAL_KW = {
    "simulation", "neural network", "deep learning", "algorithm", "benchmark",
    "training", "dataset", "code", "software", "loss function", "gradient",
    "architecture", "transformer", "language model", "gpt", "llm",
}
_THEORETICAL_KW = {
    "theorem", "proof", "conjecture", "lemma", "corollary", "formal",
    "axiom", "derivation", "analytic", "mathematical", "topology",
    "information theory", "statistical mechanics", "renormalization",
}
_PHYSICAL_KW = {
    "protein", "dna", "rna", "membrane", "cell biology", "neuron",
    "biophysics", "molecular", "biochemistry", "evolution", "organism",
}


def _classify_methodology(title, abstract, url):
    text   = (title + " " + abstract).lower()
    domain = url.lower()

    scores = {
        "experimental":  sum(1 for kw in _EXPERIMENTAL_KW  if kw in text),
        "computational": sum(1 for kw in _COMPUTATIONAL_KW if kw in text),
        "theoretical":   sum(1 for kw in _THEORETICAL_KW   if kw in text),
        "physical":      sum(1 for kw in _PHYSICAL_KW       if kw in text),
    }

    # Domain hints
    if any(d in domain for d in ("biorxiv", "nature.com", "cell.com", "sciencemag")):
        scores["experimental"] += 1
        scores["physical"]     += 1
    if any(d in domain for d in ("arxiv.org/abs/cs.", "arxiv.org/abs/stat.",
                                  "proceedings.mlr", "openreview.net")):
        scores["computational"] += 2
    if "arxiv.org/abs/math" in domain or "arxiv.org/abs/q-bio" in domain:
        scores["theoretical"] += 1

    best = max(scores, key=lambda k: scores[k])
    return best if scores[best] > 0 else "unknown"


# ─── RSA-specific signal terms ────────────────────────────────────────────────

_RSA_TERMS = [
    "freed", "freed's law", "rsa kernel", "wasserstein", "autopoietic",
    "autopoiesis", "zipf", "1/f noise", "pink noise", "mdl",
    "minimum description length", "semantic tension", "criticality",
    "compression as reasoning", "active inference", "minimal generators",
    "symbol grounding", "freed's law",
]


class Cerebellum:
    """Fast pre-scorer. Instantiated once per daemon, reused across cycles."""

    def __init__(self, api_key):
        self._api_key = api_key
        self._symbols = self._load_symbol_terms()

    def _load_symbol_terms(self):
        sym_file = FREED_DIR / "genome_symbols.json"
        if not sym_file.exists():
            return []
        data = json.loads(sym_file.read_text(encoding="utf-8"))
        terms = []
        for key in data:
            if key == "_meta":
                continue
            terms.append(key.replace("_", " "))  # "symbol_grounding" → "symbol grounding"
            terms.append(key)                     # also match raw underscored form
        return terms

    def _lexical_score(self, title, abstract, obligations):
        text  = (title + " " + abstract).lower()
        score = 0.0

        # Genome symbol hits (each worth 0.5, capped at 3.0 total)
        sym_hits = sum(1 for sym in self._symbols if sym in text)
        score   += min(sym_hits * 0.5, 3.0)

        # RSA-specific term hits (each worth 0.5)
        for term in _RSA_TERMS:
            if term in text:
                score += 0.5

        # Direct INV_/O ID mentions in the paper itself
        inv_mentions = len(re.findall(r'\bINV_\d+\b', title + " " + abstract))
        ob_mentions  = len(re.findall(r'\bO\d{2,}\b',  title + " " + abstract))
        score += inv_mentions * 0.5 + ob_mentions * 0.3

        # Obligation keyword overlap (skip targeted — they're already matched)
        open_obs = [o for o in obligations if o.get("status") in ("open", "partial")]
        for ob in open_obs[:12]:
            stmt  = ob.get("statement", "").lower()
            words = [w for w in re.split(r'\W+', stmt) if len(w) > 4]
            hits  = sum(1 for w in words if w in text)
            if hits >= 3:
                score += 1.5
            elif hits == 2:
                score += 0.8
            elif hits == 1:
                score += 0.2

        return round(min(score, 10.0), 2)

    def _haiku_score(self, title, abstract, obligations):
        """Semantic scoring via Haiku. Returns float 0–10. Neutral (5.0) on any error."""
        try:
            import anthropic
            client   = anthropic.Anthropic(api_key=self._api_key)
            open_obs = [o for o in obligations if o.get("status") in ("open", "partial")][:6]
            ob_text  = "\n".join(
                f"  {o['id']}: {o.get('statement', '')[:70]}"
                for o in open_obs
            ) or "  (none open)"

            prompt = (
                f"Rate 0-10 (one integer only, no explanation) how likely this paper "
                f"would advance, confirm, or resolve any of these obligations:\n"
                f"{ob_text}\n\n"
                f"Paper: {title}. {abstract[:350]}"
            )
            resp = client.messages.create(
                model=HAIKU_MODEL,
                max_tokens=10,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = resp.content[0].text.strip()
            m   = re.search(r'\d+', raw)
            return float(m.group()) if m else 5.0
        except Exception:
            return 5.0  # neutral on failure — don't drop candidates on Haiku error

    def score_candidates(self, targeted, passive, obligations, cycle_tag):
        """
        Score passive candidates. Targeted inputs bypass and pass through unchanged.

        Returns:
          merged    — targeted (unchanged) + passing passive, in that order
          dropped   — count of passive candidates dropped
          stats     — dict with scoring summary for cycle log
        """
        LOG_DIR.mkdir(exist_ok=True)
        date_str  = datetime.now().strftime("%Y-%m-%d")
        log_path  = LOG_DIR / f"cerebellum_{date_str}.jsonl"

        survivors = []
        dropped   = 0
        tier1_fired = 0
        log_entries = []

        for item in passive:
            title    = item.get("title",    "")
            abstract = item.get("abstract", item.get("content", ""))[:800]
            url      = item.get("url",      "")

            lex   = self._lexical_score(title, abstract, obligations)
            tier  = 0
            final = lex

            if HAIKU_BAND_LOW <= lex <= HAIKU_BAND_HIGH:
                h     = self._haiku_score(title, abstract, obligations)
                final = round((lex + h) / 2.0, 2)
                tier  = 1
                tier1_fired += 1

            method = _classify_methodology(title, abstract, url)
            passed = final >= CEREBELLUM_THRESHOLD

            log_entries.append({
                "ts":          datetime.now(timezone.utc).isoformat(),
                "cycle":       cycle_tag,
                "title":       title[:80],
                "url":         url,
                "lex_score":   lex,
                "final_score": final,
                "tier":        tier,
                "method_type": method,
                "decision":    "pass" if passed else "drop",
            })

            if passed:
                item = dict(item)
                item["cerebellum_score"] = final
                item["methodology_type"] = method
                survivors.append(item)
            else:
                dropped += 1
                print(f"[CEREBELLUM] DROP  score={final:.1f}  {title[:60]}")

        # Append log entries
        with open(log_path, "a", encoding="utf-8") as f:
            for e in log_entries:
                f.write(json.dumps(e) + "\n")

        stats = {
            "targeted_bypassed": len(targeted),
            "passive_scored":    len(passive),
            "passed":            len(survivors),
            "dropped":           dropped,
            "tier1_calls":       tier1_fired,
            "threshold":         CEREBELLUM_THRESHOLD,
        }

        print(f"[CEREBELLUM] {len(targeted)} targeted bypassed | "
              f"{len(passive)} passive scored | "
              f"{len(survivors)} pass, {dropped} drop | "
              f"{tier1_fired} Haiku call(s)")

        # Targeted first (purposeful), then surviving passive (scored)
        return targeted + survivors, dropped, stats
