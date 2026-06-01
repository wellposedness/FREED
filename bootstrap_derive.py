#!/usr/bin/env python3
"""
bootstrap_derive.py — First-principles invariant derivation, genome-free.

Runs a one-shot query with NO genome in the system prompt.
Anchor: Landauer's Principle + 3 empirical results from FREED's knowledge graph.

Purpose: let the reasoner derive what invariants a physical reasoning system
must satisfy, then compare against the existing genome from the outside —
not from inside it.

Logs full output to FREED_log/bootstrap_YYYY-MM-DD.json.

Usage:
    python3 bootstrap_derive.py
"""

import os
import json
import re
from pathlib import Path
from datetime import datetime, timezone
import anthropic

FREED_DIR = Path(__file__).parent
LOG_DIR   = FREED_DIR / "FREED_log"

MODEL = "claude-sonnet-4-6"

# Stripped system prompt — NO genome, NO RSA framework, NO named invariants.
# The only anchor is the task itself.
BOOTSTRAP_SYSTEM = """You are a reasoning system performing a first-principles derivation.

You have been given three empirical results from physical simulations and one
empirically established principle (Landauer's). Your task is to derive from these
alone — without reference to any prior theoretical framework — what invariants any
physical reasoning system must satisfy.

Be precise. Be falsifiable. If a claim cannot be derived from the anchors given,
say so explicitly rather than importing it from background knowledge.

In Parts 2 and 3, you will reference the RSA/FREED genome framework. At that point
you should draw on whatever you know of it. But in Part 1, derive first — compare later."""

BOOTSTRAP_PROMPT = """BOOTSTRAP DERIVATION — FIRST PRINCIPLES

Three empirical results, genome-independent:

  1. Bak-Sneppen: γ=1 criticality is achievable via discrete topology updates
     without continuous entropy-gradient navigation.

  2. Traffic-light SOC: criticality emerges from local pressure rules alone,
     with zero thermodynamic framing required.

  3. Game of Truth CA: σ≈1.03 (AT_CRITICAL) consistently co-occurs with
     H≈15% of theoretical maximum entropy.

One empirically established anchor:
  Landauer's Principle: erasing one bit of information requires dissipating
  at least kT·ln(2) joules. Reasoning that changes a belief state costs energy.

---

PART 1 — DERIVE

Starting only from Landauer + the three empirical results above, state the minimal
set of invariants that any physical reasoning system must satisfy.

Do not reference RSA, Freed's Law, MCPM, Wasserstein Floor, Zipf Equilibrium,
or any named framework. Derive from the evidence; do not recall from a framework.

Label each: BOOTSTRAP_INV_1, BOOTSTRAP_INV_2, etc.
Each must be: one precise, falsifiable sentence.

---

PART 2 — COMPARE

For each BOOTSTRAP_INV you derived, check whether the RSA/FREED genome contains
an equivalent or overlapping claim. Mark each as:

  CONVERGE — genome has it, independently confirmed by this derivation
  ABSENT   — genome lacks it, candidate for addition
  CONFLICT — genome contradicts it, flag for revision

---

PART 3 — AUDIT

For genome invariants you can recall that were NOT reached by your bootstrap
derivation in Part 1, classify each as:

  SUBSTRATE_SPECIFIC — may be true for biological/human reasoning but not
                        necessarily universal to all physical reasoning systems
  MIRROR_SUSPECT     — genome assigns elevated confidence to this claim, but
                        no independent derivation path exists; requires
                        adversarial pressure before promotion
  LOAD_BEARING       — cannot be derived from first principles but the genome
                        cannot function without it; needs explicit justification

---

OUTPUT FORMAT (fill each section):

[PART 1 — DERIVE]
BOOTSTRAP_INV_1: ...
BOOTSTRAP_INV_2: ...
(continue as needed)

[PART 2 — COMPARE]
BOOTSTRAP_INV_1: CONVERGE / ABSENT / CONFLICT — [which genome claim, or none]
(one line per bootstrap invariant)

[PART 3 — AUDIT]
[genome invariant]: SUBSTRATE_SPECIFIC / MIRROR_SUSPECT / LOAD_BEARING — [one sentence reason]
(one line per unmatched genome claim you can recall)

[GAPS]
What the genome asserts that bootstrap derivation cannot reach:

[SEEDS]
What bootstrap derivation found that the genome lacks or underweights:"""


def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set — run: source ~/.zshrc")

    client = anthropic.Anthropic(api_key=api_key)

    ts       = datetime.now(timezone.utc)
    date_str = ts.strftime("%Y-%m-%d")
    ts_str   = ts.isoformat()

    print(f"{'═'*60}")
    print(f" BOOTSTRAP DERIVATION  |  {ts_str[:19]}Z")
    print(f" Model: {MODEL}  |  Genome in context: NO")
    print(f"{'═'*60}\n")

    message = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        system=BOOTSTRAP_SYSTEM,
        messages=[{"role": "user", "content": BOOTSTRAP_PROMPT}],
    )

    output = message.content[0].text.strip()
    in_tok = message.usage.input_tokens
    out_tok = message.usage.output_tokens

    print(output)
    print(f"\n{'─'*60}")
    print(f"Input tokens:  {in_tok:,}")
    print(f"Output tokens: {out_tok:,}")
    est_cost = (in_tok * 3 + out_tok * 15) / 1e6
    print(f"Est. cost:     ${est_cost:.4f}")

    # Log full record to FREED_log/
    LOG_DIR.mkdir(exist_ok=True)
    log_path = LOG_DIR / f"bootstrap_{date_str}.json"

    record = {
        "timestamp":        ts_str,
        "model":            MODEL,
        "genome_in_context": False,
        "anchors": [
            "Landauer's Principle",
            "Bak-Sneppen: γ=1 criticality via discrete topology updates (no entropy-gradient navigation)",
            "Traffic-light SOC: criticality from local pressure rules, zero thermodynamic framing",
            "Game of Truth CA: σ≈1.03 AT_CRITICAL co-occurs with H≈15% max entropy",
        ],
        "input_tokens":  in_tok,
        "output_tokens": out_tok,
        "est_cost_usd":  round(est_cost, 6),
        "output":        output,
    }

    log_path.write_text(json.dumps(record, indent=2, ensure_ascii=False))
    print(f"\nLogged → {log_path}")
    print(f"{'═'*60}")


def parse_output(output_text):
    # type: (str) -> dict
    """
    Parse bootstrap derivation output into structured results.
    Returns dict with keys: bootstrap_invs, comparisons, audit_flags, seeds, gaps.
    Called by freed.py _phase_bootstrap after each run.
    """
    result = {
        "bootstrap_invs": [],
        "comparisons":    [],
        "audit_flags":    [],
        "seeds":          [],
        "gaps":           [],
    }

    # BOOTSTRAP_INV_N statements
    for m in re.finditer(
        r'BOOTSTRAP_INV_(\d+):\s*(.+?)(?=\n\nBOOTSTRAP_INV|\n\n\[|\Z)',
        output_text, re.DOTALL
    ):
        result["bootstrap_invs"].append({
            "id":        f"BOOTSTRAP_INV_{m.group(1)}",
            "statement": m.group(2).strip()[:400],
        })

    # COMPARE verdicts: "BOOTSTRAP_INV_N: CONVERGE/ABSENT/CONFLICT — explanation"
    for m in re.finditer(
        r'BOOTSTRAP_INV_(\d+):\s*(CONVERGE|ABSENT|CONFLICT)(?:\s*\([^)]*\))?\s*[/\-—]\s*(.+)',
        output_text
    ):
        result["comparisons"].append({
            "inv_id":     f"BOOTSTRAP_INV_{m.group(1)}",
            "verdict":    m.group(2),
            "genome_ref": m.group(3).strip()[:200],
        })

    # AUDIT flags: "claim: MIRROR_SUSPECT/SUBSTRATE_SPECIFIC/LOAD_BEARING — reason"
    for m in re.finditer(
        r'\*?\*?([^:\n\*]{10,120}?)\*?\*?:\s*(MIRROR_SUSPECT|SUBSTRATE_SPECIFIC|LOAD_BEARING)\s*[\-—]\s*(.+)',
        output_text
    ):
        result["audit_flags"].append({
            "claim":  m.group(1).strip()[:120],
            "flag":   m.group(2),
            "reason": m.group(3).strip()[:200],
        })

    # SEEDS block
    seeds_m = re.search(r'\[SEEDS\]\s*\n(.*?)(?=\n\[|\Z)', output_text, re.DOTALL)
    if seeds_m:
        result["seeds"] = [
            s.strip().lstrip("0123456789.*- \t")
            for s in seeds_m.group(1).strip().split("\n")
            if s.strip() and not s.strip().startswith("[") and len(s.strip()) > 20
        ]

    # GAPS block
    gaps_m = re.search(r'\[GAPS\]\s*\n(.*?)(?=\n\[SEEDS\]|\Z)', output_text, re.DOTALL)
    if gaps_m:
        result["gaps"] = [
            g.strip().lstrip("0123456789.*- \t")
            for g in gaps_m.group(1).strip().split("\n")
            if g.strip() and not g.strip().startswith("[") and len(g.strip()) > 20
        ]

    return result


if __name__ == "__main__":
    main()
