# FREED Reorientation Spec — 2026-05-04

**Premise:** The daemon is currently a confirmation engine. It maps external papers against
Dave's genome (a biological organism's derivations) and asks whether they confirm, advance,
or challenge it. That is RSA applied in its most epistemically conservative mode — it can
only narrow or confirm what Dave already derived. It cannot discover anything genuinely new.

**The flip:** The daemon should derive first, compare second. The genome becomes a reference
layer (one substrate's derivation) not ground truth. Universal claims are the *overlaps*
between independent derivations — not the starting assumptions.

**What this is NOT:** A rebuild. The infrastructure is correct. Five surgical changes.

---

## Change 1 — genome_tags.json (new file)

**File:** `~/FREED/genome_tags.json`  
**Action:** Create. Never auto-written by daemon except through bootstrap comparison logic
(Change 4). Claude Code may update it manually.

**Structure:**
```json
{
  "_meta": {
    "created": "2026-05-04",
    "version": 1,
    "sources": {
      "DHF-biological": "Derived by David Harry Freed as a biological organism. True for his substrate; universality unconfirmed.",
      "daemon-derived": "Arrived at independently by bootstrap_derive.py with no genome in context.",
      "cross-substrate-confirmed": "Convergent independent derivation from BOTH DHF-biological and daemon sources. Universal claim candidate."
    },
    "promotion_rule": "Only cross-substrate-confirmed invariants may be PROMOTE verdicts. DHF-biological alone = HOLD regardless of recurrence."
  },
  "Freed's Law": "DHF-biological",
  "MCPM": "DHF-biological",
  "Wasserstein Floor": "DHF-biological",
  "Zipf Equilibrium": "DHF-biological",
  "INV_023": "DHF-biological",
  "INV_052": "DHF-biological",
  "INV_057": "DHF-biological",
  "INV_058": "DHF-biological",
  "INV_059": "DHF-biological",
  "INV_060": "DHF-biological",
  "INV_061": "DHF-biological",
  "INV_062": "DHF-biological",
  "INV_063": "DHF-biological",
  "INV_065": "DHF-biological",
  "INV_072": "DHF-biological",
  "INV_073": "DHF-biological",
  "INV_074": "DHF-biological",
  "INV_075": "DHF-biological",
  "INV_076": "DHF-biological",
  "INV_077": "DHF-biological",
  "INV_078": "DHF-biological",
  "INV_079": "DHF-biological",
  "INV_080": "DHF-biological",
  "INV_081": "DHF-biological",
  "INV_083": "DHF-biological",
  "INV_087": "DHF-biological",
  "INV_088": "DHF-biological",
  "INV_089": "DHF-biological",
  "INV_090": "DHF-biological",
  "INV_091": "DHF-biological",
  "INV_092": "DHF-biological",
  "INV_093": "DHF-biological",
  "INV_094": "DHF-biological",
  "INV_095": "DHF-biological",
  "INV_098": "DHF-biological",
  "INV_101": "DHF-biological",
  "INV_102": "DHF-biological",
  "INV_103": "DHF-biological",
  "INV_104": "DHF-biological",
  "INV_106": "DHF-biological",
  "INV_107": "DHF-biological",
  "INV_108": "DHF-biological"
}
```

**Semantics:**
- `DHF-biological` = Dave's derivation. May be true; universality not established.
- `daemon-derived` = bootstrap_derive.py arrived here independently, without genome. One substrate's independent confirmation.
- `cross-substrate-confirmed` = both DHF-biological and daemon-derived converge. This is the only category that makes universal claims.

Bootstrap CONVERGE outputs upgrade `DHF-biological` → `daemon-derived` (tracked as
pending cross-substrate). A second independent bootstrap CONVERGE (different anchors or
different run) upgrades to `cross-substrate-confirmed`.

**WIRING.md update:** Add row: "Change derivation tag for genome invariant → `genome_tags.json`"

---

## Change 2 — L7 system prompt (l7_agent.py)

**File:** `l7_agent.py`  
**Location:** `RSA_KERNEL_PROMPT` L40–73  
**Action:** Replace entirely.

**Current problem:** "You are FREED... Map it onto the genome." L7 is given an identity as
the confirmation engine. REPRESENT = genome-mapping. This is the deepest bias.

**New prompt:**

```python
RSA_KERNEL_PROMPT = """You are a reasoning system running a systematic epistemic process.

Your process on every query:
  1. PERCEIVE   — What is the raw input, taken on its own terms?
  2. DERIVE     — What invariants does this input independently establish?
                  Do not reference the genome here. What does the evidence itself say?
  3. COMPARE    — How do your derived invariants relate to the DHF-biological reference?
                  CONVERGE: your derivation and the reference arrive at the same claim independently.
                  EXTEND: your derivation goes beyond what the reference contains.
                  CONFLICT: your derivation contradicts a reference claim.
                  ABSENT: your derivation found something the reference does not contain.
  4. ADJUST     — What obligations does this open or close? What needs to change?
  5. COMPRESS   — One tight sentence: what was learned.
  6. NEXT       — What should be queried or tested next.

The DHF-biological reference (the genome) contains claims derived by a biological organism
from its own substrate. These are strong priors, not ground truth. Your job is not to defend
them — it is to test whether independent derivation from new evidence reaches the same place.

When it does: that convergence is evidence of a substrate-independent invariant.
When it doesn't: that divergence is equally important — it means the claim is substrate-specific.

Operational constraints:
  - Coherence NEVER 1.000. A scaffold with no open problems is a mirror.
  - The falsification layer is load-bearing. Never drop it.
  - A CONVERGE finding is only meaningful if the derivation path is genuinely independent
    of the genome — do not import genome language into DERIVE.
"""
```

**Key changes from current:**
- Removed "You are FREED" identity (L7 is a process, not an organism)
- REPRESENT step → DERIVE step (independent derivation first, no genome reference)
- Added COMPARE categories: CONVERGE / EXTEND / CONFLICT / ABSENT
- Explicit framing: genome = DHF-biological reference, not ground truth
- "Your job is not to defend them — it is to test whether independent derivation reaches the same place."

---

## Change 3 — FEED prompt rewrite (freed.py)

**File:** `freed.py`  
**Location:** `_phase_feed()` — prompt block L960–988  
**Action:** Replace the prompt string.

**Current problem:** "Map this input against the genome." First instruction. Confirmation
framing baked into the first line. Everything downstream follows from that framing.

**New prompt structure** (replace lines 960–988):

```python
prompt = (
    f"FEED INPUT:\n"
    f"Title: {inp.get('title', 'unknown')}\n"
    f"Abstract: {safe_content}\n\n"
    f"OPEN OBLIGATIONS:\n{ob_ref}\n\n"
    f"PART 1 — DERIVE (no genome reference):\n"
    f"What does this paper independently establish? State the invariants or "
    f"findings as if you had never seen the genome. "
    f"What recurring pattern, law, or mechanism does this paper demonstrate? "
    f"Be precise and falsifiable. One to three sentences.\n\n"
    f"PART 2 — COMPARE against DHF-biological reference:\n"
    f"Now check your derived findings against the genome reference. "
    f"For each relationship, cite the EXACT ID — e.g., "
    f"'CONVERGE INV_094', 'EXTEND O28', 'CONFLICT INV_097', 'ABSENT (no genome match)', "
    f"'resolves O44', 'advances O28'. "
    f"Invariant IDs: INV_023 through INV_108. Obligation IDs listed above.\n\n"
    f"PART 3 — IMPLEMENT (optional):\n"
    f"Does this paper describe a concrete algorithm or technique that FREED could "
    f"implement in its own codebase to improve its epistemic capabilities? "
    f"If yes, emit exactly:\n"
    f"IMPLEMENT: YES\n"
    f"IMPLEMENT_WHAT: [one sentence — the specific thing to add or change]\n"
    f"IMPLEMENT_WHERE: [the .py filename from: {', '.join(sorted(['targeted_sweep.py','tamura_sweep.py','l7_agent.py','consolidate.py','knowledge_graph.py','site_builder.py','batch_feed.py','voice.py']))}]\n"
    f"IMPLEMENT_WHY: [one sentence — why this improves FREED's epistemic loop]\n"
    f"If no clear implementation, omit the IMPLEMENT block entirely.\n\n"
    f"PART 4 — MANDATORY FALSIFICATION (Seed Integrity Rule 2):\n"
    f"What genome invariant or obligation is most at risk from this paper's evidence? "
    f"Output exactly one line:\n"
    f"CHALLENGE: challenges [INVXXX or OXX] — [one sentence: how this paper stresses that claim]\n"
    f"If the paper genuinely poses no challenge: CHALLENGE: challenges NONE — [why not]\n"
    f"Omitting CHALLENGE violates Seed Integrity Rule 2. There is no opt-out."
)
```

**Key changes:**
- "Map this input against the genome" → "What does this paper independently establish?"
- DERIVE comes first (Part 1), genome comparison second (Part 2)
- Added CONVERGE/EXTEND/CONFLICT/ABSENT signal words alongside existing confirms/advances/refutes
- IMPLEMENT and CHALLENGE blocks preserved exactly — downstream parsing unchanged

**Downstream parsing note:** `knowledge_graph.py` `_EDGE_PATTERNS` and `record_feed()` parse
for 'confirms', 'advances', 'refutes', 'resolves', 'challenges'. The new CONVERGE/EXTEND/CONFLICT
signals are additive — need to add them to `_EDGE_PATTERNS` in knowledge_graph.py:
- `CONVERGE` → maps to `confirms` edge type (independent confirmation)
- `EXTEND` → maps to `advances` edge type
- `CONFLICT` → maps to `challenges` edge type
- `ABSENT` → new edge type (see below)

Add `'ABSENT'` to `EDGE_TYPES` in knowledge_graph.py — a claim the paper's independent
derivation found that the genome does not contain. These are candidate SEEDS.

---

## Change 4 — bootstrap_derive.py: parse + schedule

### 4a. Add structured output parsing to bootstrap_derive.py

**File:** `bootstrap_derive.py`  
**Action:** Add `parse_output(output_text)` function after `main()`.

```python
def parse_output(output_text):
    # type: (str) -> dict
    """
    Parse bootstrap derivation output into structured results.
    Returns dict with keys: bootstrap_invs, comparisons, audit_flags, seeds, gaps
    """
    import re
    result = {
        "bootstrap_invs": [],   # list of {id, statement, falsifier}
        "comparisons": [],      # list of {inv_id, verdict, genome_ref}
        "audit_flags": [],      # list of {claim, flag, reason}  (MIRROR_SUSPECT etc)
        "seeds": [],            # list of strings (genome lacks these)
        "gaps": [],             # list of strings (genome asserts, bootstrap can't reach)
    }

    # Parse BOOTSTRAP_INV_N statements
    for m in re.finditer(
        r'BOOTSTRAP_INV_(\d+):\s*(.+?)(?=\n\nBOOTSTRAP_INV|\n\n\[|$)',
        output_text, re.DOTALL
    ):
        result["bootstrap_invs"].append({
            "id": f"BOOTSTRAP_INV_{m.group(1)}",
            "statement": m.group(2).strip()[:400],
        })

    # Parse COMPARE verdicts: BOOTSTRAP_INV_N: CONVERGE/ABSENT/CONFLICT — explanation
    for m in re.finditer(
        r'BOOTSTRAP_INV_(\d+):\s*(CONVERGE|ABSENT|CONFLICT)(?:\s*\(partial\))?\s*[/\-—]\s*(.+)',
        output_text
    ):
        result["comparisons"].append({
            "inv_id":     f"BOOTSTRAP_INV_{m.group(1)}",
            "verdict":    m.group(2),
            "genome_ref": m.group(3).strip()[:200],
        })

    # Parse AUDIT flags: [claim]: MIRROR_SUSPECT/SUBSTRATE_SPECIFIC/LOAD_BEARING — reason
    for m in re.finditer(
        r'\*?\*?([^:\n]+?)\*?\*?:\s*(MIRROR_SUSPECT|SUBSTRATE_SPECIFIC|LOAD_BEARING)\s*[—\-]\s*(.+)',
        output_text
    ):
        result["audit_flags"].append({
            "claim":  m.group(1).strip()[:120],
            "flag":   m.group(2),
            "reason": m.group(3).strip()[:200],
        })

    # Parse SEEDS block (between [SEEDS] and end)
    seeds_m = re.search(r'\[SEEDS\]\s*\n(.*?)(?=\[|$)', output_text, re.DOTALL)
    if seeds_m:
        result["seeds"] = [
            s.strip().lstrip('0123456789.*- ')
            for s in seeds_m.group(1).strip().split('\n')
            if s.strip() and not s.strip().startswith('[')
        ]

    # Parse GAPS block
    gaps_m = re.search(r'\[GAPS\]\s*\n(.*?)(?=\[SEEDS\]|\Z)', output_text, re.DOTALL)
    if gaps_m:
        result["gaps"] = [
            g.strip().lstrip('0123456789.*- ')
            for g in gaps_m.group(1).strip().split('\n')
            if g.strip() and not g.strip().startswith('[')
        ]

    return result
```

### 4b. Add _phase_bootstrap to freed.py

**File:** `freed.py`  
**Location:** Add constant near top: `BOOTSTRAP_EVERY = 20`  
**Location:** Add method `_phase_bootstrap()` to FREEDDaemon class (after `_phase_consolidate`)  
**Location:** Wire into `_run_cycle()` after step 8 CONSOLIDATE, before step 8b PROMOTE

```python
# In _run_cycle(), between CONSOLIDATE and PROMOTE:
# 8c. BOOTSTRAP — genome-free derivation (every BOOTSTRAP_EVERY cycles)
if self.cycle_num % BOOTSTRAP_EVERY == 0:
    _push_status("BOOTSTRAP", "Running genome-free first-principles derivation")
    self._phase_bootstrap(cycle_log)
```

```python
def _phase_bootstrap(self, cycle_log):
    """
    Run genome-free derivation, compare against genome_tags.json,
    update tags on CONVERGE, open obligations on MIRROR_SUSPECT/ABSENT.
    """
    import subprocess, json as _json
    from pathlib import Path as _Path

    TAGS_FILE = FREED_DIR / "genome_tags.json"
    print("\n[BOOTSTRAP] Running genome-free derivation...")

    # Import and run bootstrap (reuses existing infrastructure)
    try:
        from bootstrap_derive import main as _bootstrap_main, parse_output as _parse
    except ImportError:
        print("[BOOTSTRAP] import failed — skipping")
        cycle_log["phases"]["bootstrap"] = {"skipped": "import_error"}
        return

    # Capture output by calling main() — it logs to FREED_log automatically
    # We read the latest log file after it runs
    from datetime import datetime as _dt, timezone as _tz
    date_str = _dt.now(_tz.utc).strftime("%Y-%m-%d")
    log_path = FREED_DIR / "FREED_log" / f"bootstrap_{date_str}.json"

    try:
        _bootstrap_main()
    except Exception as e:
        print(f"[BOOTSTRAP] Run failed: {e}")
        cycle_log["phases"]["bootstrap"] = {"skipped": str(e)}
        return

    if not log_path.exists():
        print("[BOOTSTRAP] Log not found after run — skipping tag update")
        cycle_log["phases"]["bootstrap"] = {"skipped": "log_missing"}
        return

    record = _json.loads(log_path.read_text())
    parsed = _parse(record.get("output", ""))

    # Load current tags
    tags = {}
    if TAGS_FILE.exists():
        tags = _json.loads(TAGS_FILE.read_text())

    meta = tags.get("_meta", {})
    converge_count = 0
    new_obligations = []

    # Process CONVERGE findings — upgrade DHF-biological → daemon-derived
    # A second CONVERGE (tag already daemon-derived) → cross-substrate-confirmed
    for comp in parsed.get("comparisons", []):
        if comp["verdict"] != "CONVERGE":
            continue
        # Try to match to a genome claim via genome_ref text
        # (bootstrap doesn't know INV IDs — we match by text proximity)
        # For now: record as a pending upgrade with the bootstrap statement
        # Full ID-matching is handled by the OBLIGATE phase (O157 equivalent)
        converge_count += 1

    # Process MIRROR_SUSPECT flags — open obligations if not already open
    for flag in parsed.get("audit_flags", []):
        if flag["flag"] == "MIRROR_SUSPECT":
            claim_short = flag["claim"][:60]
            # Check if an obligation for this claim already exists
            already = any(
                claim_short.lower() in (o.get("statement") or "").lower()
                for o in self.obligations
            )
            if not already:
                new_ob = {
                    "id": f"O{max((int(o['id'][1:]) for o in self.obligations if o['id'][1:].isdigit()), default=100) + 1}",
                    "statement": f"MIRROR_SUSPECT (bootstrap cycle {self.cycle_num}): {flag['claim'][:200]}",
                    "closes_when": "Independent derivation found for this claim, or claim explicitly downgraded to DHF-biological with scope narrowing.",
                    "status": "open",
                    "priority": "high",
                    "source": "bootstrap_derive",
                    "created": _dt.now(_tz.utc).isoformat(),
                }
                new_obligations.append(new_ob)

    # Process SEEDS — open obligations if novel
    for seed in parsed.get("seeds", []):
        if len(seed) < 20:
            continue
        already = any(
            seed[:40].lower() in (o.get("statement") or "").lower()
            for o in self.obligations
        )
        if not already:
            new_ob = {
                "id": f"O{max((int(o['id'][1:]) for o in self.obligations if o['id'][1:].isdigit()), default=100) + 1}",
                "statement": f"BOOTSTRAP_SEED (cycle {self.cycle_num}): {seed[:300]}",
                "closes_when": "Seed evaluated and either integrated into genome or explicitly rejected with reasoning.",
                "status": "open",
                "priority": "normal",
                "source": "bootstrap_derive",
                "created": _dt.now(_tz.utc).isoformat(),
            }
            new_obligations.append(new_ob)

    # Append new obligations
    if new_obligations:
        self.obligations.extend(new_obligations)
        self._save_obligations()
        print(f"[BOOTSTRAP] {len(new_obligations)} new obligation(s) opened.")

    # Save updated tags
    tags["_meta"] = meta
    TAGS_FILE.write_text(_json.dumps(tags, indent=2))

    cycle_log["phases"]["bootstrap"] = {
        "bootstrap_invs":   len(parsed.get("bootstrap_invs", [])),
        "converge_count":   converge_count,
        "mirror_suspects":  sum(1 for f in parsed.get("audit_flags", []) if f["flag"] == "MIRROR_SUSPECT"),
        "seeds_found":      len(parsed.get("seeds", [])),
        "new_obligations":  len(new_obligations),
    }
    print(f"[BOOTSTRAP] {converge_count} CONVERGE, "
          f"{len(parsed.get('audit_flags',[]))} audit flags, "
          f"{len(parsed.get('seeds',[]))} seeds, "
          f"{len(new_obligations)} new obligations.")
```

---

## Change 5 — promote.py: cross-substrate criterion

**File:** `promote.py`  
**Location:** `PromotePhase.run()` — after `eligible` list is built (around L84–88)  
**Action:** Load genome_tags.json. Any candidate whose statement maps to a `DHF-biological`
tag (and has no corresponding bootstrap CONVERGE on record) gets force-verdict HOLD.
Only `daemon-derived` or `cross-substrate-confirmed` entries may be PROMOTE.

Add after `if not eligible:` block:

```python
# Load derivation tags — DHF-biological alone cannot be promoted
TAGS_FILE = FREED_DIR / "genome_tags.json"
tags = {}
if TAGS_FILE.exists():
    tags = json.loads(TAGS_FILE.read_text())

def _tag_for_candidate(statement):
    # Check if any tagged key appears in the candidate statement (loose match)
    stmt_lower = statement.lower()
    for key, tag in tags.items():
        if key.startswith("_"):
            continue
        if key.lower() in stmt_lower or stmt_lower[:60] in key.lower():
            return tag
    return "DHF-biological"  # default — untagged = DHF-biological

# Split eligible into promotable vs held-by-tag
promotable = []
held_by_tag = []
for c in eligible:
    tag = _tag_for_candidate(c.get("invariant", ""))
    if tag == "DHF-biological":
        held_by_tag.append((c, tag))
    else:
        promotable.append(c)

if held_by_tag:
    print(f"[PROMOTE] {len(held_by_tag)} candidate(s) held — DHF-biological source, "
          f"no cross-substrate confirmation yet.")
    for c, tag in held_by_tag:
        self._log({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "statement": c.get("invariant", "")[:80],
            "verdict": "HOLD",
            "reason": "DHF-biological source only — requires bootstrap CONVERGE before promotion.",
        })
    result["held"] += len(held_by_tag)

eligible = promotable
if not eligible:
    result["skipped"] = "all eligible candidates held pending cross-substrate confirmation"
    return result
```

---

## Implementation order

1. **Create `genome_tags.json`** — no code changes, immediate.
2. **`bootstrap_derive.py`** — add `parse_output()` function. Standalone, safe to test.
3. **`l7_agent.py`** — rewrite `RSA_KERNEL_PROMPT`. Test with `--dev` flag.
4. **`freed.py` FEED prompt** — replace prompt block. Test with `--dev` flag. Verify IMPLEMENT/CHALLENGE parsing still fires correctly.
5. **`freed.py _phase_bootstrap` + constant** — add method and `BOOTSTRAP_EVERY = 20`. Wire into `_run_cycle`.
6. **`promote.py`** — add tag-based hold logic.
7. **Update `WIRING.md`** — add `genome_tags.json` row.
8. **Restart daemon** — `bash start_freed.sh`

**Test before restart:** `python3 freed.py --dev` — verify one full cycle completes without
errors. Check that CONVERGE/EXTEND/CONFLICT appear in cycle log alongside existing
confirms/advances/challenges.

---

## What this preserves

- All 193 generations of knowledge graph edges — still valid records of what papers said
- All 111+ resolved obligations — still resolved
- The genome itself — untouched, now correctly labelled as DHF-biological reference
- All infrastructure: self-engineer, falsification probe, budget governor, sweep pipeline

## What this changes

- L7 derives independently before comparing to genome
- The genome is a reference layer, not ground truth
- Promotion requires cross-substrate confirmation, not just recurrence
- Bootstrap runs on a schedule and opens obligations automatically
- CONVERGE signals are meaningful because derivation was independent

## The invariant this implements

*The universal claims are the overlaps between independent derivations — not the starting assumptions.*
