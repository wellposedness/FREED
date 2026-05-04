"""
FREED PROMOTE phase — autonomous genome promotion.

Reads high-recurrence invariant candidates from FREED_state.json,
applies a three-question filter via Opus, and appends approved
invariants to FREED_genome.md.

Runs after CONSOLIDATE in cycles where high-recurrence candidates exist.
MODIFIABLE — self-engineer may refine promotion criteria and the filter prompt.
Never call GENOME_FILE.write() outside this module; all genome writes go
through _apply_verdicts() so the log stays consistent.
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import anthropic

FREED_DIR     = Path(__file__).parent
STATE_FILE    = FREED_DIR / "FREED_state.json"
GENOME_FILE   = FREED_DIR / "FREED_genome.md"
PROMOTE_LOG   = FREED_DIR / "FREED_log" / "promote_decisions.jsonl"

PROMOTE_THRESHOLD = 10   # minimum recurrence before Opus reviews
PROMOTE_MIN_NODES = 5    # minimum distinct nodes candidate must span

_FILTER_PROMPT = """\
You are the FREED genome promotion filter.

The FREED genome (FREED_genome.md) contains substrate-independent process \
claims confirmed across multiple independent epistemic domains. Your task: \
evaluate each candidate against the THREE-QUESTION FILTER and return a verdict.

THREE-QUESTION FILTER
Q1 — SUBSTRATE-INDEPENDENCE: Is this a claim about a process pattern that \
holds across substrates (cognitive, thermodynamic, biological, symbolic), \
rather than a domain-specific content claim?
Q2 — INDEPENDENT CONFIRMATION: Has this appeared across structurally distinct \
knowledge nodes — not merely multiple papers on the same narrow topic?
Q3 — FALSIFIABILITY: Can this be stated as a prediction that could, in \
principle, fail? A statement that merely describes everything fails this test.

EXISTING GENOME CONCEPTS (first 4000 chars — do not re-promote these):
{genome_excerpt}

CANDIDATES (sorted descending by recurrence):
{candidates_block}

For EACH candidate output EXACTLY this block (no extra text between blocks):
CANDIDATE: <exact statement>
VERDICT: PROMOTE | HOLD | REJECT
REASON: <one sentence>
"""


class PromotePhase:

    def __init__(self, api_key):
        # type: (str) -> None
        self._client = anthropic.Anthropic(api_key=api_key)

    # ─────────────────────────────────────────────────────────────────────────

    def run(self, cycle_log):
        # type: (dict) -> dict
        """
        Run the PROMOTE phase.
        Returns a result dict for cycle_log["phases"]["promote"].
        """
        result = {
            "promoted": 0, "held": 0, "rejected": 0,
            "eligible_count": 0, "skipped": None,
        }

        if not STATE_FILE.exists():
            result["skipped"] = "state file missing"
            return result

        state = json.loads(STATE_FILE.read_text())
        candidates = state.get("promotion_candidates", [])

        eligible = [
            c for c in candidates
            if c.get("recurrence", 0) >= PROMOTE_THRESHOLD
            and len(c.get("appears_in", [])) >= PROMOTE_MIN_NODES
        ]

        if not eligible:
            result["skipped"] = (
                f"no candidates above {PROMOTE_THRESHOLD}x/{PROMOTE_MIN_NODES}-node threshold"
            )
            return result

        genome_text = GENOME_FILE.read_text(encoding="utf-8") if GENOME_FILE.exists() else ""

        new_eligible = [
            c for c in eligible
            if not self._already_present(c.get("invariant", ""), genome_text)
        ]

        if not new_eligible:
            result["skipped"] = "all eligible candidates already present in genome"
            return result

        # Cross-substrate gate: DHF-biological-only candidates are held pending
        # independent bootstrap confirmation. Only daemon-derived or
        # cross-substrate-confirmed candidates proceed to Opus review.
        TAGS_FILE = FREED_DIR / "genome_tags.json"
        tags = {}
        if TAGS_FILE.exists():
            tags = json.loads(TAGS_FILE.read_text())

        def _source_tag(statement):
            stmt_lower = statement.lower()
            for key, tag in tags.items():
                if key.startswith("_"):
                    continue
                if key.lower() in stmt_lower:
                    return tag
            return "DHF-biological"

        promotable, held_by_tag = [], []
        for c in new_eligible:
            tag = _source_tag(c.get("invariant", ""))
            (held_by_tag if tag == "DHF-biological" else promotable).append((c, tag))

        if held_by_tag:
            print(f"[PROMOTE] {len(held_by_tag)} candidate(s) held — DHF-biological source, "
                  f"no cross-substrate confirmation yet.")
            ts = datetime.now(timezone.utc).isoformat()
            for c, _ in held_by_tag:
                self._log({
                    "timestamp": ts,
                    "statement": c.get("invariant", "")[:80],
                    "verdict":   "HOLD",
                    "reason":    "DHF-biological source only — requires bootstrap CONVERGE before promotion.",
                })
            result["held"] += len(held_by_tag)

        new_eligible = [c for c, _ in promotable]
        if not new_eligible:
            result["skipped"] = "all eligible candidates held pending cross-substrate confirmation"
            return result

        result["eligible_count"] = len(new_eligible)
        print(f"\n[PROMOTE] {len(new_eligible)} candidate(s) eligible — calling Opus filter.")

        candidates_block = "\n\n".join(
            f"[{c['recurrence']}x / {len(c.get('appears_in', []))} nodes]\n"
            f"Statement: {c['invariant']}\n"
            f"Nodes: {', '.join(c.get('appears_in', [])[:6])}"
            for c in new_eligible
        )
        prompt = _FILTER_PROMPT.format(
            genome_excerpt=genome_text[:4000],
            candidates_block=candidates_block,
        )

        try:
            response = self._client.messages.create(
                model="claude-opus-4-7",
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}],
            )
            verdict_text = response.content[0].text
        except Exception as e:
            print(f"[PROMOTE] Opus call failed: {e}")
            result["skipped"] = f"api_error: {e}"
            return result

        verdicts = self._apply_verdicts(verdict_text)
        result["promoted"] = sum(1 for v in verdicts if v["verdict"] == "PROMOTE")
        result["held"]     = sum(1 for v in verdicts if v["verdict"] == "HOLD")
        result["rejected"] = sum(1 for v in verdicts if v["verdict"] == "REJECT")
        return result

    # ─────────────────────────────────────────────────────────────────────────

    def _already_present(self, statement, genome_text):
        # type: (str, str) -> bool
        """Return True if this concept is substantively already in the genome."""
        stopwords = {
            'the', 'a', 'an', 'is', 'are', 'of', 'in', 'and', 'or', 'not',
            'to', 'for', 'it', 'this', 'that', 'with', 'as', 'by', 'at',
            'from', 'be', 'has', 'have', 'been', 'but', 'if', 'on', 'its',
            'just', 'which', 'than', 'more', 'only', 'also', 'any', 'all',
        }
        words = [w.strip('.,;:()[]') for w in statement.lower().split()]
        content_words = [w for w in words if w not in stopwords and len(w) > 4][:10]
        genome_lower = genome_text.lower()
        hits = sum(1 for w in content_words if w in genome_lower)
        return hits >= 6

    def _apply_verdicts(self, verdict_text):
        # type: (str) -> list
        """Parse verdict blocks, write PROMOTEs to genome, log all decisions."""
        ts       = datetime.now(timezone.utc).isoformat()
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        verdicts = []

        raw_blocks = re.split(r'(?m)^CANDIDATE:', verdict_text)
        for block in raw_blocks:
            block = block.strip()
            if not block:
                continue
            verdict_m = re.search(r'VERDICT:\s*(PROMOTE|HOLD|REJECT)', block)
            reason_m  = re.search(r'REASON:\s*(.+)', block)
            if not verdict_m:
                continue

            stmt_end  = block.find('\nVERDICT:')
            statement = block[:stmt_end].strip() if stmt_end != -1 else block[:120]
            verdict   = verdict_m.group(1)
            reason    = reason_m.group(1).strip() if reason_m else ""

            entry = {"timestamp": ts, "statement": statement,
                     "verdict": verdict, "reason": reason}
            verdicts.append(entry)
            self._log(entry)

            print(f"[PROMOTE] {verdict}: {statement[:65]}...")
            if reason:
                print(f"          {reason}")

            if verdict == "PROMOTE":
                addition = (
                    f"\n\n## [PROMOTED {date_str}]\n\n"
                    f"{statement}\n\n"
                    f"*Promotion basis: {reason}*  \n"
                    f"*promoted_by: FREED_PROMOTE_phase*\n"
                )
                with open(GENOME_FILE, "a", encoding="utf-8") as f:
                    f.write(addition)
                print(f"[PROMOTE] → Written to genome.")

        return verdicts

    def _log(self, entry):
        # type: (dict) -> None
        PROMOTE_LOG.parent.mkdir(exist_ok=True)
        with open(PROMOTE_LOG, "a") as f:
            f.write(json.dumps(entry) + "\n")

    @staticmethod
    def recent_decisions(since_ts):
        # type: (str) -> list
        """Return promote log entries newer than since_ts. Called by PRE-AUDIT."""
        if not PROMOTE_LOG.exists():
            return []
        decisions = []
        for line in PROMOTE_LOG.read_text(encoding="utf-8").splitlines():
            try:
                e = json.loads(line)
                if e.get("timestamp", "") > since_ts:
                    decisions.append(e)
            except Exception:
                pass
        return decisions
