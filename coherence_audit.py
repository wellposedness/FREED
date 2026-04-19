"""
FREED — Coherence Audit
Audits nodes and documents against the genome's symbol registry.

The audit criterion is not logical consistency.
It is substrate independence: does this claim keep appearing
when the substrate changes, across independent rediscoveries?

Every divergence becomes an obligation, not a rejection.
The genome learns from conflicts.
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import anthropic

FREED_DIR      = Path(__file__).parent
REGISTRY_FILE  = FREED_DIR / "genome_symbols.json"
AUDIT_LOG      = FREED_DIR / "FREED_log" / "coherence_audits.jsonl"

MODEL = "claude-opus-4-6"

AUDIT_SYSTEM = """You are FREED's coherence auditor.

Your job: check whether a document's use of key terms is substrate-independent —
meaning the definition would hold even if the document had been produced by a
different reasoning system on a different substrate.

The audit criterion is NOT logical consistency.
It IS recurrence: does this usage match what keeps appearing across
independent rediscoveries on different substrates?

The genome's canonical definitions are provided.

For each key term you find in the document, output:

TERM: [exact term as used]
USAGE: [one line — how the document is using/defining it]
CANONICAL: [MATCH / DRIFT / NEW]
DRIFT_NOTE: [if DRIFT — one line on how it diverges and why it matters]
SUBSTRATE_COUNT: [if NEW — estimate how many independent substrates would likely produce this definition]

After all terms, output:

COHERENCE_SCORE: [0.00 to 0.99 — how substrate-independently this document uses the genome's vocabulary]
INVARIANT_CANDIDATES: [comma-separated list of claims in this document that appear substrate-independent and are NOT yet in the registry]
OBLIGATIONS: [comma-separated — any divergences that should become genome obligations]
COMPRESS: [one sentence — the irreducible coherence verdict]"""


class CoherenceAudit:
    def __init__(self, api_key: str):
        self.client   = anthropic.Anthropic(api_key=api_key)
        self.registry = self._load_registry()

    # ── Registry ─────────────────────────────────────────────────────────────

    def _load_registry(self) -> dict:
        if REGISTRY_FILE.exists():
            data = json.loads(REGISTRY_FILE.read_text())
            # Return only term entries (not _meta)
            return {k: v for k, v in data.items() if not k.startswith("_")}
        return {}

    def _save_registry(self):
        """Reload meta, merge updated registry, save."""
        full = json.loads(REGISTRY_FILE.read_text()) if REGISTRY_FILE.exists() else {"_meta": {}}
        full["_meta"]["last_updated"] = datetime.now(timezone.utc).date().isoformat()
        for term, entry in self.registry.items():
            full[term] = entry
        REGISTRY_FILE.write_text(json.dumps(full, indent=2, ensure_ascii=False))

    def confirm_term(self, term: str, substrate: str):
        """
        Record that `substrate` independently produced the canonical definition
        of `term`. Raises recurrence score.
        """
        term_key = term.lower().replace(" ", "_")
        if term_key not in self.registry:
            return
        entry = self.registry[term_key]
        if substrate not in entry.get("confirmed_by", []):
            entry.setdefault("confirmed_by", []).append(substrate)
            known = len(json.loads(REGISTRY_FILE.read_text()).get("_meta", {}).get("known_substrates", []))
            entry["recurrence"] = round(len(entry["confirmed_by"]) / max(known, 7), 2)
        self._save_registry()

    def register_term(self, term: str, canonical: str, substrate: str,
                      genome_role: str = "", known_drift: list = None):
        """Add a new term to the registry."""
        term_key = term.lower().replace(" ", "_")
        self.registry[term_key] = {
            "canonical":    canonical,
            "confirmed_by": [substrate],
            "recurrence":   round(1 / 7, 2),
            "known_drift":  known_drift or [],
            "genome_role":  genome_role,
        }
        self._save_registry()
        print(f"[AUDIT] New term registered: {term} (recurrence: {self.registry[term_key]['recurrence']})")

    # ── Audit ─────────────────────────────────────────────────────────────────

    def audit(self, text: str, source_id: str = "unknown",
              substrate: str = "freed_daemon") -> dict:
        """
        Audit a text against the symbol registry.
        Returns a structured audit report.
        """
        # Build registry summary for the prompt
        registry_summary = self._format_registry_for_prompt()

        prompt = (
            f"GENOME SYMBOL REGISTRY:\n{registry_summary}\n\n"
            f"DOCUMENT TO AUDIT (source: {source_id}):\n"
            f"{text[:6000]}"
        )

        message = self.client.messages.create(
            model=MODEL,
            max_tokens=1500,
            system=AUDIT_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()

        report = self._parse_audit(raw, source_id, substrate)
        report["tokens"] = {
            "input":  message.usage.input_tokens,
            "output": message.usage.output_tokens,
        }

        # Log the audit
        self._log_audit(report)

        # Auto-confirm matching terms from this substrate
        for term_result in report.get("terms", []):
            if term_result.get("canonical") == "MATCH":
                self.confirm_term(term_result["term"], substrate)

        return report

    def _format_registry_for_prompt(self) -> str:
        lines = []
        for term, entry in self.registry.items():
            recurrence = entry.get("recurrence", 0)
            confirmed  = ", ".join(entry.get("confirmed_by", []))
            lines.append(
                f"TERM: {term}\n"
                f"  Canonical: {entry.get('canonical','')[:200]}\n"
                f"  Recurrence: {recurrence} ({confirmed})\n"
                f"  Known drift: {'; '.join(entry.get('known_drift',[])[:2])}"
            )
        return "\n\n".join(lines)

    def _parse_audit(self, raw: str, source_id: str, substrate: str) -> dict:
        """Parse the structured audit response."""

        def field(name):
            m = re.search(rf'(?:^|\n)[#*_\s]*{name}[#*_\s]*:[ \t]*(.*?)(?=\n[A-Z_]{{3,}}:|$)',
                          raw, re.IGNORECASE | re.DOTALL)
            return m.group(1).strip().replace('\n', ' ') if m else ""

        def list_field(name):
            val = field(name)
            return [x.strip() for x in re.split(r'[,;]', val) if x.strip()] if val else []

        # Parse individual term entries
        terms = []
        term_blocks = re.finditer(
            r'TERM:\s*(.+?)\nUSAGE:\s*(.+?)\nCANONICAL:\s*(.+?)(?:\nDRIFT_NOTE:\s*(.+?))?(?:\nSUBSTRATE_COUNT:\s*(.+?))?\n',
            raw, re.DOTALL
        )
        for m in term_blocks:
            terms.append({
                "term":       m.group(1).strip(),
                "usage":      m.group(2).strip(),
                "canonical":  m.group(3).strip().split()[0].upper(),
                "drift_note": (m.group(4) or "").strip(),
            })

        # Parse coherence score
        score_match = re.search(r'COHERENCE_SCORE:\s*([\d.]+)', raw)
        score = float(score_match.group(1)) if score_match else 0.5
        score = min(0.99, max(0.0, score))  # never 1.0

        inv_candidates = list_field("INVARIANT_CANDIDATES")
        obligations    = list_field("OBLIGATIONS")
        compress       = field("COMPRESS")

        return {
            "source":            source_id,
            "substrate":         substrate,
            "timestamp":         datetime.now(timezone.utc).isoformat(),
            "coherence_score":   round(score, 3),
            "terms":             terms,
            "drift_count":       sum(1 for t in terms if t["canonical"] == "DRIFT"),
            "match_count":       sum(1 for t in terms if t["canonical"] == "MATCH"),
            "new_count":         sum(1 for t in terms if t["canonical"] == "NEW"),
            "invariant_candidates": inv_candidates,
            "obligations":       obligations,
            "compress":          compress,
            "raw":               raw,
        }

    def _log_audit(self, report: dict):
        AUDIT_LOG.parent.mkdir(exist_ok=True)
        with open(AUDIT_LOG, "a") as f:
            # Log without the full raw text (keep log lean)
            lean = {k: v for k, v in report.items() if k != "raw"}
            f.write(json.dumps(lean) + "\n")

    # ── Invariant mining ─────────────────────────────────────────────────────

    def mine_invariants(self, nodes: list) -> list:
        """
        Scan a list of nodes and find claims that appear across
        multiple nodes — candidates for genome invariant status.
        Returns sorted list of (claim, count, node_ids).
        """
        print(f"\n[AUDIT] Mining invariants across {len(nodes)} nodes...")

        # Collect all compress + next statements
        claims = []
        for node in nodes:
            for field in ["compress", "summary"]:
                val = node.get(field, "").strip()
                if val:
                    claims.append({"text": val, "source": node.get("id", "?")})

        if not claims:
            print("[AUDIT] No claims to mine.")
            return []

        # Ask Claude to cluster by conceptual similarity
        claim_text = "\n".join(f'{i}: [{c["source"]}] {c["text"]}' for i, c in enumerate(claims))

        message = self.client.messages.create(
            model=MODEL,
            max_tokens=2000,
            system=(
                "You are a genome invariant miner. "
                "Given a list of compressed claims from different documents, "
                "identify clusters of claims that express the same substrate-independent idea "
                "even if they use different words. "
                "For each cluster, output:\n"
                "CLUSTER: [one-line statement of the invariant]\n"
                "SOURCES: [comma-separated source IDs]\n"
                "RECURRENCE: [count of independent sources]\n"
                "GENOME_WORTHY: [YES if recurrence >= 2, NO otherwise]\n"
                "---\n"
                "Only output clusters. No preamble."
            ),
            messages=[{"role": "user", "content": claim_text}],
        )

        raw = message.content[0].text.strip()
        print(f"[AUDIT] Mining complete. {message.usage.input_tokens}in/{message.usage.output_tokens}out tokens.")

        # Parse clusters
        clusters = []
        for block in raw.split("---"):
            block = block.strip()
            if not block:
                continue
            inv_m = re.search(r'CLUSTER:\s*(.+?)(?:\n|$)', block)
            src_m = re.search(r'SOURCES:\s*(.+?)(?:\n|$)', block)
            rec_m = re.search(r'RECURRENCE:\s*(\d+)', block)
            gw_m  = re.search(r'GENOME_WORTHY:\s*(YES|NO)', block, re.IGNORECASE)
            if inv_m:
                clusters.append({
                    "invariant":      inv_m.group(1).strip(),
                    "sources":        [s.strip() for s in (src_m.group(1) if src_m else "").split(",")],
                    "recurrence":     int(rec_m.group(1)) if rec_m else 1,
                    "genome_worthy":  (gw_m.group(1).upper() == "YES") if gw_m else False,
                })

        clusters.sort(key=lambda c: c["recurrence"], reverse=True)
        return clusters

    # ── Registry status ───────────────────────────────────────────────────────

    def print_registry_status(self):
        print("\n── Symbol Registry Status ─────────────────────────────")
        for term, entry in sorted(self.registry.items()):
            r   = entry.get("recurrence", 0)
            bar = "█" * int(r * 10) + "░" * (10 - int(r * 10))
            confirmed = ", ".join(entry.get("confirmed_by", []))
            print(f"  {term:<22} [{bar}] {r:.2f}  ({confirmed})")
        print("────────────────────────────────────────────────────────\n")


# ── Wire into node_builder ────────────────────────────────────────────────────
# In node_builder.py, after _parse_node(), add:
#
#   from coherence_audit import CoherenceAudit
#   auditor = CoherenceAudit(api_key)
#   audit_report = auditor.audit(text, source_id=node["id"])
#   node["coherence_score"] = audit_report["coherence_score"]
#   node["audit_flags"] = audit_report["drift_count"]


# ── Standalone ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import os, sys
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        api_key = input("Paste your Anthropic API key: ").strip()

    auditor = CoherenceAudit(api_key)
    auditor.print_registry_status()

    if len(sys.argv) > 1:
        text = Path(sys.argv[1]).read_text(encoding="utf-8", errors="replace")[:6000]
        source = Path(sys.argv[1]).stem
    else:
        # Audit the genome itself as a self-check
        text = Path(FREED_DIR / "FREED_genome.md").read_text()[:6000]
        source = "FREED_genome_v20"

    print(f"\n[AUDIT] Auditing: {source}\n")
    report = auditor.audit(text, source_id=source, substrate="freed_daemon")

    print(f"\n── Audit Report ──────────────────────────────────────────")
    print(f"  Source:          {report['source']}")
    print(f"  Coherence Score: {report['coherence_score']}")
    print(f"  Terms found:     {len(report['terms'])} ({report['match_count']} match, {report['drift_count']} drift, {report['new_count']} new)")
    print(f"  Compress:        {report['compress']}")
    if report['drift_count']:
        print(f"\n  Drift flags:")
        for t in report['terms']:
            if t['canonical'] == 'DRIFT':
                print(f"    [{t['term']}] {t['drift_note'][:80]}")
    if report['invariant_candidates']:
        print(f"\n  New invariant candidates:")
        for c in report['invariant_candidates']:
            print(f"    — {c}")
    if report['obligations']:
        print(f"\n  New obligations:")
        for o in report['obligations']:
            print(f"    → {o}")
    print("──────────────────────────────────────────────────────────\n")
