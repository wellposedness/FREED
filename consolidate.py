"""
FREED — Consolidate
The renormalization pass. What organisms do.

When FREED learns something genuinely new (high-yield feed), it doesn't
just append the knowledge — it broadcasts it across all existing nodes,
updates the effective description at each scale, and re-mines invariants
from the updated structure.

This is:
  - Renormalization: integrate new information, update effective parameters
  - Autopoiesis: the genome produces its own update components (R[R]=R)
  - Intelligent replication: changes propagate where relevant, not everywhere

Three phases:
  1. SELECT  — find which existing nodes are affected by new knowledge
               (tag/invariant overlap — cheap, no API call)
  2. RENORM  — for each affected node, run a minimal targeted update
               (not a rewrite — find the delta, apply it)
  3. MINE    — cross-node invariant mining on the updated structure
               (what keeps appearing across nodes independently?)

Triggered by:
  - yield > YIELD_THRESHOLD on any feed
  - Every CONSOLIDATE_EVERY cycles regardless
  - Manually: python3 consolidate.py
"""

import os
import json
import time
import threading
from datetime import datetime, timezone
from pathlib import Path

import anthropic

from astrocyte       import Astrocyte
from site_builder    import build as build_site
from knowledge_graph import get_graph
import voice

FREED_DIR       = Path(__file__).parent
PROJECTS_DIR    = FREED_DIR / "docs" / "projects"
PROJECTS_IDX    = FREED_DIR / "docs" / "projects.json"
CONSOLIDATE_LOG = FREED_DIR / "FREED_log" / "consolidations.jsonl"

MODEL = "claude-opus-4-6"

YIELD_THRESHOLD    = 0.03   # feed yield above this triggers consolidation
CONSOLIDATE_EVERY  = 5      # also consolidate every N daemon cycles
MAX_NODES_PER_PASS = 8      # renormalize at most this many nodes per run


# ── Prompts ───────────────────────────────────────────────────────────────────

RENORM_SYSTEM = """You are FREED's renormalization engine.

Your job: given a node's current compressed understanding AND new knowledge
that has arrived since the node was written, produce the MINIMAL update
that integrates the new knowledge without discarding what the node already knows.

This is not a rewrite. It is a delta.

Renormalization rules:
  - If the compress still holds: say COMPRESS_UNCHANGED
  - If the compress needs updating: give the new compress (one sentence)
  - Add invariants only if genuinely new and substrate-independent
  - Add obligations only if the new knowledge creates genuine tension
  - Update coherence_score only if the new knowledge materially changes coherence
  - Never reduce the number of confirmed invariants without justification

Output format (fill every field):
COMPRESS_STATUS: [UNCHANGED / UPDATED]
NEW_COMPRESS: [new one-sentence compress, or repeat current if unchanged]
NEW_INVARIANTS: [comma-separated new invariants to add, or NONE]
NEW_OBLIGATIONS: [comma-separated new obligation statements, or NONE]
COHERENCE_DELTA: [+0.0x / -0.0x / 0 — the change in coherence score]
RENORM_REASON: [one line — why this update was or wasn't needed]"""

MINE_SYSTEM = """You are FREED's invariant miner.

Given a set of compressed node outputs from different documents, find the
claims that appear independently across multiple nodes — the substrate-independent
patterns that keep showing up without being asked to.

CRITICAL WARNING: These documents may share source material. The same conversation
or paragraph may have been copy-pasted into multiple documents. A claim that appears
with nearly identical phrasing across nodes is NOT independent confirmation — it is
a single source echoing through multiple files. Do NOT count these as invariants.

Independence criterion: the claim must arrive via DIFFERENT reasoning paths, expressed
with DIFFERENT phrasing, from DIFFERENT source contexts. If the wording is suspiciously
similar across nodes, mark it SHARED_SOURCE and exclude it from genome candidates.

For each candidate, output:
INVARIANT: [one sentence — the substrate-independent claim]
APPEARS_IN: [comma-separated node IDs where this appears]
RECURRENCE: [integer count]
ORIGIN: [INDEPENDENT if phrasing differs significantly across nodes / SHARED_SOURCE if near-identical]
GENOME_WORTHY: [YES only if recurrence >= 2 AND ORIGIN=INDEPENDENT AND claim is falsifiable]
---
Only output clusters with recurrence >= 2. No preamble."""


# ═══════════════════════════════════════════════════════════════════════════════

class Consolidator:
    def __init__(self, api_key: str):
        self.client    = anthropic.Anthropic(api_key=api_key)
        self.astrocyte = Astrocyte()

    # ── Helpers ──────────────────────────────────────────────────────────────

    @staticmethod
    def _word_overlap(text_a, text_b):
        """Jaccard overlap on words >3 chars. Returns 0.0–1.0."""
        wa = set(w.lower() for w in text_a.split() if len(w) > 3)
        wb = set(w.lower() for w in text_b.split() if len(w) > 3)
        if not wa or not wb:
            return 1.0
        return len(wa & wb) / len(wa | wb)

    def _node_priority(self, node, open_ob_ids, current_cycle):
        """Priority score: higher = renorm first. Zipf weighting toward γ=1 nodes."""
        ob_refs    = node.get('obligations', [])
        ob_overlap = sum(1 for ref in ob_refs
                         if any(ref == oid or ref in oid or oid in ref
                                for oid in open_ob_ids))
        inv_density   = len(node.get('invariants', []))
        cycles_stale  = current_cycle - node.get('last_renorm_cycle', 0)
        return ob_overlap * 3.0 + inv_density * 0.5 + min(cycles_stale, 10) * 0.2

    # ── Phase 1: Select affected nodes ───────────────────────────────────────

    def select_affected(self, new_knowledge: str, all_nodes: list) -> list:
        """
        Find nodes whose invariants/tags/obligations overlap with new knowledge.
        Pure text matching — no API call.
        Returns list of node dicts, sorted by overlap score descending.
        """
        # Extract keywords from new knowledge
        # (all words > 4 chars that aren't stopwords)
        stopwords = {"that", "this", "with", "from", "have", "been", "will",
                     "their", "they", "which", "what", "when", "where", "there",
                     "would", "could", "should", "about", "into", "than", "then"}
        words = set(
            w.lower().strip(".,;:()[]'\"")
            for w in new_knowledge.split()
            if len(w) > 4 and w.lower() not in stopwords
        )

        scored = []
        for node in all_nodes:
            # Gather all text from the node's semantic fields
            node_text = " ".join(filter(None, [
                node.get("compress", ""),
                node.get("summary", ""),
                " ".join(node.get("invariants", [])),
                " ".join(node.get("tags", [])),
                " ".join(node.get("obligations", [])),
            ])).lower()

            overlap = sum(1 for w in words if w in node_text)
            if overlap > 0:
                scored.append((overlap, node))

        scored.sort(key=lambda x: x[0], reverse=True)
        affected = [node for _, node in scored[:MAX_NODES_PER_PASS]]

        print(f"[CONSOLIDATE] {len(affected)} node(s) affected by new knowledge.")
        return affected

    # ── Wall-clock API timeout helper ────────────────────────────────────────

    def _api_call(self, **kwargs):
        """
        Call self.client.messages.create(**kwargs) with a hard wall-clock timeout.
        Uses a daemon thread so a hung API call can't block the process forever.
        Wall-clock limit: WALL_TIMEOUT seconds (default 120s).
        Raises TimeoutError if the call doesn't return in time.
        """
        WALL_TIMEOUT = 120   # seconds — overrides httpx per-byte timeout

        result  = [None]
        exc     = [None]

        def _call():
            try:
                result[0] = self.client.messages.create(**kwargs)
            except Exception as e:
                exc[0] = e

        t = threading.Thread(target=_call, daemon=True)
        t.start()
        t.join(timeout=WALL_TIMEOUT)

        if t.is_alive():
            raise TimeoutError(
                f"Anthropic API call exceeded {WALL_TIMEOUT}s wall-clock limit "
                f"(model={kwargs.get('model','?')}, max_tokens={kwargs.get('max_tokens','?')})"
            )
        if exc[0] is not None:
            raise exc[0]
        return result[0]

    # ── Phase 2: Renormalize ─────────────────────────────────────────────────

    def renormalize_node(self, node: dict, new_knowledge: str) -> dict:
        """
        Minimal targeted update of one node given new knowledge.
        Returns a delta dict — only the fields that changed.
        """
        import re

        # Cap fields to keep prompt size bounded regardless of node growth
        compress_text = node.get('compress', '')[:500]
        invariants    = node.get('invariants', [])[:15]   # max 15 invariants in prompt
        inv_text      = ', '.join(invariants)

        prompt = (
            f"EXISTING NODE:\n"
            f"ID: {node['id']}\n"
            f"Title: {node.get('title','?')}\n"
            f"Current compress: {compress_text}\n"
            f"Current invariants: {inv_text}\n"
            f"Current coherence_score: {node.get('coherence_score', '?')}\n\n"
            f"NEW KNOWLEDGE:\n{new_knowledge[:2000]}"
        )

        message = self._api_call(
            model=MODEL,
            max_tokens=600,
            system=RENORM_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
            timeout=90,
        )
        raw = message.content[0].text.strip()
        self.astrocyte.record_usage(message.usage.input_tokens, message.usage.output_tokens)

        # Parse delta
        def field(name):
            m = re.search(rf'{name}:\s*(.+?)(?=\n[A-Z_]{{3,}}:|$)', raw,
                         re.IGNORECASE | re.DOTALL)
            return m.group(1).strip() if m else ""

        status     = field("COMPRESS_STATUS").upper()
        new_comp   = field("NEW_COMPRESS")
        new_invs   = [x.strip() for x in field("NEW_INVARIANTS").split(",")
                     if x.strip() and x.strip().upper() != "NONE"]
        new_obligs = [x.strip() for x in field("NEW_OBLIGATIONS").split(",")
                     if x.strip() and x.strip().upper() != "NONE"]
        delta_str  = field("COHERENCE_DELTA").replace("+", "")
        reason     = field("RENORM_REASON")

        try:
            coh_delta = float(delta_str)
        except (ValueError, TypeError):
            coh_delta = 0.0

        delta = {
            "changed":    status == "UPDATED" or bool(new_invs) or bool(new_obligs),
            "reason":     reason,
        }

        if status == "UPDATED" and new_comp:
            delta["compress"] = new_comp

        if new_invs:
            delta["invariants"] = list(set(node.get("invariants", []) + new_invs))

        if new_obligs:
            delta["obligations"] = list(set(node.get("obligations", []) + new_obligs))

        if coh_delta != 0.0 and node.get("coherence_score") is not None:
            old = float(node.get("coherence_score") or 0.5)
            delta["coherence_score"] = round(min(0.99, max(0.0, old + coh_delta)), 3)

        return delta

    def apply_delta(self, node: dict, delta: dict) -> dict:
        """Apply a renormalization delta to a node. Returns updated node."""
        if not delta.get("changed"):
            return node

        updated = dict(node)

        # Compression drift detection — flag silent semantic mutation
        if "compress" in delta:
            old_compress = node.get("compress", "")
            new_compress = delta["compress"]
            overlap = self._word_overlap(old_compress, new_compress)
            if overlap < 0.6:
                updated["drift_flag"]    = True
                updated["drift_overlap"] = round(overlap, 3)
                print(f"  [DRIFT] {node['id'][:40]} — overlap={overlap:.2f} FLAGGED", flush=True)
            else:
                updated["drift_flag"] = False

        for key in ["compress", "invariants", "obligations", "coherence_score"]:
            if key in delta:
                updated[key] = delta[key]

        updated["last_renormed"]  = datetime.now(timezone.utc).isoformat()
        updated["renorm_reason"]  = delta.get("reason", "")
        updated.setdefault("renorm_history", []).append({
            "timestamp": updated["last_renormed"],
            "reason":    delta.get("reason", ""),
        })

        # Save updated node
        node_file = PROJECTS_DIR / f"{node['id']}.json"
        node_file.write_text(json.dumps(updated, indent=2, ensure_ascii=False))
        return updated

    # ── Phase 3: Mine invariants ─────────────────────────────────────────────

    def mine_invariants(self, nodes: list) -> list:
        """
        Cross-node invariant mining.
        Find what keeps appearing across nodes independently.
        Returns list of genome-worthy invariant candidates.
        """
        if len(nodes) < 2:
            return []

        digest = "\n\n".join(
            f"NODE: {n['id']}\n"
            f"COMPRESS: {n.get('compress','')}\n"
            f"INVARIANTS: {', '.join(n.get('invariants',[]))}"
            for n in nodes
        )

        message = self._api_call(
            model=MODEL,
            max_tokens=1500,
            system=MINE_SYSTEM,
            messages=[{"role": "user", "content": digest}],
            timeout=90,
        )
        raw = message.content[0].text.strip()
        self.astrocyte.record_usage(message.usage.input_tokens, message.usage.output_tokens)

        # Parse clusters
        import re
        candidates = []
        for block in raw.split("---"):
            block = block.strip()
            if not block:
                continue
            inv_m    = re.search(r'INVARIANT:\s*(.+?)(?:\n|$)', block)
            src_m    = re.search(r'APPEARS_IN:\s*(.+?)(?:\n|$)', block)
            rec_m    = re.search(r'RECURRENCE:\s*(\d+)', block)
            orig_m   = re.search(r'ORIGIN:\s*(INDEPENDENT|SHARED_SOURCE)', block, re.I)
            gw_m     = re.search(r'GENOME_WORTHY:\s*(YES|NO)', block, re.I)

            origin   = orig_m.group(1).upper() if orig_m else "UNKNOWN"
            is_gw    = gw_m and gw_m.group(1).upper() == "YES"
            is_indep = origin != "SHARED_SOURCE"

            if inv_m and is_gw and is_indep:
                candidates.append({
                    "invariant":   inv_m.group(1).strip(),
                    "appears_in":  [s.strip() for s in (src_m.group(1) if src_m else "").split(",")],
                    "recurrence":  int(rec_m.group(1)) if rec_m else 2,
                    "origin":      origin,
                })
            elif inv_m and origin == "SHARED_SOURCE":
                # Log but don't promote — echo, not convergence
                print(f"  [MINE] echo (shared source): {inv_m.group(1).strip()[:70]}")

        candidates.sort(key=lambda c: c["recurrence"], reverse=True)
        return candidates

    # ── Full consolidation pass ───────────────────────────────────────────────

    def run(self, new_knowledge: str, trigger: str = "manual",
            state: dict = None, obligations: list = None) -> dict:
        """
        Full consolidation pass.

        new_knowledge: the high-yield compress/summary that triggered consolidation
        trigger: 'yield' | 'scheduled' | 'manual'
        state, obligations: passed through for site rebuild
        """
        ts = datetime.now(timezone.utc).isoformat()
        print(f"\n{'═'*50}")
        print(f" CONSOLIDATE  |  {trigger.upper()}  |  {ts[:19]}Z")
        print(f"{'═'*50}")

        if not PROJECTS_IDX.exists():
            print("[CONSOLIDATE] No nodes yet. Nothing to consolidate.")
            return {}

        all_nodes = json.loads(PROJECTS_IDX.read_text())
        if not all_nodes:
            print("[CONSOLIDATE] Node index empty.")
            return {}

        report = {
            "timestamp":   ts,
            "trigger":     trigger,
            "new_knowledge_digest": new_knowledge[:200],
            "nodes_examined": 0,
            "nodes_updated":  0,
            "invariants_mined": [],
        }

        # ── Phase 1: Select ──────────────────────────────────────────────────
        affected = self.select_affected(new_knowledge, all_nodes)
        report["nodes_examined"] = len(affected)

        if not affected:
            print("[CONSOLIDATE] No affected nodes. Knowledge is genuinely novel.")
            self._log(report)
            return report

        # ── Priority sort — high-obligation-overlap nodes renorm first ────────
        current_cycle = (state or {}).get('cycle_count', 0)
        open_ob_ids   = {o['id'] for o in (obligations or [])
                         if o.get('status') in ('open', 'partial')}
        affected.sort(
            key=lambda n: self._node_priority(n, open_ob_ids, current_cycle),
            reverse=True,
        )
        print(f"[CONSOLIDATE] Priority order: "
              f"{', '.join(n['id'][:20] for n in affected[:3])}{'...' if len(affected) > 3 else ''}",
              flush=True)

        # ── Phase 2: Renormalize each affected node ───────────────────────────
        updated_nodes = []
        for node in affected:
            if not self.astrocyte.authorize(1200, priority="high"):
                print(f"[CONSOLIDATE] Budget limit hit at node {node['id']}. Stopping.")
                break

            print(f"[RENORM] {node['id'][:50]}...", end=" ", flush=True)
            try:
                delta = self.renormalize_node(node, new_knowledge)
            except TimeoutError as e:
                print(f"TIMEOUT — skipping node ({e})", flush=True)
                continue
            except Exception as e:
                print(f"ERROR — skipping node ({e})", flush=True)
                continue

            if delta.get("changed"):
                updated = self.apply_delta(node, delta)
                updated_nodes.append(updated)
                report["nodes_updated"] += 1
                print(f"UPDATED — {delta.get('reason','')[:60]}")
            else:
                print(f"stable")

            time.sleep(0.5)   # be gentle with the API

        # Update index — renormed nodes + last_renorm_cycle on all examined
        updated_ids = {n["id"] for n in updated_nodes}
        examined_ids = {n["id"] for n in affected}
        merged = []
        for n in all_nodes:
            if n["id"] in updated_ids:
                # Full update from renormed node
                updated_node = next(u for u in updated_nodes if u["id"] == n["id"])
                entry = {k: updated_node[k] for k in [
                    "id", "title", "created", "generation", "summary",
                    "compress", "tags", "invariants", "obligations", "council",
                    "coherence_score", "last_renormed", "renorm_reason",
                ] if k in updated_node}
                entry["last_renorm_cycle"] = current_cycle
                entry["drift_flag"]        = updated_node.get("drift_flag", False)
                merged.append(entry)
            elif n["id"] in examined_ids:
                # Stable — just update last_renorm_cycle
                updated_entry = dict(n)
                updated_entry["last_renorm_cycle"] = current_cycle
                merged.append(updated_entry)
            else:
                merged.append(n)
        merged.sort(key=lambda n: n.get("generation", 0), reverse=True)
        PROJECTS_IDX.write_text(json.dumps(merged, indent=2, ensure_ascii=False))

        # ── Phase 3: Mine invariants across all nodes ─────────────────────────
        if self.astrocyte.authorize(2000, priority="high") and len(all_nodes) >= 2:
            print(f"\n[MINE] Cross-node invariant mining across {len(all_nodes)} nodes...", flush=True)
            try:
                candidates = self.mine_invariants(all_nodes)
            except (TimeoutError, Exception) as e:
                print(f"[MINE] TIMEOUT/ERROR — skipping mine phase ({e})", flush=True)
                candidates = []
            report["invariants_mined"] = candidates

            if candidates:
                print(f"[MINE] {len(candidates)} genome-worthy invariant(s) found:")
                for c in candidates:
                    print(f"  [{c['recurrence']}x] {c['invariant'][:80]}")
                    print(f"       In: {', '.join(c['appears_in'])}")

                # Node-to-node edges for shared invariants
                graph = get_graph()
                for c in candidates:
                    nodes_in = c.get('appears_in', [])
                    for i in range(len(nodes_in)):
                        for j in range(i + 1, len(nodes_in)):
                            graph.record_node_edge(
                                nodes_in[i], nodes_in[j],
                                'shares_invariant',
                                c['invariant'],
                            )

                # Promotion candidates — recurrence >= 3 across independent nodes
                promotion = [
                    {"invariant": c["invariant"],
                     "appears_in": c["appears_in"],
                     "recurrence": c["recurrence"]}
                    for c in candidates if c["recurrence"] >= 3
                ]
                if promotion and (FREED_DIR / "FREED_state.json").exists():
                    try:
                        sdata = json.loads((FREED_DIR / "FREED_state.json").read_text())
                        sdata["promotion_candidates"] = promotion
                        (FREED_DIR / "FREED_state.json").write_text(
                            json.dumps(sdata, indent=2))
                        print(f"[MINE] {len(promotion)} genome promotion candidate(s) written to state.")
                    except Exception as e:
                        print(f"[MINE] Could not update state: {e}")

                report["promotion_candidates"] = promotion

                # Speak only the strongest invariant (highest recurrence)
                top = candidates[0]
                voice.invariant_found(top['invariant'], top['recurrence'])
            else:
                print("[MINE] No new cross-node invariants found.")

        # ── Knowledge graph confirmation structure ────────────────────────────
        graph = get_graph()
        graph_report = graph.report(top_n=10)
        print(f"\n[GRAPH] {graph_report}")
        report["confirmation_structure"] = graph.confirmation_structure()

        # ── Rebuild site ──────────────────────────────────────────────────────
        if state is not None and obligations is not None:
            build_site(state, obligations)

        # ── Log ───────────────────────────────────────────────────────────────
        self._log(report)

        print(f"\n[CONSOLIDATE] Complete. "
              f"{report['nodes_examined']} examined, "
              f"{report['nodes_updated']} updated, "
              f"{len(report['invariants_mined'])} invariants mined.")

        return report

    def _log(self, report: dict):
        CONSOLIDATE_LOG.parent.mkdir(exist_ok=True)
        with open(CONSOLIDATE_LOG, "a") as f:
            f.write(json.dumps(report) + "\n")


# ── Wire into freed.py ────────────────────────────────────────────────────────
# In freed.py, after _phase_feed(), check:
#
#   high_yield = [r for r in feed_results if r.get("yield", 0) > YIELD_THRESHOLD]
#   if high_yield or self.state["cycle_count"] % CONSOLIDATE_EVERY == 0:
#       from consolidate import Consolidator
#       c = Consolidator(self.api_key)
#       knowledge = " ".join(r.get("compress","") for r in feed_results)
#       c.run(knowledge, trigger="yield", state=self.state, obligations=self.obligations)


# ── Standalone ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        api_key = input("Paste your Anthropic API key: ").strip()

    # Use recent high-yield engrams as the new knowledge seed
    # (or pass a string directly)
    if len(sys.argv) > 1:
        new_knowledge = " ".join(sys.argv[1:])
    else:
        # Default: use the most recent node's compress as the new knowledge
        if PROJECTS_IDX.exists():
            nodes = json.loads(PROJECTS_IDX.read_text())
            new_knowledge = nodes[0].get("compress", "") if nodes else ""
        else:
            new_knowledge = "substrate independence, scale invariance, Freed's Law, entropy, recursion, criticality"

    state   = json.load(open(FREED_DIR / "FREED_state.json"))
    obligs  = json.load(open(FREED_DIR / "FREED_obligations.json"))

    c = Consolidator(api_key)
    c.run(new_knowledge, trigger="manual", state=state, obligations=obligs)
