"""
DMN — Default Mode Network for FREED.

Fires once per dead zone (1:00am–6:15am local), around 2:30am.
No external input. Pure internal consolidation:

  1. CROSS-CONNECT  — propose directional typed edges between node pairs that
                      are connected only by flat 'shares_invariant' edges but
                      whose topic words diverge (topically distant despite linked).

  2. INTERNAL-OBLIGE — generate new obligations from tensions in the existing
                       knowledge structure: unfired implications between resolved
                       obligations, gaps in the invariant frontier, obligatory
                       consequences of confirmed claims.

New edge types (none existed before — all prior edges are 'shares_invariant'):
  operationalizes     A provides the operational mechanism for B's abstract claim
  bounds_above        A sets an upper limit or constraint on B's claims
  challenges          A's methodology or premises challenge B's assumptions
  depends_on          A's validity requires B's claims to hold
  contradicts         A and B make conflicting predictions
  completes_via       A would directly resolve B's open question
  substrate_of        A is the physical substrate instantiation of B's abstract pattern

Logs every DMN run to FREED_log/dmn_{date}.jsonl.
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path

FREED_DIR = Path(__file__).parent
LOG_DIR   = FREED_DIR / "FREED_log"

OPUS_MODEL  = "claude-opus-4-6"
MAX_PAIRS   = 4    # node pairs to analyze per DMN run
MAX_OBLIGS  = 3    # max new obligations to generate per DMN run

# Nodes that are meta/admin — skip for cross-connect
_SKIP_NODES = {
    "website_review_and_demo_strategy_for_rsa",
}

# Valid directed edge types the DMN can propose
DMN_EDGE_TYPES = {
    "operationalizes", "bounds_above", "challenges",
    "depends_on", "contradicts", "completes_via", "substrate_of",
}

_CROSS_CONNECT_SYSTEM = """\
You are FREED's Default Mode Network — overnight internal consolidation without external input.

Your task: identify DIRECTIONAL relationships between two knowledge nodes that go deeper \
than their existing 'shares_invariant' connection.

Valid edge types:
  operationalizes  — A provides the operational mechanism for B's abstract claim
  bounds_above     — A sets an upper limit or constraint on B's claims
  challenges       — A's methodology or premises challenge B's assumptions
  depends_on       — A's validity requires B's claims to hold
  contradicts      — A and B make conflicting predictions
  completes_via    — A would directly resolve B's main open question
  substrate_of     — A is the physical substrate instantiation of B's abstract pattern

For each valid directional relationship you find, emit exactly one line:
EDGE: [source_id] --[type]--> [target_id] : [one sentence explaining why]

Only emit EDGE lines for relationships you are confident exist. \
If no directional relationship is warranted, emit nothing.
Do not repeat the shares_invariant edge type — it is already recorded."""

_OBLIGE_SYSTEM = """\
You are FREED's Default Mode Network. No new papers tonight.

Your task: generate new obligations from the INTERNAL STRUCTURE of the knowledge graph — \
gaps, unfired implications, obligatory consequences of confirmed claims, and tensions \
between obligations. Do not invent from nothing; derive from what is already here.

For each new obligation emit exactly:
OBLIGATION: [precise scientific statement, testable]
RATIONALE: [which internal tension, confirmed claim, or gap generates this]
RESOLVES_VIA: [specific condition that would close it — an experiment, proof, or dataset]

Generate at most """ + str(MAX_OBLIGS) + """ obligations. Quality over quantity. \
If the internal structure generates no new obligations, emit nothing."""


def _word_set(text):
    stop = {"the", "a", "an", "of", "in", "and", "to", "is", "are", "this",
            "that", "for", "with", "as", "by", "on", "at", "be", "from", "or"}
    return {w.lower() for w in re.split(r'\W+', text) if len(w) > 3} - stop


def _topical_overlap(summary_a, summary_b):
    wa = _word_set(summary_a)
    wb = _word_set(summary_b)
    if not wa or not wb:
        return 1.0
    return len(wa & wb) / len(wa | wb)


def _select_candidate_pairs(projects, node_edges):
    """
    Return up to MAX_PAIRS pairs ranked by:
      lowest topical-word overlap  (topically distant)
      while having at least one shares_invariant edge (linked)
    Skip meta/admin nodes.
    """
    # Index projects by ID prefix (graph uses truncated IDs)
    by_id = {}
    for p in projects:
        pid = p.get("id", "")
        by_id[pid[:40]] = p  # same truncation as graph keys

    # Build pair → invariant text index
    pair_inv = {}
    for e in node_edges:
        if e.get("type") != "shares_invariant":
            continue
        a, b = e.get("from", ""), e.get("to", "")
        key  = tuple(sorted([a, b]))
        if key not in pair_inv:
            pair_inv[key] = e.get("invariant", "")

    # Score each pair
    scored = []
    for (a, b), inv in pair_inv.items():
        if a in _SKIP_NODES or b in _SKIP_NODES:
            continue
        pa = next((p for pid, p in by_id.items() if a.startswith(pid[:30]) or pid.startswith(a[:30])), None)
        pb = next((p for pid, p in by_id.items() if b.startswith(pid[:30]) or pid.startswith(b[:30])), None)
        if not pa or not pb:
            continue
        overlap = _topical_overlap(
            pa.get("summary", pa.get("title", "")),
            pb.get("summary", pb.get("title", "")),
        )
        scored.append((overlap, a, b, pa, pb, inv))

    scored.sort(key=lambda x: x[0])  # ascending overlap = most distant first
    return scored[:MAX_PAIRS]


def _parse_edges(text, valid_ids):
    """Parse EDGE: lines from model output."""
    edges = []
    for line in text.splitlines():
        m = re.match(
            r'EDGE:\s*(\S+)\s*--(\w+)-->\s*(\S+)\s*:\s*(.+)',
            line.strip()
        )
        if not m:
            continue
        src, etype, tgt, reason = m.group(1), m.group(2), m.group(3), m.group(4).strip()
        if etype not in DMN_EDGE_TYPES:
            continue
        edges.append({
            "from":    src,
            "to":      tgt,
            "type":    etype,
            "reason":  reason[:200],
        })
    return edges


def _parse_obligations(text):
    """Parse OBLIGATION/RATIONALE/RESOLVES_VIA blocks from model output."""
    obligs = []
    blocks = re.split(r'\n(?=OBLIGATION:)', text.strip())
    for block in blocks:
        om = re.search(r'OBLIGATION:\s*(.+?)(?=\nRATIONALE:|$)', block, re.S)
        rm = re.search(r'RATIONALE:\s*(.+?)(?=\nRESOLVES_VIA:|$)', block, re.S)
        vm = re.search(r'RESOLVES_VIA:\s*(.+)', block, re.S)
        if om:
            obligs.append({
                "statement":          om.group(1).strip(),
                "rationale":          rm.group(1).strip() if rm else "",
                "resolution_criterion": vm.group(1).strip() if vm else "",
                "source":             "dmn",
            })
    return obligs[:MAX_OBLIGS]


class DMNAgent:
    """Runs the dead-zone Default Mode Network consolidation."""

    def __init__(self, api_key):
        self._api_key = api_key

    def _call(self, system, user_msg, max_tokens=1500):
        import anthropic
        client = anthropic.Anthropic(api_key=self._api_key)
        resp   = client.messages.create(
            model=OPUS_MODEL,
            max_tokens=max_tokens,
            system=system,
            messages=[{"role": "user", "content": user_msg}],
        )
        return resp.content[0].text if resp.content else ""

    def run(self, graph, obligations, state):
        """
        Execute one DMN pass. Modifies graph in-place (new node-edges).
        Returns dict with summary of what was produced.
        """
        LOG_DIR.mkdir(exist_ok=True)
        date_str = datetime.now().strftime("%Y-%m-%d")
        log_path = LOG_DIR / f"dmn_{date_str}.jsonl"

        projects_path = FREED_DIR / "docs" / "projects.json"
        projects = []
        if projects_path.exists():
            projects = json.loads(projects_path.read_text(encoding="utf-8"))

        graph._ensure_loaded()
        node_edges = graph._node_edges  # direct access to internal list
        result     = {
            "ts":                datetime.now(timezone.utc).isoformat(),
            "new_edges":         [],
            "new_obligations":   [],
            "pairs_analyzed":    0,
        }

        # ── 1. CROSS-CONNECT ────────────────────────────────────────────────
        print("\n[DMN] Cross-connect phase...")
        candidates = _select_candidate_pairs(projects, node_edges)
        result["pairs_analyzed"] = len(candidates)

        all_node_ids = set()
        for e in node_edges:
            all_node_ids.add(e.get("from", ""))
            all_node_ids.add(e.get("to", ""))

        for overlap, a_id, b_id, pa, pb, shared_inv in candidates:
            pa_title   = pa.get("title", a_id)
            pb_title   = pb.get("title", b_id)
            pa_summary = (pa.get("summary") or "")[:300]
            pb_summary = (pb.get("summary") or "")[:300]

            user_msg = (
                f"Node A — {a_id}\n"
                f"Title: {pa_title}\n"
                f"Summary: {pa_summary}\n\n"
                f"Node B — {b_id}\n"
                f"Title: {pb_title}\n"
                f"Summary: {pb_summary}\n\n"
                f"Existing connection: shares_invariant — \"{shared_inv[:120]}\"\n\n"
                f"Topical overlap score: {overlap:.2f} (low = topically distant)\n\n"
                f"Identify directional relationships beyond shares_invariant, if any exist."
            )

            print(f"[DMN] Analyzing: {a_id[:30]} × {b_id[:30]} (overlap={overlap:.2f})")
            try:
                raw = self._call(_CROSS_CONNECT_SYSTEM, user_msg, max_tokens=400)
            except Exception as e:
                print(f"[DMN] Cross-connect error: {e}")
                continue

            edges = _parse_edges(raw, all_node_ids)
            for edge in edges:
                graph.record_node_edge(
                    edge["from"], edge["to"],
                    edge["type"],
                    invariant_text=edge["reason"],
                )
                result["new_edges"].append(edge)
                print(f"[DMN]   {edge['from'][:25]} --{edge['type']}--> {edge['to'][:25]}")

        # ── 2. INTERNAL-OBLIGE ──────────────────────────────────────────────
        print("\n[DMN] Internal obligation generation...")
        open_obs   = [o for o in obligations if o.get("status") == "open"]
        resolved   = [o for o in obligations if o.get("status") == "resolved"]
        partial    = [o for o in obligations if o.get("status") == "partial"]

        ob_summary = "OPEN:\n" + "\n".join(
            f"  {o['id']}: {o.get('statement','')[:90]}" for o in open_obs[:10]
        )
        ob_summary += "\n\nPARTIAL:\n" + "\n".join(
            f"  {o['id']}: {o.get('statement','')[:90]}" for o in partial[:5]
        )
        ob_summary += "\n\nRECENTLY RESOLVED:\n" + "\n".join(
            f"  {o['id']}: {o.get('statement','')[:90]}" for o in resolved[-5:]
        )

        edge_types_present = {}
        for e in node_edges:
            t = e.get("type", "unknown")
            edge_types_present[t] = edge_types_present.get(t, 0) + 1

        graph_summary = (
            f"{len(all_node_ids)} nodes, {len(node_edges)} node-edges "
            f"(types: {edge_types_present}), "
            f"{len(graph._edges)} feed-edges"
        )

        genome_symbols = []
        sym_file = FREED_DIR / "genome_symbols.json"
        if sym_file.exists():
            sym_data = json.loads(sym_file.read_text(encoding="utf-8"))
            genome_symbols = [k for k in sym_data if k != "_meta"]

        user_msg = (
            f"CURRENT STATE:\n"
            f"Generation: {state.get('generation', '?')}, "
            f"Coherence: {state.get('coherence', '?')}\n\n"
            f"OBLIGATIONS:\n{ob_summary}\n\n"
            f"GRAPH TOPOLOGY:\n{graph_summary}\n\n"
            f"GENOME SYMBOLS ({len(genome_symbols)}): {', '.join(genome_symbols[:12])}\n\n"
            f"Looking at this internal structure: what new obligations arise from the "
            f"relationships, gaps, and unfired implications already present here?"
        )

        try:
            raw = self._call(_OBLIGE_SYSTEM, user_msg, max_tokens=800)
        except Exception as e:
            print(f"[DMN] Internal-oblige error: {e}")
            raw = ""

        new_obligs = _parse_obligations(raw)
        result["new_obligations"] = new_obligs

        if new_obligs:
            print(f"[DMN] {len(new_obligs)} internal obligation(s) generated:")
            for ob in new_obligs:
                print(f"  → {ob['statement'][:80]}")
        else:
            print("[DMN] No new obligations from internal state.")

        # ── LOG ─────────────────────────────────────────────────────────────
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(result) + "\n")

        print(f"\n[DMN] Complete. {len(result['new_edges'])} edge(s), "
              f"{len(result['new_obligations'])} obligation(s).")
        return result
