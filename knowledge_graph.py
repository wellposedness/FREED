"""
FREED — Knowledge Graph
Typed edges between papers/feeds, invariants, and obligations.

When L7 processes a FEED and produces output like:
  "confirms INV_094"  →  stores: paper → INV_094 (type: confirms)
  "advances O44"      →  stores: paper → O44     (type: advances)
  "refutes INV_097"   →  stores: paper → INV_097 (type: refutes)

The graph accumulates over time and lets CONSOLIDATE report the actual
evidence structure: which invariants have been confirmed how many times
and by which independent sources.

Graph file: FREED_graph.json (alongside FREED_state.json)
"""

import json
import math
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

FREED_DIR   = Path(__file__).parent
GRAPH_FILE  = FREED_DIR / "FREED_graph.json"

# ─── Edge types ───────────────────────────────────────────────────────────────
EDGE_TYPES = ("confirms", "refutes", "advances", "resolves", "extends", "supports", "contradicts")

# ─── Extraction patterns ──────────────────────────────────────────────────────
# Scans kernel output text (all fields) for typed relationships to named nodes.
# INV_\d+ = genome invariants, O\d+ = obligations, INV_\w+ = named invariants.
_NODE_PATTERN = r'(?:INV_\d+|INV_[A-Z]+\d*|O\d+)'

_EDGE_PATTERNS = [
    # "confirms INV_094"  — NOT preceded by "not", "no", "don't", "doesn't", "cannot"
    (re.compile(rf'(?<!not )\b(confirms?)\s+({_NODE_PATTERN})', re.I),        'confirms'),
    # "refutes INV_094"
    (re.compile(rf'(?<!not )\b(refutes?)\s+({_NODE_PATTERN})', re.I),         'refutes'),
    # "advances O44"
    (re.compile(rf'(?<!not )\b(advances?)\s+({_NODE_PATTERN})', re.I),        'advances'),
    # "resolves O28"
    (re.compile(rf'(?<!not )\b(resolves?|closes?)\s+({_NODE_PATTERN})', re.I),'resolves'),
    # "extends INV_094"
    (re.compile(rf'(?<!not )\b(extends?)\s+({_NODE_PATTERN})', re.I),         'extends'),
    # "supports INV_094"
    (re.compile(rf'(?<!not )\b(supports?)\s+({_NODE_PATTERN})', re.I),        'supports'),
    # "contradicts INV_097"
    (re.compile(rf'(?<!not )\b(contradicts?)\s+({_NODE_PATTERN})', re.I),     'contradicts'),
    # "confirms the invariant INV_094"
    (re.compile(rf'(?<!not )\bconfirms?\s+(?:the\s+)?(?:invariant\s+)?({_NODE_PATTERN})', re.I), 'confirms'),
    # "advances obligation O44"
    (re.compile(rf'(?<!not )\badvances?\s+(?:the\s+)?(?:obligation\s+)?({_NODE_PATTERN})', re.I), 'advances'),
]


def extract_edges(kernel_output: dict, source_url: str, source_title: str = "") -> list:
    """
    Scan all kernel output fields for typed edge claims.

    Returns list of edge dicts:
      { from, from_title, to, type, context, timestamp }
    """
    # Scan all text-bearing kernel fields
    text_fields = ["perceive", "represent", "predict", "compare",
                   "adjust", "compress", "next", "raw"]
    full_text = " ".join(
        str(kernel_output.get(f, "")) for f in text_fields
    )

    ts = datetime.now(timezone.utc).isoformat()
    edges = []
    seen  = set()   # deduplicate (source, target, type) within one feed

    for pattern, edge_type in _EDGE_PATTERNS:
        for m in pattern.finditer(full_text):
            # The node ID is in the last capturing group
            node_id = m.group(m.lastindex).upper()
            key = (source_url, node_id, edge_type)
            if key in seen:
                continue
            seen.add(key)

            # Grab a short context window around the match for provenance
            start = max(0, m.start() - 60)
            end   = min(len(full_text), m.end() + 60)
            context = full_text[start:end].replace('\n', ' ').strip()

            edges.append({
                "from":       source_url,
                "from_title": source_title[:80],
                "to":         node_id,
                "type":       edge_type,
                "context":    context,
                "timestamp":  ts,
            })

    return edges


# ─── RangeEn (Range Entropy) ─────────────────────────────────────────────────
# Amplitude-robust complexity scoring for engram embedding similarity
# distributions. Uses range-normalized tolerance (max−min) instead of
# standard-deviation-normalized tolerance, making cluster complexity
# scores robust to non-stationary amplitude shifts across feed batches.
#
# Reference: Omidvarnia et al., "Range Entropy: A Bridge between
# Approximate Entropy and Sample Entropy" (Entropy, 2018).
# ─────────────────────────────────────────────────────────────────────────────

def _chebyshev_distance(vec_a, vec_b):
    # type: (List[float], List[float]) -> float
    """Chebyshev (L-infinity) distance between two equal-length vectors."""
    return max(abs(a - b) for a, b in zip(vec_a, vec_b))


def _build_embedded_vectors(time_series, m):
    # type: (List[float], int) -> List[List[float]]
    """
    Build delay-embedding vectors of dimension m from a 1-D time series.
    For a series of length N, produces N - m + 1 vectors.
    """
    n = len(time_series)
    if n < m:
        return []
    return [time_series[i:i + m] for i in range(n - m + 1)]


def range_entropy(time_series, m=2, r=0.3):
    # type: (List[float], int, float) -> float
    """
    Compute Range Entropy (RangeEn) of a 1-D time series.

    RangeEn uses range-based (max−min) tolerance normalization instead of
    standard-deviation normalization, making it robust to non-stationary
    amplitude shifts across feed batches.

    Parameters
    ----------
    time_series : list of float
        The input signal (e.g. embedding similarity scores within a cluster).
        Must have length >= m + 2.
    m : int
        Embedding dimension (default 2). Template length for pattern matching.
    r : float
        Tolerance fraction applied to the signal range (default 0.3).
        The absolute tolerance is r * (max - min) of the time series.

    Returns
    -------
    float
        The RangeEn value. Higher values indicate greater complexity/irregularity.
        Returns 0.0 for degenerate inputs (constant signal, too short, etc.).
        Returns float('inf') if no template matches found at dimension m+1
        but matches exist at dimension m (maximally irregular).

    Notes
    -----
    - Unlike SampEn which normalizes tolerance by std(signal), RangeEn
      normalizes by range(signal) = max(signal) - min(signal).
    - This makes RangeEn invariant to additive DC shifts and more robust
      to slow amplitude drift (common when embedding magnitudes shift
      across different feed ingestion batches).
    - Computational complexity is O(N^2 * m) where N = len(time_series).
    """
    n = len(time_series)
    if n < m + 2:
        return 0.0

    # Range-based tolerance: use (max - min) instead of std
    sig_max = max(time_series)
    sig_min = min(time_series)
    sig_range = sig_max - sig_min

    if sig_range == 0.0:
        # Constant signal — zero complexity
        return 0.0

    tolerance = r * sig_range

    # Count template matches for embedding dimensions m and m+1
    # Following SampEn convention: count pairs (i, j) with i != j
    counts = []  # type: List[int]
    for dim in (m, m + 1):
        vectors = _build_embedded_vectors(time_series, dim)
        num_vectors = len(vectors)
        if num_vectors < 2:
            counts.append(0)
            continue

        match_count = 0
        for i in range(num_vectors):
            for j in range(i + 1, num_vectors):
                if _chebyshev_distance(vectors[i], vectors[j]) <= tolerance:
                    match_count += 1
        counts.append(match_count)

    count_m = counts[0]      # B^m(r) — matches at dimension m
    count_m1 = counts[1]     # A^m(r) — matches at dimension m+1

    if count_m == 0:
        # No matches even at dimension m — return 0 (undefined / no structure)
        return 0.0

    if count_m1 == 0:
        # Matches at m but none at m+1 — maximally irregular
        return float('inf')

    # RangeEn = -ln(A / B) — same formula as SampEn but with range tolerance
    return -math.log(count_m1 / count_m)


def range_entropy_from_embeddings(embeddings, m=2, r=0.3, metric="cosine"):
    # type: (List[List[float]], int, float, str) -> float
    """
    Compute RangeEn complexity score for a cluster of engram embeddings.

    Converts a set of embedding vectors into a pairwise similarity time series,
    then computes RangeEn on that series. This gives an amplitude-robust
    complexity score for a knowledge cluster.

    Parameters
    ----------
    embeddings : list of list of float
        The embedding vectors in a cluster. Each is a list/tuple of floats.
        Need at least 3 embeddings to produce a meaningful similarity series.
    m : int
        Embedding dimension for RangeEn (default 2).
    r : float
        Tolerance fraction of signal range (default 0.3).
    metric : str
        Similarity metric: "cosine" (default) or "euclidean".

    Returns
    -------
    float
        RangeEn complexity score for the cluster. Higher = more complex/novel
        similarity structure. 0.0 for degenerate clusters.
    """
    n = len(embeddings)
    if n < 3:
        return 0.0

    # Build pairwise similarity series (upper triangle, row-major order)
    similarities = []  # type: List[float]
    for i in range(n):
        for j in range(i + 1, n):
            vec_a = embeddings[i]
            vec_b = embeddings[j]
            if metric == "cosine":
                dot = sum(a * b for a, b in zip(vec_a, vec_b))
                norm_a = math.sqrt(sum(a * a for a in vec_a))
                norm_b = math.sqrt(sum(b * b for b in vec_b))
                if norm_a == 0.0 or norm_b == 0.0:
                    sim = 0.0
                else:
                    sim = dot / (norm_a * norm_b)
                similarities.append(sim)
            elif metric == "euclidean":
                dist = math.sqrt(sum((a - b) ** 2 for a, b in zip(vec_a, vec_b)))
                # Convert distance to similarity (bounded transformation)
                similarities.append(1.0 / (1.0 + dist))
            else:
                raise ValueError(f"Unknown metric: {metric!r}. Use 'cosine' or 'euclidean'.")

    if len(similarities) < m + 2:
        return 0.0

    return range_entropy(similarities, m=m, r=r)


def cluster_complexity_scores(clusters):
    # type: (Dict[str, List[List[float]]]) -> Dict[str, dict]
    """
    Compute RangeEn complexity scores for multiple knowledge clusters.

    Parameters
    ----------
    clusters : dict
        Mapping of cluster_id → list of embedding vectors (each a list of float).

    Returns
    -------
    dict
        Mapping of cluster_id → {
            "range_entropy": float,
            "n_embeddings": int,
            "n_pairs": int,
            "complexity_label": str  — one of "degenerate", "low", "medium", "high", "max"
        }
    """
    results = {}  # type: Dict[str, dict]
    for cluster_id, embeddings in clusters.items():
        n = len(embeddings)
        n_pairs = n * (n - 1) // 2
        ren = range_entropy_from_embeddings(embeddings)

        # Classify complexity
        if ren == 0.0:
            label = "degenerate"
        elif ren == float('inf'):
            label = "max"
        elif ren < 0.3:
            label = "low"
        elif ren < 1.0:
            label = "medium"
        else:
            label = "high"

        results[cluster_id] = {
            "range_entropy": ren,
            "n_embeddings": n,
            "n_pairs": n_pairs,
            "complexity_label": label,
        }
    return results


class KnowledgeGraph:
    """
    Persistent typed-edge graph.
    Loaded from FREED_graph.json on demand, flushed after each write.
    """

    def __init__(self):
        self._edges = []        # paper→invariant/obligation edges
        self._node_edges = []   # node→node structural edges (shares_invariant)
        self._loaded = False
        self._cluster_embeddings = defaultdict(list)  # type: Dict[str, List[List[float]]]

    # ── Persistence ──────────────────────────────────────────────────────────

    def load(self):
        if GRAPH_FILE.exists():
            try:
                data = json.loads(GRAPH_FILE.read_text())
                self._edges = data.get("edges", [])
                self._node_edges = data.get("node_edges", [])
            except (json.JSONDecodeError, KeyError):
                self._edges = []
                self._node_edges = []
        self._loaded = True

    def save(self):
        GRAPH_FILE.write_text(json.dumps(
            {"edges": self._edges, "node_edges": self._node_edges},
            indent=2,
            ensure_ascii=False,
        ))

    def _ensure_loaded(self):
        if not self._loaded:
            self.load()

    # ── Recording ────────────────────────────────────────────────────────────

    def record_feed(self, kernel_output: dict, source_url: str,
                    source_title: str = "") -> list:
        """
        Extract edges from a kernel output and append them to the graph.
        Returns the list of new edges added (may be empty).
        """
        self._ensure_loaded()
        new_edges = extract_edges(kernel_output, source_url, source_title)
        if new_edges:
            self._edges.extend(new_edges)
            self.save()
            print(f"[GRAPH] {len(new_edges)} edge(s) recorded → "
                  f"{', '.join(e['to'] + ':' + e['type'] for e in new_edges)}")
        return new_edges

    def record_node_edge(self, node_a_id, node_b_id, edge_type, invariant_text=""):
        # type: (str, str, str, str) -> None
        """Record a structural edge between two project nodes (e.g. shares_invariant)."""
        self._ensure_loaded()
        # Deduplicate by (from, to, type, invariant prefix)
        inv_key = invariant_text[:60]
        for e in self._node_edges:
            if (e.get("from") == node_a_id and e.get("to") == node_b_id
                    and e.get("type") == edge_type
                    and e.get("invariant", "")[:60] == inv_key):
                return
        ts = datetime.now(timezone.utc).isoformat()
        self._node_edges.append({
            "from":      node_a_id,
            "to":        node_b_id,
            "type":      edge_type,
            "invariant": invariant_text[:120],
            "timestamp": ts,
        })
        self.save()
        print(f"[GRAPH] node-edge: {node_a_id[:25]} --{edge_type}--> {node_b_id[:25]}")

    # ── Cluster Embedding Management ─────────────────────────────────────────

    def register_embedding(self, cluster_id, embedding):
        # type: (str, List[float]) -> None
        """
        Register an engram embedding vector under a knowledge cluster.
        Used for RangeEn complexity scoring.
        """
        self._cluster_embeddings[cluster_id].append(list(embedding))

    def register_embeddings(self, cluster_id, embeddings):
        # type: (str, List[List[float]]) -> None
        """Register multiple embeddings for a cluster at once."""
        for emb in embeddings:
            self._cluster_embeddings[cluster_id].append(list(emb))

    def get_cluster_complexity(self, cluster_id, m=2, r=0.3):
        # type: (str, int, float) -> dict
        """
        Compute RangeEn complexity score for a single registered cluster.

        Returns dict with range_entropy, n_embeddings, n_pairs, complexity_label.
        Returns a degenerate result if the cluster has no registered embeddings.
        """
        embeddings = self._cluster_embeddings.get(cluster_id, [])
        result = cluster_complexity_scores({cluster_id: embeddings})
        return result.get(cluster_id, {
            "range_entropy": 0.0,
            "n_embeddings": 0,
            "n_pairs": 0,
            "complexity_label": "degenerate",
        })

    def get_all_cluster_complexities(self, m=2, r=0.3):
        # type: (int, float) -> Dict[str, dict]
        """
        Compute RangeEn complexity scores for all registered clusters.

        Returns dict of cluster_id → complexity info.
        """
        if not self._cluster_embeddings:
            return {}
        return cluster_complexity_scores(dict(self._cluster_embeddings))

    # ── Querying ─────────────────────────────────────────────────────────────

    def edges_for(self, node_id: str) -> list:
        """All edges pointing to or from a given node ID."""
        self._ensure_loaded()
        node_up = node_id.upper()
        return [e for e in self._edges
                if e.get("to", "").upper() == node_up
                or e.get("from", "").upper() == node_up]

    def confirmation_structure(self) -> dict:
        """
        Summarize evidence per target node.

        Returns dict keyed by node_id:
          {
            "confirms":  int,
            "refutes":   int,
            "advances":  int,
            "resolves":  int,
            "extends":   int,
            "supports":  int,
            "contradicts": int,
            "sources":   [list of unique source URLs]
          }
        """
        self._ensure_loaded()
        summary = defaultdict(lambda: {t: 0 for t in EDGE_TYPES} | {"sources": []})

        for e in self._edges:
            target = e.get("to", "")
            etype  = e.get("type", "")
            src    = e.get("from", "")
            if not target or etype not in EDGE_TYPES:
                continue
            summary[target][etype] += 1
            if src not in summary[target]["sources"]:
                summary[target]["sources"].append(src)

        # Sort by total evidence weight (confirms > all others)
        def weight(v):
            return v["confirms"] * 3 + v["supports"] * 2 + v["extends"] + \
                   v["advances"] - v["refutes"] * 2 - v["contradicts"] * 3

        return dict(sorted(summary.items(), key=lambda kv: weight(kv[1]), reverse=True))

    def report(self, top_n: int = 10) -> str:
        """Human-readable confirmation structure report."""
        structure = self.confirmation_structure()
        if not structure:
            node_edge_count = len(self._node_edges) if self._loaded else 0
            base = "(no feed edges recorded yet)"
            if node_edge_count:
                base += f" — {node_edge_count} inter-node edge(s)"
            return base

        lines = [f"Knowledge graph: {len(self._edges)} feed edge(s), {len(self._node_edges)} inter-node edge(s)"]
        for node_id, counts in list(structure.items())[:top_n]:
            confirms    = counts["confirms"]
            refutes     = counts["refutes"]
            advances    = counts["advances"]
            n_sources   = len(counts["sources"])
            parts = []
            if confirms:   parts.append(f"{confirms}× confirmed")
            if advances:   parts.append(f"{advances}× advanced")
            if refutes:    parts.append(f"{refutes}× refuted")
            if not parts:
                total = sum(counts[t] for t in EDGE_TYPES)
                parts.append(f"{total}× referenced")
            lines.append(f"  {node_id:12s} — {', '.join(parts)} ({n_sources} source(s))")

        # Append cluster complexity summary if available
        complexities = self.get_all_cluster_complexities()
        if complexities:
            lines.append("")
            lines.append(f"Cluster complexity (RangeEn): {len(complexities)} cluster(s)")
            sorted_clusters = sorted(
                complexities.items(),
                key=lambda kv: kv[1]["range_entropy"] if kv[1]["range_entropy"] != float('inf') else 999.0,
                reverse=True,
            )
            for cid, info in sorted_clusters[:top_n]:
                ren = info["range_entropy"]
                ren_str = "∞" if ren == float('inf') else f"{ren:.4f}"
                lines.append(
                    f"  {cid:12s} — RangeEn={ren_str}  "
                    f"({info['complexity_label']}, {info['n_embeddings']} engrams)"
                )

        return "\n".join(lines)


# ─── Convenience singleton ────────────────────────────────────────────────────
_graph = None

def get_graph() -> KnowledgeGraph:
    """Return the process-level graph singleton (lazy-loaded)."""
    global _graph
    if _graph is None:
        _graph = KnowledgeGraph()
        _graph.load()
    return _graph


# ─── Quick test ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Simulate a kernel output containing edge claims
    fake_output = {
        "adjust": (
            "This confirms INV_094 — entropy asymmetry predicts intelligence across "
            "two independent EEG datasets. Advances O28 significantly. "
            "Does not refute INV_097."
        ),
        "compress": "Entropy asymmetry ratio is a confirmed predictor of neural efficiency (INV_094).",
        "next": "Resolve O28 by computing EAR composite from raw EEG dataset.",
    }

    edges = extract_edges(fake_output, "https://arxiv.org/abs/2501.12345", "Test Paper")
    print(f"Extracted {len(edges)} edge(s):")
    for e in edges:
        print(f"  [{e['type']:12s}] {e['from'][-30:]} → {e['to']}")
        print(f"             context: ...{e['context'][:70]}...")

    # Build a small graph
    g = KnowledgeGraph()
    g._edges = edges
    print("\n" + g.report())

    # ── RangeEn demonstration ────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("RangeEn (Range Entropy) demonstration")
    print("=" * 60)

    # Test 1: Basic time series
    import random
    random.seed(42)

    # Constant signal → 0 complexity
    constant = [0.5] * 20
    print(f"\nConstant signal:        RangeEn = {range_entropy(constant):.4f}")

    # Regular periodic signal → low complexity
    periodic = [math.sin(i * 0.5) for i in range(50)]
    print(f"Periodic signal:        RangeEn = {range_entropy(periodic):.4f}")

    # Random signal → high complexity
    noisy = [random.gauss(0, 1) for _ in range(50)]
    print(f"Random signal:          RangeEn = {range_entropy(noisy):.4f}")

    # Test 2: Amplitude robustness — same signal, different amplitudes
    base_signal = [random.gauss(0, 1) for _ in range(50)]
    scaled_10x = [x * 10.0 for x in base_signal]
    shifted = [x + 100.0 for x in base_signal]
    scaled_shifted = [x * 10.0 + 100.0 for x in base_signal]

    print(f"\nAmplitude robustness test:")
    print(f"  Base signal:          RangeEn = {range_entropy(base_signal):.4f}")
    print(f"  Scaled 10×:           RangeEn = {range_entropy(scaled_10x):.4f}")
    print(f"  Shifted +100:         RangeEn = {range_entropy(shifted):.4f}")
    print(f"  Scaled 10× + 100:     RangeEn = {range_entropy(scaled_shifted):.4f}")

    # Test 3: Cluster embeddings
    print(f"\nCluster embedding complexity:")
    dim = 8
    cluster_uniform = [[random.uniform(-1, 1) for _ in range(dim)] for _ in range(10)]
    cluster_similar = [[0.5 + random.gauss(0, 0.01) for _ in range(dim)] for _ in range(10)]

    ren_diverse = range_entropy_from_embeddings(cluster_uniform)
    ren_similar = range_entropy_from_embeddings(cluster_similar)
    print(f"  Diverse cluster:      RangeEn = {ren_diverse:.4f}")
    print(f"  Similar cluster:      RangeEn = {ren_similar:.4f}")

    # Test 4: Graph integration
    print(f"\nGraph cluster complexity integration:")
    g2 = KnowledgeGraph()
    g2._edges = edges
    g2.register_embeddings("INV_094", cluster_uniform)
    g2.register_embeddings("O28", cluster_similar)
    print(g2.report())