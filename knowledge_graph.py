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

Context Tags (context_tag field):
  Following the insight that complexity/entropy claims are meaningless without
  specifying the coarse-graining or observer context under which they hold,
  every graph node edge carries an optional `context_tag` field. This enables:
  - Context-aware deduplication: two claims about INV_094 under different
    coarse-graining levels are tracked separately, not collapsed.
  - Context-aware contradiction detection: "confirms INV_094" under context A
    and "refutes INV_094" under context B is not a true contradiction.
  - Explicit annotation of the observer/coarse-graining level for all
    complexity and entropy related claims.

  The context_tag is a short string describing the coarse-graining level,
  observer frame, or measurement context. Examples:
    "macro:thermodynamic", "micro:molecular", "neural:EEG-alpha",
    "info:Shannon-bits", "kolmogorov:UTM-prefix", "coarse:cell-average"
  When absent or None, the claim is treated as context-unspecified (universal).

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
EDGE_TYPES = ("confirms", "refutes", "advances", "resolves", "extends", "supports", "contradicts", "challenges")

# ─── Symbol Emergence Lineage ────────────────────────────────────────────────
# Tracks the dynamic trajectory of a concept from grounded sensorimotor origin
# through to socially-stabilized symbol. Rather than a static "grounded/ungrounded"
# label, each concept-node can carry a lineage recording its emergence phase
# transitions over time. Inspired by the symbol emergence problem framing that
# treats symbol systems as adaptively and dynamically changing, not statically
# assigned groundings.
#
# Phases follow the developmental trajectory identified in the symbol emergence
# literature:
#   sensorimotor  → perceptual concept formed from raw sensor/data grounding
#   categorical   → concept clustered into a category via repeated exposure
#   interactive   → concept refined through inter-agent or cross-feed interaction
#   conventionalized → concept stabilized by social/community consensus
#   semiotic      → concept participates in a full sign system with compositional use

EMERGENCE_PHASES = (
    "sensorimotor",
    "categorical",
    "interactive",
    "conventionalized",
    "semiotic",
)

# Numeric ordering for phase comparison
_PHASE_ORDER = {phase: idx for idx, phase in enumerate(EMERGENCE_PHASES)}

# Edge type for lineage transitions
LINEAGE_EDGE_TYPE = "emergence_transition"

# Patterns to detect emergence-related language in kernel output
_EMERGENCE_PHASE_PATTERNS = [
    (re.compile(r'\b(?:grounded?\s+in|sensorimotor|perceptual\s+origin|raw\s+data)\b', re.I),
     "sensorimotor"),
    (re.compile(r'\b(?:categori[sz](?:ed|ation)|cluster(?:ed|ing)|prototype)\b', re.I),
     "categorical"),
    (re.compile(r'\b(?:inter[-\s]?agent|cross[-\s]?feed|communicat(?:ed|ive)|negotiat(?:ed|ion)|interactive)\b', re.I),
     "interactive"),
    (re.compile(r'\b(?:convention(?:al(?:ized)?)?|consensus|standard(?:ized)?|widely\s+accepted|established\s+term)\b', re.I),
     "conventionalized"),
    (re.compile(r'\b(?:semiotic|sign\s+system|compositional|symbolic\s+(?:system|use)|language[-\s]?like)\b', re.I),
     "semiotic"),
]

# Pattern to extract concept identifiers mentioned alongside emergence language
_CONCEPT_ID_PATTERN = re.compile(
    r'(?:concept|symbol|term|notion|idea)\s+["\']?([A-Za-z_][A-Za-z0-9_]{2,})["\']?',
    re.I
)

# ─── Context-sensitive keywords ───────────────────────────────────────────────
# Used to auto-detect context tags from surrounding text when not explicitly
# provided. Maps regex patterns to canonical context_tag values.
_CONTEXT_HINT_PATTERNS = [
    (re.compile(r'\b(?:thermodynamic|macro[-\s]?scopic|bulk)\b', re.I),          "macro:thermodynamic"),
    (re.compile(r'\b(?:micro[-\s]?scopic|molecular|atomic)\b', re.I),            "micro:molecular"),
    (re.compile(r'\b(?:Shannon\s+entropy|information[- ]theoretic|bits)\b', re.I), "info:Shannon-bits"),
    (re.compile(r'\b(?:Kolmogorov|algorithmic\s+complexity|UTM)\b', re.I),        "kolmogorov:algorithmic"),
    (re.compile(r'\b(?:coarse[- ]grain(?:ed|ing)?|renormali[sz]ation)\b', re.I), "coarse:renormalization"),
    (re.compile(r'\b(?:EEG|neural|brain|cortical)\b', re.I),                     "neural:EEG"),
    (re.compile(r'\b(?:observer[- ]dependent|subjective|agent[- ]relative)\b', re.I), "observer:agent-relative"),
    (re.compile(r'\b(?:computational|Turing|halting)\b', re.I),                  "computational:Turing"),
    (re.compile(r'\b(?:statistical\s+mechanic|Boltzmann|partition\s+function)\b', re.I), "statmech:Boltzmann"),
    (re.compile(r'\b(?:quantum|wave[-\s]?function|density\s+matrix)\b', re.I),   "quantum:wavefunction"),
]

# Keywords that signal the claim is about complexity/entropy and thus
# particularly needs context annotation
_COMPLEXITY_KEYWORDS = re.compile(
    r'\b(?:complexity|entropy|information|coarse[- ]grain|emergence|disorder|randomness|compressibility)\b',
    re.I
)

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
    # "challenges INV_094" — mandatory falsification output from FEED phase
    (re.compile(rf'(?<!not )\b(challenges?)\s+({_NODE_PATTERN})', re.I),      'challenges'),
    # "confirms the invariant INV_094"
    (re.compile(rf'(?<!not )\bconfirms?\s+(?:the\s+)?(?:invariant\s+)?({_NODE_PATTERN})', re.I), 'confirms'),
    # "advances obligation O44"
    (re.compile(rf'(?<!not )\badvances?\s+(?:the\s+)?(?:obligation\s+)?({_NODE_PATTERN})', re.I), 'advances'),
]


def infer_context_tag(text_window):
    # type: (str) -> Optional[str]
    """
    Attempt to infer a context_tag from a text window surrounding an edge claim.

    Scans the text for known context-hint patterns and returns the first
    matching canonical context tag, or None if no context can be inferred.

    Parameters
    ----------
    text_window : str
        A short text excerpt (typically ~120 chars) around the edge claim.

    Returns
    -------
    str or None
        A canonical context tag string, or None if context is unspecified.
    """
    for pattern, tag in _CONTEXT_HINT_PATTERNS:
        if pattern.search(text_window):
            return tag
    return None


def is_complexity_claim(text_window):
    # type: (str) -> bool
    """
    Check whether a text window contains complexity/entropy-related keywords,
    indicating the claim particularly needs context annotation.
    """
    return bool(_COMPLEXITY_KEYWORDS.search(text_window))


def extract_edges(kernel_output, source_url, source_title="", context_tag=None):
    # type: (dict, str, str, Optional[str]) -> list
    """
    Scan all kernel output fields for typed edge claims.

    Parameters
    ----------
    kernel_output : dict
        The kernel output fields to scan.
    source_url : str
        URL of the source paper/feed.
    source_title : str
        Title of the source (truncated to 80 chars).
    context_tag : str or None
        Explicit context tag to apply to all extracted edges. If None,
        the system will attempt to auto-infer context from surrounding text.
        For complexity/entropy claims without inferrable context, the edge
        is flagged with `context_warning: True`.

    Returns list of edge dicts:
      { from, from_title, to, type, context, context_tag, context_warning, timestamp }
    """
    # Scan all text-bearing kernel fields
    text_fields = ["perceive", "represent", "predict", "compare",
                   "adjust", "compress", "next", "raw"]
    full_text = " ".join(
        str(kernel_output.get(f, "")) for f in text_fields
    )

    ts = datetime.now(timezone.utc).isoformat()
    edges = []
    seen  = set()   # deduplicate (source, target, type, context_tag) within one feed

    for pattern, edge_type in _EDGE_PATTERNS:
        for m in pattern.finditer(full_text):
            # The node ID is in the last capturing group
            node_id = m.group(m.lastindex).upper()

            # Grab a short context window around the match for provenance
            start = max(0, m.start() - 60)
            end   = min(len(full_text), m.end() + 60)
            context = full_text[start:end].replace('\n', ' ').strip()

            # Determine context_tag: explicit > auto-inferred > None
            # Use a wider window for context inference
            infer_start = max(0, m.start() - 150)
            infer_end   = min(len(full_text), m.end() + 150)
            infer_window = full_text[infer_start:infer_end]

            if context_tag is not None:
                resolved_tag = context_tag
            else:
                resolved_tag = infer_context_tag(infer_window)

            # Check if this is a complexity-related claim without context
            complexity_claim = is_complexity_claim(infer_window)
            context_warning = complexity_claim and resolved_tag is None

            # Deduplicate by (source, target, type, context_tag)
            key = (source_url, node_id, edge_type, resolved_tag)
            if key in seen:
                continue
            seen.add(key)

            edges.append({
                "from":            source_url,
                "from_title":      source_title[:80],
                "to":              node_id,
                "type":            edge_type,
                "context":         context,
                "context_tag":     resolved_tag,
                "context_warning": context_warning,
                "timestamp":       ts,
            })

    return edges


# ─── Context-aware contradiction detection ────────────────────────────────────

# Edge types that semantically oppose each other
_OPPOSING_TYPES = {
    "confirms": {"refutes", "contradicts"},
    "refutes": {"confirms", "supports"},
    "supports": {"refutes", "contradicts"},
    "contradicts": {"confirms", "supports"},
}


def detect_contradictions(edges, context_aware=True):
    # type: (list, bool) -> List[dict]
    """
    Detect contradictions among a set of edges.

    When context_aware=True (default), two edges about the same target node
    are only considered contradictory if they share the same context_tag
    (or both have context_tag=None). Edges with different context_tags
    represent claims under different coarse-graining levels and are not
    contradictions.

    Parameters
    ----------
    edges : list of dict
        Edge dicts as produced by extract_edges.
    context_aware : bool
        If True, only flag contradictions within the same context.
        If False, flag all opposing edge-type pairs regardless of context.

    Returns
    -------
    list of dict
        Each entry: {
            "node": str,
            "edge_a": dict, "edge_b": dict,
            "type_a": str, "type_b": str,
            "same_context": bool,
            "context_tag": str or None,
            "severity": str  — "true_contradiction" or "cross_context_tension"
        }
    """
    # Group edges by target node
    by_target = defaultdict(list)  # type: Dict[str, list]
    for e in edges:
        target = e.get("to", "")
        if target:
            by_target[target].append(e)

    contradictions = []
    for target, target_edges in by_target.items():
        n = len(target_edges)
        for i in range(n):
            for j in range(i + 1, n):
                ea = target_edges[i]
                eb = target_edges[j]
                ta = ea.get("type", "")
                tb = eb.get("type", "")

                # Check if types oppose
                if ta not in _OPPOSING_TYPES:
                    continue
                if tb not in _OPPOSING_TYPES.get(ta, set()):
                    continue

                ctx_a = ea.get("context_tag")
                ctx_b = eb.get("context_tag")
                same_ctx = (ctx_a == ctx_b)

                if context_aware and not same_ctx:
                    severity = "cross_context_tension"
                else:
                    severity = "true_contradiction"

                if context_aware and not same_ctx:
                    # Under context-aware mode, cross-context tensions are
                    # still reported but not as true contradictions
                    pass

                contradictions.append({
                    "node":          target,
                    "edge_a":        ea,
                    "edge_b":        eb,
                    "type_a":        ta,
                    "type_b":        tb,
                    "same_context":  same_ctx,
                    "context_tag":   ctx_a if same_ctx else None,
                    "severity":      severity,
                })

    return contradictions


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


# ─── Linear Entropy (Information Flow Proxy) ─────────────────────────────────
# Lightweight, analytically tractable proxy for information flow scoring in
# open-system / mixed-representation knowledge graph nodes. Uses the linear
# entropy functional S_L = Tr[ρ] - Tr[ρ²] (and its rate of change) instead
# of the full von Neumann entropy S_vN = -Tr[ρ ln ρ], avoiding costly matrix
# logarithms while preserving sensitivity to purity changes.
#
# For non-Hermitian / open-system dynamics (probability sinks/sources), the
# trace Tr[ρ] is not conserved, so the linear entropy generalizes to:
#   S_L(t) = Tr[ρ(t)] - Tr[ρ(t)²]
# and its rate of change dS_L/dt captures information flow between the system
# and environment.
#
# In the mixed quantum-classical (Wigner-transformed) representation, this
# gives a computationally cheap score for ranking epistemic updates without
# the full logarithmic entropy cost.
#
# Reference: "Open quantum systems with non-Hermitian Hamiltonians —
# linear entropy as indicator of information flow" (paper excerpt above).
# ─────────────────────────────────────────────────────────────────────────────


def _matrix_trace(matrix):
    # type: (List[List[float]]) -> float
    """Compute the trace of a square matrix (sum of diagonal elements)."""
    return sum(matrix[i][i] for i in range(len(matrix)))


def _matrix_multiply(a, b):
    # type: (List[List[float]], List[List[float]]) -> List[List[float]]
    """Multiply two square matrices of the same dimension."""
    n = len(a)
    result = [[0.0] * n for _ in range(n)]
    for i in range(n):
        for j in range(n):
            s = 0.0
            for k in range(n):
                s += a[i][k] * b[k][j]
            result[i][j] = s
    return result


def linear_entropy(density_matrix):
    # type: (List[List[float]]) -> float
    """
    Compute the linear entropy S_L = Tr[ρ] - Tr[ρ²] of a density-matrix-like
    belief representation.

    For a normalized pure state (Tr[ρ]=1, ρ²=ρ), S_L = 0.
    For a maximally mixed state of dimension d, S_L = 1 - 1/d.
    For open systems with non-Hermitian dynamics where Tr[ρ] ≠ 1 (probability
    sinks/sources), S_L captures both the purity change and the trace drift.

    Parameters
    ----------
    density_matrix : list of list of float
        Square matrix representing a density-matrix-like belief state.
        Can be sub-normalized (Tr[ρ] < 1) for open systems with sinks,
        or super-normalized (Tr[ρ] > 1) for systems with sources.

    Returns
    -------
    float
        The linear entropy value. 0.0 for pure states, positive for mixed.
        Can be negative if Tr[ρ²] > Tr[ρ] (highly non-physical but possible
        in numerical belief representations).
    """
    if not density_matrix or not density_matrix[0]:
        return 0.0
    n = len(density_matrix)
    if any(len(row) != n for row in density_matrix):
        return 0.0

    tr_rho = _matrix_trace(density_matrix)
    rho_sq = _matrix_multiply(density_matrix, density_matrix)
    tr_rho_sq = _matrix_trace(rho_sq)

    return tr_rho - tr_rho_sq


def linear_entropy_rate(density_matrix_t0, density_matrix_t1, dt=1.0):
    # type: (List[List[float]], List[List[float]], float) -> float
    """
    Compute the rate of change of linear entropy dS_L/dt as a finite difference.

    This serves as a lightweight proxy for information flow: positive rates
    indicate information flowing out of the system (increasing mixedness /
    decoherence), negative rates indicate information flowing in (purification).

    Parameters
    ----------
    density_matrix_t0 : list of list of float
        Density-matrix-like belief state at time t.
    density_matrix_t1 : list of list of float
        Density-matrix-like belief state at time t + dt.
    dt : float
        Time step (default 1.0). Use actual time difference for physical units.

    Returns
    -------
    float
        dS_L/dt — rate of linear entropy change. Positive = information outflow,
        negative = information inflow (purification).
    """
    if dt == 0.0:
        return 0.0
    s0 = linear_entropy(density_matrix_t0)
    s1 = linear_entropy(density_matrix_t1)
    return (s1 - s0) / dt


def linear_entropy_normalized(density_matrix):
    # type: (List[List[float]]) -> float
    """
    Compute a normalized linear entropy in [0, 1] for comparing nodes.

    Normalizes by Tr[ρ] to handle open-system (non-trace-preserving) cases,
    then scales by d/(d-1) where d is the matrix dimension, so that a
    maximally mixed state maps to 1.0.

    S_L_norm = (d / (d-1)) * (1 - Tr[ρ²] / Tr[ρ]²)

    Parameters
    ----------
    density_matrix : list of list of float
        Square density-matrix-like belief state.

    Returns
    -------
    float
        Normalized linear entropy in [0, 1]. 0 = pure, 1 = maximally mixed.
        Clamped to [0, 1] for robustness.
    """
    if not density_matrix or not density_matrix[0]:
        return 0.0
    n = len(density_matrix)
    if n < 2 or any(len(row) != n for row in density_matrix):
        return 0.0

    tr_rho = _matrix_trace(density_matrix)
    if tr_rho == 0.0:
        return 0.0

    rho_sq = _matrix_multiply(density_matrix, density_matrix)
    tr_rho_sq = _matrix_trace(rho_sq)

    # Purity relative to trace: Tr[ρ²] / Tr[ρ]²
    relative_purity = tr_rho_sq / (tr_rho * tr_rho)

    # Scale so maximally mixed (relative_purity = 1/d) maps to 1.0
    scale = float(n) / float(n - 1)
    s_norm = scale * (1.0 - relative_purity)

    # Clamp to [0, 1] for robustness
    return max(0.0, min(1.0, s_norm))


def belief_vector_to_density_matrix(belief_vector):
    # type: (List[float]) -> List[List[float]]
    """
    Convert a belief/probability vector into a density matrix via outer product.

    This is the standard pure-state construction ρ = |ψ⟩⟨ψ| where |ψ⟩ is
    the belief vector. For mixed states, use a weighted sum of such matrices.

    Parameters
    ----------
    belief_vector : list of float
        A belief/probability amplitude vector of length d.

    Returns
    -------
    list of list of float
        A d×d density matrix (outer product of the vector with itself).
    """
    d = len(belief_vector)
    if d == 0:
        return []
    return [[belief_vector[i] * belief_vector[j] for j in range(d)]
            for i in range(d)]


def score_node_information_flow(belief_states, dt=1.0):
    # type: (List[List[List[float]]], float) -> dict
    """
    Score the information flow for a knowledge graph node given a time series
    of density-matrix-like belief states.

    Computes linear entropy at each timestep and the rate of change, providing
    a lightweight alternative to von Neumann entropy for ranking epistemic
    updates in open-system or mixed-representation nodes.

    Parameters
    ----------
    belief_states : list of list of list of float
        Time-ordered sequence of density matrices representing the node's
        belief state evolution. Each entry is a square matrix.
    dt : float
        Time step between consecutive belief states (default 1.0).

    Returns
    -------
    dict
        {
            "linear_entropies": list of float — S_L at each timestep,
            "normalized_entropies": list of float — normalized S_L at each step,
            "entropy_rates": list of float — dS_L/dt between consecutive steps,
            "mean_entropy": float — average linear entropy,
            "mean_rate": float — average entropy rate (net information flow),
            "max_rate": float — peak information flow magnitude,
            "flow_direction": str — "outflow" (decoherence), "inflow" (purification), or "stable",
            "n_steps": int
        }
    """
    if not belief_states:
        return {
            "linear_entropies": [],
            "normalized_entropies": [],
            "entropy_rates": [],
            "mean_entropy": 0.0,
            "mean_rate": 0.0,
            "max_rate": 0.0,
            "flow_direction": "stable",
            "n_steps": 0,
        }

    entropies = [linear_entropy(rho) for rho in belief_states]
    norm_entropies = [linear_entropy_normalized(rho) for rho in belief_states]

    rates = []  # type: List[float]
    for i in range(len(belief_states) - 1):
        r = linear_entropy_rate(belief_states[i], belief_states[i + 1], dt)
        rates.append(r)

    mean_ent = sum(entropies) / len(entropies) if entropies else 0.0
    mean_rate = sum(rates) / len(rates) if rates else 0.0
    max_rate = max((abs(r) for r in rates), default=0.0)

    if abs(mean_rate) < 1e-10:
        direction = "stable"
    elif mean_rate > 0:
        direction = "outflow"
    else:
        direction = "inflow"

    return {
        "linear_entropies": entropies,
        "normalized_entropies": norm_entropies,
        "entropy_rates": rates,
        "mean_entropy": mean_ent,
        "mean_rate": mean_rate,
        "max_rate": max_rate,
        "flow_direction": direction,
        "n_steps": len(belief_states),
    }


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

    Each edge carries a `context_tag` field (str or None) that records the
    coarse-graining / observer context under which the claim holds. This
    enables context-aware deduplication and contradiction detection, per the
    insight that complexity/entropy claims without context specification are
    meaningless.
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
                # Migrate legacy edges: ensure context_tag field exists
                for e in self._edges:
                    if "context_tag" not in e:
                        e["context_tag"] = None
                    if "context_warning" not in e:
                        e["context_warning"] = False
                for e in self._node_edges:
                    if "context_tag" not in e:
                        e["context_tag"] = None
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

    def record_feed(self, kernel_output, source_url, source_title="",
                    context_tag=None):
        # type: (dict, str, str, Optional[str]) -> list
        """
        Extract edges from a kernel output and append them to the graph.

        Parameters
        ----------
        kernel_output : dict
            Kernel output fields to scan for edge claims.
        source_url : str
            URL of the source paper/feed.
        source_title : str
            Title of the source.
        context_tag : str or None
            Explicit context tag. If None, auto-inference is attempted.

        Returns the list of new edges added (may be empty).
        """
        self._ensure_loaded()
        new_edges = extract_edges(kernel_output, source_url, source_title,
                                  context_tag=context_tag)
        if new_edges:
            self._edges.extend(new_edges)
            self.save()
            parts = []
            for e in new_edges:
                tag_str = ""
                if e.get("context_tag"):
                    tag_str = f"[{e['context_tag']}]"
                elif e.get("context_warning"):
                    tag_str = "[⚠ no context]"
                parts.append(e['to'] + ':' + e['type'] + tag_str)
            print(f"[GRAPH] {len(new_edges)} edge(s) recorded → "
                  f"{', '.join(parts)}")

            # Report context warnings
            warnings = [e for e in new_edges if e.get("context_warning")]
            if warnings:
                print(f"[GRAPH] ⚠ {len(warnings)} complexity-related edge(s) "
                      f"lack context_tag — claims may be context-dependent")

        return new_edges

    def record_node_edge(self, node_a_id, node_b_id, edge_type,
                         invariant_text="", context_tag=None):
        # type: (str, str, str, str, Optional[str]) -> None
        """
        Record a structural edge between two project nodes (e.g. shares_invariant).

        Parameters
        ----------
        node_a_id : str
            Source node identifier.
        node_b_id : str
            Target node identifier.
        edge_type : str
            Type of structural relationship.
        invariant_text : str
            Text of the shared invariant (truncated to 120 chars).
        context_tag : str or None
            Coarse-graining / observer context under which this edge holds.
        """
        self._ensure_loaded()
        # For shares_invariant edges, one edge per (from, to, type) is enough.
        # For other types, include invariant prefix in the key.
        inv_key = "" if edge_type == "shares_invariant" else invariant_text[:60]
        for e in self._node_edges:
            if (e.get("from") == node_a_id and e.get("to") == node_b_id
                    and e.get("type") == edge_type
                    and (edge_type == "shares_invariant" or
                         e.get("invariant", "")[:60] == inv_key)
                    and e.get("context_tag") == context_tag):
                return
        ts = datetime.now(timezone.utc).isoformat()
        self._node_edges.append({
            "from":        node_a_id,
            "to":          node_b_id,
            "type":        edge_type,
            "invariant":   invariant_text[:120],
            "context_tag": context_tag,
            "timestamp":   ts,
        })
        self.save()
        tag_str = f" [{context_tag}]" if context_tag else ""
        print(f"[GRAPH] node-edge: {node_a_id[:25]} --{edge_type}--> {node_b_id[:25]}{tag_str}")

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

    # ── Context-aware querying ───────────────────────────────────────────────

    def edges_for(self, node_id, context_tag=None):
        # type: (str, Optional[str]) -> list
        """
        All edges pointing to or from a given node ID.

        Parameters
        ----------
        node_id : str
            The node to query.
        context_tag : str or None
            If provided, filter to only edges with this context_tag.
            If None, return all edges regardless of context.
        """
        self._ensure_loaded()
        node_up = node_id.upper()
        result = []
        for e in self._edges:
            if (e.get("from", "").upper() == node_up or
                    e.get("to", "").upper() == node_up):
                if context_tag is None or e.get("context_tag") == context_tag:
                    result.append(e)
        for e in self._node_edges:
            if (e.get("from", "").upper() == node_up or
                    e.get("to", "").upper() == node_up):
                if context_tag is None or e.get("context_tag") == context_tag:
                    result.append(e)
        return result

    # ── Summary reporting ────────────────────────────────────────────────────

    def report(self, top_n=10):
        # type: (int) -> str
        """Return a short text summary of the graph state."""
        self._ensure_loaded()
        total = len(self._edges)
        if total == 0:
            return "Knowledge graph: 0 edges recorded."

        # Count by type
        from collections import Counter
        type_counts = Counter(e.get("type", "unknown") for e in self._edges)
        top_targets = Counter(e.get("to", "") for e in self._edges).most_common(top_n)

        lines = [f"Knowledge graph: {total} edge(s)."]
        lines.append("  Edge types: " + ", ".join(
            f"{t}={c}" for t, c in type_counts.most_common(5)))
        lines.append("  Most-referenced nodes: " + ", ".join(
            f"{node}({cnt})" for node, cnt in top_targets[:5]))
        return "\n".join(lines)

    def confirmation_structure(self):
        # type: () -> dict
        """Return a dict mapping each target node to its confirmation count by type."""
        self._ensure_loaded()
        from collections import defaultdict
        structure = defaultdict(lambda: defaultdict(int))
        for e in self._edges:
            target = e.get("to", "")
            etype = e.get("type", "unknown")
            if target:
                structure[target][etype] += 1
        return {k: dict(v) for k, v in structure.items()}

# ── Singleton accessor ────────────────────────────────────────────────────────

_graph_instance = None  # type: Optional[KnowledgeGraph]

def get_graph():
    # type: () -> KnowledgeGraph
    """Return the process-level singleton KnowledgeGraph."""
    global _graph_instance
    if _graph_instance is None:
        _graph_instance = KnowledgeGraph()
    return _graph_instance
