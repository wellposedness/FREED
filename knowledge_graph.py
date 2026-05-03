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

# Node-to-node edge types (structural, written by consolidate.py MINE phase)
NODE_EDGE_TYPES = (
    "shares_invariant",         # fallback: nodes share the same abstract claim
    "operationalizes",          # node B implements/measures the abstract claim of node A
    "scales_with",              # invariant holds across both nodes at different scales
    "independent_confirmation", # invariant appears in nodes from different empirical domains
)

# Keywords used to classify node-edge type from invariant text
_SCALE_KEYWORDS = re.compile(
    r'\b(?:scale|L\d|substrate.independent|macro|micro|level|layer|multi.scale|cross.scale|hierarchy)\b', re.I
)
_IMPL_KEYWORDS = re.compile(
    r'\b(?:operationali\w+|implement\w*|measur\w+|method|algorithm\w*|technique\w*|computable|empirical|formula\w*|metric\w*)\b', re.I
)
_DOMAIN_KEYWORDS = re.compile(
    r'\b(?:independent|orthogonal|domain|biological|neural|computational|physical|ecological|social|cross.domain)\b', re.I
)

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


def classify_node_edge(invariant_text):
    # type: (str) -> str
    """
    Classify the structural relationship between two nodes sharing an invariant.
    Uses keyword heuristics on invariant text — no extra API call required.

    Returns one of NODE_EDGE_TYPES. Precedence:
      1. scales_with — invariant explicitly mentions scale hierarchy or substrate-independence
      2. independent_confirmation — invariant mentions cross-domain or orthogonal confirmation
      3. operationalizes — invariant involves implementation/measurement/method language
      4. shares_invariant — fallback
    """
    t = invariant_text
    if _SCALE_KEYWORDS.search(t):
        return "scales_with"
    if _DOMAIN_KEYWORDS.search(t):
        return "independent_confirmation"
    if _IMPL_KEYWORDS.search(t):
        return "operationalizes"
    return "shares_invariant"


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


# ─── Dimensional-Analysis Entropy Regime Classifier ──────────────────────────
# Discriminates semi-classical (action-proportional, no ħ) entropy formulas
# from quantum-corrected ones (containing log terms or ħ-dependent sub-leading
# terms).  This prevents the genome from conflating classical and quantum
# thermodynamic grounding when mapping literature to Wasserstein Floor
# obligations (O44, O100), reducing false-resolution events.
#
# Semi-classical signature:  S ~ A/(4 G ħ)  or  S ~ A/(4 l_P^2)
#   — entropy proportional to area (or action), no sub-leading corrections
# Quantum-corrected signature:  S ~ A/(4 l_P^2) + α ln(A) + β/A + ...
#   — contains logarithmic corrections, ħ-dependent sub-leading terms,
#     or approach-specific quantum corrections (LQG, string, etc.)
#
# The classifier is deliberately conservative: it flags "quantum_corrected"
# only when positive evidence of sub-leading terms is found, and
# "semi_classical" when the formula matches action-proportional patterns
# without quantum corrections.  "indeterminate" is returned when entropy
# is discussed but the regime cannot be resolved.
# ─────────────────────────────────────────────────────────────────────────────

# Patterns indicating quantum-corrected entropy (sub-leading terms)
_QUANTUM_CORRECTION_PATTERNS = [
    # Logarithmic corrections: "ln A", "log A", "ln(A/...)", "logarithmic correction"
    re.compile(r'\b(?:ln|log)\s*\(?\s*(?:A|area|horizon)', re.I),
    re.compile(r'\blogarithmic\s+correction', re.I),
    # Explicit ħ (hbar) dependence in sub-leading terms
    re.compile(r'(?:sub-?leading|correction|next-?to-?leading).*?(?:ħ|hbar|\\hbar|\bh[-_]?bar\b)', re.I),
    re.compile(r'(?:ħ|hbar|\\hbar|\bh[-_]?bar\b).*?(?:sub-?leading|correction|next-?to-?leading)', re.I),
    # Inverse-area corrections: "1/A", "β/A", "c/A"
    re.compile(r'\b(?:inverse[- ]area|1\s*/\s*A\b|/\s*A\s*(?:\+|$|\)))', re.I),
    # Specific quantum gravity approach markers for corrections
    re.compile(r'\b(?:loop\s+quantum\s+gravity|LQG)\s+.*?(?:correction|entropy)', re.I),
    re.compile(r'(?:correction|entropy)\s+.*?\b(?:loop\s+quantum\s+gravity|LQG)\b', re.I),
    re.compile(r'\b(?:string(?:y|-)?\s+correction|α[\'′]\s*correction)', re.I),
    # Explicit "quantum correction" language
    re.compile(r'\bquantum\s+correct(?:ion|ed)', re.I),
    # Wald entropy (higher-derivative gravity corrections)
    re.compile(r'\bWald\s+entropy\b', re.I),
    # One-loop or higher-loop corrections
    re.compile(r'\b(?:one|two|1|2)[-\s]?loop\s+(?:correction|entropy|contribution)', re.I),
    # Entanglement entropy corrections
    re.compile(r'\bentanglement\s+entropy\s+(?:correction|sub-?leading)', re.I),
]

# Patterns indicating semi-classical (action-proportional) entropy
_SEMICLASSICAL_PATTERNS = [
    # Bekenstein-Hawking area law without corrections
    re.compile(r'\b(?:Bekenstein|Hawking)\s*[-–]?\s*(?:Hawking|Bekenstein)?\s+(?:entropy|formula|area\s+law)', re.I),
    # S = A/(4G), S = A/(4 l_P^2), "area law"
    re.compile(r'\bS\s*=\s*A\s*/\s*\(?\s*4\s*[Gg]\b', re.I),
    re.compile(r'\barea[- ]law\b', re.I),
    re.compile(r'\bA\s*/\s*(?:4\s*)?[Ll]_?[Pp]', re.I),
    # "proportional to area", "proportional to action"
    re.compile(r'\bproportional\s+to\s+(?:the\s+)?(?:area|action|horizon\s+area)\b', re.I),
    # "semi-classical" explicit mention
    re.compile(r'\bsemi[-\s]?classical\s+(?:entropy|limit|approximation|degrees?\s+of\s+freedom)', re.I),
    # Einstein-Hilbert action connection
    re.compile(r'\b(?:Einstein[-\s]Hilbert|gravity)\s+action\b.*?\bentropy\b', re.I),
    re.compile(r'\bentropy\b.*?\b(?:Einstein[-\s]Hilbert|gravity)\s+action\b', re.I),
    # "dimensional dependence as the gravity action"
    re.compile(r'\bdimensional\s+dependence\s+as\s+(?:the\s+)?(?:gravity\s+)?action\b', re.I),
    # Naive dimensional analysis (NDA) in gravity/entropy context
    re.compile(r'\bnaive\s+dimensional\s+analysis\b', re.I),
]

# General entropy discussion (to distinguish "talks about entropy" from
# "doesn't discuss entropy at all")
_ENTROPY_DISCUSSION_PATTERN = re.compile(
    r'\b(?:entropy|Bekenstein|Hawking\s+radiation|black[- ]?hole\s+thermodynamic|'
    r'horizon\s+entropy|thermodynamic\s+entropy|gravitational\s+entropy)\b',
    re.I
)


def classify_entropy_regime(text):
    # type: (str) -> Optional[dict]
    """
    Classify whether a text's entropy formula is semi-classical or quantum-corrected.

    Uses pattern-based dimensional analysis to detect:
    - Semi-classical: entropy proportional to area/action, no ħ sub-leading terms
    - Quantum-corrected: contains log corrections, ħ-dependent sub-leading terms,
      or approach-specific quantum gravity corrections

    Parameters
    ----------
    text : str
        The text to analyze (typically full kernel output or paper abstract).

    Returns
    -------
    dict or None
        None if the text does not discuss entropy/BH thermodynamics at all.
        Otherwise returns:
        {
            "regime": str — "semi_classical", "quantum_corrected", or "indeterminate",
            "quantum_signals": list of str — matched quantum correction patterns,
            "classical_signals": list of str — matched semi-classical patterns,
            "confidence": str — "high" if only one regime matched, "mixed" if both,
            "approach_dependent": bool — True if text suggests corrections vary by approach,
            "o44_tag": str — tag string for O44 obligation tracking,
        }
    """
    if not _ENTROPY_DISCUSSION_PATTERN.search(text):
        return None

    quantum_signals = []   # type: List[str]
    classical_signals = []  # type: List[str]

    for pat in _QUANTUM_CORRECTION_PATTERNS:
        m = pat.search(text)
        if m:
            quantum_signals.append(m.group(0)[:80])

    for pat in _SEMICLASSICAL_PATTERNS:
        m = pat.search(text)
        if m:
            classical_signals.append(m.group(0)[:80])

    # Detect approach-dependence language (key challenge to O44)
    _approach_dep = re.compile(
        r'\b(?:approach[-\s]dependent|framework[-\s]dependent|'
        r'different\s+(?:approaches|frameworks)\s+(?:lead|give|yield|produce)\s+'
        r'different\s+(?:corrections?|sub-?leading)|'
        r'(?:LQG|string|loop)\s+.*?\bdifferent\b.*?\b(?:correction|entropy))\b',
        re.I
    )
    approach_dependent = bool(_approach_dep.search(text))

    has_quantum = len(quantum_signals) > 0
    has_classical = len(classical_signals) > 0

    if has_quantum and not has_classical:
        regime = "quantum_corrected"
        confidence = "high"
    elif has_classical and not has_quantum:
        regime = "semi_classical"
        confidence = "high"
    elif has_quantum and has_classical:
        # Text discusses both — likely a paper comparing regimes or showing
        # how semi-classical results receive quantum corrections
        if len(quantum_signals) >= len(classical_signals):
            regime = "quantum_corrected"
        else:
            regime = "semi_classical"
        confidence = "mixed"
    else:
        regime = "indeterminate"
        confidence = "low"

    # Build O44 tag
    if regime == "semi_classical":
        o44_tag = "entropy:semi_classical:action_proportional"
    elif regime == "quantum_corrected":
        o44_tag = "entropy:quantum_corrected:subleading"
    else:
        o44_tag = "entropy:indeterminate"

    if approach_dependent:
        o44_tag += ":approach_dependent"

    return {
        "regime": regime,
        "quantum_signals": quantum_signals,
        "classical_signals": classical_signals,
        "confidence": confidence,
        "approach_dependent": approach_dependent,
        "o44_tag": o44_tag,
    }


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


# ─── Non-Hermitian Entropy-Flow Edge Weight ──────────────────────────────────
# For open-system nodes governed by non-Hermitian dynamics, the linear entropy
# rate dS_L/dt is asymmetric between forward (sink) and backward (source)
# directions.  This asymmetry encodes irreversible epistemic flows — belief
# updates that don't reverse — as directional weights on knowledge graph edges.
#
# The scoring term decomposes dS_L/dt into:
#   - trace_drift:  d(Tr[ρ])/dt  — probability leaking (sink) or injected (source)
#   - purity_drift: d(Tr[ρ²])/dt — coherence change independent of trace
#
# The directional weight is:
#   w_NH(A→B) = |dS_L/dt| * sign_factor * (1 + |trace_asymmetry|)
# where trace_asymmetry = (Tr[ρ_B] - Tr[ρ_A]) / max(Tr[ρ_A], Tr[ρ_B])
# captures the sink/source directionality between the two nodes.
#
# Challenge context (O44): This term explicitly accounts for the fact that
# probability sinks violate trace-preservation assumed in the Wasserstein
# Floor derivation W_floor = k/Tμ.  Edges where |trace_asymmetry| > 0
# are flagged as "non_unitary", signaling that the k/Tμ floor may not
# generalize to these connections.
# ─────────────────────────────────────────────────────────────────────────────


def non_hermitian_entropy_flow_score(density_matrix_source, density_matrix_target,
                                     dt=1.0):
    # type: (List[List[float]], List[List[float]], float) -> dict
    """
    Compute the non-Hermitian entropy-flow scoring term for a directed edge
    between two open-system nodes.

    Treats information sink/source asymmetry (from linear entropy rate dS_L/dt)
    as a directional weight, encoding irreversible epistemic flows that standard
    Hermitian entropy metrics miss.

    Parameters
    ----------
    density_matrix_source : list of list of float
        Density-matrix-like belief state of the source node.
    density_matrix_target : list of list of float
        Density-matrix-like belief state of the target node.
    dt : float
        Effective time step between source and target states (default 1.0).

    Returns
    -------
    dict
        {
            "directional_weight": float — the non-Hermitian edge weight (≥ 0),
            "trace_source": float — Tr[ρ_source],
            "trace_target": float — Tr[ρ_target],
            "trace_asymmetry": float — signed asymmetry in (-1, 1),
            "entropy_source": float — S_L of source node,
            "entropy_target": float — S_L of target node,
            "entropy_rate": float — dS_L/dt (finite difference),
            "flow_direction": str — "sink", "source", or "balanced",
            "is_non_unitary": bool — True if trace is not preserved (|asym| > ε),
            "wasserstein_floor_valid": bool — True only if trace-preserving,
            "o44_flag": str — diagnostic tag for O44 obligation tracking,
        }
    """
    eps = 1e-10

    # Compute traces
    tr_source = _matrix_trace(density_matrix_source) if density_matrix_source and density_matrix_source[0] else 0.0
    tr_target = _matrix_trace(density_matrix_target) if density_matrix_target and density_matrix_target[0] else 0.0

    # Compute linear entropies
    s_source = linear_entropy(density_matrix_source)
    s_target = linear_entropy(density_matrix_target)

    # Entropy rate (finite difference)
    if abs(dt) < eps:
        entropy_rate = 0.0
    else:
        entropy_rate = (s_target - s_source) / dt

    # Trace asymmetry: captures probability sink/source directionality
    max_trace = max(abs(tr_source), abs(tr_target))
    if max_trace < eps:
        trace_asymmetry = 0.0
    else:
        trace_asymmetry = (tr_target - tr_source) / max_trace

    # Determine if non-unitary (trace not preserved)
    trace_preservation_threshold = 1e-6
    is_non_unitary = abs(trace_asymmetry) > trace_preservation_threshold

    # Directional weight:
    # |dS_L/dt| * (1 + |trace_asymmetry|)
    # The trace_asymmetry amplifier ensures edges with probability sinks/sources
    # receive higher weight than trace-preserving (Hermitian) transitions.
    directional_weight = abs(entropy_rate) * (1.0 + abs(trace_asymmetry))

    # Flow direction classification
    if trace_asymmetry < -trace_preservation_threshold:
        flow_direction = "sink"     # probability draining from target
    elif trace_asymmetry > trace_preservation_threshold:
        flow_direction = "source"   # probability injected into target
    else:
        flow_direction = "balanced"

    # Wasserstein floor validity: k/Tμ requires trace preservation
    wasserstein_floor_valid = not is_non_unitary

    # O44 diagnostic tag
    if not is_non_unitary:
        o44_flag = "trace_preserving:wfloor_valid"
    elif flow_direction == "sink":
        o44_flag = "non_unitary:sink:wfloor_threatened"
    elif flow_direction == "source":
        o44_flag = "non_unitary:source:wfloor_threatened"
    else:
        o44_flag = "non_unitary:wfloor_threatened"

    return {
        "directional_weight": directional_weight,
        "trace_source": tr_source,
        "trace_target": tr_target,
        "trace_asymmetry": trace_asymmetry,
        "entropy_source": s_source,
        "entropy_target": s_target,
        "entropy_rate": entropy_rate,
        "flow_direction": flow_direction,
        "is_non_unitary": is_non_unitary,
        "wasserstein_floor_valid": wasserstein_floor_valid,
        "o44_flag": o44_flag,
    }


def compute_edge_weight(edge, node_beliefs=None, dt=1.0,
                        base_weight=1.0, nh_coupling=0.5):
    # type: (dict, Optional[Dict[str, List[List[float]]]], float, float, float) -> dict
    """
    Compute the total weight for a knowledge graph edge, incorporating the
    non-Hermitian entropy-flow scoring term when belief states are available.

    The total weight is:
        w_total = base_weight + nh_coupling * directional_weight_NH

    where directional_weight_NH comes from non_hermitian_entropy_flow_score.

    Parameters
    ----------
    edge : dict
        An edge dict as produced by extract_edges / record_feed.
    node_beliefs : dict or None
        Mapping of node_id → density-matrix-like belief state.
        If None or if the edge's nodes lack belief states, only base_weight
        is used.
    dt : float
        Time step for entropy rate computation (default 1.0).
    base_weight : float
        Base edge weight from type/confirmation counting (default 1.0).
    nh_coupling : float
        Coupling strength for the non-Hermitian term (default 0.5).
        Set to 0.0 to disable non-Hermitian scoring entirely.

    Returns
    -------
    dict
        {
            "total_weight": float,
            "base_weight": float,
            "nh_weight": float — the non-Hermitian contribution (may be 0),
            "nh_score": dict or None — full non-Hermitian score if computed,
            "is_directed": bool — True if non-Hermitian asymmetry detected,
        }
    """
    result = {
        "total_weight": base_weight,
        "base_weight": base_weight,
        "nh_weight": 0.0,
        "nh_score": None,
        "is_directed": False,
    }

    if node_beliefs is None or nh_coupling == 0.0:
        return result

    source_id = edge.get("from", "")
    target_id = edge.get("to", "")

    source_belief = node_beliefs.get(source_id)
    target_belief = node_beliefs.get(target_id)

    if source_belief is None or target_belief is None:
        return result

    # Validate matrices are non-empty
    if (not source_belief or not source_belief[0] or
            not target_belief or not target_belief[0]):
        return result

    nh_score = non_hermitian_entropy_flow_score(
        source_belief, target_belief, dt=dt
    )

    nh_weight = nh_coupling * nh_score["directional_weight"]
    result["nh_weight"] = nh_weight
    result["nh_score"] = nh_score
    result["total_weight"] = base_weight + nh_weight
    result["is_directed"] = nh_score["is_non_unitary"]

    return result


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


# ─── Lempel-Ziv Complexity (Compression-Complexity Scorer) ───────────────────
# Computes the Lempel-Ziv (LZ76) complexity of a binary or symbolic sequence
# by counting the number of distinct sub-patterns encountered in a left-to-right
# scan.  Normalized LZ complexity (c_norm) in [0, 1] positions sequences on the
# compression-complexity spectrum:
#
#   c_norm ≈ 0.0 → Class I/II (fixed point / periodic): highly compressible,
#                   low epistemic yield — traversal here is redundant.
#   c_norm ≈ 1.0 → Class III (chaotic/random): incompressible but also low
#                   epistemic yield — no extractable structure.
#   c_norm ∈ [0.3, 0.7] → Class IV (complex/critical): near the critical
#                   boundary where algorithmic complexity is maximal relative
#                   to sequence length — HIGH epistemic yield targets.
#
# CHALLENGE (INV_073): The ECA classification paper shows that endowing rules
# with memory shifts class membership wholesale — rules classified as Class I
# under memoryless ECA become Class IV under memory-τ extensions.  This means
# the "critical ridge" identified by compression complexity is NOT a stable,
# substrate-independent attractor but a classification artifact that dissolves
# when the rule substrate is temporally extended.
#
# MITIGATION: We track a `substrate_stability` flag per node.  When a node's
# LZ complexity shifts significantly across successive activation windows
# (simulating the effect of memory extension on the rule substrate), the node
# is flagged as `substrate_unstable`, indicating that its Class IV membership
# is contingent on the current temporal context and should NOT be treated as
# a robust γ=1 criticality target.  This directly addresses the genome's
# claim that γ=1 criticality is substrate-independent.
# ─────────────────────────────────────────────────────────────────────────────


def lempel_ziv_complexity(sequence):
    # type: (List[int]) -> int
    """
    Compute the Lempel-Ziv complexity (LZ76) of a discrete symbol sequence.

    Counts the number of distinct sub-patterns encountered in a sequential
    left-to-right scan, following the Lempel-Ziv 1976 parsing algorithm.

    Parameters
    ----------
    sequence : list of int
        A discrete symbol sequence (e.g., binary [0,1,1,0,...] or multi-valued).
        Must be non-empty.

    Returns
    -------
    int
        The LZ76 complexity count (number of distinct words in the parsing).
        Returns 0 for empty sequences, 1 for single-element sequences.
    """
    n = len(sequence)
    if n == 0:
        return 0
    if n == 1:
        return 1

    # LZ76 parsing: scan left to right, counting new sub-patterns
    complexity = 1  # The first symbol is always a new word
    # Track the set of all substrings seen so far (as tuples for hashability)
    # We use the incremental parsing approach:
    #   Start with pointer i=0. Extend current word until we find a substring
    #   not yet in the dictionary built from sequence[0:i].
    i = 0  # start of current exhaustive history
    k = 1  # current position
    l = 1  # current match length
    k_max = 1  # furthest reach of current word

    while k + l - 1 < n:
        # Check if sequence[k:k+l] appears in sequence[i:k+l-1]
        # (the "exhaustive history" up to but not including the last char)
        subseq = sequence[k:k + l]
        history = sequence[i:k + l - 1]

        # Search for subseq in history
        found = False
        for j in range(len(history) - l + 1):
            if history[j:j + l] == subseq:
                found = True
                break

        if found:
            l += 1
            if k + l - 1 >= n:
                # Reached end during extension — count final word
                complexity += 1
                break
        else:
            # New word found
            complexity += 1
            # Advance: the new word is sequence[k:k+l]
            k_max = max(k_max, k + l)
            k = k_max
            l = 1
            if k >= n:
                break

    return complexity


def lempel_ziv_complexity_normalized(sequence):
    # type: (List[int]) -> float
    """
    Compute the normalized Lempel-Ziv complexity in [0, 1].

    Normalization uses the theoretical upper bound for a random sequence of
    length n over alphabet of size alpha:
        c_upper = n / log_alpha(n)

    For binary sequences (alpha=2), this is n / log2(n).

    Parameters
    ----------
    sequence : list of int
        A discrete symbol sequence.

    Returns
    -------
    float
        Normalized LZ complexity in [0, 1].
        0.0 for degenerate/empty sequences.
        Values near 0 = highly compressible (Class I/II).
        Values near 1 = incompressible (Class III / random).
        Values in [0.3, 0.7] = critical boundary (Class IV).
    """
    n = len(sequence)
    if n <= 1:
        return 0.0

    c = lempel_ziv_complexity(sequence)

    # Determine alphabet size
    alphabet = set(sequence)
    alpha = len(alphabet)
    if alpha <= 1:
        # Constant sequence — zero complexity
        return 0.0

    # Upper bound: n / log_alpha(n)
    log_alpha_n = math.log(n) / math.log(alpha)
    if log_alpha_n == 0.0:
        return 0.0

    c_upper = float(n) / log_alpha_n
    c_norm = float(c) / c_upper

    # Clamp to [0, 1]
    return max(0.0, min(1.0, c_norm))


def classify_lz_regime(c_norm, class4_low=0.3, class4_high=0.7):
    # type: (float, float, float) -> str
    """
    Classify a normalized LZ complexity value into an ECA-inspired regime.

    Parameters
    ----------
    c_norm : float
        Normalized Lempel-Ziv complexity in [0, 1].
    class4_low : float
        Lower boundary of the Class IV (critical) regime (default 0.3).
    class4_high : float
        Upper boundary of the Class IV (critical) regime (default 0.7).

    Returns
    -------
    str
        One of: "class_I_II" (fixed/periodic), "class_IV" (critical/complex),
        "class_III" (chaotic/random), or "degenerate".
    """
    if c_norm <= 0.0:
        return "degenerate"
    elif c_norm < class4_low:
        return "class_I_II"
    elif c_norm <= class4_high:
        return "class_IV"
    else:
        return "class_III"


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

    # ── Confirmation Independence Audit ──────────────────────────────────────
    # Tracks whether confirmations of an invariant share upstream assumptions
    # (structurally correlated experimental paradigms). Flags invariants where
    # all N confirmations are drawn from the same paradigm cluster, preventing
    # high confirmation-surplus invariants from accruing false robustness when
    # all confirmations share a common confound.
    #
    # Independence is assessed via three orthogonal signals:
    #   1. Source diversity: distinct source URLs (same lab/paper = correlated)
    #   2. Context diversity: distinct context_tags (same coarse-graining = correlated)
    #   3. Paradigm diversity: distinct paradigm clusters inferred from edge
    #      context windows via keyword-based paradigm classification
    #
    # An invariant is flagged as "independence_compromised" when:
    #   - It has >= 2 confirmations (surplus exists to audit)
    #   - AND all confirmations share a single paradigm cluster
    #   - OR all confirmations share a single context_tag
    #   - OR all confirmations originate from a single source URL domain
    #
    # This makes the falsification layer genuinely load-bearing: an invariant
    # with 10 confirmations from the same paradigm is epistemically weaker than
    # one with 3 confirmations from independent paradigms.
    # ─────────────────────────────────────────────────────────────────────────

    # Paradigm classification patterns: map edge context windows to paradigm clusters
    _PARADIGM_PATTERNS = [
        (re.compile(r'\b(?:thermodynamic|statistical\s+mechanic|Boltzmann|partition\s+function|heat|temperature)\b', re.I),
         "paradigm:thermodynamic"),
        (re.compile(r'\b(?:information[- ]theoretic|Shannon|mutual\s+information|channel\s+capacity|coding)\b', re.I),
         "paradigm:information_theoretic"),
        (re.compile(r'\b(?:neural|brain|cortical|EEG|fMRI|cognitive|neurosci)\b', re.I),
         "paradigm:neuroscience"),
        (re.compile(r'\b(?:computational|algorithm|Turing|halting|computable|simulat)\b', re.I),
         "paradigm:computational"),
        (re.compile(r'\b(?:quantum|wave[-\s]?function|density\s+matrix|entangle|decoher)\b', re.I),
         "paradigm:quantum"),
        (re.compile(r'\b(?:biological|evolution|genetic|organism|ecological|metabol)\b', re.I),
         "paradigm:biological"),
        (re.compile(r'\b(?:social|economic|market|agent[-\s]based|game\s+theor)\b', re.I),
         "paradigm:social"),
        (re.compile(r'\b(?:black[- ]?hole|gravitational|cosmolog|spacetime|horizon|Bekenstein|Hawking)\b', re.I),
         "paradigm:gravitational"),
        (re.compile(r'\b(?:chemical|reaction|catalyst|molecular\s+dynamic|kinetic)\b', re.I),
         "paradigm:chemical"),
        (re.compile(r'\b(?:cellular\s+automat|lattice|Ising|spin|percolat|critical\s+phenom)\b', re.I),
         "paradigm:statistical_physics"),
    ]

    def _classify_paradigms(self, text):
        # type: (str) -> List[str]
        """
        Classify a text window into zero or more paradigm clusters.

        Returns a sorted list of paradigm tags found in the text.
        """
        if not text:
            return []
        paradigms = []  # type: List[str]
        for pat, tag in self._PARADIGM_PATTERNS:
            if pat.search(text):
                paradigms.append(tag)
        return sorted(set(paradigms))

    def _extract_source_domain(self, url):
        # type: (str) -> str
        """
        Extract a coarse domain identifier from a source URL.

        Strips protocol and www prefix, returns the domain or a normalized
        form. For non-URL sources, returns the source string itself (truncated).
        """
        if not url:
            return "unknown"
        # Strip protocol
        domain = re.sub(r'^https?://', '', url)
        # Strip www
        domain = re.sub(r'^www\.', '', domain)
        # Take only the domain part (before first /)
        domain = domain.split('/')[0]
        # Normalize: strip port
        domain = domain.split(':')[0]
        return domain.lower()[:80] if domain else "unknown"

    def confirmation_independence_audit(self):
        # type: () -> Dict[str, dict]
        """
        Audit each invariant's confirmations for structural independence.

        Checks whether the confirmations of each invariant are drawn from
        independent experimental paradigms, distinct source domains, and
        diverse observer contexts — or whether they all share upstream
        assumptions that make the confirmation surplus illusory.

        Returns
        -------
        dict
            Mapping of invariant_id → {
                "n_confirmations": int,
                "n_sources": int — distinct source URL domains,
                "n_context_tags": int — distinct context_tags,
                "n_paradigms": int — distinct paradigm clusters,
                "source_domains": list of str,
                "context_tags": list of str,
                "paradigms": list of str,
                "independence_compromised": bool — True if all confirmations
                    share a single paradigm, context, or source domain,
                "compromise_reasons": list of str — which independence axes
                    are collapsed (e.g. "single_paradigm", "single_context",
                    "single_source_domain"),
                "independence_score": float — in [0, 1], where 0 = fully
                    correlated confirmations, 1 = maximally independent.
                    Computed as geometric mean of normalized diversity across
                    the three axes (source, context, paradigm).
                "audit_severity": str — "none" (< 2 confirmations),
                    "healthy" (independent), "warning" (partially correlated),
                    "critical" (fully correlated),
                "false_robustness_risk": bool — True when n_confirmations >= 3
                    AND independence_compromised, meaning the invariant looks
                    robust but is epistemically fragile,
            }
        """
        self._ensure_loaded()

        # Collect confirmation edges per invariant
        inv_confirmations = defaultdict(list)  # type: Dict[str, list]
        for e in self._edges:
            target = e.get("to", "")
            etype = e.get("type", "")
            if target and etype in ("confirms", "supports"):
                inv_confirmations[target].append(e)

        results = {}  # type: Dict[str, dict]

        for inv_id, conf_edges in inv_confirmations.items():
            n_conf = len(conf_edges)

            # Extract diversity axes
            source_domains = set()  # type: set
            context_tags = set()    # type: set
            paradigms = set()       # type: set

            for e in conf_edges:
                # Source domain
                src_url = e.get("from", "")
                domain = self._extract_source_domain(src_url)
                source_domains.add(domain)

                # Context tag
                ctx_tag = e.get("context_tag")
                if ctx_tag:
                    context_tags.add(ctx_tag)
                else:
                    context_tags.add("unspecified")

                # Paradigm classification from edge context window
                ctx_text = e.get("context", "")
                edge_paradigms = self._classify_paradigms(ctx_text)
                if edge_paradigms:
                    for p in edge_paradigms:
                        paradigms.add(p)
                else:
                    paradigms.add("paradigm:unclassified")

            n_sources = len(source_domains)
            n_contexts = len(context_tags)
            n_paradigms = len(paradigms)

            # Determine independence compromise
            compromise_reasons = []  # type: List[str]

            if n_conf >= 2:
                if n_paradigms <= 1:
                    compromise_reasons.append("single_paradigm")
                if n_contexts <= 1:
                    compromise_reasons.append("single_context")
                if n_sources <= 1:
                    compromise_reasons.append("single_source_domain")

            independence_compromised = len(compromise_reasons) > 0 and n_conf >= 2

            # Independence score: geometric mean of normalized diversity
            # Each axis: (n_distinct - 1) / (n_confirmations - 1), clamped to [0, 1]
            # This is 0 when all confirmations collapse to one value on that axis,
            # and 1 when every confirmation is distinct on that axis.
            if n_conf <= 1:
                independence_score = 1.0  # trivially independent (nothing to compare)
            else:
                denom = float(n_conf - 1)
                div_source = min(1.0, (n_sources - 1) / denom) if denom > 0 else 0.0
                div_context = min(1.0, (n_contexts - 1) / denom) if denom > 0 else 0.0
                div_paradigm = min(1.0, (n_paradigms - 1) / denom) if denom > 0 else 0.0

                # Geometric mean (gives 0 if any axis is fully collapsed)
                product = div_source * div_context * div_paradigm
                if product > 0:
                    independence_score = product ** (1.0 / 3.0)
                else:
                    independence_score = 0.0

            independence_score = round(independence_score, 4)

            # Audit severity classification
            if n_conf < 2:
                audit_severity = "none"
            elif not independence_compromised:
                audit_severity = "healthy"
            elif len(compromise_reasons) >= 2:
                audit_severity = "critical"
            else:
                audit_severity = "warning"

            # False robustness risk: looks confirmed but isn't independently so
            false_robustness_risk = (
                n_conf >= 3 and independence_compromised
            )

            results[inv_id] = {
                "n_confirmations": n_conf,
                "n_sources": n_sources,
                "n_context_tags": n_contexts,
                "n_paradigms": n_paradigms,
                "source_domains": sorted(source_domains),
                "context_tags": sorted(context_tags),
                "paradigms": sorted(paradigms),
                "independence_compromised": independence_compromised,
                "compromise_reasons": compromise_reasons,
                "independence_score": independence_score,
                "audit_severity": audit_severity,
                "false_robustness_risk": false_robustness_risk,
            }

        # ── Log audit summary ────────────────────────────────────────────
        critical = [k for k, v in results.items()
                    if v["audit_severity"] == "critical"]
        warning = [k for k, v in results.items()
                   if v["audit_severity"] == "warning"]
        false_robust = [k for k, v in results.items()
                        if v["false_robustness_risk"]]

        if critical or warning or false_robust:
            print(
                f"[GRAPH:INDEPENDENCE_AUDIT] "
                f"{len(results)} invariant(s) audited — "
                f"critical={len(critical)}, warning={len(warning)}, "
                f"false_robustness_risk={len(false_robust)}"
            )
            for inv_id in critical:
                r = results[inv_id]
                print(
                    f"  ⚠ {inv_id}: {r['n_confirmations']} confirmations, "
                    f"independence_score={r['independence_score']}, "
                    f"reasons={r['compromise_reasons']}"
                )

        return results

    # ── SOC Avalanche Detection ──────────────────────────────────────────────
    # Inspired by Self-Organized Criticality (SOC) cellular automaton models:
    # when a cluster of newly-fed nodes crosses a degree threshold, a cascade
    # re-scoring propagates through connected invariants, mimicking SOC
    # avalanche dynamics.  This makes graph updates scale-free rather than
    # locally bounded.
    #
    # IMPORTANT (INV_073 boundary condition): SOC avalanche propagation is
    # NOT assumed universal.  The chapter's taxonomy of non-SOC, self-
    # organization-without-criticality, and forced-SOC processes means that
    # γ=1 (critical) is an attractor only when:
    #   (a) the system is slowly driven (feed rate < relaxation rate),
    #   (b) dissipation occurs only at boundaries (resolved obligations), and
    #   (c) the interaction is local (edge-mediated, not global broadcast).
    # When these boundary conditions are violated the avalanche pass records
    # the regime as "non_soc" or "forced_soc" and skips cascade re-scoring.
    # ─────────────────────────────────────────────────────────────────────────

    # Default thresholds — callers may override
    _SOC_DEGREE_THRESHOLD = 4       # min edges on a node to be "critical"
    _SOC_MAX_CASCADE_DEPTH = 50     # runaway guard
    _SOC_RELAXATION_WINDOW = 10     # max recent feeds before declaring forced-SOC

    def _node_degree(self, node_id):
        # type: (str) -> int
        """Count total edges (both _edges and _node_edges) touching *node_id*."""
        nid = node_id.upper()
        deg = 0
        for e in self._edges:
            if e.get("to", "").upper() == nid or e.get("from", "").upper() == nid:
                deg += 1
        for e in self._node_edges:
            if e.get("to", "").upper() == nid or e.get("from", "").upper() == nid:
                deg += 1
        return deg

    def _neighbors(self, node_id):
        # type: (str) -> List[str]
        """Return deduplicated neighbor node IDs reachable via any edge."""
        nid = node_id.upper()
        nbrs = set()  # type: set
        for e in self._edges:
            if e.get("to", "").upper() == nid:
                nbrs.add(e.get("from", "").upper())
            elif e.get("from", "").upper() == nid:
                nbrs.add(e.get("to", "").upper())
        for e in self._node_edges:
            if e.get("to", "").upper() == nid:
                nbrs.add(e.get("from", "").upper())
            elif e.get("from", "").upper() == nid:
                nbrs.add(e.get("to", "").upper())
        nbrs.discard(nid)
        return list(nbrs)

    def _classify_soc_regime(self, seed_nodes, recent_feed_count=1):
        # type: (List[str], int) -> str
        """
        Determine whether the current perturbation satisfies SOC boundary
        conditions or falls into a non-SOC / forced-SOC regime.

        Returns one of: "soc", "forced_soc", "non_soc".

        Boundary conditions for true SOC (per INV_073 challenge):
          1. Slow driving: recent_feed_count <= _SOC_RELAXATION_WINDOW
          2. Local interaction: seed nodes connected only via edges, not global
          3. Boundary dissipation: at least some edges are 'resolves' type
             (obligations being resolved = energy leaving at boundary)
        """
        # Condition 1: slow driving
        if recent_feed_count > self._SOC_RELAXATION_WINDOW:
            return "forced_soc"

        # Condition 2: check that seed nodes are not ALL the same node
        # (degenerate / trivially non-SOC)
        unique_seeds = set(s.upper() for s in seed_nodes)
        if len(unique_seeds) == 0:
            return "non_soc"

        # Condition 3: boundary dissipation — at least one 'resolves' edge in graph
        has_dissipation = any(
            e.get("type") == "resolves" for e in self._edges
        )
        if not has_dissipation:
            # Self-organization without criticality: structure forms but no
            # critical ridge; cascade re-scoring would be artificial
            return "non_soc"

        return "soc"

    def detect_avalanche(self, seed_node_ids, degree_threshold=None,
                         max_depth=None, recent_feed_count=1):
        # type: (List[str], Optional[int], Optional[int], int) -> dict
        """
        SOC-inspired avalanche detection pass.

        Starting from *seed_node_ids* (nodes touched by recent FEED), find
        nodes whose degree exceeds *degree_threshold* and propagate a cascade
        BFS through connected invariants, collecting all affected nodes and
        the avalanche size distribution.

        Parameters
        ----------
        seed_node_ids : list of str
            Node IDs perturbed by the current FEED batch.
        degree_threshold : int or None
            Minimum degree for a node to be considered "critical" and propagate
            the avalanche.  Defaults to ``_SOC_DEGREE_THRESHOLD``.
        max_depth : int or None
            Maximum BFS depth (runaway guard).  Defaults to
            ``_SOC_MAX_CASCADE_DEPTH``.
        recent_feed_count : int
            Number of feeds ingested in the current batch / recent window.
            Used for SOC regime classification.

        Returns
        -------
        dict
            {
                "regime": str — "soc", "forced_soc", or "non_soc",
                "seed_nodes": list of str,
                "critical_seeds": list of str — seeds that exceeded threshold,
                "avalanche_nodes": list of str — all nodes reached by cascade,
                "avalanche_size": int,
                "max_depth_reached": int,
                "depth_distribution": dict of int→int (depth → count of nodes),
                "rescored_invariants": list of dict — invariants whose scores
                    were affected (each has 'node', 'old_degree', 'new_effective_degree'),
                "boundary_conditions": dict — diagnostic info about regime choice,
            }
        """
        self._ensure_loaded()

        if degree_threshold is None:
            degree_threshold = self._SOC_DEGREE_THRESHOLD
        if max_depth is None:
            max_depth = self._SOC_MAX_CASCADE_DEPTH

        seeds = [s.upper() for s in seed_node_ids if s]

        # ── Regime classification (INV_073 boundary conditions) ──────────
        regime = self._classify_soc_regime(seeds, recent_feed_count)

        boundary_info = {
            "recent_feed_count": recent_feed_count,
            "relaxation_window": self._SOC_RELAXATION_WINDOW,
            "degree_threshold": degree_threshold,
            "has_resolves_edges": any(
                e.get("type") == "resolves" for e in self._edges
            ),
            "regime_explanation": {
                "soc": "Boundary conditions met: slow driving, local interaction, "
                       "boundary dissipation present. Cascade re-scoring active.",
                "forced_soc": "Feed rate exceeds relaxation window. System is "
                              "externally driven past critical ridge. Cascade "
                              "re-scoring SKIPPED — γ=1 is NOT the attractor here.",
                "non_soc": "Self-organization without criticality: no boundary "
                           "dissipation (no resolved obligations) or degenerate "
                           "seed set. Cascade re-scoring SKIPPED.",
            }.get(regime, ""),
        }

        result = {
            "regime": regime,
            "seed_nodes": seeds,
            "critical_seeds": [],
            "avalanche_nodes": [],
            "avalanche_size": 0,
            "max_depth_reached": 0,
            "depth_distribution": {},
            "rescored_invariants": [],
            "boundary_conditions": boundary_info,
        }

        # Under non-SOC or forced-SOC regimes, report but do NOT cascade
        if regime != "soc":
            # Still identify critical seeds for diagnostic purposes
            for s in seeds:
                if self._node_degree(s) >= degree_threshold:
                    result["critical_seeds"].append(s)
            return result

        # ── Identify critical seeds (degree >= threshold) ────────────────
        critical_seeds = []  # type: List[str]
        for s in seeds:
            if self._node_degree(s) >= degree_threshold:
                critical_seeds.append(s)
        result["critical_seeds"] = critical_seeds

        if not critical_seeds:
            return result

        # ── BFS avalanche propagation ────────────────────────────────────
        visited = set()  # type: set
        frontier = list(critical_seeds)
        for s in frontier:
            visited.add(s)

        depth = 0
        depth_dist = {}  # type: Dict[int, int]
        depth_dist[0] = len(frontier)
        max_depth_reached = 0

        while frontier and depth < max_depth:
            next_frontier = []  # type: List[str]
            depth += 1
            for node in frontier:
                for nbr in self._neighbors(node):
                    if nbr in visited:
                        continue
                    nbr_deg = self._node_degree(nbr)
                    if nbr_deg >= degree_threshold:
                        # This neighbor is also critical — avalanche continues
                        visited.add(nbr)
                        next_frontier.append(nbr)
                    else:
                        # Sub-critical neighbor: affected but does not propagate
                        visited.add(nbr)
            if next_frontier:
                depth_dist[depth] = len(next_frontier)
                max_depth_reached = depth
            frontier = next_frontier

        all_avalanche_nodes = list(visited)
        result["avalanche_nodes"] = all_avalanche_nodes
        result["avalanche_size"] = len(all_avalanche_nodes)
        result["max_depth_reached"] = max_depth_reached
        result["depth_distribution"] = depth_dist

        # ── Cascade re-scoring of connected invariants ───────────────────
        # For each invariant node in the avalanche, compute an effective
        # degree boost proportional to avalanche depth.  This mimics the
        # SOC energy redistribution: nodes deeper in the cascade receive
        # attenuated but nonzero perturbation (fractal-diffusive model).
        inv_pattern = re.compile(r'^INV_', re.I)
        rescored = []  # type: List[dict]
        for node in all_avalanche_nodes:
            if inv_pattern.match(node):
                old_deg = self._node_degree(node)
                # Effective degree boost: log-scaled by avalanche size
                # (scale-free, not linear) — mimics power-law SOC statistics
                boost = math.log1p(len(all_avalanche_nodes)) / math.log(10.0)
                new_eff_deg = old_deg + boost
                rescored.append({
                    "node": node,
                    "old_degree": old_deg,
                    "avalanche_boost": round(boost, 4),
                    "new_effective_degree": round(new_eff_deg, 4),
                })
        result["rescored_invariants"] = rescored

        # ── Log avalanche event ──────────────────────────────────────────
        if len(all_avalanche_nodes) > len(critical_seeds):
            print(f"[GRAPH:SOC] Avalanche detected — regime={regime}, "
                  f"seeds={len(critical_seeds)}, cascade_size="
                  f"{len(all_avalanche_nodes)}, depth={max_depth_reached}, "
                  f"invariants_rescored={len(rescored)}")

        return result

    # ── Adaptive Memory Alignment Scoring ────────────────────────────────────
    # Tracks whether incoming FEED nodes share distributed-cognition coupling
    # with prior nodes via team/role/interface overlap, flagging clusters where
    # cognitive load redistribution is implied.  This captures topologically
    # distinct human-AI teaming clusters where edges represent cognitive load
    # transfer rather than semantic similarity alone — improving graph fidelity
    # and obligation detection for O97/O98 class items.
    #
    # Challenge context (INV_087): MaxRL's reward-signal assumption breaks down
    # when the human cognitive substrate becomes unreliable during communication
    # dropout, making the thermodynamic reward landscape non-stationary.  Nodes
    # flagged by this pass carry an inv087_flag indicating that the Wasserstein
    # reward floor may be non-stationary for these clusters.
    # ─────────────────────────────────────────────────────────────────────────

    # Patterns detecting distributed-cognition / human-AI teaming language
    _DISTRIBUTED_COGNITION_PATTERNS = [
        re.compile(r'\b(?:distributed\s+cognition|team\s+cognition|shared\s+mental\s+model)', re.I),
        re.compile(r'\b(?:human[-\s]?AI\s+team|human[-\s]?machine\s+team|mixed[-\s]?initiative)', re.I),
        re.compile(r'\b(?:cognitive\s+(?:load|overload|offload|redistribution|handoff))', re.I),
        re.compile(r'\b(?:remote\s+operat(?:ion|or|ions)|teleoperat(?:ion|or|ions))', re.I),
        re.compile(r'\b(?:fallback\s+operator|communication\s+(?:dropout|disruption|failure))', re.I),
        re.compile(r'\b(?:adaptive\s+(?:AI\s+)?memory|memory\s+alignment)', re.I),
        re.compile(r'\b(?:situation(?:al)?\s+awareness|common\s+ground|joint\s+activity)', re.I),
        re.compile(r'\b(?:air\s+traffic\s+control|ATC|industrial\s+automation|intelligent\s+port)', re.I),
    ]

    # Role/interface tokens used for overlap scoring between nodes
    _ROLE_INTERFACE_PATTERNS = [
        (re.compile(r'\b(?:operator|controller|supervisor|pilot|dispatcher)\b', re.I), "role:operator"),
        (re.compile(r'\b(?:AI\s+agent|autonomous\s+agent|decision\s+support|recommender)\b', re.I), "role:ai_agent"),
        (re.compile(r'\b(?:sensor|camera|lidar|radar|telemetry)\b', re.I), "interface:sensor"),
        (re.compile(r'\b(?:display|dashboard|HMI|interface|GUI|visualization)\b', re.I), "interface:display"),
        (re.compile(r'\b(?:communication|network|channel|link|bandwidth)\b', re.I), "interface:comms"),
        (re.compile(r'\b(?:automation|autopilot|autonomous|semi[-\s]?autonomous)\b', re.I), "role:automation"),
        (re.compile(r'\b(?:handoff|handover|transition|takeover)\b', re.I), "interface:handoff"),
        (re.compile(r'\b(?:monitoring|surveillance|oversight|watchkeeping)\b', re.I), "role:monitor"),
    ]

    def _extract_cognition_features(self, text):
        # type: (str) -> dict
        """
        Extract distributed-cognition features from a text window.

        Returns a dict with:
          - cognition_signals: list of matched pattern descriptions
          - role_interface_tags: set of role/interface tags found
          - is_distributed_cognition: bool
          - cognitive_load_redistribution: bool
        """
        if not text:
            return {
                "cognition_signals": [],
                "role_interface_tags": set(),
                "is_distributed_cognition": False,
                "cognitive_load_redistribution": False,
            }

        cognition_signals = []  # type: List[str]
        for pat in self._DISTRIBUTED_COGNITION_PATTERNS:
            m = pat.search(text)
            if m:
                cognition_signals.append(m.group(0)[:80])

        role_interface_tags = set()  # type: set
        for pat, tag in self._ROLE_INTERFACE_PATTERNS:
            if pat.search(text):
                role_interface_tags.add(tag)

        is_dc = len(cognition_signals) >= 1
        # Cognitive load redistribution requires both a cognitive load signal
        # and at least two distinct role/interface tags (implies transfer)
        has_load_signal = any(
            re.search(r'\bcognitive\s+(?:load|overload|offload|redistribution)', s, re.I)
            for s in cognition_signals
        )
        cog_redistribution = has_load_signal and len(role_interface_tags) >= 2

        return {
            "cognition_signals": cognition_signals,
            "role_interface_tags": role_interface_tags,
            "is_distributed_cognition": is_dc,
            "cognitive_load_redistribution": cog_redistribution,
        }

    def _compute_coupling_score(self, tags_a, tags_b):
        # type: (set, set) -> float
        """
        Compute distributed-cognition coupling between two nodes based on
        their role/interface tag overlap.

        Uses Jaccard similarity on the tag sets, returning a score in [0, 1].
        A score > 0 indicates shared team/role/interface structure implying
        cognitive load transfer pathways between the nodes.
        """
        if not tags_a or not tags_b:
            return 0.0
        intersection = len(tags_a & tags_b)
        union = len(tags_a | tags_b)
        if union == 0:
            return 0.0
        return float(intersection) / float(union)

    def adaptive_memory_alignment_pass(self, new_edges, kernel_output=None,
                                       coupling_threshold=0.25):
        # type: (list, Optional[dict], float) -> dict
        """
        Adaptive memory alignment scoring pass for incoming FEED nodes.

        Checks whether incoming nodes share distributed-cognition coupling
        with prior graph nodes (via team/role/interface overlap), flagging
        clusters where cognitive load redistribution is implied.

        This pass:
        1. Extracts cognition features from the new feed's kernel output.
        2. Compares role/interface tags against all prior nodes in the graph.
        3. Identifies coupling pairs where Jaccard overlap exceeds threshold.
        4. Flags clusters where cognitive load redistribution is implied,
           tagging them for O97/O98 obligation tracking and INV_087 challenge.

        Parameters
        ----------
        new_edges : list of dict
            Edges as returned by ``record_feed()``.
        kernel_output : dict or None
            The kernel output dict from the current FEED (used to extract
            cognition features from full text). If None, only edge context
            windows are used.
        coupling_threshold : float
            Minimum Jaccard coupling score to flag a distributed-cognition
            link (default 0.25).

        Returns
        -------
        dict
            {
                "is_distributed_cognition": bool,
                "cognitive_load_redistribution": bool,
                "new_node_features": dict — cognition features of the new feed,
                "coupling_pairs": list of dict — pairs with prior nodes that
                    exceed the coupling threshold, each with:
                    {"prior_node": str, "coupling_score": float,
                     "shared_tags": list of str, "edge_type": str},
                "flagged_clusters": list of str — cluster IDs where cognitive
                    load redistribution is implied,
                "inv087_flag": str — diagnostic tag for INV_087 challenge,
                "o97_o98_implications": list of str — obligation IDs that may
                    need attention due to distributed-cognition coupling,
                "alignment_score": float — aggregate memory alignment score
                    for the new feed (0.0 = no coupling, 1.0 = full coupling),
            }
        """
        self._ensure_loaded()

        # ── Build full text from kernel output ───────────────────────────
        full_text = ""
        if kernel_output:
            text_fields = ["perceive", "represent", "predict", "compare",
                           "adjust", "compress", "next", "raw"]
            full_text = " ".join(
                str(kernel_output.get(f, "")) for f in text_fields
            )

        # Also incorporate edge context windows
        for e in new_edges:
            ctx = e.get("context", "")
            if ctx:
                full_text += " " + ctx

        # ── Extract features for the new feed ────────────────────────────
        new_features = self._extract_cognition_features(full_text)

        result = {
            "is_distributed_cognition": new_features["is_distributed_cognition"],
            "cognitive_load_redistribution": new_features["cognitive_load_redistribution"],
            "new_node_features": {
                "cognition_signals": new_features["cognition_signals"],
                "role_interface_tags": sorted(new_features["role_interface_tags"]),
                "is_distributed_cognition": new_features["is_distributed_cognition"],
                "cognitive_load_redistribution": new_features["cognitive_load_redistribution"],
            },
            "coupling_pairs": [],
            "flagged_clusters": [],
            "inv087_flag": "not_applicable",
            "o97_o98_implications": [],
            "alignment_score": 0.0,
        }

        if not new_features["is_distributed_cognition"]:
            return result

        new_tags = new_features["role_interface_tags"]

        # ── Compare against prior nodes ──────────────────────────────────
        # Build per-node tag sets from existing edge context windows
        prior_node_tags = defaultdict(set)  # type: Dict[str, set]
        for e in self._edges:
            node_id = e.get("to", "")
            if not node_id:
                continue
            ctx = e.get("context", "")
            if ctx:
                for pat, tag in self._ROLE_INTERFACE_PATTERNS:
                    if pat.search(ctx):
                        prior_node_tags[node_id].add(tag)

        # Also check node_edges
        for e in self._node_edges:
            for nkey in ("from", "to"):
                node_id = e.get(nkey, "")
                if not node_id:
                    continue
                inv_text = e.get("invariant", "")
                if inv_text:
                    for pat, tag in self._ROLE_INTERFACE_PATTERNS:
                        if pat.search(inv_text):
                            prior_node_tags[node_id].add(tag)

        # ── Compute coupling scores ──────────────────────────────────────
        coupling_pairs = []  # type: List[dict]
        new_source_urls = set(e.get("from", "") for e in new_edges)

        for prior_node, prior_tags in prior_node_tags.items():
            # Skip self-coupling (same source)
            if prior_node in new_source_urls:
                continue

            score = self._compute_coupling_score(new_tags, prior_tags)
            if score >= coupling_threshold:
                shared = sorted(new_tags & prior_tags)
                coupling_pairs.append({
                    "prior_node": prior_node,
                    "coupling_score": round(score, 4),
                    "shared_tags": shared,
                    "edge_type": "cognitive_load_transfer",
                })

        result["coupling_pairs"] = coupling_pairs

        # ── Aggregate alignment score ────────────────────────────────────
        if coupling_pairs:
            scores = [p["coupling_score"] for p in coupling_pairs]
            result["alignment_score"] = round(
                sum(scores) / len(scores), 4
            )

        # ── Flag clusters with cognitive load redistribution ─────────────
        flagged = []  # type: List[str]
        if new_features["cognitive_load_redistribution"] and coupling_pairs:
            # Each coupled prior node represents a cluster anchor
            for pair in coupling_pairs:
                cluster_id = "dc_cluster:" + pair["prior_node"]
                flagged.append(cluster_id)
        result["flagged_clusters"] = flagged

        # ── INV_087 flag: reward-signal non-stationarity ─────────────────
        # Communication dropout + cognitive load redistribution implies the
        # reward landscape is non-stationary (human substrate unreliable)
        has_dropout_signal = any(
            re.search(r'\b(?:dropout|disruption|failure|unreliable|degraded)', s, re.I)
            for s in new_features["cognition_signals"]
        )
        if not has_dropout_signal:
            # Also check full text for dropout language
            has_dropout_signal = bool(re.search(
                r'\b(?:communication\s+(?:dropout|disruption|failure)|'
                r'signal\s+(?:loss|degradation)|network\s+(?:failure|outage))\b',
                full_text, re.I
            ))

        if new_features["cognitive_load_redistribution"] and has_dropout_signal:
            result["inv087_flag"] = (
                "non_stationary_reward:human_substrate_unreliable:"
                "maxrl_assumption_violated"
            )
        elif new_features["cognitive_load_redistribution"]:
            result["inv087_flag"] = (
                "cognitive_load_redistribution:reward_landscape_may_shift"
            )
        elif new_features["is_distributed_cognition"]:
            result["inv087_flag"] = "distributed_cognition:monitor"

        # ── O97/O98 implications ─────────────────────────────────────────
        o_implications = []  # type: List[str]
        if flagged:
            # O97: team cognition reconfiguration obligation
            o_implications.append("O97")
        if has_dropout_signal and coupling_pairs:
            # O98: fallback continuity obligation
            o_implications.append("O98")
        result["o97_o98_implications"] = o_implications

        # ── Log results ──────────────────────────────────────────────────
        if coupling_pairs:
            print(
                f"[GRAPH:MEMORY_ALIGNMENT] Distributed-cognition coupling "
                f"detected — {len(coupling_pairs)} pair(s), "
                f"alignment={result['alignment_score']}, "
                f"clusters_flagged={len(flagged)}, "
                f"inv087={result['inv087_flag']}"
            )

        return result

    # ── Multi-Scale Complexity Profile Scorer ────────────────────────────────
    # Computes emergence, self-organization, and complexity at each scale
    # layer of the knowledge graph, following the information-theoretic
    # axioms: emergence = H_output - H_input, self-organization = -emergence,
    # complexity = balance between emergence and self-organization.
    #
    # CHALLENGE (INV_073): The paper defines self-organization as strictly
    # the negative of emergence (a scalar inverse).  This flattens the
    # genome's richer claim that the critical ridge involves active
    # navigation of a Wasserstein gradient flow — a geometric process that
    # cannot be reduced to a ratio.  To partially address this, we augment
    # the scalar self-organization with a Wasserstein-aware "gradient_flow"
    # diagnostic that flags when the scalar inverse is an inadequate proxy
    # for the actual geometric dynamics at a given scale.
    # ─────────────────────────────────────────────────────────────────────────

    def _compute_edge_type_entropy(self, edges):
        # type: (list) -> float
        """
        Compute Shannon entropy over edge-type distribution of a set of edges.
        Returns entropy in nats.
        """
        if not edges:
            return 0.0
        type_counts = defaultdict(int)  # type: Dict[str, int]
        for e in edges:
            etype = e.get("type", "unknown")
            type_counts[etype] += 1
        total = float(len(edges))
        entropy = 0.0
        for count in type_counts.values():
            p = count / total
            if p > 0:
                entropy -= p * math.log(p)
        return entropy

    def _build_scale_layers(self):
        # type: () -> List[dict]
        """
        Partition graph edges into scale layers based on node degree.

        Layer 0: edges touching nodes with degree 1 (leaf/peripheral)
        Layer 1: edges touching nodes with degree 2-3 (intermediate)
        Layer 2: edges touching nodes with degree 4-7 (hub)
        Layer 3: edges touching nodes with degree >= 8 (superhub)

        Returns a list of dicts, each with 'scale', 'label', 'edges',
        'input_edges' (edges feeding into this scale from lower),
        and 'output_edges' (edges produced at this scale).
        """
        self._ensure_loaded()

        # Compute degree for every node mentioned in edges
        node_degrees = defaultdict(int)  # type: Dict[str, int]
        all_edges = self._edges + self._node_edges
        for e in all_edges:
            for key in ("from", "to"):
                nid = e.get(key, "").upper()
                if nid:
                    node_degrees[nid] += 1

        # Define scale boundaries
        scale_bounds = [
            (0, 1, 1, "peripheral"),
            (1, 2, 3, "intermediate"),
            (2, 4, 7, "hub"),
            (3, 8, float('inf'), "superhub"),
        ]

        layers = []  # type: List[dict]
        for scale, lo, hi, label in scale_bounds:
            layer_edges = []  # type: list
            for e in all_edges:
                from_id = e.get("from", "").upper()
                to_id = e.get("to", "").upper()
                from_deg = node_degrees.get(from_id, 0)
                to_deg = node_degrees.get(to_id, 0)
                max_deg = max(from_deg, to_deg)
                if lo <= max_deg <= hi:
                    layer_edges.append(e)
            layers.append({
                "scale": scale,
                "label": label,
                "degree_range": (lo, hi if hi != float('inf') else "inf"),
                "edges": layer_edges,
            })

        return layers

    def complexity_profile(self):
        # type: () -> dict
        """
        Compute multi-scale complexity profile for the knowledge graph.

        At each scale layer, computes:
          - H_input: Shannon entropy of edge-type distribution at the
            previous (lower) scale layer (or 0 for the lowest scale)
          - H_output: Shannon entropy of edge-type distribution at this
            scale layer
          - emergence: H_output - H_input (information the scale produces)
          - self_organization: -emergence (the paper's scalar inverse)
          - complexity: 4 * emergence * self_organization / (emergence -
            self_organization)^2 when both are nonzero, else 0.
            This is a normalized balance measure: maximal (=1) when
            |emergence| == |self_organization|, collapsing toward 0 when
            either dominates.
          - gradient_flow_flag: diagnostic for INV_073 — True when the
            scalar self_organization is likely an inadequate proxy for
            Wasserstein gradient flow dynamics at this scale.

        Returns
        -------
        dict
            {
                "n_scales": int,
                "scales": list of dict — per-scale metrics,
                "aggregate_complexity": float — mean complexity across scales,
                "criticality_status": str — "healthy", "emergence_dominated",
                    "self_org_dominated", or "collapsed",
                "inv073_warning": str or None — warning if scalar inverse
                    approximation is inadequate at any scale,
                "timestamp": str,
            }
        """
        self._ensure_loaded()

        layers = self._build_scale_layers()
        scales = []  # type: List[dict]
        prev_entropy = 0.0

        for layer in layers:
            h_output = self._compute_edge_type_entropy(layer["edges"])
            h_input = prev_entropy
            n_edges = len(layer["edges"])

            emergence = h_output - h_input
            self_org = -emergence  # Paper's scalar definition

            # Normalized balance measure for complexity:
            # C = 4 * E * S / (E - S)^2   when E != 0 and S != 0
            # Since S = -E, this simplifies to:
            # C = 4 * E * (-E) / (E - (-E))^2 = -4E^2 / (2E)^2 = -4E^2/4E^2 = -1
            # ... which shows the paper's strict scalar inverse is degenerate!
            # This IS the INV_073 challenge: when S = -E exactly, complexity
            # is a constant, losing all discriminative power.
            #
            # We use a modified measure that captures the BALANCE between
            # information production (H_output) and information consumption
            # (H_input) directly, avoiding the degenerate scalar inverse:
            # C = 1 - |H_output - H_input| / max(H_output, H_input, eps)
            # This is 1.0 when H_output == H_input (perfect balance),
            # 0.0 when one completely dominates.
            eps = 1e-12
            max_h = max(h_output, h_input, eps)
            complexity = 1.0 - abs(emergence) / max_h

            # Clamp to [0, 1]
            complexity = max(0.0, min(1.0, complexity))

            # Gradient flow diagnostic (INV_073):
            # The scalar self_org = -emergence is inadequate when:
            # 1. The scale has high edge diversity (many types) — geometric
            #    structure matters more than scalar information difference
            # 2. There are contradictions at this scale — the Wasserstein
            #    gradient flow must navigate opposing directions
            n_edge_types = len(set(
                e.get("type", "unknown") for e in layer["edges"]
            )) if layer["edges"] else 0

            # Check for contradictions at this scale
            scale_contradictions = detect_contradictions(
                layer["edges"], context_aware=True
            )
            n_contradictions = len([
                c for c in scale_contradictions
                if c.get("severity") == "true_contradiction"
            ])

            gradient_flow_flag = (n_edge_types >= 4 or n_contradictions > 0)

            scales.append({
                "scale": layer["scale"],
                "label": layer["label"],
                "degree_range": layer["degree_range"],
                "n_edges": n_edges,
                "h_input": round(h_input, 6),
                "h_output": round(h_output, 6),
                "emergence": round(emergence, 6),
                "self_organization": round(self_org, 6),
                "complexity": round(complexity, 6),
                "n_edge_types": n_edge_types,
                "n_contradictions": n_contradictions,
                "gradient_flow_flag": gradient_flow_flag,
            })

            # Current output becomes next layer's input
            prev_entropy = h_output

        # ── Aggregate metrics ────────────────────────────────────────────
        complexities = [s["complexity"] for s in scales if s["n_edges"] > 0]
        agg_complexity = (
            sum(complexities) / len(complexities) if complexities else 0.0
        )

        # Criticality status based on aggregate and per-scale patterns
        active_scales = [s for s in scales if s["n_edges"] > 0]
        if not active_scales:
            criticality = "collapsed"
        elif agg_complexity >= 0.6:
            criticality = "healthy"
        elif all(s["emergence"] > 0 for s in active_scales):
            criticality = "emergence_dominated"
        elif all(s["emergence"] < 0 for s in active_scales):
            criticality = "self_org_dominated"
        elif agg_complexity < 0.2:
            criticality = "collapsed"
        else:
            criticality = "healthy"

        # INV_073 warning: flag if any scale has gradient_flow_flag
        gradient_flagged = [
            s for s in scales if s.get("gradient_flow_flag")
        ]
        inv073_warning = None  # type: Optional[str]
        if gradient_flagged:
            flagged_labels = [s["label"] for s in gradient_flagged]
            inv073_warning = (
                f"Scalar self_organization=-emergence is inadequate proxy "
                f"for Wasserstein gradient flow at scale(s): "
                f"{', '.join(flagged_labels)}. "
                f"High edge-type diversity or contradictions require "
                f"geometric (non-scalar) complexity tracking."
            )

        ts = datetime.now(timezone.utc).isoformat()

        profile = {
            "n_scales": len(scales),
            "scales": scales,
            "aggregate_complexity": round(agg_complexity, 6),
            "criticality_status": criticality,
            "inv073_warning": inv073_warning,
            "timestamp": ts,
        }

        # ── Log summary ─────────────────────────────────────────────────
        active_count = len(active_scales)
        print(
            f"[GRAPH:COMPLEXITY_PROFILE] {active_count} active scale(s), "
            f"aggregate_complexity={agg_complexity:.4f}, "
            f"criticality={criticality}"
            + (f", INV_073_warning=True" if inv073_warning else "")
        )

        return profile

    def avalanche_from_feed(self, new_edges, recent_feed_count=1,
                            degree_threshold=None, max_depth=None):
        # type: (list, int, Optional[int], Optional[int]) -> dict
        """
        Convenience wrapper: run avalanche detection on nodes touched by
        edges just recorded from a FEED.

        Parameters
        ----------
        new_edges : list of dict
            Edges as returned by ``record_feed()``.
        recent_feed_count : int
            Number of feeds in the current batch (for regime classification).
        degree_threshold : int or None
            Override default degree threshold.
        max_depth : int or None
            Override default max cascade depth.

        Returns
        -------
        dict
            Avalanche detection result (see ``detect_avalanche``).
        """
        # Collect unique target node IDs from the new edges
        seed_ids = list(set(
            e.get("to", "").upper()
            for e in new_edges
            if e.get("to")
        ))
        return self.detect_avalanche(
            seed_ids,
            degree_threshold=degree_threshold,
            max_depth=max_depth,
            recent_feed_count=recent_feed_count,
        )

# ── Singleton accessor ────────────────────────────────────────────────────────

_graph_instance = None  # type: Optional[KnowledgeGraph]

def get_graph():
    # type: () -> KnowledgeGraph
    """Return the process-level singleton KnowledgeGraph."""
    global _graph_instance
    if _graph_instance is None:
        _graph_instance = KnowledgeGraph()
    return _graph_instance
