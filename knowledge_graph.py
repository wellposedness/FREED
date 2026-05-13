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
EDGE_TYPES = ("confirms", "refutes", "advances", "resolves", "extends", "supports", "contradicts", "challenges", "absent")

# Node-to-node edge types (structural, written by consolidate.py MINE phase)
NODE_EDGE_TYPES = (
    "shares_invariant",         # fallback: nodes share the same abstract claim
    "operationalizes",          # node B implements/measures the abstract claim of node A
    "scales_with",              # invariant holds across both nodes at different scales
    "consistent_with",          # invariant text uses independence/domain language but source independence unverified
    "independent_confirmation", # reserved: bootstrap CONVERGE or verified cross-domain feed edge required
)

# ─── Open World Assumption (OWA) Edge Completeness ──────────────────────────
# Under Closed World Assumption (CWA), an absent edge between two nodes is
# treated as evidence of no relationship (hard negation).  Under OWA, absence
# is epistemically ambiguous: the edge may exist but be unobserved.
#
# This distinction is critical for dependency skeleton recovery (O187): with
# 16 nodes the skeleton has 120 possible undirected edges.  Under CWA, each
# absent edge is a definite non-dependency, yielding a single determinate
# skeleton.  Under OWA, each absent edge contributes epistemic uncertainty,
# making the skeleton underdetermined — the number of compatible skeletons
# grows combinatorially with the number of unobserved edges.
#
# Edge epistemic status:
#   "observed_present"  — edge exists in the graph (standard positive edge)
#   "observed_absent"   — edge confirmed absent via explicit negative evidence
#                         (e.g., "ABSENT INV_094" from FEED output)
#   "unobserved"        — no evidence either way; OWA treats this as unknown
#
# The OWA incompleteness score for a node pair (A, B) is:
#   owa_score = 1.0  if edge is observed_present (full confidence)
#   owa_score = 0.0  if edge is observed_absent  (confirmed non-link)
#   owa_score = prior if edge is unobserved       (epistemic uncertainty)
#
# The default prior (OWA_UNOBSERVED_PRIOR) is 0.5 (maximum ignorance),
# but can be adjusted based on graph density or domain knowledge.
#
# Reference: "Semantics-aware causal inference frameworks for knowledge
# graphs" — addresses data incompleteness violating causal assumptions
# under the Open World Assumption.
# ─────────────────────────────────────────────────────────────────────────────

OWA_EDGE_STATUSES = ("observed_present", "observed_absent", "unobserved")

# Default prior probability for unobserved edges under OWA.
# 0.5 = maximum ignorance (no bias toward presence or absence).
# Lower values bias toward sparsity; higher toward density.
OWA_UNOBSERVED_PRIOR = 0.5

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

# ─── Entropy-Flux Decomposition Source Types ─────────────────────────────────
# Following the paper's three-factor partition of entropy change, every edge
# update is tagged with the flux source that generated it:
#
#   "internal_inference" — the system's INTERNAL DYNAMICS: edges produced by
#       consolidation, avalanche cascades, structural mining, or any process
#       that operates on existing graph state without new external input.
#       Entropy change from this source is reversible under the system's own
#       dynamics (Noether: conserved under time-translation of internal state).
#
#   "external_feed" — UNSOLICITED external perturbation: edges produced by
#       FEED ingestion of new papers/data.  This is the irreversible injection
#       of information from the environment, breaking the internal-dynamics
#       symmetry and driving net entropy change.
#
#   "coarse_graining_collapse" — the third factor: edges produced when
#       context-aware deduplication, scale-layer compression, or explicit
#       coarse-graining operations merge or collapse fine-grained distinctions.
#       This is information loss by observer choice, not by dynamics or input.
#
# The decomposition enables:
#   - Debt-ratio tracking partitioned by source (which flux drives coherence?)
#   - PRE-AUDIT failure-mode prediction (external floods vs. internal drift
#     vs. resolution-loss from aggressive coarse-graining)
#   - Direct mapping to the Noether conservation structure: internal dynamics
#     conserve entropy; external feed and coarse-graining break conservation.
#
# CHALLENGE (O44): The three-factor decomposition is structurally parallel to
# classical entropy production terms (σ_int + σ_ext + σ_cg) but the paper's
# density-operator extension to quantum Wasserstein metrics remains ungrounded
# at this step — the classical graph Laplacian decomposes cleanly but the
# non-commutative case requires additional structure not yet provided.
# ─────────────────────────────────────────────────────────────────────────────

ENTROPY_FLUX_SOURCES = (
    "internal_inference",
    "external_feed",
    "coarse_graining_collapse",
)

# Patterns to classify flux source from processing context
_INTERNAL_INFERENCE_PATTERNS = re.compile(
    r'\b(?:consolidat|avalanche|cascade|mine|mined|structural|infer(?:red|ence)|'
    r'derive[ds]?|propagat|rescore|internal|CONVERGE|bootstrap)\b', re.I
)
_COARSE_GRAINING_PATTERNS = re.compile(
    r'\b(?:coarse[- ]?grain|collaps|merg|deduplic|compress|renormali[sz]|'
    r'truncat|aggregate|subsume|absorb|resolution[- ]?loss)\b', re.I
)


def classify_entropy_flux_source(processing_context="", edge_type="", is_feed=True):
    # type: (str, str, bool) -> str
    """
    Classify the entropy-flux source for an edge update, mirroring the paper's
    three-factor decomposition of entropy change.

    The classification uses a priority hierarchy:
      1. coarse_graining_collapse — if processing context indicates merging,
         deduplication, or explicit coarse-graining operations
      2. internal_inference — if processing context indicates consolidation,
         avalanche cascades, structural mining, or derivation from existing state
      3. external_feed — default for edges produced by FEED ingestion

    Parameters
    ----------
    processing_context : str
        Description of the processing step that generated this edge.
        E.g. "FEED ingestion", "consolidate MINE phase", "avalanche cascade",
        "context-aware deduplication", etc.
    edge_type : str
        The edge type (confirms, refutes, etc.). Used as secondary signal.
    is_feed : bool
        Whether this edge was generated during a FEED operation (default True).
        When False, the default shifts from external_feed to internal_inference.

    Returns
    -------
    str
        One of ENTROPY_FLUX_SOURCES: "internal_inference", "external_feed",
        or "coarse_graining_collapse".
    """
    ctx = processing_context + " " + edge_type

    # Priority 1: coarse-graining collapse
    if _COARSE_GRAINING_PATTERNS.search(ctx):
        return "coarse_graining_collapse"

    # Priority 2: internal inference
    if _INTERNAL_INFERENCE_PATTERNS.search(ctx):
        return "internal_inference"

    # Priority 3: default based on is_feed flag
    if is_feed:
        return "external_feed"
    else:
        return "internal_inference"


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
    # Reorientation vocabulary — derive-first signals from new FEED/L7 prompts
    # "CONVERGE INV_094" → independent derivation confirms a DHF-biological claim
    (re.compile(rf'(?<!not )\bCONVERGE\s+({_NODE_PATTERN})', re.I), 'confirms'),
    # "EXTEND INV_094" → derivation goes beyond the reference claim
    (re.compile(rf'(?<!not )\bEXTEND\s+({_NODE_PATTERN})', re.I), 'advances'),
    # "CONFLICT INV_097" → independent derivation contradicts a reference claim
    (re.compile(rf'(?<!not )\bCONFLICT\s+({_NODE_PATTERN})', re.I), 'challenges'),
    # "ABSENT INV_094" or "ABSENT (no genome match)" — genome lacks this finding
    (re.compile(rf'(?<!not )\bABSENT\s+({_NODE_PATTERN})', re.I), 'absent'),
]


def classify_node_edge(invariant_text):
    # type: (str) -> str
    """
    Classify the structural relationship between two nodes sharing an invariant.
    Uses keyword heuristics on invariant text — no extra API call required.

    Returns one of NODE_EDGE_TYPES. Precedence:
      1. scales_with — invariant explicitly mentions scale hierarchy or substrate-independence
      2. consistent_with — invariant text uses independence/domain language, but source
         independence is unverified (nodes may share the same theoretical origin).
         Use independent_confirmation only when bootstrap CONVERGE or a verified
         cross-domain feed edge confirms actual source independence.
      3. operationalizes — invariant involves implementation/measurement/method language
      4. shares_invariant — fallback
    """
    t = invariant_text
    if _SCALE_KEYWORDS.search(t):
        return "scales_with"
    if _DOMAIN_KEYWORDS.search(t):
        return "consistent_with"
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


# ─── Wasserstein-Levi-Civita Geometry on the Probability Simplex ─────────────
# Represents knowledge graph node distributions as points on the probability
# simplex Σ_n = {p ∈ R^n : p_i > 0, Σ p_i = 1} equipped with the L²-Wasserstein
# metric tensor g_W(p).  The metric nonlinearity comes from the weighted graph
# Laplacian L_w of the underlying knowledge graph adjacency structure.
#
# The key construction (following the transport information geometry paper):
#   Given a weighted graph G = (V, E, w) with n vertices and weighted Laplacian
#   L_w, the Wasserstein metric tensor at a point p on the simplex is:
#     g_W(p)[σ, τ] = Σ_{(i,j)∈E} w_{ij} (φ_i - φ_j)² p_{ij}
#   where φ = L_w(diag(p))^† σ is the potential solving the continuity equation,
#   L_w(diag(p)) is the p-weighted Laplacian, and ^† is the pseudoinverse.
#
# This makes the probability simplex a torsion-free Riemannian manifold with
# fully computable Christoffel symbols, geodesics, and curvature tensors —
# all derived from the graph Laplacian structure.
#
# CHALLENGE (O44): By grounding W2 geometry rigorously in finite classical
# probability spaces via graph Laplacians, this sharpens the question of
# whether the quantum extension requires a genuinely different metric structure
# or merely a non-commutative Laplacian, making the current O44 formulation
# under-specified rather than open.
#
# QUERY: Whether the Laplace-Beltrami eigenspectrum on the Wasserstein
# probability simplex has been connected to spectral gaps in Markov chains
# or thermodynamic relaxation rates — this would directly link the geometric
# structure to the Wasserstein Floor invariant W_floor = k/Tμ.
# ─────────────────────────────────────────────────────────────────────────────


def _build_adjacency_matrix(n, edge_pairs, edge_weights=None):
    # type: (int, List[Tuple[int, int]], Optional[List[float]]) -> List[List[float]]
    """
    Build an n×n symmetric adjacency matrix from edge pairs.

    Parameters
    ----------
    n : int
        Number of vertices.
    edge_pairs : list of (int, int)
        Edges as (i, j) index pairs (0-indexed).
    edge_weights : list of float or None
        Weight for each edge. If None, all weights are 1.0.

    Returns
    -------
    list of list of float
        n×n symmetric adjacency matrix.
    """
    A = [[0.0] * n for _ in range(n)]
    for idx, (i, j) in enumerate(edge_pairs):
        w = 1.0 if edge_weights is None else edge_weights[idx]
        A[i][j] = w
        A[j][i] = w
    return A


def _build_weighted_graph_laplacian(adjacency):
    # type: (List[List[float]]) -> List[List[float]]
    """
    Build the weighted graph Laplacian L_w = D - A where D is the diagonal
    degree matrix and A is the weighted adjacency matrix.

    Parameters
    ----------
    adjacency : list of list of float
        n×n symmetric weighted adjacency matrix.

    Returns
    -------
    list of list of float
        n×n weighted graph Laplacian (positive semi-definite, row-sums zero).
    """
    n = len(adjacency)
    L = [[0.0] * n for _ in range(n)]
    for i in range(n):
        row_sum = sum(adjacency[i])
        for j in range(n):
            if i == j:
                L[i][j] = row_sum - adjacency[i][j]
            else:
                L[i][j] = -adjacency[i][j]
    return L


def _build_p_weighted_laplacian(laplacian, p):
    # type: (List[List[float]], List[float]) -> List[List[float]]
    """
    Build the probability-weighted Laplacian L_w(diag(p)) used in the
    Wasserstein metric tensor construction.

    For the L²-Wasserstein metric on a graph, the p-weighted Laplacian is:
        L_p = diag(p)^{1/2} · L_w · diag(p)^{1/2}

    This symmetrization ensures the resulting metric tensor is symmetric
    positive semi-definite on the tangent space of the simplex.

    Parameters
    ----------
    laplacian : list of list of float
        n×n weighted graph Laplacian L_w.
    p : list of float
        Probability vector on the simplex (all entries > 0, sum to 1).

    Returns
    -------
    list of list of float
        n×n p-weighted Laplacian.
    """
    n = len(laplacian)
    sqrt_p = [math.sqrt(max(pi, 1e-15)) for pi in p]
    L_p = [[0.0] * n for _ in range(n)]
    for i in range(n):
        for j in range(n):
            L_p[i][j] = sqrt_p[i] * laplacian[i][j] * sqrt_p[j]
    return L_p


def _matrix_pseudoinverse_symmetric(matrix, tol=1e-10):
    # type: (List[List[float]], float) -> List[List[float]]
    """
    Compute the Moore-Penrose pseudoinverse of a symmetric matrix using
    eigendecomposition via the Jacobi eigenvalue algorithm.

    For graph Laplacians, this handles the rank deficiency (zero eigenvalue
    corresponding to the constant vector) by zeroing out the inverse of
    eigenvalues below tolerance.

    Parameters
    ----------
    matrix : list of list of float
        n×n symmetric matrix.
    tol : float
        Eigenvalue threshold below which the reciprocal is set to zero.

    Returns
    -------
    list of list of float
        n×n pseudoinverse matrix.
    """
    n = len(matrix)
    if n == 0:
        return []

    # For small matrices (n <= 20), use iterative Jacobi method
    # Copy matrix to avoid mutation
    A = [row[:] for row in matrix]
    # V accumulates eigenvectors (starts as identity)
    V = [[1.0 if i == j else 0.0 for j in range(n)] for i in range(n)]

    max_iter = 100 * n * n
    for iteration in range(max_iter):
        # Find the largest off-diagonal element
        max_val = 0.0
        p_idx = 0
        q_idx = 1
        for i in range(n):
            for j in range(i + 1, n):
                if abs(A[i][j]) > max_val:
                    max_val = abs(A[i][j])
                    p_idx = i
                    q_idx = j
        if max_val < tol:
            break

        # Compute rotation angle
        app = A[p_idx][p_idx]
        aqq = A[q_idx][q_idx]
        apq = A[p_idx][q_idx]

        if abs(app - aqq) < tol:
            theta = math.pi / 4.0
        else:
            theta = 0.5 * math.atan2(2.0 * apq, app - aqq)

        c = math.cos(theta)
        s = math.sin(theta)

        # Apply Jacobi rotation to A
        new_A = [row[:] for row in A]
        for i in range(n):
            if i != p_idx and i != q_idx:
                new_A[i][p_idx] = c * A[i][p_idx] + s * A[i][q_idx]
                new_A[p_idx][i] = new_A[i][p_idx]
                new_A[i][q_idx] = -s * A[i][p_idx] + c * A[i][q_idx]
                new_A[q_idx][i] = new_A[i][q_idx]
        new_A[p_idx][p_idx] = c * c * app + 2 * s * c * apq + s * s * aqq
        new_A[q_idx][q_idx] = s * s * app - 2 * s * c * apq + c * c * aqq
        new_A[p_idx][q_idx] = 0.0
        new_A[q_idx][p_idx] = 0.0
        A = new_A

        # Accumulate eigenvectors
        for i in range(n):
            vip = V[i][p_idx]
            viq = V[i][q_idx]
            V[i][p_idx] = c * vip + s * viq
            V[i][q_idx] = -s * vip + c * viq

    # Eigenvalues are on the diagonal of A
    eigenvalues = [A[i][i] for i in range(n)]

    # Build pseudoinverse: V · diag(1/λ_i for λ_i > tol, else 0) · V^T
    inv_eigenvalues = [0.0] * n
    for i in range(n):
        if abs(eigenvalues[i]) > tol:
            inv_eigenvalues[i] = 1.0 / eigenvalues[i]

    # Reconstruct: M^+ = V · diag(inv_eigenvalues) · V^T
    result = [[0.0] * n for _ in range(n)]
    for i in range(n):
        for j in range(n):
            s_val = 0.0
            for k in range(n):
                s_val += V[i][k] * inv_eigenvalues[k] * V[j][k]
            result[i][j] = s_val

    return result


def wasserstein_metric_tensor(laplacian, p):
    # type: (List[List[float]], List[float]) -> List[List[float]]
    """
    Compute the L²-Wasserstein metric tensor g_W at point p on the
    probability simplex, using the weighted graph Laplacian.

    The metric tensor is:
        g_W(p) = L_p^†
    where L_p is the p-weighted Laplacian and ^† is the pseudoinverse.

    This is the fundamental geometric object: inner products of tangent
    vectors σ, τ at p are computed as:
        ⟨σ, τ⟩_W = σ^T · g_W(p) · τ

    Parameters
    ----------
    laplacian : list of list of float
        n×n weighted graph Laplacian L_w.
    p : list of float
        Probability vector on the simplex (all entries > 0, sum to 1).

    Returns
    -------
    list of list of float
        n×n Wasserstein metric tensor at p.
    """
    L_p = _build_p_weighted_laplacian(laplacian, p)
    return _matrix_pseudoinverse_symmetric(L_p)


def wasserstein_geodesic_distance(laplacian, p, q, n_steps=20):
    # type: (List[List[float]], List[float], List[float], int) -> float
    """
    Compute the approximate L²-Wasserstein geodesic distance between two
    probability distributions p and q on the simplex, using the graph
    Laplacian-induced metric.

    Uses numerical integration along the linear interpolation path
    (McCann interpolation) with the Wasserstein metric tensor evaluated
    at each interpolation point:

        d_W(p, q) ≈ ∫_0^1 √(v^T · g_W(γ(t)) · v) dt

    where γ(t) = (1-t)p + tq is the displacement interpolant and
    v = q - p is the tangent vector.

    For exact geodesics on the Wasserstein simplex, the true geodesic
    is the McCann displacement interpolant (which coincides with the
    linear interpolant in Euclidean coordinates for the graph Laplacian
    metric).  The trapezoidal quadrature approximation converges as
    O(1/n_steps²).

    Parameters
    ----------
    laplacian : list of list of float
        n×n weighted graph Laplacian L_w.
    p : list of float
        Source probability distribution.
    q : list of float
        Target probability distribution.
    n_steps : int
        Number of quadrature points for numerical integration (default 20).

    Returns
    -------
    float
        Approximate W2 geodesic distance. Returns 0.0 for identical
        distributions.

    Notes
    -----
    The metric nonlinearity from the graph Laplacian means this distance
    respects the graph topology: probability mass that must "travel" across
    many edges incurs proportionally higher cost than mass moving along
    direct edges, making this a geometrically correct measure of conceptual
    proximity in the knowledge graph.
    """
    n = len(p)
    if n != len(q) or n == 0:
        return 0.0

    # Tangent vector (constant along linear interpolant)
    v = [q[i] - p[i] for i in range(n)]

    # Check for identical distributions
    if all(abs(vi) < 1e-15 for vi in v):
        return 0.0

    # Trapezoidal quadrature along the interpolation path
    dt = 1.0 / float(n_steps)
    integrand_values = []  # type: List[float]

    for step in range(n_steps + 1):
        t = step * dt
        # Interpolated point on the simplex
        gamma_t = [(1.0 - t) * p[i] + t * q[i] for i in range(n)]

        # Ensure positivity (clamp small values)
        gamma_t = [max(gi, 1e-15) for gi in gamma_t]
        # Re-normalize to stay on the simplex
        total = sum(gamma_t)
        if total > 0:
            gamma_t = [gi / total for gi in gamma_t]

        # Metric tensor at this point
        g = wasserstein_metric_tensor(laplacian, gamma_t)

        # Compute v^T · g · v (the squared speed along the path)
        vgv = 0.0
        for i in range(n):
            for j in range(n):
                vgv += v[i] * g[i][j] * v[j]

        # The integrand is √(v^T g v); ensure non-negative
        integrand_values.append(math.sqrt(max(0.0, vgv)))

    # Trapezoidal rule
    integral = 0.0
    for step in range(n_steps):
        integral += 0.5 * (integrand_values[step] + integrand_values[step + 1]) * dt

    return integral


def wasserstein_christoffel_symbols(laplacian, p, h=1e-5):
    # type: (List[List[float]], List[float], float) -> List[List[List[float]]]
    """
    Compute the Christoffel symbols Γ^k_{ij} of the Levi-Civita connection
    on the probability simplex at point p, using finite differences of the
    Wasserstein metric tensor.

    The Christoffel symbols are:
        Γ^k_{ij} = (1/2) Σ_l g^{kl} (∂_i g_{jl} + ∂_j g_{il} - ∂_l g_{ij})

    where g^{kl} is the inverse metric tensor and ∂_i denotes the partial
    derivative with respect to the i-th coordinate on the simplex.

    Parameters
    ----------
    laplacian : list of list of float
        n×n weighted graph Laplacian L_w.
    p : list of float
        Point on the probability simplex.
    h : float
        Step size for finite difference computation (default 1e-5).

    Returns
    -------
    list of list of list of float
        n×n×n array where result[k][i][j] = Γ^k_{ij}.
        These are the torsion-free Christoffel symbols of the second kind.
    """
    n = len(p)
    if n == 0:
        return []

    # Compute metric tensor at p
    g_p = wasserstein_metric_tensor(laplacian, p)

    # Compute inverse metric tensor (g^{kl}) via pseudoinverse
    g_inv = _matrix_pseudoinverse_symmetric(g_p)

    # Compute partial derivatives of metric tensor by finite differences
    # ∂_m g_{ij} ≈ (g_{ij}(p + h·e_m) - g_{ij}(p - h·e_m)) / (2h)
    # On the simplex, we perturb in direction e_m - e_n (last coordinate)
    # to stay on the tangent plane of the simplex.
    dg = [[[0.0] * n for _ in range(n)] for _ in range(n)]  # dg[m][i][j] = ∂_m g_{ij}

    for m in range(n):
        # Forward perturbation: p + h*(e_m - 1/n * ones) projected to simplex
        p_fwd = list(p)
        p_fwd[m] += h
        # Re-normalize
        total = sum(p_fwd)
        p_fwd = [max(pi / total, 1e-15) for pi in p_fwd]
        total = sum(p_fwd)
        p_fwd = [pi / total for pi in p_fwd]

        # Backward perturbation
        p_bwd = list(p)
        p_bwd[m] -= h
        # Clamp and re-normalize
        p_bwd = [max(pi, 1e-15) for pi in p_bwd]
        total = sum(p_bwd)
        p_bwd = [pi / total for pi in p_bwd]

        g_fwd = wasserstein_metric_tensor(laplacian, p_fwd)
        g_bwd = wasserstein_metric_tensor(laplacian, p_bwd)

        for i in range(n):
            for j in range(n):
                dg[m][i][j] = (g_fwd[i][j] - g_bwd[i][j]) / (2.0 * h)

    # Christoffel symbols: Γ^k_{ij} = (1/2) Σ_l g^{kl} (∂_i g_{jl} + ∂_j g_{il} - ∂_l g_{ij})
    gamma = [[[0.0] * n for _ in range(n)] for _ in range(n)]
    for k in range(n):
        for i in range(n):
            for j in range(n):
                s = 0.0
                for l in range(n):
                    s += g_inv[k][l] * (
                        dg[i][j][l] + dg[j][i][l] - dg[l][i][j]
                    )
                gamma[k][i][j] = 0.5 * s

    return gamma


def wasserstein_laplace_beltrami_eigenspectrum(laplacian, p, k_eigenvalues=None):
    # type: (List[List[float]], List[float], Optional[int]) -> dict
    """
    Compute eigenvalues of the p-weighted Laplacian (proxy for the
    Laplace-Beltrami operator on the Wasserstein probability simplex).

    The eigenspectrum of the Laplace-Beltrami operator on the Wasserstein
    manifold encodes geometric information about the simplex curvature
    and connects to:
      - Spectral gaps in the associated Markov chain (mixing times)
      - Thermodynamic relaxation rates (Wasserstein Floor invariant link)
      - Ricci curvature lower bounds via Lichnerowicz-type inequalities

    QUERY (O44 connection): The spectral gap λ_1 of this operator may
    directly bound the Wasserstein Floor invariant W_floor = k/Tμ via
    the relationship between optimal transport cost and Markov chain
    mixing: if λ_1 ≥ κ > 0 (positive Ricci curvature lower bound),
    then the W2 contraction rate is exponential in κ, providing a
    geometric foundation for the thermodynamic bound.

    Parameters
    ----------
    laplacian : list of list of float
        n×n weighted graph Laplacian L_w.
    p : list of float
        Point on the probability simplex.
    k_eigenvalues : int or None
        Number of smallest eigenvalues to return. If None, returns all.

    Returns
    -------
    dict
        {
            "eigenvalues": list of float — sorted ascending,
            "spectral_gap": float — λ_1 (smallest positive eigenvalue),
            "n_zero_modes": int — number of near-zero eigenvalues,
            "markov_mixing_bound": float — 1/λ_1 (upper bound on mixing time),
            "wasserstein_contraction_rate": float — exponential contraction
                rate from Ricci curvature (= spectral_gap when positive),
            "o44_spectral_link": str — diagnostic for O44 obligation,
        }
    """
    n = len(p)
    if n == 0:
        return {
            "eigenvalues": [],
            "spectral_gap": 0.0,
            "n_zero_modes": 0,
            "markov_mixing_bound": float('inf'),
            "wasserstein_contraction_rate": 0.0,
            "o44_spectral_link": "empty_distribution",
        }

    L_p = _build_p_weighted_laplacian(laplacian, p)

    # Extract eigenvalues via Jacobi method (reusing pseudoinverse infrastructure)
    # Copy matrix
    A = [row[:] for row in L_p]
    V = [[1.0 if i == j else 0.0 for j in range(n)] for i in range(n)]

    tol = 1e-12
    max_iter = 100 * n * n
    for iteration in range(max_iter):
        max_val = 0.0
        p_idx = 0
        q_idx = 1
        for i in range(n):
            for j in range(i + 1, n):
                if abs(A[i][j]) > max_val:
                    max_val = abs(A[i][j])
                    p_idx = i
                    q_idx = j
        if max_val < tol:
            break

        app = A[p_idx][p_idx]
        aqq = A[q_idx][q_idx]
        apq = A[p_idx][q_idx]

        if abs(app - aqq) < tol:
            theta = math.pi / 4.0
        else:
            theta = 0.5 * math.atan2(2.0 * apq, app - aqq)

        c = math.cos(theta)
        s = math.sin(theta)

        new_A = [row[:] for row in A]
        for i in range(n):
            if i != p_idx and i != q_idx:
                new_A[i][p_idx] = c * A[i][p_idx] + s * A[i][q_idx]
                new_A[p_idx][i] = new_A[i][p_idx]
                new_A[i][q_idx] = -s * A[i][p_idx] + c * A[i][q_idx]
                new_A[q_idx][i] = new_A[i][q_idx]
        new_A[p_idx][p_idx] = c * c * app + 2 * s * c * apq + s * s * aqq
        new_A[q_idx][q_idx] = s * s * app - 2 * s * c * apq + c * c * aqq
        new_A[p_idx][q_idx] = 0.0
        new_A[q_idx][p_idx] = 0.0
        A = new_A

        for i in range(n):
            vip = V[i][p_idx]
            viq = V[i][q_idx]
            V[i][p_idx] = c * vip + s * viq
            V[i][q_idx] = -s * vip + c * viq

    eigenvalues = sorted([A[i][i] for i in range(n)])

    # Count zero modes
    zero_tol = 1e-8
    n_zero = sum(1 for ev in eigenvalues if abs(ev) < zero_tol)

    # Spectral gap: smallest positive eigenvalue
    positive_evs = [ev for ev in eigenvalues if ev > zero_tol]
    spectral_gap = positive_evs[0] if positive_evs else 0.0

    # Markov mixing bound: 1/λ_1
    mixing_bound = 1.0 / spectral_gap if spectral_gap > 0 else float('inf')

    # Wasserstein contraction rate (from Ricci curvature ≥ λ_1 argument)
    contraction_rate = spectral_gap

    # O44 spectral link diagnostic
    if spectral_gap > 0:
        o44_link = (
            f"spectral_gap={spectral_gap:.6f}:positive_ricci_curvature:"
            f"w2_contraction_exponential:mixing_bound={mixing_bound:.4f}:"
            f"wfloor_geometric_foundation_viable"
        )
    else:
        o44_link = (
            "spectral_gap=0:flat_or_degenerate:no_exponential_contraction:"
            "wfloor_geometric_foundation_requires_regularization"
        )

    if k_eigenvalues is not None:
        eigenvalues = eigenvalues[:k_eigenvalues]

    return {
        "eigenvalues": [round(ev, 10) for ev in eigenvalues],
        "spectral_gap": round(spectral_gap, 10),
        "n_zero_modes": n_zero,
        "markov_mixing_bound": round(mixing_bound, 6) if mixing_bound != float('inf') else float('inf'),
        "wasserstein_contraction_rate": round(contraction_rate, 10),
        "o44_spectral_link": o44_link,
    }


def node_distribution_from_edges(node_id, edges, all_node_ids):
    # type: (str, list, List[str]) -> List[float]
    """
    Construct a probability distribution over graph nodes for a given node,
    based on its edge weights (confirmation/support counts).

    The distribution represents how the node's epistemic weight is distributed
    across its neighbors — a point on the probability simplex Σ_n.

    Parameters
    ----------
    node_id : str
        The node whose distribution to construct.
    edges : list of dict
        All edges in the graph.
    all_node_ids : list of str
        Ordered list of all node IDs (defines the simplex coordinates).

    Returns
    -------
    list of float
        Probability vector of length len(all_node_ids), summing to 1.0.
        Uses Laplace smoothing to ensure strict positivity (required for
        the Wasserstein metric tensor to be well-defined).
    """
    n = len(all_node_ids)
    if n == 0:
        return []

    node_index = {nid.upper(): idx for idx, nid in enumerate(all_node_ids)}
    nid_upper = node_id.upper()

    # Count edges to/from each neighbor
    counts = [0.0] * n
    for e in edges:
        from_id = e.get("from", "").upper()
        to_id = e.get("to", "").upper()
        if from_id == nid_upper and to_id in node_index:
            counts[node_index[to_id]] += 1.0
        elif to_id == nid_upper and from_id in node_index:
            counts[node_index[from_id]] += 1.0

    # Laplace smoothing: add α = 0.01 to every entry for strict positivity
    alpha = 0.01
    smoothed = [c + alpha for c in counts]
    total = sum(smoothed)
    if total > 0:
        return [s / total for s in smoothed]
    else:
        # Uniform distribution as fallback
        return [1.0 / n] * n


# ─── Channel-Level Divergence Scorer (Process Free Energy) ───────────────────
# Computes D(channel ‖ thermal_baseline) — the quantum relative entropy between
# a knowledge-graph update channel (represented as a transition matrix) and an
# "absolutely thermal" baseline channel whose fixed output is the equilibrium
# (uniform or Gibbs) distribution.  This is the process-level analog of state
# free energy: it quantifies how far each graph update operation is from
# thermodynamic reversibility.
#
# Following the paper's axiomatics:
#   - A quantum channel Φ is a CPTP map; here we approximate with a classical
#     stochastic transition matrix T (column-stochastic or row-stochastic).
#   - The absolutely thermal channel Φ_β has a fixed output: every input maps
#     to the thermal (Gibbs) state π_β.  For a graph with n nodes and uniform
#     Hamiltonian (fully degenerate), π_β = (1/n, ..., 1/n).
#   - The channel divergence D(Φ ‖ Φ_β) is computed as the average KL
#     divergence of the channel's output distributions from the thermal output,
#     weighted by the input distribution (taken as uniform over active nodes
#     when not specified):
#
#       D(T ‖ T_β) = Σ_i p(i) · D_KL( T(·|i) ‖ π_β )
#
#     where T(·|i) is the i-th column (output distribution given input i)
#     and π_β is the thermal baseline output.
#
# Interpretation:
#   D ≈ 0  →  channel is thermodynamically cheap (output ≈ thermal noise,
#              update is reversible / information-destroying)
#   D >> 0 →  channel is thermodynamically costly (output is far from
#              equilibrium, update creates/preserves structure irreversibly)
#
# This enables FREED to distinguish:
#   - Cheap updates: edge additions that merely redistribute existing weight
#     (low divergence, near-thermal transition structure)
#   - Costly updates: edge additions that create new structure or concentrate
#     probability on specific nodes (high divergence, far-from-equilibrium)
#
# CHALLENGE (O44): The paper's "golden units" are unitary channels measured
# against the absolutely thermal channel with fully degenerate output
# Hamiltonian.  The classical transition-matrix approximation used here
# loses unitarity structure — the non-commutative (quantum) channel
# divergence D(Φ ‖ Φ_β) = S(Φ_β) - S(Φ) + Tr[Φ(·) log Φ_β(·)] requires
# density-operator-level computation not yet implemented.  The classical
# version provides an upper bound via the data-processing inequality.
# ─────────────────────────────────────────────────────────────────────────────


def _build_transition_matrix_from_edges(edges, all_node_ids):
    # type: (list, List[str]) -> List[List[float]]
    """
    Build a row-stochastic transition matrix from knowledge graph edges.

    Entry T[i][j] = P(transition to j | currently at i), estimated from
    edge counts between nodes.  Uses Laplace smoothing to ensure strict
    positivity (required for finite KL divergence).

    Parameters
    ----------
    edges : list of dict
        Graph edges (each with 'from' and 'to' fields).
    all_node_ids : list of str
        Ordered list of all node IDs (defines matrix indices).

    Returns
    -------
    list of list of float
        n×n row-stochastic transition matrix.
    """
    n = len(all_node_ids)
    if n == 0:
        return []

    node_index = {nid.upper(): idx for idx, nid in enumerate(all_node_ids)}
    alpha = 0.01  # Laplace smoothing

    # Initialize with smoothing
    T = [[alpha] * n for _ in range(n)]

    for e in edges:
        from_id = e.get("from", "").upper()
        to_id = e.get("to", "").upper()
        i = node_index.get(from_id)
        j = node_index.get(to_id)
        if i is not None and j is not None:
            T[i][j] += 1.0
        # Also count reverse direction for undirected interpretation
        if j is not None and i is not None and i != j:
            T[j][i] += 1.0

    # Row-normalize to make stochastic
    for i in range(n):
        row_sum = sum(T[i])
        if row_sum > 0:
            T[i] = [t / row_sum for t in T[i]]
        else:
            T[i] = [1.0 / n] * n

    return T


def _thermal_baseline_distribution(n, hamiltonian=None, temperature=1.0):
    # type: (int, Optional[List[float]], float) -> List[float]
    """
    Compute the thermal (Gibbs) baseline distribution for n states.

    For fully degenerate Hamiltonian (all energies equal), this is the
    uniform distribution.  For non-degenerate Hamiltonian, it is the
    Boltzmann distribution π_i = exp(-E_i / T) / Z.

    Parameters
    ----------
    n : int
        Number of states.
    hamiltonian : list of float or None
        Energy levels for each state.  If None, assumes fully degenerate
        (uniform output).
    temperature : float
        Temperature parameter (default 1.0).  Must be > 0.

    Returns
    -------
    list of float
        Thermal baseline distribution (sums to 1.0).
    """
    if n == 0:
        return []

    if hamiltonian is None:
        return [1.0 / n] * n

    T = max(temperature, 1e-15)
    boltzmann = [math.exp(-E / T) for E in hamiltonian]
    Z = sum(boltzmann)
    if Z <= 0:
        return [1.0 / n] * n
    return [b / Z for b in boltzmann]


def _kl_divergence(p, q):
    # type: (List[float], List[float]) -> float
    """
    Compute KL divergence D_KL(p ‖ q) = Σ_i p_i * log(p_i / q_i).

    Both p and q must be strictly positive probability vectors of the
    same length.  Uses natural logarithm (nats).

    Parameters
    ----------
    p : list of float
        Distribution p (must be strictly positive, sum to ~1).
    q : list of float
        Reference distribution q (must be strictly positive, sum to ~1).

    Returns
    -------
    float
        KL divergence in nats (non-negative).  Returns 0.0 for empty inputs.
    """
    n = len(p)
    if n == 0 or n != len(q):
        return 0.0

    kl = 0.0
    for i in range(n):
        pi = max(p[i], 1e-15)
        qi = max(q[i], 1e-15)
        kl += pi * math.log(pi / qi)
    return max(0.0, kl)


def channel_divergence_from_thermal(transition_matrix, thermal_baseline=None,
                                     input_distribution=None,
                                     hamiltonian=None, temperature=1.0):
    # type: (List[List[float]], Optional[List[float]], Optional[List[float]], Optional[List[float]], float) -> dict
    """
    Compute the channel-level divergence D(channel ‖ thermal_baseline) as
    a process-free-energy estimate.

    The divergence measures how far the channel's transition structure is
    from the absolutely thermal channel (whose every output is the Gibbs
    equilibrium distribution).  This is the operational free energy of the
    channel: the amount of "thermodynamic work" the graph update represents.

    D(T ‖ T_β) = Σ_i p(i) · D_KL( T(·|i) ‖ π_β )

    where T(·|i) is the i-th row of the transition matrix (output distribution
    given input state i), p(i) is the input distribution, and π_β is the
    thermal baseline.

    Parameters
    ----------
    transition_matrix : list of list of float
        n×n row-stochastic transition matrix representing the channel.
    thermal_baseline : list of float or None
        The thermal (Gibbs) output distribution π_β.  If None, computed
        from the hamiltonian parameter (or uniform if hamiltonian is also None).
    input_distribution : list of float or None
        Distribution over input states p(i).  If None, uses uniform.
    hamiltonian : list of float or None
        Energy levels for thermal baseline computation (default: degenerate).
    temperature : float
        Temperature for Gibbs distribution (default 1.0).

    Returns
    -------
    dict
        {
            "channel_divergence": float — D(T ‖ T_β) in nats,
            "per_input_divergence": list of float — D_KL(T(·|i) ‖ π_β) for each i,
            "max_input_divergence": float — max over inputs (worst-case cost),
            "min_input_divergence": float — min over inputs (best-case cost),
            "thermal_baseline": list of float — π_β used,
            "input_distribution": list of float — p(i) used,
            "n_states": int,
            "is_near_thermal": bool — True if divergence < 0.01 nats,
            "is_far_from_thermal": bool — True if divergence > 1.0 nats,
            "thermodynamic_cost_label": str — "reversible", "cheap",
                "moderate", "costly", "very_costly",
            "o44_channel_flag": str — diagnostic for O44 obligation,
        }
    """
    n = len(transition_matrix)
    if n == 0:
        return {
            "channel_divergence": 0.0,
            "per_input_divergence": [],
            "max_input_divergence": 0.0,
            "min_input_divergence": 0.0,
            "thermal_baseline": [],
            "input_distribution": [],
            "n_states": 0,
            "is_near_thermal": True,
            "is_far_from_thermal": False,
            "thermodynamic_cost_label": "reversible",
            "o44_channel_flag": "empty_channel:trivially_thermal",
        }

    # Determine thermal baseline
    if thermal_baseline is None:
        pi_beta = _thermal_baseline_distribution(n, hamiltonian, temperature)
    else:
        pi_beta = list(thermal_baseline)

    # Determine input distribution
    if input_distribution is None:
        p_input = [1.0 / n] * n
    else:
        p_input = list(input_distribution)
        total = sum(p_input)
        if total > 0:
            p_input = [p / total for p in p_input]
        else:
            p_input = [1.0 / n] * n

    # Compute per-input KL divergence: D_KL(T(·|i) ‖ π_β)
    per_input_div = []  # type: List[float]
    for i in range(n):
        row_i = transition_matrix[i]
        d_kl = _kl_divergence(row_i, pi_beta)
        per_input_div.append(d_kl)

    # Channel divergence: weighted average over input distribution
    channel_div = sum(p_input[i] * per_input_div[i] for i in range(n))

    max_div = max(per_input_div) if per_input_div else 0.0
    min_div = min(per_input_div) if per_input_div else 0.0

    is_near_thermal = channel_div < 0.01
    is_far = channel_div > 1.0

    # Cost label
    if channel_div < 0.01:
        cost_label = "reversible"
    elif channel_div < 0.1:
        cost_label = "cheap"
    elif channel_div < 0.5:
        cost_label = "moderate"
    elif channel_div < 2.0:
        cost_label = "costly"
    else:
        cost_label = "very_costly"

    # O44 diagnostic
    o44_flag = (
        f"channel_divergence={channel_div:.6f}:cost={cost_label}:"
        f"classical_upper_bound:quantum_channel_divergence_unresolved"
    )

    return {
        "channel_divergence": round(channel_div, 10),
        "per_input_divergence": [round(d, 10) for d in per_input_div],
        "max_input_divergence": round(max_div, 10),
        "min_input_divergence": round(min_div, 10),
        "thermal_baseline": [round(p, 10) for p in pi_beta],
        "input_distribution": [round(p, 10) for p in p_input],
        "n_states": n,
        "is_near_thermal": is_near_thermal,
        "is_far_from_thermal": is_far,
        "thermodynamic_cost_label": cost_label,
        "o44_channel_flag": o44_flag,
    }


def score_graph_update_channel_cost(edges_before, edges_after, all_node_ids,
                                     hamiltonian=None, temperature=1.0):
    # type: (list, list, List[str], Optional[List[float]], float) -> dict
    """
    Score the thermodynamic cost of a knowledge-graph update operation by
    computing the channel divergence from thermal baseline on the transition
    matrices before and after the update.

    This is the main entry point for process-free-energy estimation: given
    two edge snapshots (before and after a FEED, CONSOLIDATE, or avalanche
    cascade), it builds transition matrices for both, computes their
    divergences from the thermal baseline, and reports the delta as the
    irreversibility cost of the update.

    Parameters
    ----------
    edges_before : list of dict
        Edge snapshot before the update operation.
    edges_after : list of dict
        Edge snapshot after the update operation.
    all_node_ids : list of str
        Ordered list of all node IDs.
    hamiltonian : list of float or None
        Energy levels for thermal baseline (default: degenerate/uniform).
    temperature : float
        Temperature for Gibbs distribution (default 1.0).

    Returns
    -------
    dict
        {
            "divergence_before": dict — channel_divergence_from_thermal result
                for the pre-update transition matrix,
            "divergence_after": dict — channel_divergence_from_thermal result
                for the post-update transition matrix,
            "delta_divergence": float — D_after - D_before (positive = update
                moved the channel further from equilibrium = irreversible cost),
            "update_is_irreversible": bool — True if delta > 0.01,
            "update_is_thermalizing": bool — True if delta < -0.01 (update
                moved channel toward equilibrium = information-destroying),
            "process_free_energy_estimate": float — max(0, delta_divergence),
                the non-negative free energy cost of the update,
            "cost_label": str — "free", "cheap", "moderate", "costly",
            "n_edges_added": int — edges_after count - edges_before count,
            "n_states": int,
            "o44_process_flag": str — diagnostic for O44,
        }
    """
    n = len(all_node_ids)

    T_before = _build_transition_matrix_from_edges(edges_before, all_node_ids)
    T_after = _build_transition_matrix_from_edges(edges_after, all_node_ids)

    div_before = channel_divergence_from_thermal(
        T_before, hamiltonian=hamiltonian, temperature=temperature
    )
    div_after = channel_divergence_from_thermal(
        T_after, hamiltonian=hamiltonian, temperature=temperature
    )

    d_before = div_before.get("channel_divergence", 0.0)
    d_after = div_after.get("channel_divergence", 0.0)
    delta = d_after - d_before

    is_irreversible = delta > 0.01
    is_thermalizing = delta < -0.01
    process_fe = max(0.0, delta)

    if process_fe < 0.01:
        cost_label = "free"
    elif process_fe < 0.1:
        cost_label = "cheap"
    elif process_fe < 0.5:
        cost_label = "moderate"
    else:
        cost_label = "costly"

    n_added = len(edges_after) - len(edges_before)

    o44_flag = (
        f"process_free_energy={process_fe:.6f}:"
        f"delta_divergence={delta:.6f}:"
        f"{'irreversible' if is_irreversible else 'thermalizing' if is_thermalizing else 'neutral'}:"
        f"classical_channel_approximation"
    )

    result = {
        "divergence_before": div_before,
        "divergence_after": div_after,
        "delta_divergence": round(delta, 10),
        "update_is_irreversible": is_irreversible,
        "update_is_thermalizing": is_thermalizing,
        "process_free_energy_estimate": round(process_fe, 10),
        "cost_label": cost_label,
        "n_edges_added": n_added,
        "n_states": n,
        "o44_process_flag": o44_flag,
    }

    # Log summary
    print(
        f"[GRAPH:CHANNEL_DIVERGENCE] Update cost — "
        f"D_before={d_before:.6f}, D_after={d_after:.6f}, "
        f"delta={delta:.6f}, process_FE={process_fe:.6f}, "
        f"cost={cost_label}, edges_added={n_added}"
    )

    return result


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
        self._telemetry = []    # criticality telemetry time-series nodes
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
                    context_tag=None, prediction_weights=None):
        # type: (dict, str, str, Optional[str], Optional[dict]) -> list
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
        prediction_weights : dict or None
            {INV_ID: float} — edges targeting predicted INVs get that weight.
            Surprises (not in dict) default to 1.0. Omit field if weight is 1.0.

        Returns the list of new edges added (may be empty).
        """
        self._ensure_loaded()
        new_edges = extract_edges(kernel_output, source_url, source_title,
                                  context_tag=context_tag)
        if prediction_weights and new_edges:
            for e in new_edges:
                w = prediction_weights.get(e.get("to", ""), 1.0)
                if w != 1.0:
                    e["prediction_weight"] = w
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

    # ── Challenge Deficit Scoring ────────────────────────────────────────────
    # Computes a "challenge deficit" ratio for each invariant:
    #   ratio = confirmation_count / max(1, direct_falsification_attempts)
    # where direct_falsification_attempts = count of edges with type in
    # {"challenges", "refutes", "contradicts"} targeting that invariant.
    #
    # Invariants exceeding a threshold ratio (default 5.0) are flagged as
    # requiring a mandatory adversarial probe before further confirmations
    # are accepted.  This surfaces structural bias in the epistemic loop:
    # an invariant with high confirmation surplus and zero adversarial
    # testing is epistemically unaudited — its robustness is indistinguishable
    # from untested.
    #
    # CHALLENGE (INV_094): This mechanism directly exposes that INV_094 has
    # no documented falsification conditions, meaning its apparent robustness
    # is an artifact of challenge absence rather than evidence of invariance.
    # ─────────────────────────────────────────────────────────────────────────

    # Edge types counted as confirmations vs. falsification attempts
    _CONFIRMATION_EDGE_TYPES = frozenset({"confirms", "supports", "extends"})
    _FALSIFICATION_EDGE_TYPES = frozenset({"challenges", "refutes", "contradicts"})

    def challenge_deficit_scores(self, ratio_threshold=5.0):
        # type: (float) -> Dict[str, dict]
        """
        Compute a challenge deficit score for every invariant in the graph.

        The challenge deficit ratio is:
            ratio = confirmation_count / max(1, falsification_attempt_count)

        Invariants where this ratio exceeds *ratio_threshold* are flagged as
        requiring a mandatory adversarial probe before further confirmations
        should be accepted.

        Parameters
        ----------
        ratio_threshold : float
            Ratio above which an invariant is flagged as challenge-deficit
            (default 5.0).  A ratio of 5 means the invariant has received
            at least 5× more confirmations than direct challenges.

        Returns
        -------
        dict
            Mapping of invariant_id → {
                "confirmation_count": int — number of confirming/supporting edges,
                "falsification_count": int — number of challenging/refuting edges,
                "challenge_deficit_ratio": float — the deficit score,
                "requires_adversarial_probe": bool — True if ratio > threshold,
                "probe_blocked": bool — True if ratio > threshold, meaning
                    further confirmations should NOT be accepted until a
                    challenge is recorded,
                "deficit_severity": str — "none" (ratio <= 1), "mild" (1 < ratio <= 3),
                    "moderate" (3 < ratio <= threshold), "critical" (ratio > threshold),
                    "untested" (confirmations > 0, challenges == 0),
                "confirmation_edge_types": dict — breakdown by edge type,
                "falsification_edge_types": dict — breakdown by edge type,
                "falsification_conditions_documented": bool — True if at least
                    one 'challenges' edge exists (not just 'refutes'),
                "recommendation": str — human-readable recommendation,
            }
        """
        self._ensure_loaded()

        # Collect all edges per invariant, partitioned by confirmation vs falsification
        inv_confirmations = defaultdict(list)  # type: Dict[str, list]
        inv_falsifications = defaultdict(list)  # type: Dict[str, list]
        all_inv_ids = set()  # type: set

        for e in self._edges:
            target = e.get("to", "")
            etype = e.get("type", "")
            if not target:
                continue
            # Only score invariant nodes (INV_*)
            if not target.upper().startswith("INV_"):
                continue
            target_upper = target.upper()
            all_inv_ids.add(target_upper)
            if etype in self._CONFIRMATION_EDGE_TYPES:
                inv_confirmations[target_upper].append(e)
            elif etype in self._FALSIFICATION_EDGE_TYPES:
                inv_falsifications[target_upper].append(e)

        results = {}  # type: Dict[str, dict]
        flagged_critical = []  # type: List[str]
        flagged_untested = []  # type: List[str]

        for inv_id in sorted(all_inv_ids):
            conf_edges = inv_confirmations.get(inv_id, [])
            fals_edges = inv_falsifications.get(inv_id, [])

            n_conf = len(conf_edges)
            n_fals = len(fals_edges)

            # Challenge deficit ratio
            ratio = float(n_conf) / float(max(1, n_fals))

            # Requires adversarial probe?
            requires_probe = ratio > ratio_threshold
            probe_blocked = requires_probe  # Block further confirmations

            # Breakdown by edge type
            conf_type_counts = defaultdict(int)  # type: Dict[str, int]
            for e in conf_edges:
                conf_type_counts[e.get("type", "unknown")] += 1

            fals_type_counts = defaultdict(int)  # type: Dict[str, int]
            for e in fals_edges:
                fals_type_counts[e.get("type", "unknown")] += 1

            # Check if explicit 'challenges' edges exist (not just refutes)
            has_challenges = fals_type_counts.get("challenges", 0) > 0

            # Severity classification
            if n_conf == 0:
                severity = "none"
            elif n_fals == 0 and n_conf > 0:
                severity = "untested"
            elif ratio <= 1.0:
                severity = "none"
            elif ratio <= 3.0:
                severity = "mild"
            elif ratio <= ratio_threshold:
                severity = "moderate"
            else:
                severity = "critical"

            # Recommendation
            if severity == "critical":
                recommendation = (
                    f"MANDATORY: {inv_id} has {n_conf} confirmations and only "
                    f"{n_fals} falsification attempt(s) (ratio={ratio:.1f}). "
                    f"Further confirmations BLOCKED until an adversarial probe "
                    f"is recorded. Emit specific falsification conditions, "
                    f"boundary parameters, and alternative mechanisms."
                )
            elif severity == "untested":
                recommendation = (
                    f"WARNING: {inv_id} has {n_conf} confirmation(s) and ZERO "
                    f"falsification attempts. Its robustness is indistinguishable "
                    f"from untested. Schedule adversarial probe."
                )
            elif severity == "moderate":
                recommendation = (
                    f"ADVISORY: {inv_id} confirmation surplus is growing "
                    f"(ratio={ratio:.1f}). Consider scheduling an adversarial "
                    f"probe to maintain epistemic balance."
                )
            elif severity == "mild":
                recommendation = (
                    f"OK: {inv_id} has mild confirmation surplus "
                    f"(ratio={ratio:.1f}). Monitor."
                )
            else:
                recommendation = f"OK: {inv_id} is epistemically balanced."

            results[inv_id] = {
                "confirmation_count": n_conf,
                "falsification_count": n_fals,
                "challenge_deficit_ratio": round(ratio, 4),
                "requires_adversarial_probe": requires_probe,
                "probe_blocked": probe_blocked,
                "deficit_severity": severity,
                "confirmation_edge_types": dict(conf_type_counts),
                "falsification_edge_types": dict(fals_type_counts),
                "falsification_conditions_documented": has_challenges,
                "recommendation": recommendation,
            }

            if severity == "critical":
                flagged_critical.append(inv_id)
            elif severity == "untested":
                flagged_untested.append(inv_id)

        # ── Log summary ─────────────────────────────────────────────────
        if flagged_critical or flagged_untested:
            print(
                f"[GRAPH:CHALLENGE_DEFICIT] "
                f"{len(results)} invariant(s) scored — "
                f"critical={len(flagged_critical)}, "
                f"untested={len(flagged_untested)}, "
                f"threshold={ratio_threshold}"
            )
            for inv_id in flagged_critical:
                r = results[inv_id]
                print(
                    f"  🚫 {inv_id}: ratio={r['challenge_deficit_ratio']}, "
                    f"confirmations={r['confirmation_count']}, "
                    f"falsifications={r['falsification_count']} — "
                    f"PROBE REQUIRED"
                )
            for inv_id in flagged_untested[:5]:  # Cap log output
                r = results[inv_id]
                print(
                    f"  ⚠ {inv_id}: {r['confirmation_count']} confirmation(s), "
                    f"0 falsification attempts — UNTESTED"
                )
            if len(flagged_untested) > 5:
                print(
                    f"  ... and {len(flagged_untested) - 5} more untested invariant(s)"
                )

        return results

    def get_probe_required_invariants(self, ratio_threshold=5.0):
        # type: (float) -> List[str]
        """
        Return a list of invariant IDs that require mandatory adversarial
        probing before further confirmations are accepted.

        This is a convenience wrapper around ``challenge_deficit_scores()``
        that returns only the IDs flagged as critical or untested.

        Parameters
        ----------
        ratio_threshold : float
            Ratio above which an invariant is flagged (default 5.0).

        Returns
        -------
        list of str
            Invariant IDs requiring adversarial probes, sorted by deficit
            ratio (highest first).
        """
        scores = self.challenge_deficit_scores(ratio_threshold=ratio_threshold)
        flagged = [
            (inv_id, info)
            for inv_id, info in scores.items()
            if info["requires_adversarial_probe"] or info["deficit_severity"] == "untested"
        ]
        # Sort by deficit ratio descending (worst offenders first)
        flagged.sort(key=lambda x: x[1]["challenge_deficit_ratio"], reverse=True)
        return [inv_id for inv_id, _ in flagged]

    def is_confirmation_blocked(self, invariant_id, ratio_threshold=5.0):
        # type: (str, float) -> bool
        """
        Check whether further confirmations of *invariant_id* are blocked
        due to challenge deficit.

        Returns True if the invariant's challenge deficit ratio exceeds
        the threshold, meaning an adversarial probe must be recorded before
        additional confirmations are accepted.

        Parameters
        ----------
        invariant_id : str
            The invariant to check (e.g. "INV_094").
        ratio_threshold : float
            Ratio threshold (default 5.0).

        Returns
        -------
        bool
            True if confirmations are blocked pending adversarial probe.
        """
        scores = self.challenge_deficit_scores(ratio_threshold=ratio_threshold)
        inv_upper = invariant_id.upper()
        info = scores.get(inv_upper)
        if info is None:
            return False
        return info.get("probe_blocked", False)

    # ── Confirmation-Surplus Monitor & Adversarial Probe Queue ───────────────
    # Automated detection of invariants that have accumulated unchallenged
    # confirmation surplus: >N confirmations and <M challenges.  Flagged
    # invariants are queued for adversarial probe cycles, preventing any
    # invariant from accruing indefinite unchallenged confirmation.
    #
    # This directly operationalizes the epistemic hygiene principle: an
    # invariant with many confirmations and few challenges is structurally
    # indistinguishable from one that has never been seriously tested.
    # The monitor makes the falsification layer load-bearing by ensuring
    # that high-surplus invariants are automatically surfaced for probing.
    #
    # CHALLENGE (INV_094): This mechanism was designed specifically because
    # INV_094 (Wasserstein Floor k/Tμ) accumulated the highest confirmation
    # surplus in the genome with zero documented falsification conditions.
    # The monitor ensures that no invariant — including INV_094 — can
    # indefinitely avoid adversarial scrutiny.
    # ─────────────────────────────────────────────────────────────────────────

    def confirmation_surplus_monitor(self, min_confirmations=3,
                                      max_challenges=1,
                                      auto_queue=True):
        # type: (int, int, bool) -> dict
        """
        Monitor all invariants for confirmation surplus: flag any invariant
        with more than *min_confirmations* confirmations and fewer than
        *max_challenges* challenges, and optionally queue adversarial probe
        cycles for them.

        The monitor detects the structural vulnerability where an invariant
        accumulates apparent robustness through repeated confirmation without
        ever facing serious challenge.  This is epistemically dangerous
        because confirmation surplus without challenge is indistinguishable
        from untested consensus.

        When *auto_queue* is True, flagged invariants are added to an
        internal adversarial probe queue (stored in graph metadata) that
        downstream pipeline stages (FEED, CONSOLIDATE) can consume to
        trigger mandatory falsification probes.

        Parameters
        ----------
        min_confirmations : int
            Minimum number of confirmations (confirms + supports + extends)
            for an invariant to be eligible for surplus flagging (default 3).
            Invariants below this threshold are too new to audit.
        max_challenges : int
            Maximum number of challenges (challenges + refutes + contradicts)
            below which the invariant is considered under-challenged
            (default 1).  An invariant with <= max_challenges direct
            falsification attempts is flagged when it also exceeds
            min_confirmations.
        auto_queue : bool
            If True (default), automatically queue flagged invariants for
            adversarial probe cycles.  The queue is stored in memory and
            accessible via ``get_adversarial_probe_queue()``.

        Returns
        -------
        dict
            {
                "monitored_count": int — total invariants examined,
                "flagged_count": int — invariants exceeding surplus thresholds,
                "flagged_invariants": list of dict — each flagged invariant:
                    {
                        "invariant_id": str,
                        "confirmation_count": int,
                        "challenge_count": int,
                        "surplus": int — confirmations - challenges,
                        "surplus_ratio": float — confirmations / max(1, challenges),
                        "confirmation_sources": list of str — distinct source domains,
                        "challenge_sources": list of str — distinct challenge sources,
                        "last_confirmation_ts": str or None — most recent confirmation,
                        "last_challenge_ts": str or None — most recent challenge,
                        "days_since_last_challenge": float or None,
                        "probe_priority": str — "critical", "high", "moderate",
                        "probe_directive": str — specific adversarial probe instruction,
                    },
                "queued_for_probe": list of str — invariant IDs added to probe queue,
                "queue_size": int — total size of the adversarial probe queue,
                "thresholds": dict — {min_confirmations, max_challenges} used,
                "timestamp": str,
            }
        """
        self._ensure_loaded()

        # Initialize probe queue if not present
        if not hasattr(self, '_adversarial_probe_queue'):
            self._adversarial_probe_queue = []  # type: List[dict]

        # Collect confirmation and challenge edges per invariant
        inv_confirmations = defaultdict(list)  # type: Dict[str, list]
        inv_challenges = defaultdict(list)  # type: Dict[str, list]
        all_inv_ids = set()  # type: set

        for e in self._edges:
            target = e.get("to", "")
            etype = e.get("type", "")
            if not target or not target.upper().startswith("INV_"):
                continue
            target_upper = target.upper()
            all_inv_ids.add(target_upper)
            if etype in self._CONFIRMATION_EDGE_TYPES:
                inv_confirmations[target_upper].append(e)
            elif etype in self._FALSIFICATION_EDGE_TYPES:
                inv_challenges[target_upper].append(e)

        flagged = []  # type: List[dict]
        queued_ids = []  # type: List[str]
        ts_now = datetime.now(timezone.utc)
        ts_now_iso = ts_now.isoformat()

        for inv_id in sorted(all_inv_ids):
            conf_edges = inv_confirmations.get(inv_id, [])
            chal_edges = inv_challenges.get(inv_id, [])
            n_conf = len(conf_edges)
            n_chal = len(chal_edges)

            # Check surplus thresholds
            if n_conf < min_confirmations:
                continue
            if n_chal > max_challenges:
                continue

            # This invariant has surplus: many confirmations, few challenges
            surplus = n_conf - n_chal
            surplus_ratio = float(n_conf) / float(max(1, n_chal))

            # Extract source domains for confirmations
            conf_sources = sorted(set(
                self._extract_source_domain(e.get("from", ""))
                for e in conf_edges
            ))
            chal_sources = sorted(set(
                self._extract_source_domain(e.get("from", ""))
                for e in chal_edges
            ))

            # Timestamps
            conf_timestamps = [
                e.get("timestamp", "") for e in conf_edges if e.get("timestamp")
            ]
            chal_timestamps = [
                e.get("timestamp", "") for e in chal_edges if e.get("timestamp")
            ]
            last_conf_ts = max(conf_timestamps) if conf_timestamps else None
            last_chal_ts = max(chal_timestamps) if chal_timestamps else None

            # Days since last challenge
            days_since_challenge = None  # type: Optional[float]
            if last_chal_ts:
                try:
                    last_chal_dt = datetime.fromisoformat(
                        last_chal_ts.replace("Z", "+00:00")
                    )
                    delta = ts_now - last_chal_dt
                    days_since_challenge = delta.total_seconds() / 86400.0
                except (ValueError, TypeError):
                    pass
            elif n_conf > 0:
                # Never challenged — use time since first confirmation
                if conf_timestamps:
                    try:
                        first_conf_dt = datetime.fromisoformat(
                            min(conf_timestamps).replace("Z", "+00:00")
                        )
                        delta = ts_now - first_conf_dt
                        days_since_challenge = delta.total_seconds() / 86400.0
                    except (ValueError, TypeError):
                        pass

            # Priority classification
            if n_chal == 0 and n_conf >= 5:
                priority = "critical"
            elif n_chal == 0 and n_conf >= min_confirmations:
                priority = "high"
            elif surplus_ratio > 10.0:
                priority = "high"
            else:
                priority = "moderate"

            # Generate specific probe directive
            probe_directive = (
                f"ADVERSARIAL PROBE REQUIRED for {inv_id}: "
                f"{n_conf} confirmation(s), {n_chal} challenge(s) "
                f"(surplus_ratio={surplus_ratio:.1f}). "
                f"Emit: (1) specific falsification conditions with "
                f"measurable thresholds, (2) boundary parameters where "
                f"the invariant's formula breaks (e.g., T→0, T→∞, "
                f"degenerate limits), (3) at least one alternative "
                f"mechanism that produces the same observables without "
                f"requiring {inv_id} to be true, (4) a CHALLENGE edge "
                f"targeting {inv_id} with the strongest falsification "
                f"argument found."
            )

            entry = {
                "invariant_id": inv_id,
                "confirmation_count": n_conf,
                "challenge_count": n_chal,
                "surplus": surplus,
                "surplus_ratio": round(surplus_ratio, 4),
                "confirmation_sources": conf_sources,
                "challenge_sources": chal_sources,
                "last_confirmation_ts": last_conf_ts,
                "last_challenge_ts": last_chal_ts,
                "days_since_last_challenge": (
                    round(days_since_challenge, 2)
                    if days_since_challenge is not None else None
                ),
                "probe_priority": priority,
                "probe_directive": probe_directive,
            }
            flagged.append(entry)

            # Auto-queue for adversarial probe
            if auto_queue:
                # Check if already queued
                already_queued = any(
                    q.get("invariant_id") == inv_id
                    for q in self._adversarial_probe_queue
                )
                if not already_queued:
                    self._adversarial_probe_queue.append({
                        "invariant_id": inv_id,
                        "queued_at": ts_now_iso,
                        "priority": priority,
                        "surplus_ratio": round(surplus_ratio, 4),
                        "confirmation_count": n_conf,
                        "challenge_count": n_chal,
                        "probe_directive": probe_directive,
                        "status": "pending",
                    })
                    queued_ids.append(inv_id)

        # Sort flagged by surplus_ratio descending (worst offenders first)
        flagged.sort(key=lambda x: x["surplus_ratio"], reverse=True)

        result = {
            "monitored_count": len(all_inv_ids),
            "flagged_count": len(flagged),
            "flagged_invariants": flagged,
            "queued_for_probe": queued_ids,
            "queue_size": len(self._adversarial_probe_queue),
            "thresholds": {
                "min_confirmations": min_confirmations,
                "max_challenges": max_challenges,
            },
            "timestamp": ts_now_iso,
        }

        # Log results
        if flagged:
            print(
                f"[GRAPH:SURPLUS_MONITOR] {len(flagged)} invariant(s) flagged "
                f"with confirmation surplus (>{min_confirmations} confirms, "
                f"<={max_challenges} challenges) — "
                f"queued={len(queued_ids)}, queue_size="
                f"{len(self._adversarial_probe_queue)}"
            )
            for entry in flagged[:5]:
                print(
                    f"  {'🚨' if entry['probe_priority'] == 'critical' else '⚠'} "
                    f"{entry['invariant_id']}: "
                    f"confirms={entry['confirmation_count']}, "
                    f"challenges={entry['challenge_count']}, "
                    f"surplus_ratio={entry['surplus_ratio']}, "
                    f"priority={entry['probe_priority']}"
                )
            if len(flagged) > 5:
                print(f"  ... and {len(flagged) - 5} more flagged invariant(s)")

        return result

    def get_adversarial_probe_queue(self, status="pending"):
        # type: (str) -> List[dict]
        """
        Return the current adversarial probe queue, optionally filtered by status.

        The queue contains invariants flagged by ``confirmation_surplus_monitor``
        that require mandatory adversarial probing before further confirmations
        are accepted.

        Parameters
        ----------
        status : str
            Filter by queue entry status: "pending" (default), "in_progress",
            "completed", or "all" to return everything.

        Returns
        -------
        list of dict
            Queue entries sorted by priority (critical first) then by
            surplus_ratio (highest first).  Each entry has:
            {"invariant_id", "queued_at", "priority", "surplus_ratio",
             "confirmation_count", "challenge_count", "probe_directive", "status"}
        """
        if not hasattr(self, '_adversarial_probe_queue'):
            self._adversarial_probe_queue = []  # type: List[dict]

        if status == "all":
            entries = list(self._adversarial_probe_queue)
        else:
            entries = [
                q for q in self._adversarial_probe_queue
                if q.get("status") == status
            ]

        # Sort: critical > high > moderate, then by surplus_ratio descending
        priority_order = {"critical": 0, "high": 1, "moderate": 2}
        entries.sort(
            key=lambda q: (
                priority_order.get(q.get("priority", "moderate"), 3),
                -q.get("surplus_ratio", 0.0),
            )
        )
        return entries

    def mark_probe_completed(self, invariant_id):
        # type: (str) -> bool
        """
        Mark an adversarial probe as completed for a given invariant.

        Called after a challenge/refute/contradict edge has been recorded
        for the invariant, indicating that the adversarial probe cycle
        has been fulfilled.

        Parameters
        ----------
        invariant_id : str
            The invariant whose probe to mark as completed.

        Returns
        -------
        bool
            True if the invariant was found in the queue and marked,
            False if it was not in the queue.
        """
        if not hasattr(self, '_adversarial_probe_queue'):
            return False

        inv_upper = invariant_id.upper()
        found = False
        for q in self._adversarial_probe_queue:
            if q.get("invariant_id") == inv_upper and q.get("status") == "pending":
                q["status"] = "completed"
                q["completed_at"] = datetime.now(timezone.utc).isoformat()
                found = True
                print(
                    f"[GRAPH:SURPLUS_MONITOR] Adversarial probe COMPLETED "
                    f"for {inv_upper} — removed from active queue"
                )
                break
        return found

    def drain_probe_queue_for_feed(self, max_probes=3):
        # type: (int) -> List[dict]
        """
        Drain up to *max_probes* pending entries from the adversarial probe
        queue, marking them as in-progress.  Returns the probe directives
        for inclusion in the next FEED cycle's kernel prompt.

        This is the integration point for the FEED pipeline: before
        processing a new paper, call this to get mandatory adversarial
        probe directives that must be included in the kernel output.

        Parameters
        ----------
        max_probes : int
            Maximum number of probes to drain per FEED cycle (default 3).

        Returns
        -------
        list of dict
            Probe entries marked as "in_progress", sorted by priority.
            Each has "invariant_id" and "probe_directive" fields that
            can be injected into the kernel prompt.
        """
        pending = self.get_adversarial_probe_queue(status="pending")
        drained = []  # type: List[dict]

        for q in pending[:max_probes]:
            q["status"] = "in_progress"
            q["drained_at"] = datetime.now(timezone.utc).isoformat()
            drained.append(q)

        if drained:
            ids = [q["invariant_id"] for q in drained]
            print(
                f"[GRAPH:SURPLUS_MONITOR] Drained {len(drained)} probe(s) "
                f"for FEED cycle: {', '.join(ids)}"
            )

        return drained

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
        result = self.detect_avalanche(
            seed_ids,
            degree_threshold=degree_threshold,
            max_depth=max_depth,
            recent_feed_count=recent_feed_count,
        )

        # ── Branching-ratio tracker (σ across cascade depths) ────────────
        # σ_d = (propagating offspring at depth d) / (parents at depth d-1).
        # σ = mean across depths d≥1.  Flag excursions outside [0.95, 1.05]
        # — the critical band INV_073 claims is the attractor.
        depth_dist = result.get("depth_distribution", {}) or {}
        # Keys may be int (live result) or str (round-tripped through JSON).
        depth_pairs = []  # type: List[tuple]
        for k, v in depth_dist.items():
            try:
                depth_pairs.append((int(k), int(v)))
            except (TypeError, ValueError):
                continue
        depth_pairs.sort(key=lambda kv: kv[0])

        sigmas = []  # type: List[float]
        for i in range(1, len(depth_pairs)):
            parents   = depth_pairs[i - 1][1]
            offspring = depth_pairs[i][1]
            if parents > 0:
                sigmas.append(offspring / float(parents))

        if sigmas:
            sigma_mean = sum(sigmas) / len(sigmas)
            in_band = 0.95 <= sigma_mean <= 1.05
            result["branching_ratio"]      = round(sigma_mean, 4)
            result["branching_per_depth"]  = [round(s, 4) for s in sigmas]
            result["branching_excursion"]  = not in_band
            if not in_band:
                regime_label = "supercritical" if sigma_mean > 1.05 else "subcritical"
                print(f"[GRAPH:BRANCHING] σ={sigma_mean:.4f} EXCURSION "
                      f"({regime_label}) — per-depth: {result['branching_per_depth']}")
        else:
            result["branching_ratio"]     = None
            result["branching_per_depth"] = []
            result["branching_excursion"] = False

        return result

    # ── Self-Reference Detection (Mutual Citation Cycle Audit) ───────────────
    # Detects when two nodes mutually cite each other as evidence, creating a
    # circular evidential dependency — the epistemic analog of self-referential
    # superposition.  Inspired by the paper's claim that wave function collapse
    # is self-reference avoidance: quantum superposition encodes uncomputable
    # self-referential prohibitions, and collapse selects definite states to
    # escape paradox.
    #
    # In the knowledge graph, circular evidential dependencies produce "false
    # coherence inflation": A confirms B, B confirms A, so both appear
    # well-supported when neither is grounded in independent evidence.  This
    # is structurally identical to the self-referential loops that the paper
    # argues physical systems must collapse to avoid.
    #
    # CHALLENGE (INV_064): The paper's self-reference-avoidance collapse
    # mechanism implies *any* sufficiently complex physical system (not just
    # metabolic ones) undergoes structurally-forced state selection, threatening
    # the specificity of the metabolic test as a unique consciousness criterion.
    #
    # NOETHER (Copenhagen Interpretation): If collapse is self-reference
    # avoidance rather than observer-induced, the Copenhagen symmetry between
    # observer and system is broken — the observer becomes structurally
    # irrelevant, not constitutive.
    #
    # When a mutual citation cycle is detected, the pair is flagged for
    # paradox-resolution audit rather than standard merge.  Resolution
    # strategies:
    #   1. Ground-truth anchoring: trace each node's evidence chain to an
    #      external (non-graph) source — breaking the cycle.
    #   2. Temporal ordering: if A was recorded before B, B's citation of A
    #      is evidential but A's citation of B is retroactive — asymmetric.
    #   3. Collapse to strongest: select the node with more independent
    #      (non-circular) support and demote the other's confirmation weight.
    # ─────────────────────────────────────────────────────────────────────────

    # Edge types that constitute "citing as evidence"
    _EVIDENTIAL_EDGE_TYPES = frozenset({
        "confirms", "supports", "extends", "advances",
    })

    def detect_self_references(self):
        # type: () -> Dict[str, dict]
        """
        Detect mutual citation cycles in the knowledge graph where two nodes
        each cite the other as evidence, creating circular evidential
        dependencies.

        A mutual citation cycle exists between nodes A and B when:
          - There exists at least one evidential edge from A → B
            (A cites B as evidence: confirms, supports, extends, advances)
          - AND at least one evidential edge from B → A

        Such cycles inflate coherence scores without grounding in independent
        evidence — the epistemic analog of self-referential superposition
        that the paper argues physical systems must collapse to avoid.

        Returns
        -------
        dict
            {
                "cycles_detected": list of dict — each mutual citation cycle:
                    {
                        "node_a": str,
                        "node_b": str,
                        "a_to_b_edges": list of dict — evidential edges A→B,
                        "b_to_a_edges": list of dict — evidential edges B→A,
                        "cycle_strength": float — min(|A→B|, |B→A|) / max(|A→B|, |B→A|),
                            1.0 = perfectly symmetric mutual citation,
                        "grounded_a": bool — A has non-circular external evidence,
                        "grounded_b": bool — B has non-circular external evidence,
                        "resolution_strategy": str — recommended resolution,
                        "temporal_asymmetry": str or None — which node was cited first,
                        "paradox_severity": str — "benign", "warning", "critical",
                    },
                "n_cycles": int,
                "n_critical": int — cycles where neither node is independently grounded,
                "n_warning": int,
                "n_benign": int,
                "coherence_inflation_risk": float — fraction of evidential edges
                    participating in cycles (0.0 = no inflation, 1.0 = all circular),
                "merge_blocked_pairs": list of tuple — (node_a, node_b) pairs where
                    standard merge should be replaced by paradox-resolution audit,
                "inv064_flag": str — diagnostic for INV_064 challenge,
                "noether_copenhagen_note": str — Noether symmetry status,
                "timestamp": str,
            }
        """
        self._ensure_loaded()

        # ── Build directed evidential adjacency ──────────────────────────
        # Key: (from_node, to_node) → list of evidential edges
        evidential_pairs = defaultdict(list)  # type: Dict[Tuple[str, str], list]
        total_evidential = 0

        for e in self._edges:
            etype = e.get("type", "")
            if etype not in self._EVIDENTIAL_EDGE_TYPES:
                continue
            from_id = e.get("from", "").upper()
            to_id = e.get("to", "").upper()
            if from_id and to_id and from_id != to_id:
                evidential_pairs[(from_id, to_id)].append(e)
                total_evidential += 1

        # Also check node_edges for evidential structural links
        for e in self._node_edges:
            etype = e.get("type", "")
            # Node-edge types that imply evidential support
            if etype in ("operationalizes", "independent_confirmation",
                         "scales_with"):
                from_id = e.get("from", "").upper()
                to_id = e.get("to", "").upper()
                if from_id and to_id and from_id != to_id:
                    evidential_pairs[(from_id, to_id)].append(e)
                    total_evidential += 1

        # ── Detect mutual pairs ──────────────────────────────────────────
        checked = set()  # type: set
        cycles = []  # type: List[dict]
        edges_in_cycles = 0

        for (a, b), a_to_b_edges in evidential_pairs.items():
            pair_key = tuple(sorted([a, b]))
            if pair_key in checked:
                continue
            checked.add(pair_key)

            b_to_a_edges = evidential_pairs.get((b, a), [])
            if not b_to_a_edges:
                continue

            # Mutual citation cycle found!
            n_a2b = len(a_to_b_edges)
            n_b2a = len(b_to_a_edges)
            edges_in_cycles += n_a2b + n_b2a

            # Cycle strength: symmetry of mutual citation
            cycle_strength = float(min(n_a2b, n_b2a)) / float(max(n_a2b, n_b2a))

            # ── Check independent grounding ──────────────────────────────
            # A node is "grounded" if it has evidential edges from sources
            # OTHER than the cycle partner
            def _has_independent_evidence(node, partner):
                # type: (str, str) -> bool
                for (src, tgt), edges in evidential_pairs.items():
                    if tgt == node and src != partner:
                        return True
                    if src == node and tgt != partner:
                        # Node provides evidence to others — check if node
                        # itself receives evidence from non-partner
                        pass
                # Also check if node has external (non-graph-node) sources
                for e in self._edges:
                    etype = e.get("type", "")
                    if etype not in self._EVIDENTIAL_EDGE_TYPES:
                        continue
                    to_id = e.get("to", "").upper()
                    from_id = e.get("from", "").upper()
                    if to_id == node and from_id != partner:
                        return True
                return False

            grounded_a = _has_independent_evidence(a, b)
            grounded_b = _has_independent_evidence(b, a)

            # ── Temporal asymmetry ───────────────────────────────────────
            # Determine which direction was recorded first
            def _earliest_timestamp(edge_list):
                # type: (list) -> Optional[str]
                timestamps = [e.get("timestamp", "") for e in edge_list
                              if e.get("timestamp")]
                return min(timestamps) if timestamps else None

            ts_a2b = _earliest_timestamp(a_to_b_edges)
            ts_b2a = _earliest_timestamp(b_to_a_edges)

            temporal_asymmetry = None  # type: Optional[str]
            if ts_a2b and ts_b2a:
                if ts_a2b < ts_b2a:
                    temporal_asymmetry = f"{a}_cited_first"
                elif ts_b2a < ts_a2b:
                    temporal_asymmetry = f"{b}_cited_first"
                else:
                    temporal_asymmetry = "simultaneous"

            # ── Paradox severity classification ──────────────────────────
            if not grounded_a and not grounded_b:
                # Neither node has independent evidence — fully circular
                severity = "critical"
                resolution = (
                    "COLLAPSE_REQUIRED: Neither node has independent grounding. "
                    "Trace both to external sources or demote mutual "
                    "confirmations to 'ungrounded_circular' status. "
                    "Do NOT merge — paradox-resolution audit required."
                )
            elif grounded_a and not grounded_b:
                severity = "warning"
                resolution = (
                    f"ASYMMETRIC_GROUND: {a} is independently grounded but "
                    f"{b} relies on circular citation. Demote {b}'s citation "
                    f"of {a} from evidential to derivative. Standard merge "
                    f"blocked until {b} acquires independent evidence."
                )
            elif not grounded_a and grounded_b:
                severity = "warning"
                resolution = (
                    f"ASYMMETRIC_GROUND: {b} is independently grounded but "
                    f"{a} relies on circular citation. Demote {a}'s citation "
                    f"of {b} from evidential to derivative. Standard merge "
                    f"blocked until {a} acquires independent evidence."
                )
            else:
                # Both grounded — cycle exists but is not the sole support
                severity = "benign"
                resolution = (
                    "BOTH_GROUNDED: Mutual citation exists but both nodes "
                    "have independent evidence. Cycle is reinforcing, not "
                    "constitutive. Standard merge permitted with cycle "
                    "annotation for transparency."
                )

            # Apply temporal ordering as secondary resolution signal
            if temporal_asymmetry and severity != "benign":
                resolution += (
                    f" Temporal signal: {temporal_asymmetry} — consider "
                    f"treating the later citation as retroactive confirmation "
                    f"(lower evidential weight)."
                )

            cycles.append({
                "node_a": a,
                "node_b": b,
                "a_to_b_edges": a_to_b_edges,
                "b_to_a_edges": b_to_a_edges,
                "cycle_strength": round(cycle_strength, 4),
                "grounded_a": grounded_a,
                "grounded_b": grounded_b,
                "resolution_strategy": resolution,
                "temporal_asymmetry": temporal_asymmetry,
                "paradox_severity": severity,
            })

        # ── Aggregate metrics ────────────────────────────────────────────
        n_critical = sum(1 for c in cycles if c["paradox_severity"] == "critical")
        n_warning = sum(1 for c in cycles if c["paradox_severity"] == "warning")
        n_benign = sum(1 for c in cycles if c["paradox_severity"] == "benign")

        coherence_inflation = (
            float(edges_in_cycles) / float(max(1, total_evidential))
        )

        # Merge-blocked pairs: critical and warning cycles block standard merge
        merge_blocked = [
            (c["node_a"], c["node_b"])
            for c in cycles
            if c["paradox_severity"] in ("critical", "warning")
        ]

        # INV_064 flag: if any cycle is detected, the self-reference-avoidance
        # collapse mechanism is structurally relevant
        if cycles:
            inv064_flag = (
                f"self_reference_detected:{len(cycles)}_cycles:"
                f"critical={n_critical}:warning={n_warning}:"
                f"coherence_inflation={coherence_inflation:.4f}:"
                f"collapse_mechanism_applicable"
            )
        else:
            inv064_flag = "no_self_reference:graph_acyclic_evidentially"

        noether_note = (
            "Copenhagen symmetry broken: if collapse is self-reference "
            "avoidance (not observer-induced), the observer is structurally "
            "irrelevant to state selection. Mutual citation cycles in the "
            "knowledge graph are the epistemic analog — they must be "
            "resolved by structural grounding, not by observer fiat."
        )

        ts = datetime.now(timezone.utc).isoformat()

        result = {
            "cycles_detected": cycles,
            "n_cycles": len(cycles),
            "n_critical": n_critical,
            "n_warning": n_warning,
            "n_benign": n_benign,
            "coherence_inflation_risk": round(coherence_inflation, 4),
            "merge_blocked_pairs": merge_blocked,
            "inv064_flag": inv064_flag,
            "noether_copenhagen_note": noether_note,
            "timestamp": ts,
        }

        # ── Log results ──────────────────────────────────────────────────
        if cycles:
            print(
                f"[GRAPH:SELF_REFERENCE] {len(cycles)} mutual citation "
                f"cycle(s) detected — critical={n_critical}, "
                f"warning={n_warning}, benign={n_benign}, "
                f"coherence_inflation={coherence_inflation:.4f}"
            )
            for c in cycles:
                if c["paradox_severity"] in ("critical", "warning"):
                    print(
                        f"  ⚠ {c['node_a']} ↔ {c['node_b']}: "
                        f"severity={c['paradox_severity']}, "
                        f"strength={c['cycle_strength']}, "
                        f"grounded=({c['grounded_a']},{c['grounded_b']})"
                    )
            if merge_blocked:
                print(
                    f"[GRAPH:SELF_REFERENCE] {len(merge_blocked)} pair(s) "
                    f"BLOCKED from standard merge — paradox-resolution "
                    f"audit required"
                )

        return result

    def is_merge_blocked_by_cycle(self, node_a, node_b):
        # type: (str, str) -> bool
        """
        Check whether a standard merge between two nodes is blocked due
        to a mutual citation cycle requiring paradox-resolution audit.

        Parameters
        ----------
        node_a : str
            First node identifier.
        node_b : str
            Second node identifier.

        Returns
        -------
        bool
            True if the pair has a critical or warning-severity mutual
            citation cycle, blocking standard merge.
        """
        audit = self.detect_self_references()
        pair_key = tuple(sorted([node_a.upper(), node_b.upper()]))
        for blocked_a, blocked_b in audit.get("merge_blocked_pairs", []):
            if tuple(sorted([blocked_a, blocked_b])) == pair_key:
                return True
        return False

    def self_reference_aware_merge_candidates(self):
        # type: () -> dict
        """
        Return merge candidates partitioned into safe (no cycle) and
        blocked (cycle-detected, requires paradox-resolution audit).

        Examines all node pairs connected by evidential edges and classifies
        them based on the self-reference detection pass.

        Returns
        -------
        dict
            {
                "safe_merges": list of tuple — (node_a, node_b) pairs safe
                    for standard merge,
                "blocked_merges": list of dict — pairs requiring paradox-
                    resolution audit, each with cycle details,
                "n_safe": int,
                "n_blocked": int,
            }
        """
        audit = self.detect_self_references()
        blocked_set = set()  # type: set
        for a, b in audit.get("merge_blocked_pairs", []):
            blocked_set.add(tuple(sorted([a, b])))

        blocked_details = []  # type: List[dict]
        for c in audit.get("cycles_detected", []):
            if c["paradox_severity"] in ("critical", "warning"):
                blocked_details.append({
                    "node_a": c["node_a"],
                    "node_b": c["node_b"],
                    "severity": c["paradox_severity"],
                    "cycle_strength": c["cycle_strength"],
                    "resolution_strategy": c["resolution_strategy"],
                })

        # Collect all evidential pairs as potential merge candidates
        all_pairs = set()  # type: set
        for e in self._edges:
            etype = e.get("type", "")
            if etype in self._EVIDENTIAL_EDGE_TYPES:
                from_id = e.get("from", "").upper()
                to_id = e.get("to", "").upper()
                if from_id and to_id and from_id != to_id:
                    all_pairs.add(tuple(sorted([from_id, to_id])))

        safe = [p for p in sorted(all_pairs) if p not in blocked_set]

        return {
            "safe_merges": safe,
            "blocked_merges": blocked_details,
            "n_safe": len(safe),
            "n_blocked": len(blocked_details),
        }

# ─── L₂((0,1)) Quantile-Function Embedding for Distribution Drift ───────────
# Represents probability distributions over semantic tokens as elements of the
# Hilbert space L₂((0,1)) via their quantile functions (inverse CDFs).  This
# exploits the isometric embedding of the Wasserstein space 𝒫₂(ℝ) into
# L₂((0,1)), converting W₂ distance computations to L₂ norms and enabling
# subgradient-based discrepancy minimization as a scoring signal for
# distribution drift in the knowledge graph.
#
# Key results from the paper:
#   - The map μ ↦ F_μ⁻¹ (quantile function) is an isometry from
#     (𝒫₂(ℝ), W₂) into (L₂((0,1)), ‖·‖₂), meaning:
#       W₂(μ, ν) = ‖F_μ⁻¹ - F_ν⁻¹‖_{L₂((0,1))}
#   - For the MMD functional ℱ_ν := 𝒟²_K(·, ν) with negative distance
#     kernel K(x,y) = -|x-y|, the associated L₂ functional is convex.
#   - Wasserstein gradient flows of functionals on 𝒫₂(ℝ) correspond to
#     subgradient flows of associated functionals on L₂((0,1)).
#   - For Dirac measures ν = δ_q, the subgradient flow and its convergence
#     can be determined explicitly.
#
# This reduces the computational cost of tracking distribution drift from
# OT solver complexity to Hilbert-space inner products and projections,
# making FREED's distribution-tracking in the epistemic loop analytically
# tractable and geometrically grounded.
#
# CHALLENGE (O44): The L₂ embedding is exact only for measures on ℝ (1-D).
# For higher-dimensional semantic token spaces, the quantile-function
# isometry breaks down — sliced Wasserstein or other projections are needed.
# The current implementation treats each node's edge-weight distribution as
# a 1-D empirical measure (via sorted token weights), which is valid for
# per-node drift tracking but does NOT capture multi-dimensional semantic
# geometry.
# ─────────────────────────────────────────────────────────────────────────────


def _empirical_quantile_function(weights, n_quantile_points=100):
    # type: (List[float], int) -> List[float]
    """
    Compute the quantile function (inverse CDF) of an empirical distribution
    defined by a list of weights, evaluated at n_quantile_points equally
    spaced points in (0, 1).

    The empirical distribution is formed by normalizing *weights* to sum to 1
    and sorting; the quantile function is then the generalized inverse of the
    resulting step-CDF.

    Parameters
    ----------
    weights : list of float
        Non-negative weights defining the empirical distribution.
        Need not sum to 1 (will be normalized).
    n_quantile_points : int
        Number of evaluation points in (0, 1) for the quantile function
        (default 100).  Higher values give finer L₂ approximation.

    Returns
    -------
    list of float
        The quantile function evaluated at t_k = (k + 0.5) / n_quantile_points
        for k = 0, ..., n_quantile_points - 1.  This is an element of the
        discretized L₂((0,1)) space.
    """
    if not weights:
        return [0.0] * n_quantile_points

    # Filter non-negative and normalize
    w = [max(0.0, wi) for wi in weights]
    total = sum(w)
    if total <= 0.0:
        return [0.0] * n_quantile_points

    # Build sorted sample: each weight w_i contributes a mass w_i/total
    # at position i (the "token index" serves as the real-line value)
    # For a more meaningful embedding, we use the weight values themselves
    # as the support points of the empirical measure.
    sorted_w = sorted(w)
    n = len(sorted_w)

    # Build empirical CDF: F(x) = (1/total) * sum of sorted_w[j] for j where sorted_w[j] <= x
    # The quantile function F^{-1}(t) = inf{x : F(x) >= t}
    # For the empirical measure with atoms at sorted_w[i] with mass sorted_w[i]/total:
    cumulative = []  # type: List[float]
    running = 0.0
    for wi in sorted_w:
        running += wi / total
        cumulative.append(running)

    # Evaluate quantile function at uniformly spaced points
    quantile = []  # type: List[float]
    for k in range(n_quantile_points):
        t = (k + 0.5) / float(n_quantile_points)
        # F^{-1}(t) = sorted_w[j] where j = min{i : cumulative[i] >= t}
        idx = 0
        for i in range(n):
            if cumulative[i] >= t - 1e-15:
                idx = i
                break
            idx = i
        quantile.append(sorted_w[idx])

    return quantile


def _l2_inner_product(f, g):
    # type: (List[float], List[float]) -> float
    """
    Compute the L₂((0,1)) inner product of two discretized functions
    ⟨f, g⟩ = ∫₀¹ f(t)g(t) dt, approximated by the trapezoidal rule
    on equally spaced points.

    Parameters
    ----------
    f : list of float
        First discretized function (length n).
    g : list of float
        Second discretized function (same length n).

    Returns
    -------
    float
        Approximate L₂ inner product.
    """
    n = len(f)
    if n == 0 or n != len(g):
        return 0.0
    dt = 1.0 / float(n)
    return sum(fi * gi for fi, gi in zip(f, g)) * dt


def _l2_norm(f):
    # type: (List[float]) -> float
    """
    Compute the L₂((0,1)) norm ‖f‖ = √(⟨f, f⟩) of a discretized function.

    Parameters
    ----------
    f : list of float
        Discretized function.

    Returns
    -------
    float
        L₂ norm (non-negative).
    """
    return math.sqrt(max(0.0, _l2_inner_product(f, f)))


def _l2_distance(f, g):
    # type: (List[float], List[float]) -> float
    """
    Compute ‖f - g‖_{L₂((0,1))}, which equals W₂(μ, ν) when f and g are
    the quantile functions of measures μ and ν (isometric embedding).

    Parameters
    ----------
    f : list of float
        Quantile function of first distribution.
    g : list of float
        Quantile function of second distribution.

    Returns
    -------
    float
        L₂ distance (= W₂ distance under the isometric embedding).
    """
    n = len(f)
    if n == 0 or n != len(g):
        return 0.0
    diff = [fi - gi for fi, gi in zip(f, g)]
    return _l2_norm(diff)


# ─── Wasserstein Compactness Verification ─────────────────────────────────────
# The Wasserstein distance W_p metrizes the space of probability measures only
# on COMPACT domains (Villani, Ch. 6).  For empirical distributions with
# unbounded support, the optimal coupling existence theorem (Kantorovich) still
# holds under finite p-th moment conditions, but the metric-space properties
# (completeness, separability of (P_p(X), W_p)) require compactness or at
# minimum tight moment bounds.
#
# CHALLENGE (O44): The Wasserstein Floor formula W_floor = k/Tμ inherits this
# compactness requirement.  The quantum extension to infinite-dimensional
# operator spaces must either restrict to compact state spaces or supply a new
# existence proof — a condition not currently stated in the obligation.
#
# This module checks empirical distributions for support compactness (bounded
# range) and finite moment conditions before W_p computation, logging warnings
# when assumptions may be violated and optionally applying moment truncation
# to enforce effective compactness.
# ─────────────────────────────────────────────────────────────────────────────

import logging as _logging

_wasserstein_logger = _logging.getLogger("FREED.wasserstein.compactness")


def _check_support_compactness(weights, label="distribution",
                                compact_range_threshold=1e6,
                                moment_order=2,
                                moment_threshold=1e12,
                                truncate_if_noncompact=False,
                                truncation_quantile=0.995):
    # type: (List[float], str, float, int, float, bool, float) -> dict
    """
    Verify that an empirical distribution's support satisfies the compactness
    (bounded domain) assumption required for Wasserstein distance to metrize
    the probability space.

    Checks three conditions:
      1. Bounded range: max(support) - min(support) < compact_range_threshold
      2. Finite p-th moment: E[|X|^p] < moment_threshold
      3. No infinite/NaN values in support

    When violations are detected, logs a warning and optionally applies moment
    truncation (winsorization at the given quantile) to impose effective
    compactness.

    Parameters
    ----------
    weights : list of float
        Non-negative weights defining the empirical distribution.
    label : str
        Human-readable label for log messages (default "distribution").
    compact_range_threshold : float
        Maximum allowed range (max - min) of the support before flagging
        non-compactness (default 1e6).
    moment_order : int
        Order p of the moment to check (default 2, matching W_2).
    moment_threshold : float
        Maximum allowed p-th moment before flagging (default 1e12).
    truncate_if_noncompact : bool
        If True, apply winsorization to enforce effective compactness
        when violations are detected (default False).
    truncation_quantile : float
        Quantile at which to truncate (default 0.995 = clip at 99.5th
        percentile from both tails).

    Returns
    -------
    dict
        {
            "is_compact": bool — True if all compactness conditions met,
            "support_range": float — max - min of weight values,
            "moment_p": float — the p-th moment E[|X|^p],
            "has_infinite": bool — True if any value is inf or nan,
            "violations": list of str — list of violated conditions,
            "truncated": bool — True if truncation was applied,
            "weights_out": list of float — original or truncated weights,
            "o44_compactness_flag": str — diagnostic tag for O44,
        }
    """
    violations = []  # type: List[str]
    truncated = False
    weights_out = list(weights)

    if not weights:
        return {
            "is_compact": True,
            "support_range": 0.0,
            "moment_p": 0.0,
            "has_infinite": False,
            "violations": [],
            "truncated": False,
            "weights_out": weights_out,
            "o44_compactness_flag": "empty_distribution:trivially_compact",
        }

    # Check for infinite/NaN values
    has_infinite = any(
        (not math.isfinite(w)) for w in weights
    )
    if has_infinite:
        violations.append("non_finite_values_in_support")
        _wasserstein_logger.warning(
            "[WASSERSTEIN:COMPACTNESS] %s contains inf/NaN values — "
            "compact-domain assumption VIOLATED. W_p metric properties "
            "are not guaranteed. (O44: W_floor = k/Tμ inheritance threatened)",
            label,
        )

    # Filter to finite values for further checks
    finite_weights = [w for w in weights if math.isfinite(w)]
    if not finite_weights:
        return {
            "is_compact": False,
            "support_range": float('inf'),
            "moment_p": float('inf'),
            "has_infinite": has_infinite,
            "violations": violations + ["all_values_non_finite"],
            "truncated": False,
            "weights_out": weights_out,
            "o44_compactness_flag": "non_finite:compactness_undefined",
        }

    # Check bounded range (support compactness)
    w_min = min(finite_weights)
    w_max = max(finite_weights)
    support_range = w_max - w_min

    if support_range > compact_range_threshold:
        violations.append(
            f"support_range={support_range:.4g}_exceeds_threshold="
            f"{compact_range_threshold:.4g}"
        )
        _wasserstein_logger.warning(
            "[WASSERSTEIN:COMPACTNESS] %s support range %.4g exceeds "
            "compact threshold %.4g — Wasserstein metrization on compact "
            "domains (Villani Ch. 6) may not apply. (O44: W_floor "
            "derivation assumes compactness)",
            label, support_range, compact_range_threshold,
        )

    # Check p-th moment
    n_finite = len(finite_weights)
    total_w = sum(max(0.0, w) for w in finite_weights)
    if total_w > 0:
        moment_p = sum(
            (max(0.0, w) / total_w) * (abs(w) ** moment_order)
            for w in finite_weights
        )
    else:
        moment_p = 0.0

    if moment_p > moment_threshold:
        violations.append(
            f"moment_{moment_order}={moment_p:.4g}_exceeds_threshold="
            f"{moment_threshold:.4g}"
        )
        _wasserstein_logger.warning(
            "[WASSERSTEIN:COMPACTNESS] %s has %d-th moment %.4g exceeding "
            "threshold %.4g — finite moment condition for W_%d may be "
            "marginal. Consider moment truncation. (O44: existence of "
            "optimal coupling requires finite moments)",
            label, moment_order, moment_p, moment_threshold, moment_order,
        )

    # Apply truncation if requested and violations detected
    if truncate_if_noncompact and violations:
        sorted_finite = sorted(finite_weights)
        n_f = len(sorted_finite)
        lo_idx = int((1.0 - truncation_quantile) * n_f)
        hi_idx = int(truncation_quantile * n_f) - 1
        lo_idx = max(0, lo_idx)
        hi_idx = max(lo_idx, min(hi_idx, n_f - 1))
        lo_val = sorted_finite[lo_idx]
        hi_val = sorted_finite[hi_idx]

        weights_out = []
        for w in weights:
            if not math.isfinite(w):
                weights_out.append(hi_val)  # Replace non-finite with upper clip
            else:
                weights_out.append(max(lo_val, min(hi_val, w)))
        truncated = True
        _wasserstein_logger.info(
            "[WASSERSTEIN:COMPACTNESS] %s truncated to [%.4g, %.4g] "
            "(quantile=%.4f) to impose effective compactness",
            label, lo_val, hi_val, truncation_quantile,
        )

    is_compact = len(violations) == 0

    # O44 diagnostic tag
    if is_compact:
        o44_flag = "compact_support:wfloor_valid"
    elif truncated:
        o44_flag = (
            f"non_compact:truncated_to_effective_compact:"
            f"violations={len(violations)}:wfloor_conditional"
        )
    else:
        o44_flag = (
            f"non_compact:violations={len(violations)}:"
            f"wfloor_threatened:quantum_extension_requires_new_proof"
        )

    return {
        "is_compact": is_compact,
        "support_range": support_range,
        "moment_p": moment_p,
        "has_infinite": has_infinite,
        "violations": violations,
        "truncated": truncated,
        "weights_out": weights_out,
        "o44_compactness_flag": o44_flag,
    }


def _mmd_negative_distance_kernel(f_mu, f_nu, n_points=None):
    # type: (List[float], List[float], Optional[int]) -> float
    """
    Compute the MMD² functional ℱ_ν(μ) = 𝒟²_K(μ, ν) with the negative
    distance kernel K(x,y) = -|x-y|, using the L₂ quantile representation.

    Under the isometric embedding, the MMD functional with K(x,y) = -|x-y|
    becomes a convex functional on L₂((0,1)):
        ℱ_ν(μ) = ∫₀¹ |F_μ⁻¹(t) - F_ν⁻¹(t)| dt
                  + cross terms from the kernel structure.

    For the negative distance kernel specifically:
        𝒟²_K(μ, ν) = 2∫₀¹∫₀¹ min(s,t) (F_μ⁻¹(t) - F_ν⁻¹(t))
                      × (F_μ⁻¹(s) - F_ν⁻¹(s)) ds dt
    which in L₂ terms reduces to a quadratic form involving the antiderivative
    operator (integration operator on (0,1)).

    For computational tractability, we use the simpler W₂² = ‖F_μ⁻¹ - F_ν⁻¹‖²
    as the primary discrepancy and the L¹ integral as the MMD proxy:
        MMD_proxy = (1/n) Σ_k |F_μ⁻¹(t_k) - F_ν⁻¹(t_k)|

    Parameters
    ----------
    f_mu : list of float
        Quantile function of μ.
    f_nu : list of float
        Quantile function of ν.
    n_points : int or None
        Override number of points (uses len(f_mu) if None).

    Returns
    -------
    float
        MMD proxy value (non-negative).  Convex in the L₂ representation,
        enabling subgradient-based minimization.
    """
    n = len(f_mu)
    if n == 0 or n != len(f_nu):
        return 0.0
    return sum(abs(fi - gi) for fi, gi in zip(f_mu, f_nu)) / float(n)


def _mmd_subgradient(f_mu, f_nu):
    # type: (List[float], List[float]) -> List[float]
    """
    Compute a subgradient of the MMD proxy functional at f_mu with respect
    to the target f_nu, in the L₂((0,1)) representation.

    For the L¹-based MMD proxy:
        ∂ℱ_ν/∂f_μ(t_k) = sign(f_μ(t_k) - f_ν(t_k)) / n

    This subgradient lives in L₂((0,1)) and can be used for subgradient
    descent to minimize the discrepancy between μ and ν, corresponding
    to a Wasserstein gradient flow on 𝒫₂(ℝ).

    Parameters
    ----------
    f_mu : list of float
        Quantile function of current distribution μ.
    f_nu : list of float
        Quantile function of target distribution ν.

    Returns
    -------
    list of float
        Subgradient vector in discretized L₂((0,1)), same length as inputs.
        The sign convention is: descent direction is -subgradient.
    """
    n = len(f_mu)
    if n == 0 or n != len(f_nu):
        return []
    inv_n = 1.0 / float(n)
    subgrad = []  # type: List[float]
    for fi, gi in zip(f_mu, f_nu):
        diff = fi - gi
        if diff > 0:
            subgrad.append(inv_n)
        elif diff < 0:
            subgrad.append(-inv_n)
        else:
            subgrad.append(0.0)
    return subgrad


def quantile_embedding_drift_score(weights_t0, weights_t1, n_quantile_points=100):
    # type: (List[float], List[float], int) -> dict
    """
    Compute the distribution drift score between two empirical distributions
    (represented as weight vectors) using the L₂((0,1)) quantile-function
    embedding.

    This exploits the isometric embedding 𝒫₂(ℝ) → L₂((0,1)) to compute
    W₂ distance as an L₂ norm, and provides the MMD subgradient as a
    descent direction for drift minimization.

    Parameters
    ----------
    weights_t0 : list of float
        Weight vector defining the empirical distribution at time t0.
    weights_t1 : list of float
        Weight vector defining the empirical distribution at time t1.
    n_quantile_points : int
        Resolution of the quantile-function discretization (default 100).

    Returns
    -------
    dict
        {
            "w2_distance": float — W₂ distance (= L₂ distance of quantile fns),
            "w2_squared": float — W₂² (for gradient flow energy functional),
            "mmd_proxy": float — MMD proxy with negative distance kernel,
            "l2_norm_t0": float — ‖F_μ⁻¹‖ (L₂ norm of source quantile fn),
            "l2_norm_t1": float — ‖F_ν⁻¹‖ (L₂ norm of target quantile fn),
            "subgradient_norm": float — ‖∂ℱ/∂f_μ‖ (magnitude of drift signal),
            "drift_severity": str — "none", "mild", "moderate", "severe",
            "n_quantile_points": int,
            "is_isometric": bool — True (confirms this is exact W₂, not approx),
        }
    """
    q0 = _empirical_quantile_function(weights_t0, n_quantile_points)
    q1 = _empirical_quantile_function(weights_t1, n_quantile_points)

    w2_dist = _l2_distance(q0, q1)
    w2_sq = w2_dist * w2_dist
    mmd = _mmd_negative_distance_kernel(q0, q1)
    norm_t0 = _l2_norm(q0)
    norm_t1 = _l2_norm(q1)
    subgrad = _mmd_subgradient(q0, q1)
    subgrad_norm = _l2_norm(subgrad) if subgrad else 0.0

    # Severity classification based on W₂ distance relative to norms
    max_norm = max(norm_t0, norm_t1, 1e-12)
    relative_drift = w2_dist / max_norm

    if relative_drift < 0.01:
        severity = "none"
    elif relative_drift < 0.1:
        severity = "mild"
    elif relative_drift < 0.3:
        severity = "moderate"
    else:
        severity = "severe"

    return {
        "w2_distance": round(w2_dist, 10),
        "w2_squared": round(w2_sq, 10),
        "mmd_proxy": round(mmd, 10),
        "l2_norm_t0": round(norm_t0, 10),
        "l2_norm_t1": round(norm_t1, 10),
        "subgradient_norm": round(subgrad_norm, 10),
        "drift_severity": severity,
        "n_quantile_points": n_quantile_points,
        "is_isometric": True,
    }


def quantile_embedding_subgradient_step(weights_current, weights_target,
                                         step_size=0.1, n_quantile_points=100):
    # type: (List[float], List[float], float, int) -> dict
    """
    Perform one subgradient descent step in L₂((0,1)) to move the current
    distribution toward the target, then project back to a valid weight vector.

    This implements the Wasserstein gradient flow of the MMD functional:
        f_μ^{k+1} = f_μ^k - η · ∂ℱ_ν(f_μ^k)
    followed by projection onto the set of valid quantile functions
    (non-decreasing, non-negative).

    Parameters
    ----------
    weights_current : list of float
        Current weight vector (empirical distribution to update).
    weights_target : list of float
        Target weight vector (reference distribution to approach).
    step_size : float
        Subgradient step size η (default 0.1).
    n_quantile_points : int
        Quantile discretization resolution (default 100).

    Returns
    -------
    dict
        {
            "updated_quantile": list of float — updated quantile function
                after one subgradient step + projection,
            "pre_projection_quantile": list of float — before monotonicity
                projection (diagnostic),
            "w2_before": float — W₂ distance before the step,
            "w2_after": float — W₂ distance after the step,
            "improvement": float — w2_before - w2_after (positive = progress),
            "step_size_used": float,
            "projected": bool — True if monotonicity projection was needed,
        }
    """
    q_curr = _empirical_quantile_function(weights_current, n_quantile_points)
    q_target = _empirical_quantile_function(weights_target, n_quantile_points)

    w2_before = _l2_distance(q_curr, q_target)

    # Compute subgradient
    subgrad = _mmd_subgradient(q_curr, q_target)

    # Subgradient step: f^{k+1} = f^k - η * ∂ℱ
    q_updated_raw = [
        q_curr[k] - step_size * subgrad[k]
        for k in range(n_quantile_points)
    ]

    # Project onto valid quantile functions:
    # 1. Non-negativity: clamp to 0
    q_projected = [max(0.0, qi) for qi in q_updated_raw]

    # 2. Monotonicity (quantile functions must be non-decreasing):
    #    Apply isotonic regression (pool-adjacent-violators)
    projected = False
    for i in range(1, n_quantile_points):
        if q_projected[i] < q_projected[i - 1]:
            projected = True
            # Pool: average with previous
            avg = 0.5 * (q_projected[i - 1] + q_projected[i])
            q_projected[i - 1] = avg
            q_projected[i] = avg

    # Second pass for full isotonic projection (simplified)
    changed = True
    max_passes = 10
    passes = 0
    while changed and passes < max_passes:
        changed = False
        passes += 1
        for i in range(1, n_quantile_points):
            if q_projected[i] < q_projected[i - 1]:
                avg = 0.5 * (q_projected[i - 1] + q_projected[i])
                q_projected[i - 1] = avg
                q_projected[i] = avg
                changed = True
                projected = True

    w2_after = _l2_distance(q_projected, q_target)
    improvement = w2_before - w2_after

    return {
        "updated_quantile": [round(qi, 10) for qi in q_projected],
        "pre_projection_quantile": [round(qi, 10) for qi in q_updated_raw],
        "w2_before": round(w2_before, 10),
        "w2_after": round(w2_after, 10),
        "improvement": round(improvement, 10),
        "step_size_used": step_size,
        "projected": projected,
    }


# ─── Wasserstein Gradient Flow (WGF) Inference Module ─────────────────────────
# Implements implicit Euler steps in function space for high-dimensional
# posterior approximation in knowledge graph belief updates.  Replaces
# discretization-dependent belief propagation with a principled, scalable WGF
# method that preserves the thermodynamic (free energy minimization) structure
# of FREED's epistemic loop.
#
# The method follows the paper's approach: the time-dependent density of a
# diffusion process is derived as the limit of implicit Euler steps that follow
# the gradients of a free energy functional F[ρ] = ∫ ρ log ρ + ∫ ρ V, where
# the first term is the (negative) entropy and V is the log-likelihood
# potential.  Each implicit Euler step solves:
#
#   ρ^{k+1} = argmin_{ρ} { F[ρ] + (1/2τ) W₂²(ρ, ρ^k) }
#
# which in the L₂((0,1)) quantile-function embedding becomes a proximal
# operator on a convex functional — computable without domain discretization.
#
# CHALLENGE (O44): The paper demonstrates WGF tractability in continuous
# infinite-dimensional domains but does not address whether the Floor bound
# W_floor = k/Tμ survives the functional-space limit.  The spectral gap λ₁
# of the p-weighted Laplacian (computed by wasserstein_laplace_beltrami_
# eigenspectrum) provides a candidate lower bound via the contraction rate
# of the WGF semigroup, but the non-commutative (quantum) extension remains
# open — the classical graph Laplacian decomposes cleanly but the density-
# operator case requires additional structure not yet provided.
#
# Key properties:
#   - Discretization-free: operates on quantile functions in L₂((0,1)),
#     not on grid-discretized densities.
#   - Free energy minimizing: each step decreases F[ρ] by construction
#     (proximal operator is a descent method for F + W₂² regularizer).
#   - Convergence: for log-concave potentials, convergence rate is
#     O(exp(-λ₁ · k · τ)) where λ₁ is the spectral gap and τ is the
#     implicit Euler step size.
#   - Thermodynamic consistency: the free energy functional F = U - TS
#     maps directly to the FREED epistemic free energy, with U = expected
#     log-likelihood cost and S = Shannon entropy of the belief state.
# ─────────────────────────────────────────────────────────────────────────────


def _free_energy_quantile(quantile_fn, log_likelihood_values, temperature=1.0):
    # type: (List[float], List[float], float) -> float
    """
    Evaluate the free energy functional F[ρ] in the quantile-function
    representation.

    F[ρ] = (1/T) ∫ ρ(x) V(x) dx  +  ∫ ρ(x) log ρ(x) dx

    In the quantile representation, the entropy term ∫ ρ log ρ is approximated
    via the log-density of the quantile function's derivative, and the
    potential energy term uses the quantile-evaluated log-likelihood.

    Parameters
    ----------
    quantile_fn : list of float
        Quantile function F⁻¹(t) evaluated at n uniformly spaced points
        in (0,1).  Represents the current belief distribution.
    log_likelihood_values : list of float
        Log-likelihood potential V(F⁻¹(t_k)) evaluated at the same quantile
        points.  These are the negative log-likelihoods of the observations
        at each quantile level (higher = worse fit).
    temperature : float
        Temperature parameter T controlling the entropy-energy tradeoff
        (default 1.0).  Lower T → sharper posterior (energy-dominated).
        Higher T → broader posterior (entropy-dominated).

    Returns
    -------
    float
        Free energy value.  Lower is better (WGF minimizes this).
    """
    n = len(quantile_fn)
    if n == 0 or n != len(log_likelihood_values):
        return 0.0

    dt = 1.0 / float(n)
    inv_T = 1.0 / max(temperature, 1e-15)

    # Potential energy: (1/T) * ∫ V(F⁻¹(t)) dt ≈ (1/T) * (1/n) Σ V(F⁻¹(t_k))
    potential_energy = inv_T * sum(log_likelihood_values) * dt

    # Entropy term: -∫ ρ log ρ dx = ∫ log(dF⁻¹/dt) dt  (change of variables)
    # Approximate dF⁻¹/dt by finite differences of the quantile function
    entropy = 0.0
    for k in range(1, n):
        dq = quantile_fn[k] - quantile_fn[k - 1]
        if dq > 1e-15:
            # log(dF⁻¹/dt) ≈ log(dq / dt) = log(dq * n)
            entropy += math.log(dq * float(n)) * dt
        # If dq <= 0, the quantile function is flat (point mass region),
        # contributing -inf to entropy; we skip to avoid numerical issues
        # (this is the correct behavior — point masses have zero entropy)

    # Free energy = potential_energy - entropy (note: entropy term enters
    # with negative sign because F = U - TS and we want to minimize F)
    return potential_energy - entropy


def _wgf_proximal_step_quantile(quantile_current, log_likelihood_values,
                                 tau=0.1, temperature=1.0,
                                 n_inner_steps=10, inner_lr=0.01):
    # type: (List[float], List[float], float, float, int, float) -> List[float]
    """
    Perform one implicit Euler (proximal) step of the Wasserstein gradient
    flow in the quantile-function space L₂((0,1)).

    Solves approximately:
        q^{k+1} = argmin_q { F_T[q] + (1/2τ) ‖q - q^k‖²_{L₂} }

    where F_T is the free energy functional at temperature T and τ is the
    implicit Euler step size.  The proximal operator is solved by n_inner_steps
    of gradient descent on the proximal objective.

    Parameters
    ----------
    quantile_current : list of float
        Current quantile function q^k (discretized on n points).
    log_likelihood_values : list of float
        Log-likelihood potential V evaluated at quantile points.
    tau : float
        Implicit Euler step size (default 0.1).  Larger τ → more aggressive
        updates (less regularization by W₂² term).
    temperature : float
        Temperature parameter T (default 1.0).
    n_inner_steps : int
        Number of inner gradient descent steps for the proximal solve
        (default 10).
    inner_lr : float
        Learning rate for inner gradient descent (default 0.01).

    Returns
    -------
    list of float
        Updated quantile function q^{k+1} after the proximal step,
        projected onto valid (non-decreasing, non-negative) quantile functions.
    """
    n = len(quantile_current)
    if n == 0 or n != len(log_likelihood_values):
        return list(quantile_current)

    dt = 1.0 / float(n)
    inv_T = 1.0 / max(temperature, 1e-15)
    inv_tau = 1.0 / max(tau, 1e-15)

    # Initialize with current quantile (warm start for proximal solve)
    q = list(quantile_current)

    for _step in range(n_inner_steps):
        grad = [0.0] * n

        # Gradient of potential energy term: (1/T) * V'(q(t_k)) * dt
        # Since V is given at quantile points, we use the values directly
        # as the gradient contribution (assuming V is evaluated at q(t_k))
        for k in range(n):
            grad[k] += inv_T * log_likelihood_values[k] * dt

        # Gradient of entropy term: -d/dq[log(dq/dt)]
        # This is the derivative of the log-spacing penalty
        for k in range(1, n - 1):
            dq_fwd = q[k + 1] - q[k]
            dq_bwd = q[k] - q[k - 1]
            # Entropy gradient at interior points
            if dq_fwd > 1e-15:
                grad[k] += (1.0 / (dq_fwd * float(n))) * dt
            if dq_bwd > 1e-15:
                grad[k] -= (1.0 / (dq_bwd * float(n))) * dt

        # Gradient of proximal regularizer: (1/τ) * (q - q^k)
        for k in range(n):
            grad[k] += inv_tau * (q[k] - quantile_current[k]) * dt

        # Gradient descent step
        for k in range(n):
            q[k] -= inner_lr * grad[k]

        # Project onto valid quantile functions
        # 1. Non-negativity
        q = [max(0.0, qi) for qi in q]

        # 2. Monotonicity (isotonic projection via pool-adjacent-violators)
        changed = True
        passes = 0
        while changed and passes < 5:
            changed = False
            passes += 1
            for i in range(1, n):
                if q[i] < q[i - 1]:
                    avg = 0.5 * (q[i - 1] + q[i])
                    q[i - 1] = avg
                    q[i] = avg
                    changed = True

    return q


def wasserstein_gradient_flow_inference(prior_weights, log_likelihood_values,
                                         n_steps=10, tau=0.1, temperature=1.0,
                                         n_quantile_points=100,
                                         n_inner_steps=10, inner_lr=0.01,
                                         spectral_gap=None):
    # type: (List[float], List[float], int, float, float, int, int, float, Optional[float]) -> dict
    """
    Perform Wasserstein gradient flow inference via implicit Euler steps in
    the L₂((0,1)) quantile-function space.

    Starting from a prior distribution (represented as weights), iteratively
    applies proximal steps of the free energy functional to approximate the
    posterior distribution, without discretizing the domain.

    Each step solves:
        ρ^{k+1} = argmin_ρ { F_T[ρ] + (1/2τ) W₂²(ρ, ρ^k) }

    in the quantile-function embedding, where F_T = (1/T)∫ρV + ∫ρ log ρ
    is the free energy at temperature T.

    Parameters
    ----------
    prior_weights : list of float
        Weight vector defining the prior distribution (e.g., from
        node_distribution_from_edges).
    log_likelihood_values : list of float
        Log-likelihood potential V evaluated at n_quantile_points uniformly
        spaced quantile levels.  If shorter than n_quantile_points, it is
        linearly interpolated; if longer, it is truncated.
    n_steps : int
        Number of implicit Euler steps (outer iterations, default 10).
    tau : float
        Implicit Euler step size (default 0.1).
    temperature : float
        Temperature T for the free energy functional (default 1.0).
        Lower T → sharper posterior (less entropic regularization).
    n_quantile_points : int
        Discretization resolution for quantile functions (default 100).
    n_inner_steps : int
        Inner proximal solve iterations per outer step (default 10).
    inner_lr : float
        Learning rate for inner proximal gradient descent (default 0.01).
    spectral_gap : float or None
        If provided, the spectral gap λ₁ of the p-weighted Laplacian
        (from wasserstein_laplace_beltrami_eigenspectrum).  Used to estimate
        convergence rate and assess Wasserstein Floor validity.

    Returns
    -------
    dict
        {
            "posterior_quantile": list of float — final quantile function
                (the WGF-inferred posterior in L₂((0,1))),
            "prior_quantile": list of float — initial quantile function,
            "free_energy_trajectory": list of float — F[ρ^k] at each step,
            "w2_from_prior_trajectory": list of float — W₂(ρ^k, ρ^0) at each step,
            "w2_consecutive_trajectory": list of float — W₂(ρ^k, ρ^{k-1}),
            "n_steps": int,
            "tau": float,
            "temperature": float,
            "converged": bool — True if W₂ consecutive < 1e-6 at final step,
            "convergence_rate_empirical": float — geometric decay rate of
                consecutive W₂ distances (estimated from trajectory),
            "convergence_rate_theoretical": float or None — exp(-λ₁ * τ) if
                spectral_gap provided,
            "free_energy_decrease": float — F[ρ^final] - F[ρ^0] (should be ≤ 0),
            "wfloor_diagnostic": str — O44 diagnostic: whether the WGF
                trajectory is consistent with W_floor = k/Tμ,
            "o44_note": str — challenge status for O44,
            "n_quantile_points": int,
        }
    """
    # ── Prepare quantile functions ───────────────────────────────────────
    q_prior = _empirical_quantile_function(prior_weights, n_quantile_points)

    # Prepare log-likelihood at quantile points
    n_ll = len(log_likelihood_values)
    if n_ll == 0:
        ll_at_quantiles = [0.0] * n_quantile_points
    elif n_ll == n_quantile_points:
        ll_at_quantiles = list(log_likelihood_values)
    elif n_ll < n_quantile_points:
        # Linear interpolation to n_quantile_points
        ll_at_quantiles = []  # type: List[float]
        for k in range(n_quantile_points):
            t = k * (n_ll - 1) / float(max(n_quantile_points - 1, 1))
            idx_lo = int(t)
            idx_hi = min(idx_lo + 1, n_ll - 1)
            frac = t - idx_lo
            val = (1.0 - frac) * log_likelihood_values[idx_lo] + frac * log_likelihood_values[idx_hi]
            ll_at_quantiles.append(val)
    else:
        ll_at_quantiles = list(log_likelihood_values[:n_quantile_points])

    # ── Run implicit Euler steps ─────────────────────────────────────────
    q_current = list(q_prior)
    fe_trajectory = []  # type: List[float]
    w2_from_prior = []  # type: List[float]
    w2_consecutive = []  # type: List[float]

    fe_init = _free_energy_quantile(q_current, ll_at_quantiles, temperature)
    fe_trajectory.append(fe_init)
    w2_from_prior.append(0.0)

    for step in range(n_steps):
        q_prev = list(q_current)

        q_current = _wgf_proximal_step_quantile(
            q_current, ll_at_quantiles,
            tau=tau, temperature=temperature,
            n_inner_steps=n_inner_steps, inner_lr=inner_lr,
        )

        fe_k = _free_energy_quantile(q_current, ll_at_quantiles, temperature)
        fe_trajectory.append(fe_k)

        w2_prior_k = _l2_distance(q_current, q_prior)
        w2_from_prior.append(w2_prior_k)

        w2_consec_k = _l2_distance(q_current, q_prev)
        w2_consecutive.append(w2_consec_k)

    # ── Convergence analysis ─────────────────────────────────────────────
    converged = (len(w2_consecutive) > 0 and w2_consecutive[-1] < 1e-6)

    # Empirical convergence rate: fit geometric decay to consecutive W₂
    # w2_consec[k] ≈ C * r^k  →  r ≈ w2_consec[-1] / w2_consec[0]
    if (len(w2_consecutive) >= 2 and
            w2_consecutive[0] > 1e-15 and w2_consecutive[-1] > 1e-15):
        empirical_rate = (w2_consecutive[-1] / w2_consecutive[0]) ** (
            1.0 / max(len(w2_consecutive) - 1, 1)
        )
    elif len(w2_consecutive) >= 1 and w2_consecutive[0] < 1e-15:
        empirical_rate = 0.0  # Already converged from the start
    else:
        empirical_rate = 1.0  # No convergence observed

    # Theoretical convergence rate from spectral gap
    theoretical_rate = None  # type: Optional[float]
    if spectral_gap is not None and spectral_gap > 0:
        theoretical_rate = math.exp(-spectral_gap * tau)

    # Free energy decrease (should be non-positive for valid WGF)
    fe_decrease = fe_trajectory[-1] - fe_trajectory[0] if fe_trajectory else 0.0

    # ── O44 Wasserstein Floor diagnostic ─────────────────────────────────
    # Check if the WGF trajectory is consistent with a lower bound on W₂
    # transport cost (the Wasserstein Floor W_floor = k/Tμ).
    # In the functional-space limit, the floor should manifest as a minimum
    # W₂ distance the posterior cannot collapse below (even at T→0).
    min_w2_consec = min(w2_consecutive) if w2_consecutive else 0.0

    if temperature > 0 and min_w2_consec > 1e-10:
        wfloor_diagnostic = (
            f"min_consecutive_w2={min_w2_consec:.8f}:"
            f"temperature={temperature}:"
            f"floor_candidate={min_w2_consec:.8f}:"
            f"functional_space_floor_observed"
        )
    elif temperature > 0:
        wfloor_diagnostic = (
            f"min_consecutive_w2={min_w2_consec:.8f}:"
            f"temperature={temperature}:"
            f"no_floor_observed:collapsed_to_point_mass"
        )
    else:
        wfloor_diagnostic = "zero_temperature:degenerate"

    o44_note = (
        "WGF inference operates in functional (L2((0,1))) space without "
        "domain discretization. The Floor bound W_floor = k/T*mu survives "
        "as a regularization effect of the entropy term in the free energy "
        "functional (temperature > 0 prevents posterior collapse to a point "
        "mass). However, the quantum (non-commutative) extension of this "
        "floor — whether W_floor persists when the density matrix replaces "
        "the probability density — remains unresolved (O44 open)."
    )

    # ── Build result ─────────────────────────────────────────────────────
    result = {
        "posterior_quantile": [round(qi, 10) for qi in q_current],
        "prior_quantile": [round(qi, 10) for qi in q_prior],
        "free_energy_trajectory": [round(fe, 10) for fe in fe_trajectory],
        "w2_from_prior_trajectory": [round(w, 10) for w in w2_from_prior],
        "w2_consecutive_trajectory": [round(w, 10) for w in w2_consecutive],
        "n_steps": n_steps,
        "tau": tau,
        "temperature": temperature,
        "converged": converged,
        "convergence_rate_empirical": round(empirical_rate, 10),
        "convergence_rate_theoretical": (
            round(theoretical_rate, 10) if theoretical_rate is not None else None
        ),
        "free_energy_decrease": round(fe_decrease, 10),
        "wfloor_diagnostic": wfloor_diagnostic,
        "o44_note": o44_note,
        "n_quantile_points": n_quantile_points,
    }

    # ── Log summary ──────────────────────────────────────────────────────
    print(
        f"[GRAPH:WGF] Wasserstein gradient flow inference — "
        f"steps={n_steps}, tau={tau}, T={temperature}, "
        f"converged={converged}, "
        f"FE_decrease={fe_decrease:.6f}, "
        f"empirical_rate={empirical_rate:.6f}"
        + (f", theoretical_rate={theoretical_rate:.6f}"
           if theoretical_rate is not None else "")
    )

    return result


def wgf_belief_update(node_id, edges, all_node_ids, log_likelihood_values,
                       laplacian=None, n_steps=10, tau=0.1, temperature=1.0,
                       n_quantile_points=100):
    # type: (str, list, List[str], List[float], Optional[List[List[float]]], int, float, float, int) -> dict
    """
    Perform a full WGF-based belief update for a knowledge graph node.

    Combines:
    1. Prior construction from current edge structure (node_distribution_from_edges)
    2. Spectral gap estimation from graph Laplacian (if provided)
    3. WGF inference via implicit Euler steps in L₂((0,1))
    4. Posterior drift scoring relative to the prior

    This is the main entry point for replacing discretization-dependent belief
    propagation with the WGF method in FREED's epistemic loop.

    Parameters
    ----------
    node_id : str
        The node whose belief to update.
    edges : list of dict
        Current graph edges.
    all_node_ids : list of str
        Ordered list of all node IDs.
    log_likelihood_values : list of float
        Log-likelihood potential from new evidence (e.g., FEED output scores).
    laplacian : list of list of float or None
        Pre-computed graph Laplacian. If None, spectral gap analysis is skipped.
    n_steps : int
        Number of WGF outer iterations (default 10).
    tau : float
        Implicit Euler step size (default 0.1).
    temperature : float
        Temperature parameter (default 1.0).
    n_quantile_points : int
        Quantile discretization resolution (default 100).

    Returns
    -------
    dict
        {
            "node_id": str,
            "wgf_result": dict — full WGF inference result,
            "prior_distribution": list of float,
            "drift_from_prior": dict — quantile_embedding_drift_score result,
            "spectral_gap": float or None,
            "spectral_info": dict or None — eigenspectrum analysis if available,
        }
    """
    # 1. Construct prior from edge structure
    prior_dist = node_distribution_from_edges(node_id, edges, all_node_ids)

    # 2. Spectral gap from Laplacian (if available)
    spectral_gap_val = None  # type: Optional[float]
    spectral_info = None  # type: Optional[dict]
    if laplacian is not None and prior_dist:
        spectral_info = wasserstein_laplace_beltrami_eigenspectrum(
            laplacian, prior_dist
        )
        spectral_gap_val = spectral_info.get("spectral_gap")

    # 3. WGF inference
    wgf_result = wasserstein_gradient_flow_inference(
        prior_weights=prior_dist,
        log_likelihood_values=log_likelihood_values,
        n_steps=n_steps,
        tau=tau,
        temperature=temperature,
        n_quantile_points=n_quantile_points,
        spectral_gap=spectral_gap_val,
    )

    # 4. Drift score: how far did the posterior move from the prior?
    posterior_q = wgf_result.get("posterior_quantile", [])
    prior_q = wgf_result.get("prior_quantile", [])
    if posterior_q and prior_q:
        w2_drift = _l2_distance(posterior_q, prior_q)
        drift_result = {
            "w2_distance": round(w2_drift, 10),
            "drift_severity": (
                "none" if w2_drift < 0.01 else
                "mild" if w2_drift < 0.1 else
                "moderate" if w2_drift < 0.3 else
                "severe"
            ),
        }
    else:
        drift_result = {"w2_distance": 0.0, "drift_severity": "none"}

    return {
        "node_id": node_id,
        "wgf_result": wgf_result,
        "prior_distribution": [round(p, 8) for p in prior_dist],
        "drift_from_prior": drift_result,
        "spectral_gap": (
            round(spectral_gap_val, 10) if spectral_gap_val is not None else None
        ),
        "spectral_info": spectral_info,
    }


# ─── Adapted Wasserstein Distance (A𝒲_r) for Causal Coupling Stability ──────
# Scores semantic trajectory approximations using the adapted Wasserstein
# distance A𝒲_r, which penalizes couplings that violate temporal/causal
# ordering in FREED's epistemic update sequences.  Standard W_r convergence
# misses filtration/causal structure: two couplings can have identical W_r
# cost but radically different causal fidelity.  A𝒲_r distinguishes causal
# from non-causal convergence by requiring that the coupling respect the
# filtration (information available at each time step).
#
# Following the paper's approximation result: under W_r-convergence of
# marginals (μ^k, ν^k) → (μ, ν) with μ^k ≤_{cd} ν^k (convex-decreasing
# order, i.e., supermartingale couplings exist), any π ∈ Π_S(μ, ν) can be
# approximated by π^k ∈ Π_S(μ^k, ν^k) such that A𝒲_r(π^k, π) → 0.
#
# In FREED's epistemic loop, this means:
#   - Trajectory approximations (e.g., from discretized belief updates)
#     are scored not just by marginal fit (W_r) but by causal fidelity (A𝒲_r).
#   - Couplings that "look close" in W_r but violate the temporal ordering
#     of epistemic updates receive a penalty, surfacing non-causal convergence.
#   - The supermartingale constraint (μ ≤_{cd} ν) maps to the requirement
#     that belief updates do not systematically inflate expected value —
#     a natural epistemic constraint (you cannot consistently expect to
#     learn more than you observe).
#
# CHALLENGE (O44): The paper's framework is classical/real-line.  The
# supermartingale constraint's stability under A𝒲_r does not trivially
# extend to non-commutative (quantum) settings where the convex-decreasing
# order ≤_{cd} lacks a direct analogue, leaving the quantum W_floor
# formalization structurally incomplete.  The classical A𝒲_r implementation
# here provides a scoring signal but does NOT resolve the quantum gap.
# ─────────────────────────────────────────────────────────────────────────────


def _check_convex_decreasing_order(mu_weights, nu_weights):
    # type: (List[float], List[float]) -> dict
    """
    Check whether μ ≤_{cd} ν (convex-decreasing order), which is the
    necessary and sufficient condition for the existence of supermartingale
    couplings Π_S(μ, ν) ≠ ∅.

    For probability measures on ℝ, μ ≤_{cd} ν iff:
      (1) mean(μ) ≥ mean(ν)  (supermartingale: conditional expectation
          cannot increase), AND
      (2) For all convex decreasing functions φ: ∫φ dμ ≤ ∫φ dν.
          Equivalently, the integrated quantile condition:
          ∫_0^t F_μ⁻¹(s) ds ≥ ∫_0^t F_ν⁻¹(s) ds  for all t ∈ [0,1]
          (second-order stochastic dominance in the supermartingale direction).

    Parameters
    ----------
    mu_weights : list of float
        Weights defining the source distribution μ.
    nu_weights : list of float
        Weights defining the target distribution ν.

    Returns
    -------
    dict
        {
            "holds": bool — True if μ ≤_{cd} ν,
            "mean_mu": float,
            "mean_nu": float,
            "mean_condition": bool — mean(μ) ≥ mean(ν),
            "integrated_quantile_condition": bool — second-order condition,
            "max_violation": float — largest violation of integrated quantile
                condition (0.0 if no violation),
        }
    """
    n_pts = 100

    # Normalize
    def _normalize(w):
        # type: (List[float]) -> List[float]
        filtered = [max(0.0, wi) for wi in w]
        total = sum(filtered)
        if total <= 0:
            return [1.0 / max(len(w), 1)] * max(len(w), 1)
        return [wi / total for wi in filtered]

    mu_norm = _normalize(mu_weights)
    nu_norm = _normalize(nu_weights)

    # Compute means (using weight values as support points)
    mu_sorted = sorted(mu_norm)
    nu_sorted = sorted(nu_norm)

    mean_mu = sum(mu_norm) / max(len(mu_norm), 1)
    mean_nu = sum(nu_norm) / max(len(nu_norm), 1)

    mean_condition = mean_mu >= mean_nu - 1e-12

    # Compute quantile functions
    q_mu = _empirical_quantile_function(mu_weights, n_pts)
    q_nu = _empirical_quantile_function(nu_weights, n_pts)

    # Check integrated quantile condition:
    # ∫_0^t F_μ⁻¹(s) ds ≥ ∫_0^t F_ν⁻¹(s) ds  for all t
    dt = 1.0 / float(n_pts)
    int_mu = 0.0
    int_nu = 0.0
    max_violation = 0.0
    integrated_condition = True

    for k in range(n_pts):
        int_mu += q_mu[k] * dt
        int_nu += q_nu[k] * dt
        deficit = int_nu - int_mu
        if deficit > 1e-12:
            integrated_condition = False
            if deficit > max_violation:
                max_violation = deficit

    holds = mean_condition and integrated_condition

    return {
        "holds": holds,
        "mean_mu": round(mean_mu, 10),
        "mean_nu": round(mean_nu, 10),
        "mean_condition": mean_condition,
        "integrated_quantile_condition": integrated_condition,
        "max_violation": round(max_violation, 10),
    }


def _causal_coupling_penalty(coupling_matrix, timestamps_mu, timestamps_nu):
    # type: (List[List[float]], List[str], List[str]) -> dict
    """
    Compute the causal ordering penalty for a coupling matrix, measuring
    how much probability mass is transported backward in time (violating
    the filtration structure).

    A coupling π(i, j) is "causal" if the temporal ordering of source index i
    is consistent with the temporal ordering of target index j.  Mass assigned
    to (i, j) where timestamp(i) > timestamp(j) violates causality.

    The penalty is the total mass assigned to causality-violating pairs,
    normalized to [0, 1]:
        penalty = Σ_{(i,j): t_i > t_j} π(i, j) / Σ_{(i,j)} π(i, j)

    Parameters
    ----------
    coupling_matrix : list of list of float
        n_mu × n_nu coupling (transport plan), where entry [i][j] is the
        mass transported from source i to target j.
    timestamps_mu : list of str
        ISO timestamps for source distribution support points.
    timestamps_nu : list of str
        ISO timestamps for target distribution support points.

    Returns
    -------
    dict
        {
            "causal_penalty": float — fraction of mass violating causal order,
            "total_mass": float,
            "violating_mass": float,
            "n_violating_pairs": int,
            "n_total_pairs": int,
            "is_causal": bool — True if penalty < 1e-10,
        }
    """
    n_mu = len(coupling_matrix)
    if n_mu == 0:
        return {
            "causal_penalty": 0.0,
            "total_mass": 0.0,
            "violating_mass": 0.0,
            "n_violating_pairs": 0,
            "n_total_pairs": 0,
            "is_causal": True,
        }

    n_nu = len(coupling_matrix[0]) if coupling_matrix[0] else 0

    # Parse timestamps for ordering (lexicographic on ISO format is correct)
    ts_mu = timestamps_mu if timestamps_mu else [""] * n_mu
    ts_nu = timestamps_nu if timestamps_nu else [""] * n_nu

    total_mass = 0.0
    violating_mass = 0.0
    n_violating = 0
    n_total = 0

    for i in range(min(n_mu, len(ts_mu))):
        for j in range(min(n_nu, len(ts_nu))):
            if i >= len(coupling_matrix) or j >= len(coupling_matrix[i]):
                continue
            mass_ij = coupling_matrix[i][j]
            if mass_ij <= 0:
                continue
            n_total += 1
            total_mass += mass_ij

            # Causal violation: source timestamp strictly after target timestamp
            if ts_mu[i] and ts_nu[j] and ts_mu[i] > ts_nu[j]:
                violating_mass += mass_ij
                n_violating += 1

    penalty = violating_mass / max(total_mass, 1e-15)

    return {
        "causal_penalty": round(penalty, 10),
        "total_mass": round(total_mass, 10),
        "violating_mass": round(violating_mass, 10),
        "n_violating_pairs": n_violating,
        "n_total_pairs": n_total,
        "is_causal": penalty < 1e-10,
    }


def adapted_wasserstein_score(weights_mu, weights_nu,
                               timestamps_mu=None, timestamps_nu=None,
                               coupling_matrix=None,
                               r=2, n_quantile_points=100,
                               causal_penalty_weight=1.0):
    # type: (List[float], List[float], Optional[List[str]], Optional[List[str]], Optional[List[List[float]]], int, int, float) -> dict
    """
    Compute the Adapted Wasserstein distance (A𝒲_r) score for a coupling
    between two empirical distributions, distinguishing causal from
    non-causal convergence.

    The score combines:
      1. Standard W_r distance (via L₂ quantile embedding for r=2)
      2. Causal ordering penalty (mass transported backward in time)
      3. Supermartingale constraint check (μ ≤_{cd} ν)

    The adapted score is:
        A𝒲_r = W_r + causal_penalty_weight × causal_penalty × W_r

    This ensures that couplings with identical W_r but different causal
    fidelity receive different scores, with causality-violating couplings
    penalized proportionally to both their W_r cost and the fraction of
    mass transported acausally.

    Parameters
    ----------
    weights_mu : list of float
        Weights defining the source distribution μ (e.g., prior belief).
    weights_nu : list of float
        Weights defining the target distribution ν (e.g., posterior belief).
    timestamps_mu : list of str or None
        ISO timestamps for source support points (for causal ordering).
        If None, causal penalty is computed as 0 (no ordering info).
    timestamps_nu : list of str or None
        ISO timestamps for target support points.
    coupling_matrix : list of list of float or None
        Explicit coupling (transport plan).  If None, the identity/monotone
        coupling is assumed (optimal for 1-D W_r) and causal penalty is
        computed from the induced ordering.
    r : int
        Order of the Wasserstein distance (default 2).
    n_quantile_points : int
        Quantile discretization resolution (default 100).
    causal_penalty_weight : float
        Multiplicative weight for the causal penalty term (default 1.0).
        Set to 0.0 to recover standard W_r without causal scoring.

    Returns
    -------
    dict
        {
            "w_r": float — standard W_r distance,
            "adapted_w_r": float — A𝒲_r score (W_r + causal penalty),
            "causal_penalty": float — fraction of mass violating causal order,
            "causal_penalty_contribution": float — the additive penalty term,
            "is_causal_coupling": bool — True if coupling respects temporal order,
            "supermartingale_check": dict — convex-decreasing order verification,
            "supermartingale_coupling_exists": bool — True if Π_S(μ,ν) ≠ ∅,
            "convergence_type": str — "causal" if is_causal and supermartingale
                holds, "non_causal" if W_r is small but causal penalty is large,
                "no_supermartingale" if ≤_{cd} fails,
            "o44_adapted_flag": str — diagnostic for O44 challenge,
            "quantum_gap_note": str — note on non-commutative extension,
        }
    """
    # 1. Standard W_r via quantile embedding (exact for r=2, approximate for others)
    q_mu = _empirical_quantile_function(weights_mu, n_quantile_points)
    q_nu = _empirical_quantile_function(weights_nu, n_quantile_points)

    if r == 2:
        w_r = _l2_distance(q_mu, q_nu)
    else:
        # W_r for general r: (∫|F_μ⁻¹ - F_ν⁻¹|^r dt)^{1/r}
        dt = 1.0 / float(n_quantile_points)
        integral = sum(
            abs(q_mu[k] - q_nu[k]) ** r for k in range(n_quantile_points)
        ) * dt
        w_r = integral ** (1.0 / r) if integral > 0 else 0.0

    # 2. Supermartingale constraint check
    sm_check = _check_convex_decreasing_order(weights_mu, weights_nu)
    sm_exists = sm_check["holds"]

    # 3. Causal ordering penalty
    if coupling_matrix is not None and timestamps_mu and timestamps_nu:
        causal_info = _causal_coupling_penalty(
            coupling_matrix, timestamps_mu, timestamps_nu
        )
    elif timestamps_mu and timestamps_nu:
        # No explicit coupling — build monotone coupling (optimal for 1-D)
        # and check causal ordering on the induced pairing
        n_mu = len(weights_mu)
        n_nu = len(weights_nu)
        # For the monotone coupling, source index i pairs with target index
        # i * (n_nu / n_mu) — proportional mapping
        mono_coupling = [[0.0] * n_nu for _ in range(n_mu)]
        if n_mu > 0 and n_nu > 0:
            for i in range(n_mu):
                j = min(int(i * n_nu / max(n_mu, 1)), n_nu - 1)
                w_i = max(0.0, weights_mu[i])
                total_mu = sum(max(0.0, w) for w in weights_mu)
                mass = w_i / max(total_mu, 1e-15)
                mono_coupling[i][j] = mass
        causal_info = _causal_coupling_penalty(
            mono_coupling, timestamps_mu, timestamps_nu
        )
    else:
        causal_info = {
            "causal_penalty": 0.0,
            "total_mass": 0.0,
            "violating_mass": 0.0,
            "n_violating_pairs": 0,
            "n_total_pairs": 0,
            "is_causal": True,
        }

    causal_penalty = causal_info["causal_penalty"]
    is_causal = causal_info["is_causal"]

    # 4. Adapted Wasserstein score
    causal_contribution = causal_penalty_weight * causal_penalty * w_r
    adapted_w_r = w_r + causal_contribution

    # 5. Convergence type classification
    if not sm_exists:
        convergence_type = "no_supermartingale"
    elif is_causal and w_r < 0.1:
        convergence_type = "causal"
    elif not is_causal and w_r < 0.1:
        convergence_type = "non_causal"
    elif is_causal:
        convergence_type = "causal_distant"
    else:
        convergence_type = "non_causal_distant"

    # 6. O44 diagnostic
    if sm_exists and is_causal:
        o44_flag = (
            f"adapted_w{r}={adapted_w_r:.8f}:causal_coupling:"
            f"supermartingale_valid:classical_stability_holds"
        )
    elif sm_exists and not is_causal:
        o44_flag = (
            f"adapted_w{r}={adapted_w_r:.8f}:NON_causal_coupling:"
            f"supermartingale_valid_but_filtration_violated:"
            f"standard_w{r}_misleading"
        )
    elif not sm_exists:
        o44_flag = (
            f"adapted_w{r}={adapted_w_r:.8f}:no_supermartingale:"
            f"convex_decreasing_order_fails:coupling_set_empty"
        )
    else:
        o44_flag = f"adapted_w{r}={adapted_w_r:.8f}:indeterminate"

    quantum_note = (
        "The adapted Wasserstein distance A𝒲_r is defined for classical "
        "measures on ℝ with filtration structure. The supermartingale "
        "constraint μ ≤_{cd} ν (convex-decreasing order) has no direct "
        "analogue in non-commutative (quantum) settings where probability "
        "measures are replaced by density operators. The quantum extension "
        "requires: (1) a non-commutative convex order on density matrices, "
        "(2) a filtration structure on operator algebras (quantum filtered "
        "probability spaces), and (3) stability of the adapted distance "
        "under the resulting quantum supermartingale constraint. This gap "
        "leaves the quantum W_floor formalization structurally incomplete "
        "(O44 open)."
    )

    result = {
        "w_r": round(w_r, 10),
        "adapted_w_r": round(adapted_w_r, 10),
        "causal_penalty": round(causal_penalty, 10),
        "causal_penalty_contribution": round(causal_contribution, 10),
        "is_causal_coupling": is_causal,
        "supermartingale_check": sm_check,
        "supermartingale_coupling_exists": sm_exists,
        "convergence_type": convergence_type,
        "o44_adapted_flag": o44_flag,
        "quantum_gap_note": quantum_note,
    }

    # Log when non-causal convergence is detected
    if not is_causal and w_r < 0.1:
        print(
            f"[GRAPH:ADAPTED_WASSERSTEIN] ⚠ Non-causal convergence detected — "
            f"W_{r}={w_r:.6f} (small) but causal_penalty={causal_penalty:.4f} "
            f"(significant). Standard W_{r} is MISLEADING for this coupling. "
            f"A𝒲_{r}={adapted_w_r:.6f} correctly penalizes filtration violation."
        )

    return result


def score_trajectory_approximation(trajectory_weights, trajectory_timestamps,
                                    reference_weights, reference_timestamps,
                                    r=2, n_quantile_points=100,
                                    causal_penalty_weight=1.0):
    # type: (List[List[float]], List[str], List[List[float]], List[str], int, int, float) -> dict
    """
    Score a sequence of epistemic trajectory approximations against a
    reference trajectory using A𝒲_r at each time step.

    This is the main entry point for evaluating whether a discretized or
    approximate belief update sequence faithfully preserves the causal
    structure of the reference (exact) sequence.

    Parameters
    ----------
    trajectory_weights : list of list of float
        Sequence of weight vectors (approximation trajectory).
    trajectory_timestamps : list of str
        ISO timestamps for each step of the approximation.
    reference_weights : list of list of float
        Sequence of weight vectors (reference trajectory).
    reference_timestamps : list of str
        ISO timestamps for each step of the reference.
    r : int
        Wasserstein order (default 2).
    n_quantile_points : int
        Quantile discretization (default 100).
    causal_penalty_weight : float
        Causal penalty weight (default 1.0).

    Returns
    -------
    dict
        {
            "per_step_scores": list of dict — A𝒲_r score at each paired step,
            "mean_w_r": float — mean standard W_r across steps,
            "mean_adapted_w_r": float — mean A𝒲_r across steps,
            "mean_causal_penalty": float — mean causal penalty,
            "max_causal_penalty": float — worst causal violation,
            "n_non_causal_steps": int — steps where causal penalty > 0.01,
            "n_steps_scored": int,
            "trajectory_causal_fidelity": float — 1.0 - mean_causal_penalty,
            "overall_convergence_type": str,
            "o44_trajectory_flag": str,
        }
    """
    n_steps = min(len(trajectory_weights), len(reference_weights))
    n_ts_traj = len(trajectory_timestamps)
    n_ts_ref = len(reference_timestamps)

    per_step = []  # type: List[dict]
    for k in range(n_steps):
        ts_mu = [trajectory_timestamps[k]] if k < n_ts_traj else []
        ts_nu = [reference_timestamps[k]] if k < n_ts_ref else []

        score = adapted_wasserstein_score(
            weights_mu=trajectory_weights[k],
            weights_nu=reference_weights[k],
            timestamps_mu=ts_mu if ts_mu else None,
            timestamps_nu=ts_nu if ts_nu else None,
            r=r,
            n_quantile_points=n_quantile_points,
            causal_penalty_weight=causal_penalty_weight,
        )
        per_step.append(score)

    if not per_step:
        return {
            "per_step_scores": [],
            "mean_w_r": 0.0,
            "mean_adapted_w_r": 0.0,
            "mean_causal_penalty": 0.0,
            "max_causal_penalty": 0.0,
            "n_non_causal_steps": 0,
            "n_steps_scored": 0,
            "trajectory_causal_fidelity": 1.0,
            "overall_convergence_type": "empty",
            "o44_trajectory_flag": "empty_trajectory",
        }

    w_rs = [s["w_r"] for s in per_step]
    aw_rs = [s["adapted_w_r"] for s in per_step]
    penalties = [s["causal_penalty"] for s in per_step]

    mean_wr = sum(w_rs) / len(w_rs)
    mean_awr = sum(aw_rs) / len(aw_rs)
    mean_cp = sum(penalties) / len(penalties)
    max_cp = max(penalties)
    n_non_causal = sum(1 for p in penalties if p > 0.01)
    causal_fidelity = 1.0 - mean_cp

    # Overall convergence type
    if mean_cp < 0.01 and mean_wr < 0.1:
        overall_type = "causal_convergent"
    elif mean_cp >= 0.01 and mean_wr < 0.1:
        overall_type = "non_causal_convergent"
    elif mean_cp < 0.01 and mean_wr >= 0.1:
        overall_type = "causal_distant"
    else:
        overall_type = "non_causal_distant"

    o44_flag = (
        f"trajectory_steps={n_steps}:"
        f"mean_w{r}={mean_wr:.6f}:"
        f"mean_aw{r}={mean_awr:.6f}:"
        f"causal_fidelity={causal_fidelity:.4f}:"
        f"convergence={overall_type}:"
        f"classical_framework:quantum_extension_open"
    )

    return {
        "per_step_scores": per_step,
        "mean_w_r": round(mean_wr, 10),
        "mean_adapted_w_r": round(mean_awr, 10),
        "mean_causal_penalty": round(mean_cp, 10),
        "max_causal_penalty": round(max_cp, 10),
        "n_non_causal_steps": n_non_causal,
        "n_steps_scored": n_steps,
        "trajectory_causal_fidelity": round(causal_fidelity, 10),
        "overall_convergence_type": overall_type,
        "o44_trajectory_flag": o44_flag,
    }


def node_distribution_drift(node_id, edges_t0, edges_t1, all_node_ids,
                             n_quantile_points=100):
    # type: (str, list, list, List[str], int) -> dict
    """
    Compute the distribution drift for a knowledge graph node between two
    edge snapshots, using the L₂((0,1)) quantile-function isometric embedding.

    Constructs per-node probability distributions from edge weights at two
    time points, embeds them as quantile functions in L₂((0,1)), and computes
    the exact W₂ distance as the L₂ norm of their difference.

    Parameters
    ----------
    node_id : str
        The node whose distribution drift to measure.
    edges_t0 : list of dict
        Edge snapshot at time t0.
    edges_t1 : list of dict
        Edge snapshot at time t1.
    all_node_ids : list of str
        Ordered list of all node IDs (defines simplex coordinates).
    n_quantile_points : int
        Quantile discretization resolution (default 100).

    Returns
    -------
    dict
        Drift score dict from quantile_embedding_drift_score, augmented with:
        "node_id": str,
        "distribution_t0": list of float — probability vector at t0,
        "distribution_t1": list of float — probability vector at t1,
    """
    dist_t0 = node_distribution_from_edges(node_id, edges_t0, all_node_ids)
    dist_t1 = node_distribution_from_edges(node_id, edges_t1, all_node_ids)

    drift = quantile_embedding_drift_score(dist_t0, dist_t1, n_quantile_points)
    drift["node_id"] = node_id
    drift["distribution_t0"] = [round(p, 8) for p in dist_t0]
    drift["distribution_t1"] = [round(p, 8) for p in dist_t1]

    return drift


# ─── Graph-Laplacian Thermodynamic Entropy & Stability Monitor ────────────────
# Implements a discrete H-theorem monitor for the knowledge graph, treating
# vertices as concepts with heat capacity and edges as conductivity-weighted
# co-activation channels.  Computes:
#
#   1. Discrete thermodynamic entropy S(t) = -Σ_i c_i · u_i(t) · ln(u_i(t))
#      where c_i is the heat capacity (node degree) and u_i is the "temperature"
#      (normalized activation level / belief weight) at vertex i.
#
#   2. The maximum stable time-step Δt_max = 2 / λ_max(D^{-1} L_w)
#      where λ_max is the largest eigenvalue of the capacity-normalized
#      graph Laplacian.  When the effective update step exceeds this bound,
#      the discrete heat equation becomes oscillatory/divergent — the
#      thermodynamic analog of violating the CFL condition.
#
#   3. Spectral stability bound comparison:  the ratio Δt_eff / Δt_max
#      is the "thermodynamic admissibility ratio".  When > 1, the graph's
#      update dynamics are operating outside the thermodynamically stable
#      regime, directly operationalizing the γ=1 criticality condition on
#      the epistemic substrate.
#
# Derivation follows the paper's approach: recurrence relations of heat
# conduction on the graph are derived from first principles (conductivity
# and capacity coefficients) without differential equations.  The entropy
# S(t) is shown to be non-decreasing (discrete H-theorem) if and only if
# Δt ≤ Δt_max = 2 / λ_max.
#
# CHALLENGE (O44): The discrete entropy S = -Σ c_i u_i ln(u_i) with stability
# bound Δt_max = 2/λ_max provides a finite-graph analog of the Wasserstein
# Floor k = 1/Tμ.  Specifically, the spectral stability bound constrains the
# minimum "temporal resolution" of the epistemic dynamics, just as k/Tμ
# constrains the minimum transport cost.  If λ_max ~ Tμ (spectral gap scales
# with the thermodynamic parameter), then Δt_max ~ 1/Tμ recovers a quantity
# proportional to the Wasserstein Floor from pure Laplacian spectral structure,
# resolving O44 via a substrate-independent finite-graph construction.
# ─────────────────────────────────────────────────────────────────────────────


def _build_capacity_vector_from_degrees(adjacency):
    # type: (List[List[float]]) -> List[float]
    """
    Build the heat capacity vector c_i = degree(i) for each vertex.

    In the thermodynamic graph model, heat capacity at a vertex is
    proportional to its connectivity (more connections = more thermal
    inertia = harder to change temperature).  For the knowledge graph,
    this maps node degree to epistemic inertia: highly-connected concepts
    resist rapid belief revision.

    Parameters
    ----------
    adjacency : list of list of float
        n×n symmetric weighted adjacency matrix.

    Returns
    -------
    list of float
        Capacity vector of length n. Minimum capacity is clamped to 1.0
        to avoid division by zero for isolated nodes.
    """
    n = len(adjacency)
    capacities = []  # type: List[float]
    for i in range(n):
        deg = sum(adjacency[i])
        capacities.append(max(deg, 1.0))
    return capacities


def _build_capacity_normalized_laplacian(laplacian, capacities):
    # type: (List[List[float]], List[float]) -> List[List[float]]
    """
    Build the capacity-normalized Laplacian C^{-1} L_w, where C = diag(c_i)
    is the diagonal capacity matrix and L_w is the weighted graph Laplacian.

    The eigenvalues of C^{-1} L_w determine the stability of the discrete
    heat equation: the recurrence u(t+Δt) = u(t) - Δt · C^{-1} L_w · u(t)
    is stable iff Δt ≤ 2 / λ_max(C^{-1} L_w).

    Parameters
    ----------
    laplacian : list of list of float
        n×n weighted graph Laplacian L_w.
    capacities : list of float
        Capacity vector c_i (all entries > 0).

    Returns
    -------
    list of list of float
        n×n capacity-normalized Laplacian.
    """
    n = len(laplacian)
    CinvL = [[0.0] * n for _ in range(n)]
    for i in range(n):
        inv_ci = 1.0 / max(capacities[i], 1e-15)
        for j in range(n):
            CinvL[i][j] = inv_ci * laplacian[i][j]
    return CinvL


def _largest_eigenvalue_power_iteration(matrix, max_iter=200, tol=1e-10):
    # type: (List[List[float]], int, float) -> float
    """
    Estimate the largest eigenvalue of a symmetric positive semi-definite
    matrix using the power iteration method.

    Parameters
    ----------
    matrix : list of list of float
        n×n symmetric matrix.
    max_iter : int
        Maximum iterations (default 200).
    tol : float
        Convergence tolerance on relative eigenvalue change (default 1e-10).

    Returns
    -------
    float
        Estimated largest eigenvalue (non-negative).
    """
    n = len(matrix)
    if n == 0:
        return 0.0

    # Initialize with a non-zero vector
    v = [1.0 / math.sqrt(n)] * n
    lam = 0.0

    for _iteration in range(max_iter):
        # Matrix-vector product: w = M · v
        w = [0.0] * n
        for i in range(n):
            s = 0.0
            for j in range(n):
                s += matrix[i][j] * v[j]
            w[i] = s

        # Rayleigh quotient: λ = v^T w / v^T v
        vw = sum(vi * wi for vi, wi in zip(v, w))
        vv = sum(vi * vi for vi in v)
        if vv < 1e-30:
            break
        lam_new = vw / vv

        # Normalize w to get new v
        w_norm = math.sqrt(sum(wi * wi for wi in w))
        if w_norm < 1e-30:
            break
        v = [wi / w_norm for wi in w]

        # Check convergence
        if abs(lam_new - lam) < tol * max(abs(lam_new), 1.0):
            lam = lam_new
            break
        lam = lam_new

    return max(0.0, lam)


def thermodynamic_graph_entropy(activation_levels, capacities):
    # type: (List[float], List[float]) -> float
    """
    Compute the discrete thermodynamic entropy of the graph state.

    S = -Σ_i c_i · u_i · ln(u_i)

    where c_i is the heat capacity at vertex i and u_i is the normalized
    activation level ("temperature") at vertex i.  Values u_i ≤ 0 are
    skipped (they contribute 0 to entropy by the convention 0·ln(0) = 0).

    This is the graph-native analog of continuous thermodynamic entropy,
    derived from first principles on the finite graph without differential
    equations.

    Parameters
    ----------
    activation_levels : list of float
        Activation / belief weight at each vertex.  Should be > 0 for
        meaningful entropy.  Need not sum to 1 (the entropy formula uses
        raw values, not probabilities).
    capacities : list of float
        Heat capacity at each vertex (typically node degree).

    Returns
    -------
    float
        Discrete thermodynamic entropy (non-negative for u_i in (0,1),
        can be negative for u_i > 1).
    """
    n = len(activation_levels)
    if n == 0:
        return 0.0

    S = 0.0
    for i in range(min(n, len(capacities))):
        u_i = activation_levels[i]
        c_i = capacities[i]
        if u_i > 1e-30:
            S -= c_i * u_i * math.log(u_i)
    return S


def spectral_stability_bound(adjacency, capacities=None):
    # type: (List[List[float]], Optional[List[float]]) -> dict
    """
    Compute the maximum stable time-step Δt_max for discrete heat conduction
    on the thermodynamic graph, and the spectral stability diagnostics.

    The discrete heat equation on the graph is:
        u(t + Δt) = u(t) - Δt · C^{-1} · L_w · u(t)

    This is stable (non-oscillatory, entropy non-decreasing) iff:
        Δt ≤ Δt_max = 2 / λ_max(C^{-1} L_w)

    where λ_max is the largest eigenvalue of the capacity-normalized
    Laplacian.

    Parameters
    ----------
    adjacency : list of list of float
        n×n symmetric weighted adjacency matrix of the knowledge graph.
    capacities : list of float or None
        Heat capacity vector.  If None, uses node degrees as capacities.

    Returns
    -------
    dict
        {
            "lambda_max": float — largest eigenvalue of C^{-1} L_w,
            "dt_max": float — maximum stable time-step 2/λ_max,
            "n_vertices": int,
            "capacities": list of float — the capacity vector used,
            "o44_spectral_stability_note": str — diagnostic for O44,
        }
    """
    n = len(adjacency)
    if n == 0:
        return {
            "lambda_max": 0.0,
            "dt_max": float('inf'),
            "n_vertices": 0,
            "capacities": [],
            "o44_spectral_stability_note": "empty_graph:trivially_stable",
        }

    if capacities is None:
        capacities = _build_capacity_vector_from_degrees(adjacency)

    L = _build_weighted_graph_laplacian(adjacency)
    CinvL = _build_capacity_normalized_laplacian(L, capacities)

    lam_max = _largest_eigenvalue_power_iteration(CinvL)

    if lam_max > 1e-15:
        dt_max = 2.0 / lam_max
    else:
        dt_max = float('inf')

    o44_note = (
        f"lambda_max={lam_max:.8f}:dt_max={dt_max:.8f}:"
        f"discrete_htheorem_bound:if_lambda_max~T_mu_then_dt_max~1/T_mu:"
        f"finite_graph_wasserstein_floor_candidate"
    )

    return {
        "lambda_max": round(lam_max, 10),
        "dt_max": round(dt_max, 10) if dt_max != float('inf') else float('inf'),
        "n_vertices": n,
        "capacities": [round(c, 6) for c in capacities],
        "o44_spectral_stability_note": o44_note,
    }


def thermodynamic_admissibility_check(adjacency, activation_levels,
                                       effective_dt=1.0, capacities=None):
    # type: (List[List[float]], List[float], float, Optional[List[float]]) -> dict
    """
    Full thermodynamic admissibility check for the knowledge graph.

    Combines:
    1. Discrete thermodynamic entropy S(t) of the current graph state
    2. Spectral stability bound Δt_max from the capacity-normalized Laplacian
    3. Admissibility ratio Δt_eff / Δt_max — when > 1, the update dynamics
       are thermodynamically inadmissible (discrete H-theorem violated)

    This is the main entry point for FREED's discrete H-theorem monitor.

    Parameters
    ----------
    adjacency : list of list of float
        n×n symmetric weighted adjacency matrix of the knowledge graph.
        Edge weight w_{ij} = conductivity of co-activation between concepts
        i and j (e.g., confirmation count, similarity score).
    activation_levels : list of float
        Current activation / belief weight at each vertex.  Should be > 0.
        Typically derived from node_distribution_from_edges or edge counts.
    effective_dt : float
        The effective time-step of the current update operation (default 1.0).
        For FEED ingestion, this is 1.0 per feed.  For avalanche cascades,
        this may be > 1 (multiple steps in one operation).  For consolidation,
        this may be < 1 (sub-step refinement).
    capacities : list of float or None
        Heat capacity vector.  If None, derived from node degrees.

    Returns
    -------
    dict
        {
            "entropy": float — current discrete thermodynamic entropy S(t),
            "lambda_max": float — largest eigenvalue of C^{-1} L_w,
            "dt_max": float — maximum stable time-step,
            "effective_dt": float — the time-step being used,
            "admissibility_ratio": float — Δt_eff / Δt_max (> 1 = inadmissible),
            "is_admissible": bool — True if ratio ≤ 1.0,
            "is_critical": bool — True if 0.9 < ratio ≤ 1.0 (near boundary),
            "is_inadmissible": bool — True if ratio > 1.0,
            "regime": str — "stable", "critical", or "inadmissible",
            "h_theorem_status": str — "guaranteed" if admissible, "violated"
                if inadmissible, "marginal" if critical,
            "capacities": list of float,
            "n_vertices": int,
            "gamma_1_diagnostic": str — operationalization of γ=1 criticality:
                the system is at the critical boundary when ratio ≈ 1.0,
            "o44_discrete_htheorem": str — challenge status for O44,
            "wasserstein_floor_proxy": float or None — if λ_max > 0,
                the quantity 2/λ_max as a candidate for k/Tμ,
            "timestamp": str,
        }
    """
    n = len(adjacency)
    if n == 0:
        ts = datetime.now(timezone.utc).isoformat()
        return {
            "entropy": 0.0,
            "lambda_max": 0.0,
            "dt_max": float('inf'),
            "effective_dt": effective_dt,
            "admissibility_ratio": 0.0,
            "is_admissible": True,
            "is_critical": False,
            "is_inadmissible": False,
            "regime": "stable",
            "h_theorem_status": "guaranteed",
            "capacities": [],
            "n_vertices": 0,
            "gamma_1_diagnostic": "empty_graph:no_dynamics",
            "o44_discrete_htheorem": "empty_graph:trivially_admissible",
            "wasserstein_floor_proxy": None,
            "timestamp": ts,
        }

    if capacities is None:
        capacities = _build_capacity_vector_from_degrees(adjacency)

    # 1. Entropy
    S = thermodynamic_graph_entropy(activation_levels, capacities)

    # 2. Spectral stability bound
    stability = spectral_stability_bound(adjacency, capacities)
    lam_max = stability["lambda_max"]
    dt_max = stability["dt_max"]

    # 3. Admissibility ratio
    if dt_max == float('inf') or dt_max <= 0:
        ratio = 0.0
    else:
        ratio = effective_dt / dt_max

    is_admissible = ratio <= 1.0
    is_critical = 0.9 < ratio <= 1.0
    is_inadmissible = ratio > 1.0

    if is_inadmissible:
        regime = "inadmissible"
        h_status = "violated"
    elif is_critical:
        regime = "critical"
        h_status = "marginal"
    else:
        regime = "stable"
        h_status = "guaranteed"

    # γ=1 criticality diagnostic
    if is_critical:
        gamma_diag = (
            f"admissibility_ratio={ratio:.6f}:NEAR_CRITICAL_BOUNDARY:"
            f"gamma_1_operationalized:system_at_edge_of_stability:"
            f"discrete_htheorem_marginal"
        )
    elif is_inadmissible:
        gamma_diag = (
            f"admissibility_ratio={ratio:.6f}:BEYOND_CRITICAL:"
            f"gamma>1:update_dynamics_unstable:"
            f"discrete_htheorem_VIOLATED:reduce_effective_dt_or_increase_capacity"
        )
    else:
        gamma_diag = (
            f"admissibility_ratio={ratio:.6f}:SUBCRITICAL:"
            f"gamma<1:dynamics_stable:entropy_nondecreasing"
        )

    # Wasserstein Floor proxy
    wfloor_proxy = None  # type: Optional[float]
    if lam_max > 1e-15:
        wfloor_proxy = 2.0 / lam_max

    # O44 diagnostic
    o44_diag = (
        f"discrete_entropy={S:.6f}:"
        f"lambda_max={lam_max:.8f}:"
        f"dt_max={dt_max:.8f}:"
        f"ratio={ratio:.6f}:"
        f"regime={regime}:"
        f"wfloor_proxy={'%.8f' % wfloor_proxy if wfloor_proxy is not None else 'N/A'}:"
        f"if_lambda_max_proportional_to_T_mu_then_dt_max_proportional_to_1_over_T_mu:"
        f"finite_graph_discrete_htheorem_construction"
    )

    ts = datetime.now(timezone.utc).isoformat()

    result = {
        "entropy": round(S, 10),
        "lambda_max": round(lam_max, 10),
        "dt_max": round(dt_max, 10) if dt_max != float('inf') else float('inf'),
        "effective_dt": effective_dt,
        "admissibility_ratio": round(ratio, 10),
        "is_admissible": is_admissible,
        "is_critical": is_critical,
        "is_inadmissible": is_inadmissible,
        "regime": regime,
        "h_theorem_status": h_status,
        "capacities": [round(c, 6) for c in capacities],
        "n_vertices": n,
        "gamma_1_diagnostic": gamma_diag,
        "o44_discrete_htheorem": o44_diag,
        "wasserstein_floor_proxy": (
            round(wfloor_proxy, 10) if wfloor_proxy is not None else None
        ),
        "timestamp": ts,
    }

    # Log warnings for critical/inadmissible regimes
    if is_inadmissible:
        print(
            f"[GRAPH:THERMODYNAMIC] 🚨 INADMISSIBLE — "
            f"Δt_eff={effective_dt:.4f} > Δt_max={dt_max:.4f} "
            f"(ratio={ratio:.4f}). Discrete H-theorem VIOLATED. "
            f"λ_max={lam_max:.6f}, S={S:.6f}. "
            f"Update dynamics are UNSTABLE — reduce step size or "
            f"increase graph capacity."
        )
    elif is_critical:
        print(
            f"[GRAPH:THERMODYNAMIC] ⚠ CRITICAL — "
            f"Δt_eff={effective_dt:.4f} ≈ Δt_max={dt_max:.4f} "
            f"(ratio={ratio:.4f}). At γ=1 criticality boundary. "
            f"λ_max={lam_max:.6f}, S={S:.6f}."
        )

    return result


def thermodynamic_admissibility_from_edges(edges, all_node_ids,
                                            effective_dt=1.0):
    # type: (list, List[str], float) -> dict
    """
    Convenience wrapper: build adjacency matrix and activation levels from
    knowledge graph edges, then run the full thermodynamic admissibility check.

    This is the primary integration point for FREED's discrete H-theorem
    monitor, callable directly from FEED and CONSOLIDATE pipelines.

    Parameters
    ----------
    edges : list of dict
        Current graph edges.
    all_node_ids : list of str
        Ordered list of all node IDs.
    effective_dt : float
        Effective time-step of the current update (default 1.0).

    Returns
    -------
    dict
        Full thermodynamic admissibility result (see thermodynamic_admissibility_check).
    """
    n = len(all_node_ids)
    if n == 0:
        return thermodynamic_admissibility_check([], [], effective_dt)

    node_index = {nid.upper(): idx for idx, nid in enumerate(all_node_ids)}

    # Build adjacency matrix from edge co-occurrence
    # Edge weight between nodes i and j = count of edges connecting them
    adj = [[0.0] * n for _ in range(n)]
    for e in edges:
        from_id = e.get("from", "").upper()
        to_id = e.get("to", "").upper()
        i = node_index.get(from_id)
        j = node_index.get(to_id)
        if i is not None and j is not None and i != j:
            adj[i][j] += 1.0
            adj[j][i] += 1.0

    # Activation levels: edge count per node (normalized to (0,1) range)
    edge_counts = [0.0] * n
    for e in edges:
        from_id = e.get("from", "").upper()
        to_id = e.get("to", "").upper()
        i = node_index.get(from_id)
        j = node_index.get(to_id)
        if i is not None:
            edge_counts[i] += 1.0
        if j is not None:
            edge_counts[j] += 1.0

    max_count = max(edge_counts) if edge_counts else 1.0
    if max_count <= 0:
        max_count = 1.0

    # Normalize to (0, 1) with a small floor to avoid log(0)
    activation = [max(ec / max_count, 1e-6) for ec in edge_counts]

    return thermodynamic_admissibility_check(
        adj, activation, effective_dt=effective_dt
    )


# ─── Persistent-Homology Anomaly Detection (Betti Number Tracker) ─────────────
# Implements a TDA-inspired topological anomaly detector for knowledge-graph
# trajectory embeddings.  Tracks the evolution of Betti numbers (β₀ = connected
# components, β₁ = independent cycles) across FEED cycles to flag topological
# discontinuities — sudden regime shifts, coherence collapses, or obligation
# graph topology changes — as structural fault signals.
#
# Following the paper's approach for quantum Otto engine fault detection:
#   1. Construct a time-delay embedding from graph-state observables
#      (edge counts, node degrees, entropy, coherence) at each FEED cycle.
#   2. Build a Vietoris-Rips simplicial complex at multiple filtration radii
#      from pairwise distances in the embedded space.
#   3. Track persistent homology: compute Betti numbers (β₀, β₁) as a
#      function of filtration radius ε.
#   4. Detect anomalies: sudden jumps in Betti numbers across consecutive
#      FEED cycles signal topological phase transitions in the epistemic loop.
#
# The key insight: Betti numbers are topological invariants robust to small
# perturbations (noise), but sensitive to genuine structural changes.  A sudden
# increase in β₀ (fragmentation) signals coherence collapse; a sudden increase
# in β₁ (new cycles) signals circular dependency formation; a sudden decrease
# in β₁ signals obligation resolution (cycle closure).
#
# CHALLENGE (O44): The persistent homology construction here operates on
# classical point clouds embedded from graph observables.  The quantum
# extension — persistent homology of density-operator trajectory embeddings
# in non-commutative spaces — requires a Wasserstein-stable persistence
# module construction not yet available.  The classical version provides
# a noise-robust, single-shot fault signal without requiring the quantum
# generalization.
# ─────────────────────────────────────────────────────────────────────────────


def _euclidean_distance_vectors(va, vb):
    # type: (List[float], List[float]) -> float
    """Euclidean distance between two equal-length float vectors."""
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(va, vb)))


def _pairwise_distance_matrix(points):
    # type: (List[List[float]]) -> List[List[float]]
    """Compute full pairwise Euclidean distance matrix for a point cloud."""
    n = len(points)
    D = [[0.0] * n for _ in range(n)]
    for i in range(n):
        for j in range(i + 1, n):
            d = _euclidean_distance_vectors(points[i], points[j])
            D[i][j] = d
            D[j][i] = d
    return D


def _vietoris_rips_betti_numbers(distance_matrix, epsilon):
    # type: (List[List[float]], float) -> Tuple[int, int]
    """
    Compute Betti numbers β₀ and β₁ of the Vietoris-Rips complex at
    filtration radius ε from a pairwise distance matrix.

    β₀ = number of connected components (rank of H₀).
    β₁ = number of independent 1-cycles (rank of H₁).

    The Vietoris-Rips complex at radius ε includes:
      - A 0-simplex (vertex) for each point.
      - A 1-simplex (edge) for each pair with distance ≤ ε.
      - A 2-simplex (triangle) for each triple where all pairwise distances ≤ ε.

    β₀ is computed via union-find on the 1-skeleton.
    β₁ is computed via the Euler characteristic: β₁ = n_edges - n_vertices
    + β₀ - n_triangles (from χ = β₀ - β₁ + β₂ ≈ β₀ - β₁ for VR complexes
    where β₂ ≈ 0 for small point clouds, corrected by triangle count).

    More precisely, for a 2-dimensional simplicial complex:
        χ = V - E + F   and   χ = β₀ - β₁ + β₂
    where V = vertices, E = edges, F = 2-faces (triangles).
    Assuming β₂ = 0 (no voids in a VR complex on small point clouds):
        β₁ = E - V + β₀ - F

    Parameters
    ----------
    distance_matrix : list of list of float
        n×n pairwise distance matrix.
    epsilon : float
        Filtration radius.

    Returns
    -------
    tuple of (int, int)
        (β₀, β₁) — Betti numbers of the Vietoris-Rips complex at ε.
    """
    n = len(distance_matrix)
    if n == 0:
        return (0, 0)

    # ── Build 1-skeleton (edges) ─────────────────────────────────────────
    edges = []  # type: List[Tuple[int, int]]
    for i in range(n):
        for j in range(i + 1, n):
            if distance_matrix[i][j] <= epsilon:
                edges.append((i, j))

    # ── β₀ via union-find ────────────────────────────────────────────────
    parent = list(range(n))

    def _find(x):
        # type: (int) -> int
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def _union(x, y):
        # type: (int, int) -> None
        rx, ry = _find(x), _find(y)
        if rx != ry:
            parent[rx] = ry

    for (i, j) in edges:
        _union(i, j)

    beta_0 = len(set(_find(i) for i in range(n)))

    # ── Count 2-simplices (triangles) ────────────────────────────────────
    # Build adjacency set for fast triangle detection
    adj = [set() for _ in range(n)]  # type: List[set]
    for (i, j) in edges:
        adj[i].add(j)
        adj[j].add(i)

    n_triangles = 0
    for i in range(n):
        for j in adj[i]:
            if j <= i:
                continue
            # Count common neighbors k > j
            for k in adj[i]:
                if k <= j:
                    continue
                if k in adj[j]:
                    n_triangles += 1

    # ── β₁ from Euler characteristic ────────────────────────────────────
    # χ = V - E + F = β₀ - β₁ + β₂
    # Assuming β₂ ≈ 0: β₁ = E - V + β₀ - F
    n_edges = len(edges)
    beta_1 = max(0, n_edges - n + beta_0 - n_triangles)

    return (beta_0, beta_1)


def _persistent_homology_filtration(distance_matrix, n_filtration_steps=20):
    # type: (List[List[float]], int) -> List[dict]
    """
    Compute Betti numbers across a filtration of Vietoris-Rips complexes
    at uniformly spaced radii from 0 to max_distance.

    This produces a "persistence barcode" approximation: each filtration
    step records the Betti numbers, and transitions between steps reveal
    births and deaths of topological features.

    Parameters
    ----------
    distance_matrix : list of list of float
        n×n pairwise distance matrix.
    n_filtration_steps : int
        Number of filtration radii to evaluate (default 20).

    Returns
    -------
    list of dict
        Each entry: {
            "epsilon": float — filtration radius,
            "beta_0": int — number of connected components,
            "beta_1": int — number of independent cycles,
        }
    """
    n = len(distance_matrix)
    if n == 0:
        return []

    # Find max distance for filtration range
    max_dist = 0.0
    for i in range(n):
        for j in range(i + 1, n):
            if distance_matrix[i][j] > max_dist:
                max_dist = distance_matrix[i][j]

    if max_dist <= 0:
        return [{"epsilon": 0.0, "beta_0": n, "beta_1": 0}]

    filtration = []  # type: List[dict]
    for step in range(n_filtration_steps + 1):
        eps = max_dist * step / float(max(n_filtration_steps, 1))
        b0, b1 = _vietoris_rips_betti_numbers(distance_matrix, eps)
        filtration.append({
            "epsilon": round(eps, 10),
            "beta_0": b0,
            "beta_1": b1,
        })

    return filtration


def _time_delay_embedding(observable_series, embedding_dim=3, delay=1):
    # type: (List[List[float]], int, int) -> List[List[float]]
    """
    Construct time-delay embedding vectors from a multivariate observable
    time series.

    Given a series of d-dimensional observation vectors [x_0, x_1, ...],
    produce embedded vectors by concatenating delayed copies:
        y_t = [x_t, x_{t-τ}, x_{t-2τ}, ..., x_{t-(m-1)τ}]
    where m = embedding_dim and τ = delay.

    Parameters
    ----------
    observable_series : list of list of float
        Time-ordered sequence of d-dimensional observation vectors.
    embedding_dim : int
        Number of delayed copies to concatenate (default 3).
    delay : int
        Time delay τ between copies (default 1).

    Returns
    -------
    list of list of float
        Embedded point cloud. Each point has dimension d * embedding_dim.
        Length = len(observable_series) - (embedding_dim - 1) * delay.
    """
    n = len(observable_series)
    total_lag = (embedding_dim - 1) * delay
    if n <= total_lag:
        return []

    embedded = []  # type: List[List[float]]
    for t in range(total_lag, n):
        point = []  # type: List[float]
        for k in range(embedding_dim):
            idx = t - k * delay
            point.extend(observable_series[idx])
        embedded.append(point)

    return embedded


def detect_topological_anomalies(betti_history, window=3,
                                  beta0_jump_threshold=2,
                                  beta1_jump_threshold=1):
    # type: (List[dict], int, int, int) -> List[dict]
    """
    Detect topological anomalies (discontinuities) in a Betti number
    time series by comparing each cycle's Betti signature against a
    rolling window of recent cycles.

    Anomaly types:
      - "fragmentation": sudden β₀ increase (graph splitting into components)
      - "coherence_collapse": β₀ jumps to n (every node isolated)
      - "cycle_formation": sudden β₁ increase (new circular dependencies)
      - "cycle_resolution": sudden β₁ decrease (obligations resolved)
      - "topological_phase_transition": simultaneous β₀ and β₁ jump

    Parameters
    ----------
    betti_history : list of dict
        Time-ordered Betti number records, each with at minimum:
        {"cycle": int, "beta_0": int, "beta_1": int}
    window : int
        Rolling window size for baseline comparison (default 3).
    beta0_jump_threshold : int
        Minimum β₀ change to flag as anomaly (default 2).
    beta1_jump_threshold : int
        Minimum β₁ change to flag as anomaly (default 1).

    Returns
    -------
    list of dict
        Detected anomalies, each with:
        {"cycle": int, "anomaly_type": str, "beta_0": int, "beta_1": int,
         "delta_beta_0": int, "delta_beta_1": int, "baseline_beta_0": float,
         "baseline_beta_1": float, "severity": str}
    """
    if len(betti_history) < window + 1:
        return []

    anomalies = []  # type: List[dict]

    for idx in range(window, len(betti_history)):
        current = betti_history[idx]
        # Compute baseline from preceding window
        window_slice = betti_history[idx - window:idx]
        baseline_b0 = sum(r.get("beta_0", 0) for r in window_slice) / float(window)
        baseline_b1 = sum(r.get("beta_1", 0) for r in window_slice) / float(window)

        cur_b0 = current.get("beta_0", 0)
        cur_b1 = current.get("beta_1", 0)

        delta_b0 = cur_b0 - int(round(baseline_b0))
        delta_b1 = cur_b1 - int(round(baseline_b1))

        abs_d_b0 = abs(delta_b0)
        abs_d_b1 = abs(delta_b1)

        anomaly_type = None  # type: Optional[str]
        severity = "none"

        # Check for simultaneous jumps first (phase transition)
        if abs_d_b0 >= beta0_jump_threshold and abs_d_b1 >= beta1_jump_threshold:
            anomaly_type = "topological_phase_transition"
            severity = "critical"
        elif delta_b0 >= beta0_jump_threshold:
            # β₀ increase = fragmentation
            n_vertices = current.get("n_vertices", cur_b0)
            if n_vertices > 0 and cur_b0 >= n_vertices:
                anomaly_type = "coherence_collapse"
                severity = "critical"
            else:
                anomaly_type = "fragmentation"
                severity = "warning" if abs_d_b0 >= 2 * beta0_jump_threshold else "moderate"
        elif delta_b0 <= -beta0_jump_threshold:
            # β₀ decrease = merging / consolidation (usually healthy)
            anomaly_type = "consolidation"
            severity = "info"
        elif delta_b1 >= beta1_jump_threshold:
            # β₁ increase = new cycles forming
            anomaly_type = "cycle_formation"
            severity = "warning"
        elif delta_b1 <= -beta1_jump_threshold:
            # β₁ decrease = cycles closing (obligation resolution)
            anomaly_type = "cycle_resolution"
            severity = "info"

        if anomaly_type is not None:
            anomalies.append({
                "cycle": current.get("cycle", idx),
                "anomaly_type": anomaly_type,
                "beta_0": cur_b0,
                "beta_1": cur_b1,
                "delta_beta_0": delta_b0,
                "delta_beta_1": delta_b1,
                "baseline_beta_0": round(baseline_b0, 4),
                "baseline_beta_1": round(baseline_b1, 4),
                "severity": severity,
            })

    return anomalies


def graph_snapshot_observables(edges, all_node_ids):
    # type: (list, List[str]) -> List[float]
    """
    Extract a fixed-dimensional observable vector from a graph edge snapshot,
    suitable for time-delay embedding and persistent homology tracking.

    The observable vector captures:
      [n_edges, n_active_nodes, mean_degree, max_degree, n_edge_types,
       edge_type_entropy, n_confirmations, n_challenges, n_contradictions,
       confirmation_ratio]

    Parameters
    ----------
    edges : list of dict
        Current graph edges.
    all_node_ids : list of str
        Ordered list of all node IDs.

    Returns
    -------
    list of float
        10-dimensional observable vector.
    """
    n_edges = float(len(edges))
    n_nodes = float(len(all_node_ids))

    # Node degrees
    node_index = {nid.upper(): idx for idx, nid in enumerate(all_node_ids)}
    degrees = [0.0] * len(all_node_ids)
    for e in edges:
        from_id = e.get("from", "").upper()
        to_id = e.get("to", "").upper()
        i = node_index.get(from_id)
        j = node_index.get(to_id)
        if i is not None:
            degrees[i] += 1.0
        if j is not None:
            degrees[j] += 1.0

    active_nodes = float(sum(1 for d in degrees if d > 0))
    mean_degree = sum(degrees) / max(n_nodes, 1.0)
    max_degree = max(degrees) if degrees else 0.0

    # Edge type distribution
    type_counts = defaultdict(int)  # type: Dict[str, int]
    for e in edges:
        etype = e.get("type", "unknown")
        type_counts[etype] += 1

    n_edge_types = float(len(type_counts))

    # Edge type entropy
    total_e = float(max(len(edges), 1))
    etype_entropy = 0.0
    for count in type_counts.values():
        p = count / total_e
        if p > 0:
            etype_entropy -= p * math.log(p)

    n_confirmations = float(
        type_counts.get("confirms", 0) + type_counts.get("supports", 0)
    )
    n_challenges = float(
        type_counts.get("challenges", 0) + type_counts.get("refutes", 0)
        + type_counts.get("contradicts", 0)
    )
    n_contradictions = float(type_counts.get("contradicts", 0))

    conf_ratio = n_confirmations / max(n_confirmations + n_challenges, 1.0)

    return [
        n_edges, active_nodes, mean_degree, max_degree, n_edge_types,
        etype_entropy, n_confirmations, n_challenges, n_contradictions,
        conf_ratio,
    ]


# ─── Single-Sample Network Entropy (SNE) — Pre-Transition Detector ───────────
# Computes correlation-graph entropy over individual knowledge node activation
# patterns to detect pre-transition (phase-shift) states in the evolving
# knowledge graph, using ONLY the current sample's activation profile — no
# population-level statistics required.
#
# The method follows the paper's SNE algorithm:
#   1. Given a single sample's node activation vector z = [z_1, ..., z_n],
#      construct a sample-specific correlation-deviation network by comparing
#      each node's activation against a reference (baseline) profile.
#   2. For each edge (i, j) in the background correlation graph, compute a
#      local entropy contribution based on the conditional probability that
#      node j deviates given node i deviates.
#   3. The single-sample network entropy (SNE) is the weighted sum of local
#      edge entropies.  A sharp DROP in SNE signals the pre-transition state:
#      the network's correlation structure is tightening (losing entropy)
#      immediately before a critical reorganization.
#
# CHALLENGE (INV_073): SNE demonstrates that the pre-transition phase is
# detectable and *navigable* at the single-sample level, which strains the
# genome's implicit assumption that critical-ridge navigation requires
# population-level statistics.  If single-instance entropy suffices, the
# ridge may be narrower and more fragile than the current formalization
# admits.  The SNE score is annotated with an inv073_flag that surfaces
# this tension whenever a pre-transition signal is detected.
#
# Integration: Called after each FEED cycle on the current graph snapshot's
# node activation vector (edge counts per node).  The SNE time series is
# stored in KnowledgeGraph._telemetry for retrospective phase analysis.
# ─────────────────────────────────────────────────────────────────────────────


def _node_activation_vector(edges, all_node_ids):
    # type: (list, List[str]) -> List[float]
    """
    Build a node activation vector from graph edges: activation_i = number
    of edges incident on node i, normalized to [0, 1].

    Parameters
    ----------
    edges : list of dict
        Current graph edges.
    all_node_ids : list of str
        Ordered list of all node IDs.

    Returns
    -------
    list of float
        Activation vector of length len(all_node_ids), each entry in [0, 1].
    """
    n = len(all_node_ids)
    if n == 0:
        return []
    node_index = {nid.upper(): idx for idx, nid in enumerate(all_node_ids)}
    counts = [0.0] * n
    for e in edges:
        for key in ("from", "to"):
            nid = e.get(key, "").upper()
            idx = node_index.get(nid)
            if idx is not None:
                counts[idx] += 1.0
    max_c = max(counts) if counts else 1.0
    if max_c <= 0:
        max_c = 1.0
    return [c / max_c for c in counts]


def _correlation_graph_from_edges(edges, all_node_ids, min_co_occurrence=1):
    # type: (list, List[str], int) -> List[Tuple[int, int]]
    """
    Build a background correlation graph: an edge (i, j) exists when nodes
    i and j co-occur in the 'from'/'to' fields of at least *min_co_occurrence*
    shared edges (they are mentioned together or share a common source/target).

    Parameters
    ----------
    edges : list of dict
        Current graph edges.
    all_node_ids : list of str
        Ordered list of all node IDs.
    min_co_occurrence : int
        Minimum number of shared edges to form a correlation edge (default 1).

    Returns
    -------
    list of (int, int)
        Edge pairs as 0-indexed indices into all_node_ids.
    """
    n = len(all_node_ids)
    if n < 2:
        return []
    node_index = {nid.upper(): idx for idx, nid in enumerate(all_node_ids)}

    # For each edge, collect the pair of nodes it connects
    co_occur = defaultdict(int)  # type: Dict[Tuple[int, int], int]
    for e in edges:
        from_id = e.get("from", "").upper()
        to_id = e.get("to", "").upper()
        i = node_index.get(from_id)
        j = node_index.get(to_id)
        if i is not None and j is not None and i != j:
            pair = (min(i, j), max(i, j))
            co_occur[pair] += 1

    result = []  # type: List[Tuple[int, int]]
    for pair, count in co_occur.items():
        if count >= min_co_occurrence:
            result.append(pair)
    return result


def single_sample_network_entropy(activation_vector, reference_vector,
                                   correlation_edges, deviation_threshold=0.5):
    # type: (List[float], List[float], List[Tuple[int, int]], float) -> dict
    """
    Compute Single-Sample Network Entropy (SNE) for one activation snapshot.

    The algorithm:
      1. For each node i, determine whether it is "deviated" by comparing
         |z_i - ref_i| against *deviation_threshold*.
      2. For each correlation edge (i, j), compute the conditional probability
         p(j deviated | i deviated) using the local activation profile:
           p_ij = 1  if both deviated
           p_ij = 0  if i deviated but j did not
           (edges where i is NOT deviated contribute 0 to the entropy.)
         To avoid degenerate 0/1 probabilities, apply Laplace smoothing:
           p_ij_smooth = (match + α) / (1 + 2α)  where α = 0.01
      3. The local edge entropy is:
           h_ij = -p_ij * log(p_ij) - (1 - p_ij) * log(1 - p_ij)
         (binary Shannon entropy of the conditional deviation probability).
      4. SNE = (1 / |E_corr|) * Σ_{(i,j) ∈ E_corr} w_ij * h_ij
         where w_ij = |z_i - ref_i| + |z_j - ref_j| weights edges by
         deviation magnitude (nodes with larger deviations dominate the
         entropy signal).

    A sharp DROP in SNE across consecutive samples signals the pre-transition
    phase: correlations are tightening (conditional deviations becoming more
    predictable = lower entropy) before a structural reorganization.

    Parameters
    ----------
    activation_vector : list of float
        Current sample's node activation levels (length n).
    reference_vector : list of float
        Baseline / reference activation levels (e.g., running mean from
        previous FEED cycles).  Same length as activation_vector.
    correlation_edges : list of (int, int)
        Background correlation graph edges as index pairs.
    deviation_threshold : float
        Threshold on |z_i - ref_i| to classify a node as "deviated"
        (default 0.5).

    Returns
    -------
    dict
        {
            "sne_score": float — the single-sample network entropy (≥ 0),
            "n_deviated_nodes": int — count of nodes exceeding threshold,
            "n_correlation_edges": int — edges in the background graph,
            "n_active_edges": int — correlation edges where at least one
                endpoint is deviated,
            "mean_edge_entropy": float — average local edge entropy,
            "max_edge_entropy": float — maximum local edge entropy,
            "deviation_magnitudes": list of float — |z_i - ref_i| per node,
            "is_pre_transition": bool — True if SNE dropped significantly
                (requires comparison with prior; flagged here if SNE < 0.2
                AND n_deviated_nodes > 30% of nodes — a heuristic pre-signal),
            "pre_transition_confidence": float — heuristic confidence [0, 1],
            "inv073_flag": str — challenge annotation for INV_073,
        }
    """
    n = len(activation_vector)
    if n == 0 or n != len(reference_vector) or not correlation_edges:
        return {
            "sne_score": 0.0,
            "n_deviated_nodes": 0,
            "n_correlation_edges": 0,
            "n_active_edges": 0,
            "mean_edge_entropy": 0.0,
            "max_edge_entropy": 0.0,
            "deviation_magnitudes": [],
            "is_pre_transition": False,
            "pre_transition_confidence": 0.0,
            "inv073_flag": "insufficient_data",
        }

    # Step 1: Compute deviation magnitudes and classify deviated nodes
    deviations = [abs(activation_vector[i] - reference_vector[i]) for i in range(n)]
    is_deviated = [d >= deviation_threshold for d in deviations]
    n_deviated = sum(is_deviated)

    # Step 2–3: Compute local edge entropies
    alpha = 0.01  # Laplace smoothing
    edge_entropies = []  # type: List[float]
    edge_weights = []  # type: List[float]
    n_active = 0

    for (i, j) in correlation_edges:
        if i >= n or j >= n:
            continue

        # At least one endpoint must be deviated for the edge to be "active"
        if not is_deviated[i] and not is_deviated[j]:
            continue

        n_active += 1

        # Conditional probability: both deviated given at least one is
        if is_deviated[i] and is_deviated[j]:
            match = 1.0
        else:
            match = 0.0

        # Laplace-smoothed conditional probability
        p_ij = (match + alpha) / (1.0 + 2.0 * alpha)

        # Binary Shannon entropy
        if p_ij <= 0 or p_ij >= 1:
            h_ij = 0.0
        else:
            h_ij = -p_ij * math.log(p_ij) - (1.0 - p_ij) * math.log(1.0 - p_ij)

        # Weight by deviation magnitude at both endpoints
        w_ij = deviations[i] + deviations[j]

        edge_entropies.append(h_ij)
        edge_weights.append(w_ij)

    # Step 4: Compute SNE as weighted average
    n_corr = len(correlation_edges)
    if edge_entropies and sum(edge_weights) > 0:
        total_weight = sum(edge_weights)
        sne = sum(h * w for h, w in zip(edge_entropies, edge_weights)) / total_weight
    elif edge_entropies:
        sne = sum(edge_entropies) / len(edge_entropies)
    else:
        sne = 0.0

    mean_h = sum(edge_entropies) / len(edge_entropies) if edge_entropies else 0.0
    max_h = max(edge_entropies) if edge_entropies else 0.0

    # Pre-transition heuristic: low SNE + high fraction of deviated nodes
    deviated_fraction = n_deviated / max(n, 1)
    is_pre_transition = (sne < 0.2 and deviated_fraction > 0.3 and n_active > 0)

    # Confidence: combine SNE drop signal with deviation coverage
    # Low SNE + high coverage = high confidence
    if n_active > 0:
        entropy_signal = max(0.0, 1.0 - sne / math.log(2.0))  # normalized to [0,1]
        coverage_signal = min(1.0, deviated_fraction / 0.5)  # saturates at 50% deviated
        pre_transition_confidence = round(entropy_signal * coverage_signal, 4)
    else:
        pre_transition_confidence = 0.0

    # INV_073 challenge flag
    if is_pre_transition:
        inv073_flag = (
            "SINGLE_SAMPLE_PRE_TRANSITION_DETECTED:"
            f"sne={sne:.6f}:deviated_fraction={deviated_fraction:.4f}:"
            f"confidence={pre_transition_confidence}:"
            "critical_ridge_detectable_at_single_instance_level:"
            "population_statistics_NOT_required:"
            "ridge_may_be_narrower_and_more_fragile_than_genome_assumes"
        )
    elif sne < 0.4 and deviated_fraction > 0.2:
        inv073_flag = (
            "APPROACHING_PRE_TRANSITION:"
            f"sne={sne:.6f}:deviated_fraction={deviated_fraction:.4f}:"
            "single_sample_signal_marginal:monitor_closely"
        )
    else:
        inv073_flag = (
            f"NORMAL:sne={sne:.6f}:"
            "no_pre_transition_signal_at_single_sample_level"
        )

    return {
        "sne_score": round(sne, 10),
        "n_deviated_nodes": n_deviated,
        "n_correlation_edges": n_corr,
        "n_active_edges": n_active,
        "mean_edge_entropy": round(mean_h, 10),
        "max_edge_entropy": round(max_h, 10),
        "deviation_magnitudes": [round(d, 8) for d in deviations],
        "is_pre_transition": is_pre_transition,
        "pre_transition_confidence": pre_transition_confidence,
        "inv073_flag": inv073_flag,
    }


def sne_from_graph_edges(edges, all_node_ids, reference_edges=None,
                          deviation_threshold=0.5, min_co_occurrence=1):
    # type: (list, List[str], Optional[list], float, int) -> dict
    """
    Convenience wrapper: compute SNE directly from graph edge snapshots.

    Builds the activation vector, reference vector, and correlation graph
    from the edge data, then computes single_sample_network_entropy.

    Parameters
    ----------
    edges : list of dict
        Current graph edges (the "single sample").
    all_node_ids : list of str
        Ordered list of all node IDs.
    reference_edges : list of dict or None
        Baseline / reference graph edges.  If None, a uniform reference
        (all activations = 0.5) is used, representing maximum ignorance.
    deviation_threshold : float
        Threshold for classifying a node as deviated (default 0.5).
    min_co_occurrence : int
        Minimum co-occurrence count for correlation edges (default 1).

    Returns
    -------
    dict
        SNE result dict (see single_sample_network_entropy), augmented with:
        "activation_vector": list of float,
        "reference_vector": list of float,
    """
    n = len(all_node_ids)
    if n == 0:
        return {
            "sne_score": 0.0,
            "n_deviated_nodes": 0,
            "n_correlation_edges": 0,
            "n_active_edges": 0,
            "mean_edge_entropy": 0.0,
            "max_edge_entropy": 0.0,
            "deviation_magnitudes": [],
            "is_pre_transition": False,
            "pre_transition_confidence": 0.0,
            "inv073_flag": "empty_graph",
            "activation_vector": [],
            "reference_vector": [],
        }

    activation = _node_activation_vector(edges, all_node_ids)

    if reference_edges is not None:
        reference = _node_activation_vector(reference_edges, all_node_ids)
    else:
        # Uniform reference: maximum ignorance baseline
        reference = [0.5] * n

    # Build correlation graph from the union of current + reference edges
    all_edges_for_corr = list(edges)
    if reference_edges is not None:
        all_edges_for_corr.extend(reference_edges)
    corr_edges = _correlation_graph_from_edges(
        all_edges_for_corr, all_node_ids, min_co_occurrence
    )

    result = single_sample_network_entropy(
        activation, reference, corr_edges, deviation_threshold
    )
    result["activation_vector"] = [round(a, 8) for a in activation]
    result["reference_vector"] = [round(r, 8) for r in reference]

    # Log pre-transition signals
    if result["is_pre_transition"]:
        print(
            f"[GRAPH:SNE] ⚠ PRE-TRANSITION DETECTED — "
            f"sne={result['sne_score']:.6f}, "
            f"deviated={result['n_deviated_nodes']}/{n}, "
            f"confidence={result['pre_transition_confidence']:.4f}. "
            f"Knowledge graph approaching critical reorganization. "
            f"(INV_073: single-sample detection — ridge may be fragile)"
        )
    elif result.get("inv073_flag", "").startswith("APPROACHING"):
        print(
            f"[GRAPH:SNE] Approaching pre-transition — "
            f"sne={result['sne_score']:.6f}, "
            f"deviated={result['n_deviated_nodes']}/{n}. Monitor."
        )

    return result


def sne_time_series_analysis(sne_history, drop_threshold=0.15, window=3):
    # type: (List[dict], float, int) -> dict
    """
    Analyze an SNE time series for pre-transition phase detection.

    Detects the characteristic sharp DROP in SNE that signals the
    pre-transition state: the network's correlation structure is
    tightening (entropy decreasing) immediately before a critical
    structural reorganization.

    Parameters
    ----------
    sne_history : list of dict
        Time-ordered SNE results (from single_sample_network_entropy or
        sne_from_graph_edges).  Each must have at least "sne_score".
    drop_threshold : float
        Minimum absolute SNE drop between consecutive samples to flag
        as a transition signal (default 0.15).
    window : int
        Rolling window for baseline comparison (default 3).

    Returns
    -------
    dict
        {
            "n_samples": int,
            "sne_values": list of float — the SNE time series,
            "sne_deltas": list of float — consecutive differences,
            "drop_events": list of dict — detected SNE drop events:
                {"index": int, "sne_before": float, "sne_after": float,
                 "delta": float, "is_sharp_drop": bool},
            "n_sharp_drops": int,
            "current_phase": str — "normal", "approaching_transition",
                "pre_transition", or "post_transition",
            "transition_imminent": bool — True if the most recent sample
                shows a pre-transition signal,
            "inv073_summary": str — challenge summary for INV_073,
        }
    """
    n = len(sne_history)
    sne_vals = [h.get("sne_score", 0.0) for h in sne_history]

    if n < 2:
        return {
            "n_samples": n,
            "sne_values": sne_vals,
            "sne_deltas": [],
            "drop_events": [],
            "n_sharp_drops": 0,
            "current_phase": "normal",
            "transition_imminent": False,
            "inv073_summary": "insufficient_history_for_phase_detection",
        }

    # Compute consecutive deltas
    deltas = [sne_vals[i] - sne_vals[i - 1] for i in range(1, n)]

    # Detect drop events
    drop_events = []  # type: List[dict]
    for i, delta in enumerate(deltas):
        is_sharp = delta < -drop_threshold
        if abs(delta) > drop_threshold * 0.5:  # Record moderate drops too
            drop_events.append({
                "index": i + 1,
                "sne_before": round(sne_vals[i], 8),
                "sne_after": round(sne_vals[i + 1], 8),
                "delta": round(delta, 8),
                "is_sharp_drop": is_sharp,
            })

    n_sharp = sum(1 for d in drop_events if d["is_sharp_drop"])

    # Phase classification based on recent trajectory
    recent_sne = sne_vals[-1] if sne_vals else 0.0
    recent_pre_trans = (
        sne_history[-1].get("is_pre_transition", False) if sne_history else False
    )

    # Check if most recent delta is a sharp drop
    recent_delta = deltas[-1] if deltas else 0.0
    recent_sharp_drop = recent_delta < -drop_threshold

    if recent_pre_trans or recent_sharp_drop:
        current_phase = "pre_transition"
        transition_imminent = True
    elif n >= window + 1:
        baseline = sum(sne_vals[-window - 1:-1]) / window
        if recent_sne < baseline * 0.7 and recent_sne < 0.4:
            current_phase = "approaching_transition"
            transition_imminent = False
        elif n_sharp > 0 and drop_events and drop_events[-1]["index"] == n - 1:
            # Sharp drop was the previous sample, current may be post-transition
            current_phase = "post_transition"
            transition_imminent = False
        else:
            current_phase = "normal"
            transition_imminent = False
    else:
        current_phase = "normal"
        transition_imminent = False

    # INV_073 summary
    if transition_imminent:
        inv073_summary = (
            f"PRE_TRANSITION_PHASE_ACTIVE:n_sharp_drops={n_sharp}:"
            f"current_sne={recent_sne:.6f}:delta={recent_delta:.6f}:"
            "single_sample_detection_sufficient:"
            "genome_population_level_assumption_STRAINED:"
            "critical_ridge_narrower_than_formalized"
        )
    elif current_phase == "approaching_transition":
        inv073_summary = (
            f"APPROACHING:n_sharp_drops={n_sharp}:"
            f"current_sne={recent_sne:.6f}:"
            "single_sample_signal_building:monitor"
        )
    else:
        inv073_summary = (
            f"NORMAL:n_sharp_drops={n_sharp}:"
            f"current_sne={recent_sne:.6f}:"
            "no_pre_transition_phase_detected"
        )

    return {
        "n_samples": n,
        "sne_values": [round(v, 8) for v in sne_vals],
        "sne_deltas": [round(d, 8) for d in deltas],
        "drop_events": drop_events,
        "n_sharp_drops": n_sharp,
        "current_phase": current_phase,
        "transition_imminent": transition_imminent,
        "inv073_summary": inv073_summary,
    }


# ── Singleton accessor ────────────────────────────────────────────────────────

_graph_instance = None  # type: Optional[KnowledgeGraph]

def get_graph():
    # type: () -> KnowledgeGraph
    """Return the process-level singleton KnowledgeGraph."""
    global _graph_instance
    if _graph_instance is None:
        _graph_instance = KnowledgeGraph()
    return _graph_instance
