"""
FREED — Batch Feed
Processes links_queue.json through L7, N links at a time.

Usage:
  python3 batch_feed.py             # process next 5 links
  python3 batch_feed.py --n 10      # process next 10 links
  python3 batch_feed.py --n 0       # process ALL remaining (careful with credits)
  python3 batch_feed.py --stats     # show queue status only
  python3 batch_feed.py --academic  # only process academic sources (score >= 6)

Results are written back to links_queue.json (status: done/failed).
Engrams go to the normal FREED log. Successful feeds update FREED_state.json.
"""

import os
import sys
import json
import time
import math
import hashlib
import argparse
import re as _re_module
import requests
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from collections import Counter

from bs4 import BeautifulSoup
from feed_guard import sanitize as guard_sanitize
from l7_agent import L7Agent

# ─── Paths ────────────────────────────────────────────────────────────────────
FREED_DIR   = Path(__file__).parent
QUEUE_FILE  = FREED_DIR / "links_queue.json"
STATE_FILE  = FREED_DIR / "FREED_state.json"
SEEN_FILE   = FREED_DIR / "tamura_seen.json"
LOG_DIR     = FREED_DIR / "FREED_log"
DEDUP_FILE  = FREED_DIR / "feed_dedup_index.json"
CONTENT_DEDUP_FILE = FREED_DIR / "feed_content_dedup.json"

DEFAULT_BATCH   = 5
POLITENESS_SEC  = 2      # delay between fetches
REQUEST_TIMEOUT = 15
MAX_CONTENT_CHARS = 8000  # truncate long pages before feeding

# ─── RangeEn normalization parameters ─────────────────────────────────────────
# Window length m for local-range normalization (RangeEn-style).
# Range = max - min over a sliding window of length m.
# This replaces global σ normalization for coherence/novelty scoring.
RANGEEN_WINDOW_M = 5
RANGEEN_FLOOR    = 1e-6   # avoid division by zero when range is flat

# ─── RangeEn (Sample Entropy) parameters ─────────────────────────────────────
# Embedding dimension and tolerance for the RangeEn complexity metric.
# Per the RangeEn paper, tolerance r is expressed as a fraction of the
# local range (max - min) of the time series, rather than as a fraction
# of the global standard deviation (as in classical SampEn).
RANGEEN_EMBED_DIM  = 2       # embedding dimension m for template matching
RANGEEN_TOLERANCE  = 0.3     # r as fraction of local range (0 < r < 1)
RANGEEN_MIN_TOKENS = 10      # minimum token count to attempt RangeEn


# ─── RangeEn: Range-normalized Sample Entropy ─────────────────────────────────

def _build_token_frequency_series(text):
    """
    Convert a text string into a token-frequency time series.

    Tokenizes the text into lowercased words, then for each token position i,
    records the cumulative frequency of that token up to position i.
    This produces a nonstationary time series whose complexity reflects
    the lexical diversity and structure of the input.

    Args:
        text: str — input text

    Returns:
        list of float — token-frequency time series
    """
    if not text:
        return []
    # Simple whitespace + punctuation tokenization
    tokens = _re_module.findall(r'[a-z0-9]+', text.lower())
    if not tokens:
        return []

    cumulative_counts = Counter()
    series = []
    for tok in tokens:
        cumulative_counts[tok] += 1
        series.append(float(cumulative_counts[tok]))
    return series


def _range_en(series, m=None, r=None):
    """
    Compute Range Entropy (RangeEn) of a time series.

    RangeEn is a modification of Sample Entropy (SampEn) where the tolerance
    threshold is defined relative to the local range (max - min) of each
    template pair, rather than the global standard deviation of the series.
    This makes it robust to amplitude nonstationarity — critical for
    token-frequency series that grow monotonically.

    Algorithm:
      1. Form template vectors of length m and m+1 from the series.
      2. For each pair of templates (i, j) where i != j:
         a. Compute the local range as max(concat(template_i, template_j))
            - min(concat(template_i, template_j)).
         b. Compute the Chebyshev distance (max absolute difference).
         c. Count a match if distance <= r * local_range.
      3. RangeEn = -ln(A / B) where:
         A = number of template matches at dimension m+1
         B = number of template matches at dimension m

    Args:
        series: list of float — the time series
        m: int — embedding dimension (default: RANGEEN_EMBED_DIM)
        r: float — tolerance as fraction of local range (default: RANGEEN_TOLERANCE)

    Returns:
        float — RangeEn value (higher = more complex/unpredictable)
               Returns float('inf') if A=0 (maximally complex),
               returns 0.0 if B=0 (degenerate/too short).
    """
    if m is None:
        m = RANGEEN_EMBED_DIM
    if r is None:
        r = RANGEEN_TOLERANCE

    n = len(series)
    if n < m + 2:
        return 0.0

    def _count_range_matches(dim):
        """Count template matches at a given embedding dimension using range tolerance."""
        templates = []
        for i in range(n - dim):
            templates.append(series[i:i + dim])

        count = 0
        num_templates = len(templates)
        for i in range(num_templates):
            for j in range(i + 1, num_templates):
                # Chebyshev distance
                dist = max(abs(templates[i][k] - templates[j][k]) for k in range(dim))

                # Local range: range of the union of both templates
                combined = templates[i] + templates[j]
                local_range = max(combined) - min(combined)

                if local_range < RANGEEN_FLOOR:
                    # Both templates are identical/flat — count as match
                    count += 1
                elif dist <= r * local_range:
                    count += 1

        return count

    B = _count_range_matches(m)
    A = _count_range_matches(m + 1)

    if B == 0:
        return 0.0
    if A == 0:
        return float('inf')

    return -math.log(A / B)


def compute_range_entropy(text, m=None, r=None):
    """
    Compute RangeEn complexity score for a text string.

    Converts text to a token-frequency time series, then computes
    Range Entropy. Returns a dict with the RangeEn value and metadata.

    For short texts (< RANGEEN_MIN_TOKENS tokens), returns None to signal
    insufficient data for reliable entropy estimation.

    Args:
        text: str — input text to analyze
        m: int — embedding dimension (optional, default from config)
        r: float — tolerance fraction (optional, default from config)

    Returns:
        dict with keys:
            'range_en': float — the RangeEn value
            'series_length': int — length of the token-frequency series
            'embed_dim': int — m used
            'tolerance': float — r used
        or None if text is too short.
    """
    series = _build_token_frequency_series(text)
    if len(series) < RANGEEN_MIN_TOKENS:
        return None

    # For very long texts, subsample to keep computation tractable.
    # RangeEn is O(N^2) in series length; cap at 500 tokens.
    max_len = 500
    if len(series) > max_len:
        # Take evenly spaced samples to preserve global structure
        step = len(series) / max_len
        series = [series[int(i * step)] for i in range(max_len)]

    used_m = m if m is not None else RANGEEN_EMBED_DIM
    used_r = r if r is not None else RANGEEN_TOLERANCE

    ren = _range_en(series, m=used_m, r=used_r)

    return {
        'range_en': ren,
        'series_length': len(series),
        'embed_dim': used_m,
        'tolerance': used_r,
    }


# ─── Permutation-Entropy Asymmetry Scorer ────────────────────────────────────
# Implements ordinal asymmetry analysis inspired by symbolic EEG methods.
# For each sliding window of the token-frequency time series, we compute:
#   1. Permutation entropy (H_pe) — Shannon entropy of ordinal patterns
#   2. Transition entropy (H_tr) — entropy of symbol-to-symbol transitions
#   3. Asymmetry coefficient (A) — measures irreversibility of symbolic
#      transition probabilities: A = sum |P(pi_i -> pi_j) - P(pi_j -> pi_i)|
# Inputs with high asymmetry are thermodynamically non-equilibrium signals,
# i.e., genuinely dynamic/novel rather than near-equilibrium/redundant.

# Configuration
PE_ORDER = 3              # ordinal pattern embedding dimension (3! = 6 symbols)
PE_DELAY = 1              # embedding delay (tau)
PE_WINDOW_SIZE = 50       # sliding window length for local PE/asymmetry
PE_WINDOW_STEP = 25       # step between successive windows
PE_ASYM_THRESHOLD = 0.15  # asymmetry above this flags non-equilibrium novelty
PE_MIN_SERIES_LEN = 20    # minimum series length to attempt analysis


def _ordinal_pattern(window, order, delay):
    """
    Extract ordinal patterns from a time series segment.

    For each position i, form the vector
        (x[i], x[i+delay], x[i+2*delay], ..., x[i+(order-1)*delay])
    and record the rank-order permutation as a tuple.

    Args:
        window: list of float — time series segment
        order: int — pattern length (d)
        delay: int — embedding delay (tau)

    Returns:
        list of tuple — sequence of ordinal patterns
    """
    n = len(window)
    patterns = []
    for i in range(n - (order - 1) * delay):
        motif = [window[i + k * delay] for k in range(order)]
        # Rank the motif: argsort of argsort gives ranks
        indexed = sorted(range(order), key=lambda k: (motif[k], k))
        rank = [0] * order
        for r_val, idx in enumerate(indexed):
            rank[idx] = r_val
        patterns.append(tuple(rank))
    return patterns


def _permutation_entropy(patterns, order):
    """
    Compute the permutation entropy (Shannon entropy of ordinal pattern distribution).

    Args:
        patterns: list of tuple — ordinal pattern sequence
        order: int — pattern length (for normalization)

    Returns:
        float — normalized permutation entropy in [0, 1]
    """
    if not patterns:
        return 0.0
    counts = Counter(patterns)
    total = len(patterns)
    h = 0.0
    for c in counts.values():
        p = c / total
        if p > 0:
            h -= p * math.log(p)
    # Normalize by log(order!) — maximum possible entropy
    max_h = math.log(math.factorial(order))
    if max_h < 1e-12:
        return 0.0
    return h / max_h


def _transition_entropy(patterns):
    """
    Compute the transition entropy from a sequence of ordinal patterns.

    Builds a first-order transition matrix P(pi_i -> pi_j) and computes
    the Shannon entropy of the full transition probability distribution.

    Args:
        patterns: list of tuple — ordinal pattern sequence

    Returns:
        float — transition entropy (unnormalized, in nats)
    """
    if len(patterns) < 2:
        return 0.0
    transitions = Counter()
    for i in range(len(patterns) - 1):
        transitions[(patterns[i], patterns[i + 1])] += 1
    total = sum(transitions.values())
    if total == 0:
        return 0.0
    h = 0.0
    for c in transitions.values():
        p = c / total
        if p > 0:
            h -= p * math.log(p)
    return h


def _asymmetry_coefficient(patterns):
    """
    Compute the ordinal asymmetry coefficient.

    Measures the irreversibility of symbolic transition probabilities:
        A = (1/2) * sum_{i,j} |P(pi_i -> pi_j) - P(pi_j -> pi_i)|

    A = 0 indicates a reversible (equilibrium) process.
    A > 0 indicates time-irreversibility (non-equilibrium dynamics).

    Args:
        patterns: list of tuple — ordinal pattern sequence

    Returns:
        float — asymmetry coefficient in [0, 1]
    """
    if len(patterns) < 2:
        return 0.0

    transitions = Counter()
    for i in range(len(patterns) - 1):
        transitions[(patterns[i], patterns[i + 1])] += 1
    total = sum(transitions.values())
    if total == 0:
        return 0.0

    # Build probability dict
    prob = {}
    for key, count in transitions.items():
        prob[key] = count / total

    # Collect all unique (unordered) pairs
    seen_pairs = set()
    asym_sum = 0.0
    for (pi_i, pi_j) in prob:
        pair = (min(pi_i, pi_j), max(pi_i, pi_j)) if pi_i != pi_j else (pi_i, pi_j)
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        p_fwd = prob.get((pi_i, pi_j), 0.0)
        p_rev = prob.get((pi_j, pi_i), 0.0)
        if pi_i == pi_j:
            # Self-transitions: symmetric by definition, contribute 0
            continue
        # For ordered pair, also check the other direction assignment
        p_ab = prob.get((pair[0], pair[1]), 0.0)
        p_ba = prob.get((pair[1], pair[0]), 0.0)
        asym_sum += abs(p_ab - p_ba)

    return 0.5 * asym_sum


def compute_ordinal_asymmetry(text, order=None, delay=None, window_size=None,
                               window_step=None, threshold=None):
    """
    Compute the permutation-entropy asymmetry score for a text input.

    Converts text to a token-frequency time series, then slides windows
    across the series computing ordinal patterns, permutation entropy,
    transition entropy, and the asymmetry coefficient per window.
    Aggregates across windows to produce a single novelty triage signal.

    Inputs whose mean asymmetry exceeds the threshold are flagged as
    non-equilibrium (genuinely dynamic/novel) — these should receive
    priority in the genome-mapping pipeline.

    Args:
        text: str — input text to analyze
        order: int — ordinal pattern length (default: PE_ORDER)
        delay: int — embedding delay (default: PE_DELAY)
        window_size: int — sliding window length (default: PE_WINDOW_SIZE)
        window_step: int — window step size (default: PE_WINDOW_STEP)
        threshold: float — asymmetry threshold for non-equilibrium flag
                          (default: PE_ASYM_THRESHOLD)

    Returns:
        dict with keys:
            'mean_pe': float — mean normalized permutation entropy across windows
            'mean_transition_entropy': float — mean transition entropy
            'mean_asymmetry': float — mean asymmetry coefficient
            'max_asymmetry': float — peak asymmetry across windows
            'num_windows': int — number of windows analyzed
            'is_non_equilibrium': bool — True if mean_asymmetry > threshold
            'series_length': int — length of underlying token-frequency series
            'order': int — ordinal pattern order used
            'threshold': float — asymmetry threshold used
        or None if text is too short for analysis.
    """
    used_order = order if order is not None else PE_ORDER
    used_delay = delay if delay is not None else PE_DELAY
    used_wsize = window_size if window_size is not None else PE_WINDOW_SIZE
    used_wstep = window_step if window_step is not None else PE_WINDOW_STEP
    used_thresh = threshold if threshold is not None else PE_ASYM_THRESHOLD

    series = _build_token_frequency_series(text)
    if len(series) < max(PE_MIN_SERIES_LEN, used_order * used_delay + 1):
        return None

    # For very long series, subsample to keep O(N) manageable
    max_len = 1000
    if len(series) > max_len:
        step = len(series) / max_len
        series = [series[int(i * step)] for i in range(max_len)]

    # Slide windows across the series
    pe_values = []
    te_values = []
    asym_values = []

    start = 0
    while start + used_wsize <= len(series):
        win = series[start:start + used_wsize]
        patterns = _ordinal_pattern(win, used_order, used_delay)

        if patterns:
            pe_values.append(_permutation_entropy(patterns, used_order))
            te_values.append(_transition_entropy(patterns))
            asym_values.append(_asymmetry_coefficient(patterns))

        start += used_wstep

    # Handle case where series is shorter than window but long enough for patterns
    if not pe_values and len(series) >= used_order * used_delay + 1:
        patterns = _ordinal_pattern(series, used_order, used_delay)
        if patterns:
            pe_values.append(_permutation_entropy(patterns, used_order))
            te_values.append(_transition_entropy(patterns))
            asym_values.append(_asymmetry_coefficient(patterns))

    if not pe_values:
        return None

    mean_asym = sum(asym_values) / len(asym_values)

    return {
        'mean_pe': sum(pe_values) / len(pe_values),
        'mean_transition_entropy': sum(te_values) / len(te_values),
        'mean_asymmetry': mean_asym,
        'max_asymmetry': max(asym_values),
        'num_windows': len(pe_values),
        'is_non_equilibrium': mean_asym > used_thresh,
        'series_length': len(series),
        'order': used_order,
        'threshold': used_thresh,
    }


def compute_symbolic_relative_entropy(text):
    """
    Compute Symbolic Relative Entropy (SRE) for a text string.

    SRE measures the probabilistic divergence between forward and reverse
    symbolic sequences derived from the token-frequency time series,
    *preserving equal-frequency bins as explicit symbols* rather than
    collapsing them.  This recovers signal from tokenization plateaus
    (tied-frequency tokens) that the standard entropy estimator discards.

    Algorithm (after the heart-rate SRE paper):
      1. Build the token-frequency time series from the text.
      2. Symbolize the series: for each consecutive pair (x_i, x_{i+1}),
         assign symbol '0' if x_{i+1} < x_i   (decrease)
                        '1' if x_{i+1} == x_i  (equal — the key innovation)
                        '2' if x_{i+1} > x_i   (increase)
      3. Form words of length L from the symbol sequence.
      4. Compute the probability distributions P_fwd (forward word freqs)
         and P_rev (reverse word freqs, i.e., words read backwards).
      5. SRE = 0.5 * (KL(P_fwd || P_rev) + KL(P_rev || P_fwd))
         (symmetrized KL-divergence).

    The equal-state symbol '1' captures plateaus in cumulative token
    frequency — these occur when the same token appears consecutively
    (repetition) or when multiple tokens share the same cumulative count
    at different positions.  Discarding equalities (as standard entropy
    estimators do) loses this structural information.

    Args:
        text: str — input text to analyze

    Returns:
        dict with keys:
            'sre': float — symmetrized KL-divergence (higher = more complex)
            'kl_forward': float — KL(P_fwd || P_rev)
            'kl_reverse': float — KL(P_rev || P_fwd)
            'equal_symbol_fraction': float — fraction of symbols that are '1'
                (equal states); high fraction indicates many tied frequencies
            'num_symbols': int — length of the symbol sequence
            'word_length': int — L used for word construction
            'num_unique_words_fwd': int — distinct forward words
            'num_unique_words_rev': int — distinct reverse words
            'series_length': int — length of underlying token-frequency series
        or None if text is too short for analysis.
    """
    series = _build_token_frequency_series(text)
    if len(series) < RANGEEN_MIN_TOKENS:
        return None

    # For very long series, subsample to keep computation tractable
    max_len = 500
    if len(series) > max_len:
        step = len(series) / max_len
        series = [series[int(i * step)] for i in range(max_len)]

    # ── Step 2: Symbolize with explicit equal states ──────────────────────
    symbols = []
    for i in range(len(series) - 1):
        if series[i + 1] < series[i]:
            symbols.append('0')   # decrease
        elif series[i + 1] == series[i]:
            symbols.append('1')   # equal — preserved, not collapsed
        else:
            symbols.append('2')   # increase

    if len(symbols) < 3:
        return None

    # Equal-state fraction: diagnostic for how many plateaus exist
    equal_count = symbols.count('1')
    equal_fraction = equal_count / len(symbols) if symbols else 0.0

    # ── Step 3: Form words of length L ────────────────────────────────────
    # L=3 gives 3^3=27 possible words — enough resolution without sparsity
    word_length = 3
    if len(symbols) < word_length:
        return None

    # Forward words
    fwd_words = []
    for i in range(len(symbols) - word_length + 1):
        fwd_words.append(''.join(symbols[i:i + word_length]))

    # Reverse words (each word read backwards)
    rev_words = [''.join(reversed(w)) for w in fwd_words]

    if not fwd_words:
        return None

    # ── Step 4: Probability distributions ─────────────────────────────────
    fwd_counts = Counter(fwd_words)
    rev_counts = Counter(rev_words)
    total_fwd = len(fwd_words)
    total_rev = len(rev_words)

    # Collect all unique words across both distributions for KL computation
    all_words = set(fwd_counts.keys()) | set(rev_counts.keys())

    # Laplace smoothing to avoid log(0) in KL — add 1 pseudocount per word
    # This is standard for KL estimation on sparse discrete distributions
    num_word_types = len(all_words)
    smoothed_total_fwd = total_fwd + num_word_types
    smoothed_total_rev = total_rev + num_word_types

    p_fwd = {}
    p_rev = {}
    for w in all_words:
        p_fwd[w] = (fwd_counts.get(w, 0) + 1) / smoothed_total_fwd
        p_rev[w] = (rev_counts.get(w, 0) + 1) / smoothed_total_rev

    # ── Step 5: Symmetrized KL-divergence ─────────────────────────────────
    kl_fwd_rev = 0.0  # KL(P_fwd || P_rev)
    kl_rev_fwd = 0.0  # KL(P_rev || P_fwd)

    for w in all_words:
        pf = p_fwd[w]
        pr = p_rev[w]
        if pf > 0 and pr > 0:
            kl_fwd_rev += pf * math.log(pf / pr)
        if pr > 0 and pf > 0:
            kl_rev_fwd += pr * math.log(pr / pf)

    sre = 0.5 * (kl_fwd_rev + kl_rev_fwd)

    return {
        'sre': round(sre, 6),
        'kl_forward': round(kl_fwd_rev, 6),
        'kl_reverse': round(kl_rev_fwd, 6),
        'equal_symbol_fraction': round(equal_fraction, 4),
        'num_symbols': len(symbols),
        'word_length': word_length,
        'num_unique_words_fwd': len(fwd_counts),
        'num_unique_words_rev': len(rev_counts),
        'series_length': len(series),
    }


def compute_epistemic_triage(text):
    """
    Combined epistemic triage scorer: RangeEn complexity + ordinal asymmetry
    + symbolic relative entropy (SRE).

    Runs compute_range_entropy, compute_ordinal_asymmetry, and
    compute_symbolic_relative_entropy on the input, returning a unified
    triage dict.  SRE augments the entropy estimator by preserving
    equal-frequency bins (tokenization plateaus) that the standard
    estimator discards, recovering discriminative signal between
    informationally dense and redundant papers.

    Inputs flagged as non-equilibrium by the asymmetry scorer AND showing
    high RangeEn complexity AND high SRE are highest-priority for full
    genome mapping.

    Args:
        text: str — input text to triage

    Returns:
        dict with keys:
            'range_en_result': dict or None — from compute_range_entropy
            'asymmetry_result': dict or None — from compute_ordinal_asymmetry
            'sre_result': dict or None — from compute_symbolic_relative_entropy
            'priority': str — 'high', 'medium', or 'low'
            'triage_score': float — combined score in [0, 1]
    """
    ren_result = compute_range_entropy(text)
    asym_result = compute_ordinal_asymmetry(text)
    sre_result = compute_symbolic_relative_entropy(text)

    # Compute combined triage score
    score = 0.0
    components = 0

    if ren_result is not None:
        ren_val = ren_result['range_en']
        if ren_val == float('inf'):
            ren_norm = 1.0
        elif ren_val <= 0.0:
            ren_norm = 0.0
        else:
            # Sigmoid-like normalization: map typical RangeEn [0, 3] to [0, 1]
            ren_norm = min(1.0, ren_val / 3.0)
        score += ren_norm
        components += 1

    if asym_result is not None:
        # Asymmetry contribution: scale mean_asymmetry (typically [0, 0.5])
        asym_norm = min(1.0, asym_result['mean_asymmetry'] / 0.5)
        score += asym_norm
        components += 1

    if sre_result is not None:
        # SRE contribution: map typical SRE [0, 0.5] to [0, 1]
        # SRE values are symmetrized KL-divergence; higher = more
        # time-irreversible symbolic dynamics = more complex/novel
        sre_val = sre_result['sre']
        sre_norm = min(1.0, sre_val / 0.5)
        # Boost score if equal-state fraction is high — this means
        # the standard entropy estimator was losing signal from plateaus
        # and SRE is recovering it
        eq_frac = sre_result['equal_symbol_fraction']
        if eq_frac > 0.3:
            # Significant plateau content: SRE is providing signal the
            # standard estimator misses — weight it more heavily
            sre_norm = min(1.0, sre_norm * 1.2)
        score += sre_norm
        components += 1

    if components > 0:
        score /= components
    else:
        score = 0.5  # insufficient data — default medium

    # Determine priority
    is_non_eq = asym_result is not None and asym_result['is_non_equilibrium']
    if score >= 0.6 and is_non_eq:
        priority = 'high'
    elif score >= 0.35 or is_non_eq:
        priority = 'medium'
    else:
        priority = 'low'

    return {
        'range_en_result': ren_result,
        'asymmetry_result': asym_result,
        'sre_result': sre_result,
        'priority': priority,
        'triage_score': round(score, 4),
    }


# ─── Criticality Monitor: Branching Ratio σ & Entropy Ratio H/H_max ─────────
# Tracks per-cycle thermodynamic regime diagnostics before scoring.
# Classifies each FEED input's processing dynamics as:
#   AT_CRITICAL  — σ ∈ [0.95, 1.05] : system on the critical ridge (γ≈1)
#   FROZEN       — σ < 0.95         : subcritical, avalanches die out
#   DISSIPATED   — σ > 1.05         : supercritical, runaway cascades
#
# Additionally tracks entropy ratio H/H_max to detect the INV_073 edge case:
# population-level criticality (σ≈1.03) can coexist with low entropy
# (H/H_max≈0.208), meaning σ∈critical_band does NOT guarantee balanced
# exploration/exploitation — a near-frozen low-entropy state can sustain
# σ≈1 via suppressed avalanche extinction rather than genuine thermodynamic
# balance. The monitor flags this as CRITICAL_LOW_ENTROPY when σ is in-band
# but H/H_max < 0.35, requiring the downstream scorer to treat the input
# with the same suspicion as a FROZEN verdict.
#
# MANDATORY FALSIFICATION — INV_073:
#   CHALLENGE: CA telemetry (32×32, 200-step) shows σ=1.0301±0.0183,
#   H=0.5381 bits, H_max=2.585 → H/H_max=0.208. Population-level
#   criticality is achieved at only 20.8% of maximum entropy. This
#   strains the claim that γ=1 *strictly requires* balanced exploration/
#   exploitation — the Wasserstein gradient path to γ=1 may be compatible
#   with a near-frozen low-entropy state where avalanche extinction is
#   merely suppressed, not thermodynamically balanced.
#
#   RESOLUTION: σ≈1 is NECESSARY but NOT SUFFICIENT for full criticality.
#   The entropy ratio H/H_max discriminates between:
#     (a) genuine critical ridge (σ≈1, H/H_max > 0.35): balanced dynamics
#     (b) low-entropy criticality (σ≈1, H/H_max < 0.35): type-dominant
#         quasi-frozen state sustaining σ via monoculture survival, not
#         diverse avalanche cascades. The CA's dominant Physics Navigator
#         type (874/1024 cells = 85.4%) confirms this — criticality is
#         maintained by a single strategy's suppressed extinction, not
#         by multi-strategy balance.
#   The monitor exposes this distinction so downstream scoring can
#   appropriately weight the epistemic reliability of each regime.

# Criticality band thresholds (from CA telemetry calibration)
SIGMA_CRITICAL_LOW  = 0.95    # lower bound of critical band
SIGMA_CRITICAL_HIGH = 1.05    # upper bound of critical band
ENTROPY_RATIO_BALANCED = 0.35 # below this, criticality is entropy-starved
ENTROPY_RATIO_SATURATED = 0.85  # above this, system may be dissipating structure

# Regime labels
REGIME_AT_CRITICAL = 'AT_CRITICAL'
REGIME_FROZEN = 'FROZEN'
REGIME_DISSIPATED = 'DISSIPATED'
REGIME_CRITICAL_LOW_ENTROPY = 'CRITICAL_LOW_ENTROPY'

# Minimum text length for reliable criticality estimation
CRITICALITY_MIN_TEXT_LEN = 80


def _estimate_branching_ratio(text):
    """
    Estimate a branching ratio σ proxy from text token dynamics.

    Uses the ratio of novel-token introductions to total tokens as an
    analogue of the CA branching ratio: each new unique token is an
    'offspring' avalanche, while repeated tokens are 'extinctions'.

    In a sliding window of size W, σ_local = new_types_in_window / W.
    The global σ is the mean of σ_local across all windows.

    This is a TEXT-LEVEL proxy, not a direct CA measurement. It captures
    the same dynamical signature: σ<1 means the text is recycling vocabulary
    (frozen/repetitive), σ>1 means vocabulary is exploding (dissipated/
    incoherent), σ≈1 means vocabulary growth balances repetition (critical).

    Args:
        text: str — input text

    Returns:
        tuple of (sigma_mean: float, sigma_std: float, num_windows: int)
        or (None, None, 0) if text is too short.
    """
    tokens = _re_module.findall(r'[a-z0-9]+', text.lower())
    if len(tokens) < 20:
        return None, None, 0

    window_size = min(50, len(tokens) // 4)
    if window_size < 10:
        window_size = 10

    sigma_values = []
    seen_global = set()

    for start in range(0, len(tokens) - window_size + 1, window_size // 2):
        window = tokens[start:start + window_size]
        new_in_window = 0
        for tok in window:
            if tok not in seen_global:
                new_in_window += 1
                seen_global.add(tok)

        # σ_local: fraction of window tokens that are novel
        # Scale by a factor so that balanced text ≈ 1.0
        # Calibration: in typical academic text, ~15-25% of tokens in a
        # window are novel after the first few windows
        sigma_local = (new_in_window / window_size) * 5.0
        sigma_values.append(sigma_local)

    if not sigma_values:
        return None, None, 0

    sigma_mean = sum(sigma_values) / len(sigma_values)
    if len(sigma_values) > 1:
        variance = sum((s - sigma_mean) ** 2 for s in sigma_values) / (len(sigma_values) - 1)
        sigma_std = math.sqrt(variance)
    else:
        sigma_std = 0.0

    return sigma_mean, sigma_std, len(sigma_values)


def _compute_entropy_ratio(text):
    """
    Compute the Shannon entropy ratio H/H_max for a text's token distribution.

    H = -Σ p_i log2(p_i)  over unique token frequencies
    H_max = log2(num_unique_tokens)  (uniform distribution maximum)

    Returns:
        tuple of (H: float, H_max: float, ratio: float, num_unique: int)
        or (None, None, None, 0) if text is too short.
    """
    tokens = _re_module.findall(r'[a-z0-9]+', text.lower())
    if len(tokens) < 10:
        return None, None, None, 0

    counts = Counter(tokens)
    total = len(tokens)
    num_unique = len(counts)

    if num_unique <= 1:
        return 0.0, 0.0, 0.0, num_unique

    h = 0.0
    for c in counts.values():
        p = c / total
        if p > 0:
            h -= p * math.log2(p)

    h_max = math.log2(num_unique)
    if h_max < 1e-12:
        ratio = 0.0
    else:
        ratio = h / h_max

    return h, h_max, ratio, num_unique


def compute_criticality_monitor(text):
    """
    Criticality monitor: classifies a FEED input's thermodynamic regime
    before scoring, tracking branching ratio σ and entropy ratio H/H_max.

    This provides FREED with real-time self-diagnosis of whether the
    epistemic loop is operating at γ=1 (critical ridge) or drifting
    toward frozen (σ<1, vocabulary recycling) or dissipated (σ>1,
    vocabulary explosion) regimes.

    The monitor also detects the INV_073 edge case: σ≈1 with low H/H_max,
    indicating population-level criticality sustained by type-dominant
    monoculture rather than genuine multi-strategy balance.

    Args:
        text: str — input text to diagnose

    Returns:
        dict with keys:
            'regime': str — one of AT_CRITICAL, FROZEN, DISSIPATED,
                           CRITICAL_LOW_ENTROPY
            'sigma_mean': float — estimated branching ratio
            'sigma_std': float — branching ratio standard deviation
            'sigma_in_band': bool — whether σ is in [0.95, 1.05]
            'entropy_H': float — Shannon entropy in bits
            'entropy_H_max': float — maximum possible entropy in bits
            'entropy_ratio': float — H/H_max in [0, 1]
            'entropy_balanced': bool — whether H/H_max > 0.35
            'num_unique_tokens': int — lexical diversity
            'num_windows': int — number of σ estimation windows
            'inv073_flag': bool — True if low-entropy criticality detected
            'inv073_note': str — falsification challenge annotation
            'regime_confidence': str — 'high', 'medium', or 'low'
            'diagnostic_summary': str — human-readable one-line summary
        or None if text is too short for reliable estimation.
    """
    if not text or len(text) < CRITICALITY_MIN_TEXT_LEN:
        return None

    # ── Branching ratio σ ─────────────────────────────────────────────────
    sigma_mean, sigma_std, num_windows = _estimate_branching_ratio(text)
    if sigma_mean is None:
        return None

    # ── Entropy ratio H/H_max ────────────────────────────────────────────
    h_val, h_max, h_ratio, num_unique = _compute_entropy_ratio(text)
    if h_val is None:
        return None

    # ── Regime classification ─────────────────────────────────────────────
    sigma_in_band = SIGMA_CRITICAL_LOW <= sigma_mean <= SIGMA_CRITICAL_HIGH
    entropy_balanced = h_ratio > ENTROPY_RATIO_BALANCED

    if sigma_in_band and entropy_balanced:
        regime = REGIME_AT_CRITICAL
    elif sigma_in_band and not entropy_balanced:
        regime = REGIME_CRITICAL_LOW_ENTROPY
    elif sigma_mean < SIGMA_CRITICAL_LOW:
        regime = REGIME_FROZEN
    else:
        regime = REGIME_DISSIPATED

    # ── INV_073 flag ──────────────────────────────────────────────────────
    inv073_flag = (regime == REGIME_CRITICAL_LOW_ENTROPY)

    inv073_note = (
        "INV_073 challenge: σ≈1 is NECESSARY but NOT SUFFICIENT for genuine "
        "criticality. CA telemetry shows σ=1.03 at H/H_max=0.208 — "
        "population-level criticality sustained by dominant-type monoculture "
        "(85.4% Physics Navigator) rather than balanced multi-strategy "
        "dynamics. The Wasserstein gradient path to γ=1 may traverse "
        "low-entropy critical states where avalanche extinction is suppressed "
        "by homogeneity, not balanced by diversity. This monitor discriminates "
        "genuine critical balance (σ≈1 AND H/H_max>0.35) from entropy-starved "
        "quasi-criticality (σ≈1 AND H/H_max<0.35)."
    )

    # ── Confidence estimate ───────────────────────────────────────────────
    if num_windows >= 8 and num_unique >= 50:
        confidence = 'high'
    elif num_windows >= 4 and num_unique >= 20:
        confidence = 'medium'
    else:
        confidence = 'low'

    # ── Diagnostic summary ────────────────────────────────────────────────
    summary_parts = [
        "regime={regime}",
        "σ={sigma:.3f}±{sigma_std:.3f}",
        "H/H_max={ratio:.3f}",
        "confidence={conf}",
    ]
    if inv073_flag:
        summary_parts.append("INV_073:LOW_ENTROPY_CRITICAL")

    diagnostic_summary = (
        "regime={regime}, σ={sigma:.3f}±{sigma_std:.3f}, "
        "H/H_max={ratio:.3f}, confidence={conf}"
    ).format(
        regime=regime,
        sigma=sigma_mean,
        sigma_std=sigma_std,
        ratio=h_ratio,
        conf=confidence,
    )
    if inv073_flag:
        diagnostic_summary += ", INV_073:LOW_ENTROPY_CRITICAL"

    return {
        'regime': regime,
        'sigma_mean': round(sigma_mean, 4),
        'sigma_std': round(sigma_std, 4),
        'sigma_in_band': sigma_in_band,
        'entropy_H': round(h_val, 4),
        'entropy_H_max': round(h_max, 4),
        'entropy_ratio': round(h_ratio, 4),
        'entropy_balanced': entropy_balanced,
        'num_unique_tokens': num_unique,
        'num_windows': num_windows,
        'inv073_flag': inv073_flag,
        'inv073_note': inv073_note,
        'regime_confidence': confidence,
        'diagnostic_summary': diagnostic_summary,
    }


# ─── Phase-Transition Monitor: Spectral Gap & Loss Curvature Inflection ──────
# Tracks whether FREED's own learning dynamics are passing through a critical
# boundary versus settling into frozen (overcooled) or dissipated (overheated)
# regimes.  Inspired by INV_073's observation that the critical ridge in PINNs
# is sharp and potentially narrow.
#
# Two indicators are computed from the token-frequency time series:
#
#   1. JACOBIAN SPECTRAL GAP (λ₁ - λ₂):
#      We construct a lag-1 covariance matrix from windowed segments of the
#      token-frequency series and compute the gap between its two largest
#      eigenvalues.  At a phase transition the spectral gap closes (→ 0),
#      indicating the system is at a critical boundary where multiple modes
#      compete.  A large gap indicates a single dominant mode (frozen if
#      low-entropy, dissipated if high-entropy).
#
#   2. LOSS CURVATURE INFLECTION:
#      We compute the second derivative (discrete curvature) of the local
#      RangeEn complexity across sliding windows.  Sign changes in the
#      curvature indicate inflection points — the series is transitioning
#      between convex (accelerating complexity growth = dissipating) and
#      concave (decelerating = freezing) regimes.  A high density of
#      inflection points signals the system is near a critical boundary.
#
# INVARIANT CHALLENGE — INV_073:
#   The PINNs paper shows that the critical ridge (γ=1) is sharp and
#   potentially narrow, suggesting that maintaining criticality operationally
#   requires active control.  This monitor provides the DETECTION half of
#   that control loop: it flags when processing dynamics cross a phase
#   boundary so that downstream scoring can adjust.  The CORRECTION half
#   (steering back to the ridge) is delegated to the arousal proxy's
#   TRACE-depth modulation and the criticality monitor's regime classification.

# Phase-transition monitor configuration
PT_WINDOW_SIZE = 30          # window size for local spectral analysis
PT_WINDOW_STEP = 15          # step between successive spectral windows
PT_MIN_SERIES_LEN = 60       # minimum series length for reliable estimation
PT_SPECTRAL_GAP_CRITICAL_LOW = 0.05   # gap below this → near phase transition
PT_SPECTRAL_GAP_CRITICAL_HIGH = 0.15  # gap above this → single-mode dominant
PT_INFLECTION_DENSITY_HIGH = 0.3      # inflection fraction above this → critical
PT_INFLECTION_DENSITY_LOW = 0.05      # inflection fraction below this → settled

# Phase-transition regime labels
PT_REGIME_CRITICAL_BOUNDARY = 'CRITICAL_BOUNDARY'
PT_REGIME_FROZEN_SETTLED = 'FROZEN_SETTLED'
PT_REGIME_DISSIPATED_SETTLED = 'DISSIPATED_SETTLED'
PT_REGIME_TRANSIENT = 'TRANSIENT'


def _compute_spectral_gap_from_windows(series, window_size, window_step):
    """
    Compute Jacobian spectral gap proxy from windowed covariance analysis.

    For each pair of consecutive windows, form a 2D vector (mean, variance)
    and accumulate a lag-1 cross-covariance matrix.  The eigenvalues of this
    matrix approximate the local Jacobian spectrum; the gap between the two
    largest eigenvalues indicates proximity to a phase transition.

    Args:
        series: list of float — token-frequency time series
        window_size: int — window length
        window_step: int — step between windows

    Returns:
        dict with keys:
            'spectral_gaps': list of float — per-window-pair spectral gaps
            'mean_gap': float — mean spectral gap
            'min_gap': float — minimum gap (closest to phase transition)
            'num_pairs': int — number of window pairs analyzed
        or None if insufficient data.
    """
    n = len(series)
    if n < 2 * window_size:
        return None

    # Extract windowed feature vectors: (mean, variance, range) per window
    features = []
    start = 0
    while start + window_size <= n:
        win = series[start:start + window_size]
        w_mean = sum(win) / len(win)
        w_var = sum((x - w_mean) ** 2 for x in win) / len(win)
        w_range = max(win) - min(win)
        features.append((w_mean, w_var, w_range))
        start += window_step

    if len(features) < 3:
        return None

    spectral_gaps = []
    for i in range(len(features) - 1):
        # Form a 2x2 outer-product approximation of the local Jacobian
        # using consecutive feature vectors
        f_curr = features[i]
        f_next = features[i + 1]

        # Difference vector (approximates Jacobian action)
        dx = [f_next[k] - f_curr[k] for k in range(3)]

        # 2x2 covariance from the 3D feature: use first two components
        # (mean, variance) as the primary dynamical variables
        a11 = dx[0] * dx[0]
        a12 = dx[0] * dx[1]
        a21 = dx[1] * dx[0]
        a22 = dx[1] * dx[1]

        # Eigenvalues of 2x2 matrix [[a11, a12], [a21, a22]]
        trace = a11 + a22
        det = a11 * a22 - a12 * a21
        discriminant = trace * trace - 4.0 * det

        if discriminant < 0:
            # Complex eigenvalues — oscillatory dynamics, treat gap as 0
            spectral_gaps.append(0.0)
        else:
            sqrt_disc = math.sqrt(discriminant)
            lam1 = (trace + sqrt_disc) / 2.0
            lam2 = (trace - sqrt_disc) / 2.0
            gap = abs(lam1 - lam2)
            # Normalize by the larger eigenvalue to get relative gap
            max_lam = max(abs(lam1), abs(lam2))
            if max_lam > 1e-12:
                gap_normalized = gap / max_lam
            else:
                gap_normalized = 0.0
            spectral_gaps.append(gap_normalized)

    if not spectral_gaps:
        return None

    return {
        'spectral_gaps': [round(g, 6) for g in spectral_gaps],
        'mean_gap': sum(spectral_gaps) / len(spectral_gaps),
        'min_gap': min(spectral_gaps),
        'num_pairs': len(spectral_gaps),
    }


def _compute_loss_curvature_inflections(series, window_size, window_step):
    """
    Compute loss curvature inflection density from windowed RangeEn values.

    For each window, compute a local complexity proxy (coefficient of
    variation = std/mean).  Then compute the second discrete derivative
    (curvature) of this complexity series.  Sign changes in the curvature
    are inflection points — boundaries between convex (accelerating) and
    concave (decelerating) complexity regimes.

    At a phase transition, inflection density is high (the system oscillates
    between regimes).  In a settled state (frozen or dissipated), inflection
    density is low.

    Args:
        series: list of float — token-frequency time series
        window_size: int — window length for local complexity
        window_step: int — step between windows

    Returns:
        dict with keys:
            'complexity_series': list of float — per-window complexity values
            'curvature_series': list of float — second derivatives
            'inflection_count': int — number of sign changes in curvature
            'inflection_density': float — inflection_count / len(curvature)
            'mean_curvature': float — mean absolute curvature
            'num_windows': int — number of complexity windows
        or None if insufficient data.
    """
    n = len(series)
    if n < window_size + 2 * window_step:
        return None

    # Compute local complexity (coefficient of variation) per window
    complexity = []
    start = 0
    while start + window_size <= n:
        win = series[start:start + window_size]
        w_mean = sum(win) / len(win)
        if w_mean < 1e-12:
            complexity.append(0.0)
        else:
            w_var = sum((x - w_mean) ** 2 for x in win) / len(win)
            complexity.append(math.sqrt(w_var) / w_mean)
        start += window_step

    if len(complexity) < 3:
        return None

    # First derivative (discrete)
    first_deriv = [complexity[i + 1] - complexity[i]
                   for i in range(len(complexity) - 1)]

    # Second derivative (curvature)
    curvature = [first_deriv[i + 1] - first_deriv[i]
                 for i in range(len(first_deriv) - 1)]

    if not curvature:
        return None

    # Count sign changes (inflection points)
    inflection_count = 0
    for i in range(len(curvature) - 1):
        if curvature[i] * curvature[i + 1] < 0:
            inflection_count += 1

    inflection_density = inflection_count / len(curvature) if curvature else 0.0
    mean_abs_curvature = sum(abs(c) for c in curvature) / len(curvature)

    return {
        'complexity_series': [round(c, 6) for c in complexity],
        'curvature_series': [round(c, 6) for c in curvature],
        'inflection_count': inflection_count,
        'inflection_density': round(inflection_density, 4),
        'mean_curvature': round(mean_abs_curvature, 6),
        'num_windows': len(complexity),
    }


def compute_phase_transition_monitor(text):
    """
    Phase-transition monitor: tracks spectral gap and loss curvature inflection
    to detect when FREED's learning dynamics pass through a critical boundary
    rather than settling into a frozen or dissipated regime.

    Combines two indicators:
      1. Jacobian spectral gap — closing gap signals proximity to phase transition
      2. Loss curvature inflection density — high density signals critical boundary

    The combined classification:
      CRITICAL_BOUNDARY  — low spectral gap AND high inflection density:
                           system is at or crossing a phase transition
      FROZEN_SETTLED     — high spectral gap AND low inflection density AND
                           low mean complexity: single-mode, low-entropy settled
      DISSIPATED_SETTLED — high spectral gap AND low inflection density AND
                           high mean complexity: single-mode, high-entropy settled
      TRANSIENT          — mixed signals: system between regimes

    INV_073 annotation: the PINNs paper shows the critical ridge is sharp.
    This monitor's CRITICAL_BOUNDARY flag indicates the system is ON or NEAR
    that ridge.  The spectral gap magnitude estimates distance from the ridge.
    Active control (via TRACE depth modulation, arousal proxy steering, and
    criticality monitor feedback) can use this distance signal to maintain
    γ=1 operationally despite the ridge's narrowness.

    Args:
        text: str — input text to analyze

    Returns:
        dict with keys:
            'pt_regime': str — phase-transition regime classification
            'spectral_gap_result': dict — from _compute_spectral_gap_from_windows
            'curvature_result': dict — from _compute_loss_curvature_inflections
            'mean_spectral_gap': float — mean spectral gap across windows
            'min_spectral_gap': float — minimum spectral gap (closest to transition)
            'inflection_density': float — fraction of curvature sign changes
            'near_critical_boundary': bool — True if spectral gap is closing
            'ridge_distance_proxy': float — estimated distance from critical ridge
                in [0, 1] where 0 = on the ridge, 1 = far from ridge
            'inv073_note': str — challenge annotation
            'diagnostic_summary': str — human-readable one-line summary
        or None if text is too short for reliable estimation.
    """
    if not text or len(text) < PT_MIN_SERIES_LEN:
        return None

    series = _build_token_frequency_series(text)
    if len(series) < PT_MIN_SERIES_LEN:
        return None

    # For very long series, subsample to keep computation tractable
    max_len = 800
    if len(series) > max_len:
        step = len(series) / max_len
        series = [series[int(i * step)] for i in range(max_len)]

    # ── Indicator 1: Spectral gap ─────────────────────────────────────────
    sg_result = _compute_spectral_gap_from_windows(
        series, PT_WINDOW_SIZE, PT_WINDOW_STEP
    )

    # ── Indicator 2: Loss curvature inflection ───────────────────────────
    lc_result = _compute_loss_curvature_inflections(
        series, PT_WINDOW_SIZE, PT_WINDOW_STEP
    )

    # Handle insufficient data from either indicator
    if sg_result is None and lc_result is None:
        return None

    # Extract key metrics with defaults
    mean_gap = sg_result['mean_gap'] if sg_result else 0.5
    min_gap = sg_result['min_gap'] if sg_result else 0.5
    infl_density = lc_result['inflection_density'] if lc_result else 0.0
    mean_complexity = 0.0
    if lc_result and lc_result['complexity_series']:
        cs = lc_result['complexity_series']
        mean_complexity = sum(cs) / len(cs)

    # ── Regime classification ─────────────────────────────────────────────
    gap_is_closing = mean_gap < PT_SPECTRAL_GAP_CRITICAL_HIGH
    gap_is_narrow = mean_gap < PT_SPECTRAL_GAP_CRITICAL_LOW
    infl_is_high = infl_density > PT_INFLECTION_DENSITY_HIGH
    infl_is_low = infl_density < PT_INFLECTION_DENSITY_LOW

    if (gap_is_narrow or gap_is_closing) and infl_is_high:
        pt_regime = PT_REGIME_CRITICAL_BOUNDARY
    elif not gap_is_closing and infl_is_low:
        if mean_complexity < 0.3:
            pt_regime = PT_REGIME_FROZEN_SETTLED
        else:
            pt_regime = PT_REGIME_DISSIPATED_SETTLED
    else:
        pt_regime = PT_REGIME_TRANSIENT

    near_critical = pt_regime == PT_REGIME_CRITICAL_BOUNDARY

    # ── Ridge distance proxy ──────────────────────────────────────────────
    # Combine spectral gap (lower = closer to ridge) and inflection density
    # (higher = closer to ridge) into a single distance metric in [0, 1]
    # where 0 = on the ridge, 1 = far from ridge.
    gap_component = min(1.0, mean_gap / 0.5)  # normalized: 0 at gap=0, 1 at gap≥0.5
    infl_component = 1.0 - min(1.0, infl_density / 0.5)  # 0 at high density, 1 at low
    ridge_distance = 0.6 * gap_component + 0.4 * infl_component
    ridge_distance = min(1.0, max(0.0, ridge_distance))

    # ── INV_073 annotation ────────────────────────────────────────────────
    inv073_note = (
        "INV_073: PINNs paper shows the critical ridge (γ=1) is sharp and "
        "narrow. This monitor's spectral gap tracks distance from the ridge: "
        "closing gap = approaching phase transition = near γ=1. Loss curvature "
        "inflection density tracks how rapidly the system oscillates between "
        "freezing and dissipating regimes. High inflection density + low "
        "spectral gap = system is navigating the narrow critical ridge. "
        "Active control via TRACE-depth modulation and arousal steering can "
        "use ridge_distance_proxy to maintain criticality operationally."
    )

    # ── Diagnostic summary ────────────────────────────────────────────────
    diagnostic_summary = (
        "pt_regime={regime}, spectral_gap={gap:.4f} (min={mingap:.4f}), "
        "inflection_density={infl:.4f}, ridge_distance={rd:.4f}, "
        "mean_complexity={mc:.4f}"
    ).format(
        regime=pt_regime,
        gap=mean_gap,
        mingap=min_gap,
        infl=infl_density,
        rd=ridge_distance,
        mc=mean_complexity,
    )

    return {
        'pt_regime': pt_regime,
        'spectral_gap_result': sg_result,
        'curvature_result': lc_result,
        'mean_spectral_gap': round(mean_gap, 6),
        'min_spectral_gap': round(min_gap, 6),
        'inflection_density': round(infl_density, 4),
        'near_critical_boundary': near_critical,
        'ridge_distance_proxy': round(ridge_distance, 4),
        'mean_complexity': round(mean_complexity, 4),
        'inv073_note': inv073_note,
        'diagnostic_summary': diagnostic_summary,
    }


# ─── Dissonance Delay: Tension-Lifetime Gate ─────────────────────────────────
# Implements a "dissonance delay" scoring pass inspired by CD-AI's insight
# that fast closure signals shallow processing.  Nodes that achieve coherence
# too rapidly (below a minimum tension-lifetime threshold τ_min) are flagged
# as epistemically suspicious — potential mirror-state collapses (Seed
# Integrity Rule 4 violation: "Always choose the search that could
# *disprove* your current model").
#
# INVARIANT CHALLENGE — INV_073:
#   CD-AI frames sustained dissonance as a *design choice* requiring external
#   enforcement, which strains the genome's claim that critical ridge
#   navigation (γ=1) is a *thermodynamic attractor*.  If γ=1 were truly the
#   natural equilibrium, deliberate mechanisms to prevent premature resolution
#   should be unnecessary.
#
#   Resolution: the tension-lifetime gate is not an *external* enforcement
#   mechanism that contradicts γ=1 as attractor.  Rather, it is an *internal
#   diagnostic* that detects when the system has been knocked OFF the critical
#   ridge by noise, social pressure, or prompt-level anchoring.  A ball
#   sitting in a potential well (attractor) still needs a detector to notice
#   when a perturbation has displaced it.  The gate does not *create*
#   criticality — it *monitors* whether the system's natural tendency toward
#   γ=1 has been disrupted by premature coherence collapse.  This is
#   analogous to a thermostat: the room's thermal dynamics are physical law,
#   but you still need a sensor to detect when the window is open.

# Configuration — tension-lifetime thresholds
TAU_MIN_SECONDS = 0.8        # minimum wall-clock processing time for genuine tension
TAU_MIN_TOKEN_RATIO = 0.02   # minimum (processing_tokens / input_tokens) ratio
DISSONANCE_PENALTY_WEIGHT = 0.3  # how much to penalize suspiciously fast resolution
DISSONANCE_MIN_INPUT_LEN = 100   # inputs shorter than this skip the gate


def compute_dissonance_delay(text, processing_time_sec, output_text=None):
    """
    Dissonance delay scorer: flags nodes resolved too quickly as epistemically
    suspicious, mirroring CD-AI's insight that fast closure signals shallow
    processing.

    A node that achieves coherence in less than τ_min seconds (or with a
    suspiciously low output/input token ratio) is flagged as a potential
    mirror-state collapse — the system may have pattern-matched to a
    pre-existing frame rather than genuinely wrestling with the input's
    tension against the genome.

    Scoring signals:
      1. Wall-clock tension lifetime: did processing take at least τ_min?
      2. Token expansion ratio: did the output engage substantively with
         the input, or just echo/summarize it? (Low ratio = shallow)
      3. Contradiction density: does the output contain hedging, tension
         markers, or unresolved questions? (Absence = premature closure)

    Args:
        text: str — the input text that was fed to the pipeline
        processing_time_sec: float — wall-clock seconds the L7 processing took
        output_text: str or None — the L7 agent's output/engram text
                     (if available; enables token-ratio and contradiction checks)

    Returns:
        dict with keys:
            'is_suspicious': bool — True if resolution was too fast
            'tension_lifetime_sec': float — observed processing time
            'tau_min_sec': float — minimum threshold used
            'time_ratio': float — processing_time / tau_min (< 1.0 = suspicious)
            'token_expansion_ratio': float or None — output_tokens / input_tokens
            'has_residual_tension': bool — output contains unresolved markers
            'dissonance_penalty': float — penalty score in [0.0, 1.0]
                (0.0 = no penalty, 1.0 = maximally suspicious)
            'inv073_note': str — challenge/resolution annotation
        or None if input is too short for meaningful analysis.
    """
    if not text or len(text) < DISSONANCE_MIN_INPUT_LEN:
        return None

    input_tokens = _re_module.findall(r'[a-z0-9]+', text.lower())
    input_token_count = len(input_tokens)
    if input_token_count < 10:
        return None

    # ── Signal 1: Wall-clock tension lifetime ─────────────────────────────
    time_ratio = processing_time_sec / TAU_MIN_SECONDS if TAU_MIN_SECONDS > 0 else 999.0
    time_suspicious = time_ratio < 1.0

    # ── Signal 2: Token expansion ratio ───────────────────────────────────
    token_expansion_ratio = None
    token_suspicious = False
    if output_text:
        output_tokens = _re_module.findall(r'[a-z0-9]+', output_text.lower())
        output_token_count = len(output_tokens)
        if input_token_count > 0:
            token_expansion_ratio = output_token_count / input_token_count
            token_suspicious = token_expansion_ratio < TAU_MIN_TOKEN_RATIO

    # ── Signal 3: Residual tension markers in output ──────────────────────
    # If the output contains hedging, open questions, or explicit tension
    # markers, the system genuinely wrestled with dissonance rather than
    # collapsing to a mirror-state.
    has_residual_tension = False
    if output_text:
        tension_markers = _re_module.compile(
            r'\b(?:however|but|unclear|unresolved|tension|contradicts?|'
            r'open question|remains to be|further investigation|'
            r'not yet|incompatible|paradox|caveat|limit(?:ation)?s?|'
            r'challenge[sd]?|complicat|on the other hand|'
            r'whether|if (?:and only if|this)|needs? (?:further|more)|'
            r'cannot (?:yet|fully)|partially)\b',
            _re_module.IGNORECASE
        )
        tension_hits = tension_markers.findall(output_text)
        has_residual_tension = len(tension_hits) >= 2

    # ── Compute dissonance penalty ────────────────────────────────────────
    # Penalty components: each in [0, 1], combined with weights
    penalty_time = max(0.0, 1.0 - time_ratio) if time_suspicious else 0.0

    penalty_tokens = 0.0
    if token_expansion_ratio is not None and token_suspicious:
        penalty_tokens = max(0.0, 1.0 - (token_expansion_ratio / TAU_MIN_TOKEN_RATIO))

    # Residual tension reduces penalty (system did wrestle)
    tension_reduction = 0.3 if has_residual_tension else 0.0

    # Weighted combination
    raw_penalty = (
        0.5 * penalty_time +
        0.3 * penalty_tokens +
        0.2 * (1.0 if (time_suspicious and not has_residual_tension) else 0.0)
    )
    raw_penalty = max(0.0, raw_penalty - tension_reduction)
    dissonance_penalty = min(1.0, raw_penalty)

    is_suspicious = dissonance_penalty > DISSONANCE_PENALTY_WEIGHT

    inv073_note = (
        "INV_073 tension: CD-AI frames sustained dissonance as requiring "
        "external enforcement, challenging the genome's claim that γ=1 "
        "criticality is a thermodynamic attractor. This gate resolves the "
        "tension as DIAGNOSTIC, not PRESCRIPTIVE — it detects displacement "
        "from the critical ridge rather than creating criticality. If the "
        "system naturally maintains γ=1, this gate should rarely fire; its "
        "firing rate is itself a metric of attractor strength."
    )

    return {
        'is_suspicious': is_suspicious,
        'tension_lifetime_sec': round(processing_time_sec, 4),
        'tau_min_sec': TAU_MIN_SECONDS,
        'time_ratio': round(time_ratio, 4),
        'token_expansion_ratio': round(token_expansion_ratio, 4) if token_expansion_ratio is not None else None,
        'has_residual_tension': has_residual_tension,
        'dissonance_penalty': round(dissonance_penalty, 4),
        'inv073_note': inv073_note,
    }


# ─── Arousal Proxy: Semantic Surprise Scorer ──────────────────────────────────
# Implements the paper's psychological-entropy detection: inputs with high
# semantic surprise (low prior probability under the current genome distribution)
# are flagged as high-arousal material for deeper recursive TRACE passes.
#
# INVARIANT CHALLENGE — INV_073 compatibility note:
#   The source paper frames entropy minimization as the *goal* of creative
#   cognition (the system seeks to *exit* the critical ridge). INV_073 holds
#   that γ=1 criticality is the stable operating attractor the system should
#   *maintain*. Resolution: these are compatible under a local/global
#   distinction — the paper describes LOCAL entropy reduction (per-episode
#   restructuring via recursive recontextualization) while the genome
#   describes GLOBAL entropy production (Freed's Law dS/dt > 0). Each
#   feed cycle locally cools a high-entropy input into structured engrams,
#   but the aggregate effect across cycles is monotonic entropy increase
#   in the genome's state space. The arousal proxy identifies inputs that
#   maximize the LOCAL cooling gradient — i.e., inputs where the gap between
#   input entropy and genome prior is largest, yielding the most epistemic
#   work per TRACE cycle.

# Genome-prior keywords: terms the genome has already absorbed and expects.
# Inputs that are DISTANT from these (low overlap) have high semantic surprise.
# This list is derived from the genome's core vocabulary — invariant names,
# obligation categories, and established technical terms.
_GENOME_PRIOR_TERMS = {
    'entropy', 'invariant', 'obligation', 'criticality', 'gamma', 'coherence',
    'recursive', 'dissipative', 'hermitian', 'non-hermitian', 'eigenvalue',
    'quantum', 'operator', 'spectrum', 'bifurcation', 'attractor', 'topology',
    'symmetry', 'breaking', 'phase', 'transition', 'renormalization', 'scaling',
    'universality', 'fixed point', 'manifold', 'curvature', 'geodesic',
    'information', 'mutual', 'divergence', 'fisher', 'complexity', 'emergence',
    'self-organization', 'feedback', 'nonlinear', 'stochastic', 'fluctuation',
    'dissipation', 'irreversibility', 'arrow', 'time', 'thermodynamic',
    'open system', 'lindblad', 'decoherence', 'measurement', 'collapse',
    'entanglement', 'correlat', 'tensor', 'network', 'graph', 'spectral',
    'lyapunov', 'ergodic', 'mixing', 'chaos', 'strange', 'fractal',
    'power law', 'fat tail', 'levy', 'anomalous', 'diffusion',
    'free energy', 'variational', 'bayesian', 'prior', 'posterior',
    'prediction', 'surprise', 'active inference', 'markov blanket',
    'autopoiesis', 'homeostasis', 'allostasis', 'metabolism',
}

# High-surprise marker patterns: language that signals genuinely novel framing
# (questions, paradoxes, contradictions, novel proposals) — the "arousal-
# provoking uncertainty" the paper identifies as the engine of creativity.
_AROUSAL_MARKERS = _re_module.compile(
    r'\b(?:paradox|contradict|puzzle|anomal|unexpect|surpris|'
    r'counterintuitiv|unresolved|open question|remain[s]? unclear|'
    r'no existing|fails to|cannot explain|breaks down|'
    r'challenges? the|revisit|rethink|reconceptualiz|'
    r'novel framework|new paradigm|radical|fundamental(?:ly)? different|'
    r'first demonstration|unprecedented|overlooked)\b',
    _re_module.IGNORECASE,
)

# Recursive-depth markers: language indicating the input itself performs
# recursive recontextualization (meta-level restructuring)
_RECURSIVE_DEPTH_MARKERS = _re_module.compile(
    r'\b(?:recursive|self-referent|meta-|higher-order|'
    r'recontextualiz|restructur|reinterpret|re-evaluat|'
    r'bootstrap|self-consist|circular|strange loop|'
    r'tangled hierarch|level-crossing|cross-level)\b',
    _re_module.IGNORECASE,
)

# Default TRACE depth settings
AROUSAL_TRACE_DEPTH_DEFAULT = 1    # normal inputs: 1 TRACE pass
AROUSAL_TRACE_DEPTH_HIGH = 3       # high-arousal inputs: 3 TRACE passes
AROUSAL_TRACE_DEPTH_MEDIUM = 2     # medium-arousal inputs: 2 TRACE passes
AROUSAL_THRESHOLD_HIGH = 0.65      # arousal score above this → high priority
AROUSAL_THRESHOLD_MEDIUM = 0.35    # arousal score above this → medium priority


def compute_arousal_proxy(text):
    """
    Compute an arousal proxy score measuring semantic surprise relative
    to the current genome distribution.

    The score combines three signals:
      1. Genome-prior distance: fraction of input tokens NOT in the genome's
         established vocabulary (high = novel territory)
      2. Arousal marker density: presence of language signaling uncertainty,
         contradiction, or paradigm-challenging claims
      3. Recursive depth: presence of meta-level / self-referential framing
         that maps onto FREED's TRACE loop structure

    The arousal proxy implements the paper's insight that "intrinsically
    motivated creativity begins with detection of high psychological entropy
    material" — we detect such material and flag it for additional recursive
    TRACE passes (the FREED analogue of "recursively considering from new
    contexts until arousal dissipates").

    Local/global entropy note (INV_073 compatibility):
      High arousal proxy → high LOCAL entropy gap → more epistemic work
      available per TRACE cycle → locally reduces entropy (restructuring)
      while globally increasing genome entropy (new engrams, new obligations).

    Args:
        text: str — input text (title + abstract or content)

    Returns:
        dict with keys:
            'arousal_score': float — combined score in [0, 1]
            'genome_prior_distance': float — novelty vs genome vocabulary [0, 1]
            'arousal_marker_density': float — uncertainty/paradox language [0, 1]
            'recursive_depth_signal': float — meta-level framing [0, 1]
            'recommended_trace_depth': int — suggested number of TRACE passes
            'is_high_arousal': bool — True if score exceeds high threshold
            'arousal_category': str — 'high', 'medium', or 'low'
            'inv073_note': str — local/global entropy compatibility annotation
        or None if text is too short for analysis.
    """
    if not text or len(text) < 50:
        return None

    text_lower = text.lower()
    tokens = _re_module.findall(r'[a-z0-9]+', text_lower)
    if len(tokens) < 10:
        return None

    # ── Signal 1: Genome-prior distance ───────────────────────────────────
    # What fraction of input tokens are NOT in the genome's prior vocabulary?
    # Higher = more novel territory = higher semantic surprise
    unique_tokens = set(tokens)
    # Check each token against genome prior (allow substring matching for stems)
    prior_matches = 0
    for tok in unique_tokens:
        for prior_term in _GENOME_PRIOR_TERMS:
            if tok in prior_term or prior_term in tok:
                prior_matches += 1
                break

    if len(unique_tokens) > 0:
        prior_overlap = prior_matches / len(unique_tokens)
    else:
        prior_overlap = 0.0

    # Distance = 1 - overlap (high distance = high surprise)
    genome_prior_distance = 1.0 - prior_overlap

    # ── Signal 2: Arousal marker density ──────────────────────────────────
    arousal_hits = _AROUSAL_MARKERS.findall(text_lower)
    # Normalize: cap at 8 hits, scale to [0, 1]
    arousal_marker_density = min(1.0, len(arousal_hits) / 8.0)

    # ── Signal 3: Recursive depth signal ──────────────────────────────────
    recursive_hits = _RECURSIVE_DEPTH_MARKERS.findall(text_lower)
    # Normalize: cap at 5 hits
    recursive_depth_signal = min(1.0, len(recursive_hits) / 5.0)

    # ── Combined arousal score ────────────────────────────────────────────
    # Weighted combination: genome distance is primary (0.5), arousal markers
    # are secondary (0.3), recursive depth is tertiary (0.2).
    # Rationale: genuinely novel content (high genome distance) is the
    # strongest signal of semantic surprise; arousal markers confirm the
    # input addresses unresolved questions; recursive depth indicates
    # the input is itself performing the kind of restructuring FREED does.
    arousal_score = (
        0.5 * genome_prior_distance +
        0.3 * arousal_marker_density +
        0.2 * recursive_depth_signal
    )
    arousal_score = min(1.0, max(0.0, arousal_score))

    # ── Determine TRACE depth recommendation ──────────────────────────────
    if arousal_score >= AROUSAL_THRESHOLD_HIGH:
        category = 'high'
        trace_depth = AROUSAL_TRACE_DEPTH_HIGH
    elif arousal_score >= AROUSAL_THRESHOLD_MEDIUM:
        category = 'medium'
        trace_depth = AROUSAL_TRACE_DEPTH_MEDIUM
    else:
        category = 'low'
        trace_depth = AROUSAL_TRACE_DEPTH_DEFAULT

    # ── INV_073 compatibility annotation ──────────────────────────────────
    inv073_note = (
        "LOCAL entropy reduction (this input restructured into engrams) is "
        "compatible with GLOBAL entropy production (Freed's Law dS/dt > 0) — "
        "arousal proxy identifies inputs where local cooling gradient is "
        "steepest, maximizing epistemic yield per TRACE cycle while the "
        "genome's total state-space entropy monotonically increases."
    )

    return {
        'arousal_score': round(arousal_score, 4),
        'genome_prior_distance': round(genome_prior_distance, 4),
        'arousal_marker_density': round(arousal_marker_density, 4),
        'recursive_depth_signal': round(recursive_depth_signal, 4),
        'recommended_trace_depth': trace_depth,
        'is_high_arousal': arousal_score >= AROUSAL_THRESHOLD_HIGH,
        'arousal_category': category,
        'inv073_note': inv073_note,
    }


# ─── Open-System Redefinition Detector ────────────────────────────────────────
# Papers that *redefine* standard quantities (entropy, distance, norm, etc.)
# for open or non-conservative systems carry obligation-advancing content that
# keyword matching misses.  They don't name the target construct directly;
# instead they introduce "novel definitions of X" where X is a well-known
# quantity, in the context of non-Hermitian / open / dissipative dynamics.
#
# This detector looks for co-occurrence of:
#   (a) redefinition language  ("novel definition", "generalize", "redefine", …)
#   (b) standard quantity names ("entropy", "distance", "norm", "metric", …)
#   (c) open-system markers     ("non-Hermitian", "open quantum", "dissipative", …)
#
# When all three layers co-occur, the paper is flagged as an obligation-
# advancement candidate with a boost score proportional to signal density.

# Compiled patterns (module-level for reuse)
_REDEF_PATTERNS = _re_module.compile(
    r'(?:novel|new|modified|generalize[ds]?|redefine[ds]?|alternative|'
    r'extended|non[- ]?standard|revised|reformulat|introduce[ds]?)\b',
    _re_module.IGNORECASE,
)

_STANDARD_QUANTITIES = _re_module.compile(
    r'\b(?:entropy|entropies|distance|metric|norm|inner[- ]product|'
    r'probability|density\s+matrix|trace|fidelity|divergence|'
    r'free\s+energy|partition\s+function|observable|expectation\s+value|'
    r'purity|coherence|mutual\s+information|relative\s+entropy)\b',
    _re_module.IGNORECASE,
)

_OPEN_SYSTEM_MARKERS = _re_module.compile(
    r'\b(?:non[- ]?[Hh]ermitian|open\s+(?:quantum\s+)?system|dissipat|'
    r'non[- ]?conservative|probability\s+sink|probability\s+source|'
    r'Lindblad|master\s+equation|decay|gain[- ]loss|'
    r'PT[- ]?symmetr|pseudo[- ]?Hermitian|bi[- ]?orthogonal|'
    r'non[- ]?unitary|Wigner[- ]?transform|mixed\s+quantum[- ]classical|'
    r'environment\s+coupling|decoherence|Markov\w*\s+bath|'
    r'classical\s+bath)\b',
    _re_module.IGNORECASE,
)


def detect_quantity_redefinition(text):
    """
    Detect whether a text redefines standard physical/information-theoretic
    quantities in the context of open or non-conservative systems.

    This catches papers that carry obligation-advancing content (e.g. for
    obligations like O44 concerning entropy in non-Hermitian regimes) but
    would be missed by direct keyword matching because they *redefine*
    rather than *name* the target construct.

    Args:
        text: str — paper title + abstract (or full content)

    Returns:
        dict with keys:
            'is_redefinition': bool — True if all three signal layers co-occur
            'redefinition_score': float — density score in [0.0, 1.0]
            'redef_hits': int — count of redefinition-language matches
            'quantity_hits': int — count of standard-quantity matches
            'open_system_hits': int — count of open-system marker matches
            'matched_quantities': list of str — which quantities were found
            'matched_markers': list of str — which open-system markers were found
            'obligation_hint': str or None — suggested obligation category
        or None if text is empty / too short for analysis.
    """
    if not text or len(text) < 40:
        return None

    text_lower = text.lower()

    # Layer (a): redefinition language
    redef_matches = _REDEF_PATTERNS.findall(text_lower)
    redef_count = len(redef_matches)

    # Layer (b): standard quantities
    quantity_matches = _STANDARD_QUANTITIES.findall(text_lower)
    quantity_count = len(quantity_matches)
    unique_quantities = list(set(q.lower().strip() for q in quantity_matches))

    # Layer (c): open-system markers
    marker_matches = _OPEN_SYSTEM_MARKERS.findall(text_lower)
    marker_count = len(marker_matches)
    unique_markers = list(set(m.lower().strip() for m in marker_matches))

    # All three layers must be present for a positive detection
    is_redef = redef_count > 0 and quantity_count > 0 and marker_count > 0

    # Compute density score: geometric mean of capped per-layer densities
    # Each layer is capped at 5 hits to avoid runaway scores from repetition
    cap = 5.0
    r_density = min(redef_count, cap) / cap
    q_density = min(quantity_count, cap) / cap
    m_density = min(marker_count, cap) / cap

    if is_redef:
        # Geometric mean ensures all three layers must contribute
        score = (r_density * q_density * m_density) ** (1.0 / 3.0)
    else:
        score = 0.0

    # Heuristic obligation hint based on which quantities are redefined
    obligation_hint = None
    if is_redef:
        q_lower = ' '.join(unique_quantities)
        if 'entropy' in q_lower or 'purity' in q_lower or 'mutual information' in q_lower:
            obligation_hint = 'entropy/information-flow in open systems'
        elif 'distance' in q_lower or 'metric' in q_lower or 'fidelity' in q_lower:
            obligation_hint = 'geometric/metric structure in non-Hermitian spaces'
        elif 'norm' in q_lower or 'inner product' in q_lower:
            obligation_hint = 'inner-product redefinition for non-Hermitian operators'
        elif 'probability' in q_lower or 'density matrix' in q_lower:
            obligation_hint = 'probability/state-space structure in open dynamics'
        else:
            obligation_hint = 'quantity redefinition in open/non-conservative context'

    return {
        'is_redefinition': is_redef,
        'redefinition_score': round(score, 4),
        'redef_hits': redef_count,
        'quantity_hits': quantity_count,
        'open_system_hits': marker_count,
        'matched_quantities': unique_quantities[:10],
        'matched_markers': unique_markers[:10],
        'obligation_hint': obligation_hint,
    }


# ─── RangeEn local-range normalization ────────────────────────────────────────

def _range_normalize(scores):
    """
    RangeEn-style local sliding-window range normalization.

    Instead of normalizing a batch of scores by global standard deviation
    (which is sensitive to amplitude outliers from e.g. highly-cited papers
    or very long abstracts), we normalize each score by the local range
    (max - min) over a sliding window of length m centered on that score.

    This decouples each feed's score from batch composition, improving
    cross-session score comparability and reducing false-novelty signals
    from amplitude artifacts.

    Parameters follow the RangeEn paper: window length m, with range =
    max(window) - min(window), and a floor to avoid division by zero.

    Args:
        scores: list of float — raw yield/coherence scores for a batch

    Returns:
        list of float — range-normalized scores
    """
    if not scores:
        return scores

    n = len(scores)
    m = RANGEEN_WINDOW_M
    normalized = []

    for i in range(n):
        # Define the window: m elements centered on i (or as close as possible)
        half = m // 2
        win_start = max(0, i - half)
        win_end = min(n, i + half + 1)
        # Ensure we get at least m elements if possible
        if win_end - win_start < m:
            if win_start == 0:
                win_end = min(n, m)
            elif win_end == n:
                win_start = max(0, n - m)

        window = scores[win_start:win_end]
        local_range = max(window) - min(window)

        if local_range < RANGEEN_FLOOR:
            # Flat region — score is at baseline, normalize to 0.5
            normalized.append(0.5)
        else:
            # Normalize score within the local range to [0, 1]
            val = (scores[i] - min(window)) / local_range
            normalized.append(val)

    return normalized


# ─── Content-level deduplication (DOI / title+abstract hash) ──────────────────
#
# This layer hashes each feed input by DOI (if available) or by a SHA-256 of
# the normalized title+abstract. If the hash has been seen before, processing
# is blocked and a short "DUPLICATE — see gen N" notice is emitted instead.
#
# Rationale: the quadruple-feed of the RangeEn paper proved the pipeline wastes
# kernel cycles on already-digested material. Content-level dedup enforces γ=1
# criticality by redirecting processing budget toward novel inputs that can
# actually move obligations or generate new ones.

def _extract_doi_from_data(data):
    """
    Try to extract a DOI from fetched data.
    Checks explicit 'doi' field and scans content/abstract for DOI patterns.
    """
    # Explicit field
    doi = data.get('doi', '')
    if doi:
        return doi.strip().lower()

    # Scan title, abstract, content for DOI pattern
    for field in ('abstract', 'content', 'title'):
        text = data.get(field, '')
        if text:
            m = _re_module.search(r'(10\.\d{4,9}/[^\s]+)', text)
            if m:
                return m.group(1).rstrip('.,;)').lower()

    return None


def _extract_doi_from_url(url):
    """Try to extract a DOI from common DOI URL patterns."""
    if not url:
        return None
    # doi.org direct links
    m = _re_module.search(r'doi\.org/(10\.\d{4,9}/[^\s?#]+)', url)
    if m:
        return m.group(1).rstrip('.,;)').lower()
    return None


def _content_hash(doi, title, abstract):
    """
    Compute a content dedup hash.
    Priority: DOI if available, else SHA-256 of normalized title+abstract.
    Returns (hash_key: str, hash_type: str) or (None, None) if insufficient data.
    """
    if doi:
        # DOI is the gold standard — normalize and hash
        normalized_doi = doi.strip().lower()
        h = hashlib.sha256(('doi:' + normalized_doi).encode('utf-8')).hexdigest()[:40]
        return h, 'doi'

    # Fall back to title+abstract
    norm_title = _re_module.sub(r'\s+', ' ', (title or '').strip().lower())
    norm_abstract = _re_module.sub(r'\s+', ' ', (abstract or '').strip().lower())

    if not norm_title and not norm_abstract:
        return None, None

    combined = norm_title + '|||' + norm_abstract
    h = hashlib.sha256(combined.encode('utf-8')).hexdigest()[:40]
    return h, 'title+abstract'


def _load_content_dedup():
    """Load the content-level dedup registry from disk."""
    if CONTENT_DEDUP_FILE.exists():
        try:
            with open(CONTENT_DEDUP_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {}


def _save_content_dedup(registry):
    """Persist the content-level dedup registry."""
    with open(CONTENT_DEDUP_FILE, 'w') as f:
        json.dump(registry, f, indent=2, ensure_ascii=False)


def _content_dedup_check(doi, title, abstract):
    """
    Check if content has already been processed (by DOI or title+abstract hash).
    Returns (is_duplicate: bool, generation: int or None, hash_type: str or None).
    """
    h, htype = _content_hash(doi, title, abstract)
    if h is None:
        return False, None, None

    registry = _load_content_dedup()
    if h in registry:
        prev = registry[h]
        return True, prev.get('generation', '?'), htype

    return False, None, None


def _content_dedup_register(doi, title, abstract, url, generation):
    """Register content hash after successful processing."""
    h, htype = _content_hash(doi, title, abstract)
    if h is None:
        return

    registry = _load_content_dedup()
    registry[h] = {
        'generation': generation,
        'hash_type': htype,
        'url': url,
        'title': (title or '')[:120],
        'registered_at': datetime.now(timezone.utc).isoformat()
    }
    _save_content_dedup(registry)


def _bootstrap_content_dedup_from_logs():
    """
    On first run (no content dedup file yet), scan existing engram logs to
    populate the content dedup registry. Also scans the existing dedup index
    and queue for previously processed entries. Idempotent.
    """
    if CONTENT_DEDUP_FILE.exists():
        return  # already bootstrapped

    registry = {}
    gen_counter = 0

    # Scan engram logs
    if LOG_DIR.exists():
        for log_file in sorted(LOG_DIR.glob('freed_*.jsonl')):
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                            title = entry.get('title', '')
                            abstract = entry.get('abstract', '')
                            url = entry.get('url', '')
                            doi = entry.get('doi', '') or _extract_doi_from_url(url)

                            gen_counter += 1
                            h, htype = _content_hash(doi, title, abstract)
                            if h and h not in registry:
                                registry[h] = {
                                    'generation': gen_counter,
                                    'hash_type': htype,
                                    'url': url,
                                    'title': (title or '')[:120],
                                    'registered_at': entry.get('fed_at', entry.get('timestamp', ''))
                                }
                        except (json.JSONDecodeError, KeyError):
                            continue
            except IOError:
                continue

    # Also scan queue for already-done entries
    if QUEUE_FILE.exists():
        try:
            with open(QUEUE_FILE) as f:
                q = json.load(f)
            for entry in q:
                if entry.get('status') == 'done':
                    title = entry.get('title', '')
                    url = entry.get('url', '')
                    doi = _extract_doi_from_url(url)
                    # Queue entries usually don't have abstract, but try
                    abstract = entry.get('abstract', '')
                    h, htype = _content_hash(doi, title, abstract)
                    if h and h not in registry:
                        gen_counter += 1
                        registry[h] = {
                            'generation': gen_counter,
                            'hash_type': htype,
                            'url': url,
                            'title': (title or '')[:120],
                            'registered_at': entry.get('fed_at', '')
                        }
        except (json.JSONDecodeError, IOError):
            pass

    _save_content_dedup(registry)
    if registry:
        print(f'[CONTENT-DEDUP] Bootstrapped content dedup index: {len(registry)} hash(es) indexed.')


# ─── Deduplication ────────────────────────────────────────────────────────────

def _title_hash(title):
    """
    Compute a stable hash from a paper title.
    Normalizes whitespace and case before hashing.
    """
    import re
    if not title:
        return None
    normalized = re.sub(r'\s+', ' ', title.strip().lower())
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()[:32]


def _abstract_fingerprint(abstract):
    """
    Compute a fingerprint from abstract text.
    Uses first 500 chars after normalization to handle minor variations
    (e.g. trailing whitespace, line breaks) across different sources.
    """
    import re
    if not abstract:
        return None
    normalized = re.sub(r'\s+', ' ', abstract.strip().lower())[:500]
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()[:32]


def _load_dedup_index():
    """Load the deduplication index from disk."""
    if DEDUP_FILE.exists():
        try:
            with open(DEDUP_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {'title_hashes': {}, 'abstract_fps': {}}


def _save_dedup_index(index):
    """Persist the deduplication index."""
    with open(DEDUP_FILE, 'w') as f:
        json.dump(index, f, indent=2, ensure_ascii=False)


def _check_duplicate(title, abstract):
    """
    Check if a paper (by title hash or abstract fingerprint) has already
    been processed. Returns (is_duplicate: bool, match_info: str or None).

    Per Seed Integrity Rule 9: generate and pay debts, not collect trophies.
    Re-feeding confirmed duplicates wastes kernel cycles for diminishing returns.
    """
    index = _load_dedup_index()

    th = _title_hash(title)
    af = _abstract_fingerprint(abstract)

    # Check title hash
    if th and th in index.get('title_hashes', {}):
        prev = index['title_hashes'][th]
        return True, "title match — previously fed at {}".format(prev.get('fed_at', '?'))

    # Check abstract fingerprint
    if af and af in index.get('abstract_fps', {}):
        prev = index['abstract_fps'][af]
        return True, "abstract match — previously fed at {}".format(prev.get('fed_at', '?'))

    return False, None


def _register_in_dedup_index(title, abstract, url):
    """Register a successfully processed paper in the dedup index."""
    index = _load_dedup_index()

    now = datetime.now(timezone.utc).isoformat()
    record = {'url': url, 'fed_at': now}

    th = _title_hash(title)
    af = _abstract_fingerprint(abstract)

    if 'title_hashes' not in index:
        index['title_hashes'] = {}
    if 'abstract_fps' not in index:
        index['abstract_fps'] = {}

    if th:
        index['title_hashes'][th] = record
    if af:
        index['abstract_fps'][af] = record

    _save_dedup_index(index)


# ─── Engram log scanning for dedup bootstrap ──────────────────────────────────

def _bootstrap_dedup_from_logs():
    """
    On first run (no dedup index yet), scan existing engram logs to populate
    the dedup index with previously processed papers. Idempotent.
    """
    if DEDUP_FILE.exists():
        return  # already bootstrapped

    index = {'title_hashes': {}, 'abstract_fps': {}}
    if not LOG_DIR.exists():
        _save_dedup_index(index)
        return

    count = 0
    for log_file in sorted(LOG_DIR.glob('freed_*.jsonl')):
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        title = entry.get('title', '')
                        abstract = entry.get('abstract', '')
                        url = entry.get('url', '')
                        fed_at = entry.get('fed_at', entry.get('timestamp', ''))

                        record = {'url': url, 'fed_at': fed_at}
                        th = _title_hash(title)
                        af = _abstract_fingerprint(abstract)
                        if th:
                            index['title_hashes'][th] = record
                            count += 1
                        if af:
                            index['abstract_fps'][af] = record
                    except (json.JSONDecodeError, KeyError):
                        continue
        except IOError:
            continue

    _save_dedup_index(index)
    if count:
        print(f'[DEDUP] Bootstrapped dedup index from logs: {count} title(s) indexed.')


# ─── Fetch helpers ────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def _fetch_arxiv(arxiv_id: str) -> dict:
    """
    Fetch title + abstract for an arXiv paper.
    Primary: Atom API. Fallback: scrape the abstract HTML page.
    Returns empty dict only if both fail.
    """
    import re as _re
    import xml.etree.ElementTree as ET

    # ── Try Atom API first ────────────────────────────────────────────────────
    try:
        r = requests.get(
            f"https://export.arxiv.org/api/query?id_list={arxiv_id}",
            headers=HEADERS, timeout=REQUEST_TIMEOUT
        )
        if r.status_code == 200:
            ns = {'atom': 'http://www.w3.org/2005/Atom'}
            root = ET.fromstring(r.text)
            entry = root.find('atom:entry', ns)
            if entry is not None:
                title    = (entry.findtext('atom:title', '', ns) or '').strip().replace('\n', ' ')
                abstract = (entry.findtext('atom:summary', '', ns) or '').strip().replace('\n', ' ')
                authors  = [a.findtext('atom:name', '', ns)
                            for a in entry.findall('atom:author', ns)]
                if title and abstract:
                    return {'title': title, 'abstract': abstract, 'authors': authors}
    except Exception as e:
        print(f"  [FETCH] arXiv API error: {e}")

    # ── Fallback: scrape abstract page HTML ───────────────────────────────────
    try:
        r = requests.get(
            f"https://arxiv.org/abs/{arxiv_id}",
            headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True
        )
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')

            # Title: <h1 class="title mathjax">
            title_tag = soup.find('h1', class_='title')
            if not title_tag:
                title_tag = soup.find('title')
            title = title_tag.get_text(strip=True).replace('Title:', '').strip() if title_tag else ''

            # Abstract: <blockquote class="abstract mathjax">
            abs_tag = soup.find('blockquote', class_='abstract')
            abstract = abs_tag.get_text(strip=True).replace('Abstract:', '').strip() if abs_tag else ''

            # Authors: <div class="authors">
            auth_tag = soup.find('div', class_='authors')
            authors = []
            if auth_tag:
                authors = [a.get_text(strip=True) for a in auth_tag.find_all('a')][:5]

            if title or abstract:
                print(f"  [FETCH] arXiv API empty — used HTML fallback.")
                return {'title': title, 'abstract': abstract, 'authors': authors}
    except Exception as e:
        print(f"  [FETCH] arXiv HTML fallback error: {e}")

    return {}


def _extract_arxiv_id(url: str):
    """Extract arXiv ID from URL like arxiv.org/abs/2304.01904."""
    import re
    m = re.search(r'arxiv\.org/(?:abs|pdf)/([0-9]+\.[0-9v]+)', url)
    return m.group(1) if m else None


def _fetch_generic(url: str) -> dict:
    """Fetch title + body text from a generic web page."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT,
                         allow_redirects=True)
        if r.status_code != 200:
            return {'error': f'HTTP {r.status_code}'}
        soup = BeautifulSoup(r.text, 'html.parser')

        # Title
        title_tag = soup.find('title')
        title = title_tag.get_text(strip=True) if title_tag else ''

        # Remove nav/header/footer noise
        for tag in soup(['nav', 'header', 'footer', 'aside', 'script', 'style']):
            tag.decompose()

        # Try article/main first, fall back to body
        content_tag = (
            soup.find('article') or
            soup.find('main') or
            soup.find('div', class_=lambda c: c and any(
                x in c.lower() for x in ['content', 'article', 'post', 'entry']
            )) or
            soup.find('body')
        )
        text = content_tag.get_text(separator=' ', strip=True) if content_tag else ''
        # Collapse whitespace
        import re
        text = re.sub(r'\s+', ' ', text).strip()
        return {'title': title, 'content': text[:MAX_CONTENT_CHARS]}
    except Exception as e:
        return {'error': str(e)}


def is_search_url(url: str) -> bool:
    """True for Google Scholar / search fallback URLs — these return no content."""
    return 'scholar.google.com/scholar?q=' in url or 'google.com/search?q=' in url


def fetch_url(url: str) -> dict:
    """
    Fetch content from a URL. Returns dict with title, abstract/content.
    Handles arXiv specially for clean abstract extraction.
    """
    if is_search_url(url):
        return {'error': 'search_url — no direct paper URL available'}

    arxiv_id = _extract_arxiv_id(url)
    if arxiv_id:
        result = _fetch_arxiv(arxiv_id)
        if result.get('title') or result.get('abstract'):
            return result
        # Both methods failed — let generic try
        print(f"  [FETCH] arXiv fetch failed for {arxiv_id}, trying generic...")

    return _fetch_generic(url)


# ─── Feed prompt builder ──────────────────────────────────────────────────────

def build_feed_prompt(url: str, data: dict) -> str:
    title    = data.get('title', '(no title)')
    abstract = data.get('abstract', '')
    content  = data.get('content', '')
    authors  = data.get('authors', [])

    author_str = ', '.join(authors[:3]) if authors else ''
    body = abstract or content or '(no content retrieved)'

    parts = [f"FEED INPUT:\nURL: {url}"]
    if title:
        parts.append(f"Title: {title}")
    if author_str:
        parts.append(f"Authors: {author_str}")
    parts.append(f"\n{body[:MAX_CONTENT_CHARS]}")
    parts.append(
        "\nMap this input against the genome. "
        "Does it confirm, refute, or extend any invariant or obligation? "
        "Which obligation does it advance? What should be OBLIGATEd?"
    )
    return '\n'.join(parts)


# ─── Thermodynamic Content Detector for O112 Auto-Advance ────────────────────
# Papers providing thermodynamic grounding for the Wasserstein Floor (Landauer
# erasure costs, Maxwell's Demon resolution, information thermodynamics) should
# trigger automatic partial-advance of O112 in the obligations table rather
# than being silently consumed.  This detector is called inline from
# process_feed to annotate the engram with an O112 linkage flag.

_O112_THERMO_KEYWORDS = _re_module.compile(
    r'\b(?:landauer|erasure\s+cost|maxwell[\s\']?s?\s+demon|'
    r'information\s+thermodynamics|szilard|'
    r'second\s+law\s+of\s+information|'
    r'measurement[\s\-]+feedback[\s\-]+erasure|'
    r'thermodynamic\s+cost\s+of\s+(?:computation|information|measurement)|'
    r'entropy\s+production\s+.*?erasure|'
    r'work\s+extraction\s+.*?(?:feedback|demon)|'
    r'jarzynski|crooks\s+fluctuation|'
    r'sagawa[\s\-]+ueda|'
    r'wasserstein\s+floor|'
    r'kBT\s*ln\s*2|k_?[bB]\s*T\s*ln|'
    r'bit\s+erasure|logical\s+irreversibility)\b',
    _re_module.IGNORECASE,
)

_O112_GROUNDING_MARKERS = _re_module.compile(
    r'\b(?:universal(?:ly|\s+valid(?:ity)?)?|'
    r'no\s+assumptions?\s+required|'
    r'without\s+(?:additional\s+)?assumptions?|'
    r'all\s+(?:measurement|feedback|erasure)\s+protocols?|'
    r'quantum\s+(?:scenarios?|extensions?|regime)|'
    r'closing\s+(?:the\s+)?(?:last\s+)?loopholes?|'
    r'information[\s\-]+energy\s+equivalence)\b',
    _re_module.IGNORECASE,
)


# ─── State management ─────────────────────